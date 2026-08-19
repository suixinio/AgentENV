package scheduler

import (
	"context"
	"errors"
	"strings"
	"time"

	pausedregistry "agentenv/services/scheduler/internal/registry"

	"go.uber.org/zap"
)

const (
	defaultRegistryReconcileInterval = 30 * time.Second
	defaultRegistryLeaseWarnWindow   = 30 * time.Second

	// rosterStaleMultiplier is how many report intervals a node may miss
	// before its roster stops being reported as current. Three is the same
	// tolerance the node-side registry gives a lease (90s TTL over a 30s
	// refresh): two lost heartbeats are a hiccup, three is a fact.
	rosterStaleMultiplier = 3

	// ghostMinAgeMultiplier is how old a `running` row must be, in report
	// intervals, before its absence from a node's roster counts for anything.
	// One interval is the window in which the node simply has not reported
	// since the row was written; the second is margin for the row's own clock,
	// which begin_pause stamps from the node rather than the database.
	ghostMinAgeMultiplier = 2
)

// registryReconcileInput is one round's raw material: the registry as the
// database sees it, and the rosters as the nodes reported them.
//
// The two clocks are carried separately and never mixed. Registry judgements
// use the database clock the rows were read against; roster freshness uses this
// process's clock, which is the only one the heartbeat timestamps share.
type registryReconcileInput struct {
	listing pausedregistry.Listing
	// rosters is one entry per node this scheduler answers for in the cluster
	// the listing was read under — including the nodes that have never sent a
	// heartbeat, which arrive with a zero LastSeen and no sandboxes. Scoping
	// this to the same cluster as the read is not cosmetic: a scheduler
	// watching two clusters would otherwise compare one cluster's rows against
	// the other's nodes and report every healthy takeover in the second as a
	// fault in the first.
	rosters []Roster
	// resolveNodeID maps the identity a registry row names onto the one
	// discovery uses today.
	//
	// A node writes its rows under the name it reports itself with, which
	// during a fleet upgrade is still its pod name, while rosters are keyed by
	// the name discovery gives it. Comparing the two unresolved attributes
	// every one of that node's rows to a machine that has no roster at all —
	// for as long as the upgrade takes. Nil means identity.
	resolveNodeID   func(string) string
	now             time.Time
	reportTTL       time.Duration
	leaseWarnWindow time.Duration
}

// registryReconcileResult is the derived view of one round. Every field here
// is an observation; nothing acts on any of it.
type registryReconcileResult struct {
	// rowsByState is the registry's own state distribution.
	rowsByState map[pausedregistry.State]int
	// untracked counts sandboxes a node reports that the registry has no row
	// for.
	//
	// 🔴 This is *not* an orphan count, and a healthy cluster's is not zero.
	// mark_running never creates a row, so a sandbox created on a node and
	// never paused has no row by design — which in steady state is most of
	// them. Treat it as an upper bound on orphans and nothing more.
	untracked map[string]int
	// ghost counts rows the registry says are running on a node whose own
	// fresh roster does not list them.
	ghost map[string]int
	// staleCopy counts sandboxes a node reports that the registry attributes
	// to somebody else. During a cross-node takeover the origin keeps its
	// paused record until its own reconciliation drops it, so this being
	// briefly non-zero is the expected transient, not a fault.
	staleCopy map[string]int
	// rowsWithoutRoster counts registry rows whose holder has no current
	// roster, grouped by that holder.
	//
	// 🔴 It is derived from the registry side on purpose, so that it survives
	// the node disappearing. Every other per-node series here is keyed off a
	// roster, and a machine that is removed from discovery has none — so the
	// moment its problem becomes permanent, its ghost/untracked/roster_stale
	// series stop existing and every alert on them resolves itself. This one
	// goes *up* instead, because the rows it counts are still in the table.
	rowsWithoutRoster map[string]int
	// rosterStale marks nodes whose last heartbeat is older than the roster
	// staleness threshold, and nodes that have never sent one at all. It is
	// the closest thing this phase has to e2b's `answered`; the stronger
	// `sync_ok` needs the controller to ask the node something, which nothing
	// does yet.
	rosterStale map[string]bool
	// holderConflict counts sandboxes claimed by two or more nodes with fresh
	// rosters that the registry cannot attribute to one of them. The
	// attributable case is counted as staleCopy on the losing side instead,
	// and the two are mutually exclusive per sandbox: a sandbox counted here
	// is deliberately left out of every staleCopy, because "nobody can say who
	// holds it" and "this node is the one that does not" are contradictory
	// readings of the same fact.
	holderConflict int
	// parkedLeaseExpiring counts publishing/local_only rows that another node
	// could claim and whose lease is about to lapse. These are the alarming
	// ones: the VM is already stopped, and once the lease goes another node
	// may claim the sandbox and rebuild it from the previous snapshot, losing
	// the work since then.
	//
	// A row with no snapshot is not counted, because claim_for_resume requires
	// one — nobody can take those over, so warning that somebody might is a
	// permanent false alarm. They are counted as strandedRows instead.
	parkedLeaseExpiring int
	// strandedRows counts publishing/local_only rows with no snapshot
	// reference: the rows no other node can claim (claim_for_resume requires a
	// snapshot), no reclaim can clear (both reclaim paths only touch live
	// rows), and no lease expiry can move. Only the origin node can still
	// resume them from its own disk; if it never comes back, the row stays in
	// the table forever.
	//
	// Every pause passes through this count briefly — publishing starts with
	// no snapshot and complete_pause writes one — so alert on it lasting, not
	// on it appearing.
	strandedRows int
	// liveLeaseLapsed counts running/resuming rows whose lease has lapsed.
	// Informational: a lapsed lease on a live row proves only that the node
	// cannot reach the database, and nothing takes those over on a timer.
	liveLeaseLapsed int
	// reclaimableNow counts the rows reclaim_expired_holdings will act on next
	// tick — a live row whose lease has lapsed and whose sandbox has outlived
	// its own deadline.
	reclaimableNow int
	// invalidRows counts paused rows with no snapshot reference. The node-side
	// decoder fails the whole get_many batch on one of these, so a single bad
	// row silently freezes one machine's reconciliation. It should be zero.
	invalidRows int
}

// computeRegistryReconcile derives one round's counters. It is pure so the
// definitions above can be tested against fixed inputs rather than against a
// live cluster.
func computeRegistryReconcile(in registryReconcileInput) registryReconcileResult {
	reportTTL := in.reportTTL
	if reportTTL <= 0 {
		reportTTL = defaultObservedReportTTL
	}
	leaseWarnWindow := in.leaseWarnWindow
	if leaseWarnWindow <= 0 {
		leaseWarnWindow = defaultRegistryLeaseWarnWindow
	}
	resolve := in.resolveNodeID
	if resolve == nil {
		resolve = func(nodeID string) string { return nodeID }
	}

	result := registryReconcileResult{
		rowsByState: map[pausedregistry.State]int{
			pausedregistry.StatePublishing: 0,
			pausedregistry.StatePaused:     0,
			pausedregistry.StateResuming:   0,
			pausedregistry.StateLocalOnly:  0,
			pausedregistry.StateRunning:    0,
		},
		untracked:         make(map[string]int, len(in.rosters)),
		ghost:             make(map[string]int, len(in.rosters)),
		staleCopy:         make(map[string]int, len(in.rosters)),
		rowsWithoutRoster: make(map[string]int),
		rosterStale:       make(map[string]bool, len(in.rosters)),
	}

	// 1. The rosters, and above all which of them are still evidence of
	// anything. Everything below that compares a node against the registry
	// needs this answer first.
	rosterSets := make(map[string]map[string]struct{}, len(in.rosters))
	rosterFresh := make(map[string]bool, len(in.rosters))
	freshHolders := make(map[string]map[string]struct{})

	for _, roster := range in.rosters {
		age := in.now.Sub(roster.LastSeen)
		fresh := !roster.LastSeen.IsZero() && age <= reportTTL
		rosterFresh[roster.NodeID] = fresh
		result.rosterStale[roster.NodeID] = roster.LastSeen.IsZero() || age > rosterStaleMultiplier*reportTTL
		// Present with a zero so a node that is fine graphs as a flat line
		// rather than as a gap. rowsWithoutRoster is seeded here too, even
		// though its counts come from the table: a node that is reporting has
		// zero of them by construction, and seeding it means the series exists
		// before anything goes wrong rather than appearing out of nowhere.
		result.untracked[roster.NodeID] = 0
		result.ghost[roster.NodeID] = 0
		result.staleCopy[roster.NodeID] = 0
		result.rowsWithoutRoster[roster.NodeID] = 0

		set := make(map[string]struct{}, len(roster.SandboxIDs))
		for _, sandboxID := range roster.SandboxIDs {
			set[sandboxID] = struct{}{}
			if !fresh {
				continue
			}
			holders, ok := freshHolders[sandboxID]
			if !ok {
				holders = make(map[string]struct{}, 2)
				freshHolders[sandboxID] = holders
			}
			holders[roster.NodeID] = struct{}{}
		}
		rosterSets[roster.NodeID] = set
	}

	// 2. The registry rows, judged entirely against the database clock they
	// were read with.
	dbNow := in.listing.Now
	byID := make(map[string]pausedregistry.Sandbox, len(in.listing.Sandboxes))
	for _, sandbox := range in.listing.Sandboxes {
		byID[sandbox.SandboxID] = sandbox
		result.rowsByState[sandbox.State]++
		if sandbox.Invalid() {
			result.invalidRows++
		}
		if holder := resolve(sandbox.Holder()); holder != "" && !rosterFresh[holder] {
			result.rowsWithoutRoster[holder]++
		}

		switch sandbox.State {
		case pausedregistry.StatePublishing, pausedregistry.StateLocalOnly:
			if sandbox.SnapshotID == "" {
				// Nothing to claim, so no lease deadline can hurt it and none
				// can save it either.
				result.strandedRows++
				break
			}
			// COALESCE(lease_expires_at, updated_at), same as the node's
			// LEASE_EXPIRED: a row whose lease column was never written counts
			// as already expired, which is exactly the row worth warning about.
			if sandbox.LeaseDeadline().Before(dbNow.Add(leaseWarnWindow)) {
				result.parkedLeaseExpiring++
			}
		case pausedregistry.StateRunning, pausedregistry.StateResuming:
			if sandbox.LeaseExpired(dbNow) {
				result.liveLeaseLapsed++
				if sandbox.SandboxExpiresAt != nil && sandbox.SandboxExpiresAt.Before(dbNow) {
					result.reclaimableNow++
				}
			}
		}
	}

	// 3. Sandboxes two live nodes both claim. Settled before the per-node
	// counts below, because a sandbox the registry cannot attribute must not
	// also be counted as somebody's stale copy.
	unattributable := make(map[string]struct{})
	for sandboxID, holders := range freshHolders {
		if len(holders) < 2 {
			continue
		}
		if sandbox, ok := byID[sandboxID]; ok {
			if holder := resolve(sandbox.Holder()); holder != "" {
				if _, ok := holders[holder]; ok {
					// The registry names one of them, so the others are stale
					// copies rather than a conflict.
					continue
				}
			}
		}
		unattributable[sandboxID] = struct{}{}
		result.holderConflict++
	}

	// 4. What each node reports against what the registry says. Only fresh
	// rosters count: a node that has gone quiet still has its last report
	// sitting here, and reading it as current turns one silent machine into a
	// permanent stale_copy and untracked reading that no operator can clear.
	for _, roster := range in.rosters {
		if !rosterFresh[roster.NodeID] {
			continue
		}
		for _, sandboxID := range roster.SandboxIDs {
			sandbox, tracked := byID[sandboxID]
			if !tracked {
				result.untracked[roster.NodeID]++
				continue
			}
			if _, conflicted := unattributable[sandboxID]; conflicted {
				continue
			}
			if resolve(sandbox.Holder()) != roster.NodeID {
				result.staleCopy[roster.NodeID]++
			}
		}
	}

	// 5. Rows the registry says are live on a node that does not list them.
	ghostMinAge := ghostMinAgeMultiplier * reportTTL
	for _, sandbox := range in.listing.Sandboxes {
		// Only `running`. A `resuming` row is by definition in flight: the
		// claimer has not finished bringing the sandbox up, so it is not in
		// anybody's roster yet and its absence proves nothing.
		if sandbox.State != pausedregistry.StateRunning {
			continue
		}
		origin := resolve(sandbox.OriginNodeID)
		// Without a fresh roster from that node there is no evidence either
		// way, and guessing here is how a partitioned node's live sandboxes
		// get reported as gone.
		if !rosterFresh[origin] {
			continue
		}
		if _, ok := rosterSets[origin][sandbox.SandboxID]; ok {
			continue
		}
		if dbNow.Sub(sandbox.UpdatedAt) <= ghostMinAge {
			continue
		}
		result.ghost[origin]++
	}

	return result
}

// RunRegistryReconcile compares the paused registry against the heartbeat
// rosters on a timer and publishes the differences as metrics. It decides
// nothing and writes nothing.
func (s *Service) RunRegistryReconcile(ctx context.Context, interval time.Duration) {
	if s.registry == nil {
		return
	}
	if interval <= 0 {
		interval = defaultRegistryReconcileInterval
	}

	if s.reconcileRegistryOnce(ctx) {
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if s.reconcileRegistryOnce(ctx) {
				return
			}
		}
	}
}

// reconcileRegistryOnce runs one round and reports whether the loop should stop
// for good — which it should only when the registry is switched off or the
// context is done. A read failure is transient by assumption: it bumps a
// counter, leaves every gauge where it was, and the next tick tries again.
func (s *Service) reconcileRegistryOnce(ctx context.Context) bool {
	start := time.Now()
	listing, err := s.registry.List(ctx)
	if err != nil {
		if errors.Is(err, pausedregistry.ErrDisabled) {
			// Not a round at all — nothing was attempted, so nothing is timed.
			s.logger.Debug("scheduler registry reconciliation is disabled")
			return true
		}
		recordRegistryReconcileDuration(start)
		if ctx.Err() != nil {
			return true
		}
		recordRegistryReadFailure()
		// Deliberately not clearing the gauges: a database that went away for
		// one round would otherwise report every count as zero, which reads
		// exactly like a cluster that just became perfectly healthy.
		s.logger.Warn("scheduler registry read failed", zap.Error(err))
		return false
	}

	now := time.Now()
	result := computeRegistryReconcile(s.registryReconcileInput(listing, now))
	recordRegistryReconcile(result, now)
	// Timed at the end of the round, not after the read: the metric's name is
	// the round, and the derivation walks every row against every roster, which
	// is the half that grows with the cluster.
	recordRegistryReconcileDuration(start)

	s.logger.Debug("scheduler registry reconciled",
		zap.Int("rows", len(listing.Sandboxes)),
		zap.Int("invalid_rows", result.invalidRows),
		zap.Int("stranded_rows", result.strandedRows),
		zap.Int("holder_conflict", result.holderConflict),
		zap.Int("parked_lease_expiring", result.parkedLeaseExpiring),
		zap.Int("live_lease_lapsed", result.liveLeaseLapsed),
		zap.Int("reclaimable_now", result.reclaimableNow),
	)
	return false
}

// registryReconcileInput gathers everything one round compares, scoped to the
// cluster the reader itself is scoped to.
//
// The scope comes from the reader rather than from a second setting so the two
// halves of every comparison cannot drift apart: whatever the SQL filters on is
// what the rosters are filtered on.
func (s *Service) registryReconcileInput(listing pausedregistry.Listing, now time.Time) registryReconcileInput {
	return registryReconcileInput{
		listing:         listing,
		rosters:         s.nodes.RostersInCluster(s.registry.ClusterID()),
		resolveNodeID:   s.canonicalNodeID,
		now:             now,
		reportTTL:       s.reportTTL,
		leaseWarnWindow: s.registryLeaseWarnWindow,
	}
}

// canonicalNodeID resolves a node identity the same way every other lookup in
// this service does, so a row written under a node's previous name is still
// attributed to that node.
func (s *Service) canonicalNodeID(nodeID string) string {
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" {
		return ""
	}
	if node, ok := s.nodes.Resolve(nodeID); ok {
		return node.ID
	}
	return nodeID
}
