package scheduler

import (
	"context"
	"sort"
	"time"

	"go.uber.org/zap"
)

// The heartbeat-timeout sweep.
//
// ─────────────────────────────────────────────────────────────────────────────
// What it is for
// ─────────────────────────────────────────────────────────────────────────────
//
// A routing record is removed by exactly two things: an UnregisterNode (a
// graceful shutdown) and a heartbeat that arrives carrying an empty roster. A
// node that dies hard sends neither. While the projection was a thirty-second
// cache that cost nothing — the record expired on its own and the liveness-
// checked roster fallback covered the gap. Once the projection became
// authoritative the record's lifetime became the sandbox's, which on the
// shipped default is around twenty-four hours, and nothing on either read path
// cross-checks a stale binding: the scheduler's own LookupNode answers
// bound_binding from step 1 and returns, and the gateway's direct read does the
// same. A record naming a machine that is never coming back is therefore a
// route to a 404 for as long as the record lives.
//
// This sweep is the missing third remover. It is not a liveness check bolted
// onto the read path — that would put a node lookup on the hot path of every
// proxied request, which is precisely what step 1 of lookupNode exists to avoid.
// It is a background pass that decides, once, that a node has gone, and retires
// what that node's own last heartbeat said it held.
//
// ─────────────────────────────────────────────────────────────────────────────
// 🔴 Why it retires by execution and not by sandbox id
// ─────────────────────────────────────────────────────────────────────────────
//
// The obvious implementation is "delete every sandbox id this node reported".
// It is also the bug e2b shipped: their reconciliation decides orphanhood on
// sandbox-id existence alone
// (packages/api/internal/sandbox/storage/redis/main.go:205-217), so a sandbox
// rebuilt under the same id on another node is found by a returning stale
// incarnation, is not judged an orphan, and becomes something nothing can kill.
//
// The same shape here would be worse, because the id survives a resume: a
// sandbox whose node died is very often resumed elsewhere within seconds, under
// a new incarnation but the same id. A sweep that deleted by id would then
// delete the *live* record — and the sandbox that was successfully rescued
// would be the one that stopped being routable.
//
// So the sweep retires the pair the dead node itself reported: the sandbox id
// *and* the incarnation it named. That pair goes through BindingStore.Delete,
// which compares the incarnation the caller names against the one the record
// holds and refuses when they differ. Nothing new is invented for this — it is
// the same guarded delete a pause event drives, with the same four outcomes and
// the same words for them, in both stores.
//
// A roster entry that names no incarnation is skipped rather than deleted
// unguarded, for the reason written on sandboxEventIgnoredUnknownExecution: a
// node too old to report an incarnation is also too old to send a projection
// budget, so its records carry binding_ttl and expire on their own in thirty
// seconds. There is nothing to strand and nothing worth an unguarded delete for.
//
// ─────────────────────────────────────────────────────────────────────────────
// 🔴 What it deliberately does not do
// ─────────────────────────────────────────────────────────────────────────────
//
// It never touches the node registry. The roster stays exactly as the node left
// it, so the reconciliation gauges keep reporting what that machine last said
// and an operator still has the evidence. The sweep's own bookkeeping is a
// per-node "the report I already acted on", which is why a swept node costs one
// map lookup per round rather than a delete storm every thirty seconds.
//
// ─────────────────────────────────────────────────────────────────────────────
// 🔴 Why it keeps its own copy of every roster
// ─────────────────────────────────────────────────────────────────────────────
//
// The registry's observed record is not durable enough to sweep from, and the
// reason is the deployment this exists for. Kubernetes discovery drops an
// endpoint the moment its Serving condition goes false, which for a machine
// that died hard is roughly one node-monitor grace period — tens of seconds.
// AtomicNodeRegistry.Set then deletes that node's observed record and clears
// its roster. A sweep that read only the registry would therefore find nothing
// to retire in precisely the case it was built for: the node would vanish
// before any threshold worth having had elapsed, and its records would sit
// there for their full budget exactly as before.
//
// So the sweeper shadows what it has seen. Each round it copies the current
// rosters into its own map and then judges every shadow it holds, whether or
// not the registry still has the node. A node dropped from discovery cannot
// heartbeat either — Heartbeat refuses an unknown node — so its shadow's
// timestamp is frozen and the threshold elapses against a number that can no
// longer move.
//
// 🔴 With one exception, and it is not hypothetical. During a fleet upgrade
// discovery renames a node from its pod name to the machine's, which drops the
// old identity and adds a new one for the same running machine — the case
// AtomicNodeRegistry.aliasToID exists for. The shadow under the old name would
// then look exactly like a dead node, and sweeping it would retire the records
// of every sandbox on a perfectly healthy host, under the very incarnations it
// is still reporting, so the execution guard would not catch it. Resolving the
// shadow's identity separates the two: a name discovery can still map onto a
// node it knows is a rename, and a name it cannot map at all is a machine that
// is gone.
//
// It cannot resurrect the roster fallback problem either, and that ordering is
// the reason the threshold has to sit far above scheduler.report_ttl. The
// fallback in lookupNode step 2 only answers for a node whose last heartbeat is
// within report_ttl; a node silent for the sweep's threshold has long since
// failed that test, so retiring its record does not hand the lookup back to a
// roster that would name the same dead machine.

const (
	// defaultBindingSweepSilence and defaultBindingSweepInterval mirror the
	// config package's defaults, and are what an unconfigured sweeper uses.
	// The argument for the five minutes is written where the config default is.
	defaultBindingSweepSilence  = 5 * time.Minute
	defaultBindingSweepInterval = 30 * time.Second
)

// bindingSweeper holds one sweep's state. It is built once per process and
// driven by a single goroutine, so the swept map needs no lock of its own —
// and it is a plain struct rather than a method set on Service so a test can
// drive one round against a stub registry and either store.
type bindingSweeper struct {
	logger    *zap.Logger
	nodes     NodeRegistry
	store     BindingStore
	clusterID string
	silence   time.Duration
	// lastKnown is this sweeper's own copy of the last roster it saw for each
	// node, including nodes the registry has since forgotten. See the note
	// above for why reading the registry alone is not enough.
	lastKnown map[string]Roster
	// swept records, per node, the heartbeat timestamp whose roster this sweep
	// has already retired.
	//
	// 🔴 Keyed on the timestamp and not on a bare "done" flag. A node that
	// comes back reports a new timestamp, which makes it sweepable again
	// without anything having to remember to clear this; and a node that stays
	// dead keeps reporting the same one, so its records are retired once rather
	// than being re-deleted — and re-counted — on every tick forever.
	swept map[string]time.Time
}

func newBindingSweeper(logger *zap.Logger, nodes NodeRegistry, store BindingStore, clusterID string, silence time.Duration) *bindingSweeper {
	if logger == nil {
		logger = zap.NewNop()
	}
	if silence <= 0 {
		silence = defaultBindingSweepSilence
	}
	return &bindingSweeper{
		logger:    logger,
		nodes:     nodes,
		store:     store,
		clusterID: clusterID,
		silence:   silence,
		lastKnown: make(map[string]Roster),
		swept:     make(map[string]time.Time),
	}
}

// RunBindingSweep retires the routing records of nodes that have stopped
// heartbeating, until ctx is done.
//
// 🔴 It returns immediately when the switch is off, rather than being started
// conditionally by the caller. One thing decides whether the sweep runs, it is
// the setting, and it is checkable from a test that builds the Service the way
// the process does.
func (s *Service) RunBindingSweep(ctx context.Context, interval time.Duration) {
	if !s.bindingSweep {
		return
	}
	if interval <= 0 {
		interval = defaultBindingSweepInterval
	}

	sweeper := newBindingSweeper(s.logger, s.nodes, s.store, s.registry.ClusterID(), s.bindingSweepSilence)
	s.logger.Info("scheduler binding sweep started",
		zap.Duration("silence_threshold", sweeper.silence),
		zap.Duration("interval", interval),
	)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			sweeper.sweepOnce(now)
		}
	}
}

// sweepOnce is one pass.
//
// 🔴 There is no pass on start-up, unlike the registry reconciliation's. A
// freshly started scheduler has observed nothing, so every node looks like a
// node that has never reported — and this pass can only ever retire what it has
// itself watched go quiet. That is a deliberate property and not an accident of
// ordering: a scheduler must not be able to conclude, from its own ignorance,
// that a fleet it has never spoken to is dead.
func (b *bindingSweeper) sweepOnce(now time.Time) {
	listed := make(map[string]struct{})
	for _, roster := range b.nodes.RostersInCluster(b.clusterID) {
		listed[roster.NodeID] = struct{}{}
		if roster.LastSeen.IsZero() {
			// Known to discovery, never heard from. There is nothing this
			// scheduler installed on its behalf, so there is nothing to retire
			// — and shadowing it would let a cluster that has just added a
			// machine trip the whole-fleet guard below.
			continue
		}
		b.lastKnown[roster.NodeID] = roster
	}

	// candidates is every node this round could still act on: a shadow whose
	// current report has not already been retired.
	//
	// 🔴 Already-swept nodes are excluded from the count, not merely skipped
	// later, and the difference is what a cluster that dies one machine at a
	// time gets. Counting them would leave the first death propping up the
	// denominator of the whole-fleet guard below, so the second death — with
	// the first node still listed by discovery — would read as "everything is
	// silent" and be suppressed for good. Excluding them keeps the guard's
	// meaning exactly as stated (every node still capable of being acted on has
	// gone quiet at once, which is a partition) while letting staggered deaths
	// each be handled on their own.
	candidates := 0
	silent := make([]Roster, 0, len(b.lastKnown))
	for nodeID, roster := range b.lastKnown {
		if _, stillListed := listed[nodeID]; !stillListed && b.renamed(nodeID) {
			// The machine is fine; only the name discovery uses for it
			// changed. Sweeping this shadow would retire the records of every
			// sandbox on a healthy host — under the incarnations it is still
			// reporting, so nothing downstream would refuse it.
			b.forget(nodeID)
			continue
		}
		if last, done := b.swept[nodeID]; done && last.Equal(roster.LastSeen) {
			continue
		}
		candidates++
		if now.Sub(roster.LastSeen) > b.silence {
			silent = append(silent, roster)
		}
	}
	sort.Slice(silent, func(i, j int) bool { return silent[i].NodeID < silent[j].NodeID })

	defer b.prune(listed)

	if len(silent) == 0 {
		return
	}

	// 🔴 The whole-fleet guard. Every node that has ever reported has gone
	// quiet at the same moment, which is a far better description of this
	// scheduler losing its own network than of every machine in the cluster
	// dying at once. Retiring on that reading would empty the routing table and
	// 404 every sandbox in the fleet, for a fault that fixes itself.
	//
	// 🔴 The `candidates > 1` half is not a rounding-off. With a single node,
	// "all of them are silent" and "the one node died" are the same sentence,
	// and suppressing there would mean the sweep never fires on the smallest
	// deployment — which is the one a developer tests it on.
	if candidates > 1 && len(silent) == candidates {
		for range silent {
			recordBindingSweepNode(bindingSweepNodeSuppressedAllSilent)
		}
		b.logger.Warn("scheduler binding sweep suppressed: every node that has reported has gone silent at once, which is more likely this scheduler's own network than the whole fleet",
			zap.Int("silent_nodes", len(silent)),
			zap.Duration("silence_threshold", b.silence),
		)
		return
	}

	for _, roster := range silent {
		if b.retire(roster, now) {
			b.swept[roster.NodeID] = roster.LastSeen
			recordBindingSweepNode(bindingSweepNodeSwept)
		}
	}
}

// renamed says whether a node id this sweeper still holds a shadow for is an
// identity discovery has remapped onto a node it knows, rather than a machine
// that has gone.
//
// 🔴 Resolve is asked, not nodesByID, because Resolve is what follows the
// alias. An id the registry can still map onto a *different* live node is the
// rename; an id it cannot map at all is a node that left.
func (b *bindingSweeper) renamed(nodeID string) bool {
	node, ok := b.nodes.Resolve(nodeID)
	return ok && node.ID != nodeID
}

// prune drops the bookkeeping for a node that is both gone from discovery and
// already retired.
//
// 🔴 Both conditions. Dropping on absence alone would forget the shadow of the
// node the sweep exists to act on, since that node leaves discovery long before
// the threshold elapses; dropping on "swept" alone would forget a node that is
// still in the cluster and may report again.
func (b *bindingSweeper) prune(listed map[string]struct{}) {
	for nodeID, roster := range b.lastKnown {
		if _, stillListed := listed[nodeID]; stillListed {
			continue
		}
		if last, done := b.swept[nodeID]; done && last.Equal(roster.LastSeen) {
			b.forget(nodeID)
		}
	}
}

func (b *bindingSweeper) forget(nodeID string) {
	delete(b.lastKnown, nodeID)
	delete(b.swept, nodeID)
}

// retire puts one silent node's roster through the guarded delete, and says
// whether the pass completed.
//
// 🔴 A store failure returns false so the node is *not* marked as swept and the
// next tick tries again. Marking it regardless would make one unreachable Redis
// the reason a dead node's records are never retired — a permanent consequence
// for a transient fault, and one nothing else would ever repair, because the
// only other remover is a heartbeat this node will never send.
func (b *bindingSweeper) retire(roster Roster, now time.Time) bool {
	complete := true
	retired := 0
	refused := 0
	for _, entry := range roster.Entries {
		if entry.ExecutionID == "" {
			// See the note at the top: unguarded is not an option, and a node
			// too old to name an incarnation is also too old to have asked for
			// a long TTL, so its record expires on its own.
			recordBindingSweep(sandboxEventIgnoredUnknownExecution)
			continue
		}
		outcome, err := b.store.Delete(entry.SandboxID, entry.ExecutionID, now)
		if err != nil {
			complete = false
			recordBindingSweep(sandboxEventStoreError)
			b.logger.Warn("scheduler binding sweep delete failed",
				zap.String("node_id", roster.NodeID),
				zap.String("sandbox_id", entry.SandboxID),
				zap.Error(err),
			)
			continue
		}
		recordBindingSweep(string(outcome))
		switch outcome {
		case BindingDeleteDeleted, BindingDeleteUnknownIncumbent:
			retired++
		case BindingDeleteRejectedStale:
			refused++
			// 🔴 Worth a line at info rather than debug. This is the guard
			// refusing to retire a record naming a different incarnation, which
			// on this path means the sandbox was rebuilt elsewhere while its
			// node was dying — a successful rescue, and the exact case an
			// id-scoped sweep would have broken.
			b.logger.Info("scheduler binding sweep refused to retire a record naming a live incarnation",
				zap.String("node_id", roster.NodeID),
				zap.String("sandbox_id", entry.SandboxID),
				zap.String("reported_execution_id", entry.ExecutionID),
			)
		}
	}

	b.logger.Warn("scheduler binding sweep retired the records of a node that stopped heartbeating",
		zap.String("node_id", roster.NodeID),
		zap.Time("last_seen", roster.LastSeen),
		zap.Duration("silent_for", now.Sub(roster.LastSeen)),
		zap.Int("roster_size", len(roster.Entries)),
		zap.Int("retired", retired),
		zap.Int("refused_live_incarnation", refused),
		zap.Bool("complete", complete),
	)
	return complete
}
