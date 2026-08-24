package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	pausedregistry "agentenv/services/scheduler/internal/registry"

	"go.uber.org/zap"
)

const testReportTTL = 30 * time.Second

func at(base time.Time, offset time.Duration) *time.Time {
	t := base.Add(offset)
	return &t
}

// reconcileFixture builds one round's input against a single fixed clock, so a
// test only has to say how far a row or a roster is from "now".
type reconcileFixture struct {
	dbNow     time.Time
	localNow  time.Time
	sandboxes []pausedregistry.Sandbox
	rosters   []Roster
	// resolveNodeID stands in for the node registry's identity resolution.
	// Nil, as in most tests, means identities are already canonical.
	resolveNodeID func(string) string
}

func newReconcileFixture() *reconcileFixture {
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	return &reconcileFixture{dbNow: now, localNow: now}
}

func (f *reconcileFixture) run(leaseWarnWindow time.Duration) registryReconcileResult {
	return computeRegistryReconcile(registryReconcileInput{
		listing:         pausedregistry.Listing{Sandboxes: f.sandboxes, Now: f.dbNow},
		rosters:         f.rosters,
		resolveNodeID:   f.resolveNodeID,
		now:             f.localNow,
		reportTTL:       testReportTTL,
		leaseWarnWindow: leaseWarnWindow,
	})
}

func (f *reconcileFixture) roster(nodeID string, age time.Duration, sandboxIDs ...string) {
	entries := make([]RosterEntry, 0, len(sandboxIDs))
	for _, sandboxID := range sandboxIDs {
		entries = append(entries, RosterEntry{SandboxID: sandboxID})
	}
	f.rosters = append(f.rosters, Roster{
		NodeID:   nodeID,
		Entries:  entries,
		LastSeen: f.localNow.Add(-age),
	})
}

// rosterWithExecutions is the same, with the incarnations the node reported.
func (f *reconcileFixture) rosterWithExecutions(nodeID string, age time.Duration, entries ...RosterEntry) {
	f.rosters = append(f.rosters, Roster{
		NodeID:   nodeID,
		Entries:  entries,
		LastSeen: f.localNow.Add(-age),
	})
}

func TestReconcileCountsRowsByState(t *testing.T) {
	f := newReconcileFixture()
	f.sandboxes = []pausedregistry.Sandbox{
		{SandboxID: "s1", State: pausedregistry.StatePaused, OriginNodeID: "node-a", SnapshotID: "snap", LeaseExpiresAt: at(f.dbNow, time.Hour)},
		{SandboxID: "s2", State: pausedregistry.StatePaused, OriginNodeID: "node-a", SnapshotID: "snap", LeaseExpiresAt: at(f.dbNow, time.Hour)},
		{SandboxID: "s3", State: pausedregistry.StateRunning, OriginNodeID: "node-a", LeaseExpiresAt: at(f.dbNow, time.Hour)},
	}
	f.roster("node-a", 0, "s3")

	result := f.run(testReportTTL)

	if got := result.rowsByState[pausedregistry.StatePaused]; got != 2 {
		t.Fatalf("expected 2 paused rows, got %d", got)
	}
	if got := result.rowsByState[pausedregistry.StateRunning]; got != 1 {
		t.Fatalf("expected 1 running row, got %d", got)
	}
	// Every known state is present so an unused one graphs as zero rather than
	// as a missing series.
	for _, state := range []pausedregistry.State{
		pausedregistry.StatePublishing,
		pausedregistry.StateResuming,
		pausedregistry.StateLocalOnly,
	} {
		if _, ok := result.rowsByState[state]; !ok {
			t.Fatalf("expected state %q to be reported as zero", state)
		}
	}
}

// A sandbox created on a node and never paused has no row at all, so untracked
// is normally non-zero. The test exists to pin that expectation, not to demand
// zero.
func TestReconcileUntrackedIsRosterMinusRegistry(t *testing.T) {
	f := newReconcileFixture()
	f.sandboxes = []pausedregistry.Sandbox{
		{SandboxID: "s1", State: pausedregistry.StateRunning, OriginNodeID: "node-a", LeaseExpiresAt: at(f.dbNow, time.Hour)},
	}
	f.roster("node-a", 0, "s1", "never-paused-1", "never-paused-2")
	f.roster("node-b", 0)

	result := f.run(testReportTTL)

	if got := result.untracked["node-a"]; got != 2 {
		t.Fatalf("expected 2 untracked on node-a, got %d", got)
	}
	if got, ok := result.untracked["node-b"]; !ok || got != 0 {
		t.Fatalf("expected node-b to report zero untracked, got %d (present=%v)", got, ok)
	}
}

func TestReconcileGhostNeedsAFreshRosterAndAnOldRow(t *testing.T) {
	cases := []struct {
		name       string
		state      pausedregistry.State
		rowAge     time.Duration
		rosterAge  time.Duration
		wantGhosts int
	}{
		{
			name:       "old running row absent from a fresh roster",
			state:      pausedregistry.StateRunning,
			rowAge:     10 * time.Minute,
			rosterAge:  time.Second,
			wantGhosts: 1,
		},
		{
			// The node has not reported recently enough for its roster to be
			// evidence of anything; concluding from it is how a partitioned
			// node's live sandboxes get reported as gone.
			name:       "stale roster proves nothing",
			state:      pausedregistry.StateRunning,
			rowAge:     10 * time.Minute,
			rosterAge:  5 * time.Minute,
			wantGhosts: 0,
		},
		{
			// Written within the last couple of report intervals: the node
			// simply has not reported since.
			name:       "young row is not a ghost yet",
			state:      pausedregistry.StateRunning,
			rowAge:     10 * time.Second,
			rosterAge:  time.Second,
			wantGhosts: 0,
		},
		{
			// A resume in flight is not yet in the claimer's store.
			name:       "resuming is never a ghost",
			state:      pausedregistry.StateResuming,
			rowAge:     10 * time.Minute,
			rosterAge:  time.Second,
			wantGhosts: 0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newReconcileFixture()
			f.sandboxes = []pausedregistry.Sandbox{{
				SandboxID:      "s1",
				State:          tc.state,
				OriginNodeID:   "node-a",
				UpdatedAt:      f.dbNow.Add(-tc.rowAge),
				LeaseExpiresAt: at(f.dbNow, time.Hour),
			}}
			f.roster("node-a", tc.rosterAge)

			if got := f.run(testReportTTL).ghost["node-a"]; got != tc.wantGhosts {
				t.Fatalf("expected %d ghosts, got %d", tc.wantGhosts, got)
			}
		})
	}
}

// The origin keeps its paused record until its own reconciliation drops it, so
// both nodes report the sandbox. The registry names one of them, which makes
// the other a stale copy rather than a conflict.
func TestReconcileTakeoverIsStaleCopyNotConflict(t *testing.T) {
	f := newReconcileFixture()
	f.sandboxes = []pausedregistry.Sandbox{{
		SandboxID:       "s1",
		State:           pausedregistry.StateResuming,
		OriginNodeID:    "node-a",
		ClaimedByNodeID: "node-b",
		SnapshotID:      "snap",
		UpdatedAt:       f.dbNow.Add(-time.Second),
		LeaseExpiresAt:  at(f.dbNow, time.Hour),
	}}
	f.roster("node-a", 0, "s1")
	f.roster("node-b", 0, "s1")

	result := f.run(testReportTTL)

	if result.holderConflict != 0 {
		t.Fatalf("expected an attributable takeover not to count as a conflict, got %d", result.holderConflict)
	}
	// node-a is the losing side because the row is held by its claimer.
	if got := result.staleCopy["node-a"]; got != 1 {
		t.Fatalf("expected 1 stale copy on node-a, got %d", got)
	}
	if got := result.staleCopy["node-b"]; got != 0 {
		t.Fatalf("expected no stale copy on node-b, got %d", got)
	}
}

func TestReconcileHolderConflictNeedsTwoFreshRostersTheRegistryCannotSettle(t *testing.T) {
	t.Run("registry names neither reporter", func(t *testing.T) {
		f := newReconcileFixture()
		f.sandboxes = []pausedregistry.Sandbox{{
			SandboxID:      "s1",
			State:          pausedregistry.StateRunning,
			OriginNodeID:   "node-c",
			UpdatedAt:      f.dbNow.Add(-time.Second),
			LeaseExpiresAt: at(f.dbNow, time.Hour),
		}}
		f.roster("node-a", 0, "s1")
		f.roster("node-b", 0, "s1")

		result := f.run(testReportTTL)

		if got := result.holderConflict; got != 1 {
			t.Fatalf("expected 1 holder conflict, got %d", got)
		}
		// 🔴 And counted once. The registry names node-c, so from each
		// reporter's own point of view it is holding somebody else's sandbox —
		// which would make the same fact a conflict *and* two stale copies,
		// three readings of one problem. A sandbox nobody can be shown to hold
		// is a conflict and nothing else.
		for _, nodeID := range []string{"node-a", "node-b"} {
			if got := result.staleCopy[nodeID]; got != 0 {
				t.Fatalf("expected an unattributable sandbox not to be counted as a stale copy on %s, got %d", nodeID, got)
			}
		}
	})

	t.Run("registry has no row at all", func(t *testing.T) {
		f := newReconcileFixture()
		f.roster("node-a", 0, "s1")
		f.roster("node-b", 0, "s1")

		result := f.run(testReportTTL)

		if got := result.holderConflict; got != 1 {
			t.Fatalf("expected 1 holder conflict, got %d", got)
		}
		// Untracked still counts it: "the registry has no row for this" is a
		// different statement from "two nodes disagree about who holds it", and
		// both are true here.
		for _, nodeID := range []string{"node-a", "node-b"} {
			if got := result.untracked[nodeID]; got != 1 {
				t.Fatalf("expected %s to report the sandbox as untracked, got %d", nodeID, got)
			}
			if got := result.staleCopy[nodeID]; got != 0 {
				t.Fatalf("expected no stale copy on %s, got %d", nodeID, got)
			}
		}
	})

	t.Run("one of the two rosters is stale", func(t *testing.T) {
		f := newReconcileFixture()
		f.roster("node-a", 0, "s1")
		f.roster("node-b", 5*time.Minute, "s1")

		if got := f.run(testReportTTL).holderConflict; got != 0 {
			t.Fatalf("expected a stale roster not to make a conflict, got %d", got)
		}
	})
}

func TestReconcileSplitsParkedFromLiveLeases(t *testing.T) {
	f := newReconcileFixture()
	f.sandboxes = []pausedregistry.Sandbox{
		// Parked and about to lapse: the alarming one. Once the lease goes,
		// another node may claim it and rebuild from the previous snapshot.
		{SandboxID: "s1", State: pausedregistry.StatePublishing, OriginNodeID: "node-a", SnapshotID: "snap", UpdatedAt: f.dbNow, LeaseExpiresAt: at(f.dbNow, 10*time.Second)},
		// Parked with a NULL lease, which the nodes read as already expired.
		{SandboxID: "s2", State: pausedregistry.StateLocalOnly, OriginNodeID: "node-a", SnapshotID: "snap", UpdatedAt: f.dbNow.Add(-time.Hour)},
		// Parked with plenty of lease left.
		{SandboxID: "s3", State: pausedregistry.StateLocalOnly, OriginNodeID: "node-a", SnapshotID: "snap", UpdatedAt: f.dbNow, LeaseExpiresAt: at(f.dbNow, time.Hour)},
		// Paused rows never enter a lease count: their claimability does not
		// depend on one.
		{SandboxID: "s4", State: pausedregistry.StatePaused, OriginNodeID: "node-a", SnapshotID: "snap", UpdatedAt: f.dbNow, LeaseExpiresAt: at(f.dbNow, -time.Hour)},
		// Live and lapsed, but with no deadline: informational only.
		{SandboxID: "s5", State: pausedregistry.StateRunning, OriginNodeID: "node-a", UpdatedAt: f.dbNow, LeaseExpiresAt: at(f.dbNow, -time.Minute)},
		// Live, lapsed, and past its own deadline: the next reclaim acts on it.
		{SandboxID: "s6", State: pausedregistry.StateRunning, OriginNodeID: "node-a", UpdatedAt: f.dbNow, LeaseExpiresAt: at(f.dbNow, -time.Minute), SandboxExpiresAt: at(f.dbNow, -time.Second)},
		// Live, lapsed, deadline still ahead: not reclaimable.
		{SandboxID: "s7", State: pausedregistry.StateResuming, OriginNodeID: "node-a", ClaimedByNodeID: "node-a", SnapshotID: "snap", UpdatedAt: f.dbNow, LeaseExpiresAt: at(f.dbNow, -time.Minute), SandboxExpiresAt: at(f.dbNow, time.Hour)},
	}

	result := f.run(30 * time.Second)

	if result.parkedLeaseExpiring != 2 {
		t.Fatalf("expected 2 parked leases expiring, got %d", result.parkedLeaseExpiring)
	}
	if result.liveLeaseLapsed != 3 {
		t.Fatalf("expected 3 lapsed live leases, got %d", result.liveLeaseLapsed)
	}
	if result.reclaimableNow != 1 {
		t.Fatalf("expected 1 reclaimable row, got %d", result.reclaimableNow)
	}
	if result.strandedRows != 0 {
		t.Fatalf("expected no stranded rows when every parked row has a snapshot, got %d", result.strandedRows)
	}
}

// 🔴 A parked row with no snapshot is not a row somebody is about to take
// away: claim_for_resume requires a snapshot, and neither reclaim path touches
// a parked row at all. Counting it as an expiring lease produces an alert that
// is permanently true and never actionable — which is exactly what the dev
// cluster's single local_only row does today.
func TestReconcileSeparatesStrandedParkedRowsFromClaimableOnes(t *testing.T) {
	f := newReconcileFixture()
	f.sandboxes = []pausedregistry.Sandbox{
		// The first pause never finished uploading, so there is no snapshot and
		// the lease column was never written. Nobody can claim it, nobody can
		// reclaim it, and nobody can delete it.
		{SandboxID: "s1", State: pausedregistry.StateLocalOnly, OriginNodeID: "node-a", UpdatedAt: f.dbNow.Add(-time.Hour)},
		// An upload in flight: also snapshotless, and also nobody else's to
		// take. Every pause passes through this state.
		{SandboxID: "s2", State: pausedregistry.StatePublishing, OriginNodeID: "node-a", UpdatedAt: f.dbNow, LeaseExpiresAt: at(f.dbNow, 90*time.Second)},
		// A later pause failed after an earlier one succeeded: this one *is*
		// claimable, and claiming it rewinds the sandbox to the older snapshot.
		{SandboxID: "s3", State: pausedregistry.StateLocalOnly, OriginNodeID: "node-a", SnapshotID: "old-snap", UpdatedAt: f.dbNow.Add(-time.Hour)},
	}

	result := f.run(30 * time.Second)

	if result.strandedRows != 2 {
		t.Fatalf("expected 2 stranded rows, got %d", result.strandedRows)
	}
	if result.parkedLeaseExpiring != 1 {
		t.Fatalf("expected only the claimable row to count as an expiring lease, got %d", result.parkedLeaseExpiring)
	}
	// Neither is a paused row, so neither is an invalid one: the invalid count
	// means something else entirely (a row the nodes cannot even decode).
	if result.invalidRows != 0 {
		t.Fatalf("expected no invalid rows, got %d", result.invalidRows)
	}
}

// hasParkedLeaseRenewal reports whether the candidate list names exactly this
// (sandbox, node) pair, so a test does not have to depend on the slice's
// iteration order.
func hasParkedLeaseRenewal(candidates []pausedregistry.ParkedLeaseHolder, sandboxID, nodeID string) bool {
	for _, c := range candidates {
		if c.SandboxID == sandboxID && c.NodeID == nodeID {
			return true
		}
	}
	return false
}

// TestReconcileParkedLeaseRenewalCandidatesNeedTheHoldersOwnFreshRoster pins
// the whole eligibility rule computeRegistryReconcile derives — see
// registryReconcileResult.parkedLeaseRenewals — with one round carrying a
// positive case for every way to be excluded, so each exclusion has to
// actually be checked rather than merely coinciding with an empty round.
func TestReconcileParkedLeaseRenewalCandidatesNeedTheHoldersOwnFreshRoster(t *testing.T) {
	f := newReconcileFixture()
	f.sandboxes = []pausedregistry.Sandbox{
		// Eligible: publishing, snapshot published, holder's roster fresh and
		// lists it.
		{SandboxID: "s1", State: pausedregistry.StatePublishing, OriginNodeID: "node-a", SnapshotID: "snap", LeaseExpiresAt: at(f.dbNow, time.Hour)},
		// Eligible: same, local_only.
		{SandboxID: "s2", State: pausedregistry.StateLocalOnly, OriginNodeID: "node-a", SnapshotID: "snap", LeaseExpiresAt: at(f.dbNow, time.Hour)},
		// Excluded: node-a's roster is fresh but does not list s3 — the node
		// itself is not vouching for this row.
		{SandboxID: "s3", State: pausedregistry.StatePublishing, OriginNodeID: "node-a", SnapshotID: "snap", LeaseExpiresAt: at(f.dbNow, time.Hour)},
		// Excluded: node-b's roster does list s4, but it is stale — a silent
		// node's last report is not evidence of anything current.
		{SandboxID: "s4", State: pausedregistry.StatePublishing, OriginNodeID: "node-b", SnapshotID: "snap", LeaseExpiresAt: at(f.dbNow, time.Hour)},
		// Excluded: stranded (no snapshot) — nobody can claim it, so there is
		// nothing a renewal protects.
		{SandboxID: "s5", State: pausedregistry.StatePublishing, OriginNodeID: "node-a", LeaseExpiresAt: at(f.dbNow, time.Hour)},
		// Excluded: running — this mechanism is scoped to the two parked
		// states, even though node-a's roster is fresh and lists it too.
		{SandboxID: "s6", State: pausedregistry.StateRunning, OriginNodeID: "node-a", LeaseExpiresAt: at(f.dbNow, time.Hour)},
		// Excluded: paused — nobody holds it, so it never reaches the branch
		// that builds a candidate at all.
		{SandboxID: "s7", State: pausedregistry.StatePaused, OriginNodeID: "node-a", SnapshotID: "snap", LeaseExpiresAt: at(f.dbNow, time.Hour)},
	}
	// node-a: fresh, and vouches for everything except s3.
	f.roster("node-a", 0, "s1", "s2", "s5", "s6", "s7")
	// node-b: vouches for s4, but its last report is well past reportTTL.
	f.roster("node-b", 5*time.Minute, "s4")

	result := f.run(testReportTTL)

	if got := len(result.parkedLeaseRenewals); got != 2 {
		t.Fatalf("expected exactly 2 renewal candidates, got %d: %+v", got, result.parkedLeaseRenewals)
	}
	if !hasParkedLeaseRenewal(result.parkedLeaseRenewals, "s1", "node-a") {
		t.Fatalf("expected s1 to be a candidate: %+v", result.parkedLeaseRenewals)
	}
	if !hasParkedLeaseRenewal(result.parkedLeaseRenewals, "s2", "node-a") {
		t.Fatalf("expected s2 to be a candidate: %+v", result.parkedLeaseRenewals)
	}
	for _, excluded := range []string{"s3", "s4", "s5", "s6", "s7"} {
		for _, c := range result.parkedLeaseRenewals {
			if c.SandboxID == excluded {
				t.Fatalf("%s must not be a renewal candidate: %+v", excluded, result.parkedLeaseRenewals)
			}
		}
	}
}

// TestReconcileParkedLeaseRenewalUsesTheRowsRawHolderNotTheResolvedOne is the
// correctness detail the write path depends on: RenewParkedLeases' statement
// matches a row's origin_node_id column literally, and that column can still
// hold a node's previous identity mid-upgrade even though every roster below
// is keyed by the current one. Sending the resolved identity here would build
// a candidate that can never match the row it was derived from.
func TestReconcileParkedLeaseRenewalUsesTheRowsRawHolderNotTheResolvedOne(t *testing.T) {
	f := newReconcileFixture()
	f.sandboxes = []pausedregistry.Sandbox{
		{SandboxID: "s1", State: pausedregistry.StatePublishing, OriginNodeID: "pod-old", SnapshotID: "snap", LeaseExpiresAt: at(f.dbNow, time.Hour)},
	}
	// The roster is reported under the canonical identity, and freshness has
	// to be judged against that — resolveNodeID is what bridges the two.
	f.roster("node-a", 0, "s1")
	f.resolveNodeID = func(id string) string {
		if id == "pod-old" {
			return "node-a"
		}
		return id
	}

	result := f.run(testReportTTL)

	if got := len(result.parkedLeaseRenewals); got != 1 {
		t.Fatalf("expected exactly 1 renewal candidate, got %d: %+v", got, result.parkedLeaseRenewals)
	}
	if result.parkedLeaseRenewals[0].NodeID != "pod-old" {
		t.Fatalf("expected the raw, unresolved holder %q, got %q", "pod-old", result.parkedLeaseRenewals[0].NodeID)
	}
	// Control: without the resolver, the row's own identity already equals
	// the roster's, and the same candidate must still be produced — proving
	// the assertion above is about which identity is *sent*, not an artefact
	// of the resolver being absent.
	f.resolveNodeID = nil
	f.sandboxes[0].OriginNodeID = "node-a"
	identity := f.run(testReportTTL)
	if got := len(identity.parkedLeaseRenewals); got != 1 || identity.parkedLeaseRenewals[0].NodeID != "node-a" {
		t.Fatalf("expected the identity-resolver control to also produce one candidate under node-a, got %+v", identity.parkedLeaseRenewals)
	}
}

func TestReconcileCountsInvalidRows(t *testing.T) {
	f := newReconcileFixture()
	f.sandboxes = []pausedregistry.Sandbox{
		{SandboxID: "s1", State: pausedregistry.StatePaused, OriginNodeID: "node-a", UpdatedAt: f.dbNow, LeaseExpiresAt: at(f.dbNow, time.Hour)},
		{SandboxID: "s2", State: pausedregistry.StatePaused, OriginNodeID: "node-a", SnapshotID: "snap", UpdatedAt: f.dbNow, LeaseExpiresAt: at(f.dbNow, time.Hour)},
		{SandboxID: "s3", State: pausedregistry.StatePublishing, OriginNodeID: "node-a", UpdatedAt: f.dbNow, LeaseExpiresAt: at(f.dbNow, time.Hour)},
	}

	if got := f.run(testReportTTL).invalidRows; got != 1 {
		t.Fatalf("expected 1 invalid row, got %d", got)
	}
}

func TestReconcileMarksStaleRosters(t *testing.T) {
	f := newReconcileFixture()
	f.roster("node-fresh", 5*time.Second)
	f.roster("node-borderline", 2*testReportTTL)
	f.roster("node-stale", 4*testReportTTL)

	result := f.run(testReportTTL)

	if result.rosterStale["node-fresh"] {
		t.Fatal("expected a fresh roster not to be stale")
	}
	if result.rosterStale["node-borderline"] {
		t.Fatal("expected two missed reports to be tolerated")
	}
	if !result.rosterStale["node-stale"] {
		t.Fatal("expected a long-silent node to be stale")
	}
}

// 🔴 A node that has gone quiet stops being evidence, and everything derived
// from its roster has to stop with it.
//
// The old roster is still sitting in memory and still lists sandboxes. Reading
// it as current does not merely add noise: it pins stale_copy and untracked at
// whatever that node last said, on a machine nobody can do anything about, for
// as long as discovery keeps the node — which is how a gauge whose help text
// says "briefly non-zero during a takeover" ends up permanently non-zero.
func TestReconcileStopsCountingAgainstAStaleRoster(t *testing.T) {
	f := newReconcileFixture()
	f.sandboxes = []pausedregistry.Sandbox{{
		SandboxID:      "s1",
		State:          pausedregistry.StateRunning,
		OriginNodeID:   "node-b",
		UpdatedAt:      f.dbNow.Add(-time.Second),
		LeaseExpiresAt: at(f.dbNow, time.Hour),
	}}
	// One sandbox the registry attributes to node-b, and one it has no row for.
	f.roster("node-a", time.Hour, "s1", "never-paused")

	result := f.run(testReportTTL)

	if got := result.staleCopy["node-a"]; got != 0 {
		t.Fatalf("expected a stale roster not to produce stale copies, got %d", got)
	}
	if got := result.untracked["node-a"]; got != 0 {
		t.Fatalf("expected a stale roster not to produce untracked sandboxes, got %d", got)
	}
	// The node is still reported — as stale, which is the one thing that *can*
	// be said about it.
	if !result.rosterStale["node-a"] {
		t.Fatal("expected the silent node to be reported as stale")
	}
	if _, present := result.staleCopy["node-a"]; !present {
		t.Fatal("expected the silent node to keep its series at zero rather than vanish")
	}
}

// 🔴 P0: when a node leaves the cluster entirely, its rows must become *more*
// visible, not less.
//
// Every other per-node series here is labelled from a roster, and a node that
// has been removed from discovery has none — so at the exact moment its
// problem becomes permanent, ghost/untracked/stale_copy/roster_stale all stop
// existing for it and any alert on them resolves itself. This count is
// labelled from the table instead.
func TestReconcileCountsRowsHeldByNodesWithNoRoster(t *testing.T) {
	f := newReconcileFixture()
	f.sandboxes = []pausedregistry.Sandbox{
		// Held by a node that is simply gone: no roster, fresh or otherwise.
		{SandboxID: "s1", State: pausedregistry.StateRunning, OriginNodeID: "node-gone", UpdatedAt: f.dbNow, LeaseExpiresAt: at(f.dbNow, time.Hour)},
		{SandboxID: "s2", State: pausedregistry.StateLocalOnly, OriginNodeID: "node-gone", SnapshotID: "snap", UpdatedAt: f.dbNow, LeaseExpiresAt: at(f.dbNow, time.Hour)},
		// Held by a node that is still here and reporting.
		{SandboxID: "s3", State: pausedregistry.StateRunning, OriginNodeID: "node-a", UpdatedAt: f.dbNow, LeaseExpiresAt: at(f.dbNow, time.Hour)},
		// A resuming row is held by its claimer, so it is the claimer's silence
		// that matters here, not the origin's.
		{SandboxID: "s4", State: pausedregistry.StateResuming, OriginNodeID: "node-a", ClaimedByNodeID: "node-quiet", SnapshotID: "snap", UpdatedAt: f.dbNow, LeaseExpiresAt: at(f.dbNow, time.Hour)},
	}
	f.roster("node-a", time.Second, "s3")
	// Still in discovery, but long past the freshness window.
	f.roster("node-quiet", time.Hour)

	result := f.run(testReportTTL)

	if got := result.rowsWithoutRoster["node-gone"]; got != 2 {
		t.Fatalf("expected both of the departed node's rows to be counted, got %d", got)
	}
	if got := result.rowsWithoutRoster["node-quiet"]; got != 1 {
		t.Fatalf("expected the resuming row to be counted against its claimer, got %d", got)
	}
	// A node that is reporting is seeded at zero rather than left out, so the
	// series exists before anything goes wrong.
	if got, present := result.rowsWithoutRoster["node-a"]; !present || got != 0 {
		t.Fatalf("expected a reporting node to be reported with zero rows without a roster, got %d (present=%v)", got, present)
	}
}

// A registry row names the node under the identity that node reported itself
// with, which during a fleet upgrade is its pod name, while rosters are keyed
// by the name discovery gives it. Unresolved, every row of an upgrading node
// reads as held by a machine with no roster — and simultaneously as a stale
// copy on the machine that actually holds it.
func TestReconcileResolvesRowsWrittenUnderAPreviousNodeIdentity(t *testing.T) {
	f := newReconcileFixture()
	f.resolveNodeID = func(nodeID string) string {
		if nodeID == "pod-a" {
			return "node-a"
		}
		return nodeID
	}
	f.sandboxes = []pausedregistry.Sandbox{{
		SandboxID:      "s1",
		State:          pausedregistry.StateRunning,
		OriginNodeID:   "pod-a",
		UpdatedAt:      f.dbNow,
		LeaseExpiresAt: at(f.dbNow, time.Hour),
	}}
	f.roster("node-a", time.Second, "s1")

	result := f.run(testReportTTL)

	if got := result.rowsWithoutRoster["node-a"]; got != 0 {
		t.Fatalf("expected the row to be attributed to the reporting node, got %d without a roster", got)
	}
	if _, present := result.rowsWithoutRoster["pod-a"]; present {
		t.Fatalf("expected no series under the identity the row was written with, got %v", result.rowsWithoutRoster)
	}
	if got := result.staleCopy["node-a"]; got != 0 {
		t.Fatalf("expected the holder not to look like a stale copy of itself, got %d", got)
	}
	if got := result.ghost["node-a"]; got != 0 {
		t.Fatalf("expected no ghost for a row the node reports, got %d", got)
	}
}

// Rows and rosters are judged against different clocks on purpose. If the
// reconciler mixed them, a scheduler whose clock is minutes off the database
// would invent ghosts.
// Rows and rosters are judged against different clocks on purpose. If the
// reconciler mixed them, a scheduler whose clock is minutes off the database
// would invent ghosts.
//
// 🔴 The offset direction is load-bearing, and one direction alone is not
// enough. With the process clock *behind* the database, "has this lease
// expired" and "is this row old enough" come out the same under either clock,
// so three of the four ways to mix them survive. The process clock is put
// ahead here, and node-drifted's roster is timestamped on the database clock,
// so each comparison below has an answer that only one clock produces:
//
//	ghost                → dbNow.Sub(row.UpdatedAt), not in.now.Sub(...)
//	liveLeaseLapsed      → LeaseExpired(dbNow), not LeaseExpired(in.now)
//	parkedLeaseExpiring  → dbNow.Add(window), not in.now.Add(window)
//	rosterStale          → in.now.Sub(LastSeen), not in.listing.Now.Sub(...)
func TestReconcileDoesNotMixTheTwoClocks(t *testing.T) {
	f := newReconcileFixture()
	f.localNow = f.dbNow.Add(90 * time.Minute)
	f.sandboxes = []pausedregistry.Sandbox{
		{
			SandboxID:      "s1",
			State:          pausedregistry.StateRunning,
			OriginNodeID:   "node-a",
			UpdatedAt:      f.dbNow.Add(-10 * time.Second),
			LeaseExpiresAt: at(f.dbNow, time.Hour),
		},
		{
			SandboxID:      "s2",
			State:          pausedregistry.StateLocalOnly,
			OriginNodeID:   "node-a",
			SnapshotID:     "snap",
			UpdatedAt:      f.dbNow,
			LeaseExpiresAt: at(f.dbNow, time.Hour),
		},
	}
	f.roster("node-a", time.Second)
	// A node whose last heartbeat landed 90 minutes ago by this process's
	// clock, and "just now" by the database's.
	f.rosters = append(f.rosters, Roster{NodeID: "node-drifted", LastSeen: f.dbNow})

	result := f.run(30 * time.Second)

	if got := result.ghost["node-a"]; got != 0 {
		t.Fatalf("expected the young row to be judged against the database clock, got %d ghosts", got)
	}
	if result.rosterStale["node-a"] {
		t.Fatal("expected the roster to be judged against the local clock")
	}
	if !result.rosterStale["node-drifted"] {
		t.Fatal("expected a roster last seen 90 minutes ago to be stale on the local clock")
	}
	if result.liveLeaseLapsed != 0 {
		t.Fatalf("expected the live lease to be judged against the database clock, got %d", result.liveLeaseLapsed)
	}
	if result.parkedLeaseExpiring != 0 {
		t.Fatalf("expected the parked lease window to be measured from the database clock, got %d", result.parkedLeaseExpiring)
	}
}

type stubRegistryReader struct {
	listing pausedregistry.Listing
	err     error
	// clusterID is the scope this reader claims. The reconciliation narrows
	// the rosters it compares against to the same one.
	clusterID string
	calls     int
	// gets counts single-row reads, so a test can assert that a lookup
	// answered without touching the database at all.
	gets int
	// neverReady models a reader that can answer without ever having read the
	// table. The Postgres reader cannot — a successful read is what marks it
	// ready — so this stands in for the class of readers that could, and pins
	// the rule that such an answer is never authoritative.
	neverReady bool
	ready      bool
}

func (s *stubRegistryReader) Get(_ context.Context, sandboxID string) (pausedregistry.Sandbox, bool, error) {
	s.gets++
	if s.err != nil {
		return pausedregistry.Sandbox{}, false, s.err
	}
	s.ready = true
	for _, sandbox := range s.listing.Sandboxes {
		if sandbox.SandboxID == sandboxID {
			return sandbox, true, nil
		}
	}
	return pausedregistry.Sandbox{}, false, nil
}

func (s *stubRegistryReader) List(context.Context) (pausedregistry.Listing, error) {
	s.calls++
	if s.err != nil {
		return pausedregistry.Listing{}, s.err
	}
	s.ready = true
	return s.listing, nil
}

func (s *stubRegistryReader) Ready() bool { return !s.neverReady && s.ready }

func (s *stubRegistryReader) ClusterID() string { return s.clusterID }

func (s *stubRegistryReader) Close() {}

// A disabled registry must stop the loop outright rather than tick forever
// against a reader that will never answer.
func TestReconcileLoopStopsWhenTheRegistryIsDisabled(t *testing.T) {
	svc := NewService(
		zap.NewNop(),
		NewAtomicNodeRegistry(nil, defaultObservedReportTTL),
		NewStrategy("round_robin"),
		NewInMemoryBindingStore(time.Second),
	)

	done := make(chan struct{})
	go func() {
		svc.RunRegistryReconcile(context.Background(), time.Millisecond)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("expected the reconcile loop to stop when the registry is disabled")
	}
}

// A read failure is transient by assumption: the round is skipped, the loop
// keeps going, and — critically — nothing is zeroed.
func TestReconcileOnceKeepsGoingAfterAReadFailure(t *testing.T) {
	reader := &stubRegistryReader{err: errors.New("connection refused")}
	svc := NewService(
		zap.NewNop(),
		NewAtomicNodeRegistry(nil, defaultObservedReportTTL),
		NewStrategy("round_robin"),
		NewInMemoryBindingStore(time.Second),
		WithPausedRegistry(reader, testReportTTL, testReportTTL),
	)

	if stop := svc.reconcileRegistryOnce(context.Background()); stop {
		t.Fatal("expected a read failure to leave the loop running")
	}
	if reader.calls != 1 {
		t.Fatalf("expected exactly one read attempt, got %d", reader.calls)
	}
}

func TestReconcileOnceSucceedsWithARealReader(t *testing.T) {
	now := time.Now()
	reader := &stubRegistryReader{listing: pausedregistry.Listing{
		Now: now,
		Sandboxes: []pausedregistry.Sandbox{
			{SandboxID: "s1", State: pausedregistry.StatePaused, OriginNodeID: "node-a", SnapshotID: "snap", UpdatedAt: now, LeaseExpiresAt: at(now, time.Hour)},
		},
	}}
	svc := NewService(
		zap.NewNop(),
		NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, defaultObservedReportTTL),
		NewStrategy("round_robin"),
		NewInMemoryBindingStore(time.Second),
		WithPausedRegistry(reader, testReportTTL, testReportTTL),
	)

	if stop := svc.reconcileRegistryOnce(context.Background()); stop {
		t.Fatal("expected a successful round to leave the loop running")
	}
	if !reader.Ready() {
		t.Fatal("expected the reader to be ready after a successful round")
	}
}

// 🔴 The wiring the two tests above cannot see: where the rosters and the
// identity resolution actually come from.
//
// The reconciliation compares a cluster-filtered read against the rosters, so
// the rosters have to be narrowed by the same cluster — and the scope is taken
// from the reader itself rather than from a second setting, so the two cannot
// be configured apart. The identity resolver is the node registry's own, so a
// row written under a node's previous name lands on that node.
func TestReconcileInputIsScopedByTheReadersOwnCluster(t *testing.T) {
	nodes := NewAtomicNodeRegistry(nil, defaultObservedReportTTL)
	nodes.Set([]Node{
		{ID: "node-a", Endpoint: "http://node-a", PodName: "pod-a"},
		{ID: "node-b", Endpoint: "http://node-b"},
	}, nil)
	now := time.Now()
	heartbeatWithClusterRoster(t, nodes, "node-a", "cluster-a", now, "s1")
	heartbeatWithClusterRoster(t, nodes, "node-b", "cluster-b", now, "s2")

	svc := NewService(
		zap.NewNop(),
		nodes,
		NewStrategy("round_robin"),
		NewInMemoryBindingStore(time.Minute),
		WithPausedRegistry(&stubRegistryReader{clusterID: "cluster-a"}, testReportTTL, testReportTTL),
	)

	input := svc.registryReconcileInput(pausedregistry.Listing{Now: now}, now)

	if len(input.rosters) != 1 || input.rosters[0].NodeID != "node-a" {
		t.Fatalf("expected only the reader's own cluster to be compared, got %v", input.rosters)
	}
	if input.resolveNodeID == nil {
		t.Fatal("expected the node registry's identity resolution to be wired in")
	}
	if got := input.resolveNodeID("pod-a"); got != "node-a" {
		t.Fatalf("expected a row written under the pod name to resolve to node-a, got %q", got)
	}
	if got := input.resolveNodeID("node-gone"); got != "node-gone" {
		t.Fatalf("expected an unknown identity to be left alone, got %q", got)
	}
}

// TestReconcileCountsExecutionMismatches.
//
// 🔴 The most direct signal in this pass that a sandbox is live twice: the row
// and the machine disagree about which VM is the sandbox. It costs nothing —
// the round already holds both sides — and it is what an orphan reaper will
// eventually act on.
func TestReconcileCountsExecutionMismatches(t *testing.T) {
	f := newReconcileFixture()
	f.sandboxes = []pausedregistry.Sandbox{
		// The registry says node-a is running it under execNew.
		{SandboxID: "s1", State: pausedregistry.StateRunning, OriginNodeID: "node-a",
			ExecutionID: execNew, LeaseExpiresAt: at(f.dbNow, time.Hour)},
		// Agreed: no mismatch.
		{SandboxID: "s2", State: pausedregistry.StateRunning, OriginNodeID: "node-a",
			ExecutionID: execOld, LeaseExpiresAt: at(f.dbNow, time.Hour)},
		// The node reports an incarnation, the row names none. Not a mismatch:
		// a parked row names none by construction, and counting it would make
		// this series report every ordinary pause.
		{SandboxID: "s3", State: pausedregistry.StatePaused, OriginNodeID: "node-a",
			SnapshotID: "snap", LeaseExpiresAt: at(f.dbNow, time.Hour)},
	}
	f.rosterWithExecutions("node-a", time.Second,
		RosterEntry{SandboxID: "s1", ExecutionID: execOld},
		RosterEntry{SandboxID: "s2", ExecutionID: execOld},
		RosterEntry{SandboxID: "s3", ExecutionID: execNew},
	)

	result := f.run(time.Minute)
	if got := result.executionMismatch["node-a"]; got != 1 {
		t.Fatalf("execution mismatches for node-a: got %d, want 1 (%+v)", got, result.executionMismatch)
	}
}

// TestReconcileDoesNotCountAMismatchAgainstANodeTooOldToReportOne is the
// control, and the reason this series has to be read beside the legacy roster
// counter.
//
// 🟢 Without it, a build that counted "the node reported nothing" as a
// disagreement would report every sandbox on every pre-incarnation node as a
// double-live, for the whole length of a rollout.
func TestReconcileDoesNotCountAMismatchAgainstANodeTooOldToReportOne(t *testing.T) {
	f := newReconcileFixture()
	f.sandboxes = []pausedregistry.Sandbox{
		{SandboxID: "s1", State: pausedregistry.StateRunning, OriginNodeID: "node-a",
			ExecutionID: execNew, LeaseExpiresAt: at(f.dbNow, time.Hour)},
	}
	f.roster("node-a", time.Second, "s1")

	result := f.run(time.Minute)
	if got := result.executionMismatch["node-a"]; got != 0 {
		t.Fatalf("a node that reported no incarnation was counted as disagreeing: %d", got)
	}
}
