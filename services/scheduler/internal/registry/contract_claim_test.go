// Contract tests for the resume claim and the running mark — the three-way
// test in ClaimForResume and the claim guard in MarkRunning.
//
// Ported from tests/paused_registry.rs; see contract_test.go for the harness
// and the discipline these are written under.
package registry

import (
	"context"
	"testing"
	"time"
)

// A `paused` row has no holder by definition — nobody is running the sandbox
// and its snapshot is durable — so it stays claimable straight away. Making
// the lease apply here too would add a delay to every ordinary cross-node
// resume for no gain.
//
// Ported from a_paused_sandbox_is_claimable_immediately.
func TestContractAPausedSandboxIsClaimableImmediately(t *testing.T) {
	env := contractSetup(t)
	sandboxID := contractUUID(t)

	snapshot := env.pauseAndPublish(t, env.cluster, sandboxID, contractNodeA)

	claim := env.claim(t, env.cluster, sandboxID, contractNodeB)
	if claim.Outcome != ClaimOutcomeClaimed {
		t.Fatalf("a durably paused sandbox is claimable by anybody: got %q", claim.Outcome)
	}
	if claim.Entry == nil {
		t.Fatal("a granted claim must carry the row")
	}
	if claim.Entry.State != StateResuming {
		t.Fatalf("the claim moves the row to resuming: got %q", claim.Entry.State)
	}
	if claim.Entry.ClaimedByNodeID != contractNodeB {
		t.Fatalf("the claimer must be recorded: got %q, want %q", claim.Entry.ClaimedByNodeID, contractNodeB)
	}
	if claim.Entry.OriginNodeID != contractNodeA {
		t.Fatalf("a claim deliberately leaves origin_node_id pointing at whoever holds the local artifacts: got %q, want %q", claim.Entry.OriginNodeID, contractNodeA)
	}
	if claim.Entry.SnapshotID != snapshot {
		t.Fatalf("the claim must hand back the snapshot to rebuild from: got %q, want %q", claim.Entry.SnapshotID, snapshot)
	}
	// begin_pause wrote generation 1 and complete_pause does not bump, so a
	// claim that bumps once lands on 2.
	if claim.Entry.Generation != 2 {
		t.Fatalf("a claim bumps the generation exactly once: got %d, want 2", claim.Entry.Generation)
	}
}

// 🔴 An ordinary resume must not report itself as a takeover.
//
// The claim is one conditional UPDATE that sets `state = 'resuming'`, and
// RETURNING describes the row it produced — so the returned state reads
// `resuming` for every claim, whatever the row said a moment earlier. Deciding
// the outcome from it labelled every routine resume "holder stopped renewing
// its lease", which buried the rare claim that really does cost someone an
// unpublished pause under the common one that costs nothing.
//
// Ported from an_ordinary_claim_reports_no_takeover.
func TestContractAnOrdinaryClaimReportsNoTakeover(t *testing.T) {
	env := contractSetup(t)
	sandboxID := contractUUID(t)

	env.pauseAndPublish(t, env.cluster, sandboxID, contractNodeA)

	claim := env.claim(t, env.cluster, sandboxID, contractNodeB)
	if claim.Outcome != ClaimOutcomeClaimed {
		t.Fatalf("a durably paused sandbox is claimable: got %q", claim.Outcome)
	}
	if claim.PreviousState != StatePaused {
		t.Fatalf("no lease was in question here — the row was published and idle — so the claim must report what it actually replaced: got %q, want %q", claim.PreviousState, StatePaused)
	}
}

// 🔴 The second-copy bug. A `running` row means some node is running the
// sandbox; taking it over on any signal weaker than the node itself saying so
// starts a second live copy. The gateway routes a resume to an arbitrary node
// whenever the scheduler holds no binding, and bindings are in-memory with a
// 30s TTL — so "nobody is bound to it" arrives routinely for perfectly healthy
// sandboxes.
//
// Ported from a_live_holder_cannot_have_its_sandbox_taken_away.
func TestContractALiveHolderCannotHaveItsSandboxTakenAway(t *testing.T) {
	env := contractSetup(t)
	sandboxID := contractUUID(t)

	env.pauseAndPublish(t, env.cluster, sandboxID, contractNodeA)
	env.markRunning(t, env.cluster, sandboxID, contractNodeA)

	claim := env.claim(t, env.cluster, sandboxID, contractNodeB)
	if claim.Outcome != ClaimOutcomeConflict {
		t.Fatalf("a live holder must not be displaced: got %q, want %q", claim.Outcome, ClaimOutcomeConflict)
	}
	if claim.OriginNodeID != contractNodeA {
		t.Fatalf("the conflict must name where the sandbox actually is: got %q, want %q", claim.OriginNodeID, contractNodeA)
	}
	if row := env.requireRow(t, env.cluster, sandboxID); row.State != StateRunning {
		t.Fatalf("a refused claim must leave the row running: got %q", row.State)
	}
}

// 🔴 The duplication this design exists to make impossible.
//
// A lapsed lease proves the holder cannot reach this database. It does not
// prove the holder is dead — a partitioned node keeps running every sandbox it
// has, keeps being routed traffic, and keeps writing to its rootfs layers,
// while its rows expire on schedule. Rebuilding one of those sandboxes on
// another node produces two live copies diverging from the same snapshot, and
// nothing downstream can merge them again.
//
// Ported from a_live_sandbox_is_never_taken_over_on_a_lapsed_lease.
func TestContractALiveSandboxIsNeverTakenOverOnALapsedLease(t *testing.T) {
	env := contractSetup(t)
	sandboxID := contractUUID(t)

	env.pauseAndPublish(t, env.cluster, sandboxID, contractNodeA)
	env.markRunning(t, env.cluster, sandboxID, contractNodeA)

	time.Sleep(contractPastLease)

	claim := env.claim(t, env.cluster, sandboxID, contractNodeB)
	if claim.Outcome != ClaimOutcomeConflict {
		t.Fatalf("a live sandbox must never be rebuilt elsewhere on a timer: got %q, want %q", claim.Outcome, ClaimOutcomeConflict)
	}
	if claim.OriginNodeID != contractNodeA {
		t.Fatalf("the conflict must name where the sandbox actually is: got %q, want %q", claim.OriginNodeID, contractNodeA)
	}
	if row := env.requireRow(t, env.cluster, sandboxID); row.State != StateRunning {
		t.Fatalf("a refused claim must leave the row running: got %q", row.State)
	}
}

// 🔴 The same refusal for a resume that is still in flight. `resuming` names a
// node that is bringing the sandbox up right now; a lapsed lease there says
// only that the node cannot reach the database, and a second claimer would
// build a second copy from the same snapshot.
//
// Not in the Rust suite as a case of its own — it covers `running` and leaves
// the `resuming` half of the same predicate branch untested.
func TestContractAnInFlightResumeIsNeverTakenOverOnALapsedLease(t *testing.T) {
	env := contractSetup(t)
	sandboxID := contractUUID(t)

	env.pauseAndPublish(t, env.cluster, sandboxID, contractNodeA)
	if claim := env.claim(t, env.cluster, sandboxID, contractNodeB); claim.Outcome != ClaimOutcomeClaimed {
		t.Fatalf("node B should have taken the claim: got %q", claim.Outcome)
	}

	time.Sleep(contractPastLease)

	claim := env.claim(t, env.cluster, sandboxID, "node-c")
	if claim.Outcome != ClaimOutcomeConflict {
		t.Fatalf("a resume in flight must not be taken over on a timer: got %q, want %q", claim.Outcome, ClaimOutcomeConflict)
	}
	if claim.OriginNodeID != contractNodeB {
		t.Fatalf("the conflict must name the claimer rather than the node holding the artifacts: got %q, want %q", claim.OriginNodeID, contractNodeB)
	}
	row := env.requireRow(t, env.cluster, sandboxID)
	if row.State != StateResuming || row.ClaimedByNodeID != contractNodeB {
		t.Fatalf("a refused claim must leave the row with its claimer: got state %q claimed by %q", row.State, row.ClaimedByNodeID)
	}
}

// The parked half, which is what the lease is still for. `publishing` and
// `local_only` name a node that already stopped the VM, so taking the sandbox
// over cannot duplicate it — it only rewinds to the snapshot the previous
// pause left behind. That loss is worth accepting to avoid stranding the
// sandbox on a node that may never come back, so here the lease does decide.
//
// Ported from a_parked_sandbox_moves_on_once_its_holder_stops_renewing.
func TestContractAParkedSandboxMovesOnOnceItsHolderStopsRenewing(t *testing.T) {
	env := contractSetup(t)
	sandboxID := contractUUID(t)

	// A first pause that published, then a second that did not: the row is
	// `publishing` while still naming the older, durable snapshot.
	snapshot := env.pauseAndPublish(t, env.cluster, sandboxID, contractNodeA)
	env.beginPause(t, env.cluster, sandboxID, contractNodeA)

	claim := env.claim(t, env.cluster, sandboxID, contractNodeB)
	if claim.Outcome != ClaimOutcomeNotReady {
		t.Fatalf("a live lease must keep the upload on its own node: got %q, want %q", claim.Outcome, ClaimOutcomeNotReady)
	}
	if claim.OriginNodeID != contractNodeA {
		t.Fatalf("the redirect must name the node that can serve the resume: got %q, want %q", claim.OriginNodeID, contractNodeA)
	}

	time.Sleep(contractPastLease)

	claim = env.claim(t, env.cluster, sandboxID, contractNodeB)
	if claim.Outcome != ClaimOutcomeClaimed {
		t.Fatalf("a parked sandbox whose holder went quiet must be recoverable elsewhere: got %q", claim.Outcome)
	}
	if claim.PreviousState != StatePublishing {
		t.Fatalf("the claim has to name the state it overrode: this one cost its holder an unpublished pause, which is the whole reason the event is worth logging: got %q, want %q", claim.PreviousState, StatePublishing)
	}
	if claim.Entry == nil {
		t.Fatal("a granted claim must carry the row")
	} else if claim.Entry.SnapshotID != snapshot {
		t.Fatalf("the takeover rewinds to the last durable snapshot: got %q, want %q", claim.Entry.SnapshotID, snapshot)
	}
}

// A pause whose very first publish fails has nothing to fall back on, so the
// row must stay unclaimable however long its lease has been lapsed — there is
// no snapshot to rebuild from, and answering otherwise would hand a caller a
// claim it cannot use.
//
// Ported from a_sandbox_that_never_published_is_never_claimable.
func TestContractASandboxThatNeverPublishedIsNeverClaimable(t *testing.T) {
	env := contractSetup(t)
	sandboxID := contractUUID(t)

	began := env.beginPause(t, env.cluster, sandboxID, contractNodeA)
	if err := env.store.MarkLocalOnly(context.Background(), env.cluster, sandboxID, began.Generation); err != nil {
		t.Fatalf("downgrade to local-only: %v", err)
	}

	time.Sleep(contractPastLease)

	claim := env.claim(t, env.cluster, sandboxID, contractNodeB)
	if claim.Outcome != ClaimOutcomeNotReady {
		t.Fatalf("a sandbox with no snapshot must stay on its own node: got %q, want %q", claim.Outcome, ClaimOutcomeNotReady)
	}
	if claim.OriginNodeID != contractNodeA {
		t.Fatalf("the redirect must name the only node that can serve it: got %q, want %q", claim.OriginNodeID, contractNodeA)
	}
	if row := env.requireRow(t, env.cluster, sandboxID); row.State != StateLocalOnly {
		t.Fatalf("a refused claim must leave the row alone: got %q", row.State)
	}
}

// A resume that fails after claiming has to put the sandbox back, or one bad
// attempt parks it until the lease lapses.
//
// Ported from releasing_a_claim_puts_the_sandbox_back.
func TestContractReleasingAClaimPutsTheSandboxBack(t *testing.T) {
	env := contractSetup(t)
	sandboxID := contractUUID(t)

	env.pauseAndPublish(t, env.cluster, sandboxID, contractNodeA)
	claim := env.claim(t, env.cluster, sandboxID, contractNodeB)
	if claim.Outcome != ClaimOutcomeClaimed || claim.Entry == nil {
		t.Fatalf("the claim should have been granted: got %q", claim.Outcome)
	}

	matched, err := env.store.ReleaseClaim(context.Background(), env.cluster, sandboxID, claim.Entry.Generation)
	if err != nil {
		t.Fatalf("release the claim: %v", err)
	}
	if !matched {
		t.Fatal("a release quoting the claim's own generation must match the row it holds")
	}

	row := env.requireRow(t, env.cluster, sandboxID)
	if row.State != StatePaused {
		t.Fatalf("a released claim returns the sandbox to paused: got %q", row.State)
	}
	if row.ClaimedByNodeID != "" {
		t.Fatalf("a released claim must clear the claimer: got %q", row.ClaimedByNodeID)
	}

	if claim := env.claim(t, env.cluster, sandboxID, "node-c"); claim.Outcome != ClaimOutcomeClaimed {
		t.Fatalf("a released sandbox must be claimable again: got %q", claim.Outcome)
	}
}

// 🔴 One node's resume must not erase another's in-flight one. A blind write
// here cleared claimed_by_node_id mid-claim, after which both nodes believed
// they held the sandbox and both brought it up.
//
// Ported from marking_a_sandbox_running_cannot_erase_another_nodes_claim.
func TestContractMarkingASandboxRunningCannotEraseAnotherNodesClaim(t *testing.T) {
	env := contractSetup(t)
	sandboxID := contractUUID(t)

	env.pauseAndPublish(t, env.cluster, sandboxID, contractNodeA)
	if claim := env.claim(t, env.cluster, sandboxID, contractNodeB); claim.Outcome != ClaimOutcomeClaimed {
		t.Fatalf("node B should have taken the claim: got %q", claim.Outcome)
	}

	// Node A resumes from its own disk at the same moment.
	if confirmed := env.markRunning(t, env.cluster, sandboxID, contractNodeA); confirmed {
		t.Fatal("a node that does not hold the claim must not be told it is the holder")
	}

	row := env.requireRow(t, env.cluster, sandboxID)
	if row.State != StateResuming {
		t.Fatalf("node A must not have taken the row from node B's claim: got %q", row.State)
	}
	if row.ClaimedByNodeID != contractNodeB {
		t.Fatalf("the claimer must survive another node's mark: got %q, want %q", row.ClaimedByNodeID, contractNodeB)
	}

	// The node that does hold the claim still completes normally.
	if confirmed := env.markRunning(t, env.cluster, sandboxID, contractNodeB); !confirmed {
		t.Fatal("the claimer must be able to complete its own resume")
	}
	row = env.requireRow(t, env.cluster, sandboxID)
	if row.State != StateRunning {
		t.Fatalf("unexpected state after the claimer marked it running: got %q", row.State)
	}
	if row.OriginNodeID != contractNodeB {
		t.Fatalf("the holder is now the node that resumed it: got %q, want %q", row.OriginNodeID, contractNodeB)
	}
	if row.ClaimedByNodeID != "" {
		t.Fatalf("a completed resume clears the claim: got %q", row.ClaimedByNodeID)
	}
}

// 🔴 The signal the running-sandbox reaper is built on. A node may only judge
// its live copy against a registry row once the registry has confirmed the row
// is about that copy; without a confirmation an absent row means nothing, and
// reading it as "the cluster moved on" tears down sandboxes that were simply
// created here and never announced.
//
// Ported from marking_an_untracked_sandbox_running_reports_that_it_is_untracked,
// with the "never creates a row" half of the contract asserted as well.
func TestContractMarkingAnUntrackedSandboxRunningReportsThatItIsUntracked(t *testing.T) {
	env := contractSetup(t)
	sandboxID := contractUUID(t)

	if confirmed := env.markRunning(t, env.cluster, sandboxID, contractNodeA); confirmed {
		t.Fatal("a sandbox with no row must not be reported as tracked")
	}

	env.requireNoRow(t, env.cluster, sandboxID)
}

// Ported from marking_a_tracked_sandbox_running_reports_the_node_as_holder.
func TestContractMarkingATrackedSandboxRunningReportsTheNodeAsHolder(t *testing.T) {
	env := contractSetup(t)
	sandboxID := contractUUID(t)

	env.pauseAndPublish(t, env.cluster, sandboxID, contractNodeA)

	if confirmed := env.markRunning(t, env.cluster, sandboxID, contractNodeA); !confirmed {
		t.Fatal("the row now names this node as the holder")
	}
	if row := env.requireRow(t, env.cluster, sandboxID); row.State != StateRunning {
		t.Fatalf("unexpected state: got %q, want %q", row.State, StateRunning)
	}
}

// A refusal has to be distinguishable from a success, or the node would enrol
// a sandbox it does not hold and then reconcile the wrong copy away.
//
// Ported from marking_running_reports_a_refusal_when_another_node_holds_the_claim.
func TestContractMarkingRunningReportsARefusalWhenAnotherNodeHoldsTheClaim(t *testing.T) {
	env := contractSetup(t)
	sandboxID := contractUUID(t)

	env.pauseAndPublish(t, env.cluster, sandboxID, contractNodeA)
	if claim := env.claim(t, env.cluster, sandboxID, contractNodeB); claim.Outcome != ClaimOutcomeClaimed {
		t.Fatalf("node B should have taken the claim: got %q", claim.Outcome)
	}

	if confirmed := env.markRunning(t, env.cluster, sandboxID, contractNodeA); confirmed {
		t.Fatal("another node holds the claim, so this node is not the holder")
	}
}

// 🔴 Not in the Rust suite. MarkRunning is scoped to one cluster like every
// other write; without the scope a node marks a same-named sandbox in another
// cluster as running on itself, which both moves that row's origin and hands
// this node a confirmation it will later reconcile against.
func TestContractMarkingRunningCannotReachAnotherClustersRow(t *testing.T) {
	env := contractSetup(t)
	theirs := env.otherCluster(t)
	sandboxID := contractUUID(t)

	env.pauseAndPublish(t, theirs, sandboxID, contractNodeA)

	if confirmed := env.markRunning(t, env.cluster, sandboxID, contractNodeB); confirmed {
		t.Fatal("another cluster's row must not be markable")
	}

	row := env.requireRow(t, theirs, sandboxID)
	if row.State != StatePaused {
		t.Fatalf("another cluster's row must be untouched: got state %q", row.State)
	}
	if row.OriginNodeID != contractNodeA {
		t.Fatalf("another cluster's row must be untouched: got origin %q", row.OriginNodeID)
	}
}
