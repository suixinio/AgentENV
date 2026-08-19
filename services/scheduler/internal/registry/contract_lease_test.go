// Contract tests for the two clocks — the lease that says whether a holder can
// still reach the database, and the deadline the sandbox's own owner set — and
// for the two sweeps that act on them.
//
// Ported from tests/paused_registry.rs; see contract_test.go for the harness
// and the discipline these are written under.
package registry

import (
	"context"
	"testing"
	"time"
)

// Renewal is what keeps a healthy node's sandboxes its own, and it must be the
// holder doing it — otherwise any node could keep another node's dead
// sandboxes out of reach indefinitely. The caller passes its whole roster and
// the predicate, not the caller, decides which rows are eligible.
//
// Ported from only_the_holder_can_renew_its_lease.
func TestContractOnlyTheHolderCanRenewItsLease(t *testing.T) {
	env := contractSetup(t)
	sandboxID := contractUUID(t)

	env.pauseAndPublish(t, env.cluster, sandboxID, contractNodeA)
	env.markRunning(t, env.cluster, sandboxID, contractNodeA)

	if renewed := env.renew(t, env.cluster, contractNodeB, contractHeld(sandboxID)); renewed != 0 {
		t.Fatalf("a node must not be able to renew a lease on a sandbox it does not hold: renewed %d", renewed)
	}
	if renewed := env.renew(t, env.cluster, contractNodeA, contractHeld(sandboxID)); renewed != 1 {
		t.Fatalf("the holder's renewal must land: renewed %d, want 1", renewed)
	}

	// Renewed, so the takeover that would otherwise succeed by now does not.
	time.Sleep(600 * time.Millisecond)
	env.renew(t, env.cluster, contractNodeA, contractHeld(sandboxID))
	time.Sleep(600 * time.Millisecond)

	if claim := env.claim(t, env.cluster, sandboxID, contractNodeB); claim.Outcome != ClaimOutcomeConflict {
		t.Fatalf("a renewed lease must keep the sandbox: got %q, want %q", claim.Outcome, ClaimOutcomeConflict)
	}
}

// 🔴 Not in the Rust suite. Renewal is scoped to one cluster like every other
// write: node ids name machines and repeat across clusters, so a renewal that
// ignored the scope would let one cluster's heartbeat hold another cluster's
// rows out of reach forever.
func TestContractARenewalCannotReachAnotherClustersRows(t *testing.T) {
	env := contractSetup(t)
	theirs := env.otherCluster(t)
	sandboxID := contractUUID(t)

	env.pauseAndPublish(t, theirs, sandboxID, contractNodeA)
	env.markRunning(t, theirs, sandboxID, contractNodeA)

	if renewed := env.renew(t, env.cluster, contractNodeA, contractHeld(sandboxID)); renewed != 0 {
		t.Fatalf("another cluster's row must not be renewable: renewed %d", renewed)
	}
}

// 🔴 Not in the Rust suite, which only ever checks the row count a renewal
// returns. That count is the same whether the statement moved the lease or
// merely matched the row, so nothing there fails if the renewal writes no
// clock at all — and a renewal that does not move `lease_expires_at` lets
// every parked row on a perfectly healthy node be taken over one TTL later.
func TestContractARenewalKeepsAParkedRowWithItsHolder(t *testing.T) {
	env := contractSetup(t)
	sandboxID := contractUUID(t)

	// Published once, then a second pause that is still uploading: parked,
	// with a durable snapshot behind it, which is the shape the lease decides.
	env.pauseAndPublish(t, env.cluster, sandboxID, contractNodeA)
	env.beginPause(t, env.cluster, sandboxID, contractNodeA)

	deadline := time.Now().Add(time.Hour)
	for waited := time.Duration(0); waited < contractPastLease; waited += 400 * time.Millisecond {
		time.Sleep(400 * time.Millisecond)
		if renewed := env.renew(t, env.cluster, contractNodeA, HeldSandbox{SandboxID: sandboxID, ExpiresAt: &deadline}); renewed != 1 {
			t.Fatalf("the holder of a publishing row must be able to renew it: renewed %d, want 1", renewed)
		}
	}

	claim := env.claim(t, env.cluster, sandboxID, contractNodeB)
	if claim.Outcome != ClaimOutcomeNotReady {
		t.Fatalf("a node that keeps renewing must keep its unpublished pause: got %q, want %q", claim.Outcome, ClaimOutcomeNotReady)
	}
	if claim.OriginNodeID != contractNodeA {
		t.Fatalf("the redirect must name the holder: got %q, want %q", claim.OriginNodeID, contractNodeA)
	}
}

// How a live row is *actually* released: by the next process on the machine
// that was holding it.
//
// Being the successor is the proof no timeout can supply — the previous
// process's VMs were its children, so a process that has just started on that
// machine and holds nothing is looking at rows whose sandboxes are certainly
// gone. The sandbox goes back to `paused` and its snapshot stays, so the next
// resume rebuilds it anywhere.
//
// Ported from a_successor_process_releases_what_the_previous_one_was_running.
func TestContractASuccessorProcessReleasesWhatThePreviousOneWasRunning(t *testing.T) {
	env := contractSetup(t)
	sandboxID := contractUUID(t)

	snapshot := env.pauseAndPublish(t, env.cluster, sandboxID, contractNodeA)
	env.markRunning(t, env.cluster, sandboxID, contractNodeA)

	released := env.releaseHoldings(t, env.cluster, contractNodeA)
	if released.Released != 1 || released.Discarded != 0 {
		t.Fatalf("a live row with a snapshot is released, not discarded: got %+v, want {Released:1 Discarded:0}", released)
	}

	row := env.requireRow(t, env.cluster, sandboxID)
	if row.State != StatePaused {
		t.Fatalf("a released row goes back to paused: got %q", row.State)
	}
	if row.SnapshotID != snapshot {
		t.Fatalf("the snapshot is the whole reason the sandbox survives the node: got %q, want %q", row.SnapshotID, snapshot)
	}

	if claim := env.claim(t, env.cluster, sandboxID, contractNodeB); claim.Outcome != ClaimOutcomeClaimed {
		t.Fatalf("a released sandbox must be recoverable on any node: got %q", claim.Outcome)
	}
}

// A live sandbox whose snapshot never published has its only artifacts on the
// disk of the process that just died, and the resume that started it consumed
// the paused record they belonged to. Keeping the row would leave something no
// node can claim (a claim requires a snapshot) and no node will ever clear.
//
// Ported from a_successor_process_discards_live_rows_that_never_published.
func TestContractASuccessorProcessDiscardsLiveRowsThatNeverPublished(t *testing.T) {
	env := contractSetup(t)
	sandboxID := contractUUID(t)

	began := env.beginPause(t, env.cluster, sandboxID, contractNodeA)
	if err := env.store.MarkLocalOnly(context.Background(), env.cluster, sandboxID, began.Generation); err != nil {
		t.Fatalf("the publish never landed: %v", err)
	}
	if confirmed := env.markRunning(t, env.cluster, sandboxID, contractNodeA); !confirmed {
		t.Fatal("the origin node resumes its own local-only sandbox and the cluster tracks it")
	}

	released := env.releaseHoldings(t, env.cluster, contractNodeA)
	if released.Released != 0 || released.Discarded != 1 {
		t.Fatalf("a live row with nothing published behind it is discarded: got %+v, want {Released:0 Discarded:1}", released)
	}

	env.requireNoRow(t, env.cluster, sandboxID)
}

// The release is scoped to one node's own holdings twice over: another node's
// live sandboxes are untouchable, and this node's parked rows are left exactly
// as they were. Widening either would turn a routine restart into a cluster
// event.
//
// Ported from releasing_holdings_touches_nothing_but_this_nodes_live_rows,
// with a third row proving the cluster scope — node ids name machines, and the
// same name in another cluster is a different machine.
func TestContractReleasingHoldingsTouchesNothingButThisNodesLiveRows(t *testing.T) {
	env := contractSetup(t)
	theirs := env.otherCluster(t)

	liveElsewhere := contractUUID(t)
	env.pauseAndPublish(t, env.cluster, liveElsewhere, contractNodeB)
	env.markRunning(t, env.cluster, liveElsewhere, contractNodeB)

	parkedHere := contractUUID(t)
	env.pauseAndPublish(t, env.cluster, parkedHere, contractNodeA)

	liveInAnotherCluster := contractUUID(t)
	env.pauseAndPublish(t, theirs, liveInAnotherCluster, contractNodeA)
	env.markRunning(t, theirs, liveInAnotherCluster, contractNodeA)

	released := env.releaseHoldings(t, env.cluster, contractNodeA)
	if released.Released != 0 || released.Discarded != 0 {
		t.Fatalf("a node with nothing live of its own must release nothing: got %+v", released)
	}

	if row := env.requireRow(t, env.cluster, liveElsewhere); row.State != StateRunning {
		t.Fatalf("another node's live sandbox must be untouched: got %q", row.State)
	}
	if row := env.requireRow(t, env.cluster, parkedHere); row.State != StatePaused {
		t.Fatalf("this node's parked row must be untouched: got %q", row.State)
	}
	if row := env.requireRow(t, theirs, liveInAnotherCluster); row.State != StateRunning {
		t.Fatalf("another cluster's row must be untouched: got %q", row.State)
	}
}

// A resume that was in flight when the process died is released by the node
// that claimed it, not by the one whose disk holds the artifacts — the claim
// deliberately leaves origin_node_id alone, so judging by it would let the
// origin release a rebuild another node is midway through.
//
// Ported from an_interrupted_resume_is_released_by_the_node_that_claimed_it.
func TestContractAnInterruptedResumeIsReleasedByTheNodeThatClaimedIt(t *testing.T) {
	env := contractSetup(t)
	sandboxID := contractUUID(t)

	env.pauseAndPublish(t, env.cluster, sandboxID, contractNodeA)
	if claim := env.claim(t, env.cluster, sandboxID, contractNodeB); claim.Outcome != ClaimOutcomeClaimed {
		t.Fatalf("the claim should have been granted: got %q", claim.Outcome)
	}

	byOrigin := env.releaseHoldings(t, env.cluster, contractNodeA)
	if byOrigin.Released != 0 || byOrigin.Discarded != 0 {
		t.Fatalf("the origin must not release a resume another node is running: got %+v", byOrigin)
	}
	if row := env.requireRow(t, env.cluster, sandboxID); row.State != StateResuming {
		t.Fatalf("the row must still be resuming: got %q", row.State)
	}

	byClaimer := env.releaseHoldings(t, env.cluster, contractNodeB)
	if byClaimer.Released != 1 || byClaimer.Discarded != 0 {
		t.Fatalf("the claimer releases its own interrupted resume: got %+v, want {Released:1 Discarded:0}", byClaimer)
	}

	row := env.requireRow(t, env.cluster, sandboxID)
	if row.State != StatePaused {
		t.Fatalf("a released resume goes back to paused: got %q", row.State)
	}
	if row.ClaimedByNodeID != "" {
		t.Fatalf("a released resume clears the claimer: got %q", row.ClaimedByNodeID)
	}
}

// 🔴 The one thing that keeps a decommissioned machine's sandboxes from being
// stranded forever.
//
// Nothing else can release them: ReleaseNodeHoldings needs a successor process
// on that machine, and ClaimForResume refuses live rows outright. So the
// cluster steps in when the sandbox has outlived the deadline its own user
// gave it *and* nobody has renewed for it since — which is enforcing the
// timeout, not guessing whether the node is dead.
//
// Ported from a_sandbox_that_outlived_its_deadline_on_a_silent_node_is_reclaimed,
// with a second cluster's identically expired row proving the scope: this
// sweep is the most destructive statement in the registry and it runs on a
// timer nobody is watching.
func TestContractASandboxThatOutlivedItsDeadlineOnASilentNodeIsReclaimed(t *testing.T) {
	env := contractSetup(t)
	theirs := env.otherCluster(t)
	sandboxID := contractUUID(t)
	elsewhere := contractUUID(t)

	snapshot := env.pauseAndPublish(t, env.cluster, sandboxID, contractNodeA)
	env.markRunning(t, env.cluster, sandboxID, contractNodeA)
	env.renew(t, env.cluster, contractNodeA, contractHeldDue(sandboxID, -time.Second))

	env.pauseAndPublish(t, theirs, elsewhere, contractNodeA)
	env.markRunning(t, theirs, elsewhere, contractNodeA)
	env.renew(t, theirs, contractNodeA, contractHeldDue(elsewhere, -time.Second))

	time.Sleep(contractPastLease)

	reclaimed := env.reclaim(t, env.cluster)
	if reclaimed.Released != 1 || reclaimed.Discarded != 0 {
		t.Fatalf("an expired sandbox on a silent node comes back to the cluster: got %+v, want {Released:1 Discarded:0}", reclaimed)
	}

	row := env.requireRow(t, env.cluster, sandboxID)
	if row.State != StatePaused {
		t.Fatalf("a reclaimed row goes back to paused: got %q", row.State)
	}
	if row.SnapshotID != snapshot {
		t.Fatalf("a reclaimed row keeps the snapshot it can be rebuilt from: got %q, want %q", row.SnapshotID, snapshot)
	}
	if row.ClaimedByNodeID != "" {
		t.Fatalf("a reclaimed row has no claimer: got %q", row.ClaimedByNodeID)
	}

	if row := env.requireRow(t, theirs, elsewhere); row.State != StateRunning {
		t.Fatalf("another cluster's expired row is not this sweep's business: got %q", row.State)
	}
}

// The lease is only half the condition. A node can go silent while its
// sandboxes still have hours to run — that is a partition, and taking those
// sandboxes would duplicate them.
//
// Ported from a_sandbox_still_within_its_deadline_survives_a_silent_node.
func TestContractASandboxStillWithinItsDeadlineSurvivesASilentNode(t *testing.T) {
	env := contractSetup(t)
	sandboxID := contractUUID(t)

	env.pauseAndPublish(t, env.cluster, sandboxID, contractNodeA)
	env.markRunning(t, env.cluster, sandboxID, contractNodeA)
	env.renew(t, env.cluster, contractNodeA, contractHeldDue(sandboxID, time.Hour))

	time.Sleep(contractPastLease)

	reclaimed := env.reclaim(t, env.cluster)
	if reclaimed.Released != 0 || reclaimed.Discarded != 0 {
		t.Fatalf("a sandbox with time left must not be taken from a node that is merely quiet: got %+v", reclaimed)
	}
	if row := env.requireRow(t, env.cluster, sandboxID); row.State != StateRunning {
		t.Fatalf("the row must still be running: got %q", row.State)
	}
}

// The other half. A node that is still renewing evicts its own expired
// sandboxes — pausing them properly and publishing a fresh snapshot, which is
// strictly the better outcome. Stepping in front of that would rewind the
// sandbox to an older snapshot for no reason.
//
// Ported from an_expired_sandbox_stays_with_a_node_that_is_still_reporting.
func TestContractAnExpiredSandboxStaysWithANodeThatIsStillReporting(t *testing.T) {
	env := contractSetup(t)
	sandboxID := contractUUID(t)

	env.pauseAndPublish(t, env.cluster, sandboxID, contractNodeA)
	env.markRunning(t, env.cluster, sandboxID, contractNodeA)
	env.renew(t, env.cluster, contractNodeA, contractHeldDue(sandboxID, -time.Second))

	// No sleep: the lease this renewal just issued is still live.
	reclaimed := env.reclaim(t, env.cluster)
	if reclaimed.Released != 0 || reclaimed.Discarded != 0 {
		t.Fatalf("a reporting node must get to evict its own sandbox: got %+v", reclaimed)
	}
	if row := env.requireRow(t, env.cluster, sandboxID); row.State != StateRunning {
		t.Fatalf("the row must still be running: got %q", row.State)
	}
}

// A sandbox asked never to expire has no deadline to outlive, so no amount of
// silence makes it reclaimable. Same for a row whose holder has not renewed
// since the deadline column existed — an unknown deadline reads as no
// deadline, which is the safe direction.
//
// Ported from a_sandbox_with_no_deadline_is_never_reclaimed.
func TestContractASandboxWithNoDeadlineIsNeverReclaimed(t *testing.T) {
	env := contractSetup(t)
	sandboxID := contractUUID(t)

	env.pauseAndPublish(t, env.cluster, sandboxID, contractNodeA)
	env.markRunning(t, env.cluster, sandboxID, contractNodeA)
	if renewed := env.renew(t, env.cluster, contractNodeA, contractHeld(sandboxID)); renewed != 1 {
		t.Fatalf("a renewal without a deadline still renews the lease: renewed %d, want 1", renewed)
	}

	time.Sleep(contractPastLease)

	reclaimed := env.reclaim(t, env.cluster)
	if reclaimed.Released != 0 || reclaimed.Discarded != 0 {
		t.Fatalf("a sandbox with no deadline must never be reclaimed: got %+v", reclaimed)
	}
	if row := env.requireRow(t, env.cluster, sandboxID); row.State != StateRunning {
		t.Fatalf("the row must still be running: got %q", row.State)
	}
}

// Reclamation is scoped to live rows. Parked ones already have a mechanism —
// the lease lets another node take them over — and rewriting them here would
// bypass the NotReady redirect that keeps a still-publishing snapshot on its
// own node.
//
// Ported from reclamation_leaves_parked_rows_alone, with the renewal counts
// asserted as well: `paused` is deliberately absent from the renewal
// predicate, because that state means nobody is holding the sandbox.
func TestContractReclamationLeavesParkedRowsAlone(t *testing.T) {
	env := contractSetup(t)

	paused := contractUUID(t)
	env.pauseAndPublish(t, env.cluster, paused, contractNodeA)
	if renewed := env.renew(t, env.cluster, contractNodeA, contractHeldDue(paused, -time.Second)); renewed != 0 {
		t.Fatalf("a paused row has no holder, so nothing should renew it: renewed %d, want 0", renewed)
	}

	publishing := contractUUID(t)
	env.beginPause(t, env.cluster, publishing, contractNodeA)
	if renewed := env.renew(t, env.cluster, contractNodeA, contractHeldDue(publishing, -time.Second)); renewed != 1 {
		t.Fatalf("the node uploading a snapshot still holds the sandbox: renewed %d, want 1", renewed)
	}

	// The shape that actually tempts the predicate: parked, but *with* a
	// snapshot behind it. A pause that published once and then failed to
	// publish again leaves exactly this. Reclaiming it would mark it `paused`,
	// another node would claim it and rebuild from the older snapshot, and the
	// newer artifacts still sitting on the origin node would be thrown away —
	// all while the NotReady redirect that exists to prevent precisely that is
	// bypassed.
	localOnly := contractUUID(t)
	env.pauseAndPublish(t, env.cluster, localOnly, contractNodeA)
	second := env.beginPause(t, env.cluster, localOnly, contractNodeA)
	if err := env.store.MarkLocalOnly(context.Background(), env.cluster, localOnly, second.Generation); err != nil {
		t.Fatalf("the second publish never landed: %v", err)
	}
	if renewed := env.renew(t, env.cluster, contractNodeA, contractHeldDue(localOnly, -time.Second)); renewed != 1 {
		t.Fatalf("the origin of a local-only row still holds the sandbox: renewed %d, want 1", renewed)
	}

	time.Sleep(contractPastLease)

	reclaimed := env.reclaim(t, env.cluster)
	if reclaimed.Released != 0 || reclaimed.Discarded != 0 {
		t.Fatalf("parked rows are the lease's business, not reclamation's: got %+v", reclaimed)
	}
	if row := env.requireRow(t, env.cluster, publishing); row.State != StatePublishing {
		t.Fatalf("unexpected state for the publishing row: got %q", row.State)
	}
	if row := env.requireRow(t, env.cluster, localOnly); row.State != StateLocalOnly {
		t.Fatalf("a parked row with a snapshot must not be handed to the cluster behind its origin's back: got %q", row.State)
	}
	if row := env.requireRow(t, env.cluster, paused); row.State != StatePaused {
		t.Fatalf("unexpected state for the paused row: got %q", row.State)
	}
}

// An expired live row with nothing published behind it leaves nothing to
// rebuild, so it is deleted rather than parked — the same call reclamation's
// sibling makes, for the same reason.
//
// Ported from reclamation_discards_expired_rows_with_nothing_to_rebuild_from.
func TestContractReclamationDiscardsExpiredRowsWithNothingToRebuildFrom(t *testing.T) {
	env := contractSetup(t)
	sandboxID := contractUUID(t)

	began := env.beginPause(t, env.cluster, sandboxID, contractNodeA)
	if err := env.store.MarkLocalOnly(context.Background(), env.cluster, sandboxID, began.Generation); err != nil {
		t.Fatalf("the publish never landed: %v", err)
	}
	if confirmed := env.markRunning(t, env.cluster, sandboxID, contractNodeA); !confirmed {
		t.Fatal("the origin node resumed it locally and the cluster tracks it")
	}
	env.renew(t, env.cluster, contractNodeA, contractHeldDue(sandboxID, -time.Second))

	time.Sleep(contractPastLease)

	reclaimed := env.reclaim(t, env.cluster)
	if reclaimed.Released != 0 || reclaimed.Discarded != 1 {
		t.Fatalf("an expired row with nothing behind it is discarded: got %+v, want {Released:0 Discarded:1}", reclaimed)
	}

	env.requireNoRow(t, env.cluster, sandboxID)
}

// The deadline has to come from the holder, not from the row: metadata is
// whatever the sandbox looked like when it was paused, and a resume that set a
// longer timeout only exists in what the holder reports. Deriving it from the
// row would retire a sandbox that still had hours left.
//
// Ported from a_renewal_moves_the_deadline_the_row_is_judged_against.
func TestContractARenewalMovesTheDeadlineTheRowIsJudgedAgainst(t *testing.T) {
	env := contractSetup(t)
	sandboxID := contractUUID(t)

	env.pauseAndPublish(t, env.cluster, sandboxID, contractNodeA)
	env.markRunning(t, env.cluster, sandboxID, contractNodeA)
	env.renew(t, env.cluster, contractNodeA, contractHeldDue(sandboxID, -time.Second))
	// The sandbox's timeout is extended while it runs.
	env.renew(t, env.cluster, contractNodeA, contractHeldDue(sandboxID, time.Hour))

	time.Sleep(contractPastLease)

	reclaimed := env.reclaim(t, env.cluster)
	if reclaimed.Released != 0 || reclaimed.Discarded != 0 {
		t.Fatalf("an extended deadline must be what counts: got %+v", reclaimed)
	}
	if row := env.requireRow(t, env.cluster, sandboxID); row.State != StateRunning {
		t.Fatalf("the row must still be running: got %q", row.State)
	}
}

// 🔴 Not in the Rust suite, where the TTL is a property of the connection and
// every caller shares one. Here the caller reports its own, because the node
// is what renews and its configuration is what guarantees the TTL leaves room
// for two missed renewals — so a row must be stamped with the TTL its caller
// reported, not with this process's default.
//
// The failure this pins is silent: a store that ignores the caller's TTL and
// stamps its own shorter default lets rows lapse underneath a node that is
// renewing exactly as it was told to, and every parked row on that node is
// then taken over one default-TTL later.
func TestContractTheLeaseTTLTheCallerReportsIsWhatStampsTheRow(t *testing.T) {
	env := contractSetup(t)
	sandboxID := contractUUID(t)

	patient := env.store.WithLeaseTTL(time.Hour)

	env.pauseAndPublish(t, env.cluster, sandboxID, contractNodeA)
	if _, err := patient.BeginPause(context.Background(), BeginPauseInput{
		ClusterID:    env.cluster,
		SandboxID:    sandboxID,
		OriginNodeID: contractNodeA,
		Metadata:     contractMetadata(contractNodeA),
	}); err != nil {
		t.Fatalf("begin a pause reporting an hour-long lease: %v", err)
	}

	row := env.requireRow(t, env.cluster, sandboxID)
	if row.LeaseExpiresAt == nil {
		t.Fatal("expected the pause to stamp a lease")
	} else if remaining := time.Until(*row.LeaseExpiresAt); remaining < 30*time.Minute {
		t.Fatalf("the caller's TTL is what the row is stamped with: %s left, want about an hour", remaining)
	}

	// And the row is judged against that stamp, not against the default this
	// store was built with.
	time.Sleep(contractPastLease)
	claim := env.claim(t, env.cluster, sandboxID, contractNodeB)
	if claim.Outcome != ClaimOutcomeNotReady {
		t.Fatalf("a lease the caller reported as an hour long must still be live: got %q, want %q", claim.Outcome, ClaimOutcomeNotReady)
	}
}
