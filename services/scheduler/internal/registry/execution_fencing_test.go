package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

// expireLease pushes a row's lease into the past. Half of what reclamation
// needs; the other half is expireDeadline.
func (f *storeFixture) expireLease(sandboxID string) {
	f.t.Helper()
	if _, err := f.pool.Exec(context.Background(),
		`UPDATE paused_sandboxes SET lease_expires_at = now() - interval '1 hour' WHERE sandbox_id = $1::uuid`,
		sandboxID); err != nil {
		f.t.Fatalf("expire the lease on %s: %v", sandboxID, err)
	}
}

// expireDeadline pushes a row's own deadline into the past.
func (f *storeFixture) expireDeadline(sandboxID string) {
	f.t.Helper()
	if _, err := f.pool.Exec(context.Background(),
		`UPDATE paused_sandboxes SET sandbox_expires_at = now() - interval '1 hour' WHERE sandbox_id = $1::uuid`,
		sandboxID); err != nil {
		f.t.Fatalf("expire the deadline on %s: %v", sandboxID, err)
	}
}

// assertRowUnchanged compares every column, not just the state.
//
// 🔴 Comparing the state alone is not enough and the difference matters: a
// refusal that still touched updated_at or lease_expires_at has written to a
// row it was supposed to leave alone, which moves the clock every other
// decision about that row is made against. "Refused" has to mean no bytes.
func (f *storeFixture) assertRowUnchanged(sandboxID string, before rawRow) {
	f.t.Helper()

	after := f.raw(sandboxID)
	if after.found != before.found {
		f.t.Fatalf("row presence changed: before found=%v, after found=%v", before.found, after.found)
	}
	for _, column := range []struct {
		name          string
		before, after string
	}{
		{"state", before.state, after.state},
		{"generation", fmt.Sprint(before.generation), fmt.Sprint(after.generation)},
		{"origin_node_id", before.originNode, after.originNode},
		{"claimed_by_node_id", derefString(before.claimedBy), derefString(after.claimedBy)},
		{"snapshot_id", derefString(before.snapshotID), derefString(after.snapshotID)},
		{"paused_at", before.pausedAt.String(), after.pausedAt.String()},
		{"updated_at", before.updatedAt.String(), after.updatedAt.String()},
		{"lease_expires_at", derefTime(before.leaseExpires), derefTime(after.leaseExpires)},
		{"sandbox_expires_at", derefTime(before.sandboxExpiry), derefTime(after.sandboxExpiry)},
		{"execution_id", before.execution(), after.execution()},
		{"execution_started_at", derefTime(before.executionStart), derefTime(after.executionStart)},
	} {
		if column.before != column.after {
			f.t.Fatalf("the refused write still changed %s: %q -> %q", column.name, column.before, column.after)
		}
	}
}

func derefString(v *string) string {
	if v == nil {
		return "<null>"
	}
	return *v
}

func derefTime(v *time.Time) string {
	if v == nil {
		return "<null>"
	}
	return v.String()
}

// liveRow drives a sandbox all the way to `running` on node under a fresh
// incarnation, the way a real cross-node resume does, and returns that
// incarnation.
func (f *storeFixture) liveRow(sandboxID, node string) string {
	f.t.Helper()
	ctx := context.Background()

	began := f.beginPause(sandboxID, node)
	snapshot := snapshotUUID(1)
	if err := f.store.CompletePause(ctx, f.cluster, sandboxID, began.Generation, snapshot); err != nil {
		f.t.Fatalf("complete pause on %s: %v", sandboxID, err)
	}
	claim, err := f.store.ClaimForResume(ctx, f.cluster, sandboxID, node, f.nextExecutionFor(sandboxID))
	if err != nil || claim.Outcome != ClaimOutcomeClaimed {
		f.t.Fatalf("claim %s for %s: outcome %q err %v", sandboxID, node, claim.Outcome, err)
	}
	execution := f.executionFor(sandboxID)
	deadline := time.Now().Add(time.Hour)
	outcome, err := f.store.MarkRunning(ctx, f.cluster, sandboxID, node, execution, &deadline)
	if err != nil || outcome != MarkRunningAdopted {
		f.t.Fatalf("mark %s running on %s: outcome %q err %v", sandboxID, node, outcome, err)
	}
	return execution
}

// ─────────────────────────────────────────────────────────────────────────────
// A2 — the identity axis in the table
// ─────────────────────────────────────────────────────────────────────────────

// TestARunningRowCannotExistWithoutAnExecution — T-A2-2.
//
// 🔴 The one mechanical guarantee this release has. Every statement below can
// be got right today and wrong by whoever adds the next write point; the
// constraint is what makes that a refusal rather than a row that quietly opts
// out of fencing.
func TestARunningRowCannotExistWithoutAnExecution(t *testing.T) {
	f := newStoreFixture(t)

	for _, state := range []string{"running", "publishing", "resuming"} {
		t.Run(state, func(t *testing.T) {
			f.seedCleanup()
			_, err := f.pool.Exec(context.Background(), `INSERT INTO paused_sandboxes (
                sandbox_id, cluster_id, state, generation, origin_node_id, metadata,
                paused_at, updated_at, execution_id, execution_started_at
            ) VALUES ($1::uuid, $2::uuid, $3, 1, $4, '{}'::jsonb, now(), now(), NULL, NULL)`,
				sandboxUUID(60), f.cluster, state, stNodeA)
			if err == nil {
				t.Fatalf("a %s row was accepted with no incarnation; nothing then stops a second VM from claiming it", state)
			}
			if !strings.Contains(err.Error(), "23514") && !strings.Contains(err.Error(), "paused_sandboxes_execution_check") {
				t.Fatalf("refused, but not by the execution CHECK: %v", err)
			}
		})
	}
}

// TestAParkedRowCannotCarryAnExecution — T-A2-3, the other direction.
//
// 🔴 `paused` and `local_only` only. `resuming` is a live state under
// pre-allocation — a claim writes the incarnation the resume will run under —
// so using it here would be asserting the opposite of the design.
func TestAParkedRowCannotCarryAnExecution(t *testing.T) {
	f := newStoreFixture(t)

	for _, state := range []string{"paused", "local_only"} {
		t.Run(state, func(t *testing.T) {
			f.seedCleanup()
			_, err := f.pool.Exec(context.Background(), `INSERT INTO paused_sandboxes (
                sandbox_id, cluster_id, state, generation, origin_node_id, snapshot_id, metadata,
                paused_at, updated_at, execution_id, execution_started_at
            ) VALUES ($1::uuid, $2::uuid, $3, 1, $4, $5::uuid, '{}'::jsonb, now(), now(), $6::uuid, now())`,
				sandboxUUID(61), f.cluster, state, stNodeA, snapshotUUID(61), newExecutionID())
			if err == nil {
				t.Fatalf("a %s row was accepted while naming an incarnation; the old node's next pause would match it", state)
			}
			if !strings.Contains(err.Error(), "23514") && !strings.Contains(err.Error(), "paused_sandboxes_execution_check") {
				t.Fatalf("refused, but not by the execution CHECK: %v", err)
			}
		})
	}
}

// TestASnapshotDoesNotChangeTheExecution — T-A2-5.
//
// A pause is the same VM being stopped. The version axis moves; the identity
// axis does not, and that is the difference between the two columns stated as
// an assertion.
func TestASnapshotDoesNotChangeTheExecution(t *testing.T) {
	f := newStoreFixture(t)
	id := sandboxUUID(62)

	execution := f.liveRow(id, stNodeA)
	before := f.raw(id)

	f.beginPause(id, stNodeA)

	after := f.raw(id)
	if after.execution() != execution {
		t.Fatalf("the pause changed the incarnation: %s -> %s", execution, after.execution())
	}
	// 🔴 And it did not re-stamp the start time either. The predicate has
	// already established the two incarnations are the same one, so the columns
	// are left out of the update list entirely — a statement that wrote them
	// back would read as though a pause could change incarnations, and would
	// move the origin of a grace period that is supposed to date from when the
	// VM started.
	if derefTime(after.executionStart) != derefTime(before.executionStart) {
		t.Fatalf("the pause re-stamped execution_started_at: %s -> %s",
			derefTime(before.executionStart), derefTime(after.executionStart))
	}
	if after.generation != before.generation+1 {
		t.Fatalf("generation: got %d, want %d — one write, one version", after.generation, before.generation+1)
	}
	if after.state != "publishing" {
		t.Fatalf("state: %q", after.state)
	}
}

// TestReclaimClearsTheExecution — T-A2-6, and the precondition for T-A3-1.
func TestReclaimClearsTheExecution(t *testing.T) {
	f := newStoreFixture(t)
	id := sandboxUUID(63)

	f.liveRow(id, stNodeA)
	f.expireLease(id)
	f.expireDeadline(id)

	freed, err := f.store.ReclaimExpiredHoldings(context.Background(), f.cluster)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if freed.Released != 1 {
		t.Fatalf("released %d rows, want 1", freed.Released)
	}

	row := f.raw(id)
	if row.state != "paused" {
		t.Fatalf("state: %q, want paused", row.state)
	}
	if row.executionID != nil || row.executionStart != nil {
		t.Fatalf("reclaim left the incarnation on the row (%s); the node it took the sandbox from would pause straight back over it", row.execution())
	}
}

// TestReleaseNodeHoldingsClearsTheExecution — T-A2-7.
func TestReleaseNodeHoldingsClearsTheExecution(t *testing.T) {
	f := newStoreFixture(t)
	id := sandboxUUID(64)

	f.liveRow(id, stNodeA)

	freed, err := f.store.ReleaseNodeHoldings(context.Background(), f.cluster, stNodeA)
	if err != nil {
		t.Fatalf("release node holdings: %v", err)
	}
	if freed.Released != 1 {
		t.Fatalf("released %d rows, want 1", freed.Released)
	}

	row := f.raw(id)
	if row.executionID != nil || row.executionStart != nil {
		t.Fatalf("release left the incarnation on the row: %s", row.execution())
	}
}

// TestReleaseClaimClearsTheExecution — T-A2-10, the third seizure path.
//
// 🔴 Same family as the two above, and one of the three is not optional: a
// single path that leaves the incarnation behind puts the whole sequence back.
func TestReleaseClaimClearsTheExecution(t *testing.T) {
	f := newStoreFixture(t)
	ctx := context.Background()
	id := sandboxUUID(65)

	began := f.beginPause(id, stNodeA)
	if err := f.store.CompletePause(ctx, f.cluster, id, began.Generation, snapshotUUID(65)); err != nil {
		t.Fatalf("complete pause: %v", err)
	}
	claim, err := f.store.ClaimForResume(ctx, f.cluster, id, stNodeB, f.nextExecutionFor(id))
	if err != nil || claim.Outcome != ClaimOutcomeClaimed {
		t.Fatalf("claim: outcome %q err %v", claim.Outcome, err)
	}

	// The claim is where the incarnation is allocated, so the row is carrying
	// one right now. That is the pre-allocation half of the design, asserted
	// rather than assumed.
	claimed := f.raw(id)
	if claimed.executionID == nil {
		t.Fatal("a claim left the row with no incarnation; mark_running would then have nothing to check itself against")
	}
	if claimed.executionID != nil && *claimed.executionID != f.executionFor(id) {
		t.Fatalf("the claim installed %s, not the incarnation it was given (%s)", claimed.execution(), f.executionFor(id))
	}

	matched, err := f.store.ReleaseClaim(ctx, f.cluster, id, claim.Entry.Generation)
	if err != nil || !matched {
		t.Fatalf("release claim: matched=%v err=%v", matched, err)
	}

	row := f.raw(id)
	if row.state != "paused" {
		t.Fatalf("state: %q, want paused", row.state)
	}
	if row.claimedBy != nil {
		t.Fatalf("claimed_by: %q, want NULL", *row.claimedBy)
	}
	if row.executionID != nil || row.executionStart != nil {
		t.Fatalf("release_claim left the incarnation on the row (%s); the claimant's own next pause would match it", row.execution())
	}
}

// TestCompletePauseParksTheRowWithoutAnExecution — T-A2-8.
func TestCompletePauseParksTheRowWithoutAnExecution(t *testing.T) {
	f := newStoreFixture(t)
	ctx := context.Background()

	t.Run("complete_pause", func(t *testing.T) {
		id := sandboxUUID(66)
		began := f.beginPause(id, stNodeA)
		if err := f.store.CompletePause(ctx, f.cluster, id, began.Generation, snapshotUUID(66)); err != nil {
			t.Fatalf("complete pause: %v", err)
		}
		row := f.raw(id)
		if row.state != "paused" || row.executionID != nil || row.executionStart != nil {
			t.Fatalf("state %q incarnation %s, want paused with none", row.state, row.execution())
		}
	})

	t.Run("mark_local_only", func(t *testing.T) {
		id := sandboxUUID(67)
		began := f.beginPause(id, stNodeA)
		if err := f.store.MarkLocalOnly(ctx, f.cluster, id, began.Generation); err != nil {
			t.Fatalf("mark local only: %v", err)
		}
		row := f.raw(id)
		if row.state != "local_only" || row.executionID != nil || row.executionStart != nil {
			t.Fatalf("state %q incarnation %s, want local_only with none", row.state, row.execution())
		}
	})
}

// TestEveryReadPathCarriesTheExecution — T-A2-9 / §10.D.
//
// 🔴 One row, all four paths, in one test. The table is read through two
// separate column lists — the write store's entryColumns and the reader's
// selectColumns — and adding a column to one and not the other does not fail:
// it makes that column read as empty on whichever paths use the list that was
// missed. For this column, empty means an answer that fences nothing.
func TestEveryReadPathCarriesTheExecution(t *testing.T) {
	f := newStoreFixture(t)
	ctx := context.Background()
	id := sandboxUUID(68)

	execution := f.liveRow(id, stNodeA)

	t.Run("Store.Get", func(t *testing.T) {
		entry, found, err := f.store.Get(ctx, f.cluster, id)
		if err != nil || !found {
			t.Fatalf("get: found=%v err=%v", found, err)
		}
		if entry.ExecutionID != execution {
			t.Fatalf("Store.Get: got %q, want %q", entry.ExecutionID, execution)
		}
		if entry.ExecutionStartedAt == nil {
			t.Fatal("Store.Get: execution_started_at is nil; the grace period a later release measures has no origin")
		}
	})

	t.Run("Store.GetMany", func(t *testing.T) {
		rows, err := f.store.GetMany(ctx, f.cluster, []string{id})
		if err != nil {
			t.Fatalf("get many: %v", err)
		}
		entry, ok := rows.Entries[id]
		if !ok {
			t.Fatal("get many returned no row")
		}
		if entry.ExecutionID != execution {
			t.Fatalf("Store.GetMany: got %q, want %q", entry.ExecutionID, execution)
		}
	})

	// The reader is the other column list, and the one the routing half and
	// the read-only API are served from.
	reader := &PostgresReader{pool: f.pool, queryTimeout: 10 * time.Second}
	t.Run("Reader.Get", func(t *testing.T) {
		sandbox, found, err := reader.Get(ctx, id)
		if err != nil || !found {
			t.Fatalf("reader get: found=%v err=%v", found, err)
		}
		if sandbox.ExecutionID != execution {
			t.Fatalf("Reader.Get: got %q, want %q", sandbox.ExecutionID, execution)
		}
	})
	t.Run("Reader.List", func(t *testing.T) {
		listing, err := reader.List(ctx)
		if err != nil {
			t.Fatalf("reader list: %v", err)
		}
		for _, sandbox := range listing.Sandboxes {
			if sandbox.SandboxID != id {
				continue
			}
			if sandbox.ExecutionID != execution {
				t.Fatalf("Reader.List: got %q, want %q", sandbox.ExecutionID, execution)
			}
			return
		}
		t.Fatal("reader list did not return the row")
	})

	// And the claim's own RETURNING list, which is the copy the node reads
	// back and starts the VM under.
	t.Run("ClaimForResume entry", func(t *testing.T) {
		parked := sandboxUUID(69)
		began := f.beginPause(parked, stNodeA)
		if err := f.store.CompletePause(ctx, f.cluster, parked, began.Generation, snapshotUUID(69)); err != nil {
			t.Fatalf("complete pause: %v", err)
		}
		allocated := f.nextExecutionFor(parked)
		claim, err := f.store.ClaimForResume(ctx, f.cluster, parked, stNodeB, allocated)
		if err != nil || claim.Outcome != ClaimOutcomeClaimed {
			t.Fatalf("claim: outcome %q err %v", claim.Outcome, err)
		}
		if claim.Entry.ExecutionID != allocated {
			t.Fatalf("the granted claim reports %q, not the incarnation it allocated (%q); a node reading this back would start the VM under the wrong one and never be adopted",
				claim.Entry.ExecutionID, allocated)
		}
	})
}

// TestOnlyBeginPauseEverInsertsARow — §7's mechanical guarantee.
//
// The invariant that makes "a sandbox nobody paused cannot be duplicated" true:
// only a row authorises rebuilding a sandbox somewhere else, and only one
// statement creates rows.
func TestOnlyBeginPauseEverInsertsARow(t *testing.T) {
	f := newStoreFixture(t)
	ctx := context.Background()
	id := sandboxUUID(70)
	execution := newExecutionID()

	rowCount := func() int64 {
		t.Helper()
		var n int64
		if err := f.pool.QueryRow(ctx, `SELECT count(*) FROM paused_sandboxes`).Scan(&n); err != nil {
			t.Fatalf("count rows: %v", err)
		}
		return n
	}

	if _, err := f.store.MarkRunning(ctx, f.cluster, id, stNodeA, execution, nil); err != nil {
		t.Fatalf("mark running: %v", err)
	}
	if err := f.store.CompletePause(ctx, f.cluster, id, 1, snapshotUUID(70)); !errors.Is(err, ErrGenerationConflict) {
		t.Fatalf("complete pause on a sandbox with no row: %v", err)
	}
	if err := f.store.MarkLocalOnly(ctx, f.cluster, id, 1); !errors.Is(err, ErrGenerationConflict) {
		t.Fatalf("mark local only on a sandbox with no row: %v", err)
	}
	if claim, err := f.store.ClaimForResume(ctx, f.cluster, id, stNodeB, newExecutionID()); err != nil || claim.Outcome != ClaimOutcomeNotFound {
		t.Fatalf("claim on a sandbox with no row: outcome %q err %v", claim.Outcome, err)
	}
	if _, err := f.store.ReleaseClaim(ctx, f.cluster, id, 1); err != nil {
		t.Fatalf("release claim on a sandbox with no row: %v", err)
	}
	if _, err := f.store.RenewLease(ctx, f.cluster, stNodeA, []HeldSandbox{{SandboxID: id}}); err != nil {
		t.Fatalf("renew lease on a sandbox with no row: %v", err)
	}
	if _, err := f.store.Remove(ctx, f.cluster, id, 1); err != nil {
		t.Fatalf("remove a sandbox with no row: %v", err)
	}
	if _, err := f.store.ReclaimExpiredHoldings(ctx, f.cluster); err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if _, err := f.store.ReleaseNodeHoldings(ctx, f.cluster, stNodeA); err != nil {
		t.Fatalf("release node holdings: %v", err)
	}

	if n := rowCount(); n != 0 {
		t.Fatalf("%d rows exist for a sandbox that was never paused; a row is what authorises rebuilding it on another machine", n)
	}

	// 🟢 The control. Without it an implementation whose fixture cannot create
	// a row at all would pass the assertion above for the wrong reason.
	f.beginPause(id, stNodeA)
	if n := rowCount(); n != 1 {
		t.Fatalf("begin_pause produced %d rows, want 1 — the assertion above proves nothing if nothing here can create one", n)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// A3 — the fenced statements
// ─────────────────────────────────────────────────────────────────────────────

// TestAReclaimedSandboxCannotBePausedBackByItsOldNode — T-A3-1, the sequence
// this whole release exists for.
//
//	reclaim parks a live row and blanks it
//	  → node B claims it and brings it up under its own incarnation
//	    → node A comes back, its one-second eviction timer fires
//	      → A's begin_pause lands on the row
//
// Before fencing that last step matched, because the upsert compared only the
// cluster id — and what followed was A publishing a stale snapshot over B's and
// then deleting the snapshot B was running from.
func TestAReclaimedSandboxCannotBePausedBackByItsOldNode(t *testing.T) {
	f := newStoreFixture(t)
	ctx := context.Background()
	id := sandboxUUID(71)

	// ① A is running it.
	executionA := f.liveRow(id, stNodeA)

	// ② Its lease lapses and its deadline passes.
	f.expireLease(id)
	f.expireDeadline(id)

	// ③ The cluster reclaims it.
	if freed, err := f.store.ReclaimExpiredHoldings(ctx, f.cluster); err != nil || freed.Released != 1 {
		t.Fatalf("reclaim: released=%d err=%v", freed.Released, err)
	}
	if row := f.raw(id); row.executionID != nil {
		t.Fatalf("reclaim left A's incarnation on the row: %s", row.execution())
	}

	// ④ B takes it over and brings it up.
	executionB := f.nextExecutionFor(id)
	claim, err := f.store.ClaimForResume(ctx, f.cluster, id, stNodeB, executionB)
	if err != nil || claim.Outcome != ClaimOutcomeClaimed {
		t.Fatalf("B's claim: outcome %q err %v", claim.Outcome, err)
	}
	deadline := time.Now().Add(time.Hour)
	if outcome, err := f.store.MarkRunning(ctx, f.cluster, id, stNodeB, executionB, &deadline); err != nil || outcome != MarkRunningAdopted {
		t.Fatalf("B's mark_running: outcome %q err %v", outcome, err)
	}

	// ⑤ 🟢 The probe proves itself first. The same sandbox, the same
	// statement, only the incarnation changed: B's pause must go through. Run
	// before the assertion, because a refusal that would have arrived for any
	// input at all is not evidence of anything.
	//
	// It is B's row now, so this is also what the world is supposed to look
	// like — and it leaves the row `publishing` under B's incarnation, which is
	// still a row A has no business writing to.
	if _, err := f.store.BeginPause(ctx, BeginPauseInput{
		ClusterID:    f.cluster,
		SandboxID:    id,
		OriginNodeID: stNodeB,
		Metadata:     json.RawMessage(stMetadata),
		ExecutionID:  executionB,
	}); err != nil {
		t.Fatalf("the control input was refused too, so this test cannot tell fencing from a broken fixture: %v", err)
	}

	before := f.raw(id)

	// ⑥ A's automatic pause, one second after it came back.
	_, err = f.store.BeginPause(ctx, BeginPauseInput{
		ClusterID:    f.cluster,
		SandboxID:    id,
		OriginNodeID: stNodeA,
		Metadata:     json.RawMessage(stMetadata),
		ExecutionID:  executionA,
	})
	if !errors.Is(err, ErrExecutionFenced) {
		t.Fatalf("the superseded incarnation's pause was not fenced: %v", err)
	}
	// 🔴 Never a generation conflict: the node's handler for that one is to
	// re-read and try again, and re-reading here hands it B's generation.
	if errors.Is(err, ErrGenerationConflict) {
		t.Fatalf("fenced as a generation conflict, which the node retries around: %v", err)
	}

	f.assertRowUnchanged(id, before)
}

// TestARefusedPauseWritesNothing — T-A3-9.
//
// The same property as the last assertion above, isolated so it fails on its
// own terms: a predicate that had been moved into the SET list would leave the
// state alone and still stamp updated_at.
func TestARefusedPauseWritesNothing(t *testing.T) {
	f := newStoreFixture(t)
	id := sandboxUUID(72)

	f.liveRow(id, stNodeA)
	before := f.raw(id)

	_, err := f.store.BeginPause(context.Background(), BeginPauseInput{
		ClusterID:    f.cluster,
		SandboxID:    id,
		OriginNodeID: stNodeB,
		Metadata:     json.RawMessage(stMetadata),
		ExecutionID:  newExecutionID(),
	})
	if !errors.Is(err, ErrExecutionFenced) {
		t.Fatalf("expected ErrExecutionFenced, got %v", err)
	}
	f.assertRowUnchanged(id, before)
}

// TestAResumeCannotSlipInBetweenTheCheckAndTheWrite — T-A3-2.
//
// 🔴 What this rules out is not a bug in the statement but a shape of
// implementation: checking the incarnation in Go, before issuing an unguarded
// write. The interleaving is made deterministic with an external transaction
// holding the row lock, so neither side needs a sleep:
//
//	the real implementation blocks on the lock and evaluates its predicate
//	after the release, against the new incarnation ⇒ refused;
//
//	a Go-side check would not block at all — PostgreSQL's readers do not wait
//	on writers — so it would read the *old* incarnation, pass, and only then
//	block on its unguarded UPDATE, which would land on top of the new one.
//
// It is e2b's note about a lockless Add, in a different database.
func TestAResumeCannotSlipInBetweenTheCheckAndTheWrite(t *testing.T) {
	f := newStoreFixture(t)
	ctx := context.Background()
	id := sandboxUUID(73)

	executionA := f.liveRow(id, stNodeA)
	executionB := newExecutionID()

	tx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin the interleaving transaction: %v", err)
	}
	var locked int
	if err := tx.QueryRow(ctx, `SELECT 1 FROM paused_sandboxes WHERE sandbox_id = $1::uuid FOR UPDATE`, id).Scan(&locked); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("take the row lock: %v", err)
	}

	type pauseResult struct{ err error }
	results := make(chan pauseResult, 1)
	go func() {
		_, pauseErr := f.store.BeginPause(context.Background(), BeginPauseInput{
			ClusterID:    f.cluster,
			SandboxID:    id,
			OriginNodeID: stNodeA,
			Metadata:     json.RawMessage(stMetadata),
			ExecutionID:  executionA,
		})
		results <- pauseResult{err: pauseErr}
	}()

	// Wait until the pause is actually blocked on the lock rather than
	// racing it. Anything else here is a sleep pretending to be a
	// synchronisation point.
	waitForBlockedWriter(t, f, id)

	// A new incarnation is installed while the pause is waiting.
	if _, err := tx.Exec(ctx, `
        UPDATE paused_sandboxes
           SET state = 'running', origin_node_id = $2,
               execution_id = $3::uuid, execution_started_at = now(),
               generation = generation + 1, updated_at = now()
         WHERE sandbox_id = $1::uuid`, id, stNodeB, executionB); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("install the new incarnation: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit the interleaving transaction: %v", err)
	}

	select {
	case result := <-results:
		if !errors.Is(result.err, ErrExecutionFenced) {
			t.Fatalf("the pause that was in flight while a new incarnation took over was not fenced: %v", result.err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the blocked pause never returned")
	}

	row := f.raw(id)
	if row.state != "running" || row.originNode != stNodeB || row.execution() != executionB {
		t.Fatalf("the fenced pause overwrote the new incarnation: state=%q origin=%q execution=%s",
			row.state, row.originNode, row.execution())
	}
}

// waitForBlockedWriter blocks until some backend is waiting on a lock for this
// row. Reading pg_locks is the only way to know the writer is parked rather
// than merely slow.
func waitForBlockedWriter(t *testing.T, f *storeFixture, sandboxID string) {
	t.Helper()

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		var waiting int64
		if err := f.pool.QueryRow(context.Background(),
			`SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND state = 'active'`).Scan(&waiting); err != nil {
			t.Fatalf("inspect pg_stat_activity: %v", err)
		}
		if waiting > 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("no writer ever blocked on the row lock for %s; the interleaving this test needs did not happen", sandboxID)
}

// TestAStaleExecutionCannotStealARunningRow — T-A3-3, the hole R5 found.
//
// A `running` row carries a NULL claimed_by_node_id, so the old guard —
// "claimed_by_node_id IS NULL OR = me" — was true for every live row in the
// cluster.
func TestAStaleExecutionCannotStealARunningRow(t *testing.T) {
	f := newStoreFixture(t)
	ctx := context.Background()
	id := sandboxUUID(74)

	f.liveRow(id, stNodeB)
	before := f.raw(id)

	outcome, err := f.store.MarkRunning(ctx, f.cluster, id, stNodeA, newExecutionID(), nil)
	if err != nil {
		t.Fatalf("mark running: %v", err)
	}
	if outcome != MarkRunningHeldElsewhere {
		t.Fatalf("outcome %q, want %q — one data-plane request must not repoint a sandbox running elsewhere",
			outcome, MarkRunningHeldElsewhere)
	}
	f.assertRowUnchanged(id, before)
}

// TestTheSameExecutionCanReassertItself — T-A3-4.
//
// 🟢 The control for the test above. Without it an implementation that refused
// every `running` row would pass that one, and every retried mark_running in
// production would fail.
func TestTheSameExecutionCanReassertItself(t *testing.T) {
	f := newStoreFixture(t)
	ctx := context.Background()
	id := sandboxUUID(75)

	execution := f.liveRow(id, stNodeA)

	outcome, err := f.store.MarkRunning(ctx, f.cluster, id, stNodeA, execution, nil)
	if err != nil {
		t.Fatalf("a retried mark_running was refused: %v", err)
	}
	if outcome != MarkRunningAdopted {
		t.Fatalf("outcome %q, want %q — the same incarnation saying so twice is a retry, not a conflict", outcome, MarkRunningAdopted)
	}
}

// TestReassertingTheSameExecutionDoesNotMoveItsStart pins the half of branch ③
// that the retry outcome above does not reach.
//
// 🔴 execution_started_at exists for exactly one reason: updated_at is refreshed
// by every lease renewal, so it cannot be the origin of the grace period B3's
// KillOrphan measures. A branch ③ that re-stamped would put that drift straight
// back — a retried mark_running would restart the grace, and the one caller that
// retries hardest is the one that is going nowhere.
func TestReassertingTheSameExecutionDoesNotMoveItsStart(t *testing.T) {
	assertAReassertionDoesNotMoveTheStart(t, newStoreFixture(t), sandboxUUID(86))
}

// TestReassertingTheSameExecutionDoesNotMoveItsStartWithFencingOff is the same
// invariant with the switch in the other position, and it is not a duplicate.
//
// 🔴 What execution_started_at records is a property of the column, not a
// feature of fencing (adjudicated 2026-08-20). The two statements are separate
// constants — that is the whole design of the rollback switch — so an invariant
// that is only asserted against one of them is only true in one position of a
// switch an operator may flip mid-incident. If the start moved when fencing was
// off, the origin of the grace period B3 measures would depend on where the
// switch stood when the row was last written, and rows nobody touched
// afterwards would silently mean something else.
//
// The earlier reading — that a statement which cannot tell a retry from a
// takeover has no start time worth preserving — is recorded next to
// markRunningUnfencedSQL and was decided against: the condition needs no
// predicate to be decidable. `write_fencing` turns off whether a write is
// refused, never what a column records.
func TestReassertingTheSameExecutionDoesNotMoveItsStartWithFencingOff(t *testing.T) {
	assertAReassertionDoesNotMoveTheStart(t, newUnfencedStoreFixture(t), sandboxUUID(87))
}

// assertAReassertionDoesNotMoveTheStart drives one fixture through a retried
// mark_running and checks the incarnation's start time stayed where it was.
//
// Shared by both settings deliberately: one invariant, two statements, one set
// of assertions — so a change made to the fenced statement and forgotten for the
// other has somewhere to fail.
//
// The start time is pushed an hour into the past first, so a re-stamp moves it
// by an hour rather than by the microseconds two consecutive statements would
// otherwise differ by. Without that the assertion would be a coin toss on clock
// resolution, which is the same as no assertion.
func assertAReassertionDoesNotMoveTheStart(t *testing.T, f *storeFixture, id string) {
	t.Helper()
	ctx := context.Background()

	execution := f.liveRow(id, stNodeA)

	if _, err := f.pool.Exec(ctx,
		`UPDATE paused_sandboxes SET execution_started_at = now() - interval '1 hour' WHERE sandbox_id = $1::uuid`,
		id); err != nil {
		t.Fatalf("age the incarnation's start time: %v", err)
	}
	before := f.raw(id)

	outcome, err := f.store.MarkRunning(ctx, f.cluster, id, stNodeA, execution, nil)
	if err != nil {
		t.Fatalf("a retried mark_running was refused: %v", err)
	}
	if outcome != MarkRunningAdopted {
		t.Fatalf("outcome %q, want %q", outcome, MarkRunningAdopted)
	}

	after := f.raw(id)
	if derefTime(after.executionStart) != derefTime(before.executionStart) {
		t.Fatalf("the retry re-stamped execution_started_at: %s -> %s — the grace period a later orphan sweep measures now dates from the retry, not from the VM",
			derefTime(before.executionStart), derefTime(after.executionStart))
	}
	// The control. Without it an implementation that refused the retry outright,
	// or one that stopped writing the row at all, would pass the assertion above
	// for the wrong reason.
	if after.execution() != execution {
		t.Fatalf("the retry changed the incarnation: %s -> %s", execution, after.execution())
	}
	if after.generation != before.generation+1 {
		t.Fatalf("generation: got %d, want %d — the retry is still a write", after.generation, before.generation+1)
	}
	if !after.updatedAt.After(before.updatedAt) {
		t.Fatalf("updated_at did not move: %s -> %s; the row was not written, so the start time was preserved by accident",
			before.updatedAt, after.updatedAt)
	}
}

// TestAStaleExecutionOnThisNodeIsFencedRatherThanHeldElsewhere: the third
// answer mark_running now has.
//
// A node whose own row it is, quoting an incarnation the row has moved past,
// is not "somebody else holds it" — nobody else does. It is the caller being
// told it is dead, which is a different instruction: stop, do not retry.
func TestAStaleExecutionOnThisNodeIsFencedRatherThanHeldElsewhere(t *testing.T) {
	f := newStoreFixture(t)
	ctx := context.Background()
	id := sandboxUUID(76)

	f.liveRow(id, stNodeA)
	before := f.raw(id)

	stale := newExecutionID()
	outcome, err := f.store.MarkRunning(ctx, f.cluster, id, stNodeA, stale, nil)
	if !errors.Is(err, ErrExecutionFenced) {
		t.Fatalf("a superseded incarnation on this very node was not fenced: outcome %q err %v", outcome, err)
	}
	if errors.Is(err, ErrGenerationConflict) {
		t.Fatalf("fenced as a generation conflict, which the node retries around: %v", err)
	}
	f.assertRowUnchanged(id, before)
}

// TestTwoRefusalsAreToldApart — T-A3-5.
//
// 🔴 The two errors leave the table equally unchanged and call for opposite
// responses. A build that wrapped one in the other would look correct in every
// test that asserts "the write was refused", and would send a node into a
// re-read loop that walks straight around the fence.
func TestTwoRefusalsAreToldApart(t *testing.T) {
	f := newStoreFixture(t)
	ctx := context.Background()
	id := sandboxUUID(77)

	// (a) The version axis: the right incarnation, a generation that has moved.
	began := f.beginPause(id, stNodeA)
	stale := f.store.CompletePause(ctx, f.cluster, id, began.Generation+7, snapshotUUID(77))
	if !errors.Is(stale, ErrGenerationConflict) {
		t.Fatalf("a stale generation was not a generation conflict: %v", stale)
	}
	if errors.Is(stale, ErrExecutionFenced) {
		t.Fatalf("a stale generation was also reported as fenced, so a caller cannot tell whether to retry: %v", stale)
	}

	// (b) The identity axis: the current generation, an incarnation that has moved.
	_, fenced := f.store.BeginPause(ctx, BeginPauseInput{
		ClusterID:    f.cluster,
		SandboxID:    id,
		OriginNodeID: stNodeA,
		Metadata:     json.RawMessage(stMetadata),
		ExecutionID:  newExecutionID(),
	})
	if !errors.Is(fenced, ErrExecutionFenced) {
		t.Fatalf("a superseded incarnation was not fenced: %v", fenced)
	}
	if errors.Is(fenced, ErrGenerationConflict) {
		t.Fatalf("a fenced write was also reported as a generation conflict, which the node retries around: %v", fenced)
	}
}

// TestMarkLocalOnlyRefusesAStaleGeneration and
// TestReleaseClaimRefusesAStaleGeneration cover the two conditional writes
// this release touches that had no stale-generation case of their own —
// "matched nothing" was tested, "the caller's version has moved" was not.
func TestMarkLocalOnlyRefusesAStaleGeneration(t *testing.T) {
	f := newStoreFixture(t)
	ctx := context.Background()
	id := sandboxUUID(78)

	began := f.beginPause(id, stNodeA)
	before := f.raw(id)

	err := f.store.MarkLocalOnly(ctx, f.cluster, id, began.Generation+1)
	if !errors.Is(err, ErrGenerationConflict) {
		t.Fatalf("expected a generation conflict, got %v", err)
	}
	f.assertRowUnchanged(id, before)
}

func TestReleaseClaimRefusesAStaleGeneration(t *testing.T) {
	f := newStoreFixture(t)
	ctx := context.Background()
	id := sandboxUUID(79)

	began := f.beginPause(id, stNodeA)
	if err := f.store.CompletePause(ctx, f.cluster, id, began.Generation, snapshotUUID(79)); err != nil {
		t.Fatalf("complete pause: %v", err)
	}
	claim, err := f.store.ClaimForResume(ctx, f.cluster, id, stNodeB, f.nextExecutionFor(id))
	if err != nil || claim.Outcome != ClaimOutcomeClaimed {
		t.Fatalf("claim: outcome %q err %v", claim.Outcome, err)
	}
	before := f.raw(id)

	// Zero rows is a success here — the row this caller meant to release has
	// moved on — but it must not have released anything.
	matched, err := f.store.ReleaseClaim(ctx, f.cluster, id, claim.Entry.Generation+1)
	if err != nil {
		t.Fatalf("release claim: %v", err)
	}
	if matched {
		t.Fatal("a stale generation released a claim it no longer held")
	}
	f.assertRowUnchanged(id, before)
}

// TestTheVersionAxisMovesExactlyOncePerWrite is the only mechanical statement
// of what separates the two columns: the version moves on every write, the
// identity moves only when a VM is born.
func TestTheVersionAxisMovesExactlyOncePerWrite(t *testing.T) {
	f := newStoreFixture(t)
	ctx := context.Background()
	id := sandboxUUID(80)

	step := func(name string, wantGeneration int64, wantExecutionChanged bool, previous string, do func()) string {
		t.Helper()
		do()
		row := f.raw(id)
		if row.generation != wantGeneration {
			t.Fatalf("%s: generation %d, want %d", name, row.generation, wantGeneration)
		}
		changed := row.execution() != previous
		if changed != wantExecutionChanged {
			t.Fatalf("%s: incarnation %s (previously %s), changed=%v want changed=%v",
				name, row.execution(), previous, changed, wantExecutionChanged)
		}
		return row.execution()
	}

	var began BeganPause
	execution := step("begin_pause (new row)", 1, true, "none", func() {
		began = f.beginPause(id, stNodeA)
	})
	execution = step("complete_pause", 1, true, execution, func() {
		if err := f.store.CompletePause(ctx, f.cluster, id, began.Generation, snapshotUUID(80)); err != nil {
			t.Fatalf("complete pause: %v", err)
		}
	})
	if execution != "none" {
		t.Fatalf("a parked row still names an incarnation: %s", execution)
	}

	var claim ResumeClaim
	claimed := f.nextExecutionFor(id)
	execution = step("claim_for_resume", 2, true, execution, func() {
		var err error
		claim, err = f.store.ClaimForResume(ctx, f.cluster, id, stNodeB, claimed)
		if err != nil || claim.Outcome != ClaimOutcomeClaimed {
			t.Fatalf("claim: outcome %q err %v", claim.Outcome, err)
		}
	})
	if execution != claimed {
		t.Fatalf("the claim installed %s, not %s", execution, claimed)
	}

	// 🔴 The one every reading of "an incarnation per resume" gets wrong: the
	// generation moves again here, and the incarnation does not. Collapsing the
	// two columns would make this step contradict itself.
	execution = step("mark_running", 3, false, execution, func() {
		if outcome, err := f.store.MarkRunning(ctx, f.cluster, id, stNodeB, claimed, nil); err != nil || outcome != MarkRunningAdopted {
			t.Fatalf("mark running: outcome %q err %v", outcome, err)
		}
	})

	step("begin_pause (same VM stopping)", 4, false, execution, func() {
		f.beginPause(id, stNodeB)
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// The rollback switch
// ─────────────────────────────────────────────────────────────────────────────

// TestWriteFencingOffRestoresTheOldPredicates.
//
// 🔴 The point of two statement constants rather than one with a flag in it:
// the "off" behaviour is a code path of its own and gets a test of its own. It
// is also the assertion that the rollback really is a rollback — an operator
// reaching for this setting during an incident needs the old behaviour, not a
// third one.
func TestWriteFencingOffRestoresTheOldPredicates(t *testing.T) {
	f := newUnfencedStoreFixture(t)
	ctx := context.Background()
	id := sandboxUUID(81)

	execution := f.liveRow(id, stNodeA)

	// A superseded incarnation's pause goes through again, which is exactly
	// what switching this off means and exactly why it is loud.
	if _, err := f.store.BeginPause(ctx, BeginPauseInput{
		ClusterID:    f.cluster,
		SandboxID:    id,
		OriginNodeID: stNodeB,
		Metadata:     json.RawMessage(stMetadata),
		ExecutionID:  newExecutionID(),
	}); err != nil {
		t.Fatalf("with fencing off, an unrelated incarnation's pause must behave as it did before: %v", err)
	}

	// 🔴 And the column is still maintained. The CHECK constraint is DDL and
	// does not follow the setting, so a rollback that stopped writing the
	// column would fail every pause with 23514 instead of restoring anything.
	row := f.raw(id)
	if row.state != "publishing" {
		t.Fatalf("state: %q, want publishing", row.state)
	}
	if row.executionID == nil {
		t.Fatal("with fencing off the row was left with no incarnation, which the CHECK constraint forbids")
	}
	if row.execution() == execution {
		t.Fatalf("the unfenced upsert kept the old incarnation %s; with no predicate to make them equal it has to install the caller's", execution)
	}
}

// TestFencingOffStillRequiresAnExecutionID states the boundary in the other
// direction: the setting turns the checking off, never the field.
func TestFencingOffStillRequiresAnExecutionID(t *testing.T) {
	f := newUnfencedStoreFixture(t)

	_, err := f.store.BeginPause(context.Background(), BeginPauseInput{
		ClusterID:    f.cluster,
		SandboxID:    sandboxUUID(82),
		OriginNodeID: stNodeA,
		Metadata:     json.RawMessage(stMetadata),
	})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("expected the missing incarnation to be refused as a bad argument, got %v", err)
	}
}

// TestAnExecutionIDMustBeAUUID: shape is checked in the store, next to the
// other two id columns, so "not a uuid" and "not the right uuid" fail in the
// same layer and carry the same code.
func TestAnExecutionIDMustBeAUUID(t *testing.T) {
	f := newStoreFixture(t)

	for name, value := range map[string]string{
		"empty":        "",
		"not a uuid":   "the-current-one",
		"unhyphenated": "0000000100007000800000000000000a",
		"truncated":    "00000001-0000-7000-8000",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := f.store.BeginPause(context.Background(), BeginPauseInput{
				ClusterID:    f.cluster,
				SandboxID:    sandboxUUID(83),
				OriginNodeID: stNodeA,
				Metadata:     json.RawMessage(stMetadata),
				ExecutionID:  value,
			})
			if !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("expected ErrInvalidArgument, got %v", err)
			}
		})
	}
}

// TestAnUpperCaseExecutionIDIsNormalised.
//
// 🔴 Not cosmetic. The routing half decides which of two incarnations is newer
// by ordering them as strings, and in ASCII '0'-'9' < 'A'-'F' < 'a'-'f' — one
// upper-case id reverses that order, and the older VM wins.
func TestAnUpperCaseExecutionIDIsNormalised(t *testing.T) {
	f := newStoreFixture(t)
	ctx := context.Background()
	id := sandboxUUID(84)

	upper := "00000001-0000-7000-8000-0000000000AB"
	if _, err := f.store.BeginPause(ctx, BeginPauseInput{
		ClusterID:    f.cluster,
		SandboxID:    id,
		OriginNodeID: stNodeA,
		Metadata:     json.RawMessage(stMetadata),
		ExecutionID:  upper,
	}); err != nil {
		t.Fatalf("begin pause: %v", err)
	}

	entry, found, err := f.store.Get(ctx, f.cluster, id)
	if err != nil || !found {
		t.Fatalf("get: found=%v err=%v", found, err)
	}
	if entry.ExecutionID != strings.ToLower(upper) {
		t.Fatalf("the incarnation came back as %q, want the lower-case canonical %q", entry.ExecutionID, strings.ToLower(upper))
	}
	// And the row still recognises the caller under either spelling: the
	// column is a uuid, so PostgreSQL compares values rather than text.
	if _, err := f.store.BeginPause(ctx, BeginPauseInput{
		ClusterID:    f.cluster,
		SandboxID:    id,
		OriginNodeID: stNodeA,
		Metadata:     json.RawMessage(stMetadata),
		ExecutionID:  strings.ToLower(upper),
	}); err != nil {
		t.Fatalf("the same incarnation spelled in lower case was fenced: %v", err)
	}
}

// TestMigrateRefusesAParkedRowCarryingAnExecution is the preflight's other
// direction, which the "count the live rows with no incarnation" phrasing
// would have missed. A table rolled forward, back and forward again can hold
// either shape.
func TestMigrateRefusesAParkedRowCarryingAnExecution(t *testing.T) {
	f := newStoreFixture(t)
	ctx := context.Background()

	// Reached by dropping the constraint first, which is what a rollback to a
	// build without it leaves behind.
	if _, err := f.pool.Exec(ctx, `ALTER TABLE paused_sandboxes DROP CONSTRAINT paused_sandboxes_execution_check`); err != nil {
		t.Fatalf("drop the constraint: %v", err)
	}
	if _, err := f.pool.Exec(ctx, `INSERT INTO paused_sandboxes (
        sandbox_id, cluster_id, state, generation, origin_node_id, snapshot_id, metadata,
        paused_at, updated_at, execution_id, execution_started_at
    ) VALUES ($1::uuid, $2::uuid, 'paused', 1, $3, $4::uuid, '{}'::jsonb, now(), now(), $5::uuid, now())`,
		sandboxUUID(85), f.cluster, stNodeA, snapshotUUID(85), newExecutionID()); err != nil {
		t.Fatalf("seed a parked row carrying an incarnation: %v", err)
	}

	err := migrateRetryingDeadlock(func() error { return Migrate(ctx, f.pool) })
	if err == nil {
		t.Fatal("migrating over a parked row that names an incarnation succeeded; that row is one the old holder's next pause matches")
	}
	if !strings.Contains(err.Error(), "DROP TABLE") {
		t.Fatalf("the refusal does not name the command that fixes it: %v", err)
	}
}
