package scheduler

import (
	"context"
	"testing"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// The deadline contract, run against both stores
// ─────────────────────────────────────────────────────────────────────────────
//
// 🔴 One contract, two implementations, for the reason written at the top of
// binding_arbitration_test.go — and this axis has already drifted once in a way
// nothing here caught. The write-side switch reached the store as a TTL *value*
// the Service clamped to zero, and never as the branch the store takes: with
// the switch off, `KEEPTTL` went on firing on every heartbeat. Two consequences
// followed, and neither was visible in any test:
//
//   - a rollback could not shorten a record already installed with a long
//     budget, so the documented "back to binding_ttl within one heartbeat"
//     never happened; and
//   - a record installed at binding_ttl expired thirty seconds later while its
//     node was still reporting it every five, reinstalled only by the next
//     heartbeat — a healthy binding blinking out on the code default.
//
// Every test below therefore names both modes, and each of the three
// conditions in InMemoryBindingStore.keepsDeadline has one test that fails if
// that condition alone is removed.

type projectionStoreFactory func(t *testing.T, ttl time.Duration, projectionAuthoritative bool, arbitration string) BindingStore

func projectionStoreContract(t *testing.T, run func(t *testing.T, newStore projectionStoreFactory)) {
	t.Helper()

	t.Run("in-memory", func(t *testing.T) {
		run(t, func(t *testing.T, ttl time.Duration, projectionAuthoritative bool, arbitration string) BindingStore {
			return NewInMemoryBindingStoreWithModes(ttl, InMemoryArbitrationFor(arbitration), projectionAuthoritative)
		})
	})
	t.Run("redis", func(t *testing.T) {
		run(t, func(t *testing.T, ttl time.Duration, projectionAuthoritative bool, arbitration string) BindingStore {
			return newRedisBindingStoreWithModesForTest(t, ttl, RedisArbitrationFor(arbitration), projectionAuthoritative)
		})
	})
}

// remainingLife is how long a record has left, asked of either store in the one
// way both can answer. The in-memory store keeps an absolute deadline against
// an injected clock; Redis keeps a relative one against the real clock, and the
// tests below pass time.Now() to both so the two mean the same thing.
func remainingLife(t *testing.T, store BindingStore, sandboxID string, now time.Time) time.Duration {
	t.Helper()
	switch s := store.(type) {
	case *InMemoryBindingStore:
		s.mu.Lock()
		defer s.mu.Unlock()
		record, ok := s.bindings[sandboxID]
		if !ok {
			t.Fatalf("no record for %s", sandboxID)
		}
		return record.expiresAt.Sub(now)
	case *RedisBindingStore:
		return readPTTL(t, s, sandboxID)
	default:
		t.Fatalf("remainingLife does not know how to read %T", store)
		return 0
	}
}

func assertRemainingLife(t *testing.T, store BindingStore, sandboxID string, now time.Time, want time.Duration, tolerance time.Duration) {
	t.Helper()
	got := remainingLife(t, store, sandboxID, now)
	delta := got - want
	if delta < 0 {
		delta = -delta
	}
	if delta > tolerance {
		t.Fatalf("%s has %s left, want %s (+/- %s)", sandboxID, got, want, tolerance)
	}
}

// TestHeartbeatWithTheSwitchOffAlwaysSetsTheDeadline is the rollback, and the
// test that fails if the switch stops gating the branch.
//
// The sequence is the operational one from §8: a record was installed while the
// projection was authoritative, carrying a budget far longer than binding_ttl;
// the switch is then turned off and the deployment rolled. The very next
// heartbeat has to write the deadline again — that is what makes the rollback a
// rollback, and the reason it needs no rollback logic of its own is that the
// `else` branch *is* the previous behaviour.
func TestHeartbeatWithTheSwitchOffAlwaysSetsTheDeadline(t *testing.T) {
	projectionStoreContract(t, func(t *testing.T, newStore projectionStoreFactory) {
		store := newStore(t, 30*time.Second, false, "enforce")
		node := projectionTestNode()
		now := time.Now()

		mustReconcile(t, store, node, []RosterEntry{{SandboxID: "sbx-1", ExecutionID: execA, ProjectionTTL: 2 * time.Hour}}, now)
		assertRemainingLife(t, store, "sbx-1", now, 2*time.Hour, 5*time.Second)

		// 🔴 The same incarnation, reporting again with a shorter budget — which
		// is what a real roster looks like, since the budget is what remains of
		// the sandbox's life. With the projection authoritative this is a tick
		// and the deadline is left alone. With the switch off there is no such
		// thing as a tick: every accepted write sets a deadline.
		now = time.Now()
		mustReconcile(t, store, node, []RosterEntry{{SandboxID: "sbx-1", ExecutionID: execA, ProjectionTTL: time.Hour}}, now)
		assertRemainingLife(t, store, "sbx-1", now, time.Hour, 5*time.Second)
	})
}

// TestRollbackReturnsALongRecordToBindingTTL is the same rollback stated the
// way an operator reads it in §8: within one heartbeat of the switch going off,
// a 24-hour record is a 30-second record again.
//
// The heartbeat carries no budget because that is what an unswitched scheduler
// forwards — Service.resolveProjectionTTL zeroes every roster entry — so this is
// the exact pair of writes the cluster performs across a rollback.
func TestRollbackReturnsALongRecordToBindingTTL(t *testing.T) {
	projectionStoreContract(t, func(t *testing.T, newStore projectionStoreFactory) {
		store := newStore(t, 30*time.Second, false, "enforce")
		node := projectionTestNode()
		now := time.Now()

		// Installed by the process that ran before the switch was flipped off.
		if err := store.Record("sbx-1", Binding{Node: node, ExecutionID: execA, ProjectionTTL: 24 * time.Hour}, now); err != nil {
			t.Fatalf("record failed: %v", err)
		}
		assertRemainingLife(t, store, "sbx-1", now, 24*time.Hour, 5*time.Second)

		now = time.Now()
		mustReconcile(t, store, node, []RosterEntry{{SandboxID: "sbx-1", ExecutionID: execA}}, now)
		assertRemainingLife(t, store, "sbx-1", now, 30*time.Second, 5*time.Second)
	})
}

// TestHeartbeatWithTheSwitchOnKeepsTheDeadline is the branch itself: the tick
// that must not extend a record.
func TestHeartbeatWithTheSwitchOnKeepsTheDeadline(t *testing.T) {
	projectionStoreContract(t, func(t *testing.T, newStore projectionStoreFactory) {
		store := newStore(t, 30*time.Second, true, "enforce")
		node := projectionTestNode()
		now := time.Now()

		mustReconcile(t, store, node, []RosterEntry{{SandboxID: "sbx-1", ExecutionID: execA, ProjectionTTL: 2 * time.Hour}}, now)
		assertRemainingLife(t, store, "sbx-1", now, 2*time.Hour, 5*time.Second)

		// A shorter budget on the next tick changes nothing: the deadline the
		// record already has is the one it keeps.
		now = time.Now()
		mustReconcile(t, store, node, []RosterEntry{{SandboxID: "sbx-1", ExecutionID: execA, ProjectionTTL: time.Hour}}, now)
		assertRemainingLife(t, store, "sbx-1", now, 2*time.Hour, 5*time.Second)
	})
}

// TestHeartbeatKeepsTheDeadlineWithArbitrationOff is §6.5 under the other
// switch, and the test that fails if the rule goes back to reading the
// arbiter's decision.
//
// 🔴 With `execution_arbitration=off` the arbiter names no decision at all — it
// made none, and reporting one would be reporting a decision that was not
// taken. A deadline rule spelled `decision == "refreshed"` therefore stops
// running entirely in that mode, and what stops running is the guarantee that a
// heartbeat cannot extend a record. The rule asks about the writer and the
// record instead, which is a question no arbiter has to answer.
func TestHeartbeatKeepsTheDeadlineWithArbitrationOff(t *testing.T) {
	projectionStoreContract(t, func(t *testing.T, newStore projectionStoreFactory) {
		store := newStore(t, 30*time.Second, true, "off")
		node := projectionTestNode()
		now := time.Now()

		mustReconcile(t, store, node, []RosterEntry{{SandboxID: "sbx-1", ExecutionID: execA, ProjectionTTL: 2 * time.Hour}}, now)
		assertRemainingLife(t, store, "sbx-1", now, 2*time.Hour, 5*time.Second)

		now = time.Now()
		mustReconcile(t, store, node, []RosterEntry{{SandboxID: "sbx-1", ExecutionID: execA, ProjectionTTL: time.Hour}}, now)
		assertRemainingLife(t, store, "sbx-1", now, 2*time.Hour, 5*time.Second)
	})
}

// TestHeartbeatWithNoBudgetRefreshesTheDefaultDeadline is the third condition,
// and the one that keeps a healthy binding from blinking out.
//
// A writer that names no budget gets binding_ttl — 30 seconds, against a
// heartbeat every 5. If a tick from such a writer *kept* that deadline instead
// of writing it again, the record would expire thirty seconds after it was
// installed while its node was still reporting it, and be reinstalled only by
// the heartbeat after that. Measured on the in-memory store before this: a
// binding absent for 2.3s of every 60, with nothing wrong anywhere.
func TestHeartbeatWithNoBudgetRefreshesTheDefaultDeadline(t *testing.T) {
	projectionStoreContract(t, func(t *testing.T, newStore projectionStoreFactory) {
		store := newStore(t, 30*time.Second, true, "enforce")
		node := projectionTestNode()
		now := time.Now()

		// Installed with a long budget, so that "kept" and "written again" are
		// two visibly different numbers.
		mustReconcile(t, store, node, []RosterEntry{{SandboxID: "sbx-1", ExecutionID: execA, ProjectionTTL: 2 * time.Hour}}, now)

		// The node stops reporting a budget — an older build, or a scheduler
		// that has just had its switch turned off. Either way the deadline is
		// the store's own, refreshed on every tick exactly as it always was.
		now = time.Now()
		mustReconcile(t, store, node, []RosterEntry{{SandboxID: "sbx-1", ExecutionID: execA}}, now)
		assertRemainingLife(t, store, "sbx-1", now, 30*time.Second, 5*time.Second)
	})
}

// TestHeartbeatFromANewIncarnationSetsTheDeadline: a resume is a lifecycle
// event, not a tick, and arrives with a budget of its own.
func TestHeartbeatFromANewIncarnationSetsTheDeadline(t *testing.T) {
	projectionStoreContract(t, func(t *testing.T, newStore projectionStoreFactory) {
		store := newStore(t, 30*time.Second, true, "enforce")
		node := projectionTestNode()
		now := time.Now()

		mustReconcile(t, store, node, []RosterEntry{{SandboxID: "sbx-1", ExecutionID: execA, ProjectionTTL: 2 * time.Hour}}, now)

		now = time.Now()
		mustReconcile(t, store, node, []RosterEntry{{SandboxID: "sbx-1", ExecutionID: execB, ProjectionTTL: time.Hour}}, now)
		assertRemainingLife(t, store, "sbx-1", now, time.Hour, 5*time.Second)
	})
}

// TestRedisRefreshGivesADeadlineToARecordThatHasNone is Redis-only because it
// is a state only Redis can hold.
//
// 🔴 KEEPTTL on a key with no expiry keeps it having none, and so does every
// tick after it: one such key is a route nothing can retire, which is the
// failure §6.4's third rule exists to forbid. The in-memory store cannot
// reach this state — its record type has no way to spell "no deadline" — so
// there is nothing to assert there, and this is one of the two places the two
// implementations legitimately differ.
func TestRedisRefreshGivesADeadlineToARecordThatHasNone(t *testing.T) {
	store := newAuthoritativeRedisBindingStoreForTest(t, 30*time.Second)
	node := projectionTestNode()

	value, err := marshalRedisBindingRecord(node, execA)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	// Written with no expiry at all, the way a key restored from an RDB of an
	// older build or set by hand during an incident would be.
	if err := store.client.Set(context.Background(), store.bindingKey("sbx-1"), value, 0).Err(); err != nil {
		t.Fatalf("write an immortal record failed: %v", err)
	}

	now := time.Now()
	mustReconcile(t, store, node, []RosterEntry{{SandboxID: "sbx-1", ExecutionID: execA, ProjectionTTL: time.Hour}}, now)
	assertRemainingLife(t, store, "sbx-1", now, time.Hour, 5*time.Second)
}
