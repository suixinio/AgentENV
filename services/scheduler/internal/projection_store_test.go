package scheduler

import (
	"testing"
	"time"

	"agentenv/services/shared/routing"
)

const (
	execA = "0198b7cc-1111-7000-8000-000000000001"
	execB = "0198b7cc-2222-7000-8000-000000000002"
)

func projectionTestNode() Node {
	return Node{ID: "node-a", Endpoint: "http://node-a"}
}

// newAuthoritativeInMemoryBindingStore is the in-memory store as a scheduler
// running with SCHEDULER_ROUTING_PROJECTION_AUTHORITATIVE=on builds it. Its
// Redis twin is newAuthoritativeRedisBindingStoreForTest, and anything
// asserting on a deadline a heartbeat left alone has to use one of the two:
// with the switch off there is no branch that could leave one alone.
func newAuthoritativeInMemoryBindingStore(bindingTTL time.Duration) *InMemoryBindingStore {
	return NewInMemoryBindingStoreWithModes(bindingTTL, arbitrateFenced, true)
}

// TestInMemoryDeleteGuardRules walks the four outcomes. Two of them are
// deliberately not what a delete-by-id would do, and those are the two the
// comments on BindingDeleteOutcome argue for.
func TestInMemoryDeleteGuardRules(t *testing.T) {
	now := time.Now()

	t.Run("absent is a clean no-op", func(t *testing.T) {
		store := NewInMemoryBindingStore(30 * time.Second)
		outcome, err := store.Delete("sbx-1", execA, now)
		if err != nil || outcome != BindingDeleteAbsent {
			t.Fatalf("got (%v, %v), want noop_absent", outcome, err)
		}
	})

	t.Run("same incarnation deletes", func(t *testing.T) {
		store := NewInMemoryBindingStore(30 * time.Second)
		mustRecord(t, store, "sbx-1", Binding{Node: projectionTestNode(), ExecutionID: execA}, now)

		outcome, err := store.Delete("sbx-1", execA, now)
		if err != nil || outcome != BindingDeleteDeleted {
			t.Fatalf("got (%v, %v), want deleted", outcome, err)
		}
		if _, ok, _ := store.Get("sbx-1", now); ok {
			t.Fatal("the record survived a delete it should have matched")
		}
		if len(store.nodeBinding[projectionTestNode().ID]) != 0 {
			t.Fatal("the reverse index kept a member pointing at a deleted record")
		}
	})

	// 🔴 The guard. This is the whole reason the event carries an incarnation:
	// a pause event that arrives late, for a sandbox id that has since been
	// resumed elsewhere, must not tear down the live record.
	t.Run("a different incarnation is refused", func(t *testing.T) {
		store := NewInMemoryBindingStore(30 * time.Second)
		mustRecord(t, store, "sbx-1", Binding{Node: projectionTestNode(), ExecutionID: execB}, now)

		outcome, err := store.Delete("sbx-1", execA, now)
		if err != nil || outcome != BindingDeleteRejectedStale {
			t.Fatalf("got (%v, %v), want rejected_stale", outcome, err)
		}
		if _, ok, _ := store.Get("sbx-1", now); !ok {
			t.Fatal("a stale event deleted the live record")
		}
	})

	// 🔴 Deliberately the opposite of what the write path does with an empty
	// incumbent. Empty means "unknown", not "somebody else's" — and refusing
	// here would leave a create-then-delete pair behind as a long-lived record
	// pointing at a sandbox that no longer exists.
	t.Run("an unknown incumbent is deleted, not protected", func(t *testing.T) {
		store := NewInMemoryBindingStore(30 * time.Second)
		mustRecord(t, store, "sbx-1", Binding{Node: projectionTestNode()}, now)

		outcome, err := store.Delete("sbx-1", execA, now)
		if err != nil || outcome != BindingDeleteUnknownIncumbent {
			t.Fatalf("got (%v, %v), want deleted_unknown_incumbent", outcome, err)
		}
		if _, ok, _ := store.Get("sbx-1", now); ok {
			t.Fatal("a record naming no incarnation survived a named delete")
		}
	})

	t.Run("an expired record is absent", func(t *testing.T) {
		store := NewInMemoryBindingStore(30 * time.Second)
		mustRecord(t, store, "sbx-1", Binding{Node: projectionTestNode(), ExecutionID: execA}, now)

		outcome, err := store.Delete("sbx-1", execA, now.Add(time.Minute))
		if err != nil || outcome != BindingDeleteAbsent {
			t.Fatalf("got (%v, %v), want noop_absent", outcome, err)
		}
	})

	t.Run("a blank sandbox id is a no-op", func(t *testing.T) {
		store := NewInMemoryBindingStore(30 * time.Second)
		if outcome, err := store.Delete("   ", execA, now); err != nil || outcome != BindingDeleteAbsent {
			t.Fatalf("got (%v, %v), want noop_absent", outcome, err)
		}
	})

	// The twin of the Redis case of the same name. A caller must never reach
	// either store without an incarnation — that is the unguarded delete the
	// guard exists to prevent — and neither store may rely on the other's
	// caller to be the one that stops it.
	t.Run("a blank incarnation is refused", func(t *testing.T) {
		store := NewInMemoryBindingStore(30 * time.Second)
		mustRecord(t, store, "sbx-1", Binding{Node: projectionTestNode()}, now)

		if outcome, err := store.Delete("sbx-1", "  ", now); err != nil || outcome != BindingDeleteAbsent {
			t.Fatalf("got (%v, %v), want a refused no-op", outcome, err)
		}
		if _, ok, _ := store.Get("sbx-1", now); !ok {
			t.Fatal("a delete naming no incarnation removed a record anyway")
		}
	})
}

// TestInMemoryRecordUsesTheNodeBudget covers the create path's TTL: the node's
// own number when it sent one, and the store's default when it did not.
func TestInMemoryRecordUsesTheNodeBudget(t *testing.T) {
	now := time.Now()
	store := NewInMemoryBindingStore(30 * time.Second)

	mustRecord(t, store, "sbx-budget", Binding{
		Node:          projectionTestNode(),
		ExecutionID:   execA,
		ProjectionTTL: time.Hour,
	}, now)
	assertExpiry(t, store, "sbx-budget", now.Add(time.Hour))

	// 🔴 No budget is the store's default, never "no expiry".
	mustRecord(t, store, "sbx-default", Binding{Node: projectionTestNode(), ExecutionID: execA}, now)
	assertExpiry(t, store, "sbx-default", now.Add(30*time.Second))
}

// TestInMemoryHeartbeatRefreshDoesNotExtendTheDeadline is the in-memory twin of
// the Lua KEEPTTL branch, and it is the single assertion that decides whether
// any of this stage works.
//
// A heartbeat arrives every five seconds and reports every sandbox the node
// holds. If each of those rewrote the record's deadline, the projection's
// survival would still depend on a periodic write path — which is the exact
// dependency the long TTL exists to remove, and its absence is what lets the
// data plane outlive a stopped scheduler.
func TestInMemoryHeartbeatRefreshDoesNotExtendTheDeadline(t *testing.T) {
	start := time.Now()
	store := newAuthoritativeInMemoryBindingStore(30 * time.Second)
	node := projectionTestNode()

	roster := []RosterEntry{{SandboxID: "sbx-1", ExecutionID: execA, ProjectionTTL: time.Hour}}
	if err := store.ReconcileNode(node, roster, start); err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	assertExpiry(t, store, "sbx-1", start.Add(time.Hour))

	// Six heartbeats later — the same incarnation each time.
	for i := 1; i <= 6; i++ {
		if err := store.ReconcileNode(node, roster, start.Add(time.Duration(i)*5*time.Second)); err != nil {
			t.Fatalf("reconcile %d failed: %v", i, err)
		}
	}
	assertExpiry(t, store, "sbx-1", start.Add(time.Hour))
}

// TestInMemoryHeartbeatRepairSetsTheDeadline is the other side of the same
// branch: installing a record that was lost, and a new incarnation arriving
// with a new budget, are lifecycle events rather than ticks and both set the
// deadline.
func TestInMemoryHeartbeatRepairSetsTheDeadline(t *testing.T) {
	start := time.Now()
	store := newAuthoritativeInMemoryBindingStore(30 * time.Second)
	node := projectionTestNode()

	// Installed from nothing.
	repair := start.Add(2 * time.Minute)
	if err := store.ReconcileNode(node, []RosterEntry{{SandboxID: "sbx-1", ExecutionID: execA, ProjectionTTL: time.Hour}}, repair); err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	assertExpiry(t, store, "sbx-1", repair.Add(time.Hour))

	// Superseded by a newer incarnation carrying a different budget.
	resume := repair.Add(time.Minute)
	if err := store.ReconcileNode(node, []RosterEntry{{SandboxID: "sbx-1", ExecutionID: execB, ProjectionTTL: 2 * time.Hour}}, resume); err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	assertExpiry(t, store, "sbx-1", resume.Add(2*time.Hour))
}

// TestInMemoryHeartbeatRefreshStillRewritesTheRecord: KEEPTTL keeps the
// deadline, not the contents. A node whose endpoint or pod name changed still
// has to land in the record.
func TestInMemoryHeartbeatRefreshStillRewritesTheRecord(t *testing.T) {
	start := time.Now()
	store := newAuthoritativeInMemoryBindingStore(30 * time.Second)

	roster := []RosterEntry{{SandboxID: "sbx-1", ExecutionID: execA, ProjectionTTL: time.Hour}}
	if err := store.ReconcileNode(Node{ID: "node-a", Endpoint: "http://old"}, roster, start); err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	if err := store.ReconcileNode(Node{ID: "node-a", Endpoint: "http://new"}, roster, start.Add(5*time.Second)); err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	binding, ok, err := store.Get("sbx-1", start.Add(5*time.Second))
	if err != nil || !ok {
		t.Fatalf("expected a hit, got (%v, %v)", ok, err)
	}
	if binding.Node.Endpoint != "http://new" {
		t.Fatalf("endpoint = %q, want the refreshed one", binding.Node.Endpoint)
	}
	assertExpiry(t, store, "sbx-1", start.Add(time.Hour))
}

// TestInMemoryNonPositiveBudgetNeverMeansForever is the rule stated three times
// in the spec and once in a shipped bug elsewhere: a value that is not a
// positive number of seconds falls back to the store's default.
func TestInMemoryNonPositiveBudgetNeverMeansForever(t *testing.T) {
	now := time.Now()
	for _, ttl := range []time.Duration{0, -time.Second, -time.Hour} {
		store := NewInMemoryBindingStore(30 * time.Second)
		mustRecord(t, store, "sbx-1", Binding{Node: projectionTestNode(), ProjectionTTL: ttl}, now)
		assertExpiry(t, store, "sbx-1", now.Add(30*time.Second))
	}
}

// 🔴 Compile-time, not a test. The scheduler's Node and the shared record's
// Node must remain one type — an alias, not two structs that happen to match —
// or the gateway decodes a shape this never writes. A runtime assertion could
// only ever confirm that the assignment on the line above it compiled, which is
// what the compiler already said; this is that same statement with nothing
// around it pretending to check.
var (
	_ routing.Node = Node{}
	_ Node         = routing.Node{}
)

func mustRecord(t *testing.T, store *InMemoryBindingStore, sandboxID string, binding Binding, now time.Time) {
	t.Helper()
	if err := store.Record(sandboxID, binding, now); err != nil {
		t.Fatalf("record %s failed: %v", sandboxID, err)
	}
}

func assertExpiry(t *testing.T, store *InMemoryBindingStore, sandboxID string, want time.Time) {
	t.Helper()
	store.mu.Lock()
	record, ok := store.bindings[sandboxID]
	store.mu.Unlock()
	if !ok {
		t.Fatalf("no record for %s", sandboxID)
	}
	if !record.expiresAt.Equal(want) {
		t.Fatalf("%s expires at %s, want %s (delta %s)", sandboxID, record.expiresAt, want, record.expiresAt.Sub(want))
	}
}
