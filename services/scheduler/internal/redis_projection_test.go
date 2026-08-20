package scheduler

import (
	"context"
	"testing"
	"time"
)

// TestRedisDeleteGuardRules is the Lua twin of TestInMemoryDeleteGuardRules.
//
// 🔴 Both exist, and neither substitutes for the other. The in-memory store is
// what `make test` runs by default; the Redis one is what every HA deployment
// runs. A rule implemented for one and forgotten for the other passes every
// test locally and shows up in production as a guard that quietly does nothing.
func TestRedisDeleteGuardRules(t *testing.T) {
	store := newRedisBindingStoreForTest(t, 30*time.Second)
	node := Node{ID: "node-a", Endpoint: "http://node-a"}
	now := time.Now()

	if outcome, err := store.Delete("never-existed", execA, now); err != nil || outcome != BindingDeleteAbsent {
		t.Fatalf("absent: got (%v, %v), want noop_absent", outcome, err)
	}

	if err := store.Record("sbx-match", Binding{Node: node, ExecutionID: execA}, now); err != nil {
		t.Fatalf("record failed: %v", err)
	}
	if outcome, err := store.Delete("sbx-match", execA, now); err != nil || outcome != BindingDeleteDeleted {
		t.Fatalf("match: got (%v, %v), want deleted", outcome, err)
	}
	assertRedisMissing(t, store, "sbx-match")
	// 🔴 In step with the record: a member left behind points at a key that is
	// gone, and the next empty roster spends a GET rediscovering that.
	assertRedisSetEqual(t, store, store.nodeKey("node-a"), nil)

	if err := store.Record("sbx-stale", Binding{Node: node, ExecutionID: execB}, now); err != nil {
		t.Fatalf("record failed: %v", err)
	}
	if outcome, err := store.Delete("sbx-stale", execA, now); err != nil || outcome != BindingDeleteRejectedStale {
		t.Fatalf("stale: got (%v, %v), want rejected_stale", outcome, err)
	}
	assertRedisBinding(t, store, "sbx-stale", node)

	if err := store.Record("sbx-unknown", Binding{Node: node}, now); err != nil {
		t.Fatalf("record failed: %v", err)
	}
	if outcome, err := store.Delete("sbx-unknown", execA, now); err != nil || outcome != BindingDeleteUnknownIncumbent {
		t.Fatalf("unknown incumbent: got (%v, %v), want deleted_unknown_incumbent", outcome, err)
	}
	assertRedisMissing(t, store, "sbx-unknown")

	// A caller must never reach the script without an incarnation: that would
	// be the unguarded delete the guard exists to prevent.
	if err := store.Record("sbx-guarded", Binding{Node: node, ExecutionID: execA}, now); err != nil {
		t.Fatalf("record failed: %v", err)
	}
	if outcome, err := store.Delete("sbx-guarded", "  ", now); err != nil || outcome != BindingDeleteAbsent {
		t.Fatalf("blank incarnation: got (%v, %v), want a refused no-op", outcome, err)
	}
	assertRedisBinding(t, store, "sbx-guarded", node)
}

// TestRedisDeleteRemovesAnUndecodableRecord: a record that names no node routes
// nowhere, so an event naming its sandbox may clear it.
func TestRedisDeleteRemovesAnUndecodableRecord(t *testing.T) {
	store := newRedisBindingStoreForTest(t, 30*time.Second)
	key := store.bindingKey("sbx-garbage")
	if err := store.client.Set(context.Background(), key, "not-json", time.Minute).Err(); err != nil {
		t.Fatalf("write garbage failed: %v", err)
	}
	if outcome, err := store.Delete("sbx-garbage", execA, time.Now()); err != nil || outcome != BindingDeleteUnknownIncumbent {
		t.Fatalf("got (%v, %v), want deleted_unknown_incumbent", outcome, err)
	}
	if redisExists(t, store, key) {
		t.Fatal("an undecodable record survived a delete")
	}
}

// TestRedisRecordHonoursTheNodeBudget covers the assignment write's TTL.
func TestRedisRecordHonoursTheNodeBudget(t *testing.T) {
	store := newRedisBindingStoreForTest(t, 30*time.Second)
	node := Node{ID: "node-a", Endpoint: "http://node-a"}

	if err := store.Record("sbx-budget", Binding{Node: node, ExecutionID: execA, ProjectionTTL: time.Hour}, time.Now()); err != nil {
		t.Fatalf("record failed: %v", err)
	}
	assertPTTLWithin(t, store, "sbx-budget", time.Hour, 5*time.Second)

	// 🔴 No budget falls back to binding_ttl. It must not fall back to a key
	// with no expiry at all.
	if err := store.Record("sbx-default", Binding{Node: node, ExecutionID: execA}, time.Now()); err != nil {
		t.Fatalf("record failed: %v", err)
	}
	assertPTTLWithin(t, store, "sbx-default", 30*time.Second, 5*time.Second)
}

// TestRedisHeartbeatRefreshKeepsTheDeadline is the assertion the whole stage
// stands on, and the one the parent proposal missed entirely.
//
// Before this, the reconciliation script wrote `SET … PX binding_ttl` on every
// accepted entry. A heartbeat every five seconds therefore reset a
// twenty-four-hour projection back to thirty seconds after the first tick,
// leaving the record's survival dependent on the scheduler still running — the
// exact dependency the long TTL is meant to remove.
func TestRedisHeartbeatRefreshKeepsTheDeadline(t *testing.T) {
	store := newRedisBindingStoreForTest(t, 30*time.Second)
	node := Node{ID: "node-a", Endpoint: "http://node-a"}
	roster := []RosterEntry{{SandboxID: "sbx-1", ExecutionID: execA, ProjectionTTL: time.Hour}}

	if err := store.ReconcileNode(node, roster, time.Now()); err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	assertPTTLWithin(t, store, "sbx-1", time.Hour, 5*time.Second)
	first := readPTTL(t, store, "sbx-1")

	// Six more heartbeats, same incarnation. The record's remaining life must
	// only ever go down.
	for i := 0; i < 6; i++ {
		if err := store.ReconcileNode(node, roster, time.Now()); err != nil {
			t.Fatalf("reconcile %d failed: %v", i, err)
		}
	}
	after := readPTTL(t, store, "sbx-1")
	if after > first {
		t.Fatalf("a heartbeat extended the projection: %s -> %s", first, after)
	}
	if after < 55*time.Minute {
		t.Fatalf("a heartbeat collapsed the projection back to the default: %s", after)
	}
}

// TestRedisHeartbeatRepairSetsTheDeadline is the else branch: a record being
// installed or superseded is a real lifecycle event and carries a new budget.
func TestRedisHeartbeatRepairSetsTheDeadline(t *testing.T) {
	store := newRedisBindingStoreForTest(t, 30*time.Second)
	node := Node{ID: "node-a", Endpoint: "http://node-a"}

	// Installed from nothing, with a budget.
	if err := store.ReconcileNode(node, []RosterEntry{{SandboxID: "sbx-1", ExecutionID: execA, ProjectionTTL: time.Hour}}, time.Now()); err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	assertPTTLWithin(t, store, "sbx-1", time.Hour, 5*time.Second)

	// Superseded by a newer incarnation with a different budget.
	if err := store.ReconcileNode(node, []RosterEntry{{SandboxID: "sbx-1", ExecutionID: execB, ProjectionTTL: 2 * time.Hour}}, time.Now()); err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	assertPTTLWithin(t, store, "sbx-1", 2*time.Hour, 5*time.Second)

	// No budget on the repair path falls back to binding_ttl, not to forever.
	if err := store.ReconcileNode(node, []RosterEntry{{SandboxID: "sbx-2", ExecutionID: execA}}, time.Now()); err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	assertPTTLWithin(t, store, "sbx-2", 30*time.Second, 5*time.Second)
}

// TestRedisHeartbeatRefreshStillRewritesTheRecord: KEEPTTL keeps the deadline,
// not the contents.
func TestRedisHeartbeatRefreshStillRewritesTheRecord(t *testing.T) {
	store := newRedisBindingStoreForTest(t, 30*time.Second)
	roster := []RosterEntry{{SandboxID: "sbx-1", ExecutionID: execA, ProjectionTTL: time.Hour}}

	if err := store.ReconcileNode(Node{ID: "node-a", Endpoint: "http://old"}, roster, time.Now()); err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	if err := store.ReconcileNode(Node{ID: "node-a", Endpoint: "http://new"}, roster, time.Now()); err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	assertRedisBinding(t, store, "sbx-1", Node{ID: "node-a", Endpoint: "http://new"})
	assertPTTLWithin(t, store, "sbx-1", time.Hour, 5*time.Second)
}

// TestRedisRosterBudgetsStayAlignedWithTheirSandboxes is why the TTLs travel as
// a third parallel run of arguments rather than as triples: the failure mode of
// a flat list is every sandbox silently getting its neighbour's number.
func TestRedisRosterBudgetsStayAlignedWithTheirSandboxes(t *testing.T) {
	store := newRedisBindingStoreForTest(t, 30*time.Second)
	node := Node{ID: "node-a", Endpoint: "http://node-a"}

	roster := []RosterEntry{
		{SandboxID: "sbx-short", ExecutionID: execA, ProjectionTTL: 10 * time.Minute},
		{SandboxID: "sbx-long", ExecutionID: execB, ProjectionTTL: 5 * time.Hour},
		{SandboxID: "sbx-none", ExecutionID: execA},
	}
	if err := store.ReconcileNode(node, roster, time.Now()); err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	assertPTTLWithin(t, store, "sbx-short", 10*time.Minute, 5*time.Second)
	assertPTTLWithin(t, store, "sbx-long", 5*time.Hour, 5*time.Second)
	assertPTTLWithin(t, store, "sbx-none", 30*time.Second, 5*time.Second)
}

func readPTTL(t *testing.T, store *RedisBindingStore, sandboxID string) time.Duration {
	t.Helper()
	ttl, err := store.client.PTTL(context.Background(), store.bindingKey(sandboxID)).Result()
	if err != nil {
		t.Fatalf("read pttl for %s failed: %v", sandboxID, err)
	}
	if ttl < 0 {
		t.Fatalf("%s has no expiry (pttl %s): a record with no deadline is a route nothing can retire", sandboxID, ttl)
	}
	return ttl
}

func assertPTTLWithin(t *testing.T, store *RedisBindingStore, sandboxID string, want time.Duration, tolerance time.Duration) {
	t.Helper()
	got := readPTTL(t, store, sandboxID)
	delta := got - want
	if delta < 0 {
		delta = -delta
	}
	if delta > tolerance {
		t.Fatalf("%s pttl = %s, want %s (+/- %s)", sandboxID, got, want, tolerance)
	}
}
