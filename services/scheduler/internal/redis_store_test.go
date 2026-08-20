package scheduler

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestRedisBindingStoreGetCases(t *testing.T) {
	store := newRedisBindingStoreForTest(t, 5*time.Second)
	node := Node{ID: "node-a", Endpoint: "http://node-a"}

	if got, ok, err := store.Get("missing", time.Now()); err != nil || ok || got != (Binding{}) {
		t.Fatalf("expected missing binding to return zero/miss, got (%+v, %v, %v)", got, ok, err)
	}
	if got, ok, err := store.Get("   ", time.Now()); err != nil || ok || got != (Binding{}) {
		t.Fatalf("expected blank sandbox id to return zero/miss, got (%+v, %v, %v)", got, ok, err)
	}

	writeRawRedisBinding(t, store, "valid", node)
	assertRedisBinding(t, store, " valid ", node)

	// Malformed values should behave like a cache miss but must not be deleted by Get.
	malformedKey := store.bindingKey("malformed")
	if err := store.client.Set(context.Background(), malformedKey, "not-json", time.Minute).Err(); err != nil {
		t.Fatalf("write malformed binding failed: %v", err)
	}
	if got, ok, err := store.Get("malformed", time.Now()); err != nil || ok || got != (Binding{}) {
		t.Fatalf("expected malformed binding to return zero/miss, got (%+v, %v, %v)", got, ok, err)
	}
	if exists := redisExists(t, store, malformedKey); !exists {
		t.Fatal("expected malformed binding key to remain after Get")
	}

	invalidNodeKey := store.bindingKey("invalid-node")
	if err := store.client.Set(context.Background(), invalidNodeKey, `{"node":{"node_id":"node-a","endpoint":""}}`, time.Minute).Err(); err != nil {
		t.Fatalf("write invalid-node binding failed: %v", err)
	}
	if got, ok, err := store.Get("invalid-node", time.Now()); err != nil || ok || got != (Binding{}) {
		t.Fatalf("expected invalid-node binding to return zero/miss, got (%+v, %v, %v)", got, ok, err)
	}
	if exists := redisExists(t, store, invalidNodeKey); !exists {
		t.Fatal("expected invalid-node binding key to remain after Get")
	}
}

func TestRedisBindingStoreRecordCases(t *testing.T) {
	store := newRedisBindingStoreForTest(t, 5*time.Second)
	nodeA := Node{ID: "node-a", Endpoint: "http://node-a"}
	nodeATrimmed := Node{ID: " node-a ", Endpoint: " http://node-a "}
	nodeB := Node{ID: "node-b", Endpoint: "http://node-b"}

	store.Record("  sbx-1  ", Binding{Node: nodeATrimmed}, time.Now())
	assertRedisBinding(t, store, "sbx-1", nodeA)
	assertRedisSetEqual(t, store, store.nodeKey("node-a"), []string{"sbx-1"})
	assertRedisBindingJSON(t, store, "sbx-1", nodeA)
	assertRedisBindingHasPositiveTTL(t, store, "sbx-1")
	assertRedisKeyHasPositiveTTL(t, store, store.nodeKey("node-a"))

	store.Record("sbx-1", Binding{Node: nodeB}, time.Now())
	assertRedisBinding(t, store, "sbx-1", nodeB)
	assertRedisSetEqual(t, store, store.nodeKey("node-a"), nil)
	assertRedisSetEqual(t, store, store.nodeKey("node-b"), []string{"sbx-1"})
	assertRedisKeyHasPositiveTTL(t, store, store.nodeKey("node-b"))

	store.Record("", Binding{Node: Node{ID: "node-c", Endpoint: "http://node-c"}}, time.Now())
	store.Record("sbx-blank-node", Binding{Node: Node{Endpoint: "http://node-c"}}, time.Now())
	store.Record("sbx-blank-endpoint", Binding{Node: Node{ID: "node-c"}}, time.Now())
	assertRedisMissing(t, store, "sbx-blank-node")
	assertRedisMissing(t, store, "sbx-blank-endpoint")
	assertRedisSetEqual(t, store, store.nodeKey("node-c"), nil)

	// Existing malformed JSON should not block a Record; the new binding is written.
	if err := store.client.Set(context.Background(), store.bindingKey("malformed"), "not-json", time.Minute).Err(); err != nil {
		t.Fatalf("write malformed binding failed: %v", err)
	}
	store.Record("malformed", Binding{Node: nodeA}, time.Now())
	assertRedisBinding(t, store, "malformed", nodeA)
	assertRedisSetEqual(t, store, store.nodeKey("node-a"), []string{"malformed"})
}

func TestRedisBindingStoreReconcileCases(t *testing.T) {
	store := newRedisBindingStoreForTest(t, 5*time.Second)
	nodeA := Node{ID: "node-a", Endpoint: "http://node-a"}
	nodeB := Node{ID: "node-b", Endpoint: "http://node-b"}
	nodeC := Node{ID: "node-c", Endpoint: "http://node-c"}

	// Build initial state with a stale binding for node-a, two active node-b bindings,
	// and one node-c binding that will be moved by ReconcileNode.
	store.Record("stale-a", Binding{Node: nodeA}, time.Now())
	store.Record("keep-b", Binding{Node: nodeB}, time.Now())
	store.Record("drop-b", Binding{Node: nodeB}, time.Now())
	store.Record("move-c-to-b", Binding{Node: nodeC}, time.Now())

	store.ReconcileNode(Node{ID: " node-b ", Endpoint: " http://node-b "}, rosterOf("keep-b", "new-b", "move-c-to-b", "keep-b", ""), time.Now())

	assertRedisBinding(t, store, "keep-b", nodeB)
	assertRedisBinding(t, store, "new-b", nodeB)
	assertRedisBinding(t, store, "move-c-to-b", nodeB)
	assertRedisMissing(t, store, "drop-b")
	assertRedisBinding(t, store, "stale-a", nodeA)
	assertRedisSetEqual(t, store, store.nodeKey("node-b"), []string{"keep-b", "new-b", "move-c-to-b"})
	assertRedisSetEqual(t, store, store.nodeKey("node-c"), nil)
	assertRedisSetEqual(t, store, store.nodeKey("node-a"), []string{"stale-a"})
	for _, sandboxID := range []string{"keep-b", "new-b", "move-c-to-b"} {
		assertRedisBindingHasPositiveTTL(t, store, sandboxID)
	}
	assertRedisKeyHasPositiveTTL(t, store, store.nodeKey("node-b"))

	// If a node reverse index has a stale sandbox whose binding now points to another node,
	// empty reconcile should remove only the reverse-index entry and must not delete that binding.
	store.Record("foreign", Binding{Node: nodeB}, time.Now())
	if err := store.client.SAdd(context.Background(), store.nodeKey("node-a"), "foreign").Err(); err != nil {
		t.Fatalf("inject stale reverse-index entry failed: %v", err)
	}
	store.ReconcileNode(nodeA, nil, time.Now())
	assertRedisMissing(t, store, "stale-a")
	assertRedisBinding(t, store, "foreign", nodeB)
	assertRedisSetEqual(t, store, store.nodeKey("node-a"), nil)
	assertRedisSetEqual(t, store, store.nodeKey("node-b"), []string{"keep-b", "new-b", "move-c-to-b", "foreign"})

	// Invalid node inputs should be ignored.
	store.ReconcileNode(Node{Endpoint: "http://missing-id"}, rosterOf("ignored"), time.Now())
	assertRedisMissing(t, store, "ignored")
	store.ReconcileNode(Node{ID: "node-no-endpoint"}, rosterOf("ignored-no-endpoint"), time.Now())
	assertRedisMissing(t, store, "ignored-no-endpoint")
	if redisExists(t, store, store.bindingKey("ignored-no-endpoint")) {
		t.Fatal("expected reconcile with non-empty desired list and empty endpoint not to write a phantom binding key")
	}
}

// rosterOf turns a list of sandbox ids into a roster carrying no incarnations,
// which is what a node too old to report them sends.
func rosterOf(sandboxIDs ...string) []RosterEntry {
	entries := make([]RosterEntry, 0, len(sandboxIDs))
	for _, sandboxID := range sandboxIDs {
		entries = append(entries, RosterEntry{SandboxID: sandboxID})
	}
	return entries
}

func newRedisBindingStoreForTest(t *testing.T, ttl time.Duration) *RedisBindingStore {
	t.Helper()
	addr := startRedisServerForTest(t)
	store, err := NewRedisBindingStore(addr, ttl)
	if err != nil {
		t.Fatalf("create redis binding store failed: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})
	return store
}

func startRedisServerForTest(t *testing.T) string {
	t.Helper()

	bin := strings.TrimSpace(os.Getenv("REDIS_SERVER_BIN"))
	if bin == "" {
		var err error
		bin, err = exec.LookPath("redis-server")
		if err != nil {
			// 🔴 A skip reports as a pass, so a run that is *meant* to be
			// exercising Redis — CI, or a verification pass — sets
			// SCHEDULER_REDIS_TEST_REQUIRED and gets a failure instead.
			//
			// It matters more here than the same escape hatch does for
			// PostgreSQL. The tests below are the only ones that catch an
			// incarnation implemented for the in-memory store and forgotten
			// for Redis, and that defect is invisible everywhere else: every
			// other test in this package passes, and the failure only appears
			// in the one deployment shape that uses Redis, as fencing that
			// quietly does nothing.
			if os.Getenv("SCHEDULER_REDIS_TEST_REQUIRED") != "" {
				t.Fatal("SCHEDULER_REDIS_TEST_REQUIRED is set but no redis-server was found: these binding-store tests would have been skipped")
			}
			t.Skip("redis-server not found; set REDIS_SERVER_BIN or install redis-server to run RedisBindingStore integration test")
		}
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("allocate redis test port failed: %v", err)
	}
	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		_ = listener.Close()
		t.Fatalf("parse redis test listener addr failed: %v", err)
	}
	_ = listener.Close()

	var output bytes.Buffer
	cmd := exec.Command(
		bin,
		"--bind", "127.0.0.1",
		"--port", port,
		"--save", "",
		"--appendonly", "no",
		"--dir", t.TempDir(),
		"--loglevel", "warning",
	)
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err := cmd.Start(); err != nil {
		t.Fatalf("start redis-server failed: %v", err)
	}
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
	})

	addr := net.JoinHostPort("127.0.0.1", port)
	client := redis.NewClient(&redis.Options{Addr: addr})
	defer client.Close()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		err := client.Ping(ctx).Err()
		cancel()
		if err == nil {
			return addr
		}
		if cmd.ProcessState != nil && cmd.ProcessState.Exited() {
			t.Fatalf("redis-server exited before readiness: %s", output.String())
		}
		time.Sleep(25 * time.Millisecond)
	}
	pid := 0
	if cmd.Process != nil {
		pid = cmd.Process.Pid
	}
	t.Fatalf("redis-server pid %s did not become ready; output: %s", strconv.Itoa(pid), output.String())
	return ""
}

func writeRawRedisBinding(t *testing.T, store *RedisBindingStore, sandboxID string, node Node) {
	t.Helper()
	value, err := marshalRedisBindingRecord(node, "")
	if err != nil {
		t.Fatalf("marshal binding record failed: %v", err)
	}
	if err := store.client.Set(context.Background(), store.bindingKey(sandboxID), value, time.Minute).Err(); err != nil {
		t.Fatalf("write raw redis binding failed: %v", err)
	}
}

func assertRedisBinding(t *testing.T, store *RedisBindingStore, sandboxID string, want Node) {
	t.Helper()
	got, ok, err := store.Get(sandboxID, time.Now())
	if err != nil || !ok || got.Node != want {
		t.Fatalf("expected %s to resolve to %+v, got (%+v, %v, %v)", strings.TrimSpace(sandboxID), want, got, ok, err)
	}
}

func assertRedisMissing(t *testing.T, store *RedisBindingStore, sandboxID string) {
	t.Helper()
	if got, ok, err := store.Get(sandboxID, time.Now()); err != nil || ok {
		t.Fatalf("expected %s to be missing, got %+v ok=%v err=%v", sandboxID, got, ok, err)
	}
}

func assertRedisBindingJSON(t *testing.T, store *RedisBindingStore, sandboxID string, want Node) {
	t.Helper()
	raw, err := store.client.Get(context.Background(), store.bindingKey(sandboxID)).Bytes()
	if err != nil {
		t.Fatalf("read raw redis binding %s failed: %v", sandboxID, err)
	}
	var record redisBindingRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		t.Fatalf("binding value for %s is not JSON: %v; raw=%q", sandboxID, err, raw)
	}
	if record.Node != want {
		t.Fatalf("unexpected JSON binding value for %s: got %+v want %+v", sandboxID, record.Node, want)
	}
}

func assertRedisBindingHasPositiveTTL(t *testing.T, store *RedisBindingStore, sandboxID string) {
	t.Helper()
	assertRedisKeyHasPositiveTTL(t, store, store.bindingKey(sandboxID))
}

func assertRedisKeyHasPositiveTTL(t *testing.T, store *RedisBindingStore, key string) {
	t.Helper()
	ttl, err := store.client.TTL(context.Background(), key).Result()
	if err != nil {
		t.Fatalf("read ttl for %s failed: %v", key, err)
	}
	if ttl <= 0 {
		t.Fatalf("expected %s to have positive ttl, got %s", key, ttl)
	}
}

func assertRedisSetEqual(t *testing.T, store *RedisBindingStore, key string, want []string) {
	t.Helper()
	got, err := store.client.SMembers(context.Background(), key).Result()
	if err != nil {
		t.Fatalf("read redis set %s failed: %v", key, err)
	}
	sort.Strings(got)
	want = append([]string{}, want...)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("redis set %s mismatch: got %v want %v", key, got, want)
	}
}

func redisExists(t *testing.T, store *RedisBindingStore, key string) bool {
	t.Helper()
	exists, err := store.client.Exists(context.Background(), key).Result()
	if err != nil {
		t.Fatalf("check redis key %s exists failed: %v", key, err)
	}
	return exists > 0
}
