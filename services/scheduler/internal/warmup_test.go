package scheduler

import (
	"context"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func heartbeatRequest(nodeID string) *schedulerv1.HeartbeatRequest {
	return &schedulerv1.HeartbeatRequest{
		NodeId:            nodeID,
		ClusterId:         "cluster-a",
		ServiceInstanceId: "svc-" + nodeID,
		Snapshot:          &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
	}
}

// heartbeat drives the registry directly and tells the gate about it, which is
// what the service does either side of applying the roster to the binding store.
func heartbeat(t *testing.T, registry *AtomicNodeRegistry, gate *warmupGate, nodeID string, now time.Time) {
	t.Helper()
	if _, _, err := registry.Heartbeat(heartbeatRequest(nodeID), now); err != nil {
		t.Fatalf("heartbeat from %s: %v", nodeID, err)
	}
	if gate != nil {
		gate.reportedIn(now)
	}
}

func heartbeatVia(t *testing.T, service *Service, nodeID string) {
	t.Helper()
	if _, err := service.Heartbeat(context.Background(), heartbeatRequest(nodeID)); err != nil {
		t.Fatalf("heartbeat from %s: %v", nodeID, err)
	}
}

// Discovery has not produced a node list yet, so "every known node reported"
// would be vacuously true. That is exactly the coldest moment there is.
func TestWarmupGateIsColdBeforeDiscovery(t *testing.T) {
	registry := NewAtomicNodeRegistry(nil, 30*time.Second)
	now := time.Unix(100, 0)
	gate := newWarmupGate(registry, 15*time.Second, now)

	if gate.warmedUp(now) {
		t.Fatal("a scheduler that has discovered no nodes must not be warm")
	}
}

func TestWarmupGateStaysColdUntilEveryKnownNodeReports(t *testing.T) {
	registry := NewAtomicNodeRegistry([]Node{
		{ID: "node-a", Endpoint: "http://node-a"},
		{ID: "node-b", Endpoint: "http://node-b"},
	}, 30*time.Second)
	now := time.Unix(100, 0)
	gate := newWarmupGate(registry, 15*time.Second, now)

	heartbeat(t, registry, gate, "node-a", now)
	if gate.warmedUp(now) {
		t.Fatal("node-b has not reported its sandboxes yet")
	}

	heartbeat(t, registry, gate, "node-b", now)
	if !gate.warmedUp(now) {
		t.Fatal("every known node has reported; the gate must open")
	}
}

// A node that is genuinely down never reports. Waiting for it forever would
// turn every legitimate 404 into a 503 for the life of the process.
func TestWarmupGateOpensAtTheDeadline(t *testing.T) {
	registry := NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, 30*time.Second)
	start := time.Unix(100, 0)
	gate := newWarmupGate(registry, 15*time.Second, start)

	if gate.warmedUp(start.Add(14 * time.Second)) {
		t.Fatal("the gate must stay shut until the deadline")
	}
	if !gate.warmedUp(start.Add(15 * time.Second)) {
		t.Fatal("the gate must open at the deadline even with a silent node")
	}
}

// Nodes join and leave for the life of the cluster. A node discovered an hour
// in must not put the scheduler back into warm-up and start withholding
// answers about sandboxes it already knows.
func TestWarmupGateStaysOpenWhenANewNodeAppears(t *testing.T) {
	registry := NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, 30*time.Second)
	now := time.Unix(100, 0)
	gate := newWarmupGate(registry, 15*time.Second, now)
	heartbeat(t, registry, gate, "node-a", now)
	if !gate.warmedUp(now) {
		t.Fatal("expected the gate to open")
	}

	registry.Set([]Node{
		{ID: "node-a", Endpoint: "http://node-a"},
		{ID: "node-b", Endpoint: "http://node-b"},
	}, nil)

	if !gate.warmedUp(now) {
		t.Fatal("a newly discovered node must not reopen warm-up")
	}
}

// 🔴 The whole point. While cold, a miss must be retryable rather than a
// verdict: the gateway turns NotFound into a 404 for live sandboxes, and turns
// NotFound on a resume into "hand this to any node", which claims the sandbox
// away from the node that actually holds it.
func TestLookupWithholdsNotFoundWhileCold(t *testing.T) {
	registry := NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, 30*time.Second)
	store := NewInMemoryBindingStore(30 * time.Second)
	service := NewService(zap.NewNop(), registry, NewStrategy("round_robin"), store,
		WithWarmupTimeout(15*time.Second))

	_, err := service.LookupNode(context.Background(), &schedulerv1.LookupNodeRequest{SandboxId: "sbx-1"})

	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected a retryable unavailable while cold, got %v", err)
	}
}

// And once warm it must go back to answering honestly, or a deleted sandbox
// would report as retryable forever.
func TestLookupReportsNotFoundOnceWarm(t *testing.T) {
	registry := NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, 30*time.Second)
	store := NewInMemoryBindingStore(30 * time.Second)
	service := NewService(zap.NewNop(), registry, NewStrategy("round_robin"), store,
		WithWarmupTimeout(15*time.Second))
	heartbeatVia(t, service, "node-a")

	_, err := service.LookupNode(context.Background(), &schedulerv1.LookupNodeRequest{SandboxId: "sbx-1"})

	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected not found once warm, got %v", err)
	}
}

// The query-only replica requires Redis, so its bindings outlive its own
// restart and a miss there is a real miss, not a cold cache.
func TestQueryOnlyLookupIsNotGated(t *testing.T) {
	service := NewQueryOnlyService(zap.NewNop(), NewInMemoryBindingStore(30*time.Second))

	_, err := service.LookupNode(context.Background(), &schedulerv1.LookupNodeRequest{SandboxId: "sbx-1"})

	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected not found from an ungated replica, got %v", err)
	}
}
