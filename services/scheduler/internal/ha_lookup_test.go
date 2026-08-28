package scheduler

import (
	"context"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"
	pausedregistry "agentenv/services/scheduler/internal/registry"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The HA shape, and the only place one particular failure is visible.
//
// 🔴 Every other test in this package can be satisfied by an implementation
// that stores the incarnation in the in-memory binding store and forgets the
// Redis one. That build is green here on a laptop and, in the only deployment
// that runs Redis, routes on arrival order with fencing that reports itself as
// enabled. The tests below are the catcher, which is why the Makefile makes a
// missing redis-server a failure rather than a skip.
//
// The shape mirrors the deployment: one primary taking heartbeats, one
// query-only replica answering data-plane lookups, one Redis between them.

const (
	haNodeAID = "node-a"
	haNodeBID = "node-b"
)

func newHAPair(t *testing.T) (*Service, *QueryOnlyService, *RedisBindingStore, *stubRegistryReader) {
	t.Helper()

	store := newRedisBindingStoreForTest(t, time.Minute)
	reader := &stubRegistryReader{}
	primary := NewService(
		zap.NewNop(),
		NewAtomicNodeRegistry([]Node{
			{ID: haNodeAID, Endpoint: "http://node-a"},
			{ID: haNodeBID, Endpoint: "http://node-b"},
		}, defaultObservedReportTTL),
		NewStrategy("round_robin"),
		store,
		WithPausedRegistry(reader, testReportTTL, testReportTTL),
	)
	replica := NewQueryOnlyService(zap.NewNop(), store, WithQueryOnlyPausedRegistry(reader))
	return primary, replica, store, reader
}

// TestQueryOnlyReplicaAnswersWithTheExecutionOverRedis.
func TestQueryOnlyReplicaAnswersWithTheExecutionOverRedis(t *testing.T) {
	primary, replica, _, reader := newHAPair(t)

	// ① 🟢 The probe proves itself before it proves anything else.
	//
	// A sandbox that exists only as a registry row, asked on the replica, must
	// come back Unavailable — the replica runs no discovery and has no placer,
	// so it cannot answer for a registered sandbox at all. If somebody rewrote
	// this test against the primary by accident, this step would return a
	// successful answer instead and say so immediately.
	reader.listing = pausedregistry.Listing{
		Now: time.Now(),
		Sandboxes: []pausedregistry.Sandbox{{
			SandboxID: "row-only", ClusterID: "cluster-1", State: pausedregistry.StatePaused,
			OriginNodeID: haNodeAID, SnapshotID: "snap", UpdatedAt: time.Now(),
		}},
	}
	_, err := replica.LookupNode(context.Background(), &schedulerv1.LookupNodeRequest{SandboxId: "row-only"})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("the control input answered %v; this test is not running against a replica without a placer", status.Code(err))
	}

	// ② The primary takes a heartbeat naming an incarnation.
	heartbeatWithExecutions(t, primary, haNodeAID, RosterEntry{SandboxID: "sbx", ExecutionID: execOld})

	// ③ The replica answers with it — over Redis, which is the whole point.
	resp := haLookup(t, replica, "sbx")
	if got := resp.GetNode().GetNodeId(); got != haNodeAID {
		t.Fatalf("node: got %q, want %q", got, haNodeAID)
	}
	if resp.GetExecutionId() != execOld {
		t.Fatalf("execution_id: got %q, want %q — the incarnation did not survive the Redis round trip", resp.GetExecutionId(), execOld)
	}
	if got := resp.GetExecutionAuthority(); got != schedulerv1.ExecutionAuthority_EXECUTION_AUTHORITY_REGISTRY {
		t.Fatalf("authority: got %v, want REGISTRY", got)
	}

	// ④ A takeover: node-b reports the same sandbox under a newer incarnation.
	heartbeatWithExecutions(t, primary, haNodeBID, RosterEntry{SandboxID: "sbx", ExecutionID: execNew})
	resp = haLookup(t, replica, "sbx")
	if got := resp.GetNode().GetNodeId(); got != haNodeBID {
		t.Fatalf("after the takeover the replica still routes to %q", got)
	}
	if resp.GetExecutionId() != execNew {
		t.Fatalf("execution_id: got %q, want %q", resp.GetExecutionId(), execNew)
	}

	// ⑤ The old holder comes back and keeps reporting. It must not win, ever —
	// this is the loop that used to run for as long as both nodes were alive.
	for i := 0; i < 3; i++ {
		heartbeatWithExecutions(t, primary, haNodeAID, RosterEntry{SandboxID: "sbx", ExecutionID: execOld})
		resp = haLookup(t, replica, "sbx")
		if got := resp.GetNode().GetNodeId(); got != haNodeBID {
			t.Fatalf("heartbeat %d: the superseded node took the binding back over Redis (routing to %q)", i+1, got)
		}
		if resp.GetExecutionId() != execNew {
			t.Fatalf("heartbeat %d: execution_id fell back to %q", i+1, resp.GetExecutionId())
		}
	}
}

// TestTwoPrimariesSharingOneRedisConvergeOnTheNewerExecution.
//
// The rolling-upgrade shape: two scheduler processes alive at once, sharing one
// Redis, each taking heartbeats. Arbitration has to hold across processes,
// which it only does if it happens inside the store rather than in either
// process's memory.
func TestTwoPrimariesSharingOneRedisConvergeOnTheNewerExecution(t *testing.T) {
	addr := startRedisServerForTest(t)

	newPrimary := func() *Service {
		store, err := NewRedisBindingStore(addr, time.Minute)
		if err != nil {
			t.Fatalf("create redis binding store: %v", err)
		}
		t.Cleanup(func() { _ = store.Close() })
		return NewService(
			zap.NewNop(),
			NewAtomicNodeRegistry([]Node{
				{ID: haNodeAID, Endpoint: "http://node-a"},
				{ID: haNodeBID, Endpoint: "http://node-b"},
			}, defaultObservedReportTTL),
			NewStrategy("round_robin"),
			store,
		)
	}

	outgoing := newPrimary()
	incoming := newPrimary()

	replicaStore, err := NewRedisBindingStore(addr, time.Minute)
	if err != nil {
		t.Fatalf("create redis binding store: %v", err)
	}
	t.Cleanup(func() { _ = replicaStore.Close() })
	replica := NewQueryOnlyService(zap.NewNop(), replicaStore)

	// The two processes take alternating heartbeats from the two nodes, in the
	// order that would produce the wrong answer if the last write won.
	heartbeatWithExecutions(t, incoming, haNodeBID, RosterEntry{SandboxID: "sbx", ExecutionID: execNew})
	heartbeatWithExecutions(t, outgoing, haNodeAID, RosterEntry{SandboxID: "sbx", ExecutionID: execOld})
	heartbeatWithExecutions(t, incoming, haNodeAID, RosterEntry{SandboxID: "sbx", ExecutionID: execOld})
	heartbeatWithExecutions(t, outgoing, haNodeAID, RosterEntry{SandboxID: "sbx", ExecutionID: execOld})

	resp := haLookup(t, replica, "sbx")
	if got := resp.GetNode().GetNodeId(); got != haNodeBID {
		t.Fatalf("with two schedulers on one Redis the last writer won: routing to %q", got)
	}
	if resp.GetExecutionId() != execNew {
		t.Fatalf("execution_id: got %q, want %q", resp.GetExecutionId(), execNew)
	}
}

func haLookup(t *testing.T, replica *QueryOnlyService, sandboxID string) *schedulerv1.LookupNodeResponse {
	t.Helper()

	resp, err := replica.LookupNode(context.Background(), &schedulerv1.LookupNodeRequest{SandboxId: sandboxID})
	if err != nil {
		t.Fatalf("replica lookup of %s: %v", sandboxID, err)
	}
	return resp
}
