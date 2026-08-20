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

// newRestartedService builds a Service the way a freshly started process is:
// nothing observed, no heartbeat yet, no paused registry — over a binding store
// that already exists.
//
// The warm-up timeout is the knob these tests drive. It is what decides whether
// a miss is answered as "I cannot know yet" (Unavailable) or as "this sandbox
// does not exist" (NotFound).
func newRestartedService(t *testing.T, store BindingStore, warmup time.Duration) *Service {
	t.Helper()
	registry := NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, defaultObservedReportTTL)
	return NewService(zap.NewNop(), registry, NewStrategy("round_robin"), store,
		WithWarmupTimeout(warmup),
		WithAuthoritativeProjection(25*time.Hour),
	)
}

func lookupCode(t *testing.T, service *Service, sandboxID string) (*schedulerv1.LookupNodeResponse, codes.Code) {
	t.Helper()
	resp, err := service.LookupNode(context.Background(), &schedulerv1.LookupNodeRequest{SandboxId: sandboxID})
	if err == nil {
		return resp, codes.OK
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("lookup returned a non-status error: %v", err)
	}
	return nil, st.Code()
}

// TestARestartedSchedulerAnswersFromASurvivingProjection is the assertion the
// whole stage is for, written against the shape measured on the cluster.
//
// Measured today, with a genuinely running healthy sandbox and the scheduler
// restarted: 503 while it is down, then a stretch of *404* — the gateway
// reporting a live sandbox as nonexistent — and only then a normal answer. The
// 404 is not the gateway's; it comes from here, and its cause is that a
// projection with a thirty-second TTL does not survive an outage longer than
// thirty seconds. The restarted process then finds no binding, has received no
// heartbeat so has no roster, and a sandbox that was never paused has no
// registry row by design.
//
// A record with a lifetime of its own removes the whole window: it is still in
// the store when the process comes back, so step 1 of the lookup hits on the
// very first request and nothing further is consulted — not the roster, not the
// registry, not the warm-up gate.
func TestARestartedSchedulerAnswersFromASurvivingProjection(t *testing.T) {
	store := newRedisBindingStoreForTest(t, 30*time.Second)
	node := Node{ID: "node-a", Endpoint: "http://node-a"}

	// Written by the process that has since died, with a budget from the node.
	if err := store.Record("sbx-1", Binding{Node: node, ExecutionID: execA, ProjectionTTL: 24 * time.Hour}, time.Now()); err != nil {
		t.Fatalf("record failed: %v", err)
	}

	// A new process. It has been told nothing at all, and its warm-up gate has
	// already lapsed — the worst moment there is.
	service := newRestartedService(t, store, time.Millisecond)
	time.Sleep(20 * time.Millisecond)

	resp, code := lookupCode(t, service, "sbx-1")
	if code != codes.OK {
		t.Fatalf("lookup returned %v; a live sandbox must be routable from the surviving projection", code)
	}
	if resp.GetNode().GetNodeId() != "node-a" {
		t.Fatalf("routed to %q, want node-a", resp.GetNode().GetNodeId())
	}
	if resp.GetLocation() != schedulerv1.SandboxLocation_SANDBOX_LOCATION_BOUND {
		t.Fatalf("location = %v, want BOUND", resp.GetLocation())
	}
	if resp.GetExecutionId() != execA {
		t.Fatalf("execution id = %q, want the recorded incarnation", resp.GetExecutionId())
	}
}

// TestARestartedSchedulerWithoutTheProjectionReproducesTheMeasured404 is the
// control face: the same process, the same absent heartbeat, and a record that
// did not survive.
//
// 🔴 It must fail differently from the test above, or that test proves nothing.
// This is also the honest statement of what is *not* fixed: once the gate opens
// and nothing knows about the sandbox, NotFound is what comes back, and the
// gateway turns it into a 404. The long TTL is what keeps a live sandbox out of
// this branch; it does not change the branch.
func TestARestartedSchedulerWithoutTheProjectionReproducesTheMeasured404(t *testing.T) {
	store := newRedisBindingStoreForTest(t, 30*time.Second)

	// Cold: nothing has reported, and the gate has not lapsed. A miss here is
	// "I cannot know yet".
	//
	// 🔴 Unavailable, never NotFound. The gateway maps Unavailable to 503,
	// which a client retries, and NotFound to 404, which tells a client its
	// sandbox is gone. Nothing may narrow this to the second while the answer
	// is genuinely unknown.
	cold := newRestartedService(t, store, time.Hour)
	if _, code := lookupCode(t, cold, "sbx-1"); code != codes.Unavailable {
		t.Fatalf("a cold scheduler answered %v; a miss it cannot vouch for must be Unavailable", code)
	}

	// The gate lapses, and the same miss becomes an assertion.
	warm := newRestartedService(t, store, time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	if _, code := lookupCode(t, warm, "sbx-1"); code != codes.NotFound {
		t.Fatalf("a warm scheduler answered %v, want NotFound", code)
	}
}

// TestAProjectionThatOutlivedTheOutageStillExpires keeps the fix from becoming
// a leak: surviving a restart is not the same as surviving forever.
func TestAProjectionThatOutlivedTheOutageStillExpires(t *testing.T) {
	store := newRedisBindingStoreForTest(t, 30*time.Second)
	node := Node{ID: "node-a", Endpoint: "http://node-a"}

	if err := store.Record("sbx-1", Binding{Node: node, ExecutionID: execA, ProjectionTTL: 24 * time.Hour}, time.Now()); err != nil {
		t.Fatalf("record failed: %v", err)
	}
	// A deadline exists and it is the node's, not the store's default.
	assertPTTLWithin(t, store, "sbx-1", 24*time.Hour, 5*time.Second)

	// And the delete path can still retire it, which is the other half of why a
	// long TTL is safe: an event guarded by the incarnation removes it in
	// milliseconds rather than in a day.
	outcome, err := store.Delete("sbx-1", execA, time.Now())
	if err != nil || outcome != BindingDeleteDeleted {
		t.Fatalf("got (%v, %v), want deleted", outcome, err)
	}
	assertRedisMissing(t, store, "sbx-1")
}
