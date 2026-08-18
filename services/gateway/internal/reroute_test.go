package gateway

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	schedulerv1 "agentenv/services/api/proto"
)

// An isolated node declines a resume it does not want to serve. The gateway
// must hand the request to a scheduled node instead, and the client must never
// see the decline.
func TestDeclinedResumeIsReroutedToScheduledNode(t *testing.T) {
	declined := 0
	isolated := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		declined++
		w.Header().Set(headerReroute, rerouteReasonSchedule)
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("node is isolated"))
	}))
	defer isolated.Close()

	receivedBody := make(chan string, 1)
	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		receivedBody <- string(body)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"state":"running"}`))
	}))
	defer healthy.Close()

	scheduleCalled := 0
	server := newTestServer(t, stubSchedulerClient{
		lookupNodeFunc: func(context.Context, *schedulerv1.LookupNodeRequest, ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			return &schedulerv1.LookupNodeResponse{
				Node: &schedulerv1.Node{NodeId: "node-a", Endpoint: isolated.URL},
			}, nil
		},
		scheduleFunc: func(context.Context, *schedulerv1.ScheduleRequest, ...grpc.CallOption) (*schedulerv1.ScheduleResponse, error) {
			scheduleCalled++
			return &schedulerv1.ScheduleResponse{
				Node: &schedulerv1.Node{NodeId: "node-b", Endpoint: healthy.URL},
			}, nil
		},
		recordAssignmentFunc: func(context.Context, *schedulerv1.RecordAssignmentRequest, ...grpc.CallOption) (*schedulerv1.RecordAssignmentResponse, error) {
			return &schedulerv1.RecordAssignmentResponse{}, nil
		},
	}, 5*time.Second, 4<<20)

	request := httptest.NewRequest(http.MethodPost, "/sandboxes/sbx-1/resume", strings.NewReader(`{"timeout":600}`))
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("expected the reroute target's status 200, got %d (body %q)", response.Code, response.Body.String())
	}
	if strings.Contains(response.Body.String(), "isolated") {
		t.Fatalf("the declining node's body leaked to the client: %q", response.Body.String())
	}
	if response.Header().Get(headerReroute) != "" {
		t.Fatalf("the reroute marker leaked to the client")
	}
	if declined != 1 {
		t.Fatalf("expected the bound node to be tried exactly once, got %d", declined)
	}
	if scheduleCalled != 1 {
		t.Fatalf("expected exactly one Schedule call, got %d", scheduleCalled)
	}

	select {
	case body := <-receivedBody:
		if body != `{"timeout":600}` {
			t.Fatalf("request body was not replayed intact: %q", body)
		}
	default:
		t.Fatal("request never reached the scheduled node")
	}
}

// The control for the test above: a plain 503 is an upstream failure, not an
// invitation to try somebody else. Without this, any failing node would have
// its work silently sprayed across the cluster.
func TestPlainServiceUnavailableIsNotRerouted(t *testing.T) {
	server503 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("busy"))
	}))
	defer server503.Close()

	scheduleCalled := 0
	server := newTestServer(t, stubSchedulerClient{
		lookupNodeFunc: func(context.Context, *schedulerv1.LookupNodeRequest, ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			return &schedulerv1.LookupNodeResponse{
				Node: &schedulerv1.Node{NodeId: "node-a", Endpoint: server503.URL},
			}, nil
		},
		scheduleFunc: func(context.Context, *schedulerv1.ScheduleRequest, ...grpc.CallOption) (*schedulerv1.ScheduleResponse, error) {
			scheduleCalled++
			return nil, nil
		},
	}, 5*time.Second, 4<<20)

	request := httptest.NewRequest(http.MethodPost, "/sandboxes/sbx-1/resume", strings.NewReader("{}"))
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected the upstream 503 to pass through, got %d", response.Code)
	}
	if !strings.Contains(response.Body.String(), "busy") {
		t.Fatalf("upstream body did not pass through: %q", response.Body.String())
	}
	if scheduleCalled != 0 {
		t.Fatalf("expected no Schedule call, got %d", scheduleCalled)
	}
}

// Only resume may be rebuilt on a node that never held the sandbox. Every other
// endpoint addresses a sandbox that lives on one specific node, so a marker on
// those must be forwarded rather than acted on.
func TestDeclineMarkerOnNonResumeIsNotRerouted(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(headerReroute, rerouteReasonSchedule)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer upstream.Close()

	scheduleCalled := 0
	server := newTestServer(t, stubSchedulerClient{
		lookupNodeFunc: func(context.Context, *schedulerv1.LookupNodeRequest, ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			return &schedulerv1.LookupNodeResponse{
				Node: &schedulerv1.Node{NodeId: "node-a", Endpoint: upstream.URL},
			}, nil
		},
		scheduleFunc: func(context.Context, *schedulerv1.ScheduleRequest, ...grpc.CallOption) (*schedulerv1.ScheduleResponse, error) {
			scheduleCalled++
			return nil, nil
		},
	}, 5*time.Second, 4<<20)

	request := httptest.NewRequest(http.MethodPost, "/sandboxes/sbx-1/pause", nil)
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected the marker to be forwarded as a plain 503, got %d", response.Code)
	}
	if scheduleCalled != 0 {
		t.Fatalf("expected no Schedule call, got %d", scheduleCalled)
	}
}

// Changing a node's status is owned by the node; the gateway resolves the id
// and proxies the call through unchanged, body and all.
func TestNodeStatusChangeIsProxiedToTheNode(t *testing.T) {
	type call struct {
		method string
		path   string
		body   string
	}
	calls := make(chan call, 1)
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		calls <- call{method: r.Method, path: r.URL.Path, body: string(body)}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer node.Close()

	server := newTestServer(t, stubSchedulerClient{
		getNodeFunc: func(_ context.Context, req *schedulerv1.GetNodeRequest, _ ...grpc.CallOption) (*schedulerv1.GetNodeResponse, error) {
			if req.GetNodeId() != "node-a" {
				t.Errorf("unexpected node id: %s", req.GetNodeId())
			}
			return &schedulerv1.GetNodeResponse{
				Node: &schedulerv1.ObservedNode{NodeId: "node-a", Endpoint: node.URL},
			}, nil
		},
	}, 5*time.Second, 4<<20)

	// Draining and back again travel the same route; a node on its way out has
	// to stay reachable or isolation could never be undone.
	for _, status := range []string{"draining", "ready"} {
		payload := `{"status":"` + status + `"}`
		request := httptest.NewRequest(http.MethodPost, "/nodes/node-a", strings.NewReader(payload))
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, request)

		if response.Code != http.StatusNoContent {
			t.Fatalf("%s: expected status 204, got %d", status, response.Code)
		}
		select {
		case got := <-calls:
			if got.method != http.MethodPost || got.path != "/nodes/node-a" {
				t.Fatalf("%s: node saw %s %s", status, got.method, got.path)
			}
			if got.body != payload {
				t.Fatalf("%s: node saw body %q, want %q", status, got.body, payload)
			}
		default:
			t.Fatalf("%s: request never reached the node", status)
		}
	}
}

// The control: only the methods the node admin surface actually has are routed
// to a node. Everything else has to keep falling through to sandbox routing,
// or an unrelated request to a /nodes-shaped path would be answered by a node
// that was never asked.
func TestOtherMethodsOnNodePathAreNotProxiedToTheNode(t *testing.T) {
	getNodeCalled := 0
	server := newTestServer(t, stubSchedulerClient{
		getNodeFunc: func(context.Context, *schedulerv1.GetNodeRequest, ...grpc.CallOption) (*schedulerv1.GetNodeResponse, error) {
			getNodeCalled++
			return nil, nil
		},
		scheduleFunc: func(context.Context, *schedulerv1.ScheduleRequest, ...grpc.CallOption) (*schedulerv1.ScheduleResponse, error) {
			return nil, status.Error(codes.Unavailable, "no nodes available")
		},
	}, 5*time.Second, 4<<20)

	for _, method := range []string{http.MethodPut, http.MethodDelete, http.MethodPatch} {
		request := httptest.NewRequest(method, "/nodes/node-a", nil)
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, request)

		if getNodeCalled != 0 {
			t.Fatalf("%s: resolved a node it should not have", method)
		}
		if response.Code == http.StatusNoContent {
			t.Fatalf("%s: was served as a node admin call", method)
		}
	}
}
