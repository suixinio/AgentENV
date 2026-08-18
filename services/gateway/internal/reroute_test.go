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

// Isolation is owned by the node; the gateway resolves the node and proxies the
// call through unchanged.
func TestNodeIsolationIsProxiedToTheNode(t *testing.T) {
	type call struct {
		method string
		path   string
	}
	calls := make(chan call, 1)
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls <- call{method: r.Method, path: r.URL.Path}
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

	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete} {
		request := httptest.NewRequest(method, "/nodes/node-a/isolation", nil)
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, request)

		if response.Code != http.StatusNoContent {
			t.Fatalf("%s: expected status 204, got %d", method, response.Code)
		}
		select {
		case got := <-calls:
			if got.method != method || got.path != "/nodes/node-a/isolation" {
				t.Fatalf("%s: node saw %s %s", method, got.method, got.path)
			}
		default:
			t.Fatalf("%s: request never reached the node", method)
		}
	}
}
