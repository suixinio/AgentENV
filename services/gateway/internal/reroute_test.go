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

// An isolated node still marks the resumes it refuses, and the gateway
// forwards that answer to the client untouched.
//
// 🔴 This replaces a reroute the gateway used to perform on its own: buffer the
// body, drop the first node's response, pick a second node, replay. That only
// existed because nothing else could decide where a paused sandbox belonged.
// The scheduler decides now — it excludes isolated nodes from a placement, and
// refuses up front when an isolated node is the only one that could serve the
// sandbox — so a marker arriving here means the node refused work that was
// legitimately its own, and hiding it would hide a real fault.
func TestDeclinedResumeIsForwardedToTheClient(t *testing.T) {
	for _, tc := range []struct {
		name   string
		marker bool
	}{
		{name: "with the reroute marker", marker: true},
		// The control: a plain 503 was never rerouted, and must still not be.
		{name: "without the reroute marker", marker: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			served := 0
			isolated := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				served++
				if tc.marker {
					w.Header().Set(headerReroute, rerouteReasonSchedule)
				}
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte("node is isolated"))
			}))
			defer isolated.Close()

			// scheduleFunc is unset: the stub fails the test if the gateway
			// tries to pick a second node.
			server := newTestServer(t, stubSchedulerClient{
				lookupNodeFunc: lookupNodeReturning(
					&schedulerv1.Node{NodeId: "node-a", Endpoint: isolated.URL},
					schedulerv1.SandboxLocation_SANDBOX_LOCATION_BOUND,
					"",
				),
			}, 5*time.Second, 4<<20)

			request := httptest.NewRequest(http.MethodPost, "/sandboxes/sbx-1/resume", strings.NewReader(`{"timeout":600}`))
			response := httptest.NewRecorder()
			server.Handler().ServeHTTP(response, request)

			if response.Code != http.StatusServiceUnavailable {
				t.Fatalf("expected the upstream 503 to pass through, got %d", response.Code)
			}
			if !strings.Contains(response.Body.String(), "isolated") {
				t.Fatalf("upstream body did not pass through: %q", response.Body.String())
			}
			if got := response.Header().Get(headerReroute); tc.marker && got != rerouteReasonSchedule {
				t.Fatalf("the node's reroute marker was swallowed, got %q", got)
			}
			if served != 1 {
				t.Fatalf("expected the bound node to be tried exactly once, got %d", served)
			}
		})
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
