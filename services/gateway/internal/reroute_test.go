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

// 🔴 TestDeclinedResumeIsForwardedToTheClient used to live here: a resume
// (POST /sandboxes/sbx-1/resume) routed straight to a node by this gateway
// against an unconfigured (restUpstream=="") fixture, asserting that an
// isolated node's 503-with-marker response passed through untouched — and,
// deliberately, that the gateway never picked a second node itself. Resume is
// a routeSourcePath call and is now always forwarded to the api half before
// this gateway ever resolves or proxies to a node for it, so there is no
// longer a live call site here that could reroute in the first place: this
// package does not route resume to a node at all any more, isolated or not.
//
// The `headerReroute`/`rerouteReasonSchedule` constants this test pinned went
// with it — they named a header the gateway only ever read back off a
// response it had itself proxied to a node, and no code path in this package
// does that for resume any more. Whether — and how — an isolated origin's
// refusal reaches a resume caller is now entirely the api half's concern; see
// `resume_route_test.go` for the wake-up refusals this package still routes
// and answers (`TestAPinRefusalIs503AndNeverReachesTheScheduler`,
// `TestATransitionInProgressCarriesRetryAfter`).
//
// The generic "an upstream's response headers and body pass through
// untouched" property this test also incidentally covered is not
// resume-specific: it is ordinary reverse-proxy behaviour, exercised by every
// other test in this package that inspects a forwarded response.

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
