package gateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// recordingUpstream answers 200 and remembers what it was asked, so a test can
// say *which* upstream saw a request and what arrived there — not merely that
// something was hit.
type recordingUpstream struct {
	server *httptest.Server

	mu      sync.Mutex
	paths   []string
	headers []http.Header
}

func newRecordingUpstream(t *testing.T) *recordingUpstream {
	t.Helper()
	upstream := &recordingUpstream{}
	upstream.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstream.mu.Lock()
		upstream.paths = append(upstream.paths, r.URL.Path)
		upstream.headers = append(upstream.headers, r.Header.Clone())
		upstream.mu.Unlock()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(upstream.server.Close)
	return upstream
}

func (u *recordingUpstream) hits() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return len(u.paths)
}

func (u *recordingUpstream) lastPath(t *testing.T) string {
	t.Helper()
	u.mu.Lock()
	defer u.mu.Unlock()
	if len(u.paths) == 0 {
		t.Fatal("this upstream was never asked for anything")
	}
	return u.paths[len(u.paths)-1]
}

func (u *recordingUpstream) lastHeader(t *testing.T, name string) string {
	t.Helper()
	u.mu.Lock()
	defer u.mu.Unlock()
	if len(u.headers) == 0 {
		t.Fatal("this upstream was never asked for anything")
	}
	return u.headers[len(u.headers)-1].Get(name)
}

func withRestUpstream(addr string) testServerOption {
	return func(options *ServerOptions) {
		options.RestUpstreamAddr = addr
	}
}

func serve(t *testing.T, server *Server, req *http.Request) *http.Response {
	t.Helper()
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)
	return rec.Result()
}

// 🔴 This used to be a pair with an "off" subtest: the scheduler placed the
// call and a node served it, against an unconfigured (restUpstream=="")
// fixture. Off is gone — handleProxy no longer has a node-routing fallback
// for user-facing REST to fall through to, so a create against an
// unconfigured server now just 502s rather than exercising anything (see
// rest_upstream.go) — so only the "on" case is left to pin, and unwrapped
// from its subtest since it no longer has a sibling to be paired against.
func TestTheRestUpstreamSwitchDecidesWhoServesAUserFacingRestCall(t *testing.T) {
	node := newRecordingUpstream(t)
	api := newRecordingUpstream(t)
	// 🔴 Every scheduler method fails the test. Placement is the api half's
	// decision, and a Schedule call whose answer is discarded is not
	// harmless: it consumes a placement and moves the strategy's cursor for
	// a request that never went there.
	scheduler := stubSchedulerClient{
		scheduleFunc: func(context.Context, *schedulerv1.ScheduleRequest, ...grpc.CallOption) (*schedulerv1.ScheduleResponse, error) {
			t.Fatal("the api half places its own sandboxes; the gateway must not schedule one")
			return nil, nil
		},
		recordAssignmentFunc: func(context.Context, *schedulerv1.RecordAssignmentRequest, ...grpc.CallOption) (*schedulerv1.RecordAssignmentResponse, error) {
			t.Fatal("the gateway has no node to record: writing this address into a binding " +
				"would route the next data-plane request to a process with no sandbox on it")
			return nil, nil
		},
	}

	server := newTestServer(t, scheduler, 5*time.Second, 1<<20, withRestUpstream(api.server.URL))
	resp := serve(t, server, httptest.NewRequest(http.MethodPost, "/sandboxes", strings.NewReader(`{}`)))
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if api.hits() != 1 {
		t.Fatalf("api hits = %d, want 1", api.hits())
	}
	if got := api.lastPath(t); got != "/sandboxes" {
		t.Fatalf("the api half was asked for %q, want /sandboxes: a REST path must arrive unrewritten", got)
	}
	if node.hits() != 0 {
		t.Fatalf("node hits = %d, want 0", node.hits())
	}
}

// A sandbox-scoped control-plane call is the other half of the REST surface,
// and it takes a different route through handleProxy — it resolves a sandbox
// before it forwards. It must not resolve one at all.
//
// 🔴 Also unwrapped from an "off"/"on" pair for the same reason as the test
// above: off no longer has any behaviour of its own to contrast this against.
func TestASandboxControlPlaneCallGoesToTheApiHalfWithoutResolvingANode(t *testing.T) {
	api := newRecordingUpstream(t)
	scheduler := refusingScheduler(t, "the api half knows which machine holds the sandbox")

	server := newTestServer(t, scheduler, 5*time.Second, 1<<20,
		withRestUpstream(api.server.URL),
		withControlPlaneToken("shared-secret"),
	)
	resp := serve(t, server, httptest.NewRequest(http.MethodPost, "/sandboxes/sbx-1/pause", nil))
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if got := api.lastPath(t); got != "/sandboxes/sbx-1/pause" {
		t.Fatalf("the api half was asked for %q", got)
	}
	// 🔴 The api half sits behind the same control-plane gate the nodes do.
	// A forward that dropped the stamp would be refused by every gated api
	// replica, and the symptom — 403 on every REST call — arrives at the
	// moment the switch is flipped and looks like the switch being wrong.
	if got := api.lastHeader(t, headerControlPlane); got != "shared-secret" {
		t.Fatalf("control-plane stamp on the api-bound request = %q, want the configured token", got)
	}
}

// 🔴 The switch is about REST and only REST. Data-plane traffic is addressed to
// a process inside one sandbox, and the api half runs none.
func TestTheRestUpstreamNeverTakesDataPlaneTraffic(t *testing.T) {
	for _, tc := range []struct {
		name    string
		request func() *http.Request
	}{
		{
			name: "routed by proxy headers",
			request: func() *http.Request {
				req := httptest.NewRequest(http.MethodGet, "/anything", nil)
				req.Header.Set(headerSandboxID, "sbx-1")
				req.Header.Set(headerTargetPort, "8080")
				return req
			},
		},
		{
			// 🔴 The path here is one the REST predicate would otherwise claim.
			// A sandbox proxy domain can carry any path at all, `/sandboxes/...`
			// included, and only the host says what the request is.
			name: "routed by a sandbox proxy domain, at a rest-shaped path",
			request: func() *http.Request {
				req := httptest.NewRequest(http.MethodGet, "/sandboxes/sbx-9/pause", nil)
				req.Host = "8080-sbx-1.sandbox-proxy.example.invalid"
				return req
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node := newRecordingUpstream(t)
			api := newRecordingUpstream(t)
			lookups := 0
			scheduler := stubSchedulerClient{
				lookupNodeFunc: func(context.Context, *schedulerv1.LookupNodeRequest, ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
					lookups++
					return &schedulerv1.LookupNodeResponse{
						Node:     &schedulerv1.Node{NodeId: "node-a", Endpoint: node.server.URL},
						Location: schedulerv1.SandboxLocation_SANDBOX_LOCATION_BOUND,
					}, nil
				},
			}

			server := newTestServer(t, scheduler, 5*time.Second, 1<<20,
				withRestUpstream(api.server.URL),
				withSandboxProxyDomains("sandbox-proxy.example.invalid"),
			)
			resp := serve(t, server, tc.request())
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, want 200", resp.StatusCode)
			}
			if api.hits() != 0 {
				t.Fatalf("api hits = %d, want 0: the api half runs no sandboxes and has nothing to proxy to", api.hits())
			}
			if lookups != 1 {
				t.Fatalf("LookupNode calls = %d, want 1: data-plane traffic still resolves to the holding node", lookups)
			}
			if node.hits() != 1 {
				t.Fatalf("node hits = %d, want 1", node.hits())
			}
			// The node's data-plane sub-tree, unchanged by the switch.
			if got := node.lastPath(t); !strings.HasPrefix(got, "/proxy/") {
				t.Fatalf("the node was asked for %q, want a /proxy/… path", got)
			}
		})
	}
}

// The gateway answers `/nodes` out of the scheduler's observed-node state, which
// no position of this switch moves: the api half does not hold it.
//
// 🔴 The sandbox listings used to be asserted here alongside it and are not any
// more — they aggregate the *nodes*, not the scheduler, and the api half owns
// the ledger they aggregate, so they move with this value. Their two positions
// are in TestTheRestUpstreamSwitchDecidesWhoAnswersTheClusterSandboxList.
func TestTheGatewaysSchedulerAggregationsAreNotSentToTheApiHalf(t *testing.T) {
	api := newRecordingUpstream(t)
	answeredHere := 0
	scheduler := stubSchedulerClient{
		listObservedFunc: func(context.Context, *schedulerv1.ListObservedNodesRequest, ...grpc.CallOption) (*schedulerv1.ListObservedNodesResponse, error) {
			answeredHere++
			return &schedulerv1.ListObservedNodesResponse{}, nil
		},
	}

	server := newTestServer(t, scheduler, 5*time.Second, 1<<20, withRestUpstream(api.server.URL))
	resp := serve(t, server, httptest.NewRequest(http.MethodGet, "/nodes", nil))
	defer resp.Body.Close()

	if api.hits() != 0 {
		t.Fatalf("api hits = %d, want 0: /nodes is answered by the gateway itself", api.hits())
	}
	if answeredHere == 0 {
		t.Fatal("/nodes did not reach the gateway's own aggregation either; this test is " +
			"asserting an absence with nothing to compare it against")
	}
}

// 🔴 Only one arm moves now. 阶段 3a's acceptance criterion was "no node
// serves user REST any more", and this test used to prove it two ways at
// once: an "off" server (restUpstream=="") against the same scheduler stub,
// asserting the node arm moved instead. Off no longer forwards anything to a
// node — see rest_upstream.go — so there is no second server left to compare
// against, and gatewayRestUpstream.WithLabelValues("node") is asserted never
// to move by any test in this package any more. "node" is a literal, not the
// constant restUpstreamNode: that constant is deleted along with the
// node-routing fallback it named, but the label value it stood for is still
// a real value the label set `{"upstream"}` could take, so this sentinel
// keeps asserting against it directly.
func TestBothArmsOfTheRestUpstreamCounterMove(t *testing.T) {
	apiBefore := testutil.ToFloat64(gatewayRestUpstream.WithLabelValues(restUpstreamAPI))
	nodeBefore := testutil.ToFloat64(gatewayRestUpstream.WithLabelValues("node"))

	api := newRecordingUpstream(t)
	scheduler := refusingScheduler(t, "the api half places its own sandboxes")

	on := newTestServer(t, scheduler, 5*time.Second, 1<<20, withRestUpstream(api.server.URL))
	resp := serve(t, on, httptest.NewRequest(http.MethodPost, "/sandboxes", strings.NewReader(`{}`)))
	_ = resp.Body.Close()

	if got := testutil.ToFloat64(gatewayRestUpstream.WithLabelValues(restUpstreamAPI)) - apiBefore; got != 1 {
		t.Fatalf("api arm moved by %v, want 1", got)
	}
	// 🔴 The regression guard this test still owns even without a second
	// server to contrast against: nothing in this package may ever record
	// against `{upstream="node"}` again. If a future change reintroduces a
	// node-routing fallback for user-facing REST, this is what catches it.
	if got := testutil.ToFloat64(gatewayRestUpstream.WithLabelValues("node")) - nodeBefore; got != 0 {
		t.Fatalf("node arm moved by %v, want 0: there is no node-routing fallback left to record against", got)
	}
}

// 🔴 A REST upstream that cannot be used stops the process, rather than
// becoming a 502 on every REST call while the switch reports itself as on.
func TestARestUpstreamThatCannotBeUsedIsRefusedAtStartup(t *testing.T) {
	for _, tc := range []struct {
		name string
		addr string
		want string
	}{
		{name: "no host", addr: "http://", want: "names no host"},
		{name: "wrong scheme", addr: "grpc://agentenv-api:8002", want: "must be http or https"},
		{name: "carries a path", addr: "http://agentenv-api:8000/v2", want: "must not carry a path"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewServer(zap.NewNop(), stubSchedulerClient{}, ServerOptions{RestUpstreamAddr: tc.addr})
			if err == nil {
				t.Fatalf("NewServer accepted %q", tc.addr)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not say %q", err, tc.want)
			}
		})
	}

	// Resolution: the same constructor accepts the two shapes an operator
	// actually writes, so the refusals above are about the values and not about
	// the switch being unusable.
	for _, addr := range []string{"agentenv-api:8000", "http://agentenv-api:8000"} {
		server, err := NewServer(zap.NewNop(), stubSchedulerClient{}, ServerOptions{RestUpstreamAddr: addr})
		if err != nil {
			t.Fatalf("NewServer(%q) failed: %v", addr, err)
		}
		if server.restUpstream != "http://agentenv-api:8000" {
			t.Fatalf("NewServer(%q) normalised to %q", addr, server.restUpstream)
		}
	}

	// ...and the empty string is not an error at all: it is the switch off.
	server, err := NewServer(zap.NewNop(), stubSchedulerClient{}, ServerOptions{})
	if err != nil {
		t.Fatalf("NewServer with no upstream failed: %v", err)
	}
	if server.restUpstream != "" {
		t.Fatalf("an unconfigured gateway resolved an upstream: %q", server.restUpstream)
	}
}
