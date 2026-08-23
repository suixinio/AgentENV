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

// 🔴 The pair, and neither half is evidence without the other.
//
// "The api half was asked" passes against a gateway that sends it everything,
// including the data plane. "A node was asked" passes against a gateway that
// ignores the switch entirely. The same request, through the same handler,
// differing only in whether an address is configured, is what says the switch
// is live and that it switches.
func TestTheRestUpstreamSwitchDecidesWhoServesAUserFacingRestCall(t *testing.T) {
	t.Run("off: the scheduler places the call and a node serves it", func(t *testing.T) {
		node := newRecordingUpstream(t)
		api := newRecordingUpstream(t)
		schedules := 0
		scheduler := stubSchedulerClient{
			scheduleFunc: func(context.Context, *schedulerv1.ScheduleRequest, ...grpc.CallOption) (*schedulerv1.ScheduleResponse, error) {
				schedules++
				return &schedulerv1.ScheduleResponse{
					Node: &schedulerv1.Node{NodeId: "node-a", Endpoint: node.server.URL},
				}, nil
			},
			recordAssignmentFunc: func(context.Context, *schedulerv1.RecordAssignmentRequest, ...grpc.CallOption) (*schedulerv1.RecordAssignmentResponse, error) {
				return &schedulerv1.RecordAssignmentResponse{}, nil
			},
		}

		server := newTestServer(t, scheduler, 5*time.Second, 1<<20)
		resp := serve(t, server, httptest.NewRequest(http.MethodPost, "/sandboxes", strings.NewReader(`{}`)))
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200", resp.StatusCode)
		}
		if schedules != 1 {
			t.Fatalf("Schedule calls = %d, want 1: with no upstream configured the scheduler still places creates", schedules)
		}
		if node.hits() != 1 {
			t.Fatalf("node hits = %d, want 1", node.hits())
		}
		if api.hits() != 0 {
			t.Fatalf("api hits = %d, want 0: nothing is configured to send it anything", api.hits())
		}
	})

	t.Run("on: the api half serves it and the scheduler is not consulted", func(t *testing.T) {
		node := newRecordingUpstream(t)
		api := newRecordingUpstream(t)
		// 🔴 Every scheduler method fails the test. Placement is the api half's
		// decision once this switch is on, and a Schedule call whose answer is
		// discarded is not harmless: it consumes a placement and moves the
		// strategy's cursor for a request that never went there.
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
	})
}

// A sandbox-scoped control-plane call is the other half of the REST surface,
// and it takes a different route through handleProxy — it resolves a sandbox
// before it forwards. With the switch on it must not resolve one at all.
func TestASandboxControlPlaneCallGoesToTheApiHalfWithoutResolvingANode(t *testing.T) {
	t.Run("off: the sandbox is resolved and the holding node serves it", func(t *testing.T) {
		node := newRecordingUpstream(t)
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

		server := newTestServer(t, scheduler, 5*time.Second, 1<<20)
		resp := serve(t, server, httptest.NewRequest(http.MethodPost, "/sandboxes/sbx-1/pause", nil))
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200", resp.StatusCode)
		}
		if lookups != 1 {
			t.Fatalf("LookupNode calls = %d, want 1", lookups)
		}
		if node.hits() != 1 {
			t.Fatalf("node hits = %d, want 1", node.hits())
		}
	})

	t.Run("on: the api half is asked directly", func(t *testing.T) {
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
	})
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

// The gateway answers some paths itself out of the scheduler. Those are a
// different read path with a switch of its own, and this one does not move them.
func TestTheGatewaysOwnAggregationsAreNotSentToTheApiHalf(t *testing.T) {
	for _, path := range []string{"/v2/sandboxes", "/sandboxes", "/nodes"} {
		t.Run(path, func(t *testing.T) {
			api := newRecordingUpstream(t)
			// One counter for two different RPCs: the cluster listing walks the
			// nodes the scheduler knows, the node listing reads the observed
			// ones. Which of them answered is not this test's subject — that
			// the gateway answered at all is.
			answeredHere := 0
			scheduler := stubSchedulerClient{
				listNodesFunc: func(context.Context, *schedulerv1.ListNodesRequest, ...grpc.CallOption) (*schedulerv1.ListNodesResponse, error) {
					answeredHere++
					return &schedulerv1.ListNodesResponse{}, nil
				},
				listObservedFunc: func(context.Context, *schedulerv1.ListObservedNodesRequest, ...grpc.CallOption) (*schedulerv1.ListObservedNodesResponse, error) {
					answeredHere++
					return &schedulerv1.ListObservedNodesResponse{}, nil
				},
			}

			server := newTestServer(t, scheduler, 5*time.Second, 1<<20, withRestUpstream(api.server.URL))
			resp := serve(t, server, httptest.NewRequest(http.MethodGet, path, nil))
			defer resp.Body.Close()

			if api.hits() != 0 {
				t.Fatalf("api hits = %d, want 0: %s is answered by the gateway itself", api.hits(), path)
			}
			if answeredHere == 0 {
				t.Fatalf("%s did not reach the gateway's own aggregation either; this test is "+
					"asserting an absence with nothing to compare it against", path)
			}
		})
	}
}

// 🔴 Both arms move, and that is what makes either one readable on a cluster.
// 阶段 3a's acceptance criterion is "no node serves user REST any more"; a
// counter with only an api arm could not tell that from a gateway receiving no
// REST at all.
func TestBothArmsOfTheRestUpstreamCounterMove(t *testing.T) {
	apiBefore := testutil.ToFloat64(gatewayRestUpstream.WithLabelValues(restUpstreamAPI))
	nodeBefore := testutil.ToFloat64(gatewayRestUpstream.WithLabelValues(restUpstreamNode))

	node := newRecordingUpstream(t)
	api := newRecordingUpstream(t)
	scheduler := stubSchedulerClient{
		scheduleFunc: func(context.Context, *schedulerv1.ScheduleRequest, ...grpc.CallOption) (*schedulerv1.ScheduleResponse, error) {
			return &schedulerv1.ScheduleResponse{
				Node: &schedulerv1.Node{NodeId: "node-a", Endpoint: node.server.URL},
			}, nil
		},
		recordAssignmentFunc: func(context.Context, *schedulerv1.RecordAssignmentRequest, ...grpc.CallOption) (*schedulerv1.RecordAssignmentResponse, error) {
			return &schedulerv1.RecordAssignmentResponse{}, nil
		},
	}

	off := newTestServer(t, scheduler, 5*time.Second, 1<<20)
	resp := serve(t, off, httptest.NewRequest(http.MethodPost, "/sandboxes", strings.NewReader(`{}`)))
	_ = resp.Body.Close()

	on := newTestServer(t, scheduler, 5*time.Second, 1<<20, withRestUpstream(api.server.URL))
	resp = serve(t, on, httptest.NewRequest(http.MethodPost, "/sandboxes", strings.NewReader(`{}`)))
	_ = resp.Body.Close()

	if got := testutil.ToFloat64(gatewayRestUpstream.WithLabelValues(restUpstreamNode)) - nodeBefore; got != 1 {
		t.Fatalf("node arm moved by %v, want 1", got)
	}
	if got := testutil.ToFloat64(gatewayRestUpstream.WithLabelValues(restUpstreamAPI)) - apiBefore; got != 1 {
		t.Fatalf("api arm moved by %v, want 1", got)
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
