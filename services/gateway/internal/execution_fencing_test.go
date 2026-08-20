package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"
	"agentenv/services/shared/config"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Two incarnations of the same sandbox, in minting order. The comparison this
// whole feature turns on is lexicographic, so the constants are written so that
// executionOlder < executionNewer is visible by eye.
//
// 🔴 They differ at a hex letter rather than at a digit, and that is deliberate.
// Case only changes the ordering of the letters — '0'-'9' (0x30) < 'A'-'F' (0x41)
// < 'a'-'f' (0x61) — so a pair that differed at a digit would come out in the
// same order whether or not anything lower-cased them, and every test using them
// would be blind to the normalisation.
const (
	executionOlder = "01890000-0000-7000-8000-0000000000aa"
	executionNewer = "01890000-0000-7000-8000-0000000000ab"
)

func withExecutionFencing(mode config.GatewayExecutionFencing) testServerOption {
	return func(options *ServerOptions) {
		options.ExecutionFencing = string(mode)
	}
}

func withControlPlaneToken(token string) testServerOption {
	return func(options *ServerOptions) {
		options.ControlPlaneToken = token
	}
}

func lookupNodeWithExecution(
	node *schedulerv1.Node,
	location schedulerv1.SandboxLocation,
	executionID string,
	authority schedulerv1.ExecutionAuthority,
) func(context.Context, *schedulerv1.LookupNodeRequest, ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
	return func(context.Context, *schedulerv1.LookupNodeRequest, ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
		return &schedulerv1.LookupNodeResponse{
			Node:               node,
			Location:           location,
			OriginNodeId:       node.GetNodeId(),
			ExecutionId:        executionID,
			ExecutionAuthority: authority,
		}, nil
	}
}

// boundToRegistry is the ordinary data-plane answer: the sandbox is bound to a
// node and the scheduler can name the incarnation authoritatively.
func boundToRegistry(node *schedulerv1.Node, executionID string) func(context.Context, *schedulerv1.LookupNodeRequest, ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
	return lookupNodeWithExecution(
		node,
		schedulerv1.SandboxLocation_SANDBOX_LOCATION_BOUND,
		executionID,
		schedulerv1.ExecutionAuthority_EXECUTION_AUTHORITY_REGISTRY,
	)
}

// dataPlaneRequest is a proxied sandbox request routed by header, which is the
// entry the interactive traffic uses.
func dataPlaneRequest(sandboxID string) *http.Request {
	request := httptest.NewRequest(http.MethodGet, "/anything", nil)
	request.Header.Set(headerSandboxID, sandboxID)
	return request
}

// scrapeCounter reads one series off the process's own /metrics, which is the
// same surface Prometheus reads. Going through the exporter rather than the
// counter object also proves the series is registered and named as expected —
// a counter that is incremented but never exported would pass a direct read.
//
// Label order follows the exposition format, which sorts label names.
func scrapeCounter(t *testing.T, series string) float64 {
	t.Helper()

	recorder := httptest.NewRecorder()
	promhttp.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, line := range strings.Split(recorder.Body.String(), "\n") {
		name, value, ok := strings.Cut(strings.TrimSpace(line), " ")
		if !ok || name != series {
			continue
		}
		parsed, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
		if err != nil {
			t.Fatalf("counter %s has unparsable value %q: %v", series, value, err)
		}
		return parsed
	}
	// A series with no observations is not exported at all, which reads as zero.
	return 0
}

func fencingCounter(t *testing.T, plane fencingPlane, decision string) float64 {
	t.Helper()
	return scrapeCounter(t, fmt.Sprintf(`agentenv_gateway_execution_fencing_total{decision=%q,plane=%q}`, decision, plane))
}

func duplicateCounter(t *testing.T, resolution string) float64 {
	t.Helper()
	return scrapeCounter(t, fmt.Sprintf(`agentenv_gateway_cluster_list_duplicate_total{resolution=%q}`, resolution))
}

// 🔴 The probe before the assertions that lean on it.
//
// Every count in this file is a delta read through scrapeCounter, so a reader
// that answered zero for everything — a mistyped series name, a label order that
// does not match the exposition format — would make "the request was counted"
// and "the request was not counted" look the same, and every metric assertion
// here would pass against a gateway that records nothing.
//
// The control input is a decision that cannot have happened: the request below
// is a data-plane pass, so the refusal series must not move while the pass
// series moves by exactly one.
func TestTheFencingCounterProbeCanTellSeriesApart(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(headerExecutionID, executionNewer)
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	// A label that is never recorded reads zero, and stays zero.
	if got := fencingCounter(t, fencingPlaneData, "a_decision_this_build_never_records"); got != 0 {
		t.Fatalf("a decision that cannot be recorded reads %v, so the probe is matching the wrong series", got)
	}

	passBefore := fencingCounter(t, fencingPlaneData, fencingDecisionEnforcedPass)
	refusedBefore := fencingCounter(t, fencingPlaneData, fencingDecisionRefusedEcho)

	server := newTestServer(t, stubSchedulerClient{
		lookupNodeFunc: boundToRegistry(&schedulerv1.Node{NodeId: "node-a", Endpoint: upstream.URL}, executionNewer),
	}, 5*time.Second, 4<<20, withExecutionFencing(config.GatewayExecutionFencingEnforce))

	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, dataPlaneRequest("sbx-1"))
	if response.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d (body %q)", response.Code, response.Body.String())
	}

	if got := fencingCounter(t, fencingPlaneData, fencingDecisionEnforcedPass) - passBefore; got != 1 {
		t.Fatalf("the pass series moved by %v, want 1 — the probe cannot see a recorded decision", got)
	}
	if got := fencingCounter(t, fencingPlaneData, fencingDecisionRefusedEcho) - refusedBefore; got != 0 {
		t.Fatalf("the refusal series moved by %v on a passing request, so the probe is not label-selective", got)
	}
}

// The first gate: the gateway stamps the incarnation it routed against, so the
// node can compare it against what it is actually running.
//
// 🔴 Only enforce does this, and this test is one half of a pair. Without
// TestObserveModeCountsTheMismatchWithoutRefusing, a gateway that stamped in
// every mode would pass here; without this one, a gateway that stamped in no mode
// would pass there. Neither is worth anything alone.
func TestDataPlaneRequestCarriesTheExpectedExecutionHeader(t *testing.T) {
	stamped := make(chan string, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		stamped <- r.Header.Get(headerExpectExecutionID)
		w.Header().Set(headerExecutionID, executionNewer)
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	server := newTestServer(t, stubSchedulerClient{
		lookupNodeFunc: boundToRegistry(&schedulerv1.Node{NodeId: "node-a", Endpoint: upstream.URL}, executionNewer),
	}, 5*time.Second, 4<<20, withExecutionFencing(config.GatewayExecutionFencingEnforce))

	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, dataPlaneRequest("sbx-1"))
	if response.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d (body %q)", response.Code, response.Body.String())
	}

	select {
	case got := <-stamped:
		if got != executionNewer {
			t.Fatalf("node saw expect header %q, want %q", got, executionNewer)
		}
	default:
		t.Fatal("the request never reached the node")
	}
}

// 🔴 The header is the gateway's assertion about its own routing decision, so a
// client-supplied one has to be overwritten rather than merged with — and when
// there is nothing authoritative to stamp, deleted rather than left in place.
// Without this the header is a fencing token any caller can forge.
//
// 🔴 The mode axis is load-bearing, not thoroughness. The two modes that stamp
// nothing are exactly the two that make a forged header dangerous: in off it
// would let a caller reach past a rolled-back gateway into whatever gate the
// nodes still carry, and in observe it would let a caller manufacture the one
// thing observe promises cannot happen — a 409, produced by a node the gateway
// never armed. Stripping is what makes "this mode refuses nothing" a property of
// the gateway rather than a hope about its clients.
func TestGatewayStripsClientSuppliedExecutionHeaders(t *testing.T) {
	for _, tc := range []struct {
		name       string
		mode       config.GatewayExecutionFencing
		authority  schedulerv1.ExecutionAuthority
		execution  string
		wantExpect string
	}{
		{
			name:       "authoritative answer overwrites the forged value",
			mode:       config.GatewayExecutionFencingEnforce,
			authority:  schedulerv1.ExecutionAuthority_EXECUTION_AUTHORITY_REGISTRY,
			execution:  executionNewer,
			wantExpect: executionNewer,
		},
		{
			name:      "no authority deletes the forged value rather than forwarding it",
			mode:      config.GatewayExecutionFencingEnforce,
			authority: schedulerv1.ExecutionAuthority_EXECUTION_AUTHORITY_UNKNOWN,
		},
		{
			// Authoritative, and still deleted: observe resolves the incarnation
			// and compares against it, but delegates nothing to the node — so the
			// forged value has no legitimate value to be overwritten by, and
			// forwarding it would hand the caller the refusal observe withheld.
			name:      "observe deletes the forged value rather than forwarding it",
			mode:      config.GatewayExecutionFencingObserve,
			authority: schedulerv1.ExecutionAuthority_EXECUTION_AUTHORITY_REGISTRY,
			execution: executionNewer,
		},
		{
			// The rollback rolls back this gateway, not the fleet's nodes. Off
			// removing headers is a deliberate deviation from "byte for byte as
			// before", stated in stampOutboundGatewayHeaders.
			name:      "off deletes the forged value rather than forwarding it",
			mode:      config.GatewayExecutionFencingOff,
			authority: schedulerv1.ExecutionAuthority_EXECUTION_AUTHORITY_REGISTRY,
			execution: executionNewer,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			seen := make(chan http.Header, 1)
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				seen <- r.Header.Clone()
				w.Header().Set(headerExecutionID, executionNewer)
				w.WriteHeader(http.StatusOK)
			}))
			defer upstream.Close()

			server := newTestServer(t, stubSchedulerClient{
				lookupNodeFunc: lookupNodeWithExecution(
					&schedulerv1.Node{NodeId: "node-a", Endpoint: upstream.URL},
					schedulerv1.SandboxLocation_SANDBOX_LOCATION_BOUND,
					tc.execution,
					tc.authority,
				),
			}, 5*time.Second, 4<<20, withExecutionFencing(tc.mode))

			request := dataPlaneRequest("sbx-1")
			request.Header.Set(headerExpectExecutionID, "forged-expect")
			request.Header.Set(headerExecutionID, "forged-echo")
			server.Handler().ServeHTTP(httptest.NewRecorder(), request)

			var header http.Header
			select {
			case header = <-seen:
			default:
				t.Fatal("the request never reached the node")
			}

			if got := header.Get(headerExpectExecutionID); got != tc.wantExpect {
				t.Fatalf("node saw expect header %q, want %q", got, tc.wantExpect)
			}
			if got := header.Get(headerExecutionID); got != "" {
				t.Fatalf("the client's echo header reached the node as %q; it is a response header and must be stripped", got)
			}
		})
	}
}

// Host-routed and header-routed traffic converge on one sandbox id before
// anything is decided, so both are fenced by the same code. This pins that they
// stay converged.
func TestHostRoutedDataPlaneIsFencedLikeHeaderRouted(t *testing.T) {
	stamped := make(chan string, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		stamped <- r.Header.Get(headerExpectExecutionID)
		w.Header().Set(headerExecutionID, executionNewer)
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	server := newTestServer(t, stubSchedulerClient{
		lookupNodeFunc: boundToRegistry(&schedulerv1.Node{NodeId: "node-a", Endpoint: upstream.URL}, executionNewer),
	}, 5*time.Second, 4<<20,
		withExecutionFencing(config.GatewayExecutionFencingEnforce),
		withSandboxProxyDomains("sandbox-proxy.example.invalid"),
	)

	request := httptest.NewRequest(http.MethodGet, "/anything", nil)
	request.Host = "8080-11111111-2222-3333-4444-555555555555.sandbox-proxy.example.invalid"
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d (body %q)", response.Code, response.Body.String())
	}
	select {
	case got := <-stamped:
		if got != executionNewer {
			t.Fatalf("host-routed request carried expect header %q, want %q", got, executionNewer)
		}
	default:
		t.Fatal("the request never reached the node")
	}
}

// 🔴 The control plane is never refused and never stamped.
//
// resume is the operation that mints a new incarnation, so a gate in front of it
// that compares against the old one would refuse the very thing it is waiting
// for. pause is here for the same reason in reverse: the registry's own
// transaction is the authoritative check, and a second one here, built from a
// lookup that may be a heartbeat behind, can only disagree with it.
func TestControlPlaneRequestIsNeverRefusedOnExecutionMismatch(t *testing.T) {
	for _, path := range []string{"/sandboxes/sbx-1/pause", "/sandboxes/sbx-1/resume"} {
		t.Run(path, func(t *testing.T) {
			stamped := make(chan string, 1)
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				stamped <- r.Header.Get(headerExpectExecutionID)
				// The node answers with an incarnation older than the one the
				// scheduler named — the exact shape the data plane refuses.
				w.Header().Set(headerExecutionID, executionOlder)
				w.WriteHeader(http.StatusOK)
			}))
			defer upstream.Close()

			server := newTestServer(t, stubSchedulerClient{
				lookupNodeFunc: boundToRegistry(&schedulerv1.Node{NodeId: "node-a", Endpoint: upstream.URL}, executionNewer),
			}, 5*time.Second, 4<<20, withExecutionFencing(config.GatewayExecutionFencingEnforce))

			response := httptest.NewRecorder()
			server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodPost, path, strings.NewReader("{}")))

			if response.Code != http.StatusOK {
				t.Fatalf("control-plane %s was answered %d (body %q); it must pass through", path, response.Code, response.Body.String())
			}
			select {
			case got := <-stamped:
				if got != "" {
					t.Fatalf("control-plane %s carried expect header %q; it must carry none", path, got)
				}
			default:
				t.Fatalf("control-plane %s never reached the node", path)
			}
		})
	}
}

// 🔴 PLACED and PINNED both mean the node is about to mint a new incarnation, so
// any incarnation on the answer names the previous one. Stamping it would refuse
// every auto-resume the data plane triggers — and auto-resume is the ordinary
// way a paused sandbox comes back.
func TestPlacedAndPinnedSandboxesAreNeverFenced(t *testing.T) {
	for _, location := range []schedulerv1.SandboxLocation{
		schedulerv1.SandboxLocation_SANDBOX_LOCATION_PLACED,
		schedulerv1.SandboxLocation_SANDBOX_LOCATION_PINNED,
	} {
		t.Run(gatewaySandboxLocationLabel(location), func(t *testing.T) {
			stamped := make(chan string, 1)
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				stamped <- r.Header.Get(headerExpectExecutionID)
				w.Header().Set(headerSandboxID, "sbx-1")
				// The freshly minted incarnation, which is older than nothing
				// and would be refused against a stale expect.
				w.Header().Set(headerExecutionID, executionOlder)
				w.WriteHeader(http.StatusOK)
			}))
			defer upstream.Close()

			pendingBefore := fencingCounter(t, fencingPlaneData, fencingDecisionPending)

			server := newTestServer(t, stubSchedulerClient{
				lookupNodeFunc: lookupNodeWithExecution(
					&schedulerv1.Node{NodeId: "node-a", Endpoint: upstream.URL},
					location,
					// A scheduler that leaks the previous incarnation onto a
					// pending answer must still not cause a stamp: the authority
					// decides, never the value.
					executionNewer,
					schedulerv1.ExecutionAuthority_EXECUTION_AUTHORITY_PENDING,
				),
				recordAssignmentFunc: func(context.Context, *schedulerv1.RecordAssignmentRequest, ...grpc.CallOption) (*schedulerv1.RecordAssignmentResponse, error) {
					return &schedulerv1.RecordAssignmentResponse{}, nil
				},
			}, 5*time.Second, 4<<20, withExecutionFencing(config.GatewayExecutionFencingEnforce))

			response := httptest.NewRecorder()
			server.Handler().ServeHTTP(response, dataPlaneRequest("sbx-1"))

			if response.Code != http.StatusOK {
				t.Fatalf("a %s sandbox was answered %d (body %q); it must not be fenced",
					gatewaySandboxLocationLabel(location), response.Code, response.Body.String())
			}
			select {
			case got := <-stamped:
				if got != "" {
					t.Fatalf("a %s sandbox carried expect header %q; nothing may be expected of a node about to mint one",
						gatewaySandboxLocationLabel(location), got)
				}
			default:
				t.Fatal("the request never reached the node")
			}
			if got := fencingCounter(t, fencingPlaneData, fencingDecisionPending) - pendingBefore; got != 1 {
				t.Fatalf("the pending series moved by %v, want 1", got)
			}
		})
	}
}

// The second gate, on the way back: the node answered from an incarnation the
// control plane has moved past.
func TestDataPlaneRequestRefusesWhenNodeEchoesAnOlderExecution(t *testing.T) {
	const nodeBody = "this body came from a superseded incarnation"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(headerExecutionID, executionOlder)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(nodeBody))
	}))
	defer upstream.Close()

	refusedBefore := fencingCounter(t, fencingPlaneData, fencingDecisionRefusedEcho)

	server := newTestServer(t, stubSchedulerClient{
		lookupNodeFunc: boundToRegistry(&schedulerv1.Node{NodeId: "node-a", Endpoint: upstream.URL}, executionNewer),
	}, 5*time.Second, 4<<20, withExecutionFencing(config.GatewayExecutionFencingEnforce))

	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, dataPlaneRequest("sbx-1"))

	if response.Code != http.StatusConflict {
		t.Fatalf("expected status 409, got %d (body %q)", response.Code, response.Body.String())
	}
	if strings.Contains(response.Body.String(), nodeBody) {
		t.Fatalf("the superseded node's body reached the client: %q", response.Body.String())
	}
	if got := response.Header().Get(headerRefusal); got != refusalCodeExecutionSuperseded {
		t.Fatalf("refusal header is %q, want %q", got, refusalCodeExecutionSuperseded)
	}

	var body executionRefusalBody
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("refusal body is not JSON: %v (body %q)", err, response.Body.String())
	}
	if body.Code != refusalCodeExecutionSuperseded {
		t.Fatalf("refusal code is %q, want %q", body.Code, refusalCodeExecutionSuperseded)
	}
	if body.RefusedBy != refusedByGateway {
		t.Fatalf("refusedBy is %q, want %q", body.RefusedBy, refusedByGateway)
	}
	if body.ExpectedExecutionID != executionNewer || body.ObservedExecutionID != executionOlder {
		t.Fatalf("refusal named expected=%q observed=%q, want %q and %q",
			body.ExpectedExecutionID, body.ObservedExecutionID, executionNewer, executionOlder)
	}
	if got := fencingCounter(t, fencingPlaneData, fencingDecisionRefusedEcho) - refusedBefore; got != 1 {
		t.Fatalf("the echo refusal series moved by %v, want 1", got)
	}
}

// The node refused before running anything, using the internal 412. The client
// gets the one external shape instead, and never sees the 412 or the internal
// header pair.
func TestDataPlaneRequestTranslatesNodePreconditionRefusal(t *testing.T) {
	const nodeBody = `{"error":"an internal refusal shape"}`
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(headerExecutionID, executionOlder)
		w.Header().Set(headerRefusal, refusalCodeExecutionSuperseded)
		w.WriteHeader(http.StatusPreconditionFailed)
		_, _ = w.Write([]byte(nodeBody))
	}))
	defer upstream.Close()

	refusedBefore := fencingCounter(t, fencingPlaneData, fencingDecisionRefusedPreflight)

	server := newTestServer(t, stubSchedulerClient{
		lookupNodeFunc: boundToRegistry(&schedulerv1.Node{NodeId: "node-a", Endpoint: upstream.URL}, executionNewer),
	}, 5*time.Second, 4<<20, withExecutionFencing(config.GatewayExecutionFencingEnforce))

	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, dataPlaneRequest("sbx-1"))

	if response.Code != http.StatusConflict {
		t.Fatalf("expected the node's 412 to be translated to 409, got %d (body %q)", response.Code, response.Body.String())
	}
	if strings.Contains(response.Body.String(), "an internal refusal shape") {
		t.Fatalf("the node's own refusal body reached the client: %q", response.Body.String())
	}

	var body executionRefusalBody
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("refusal body is not JSON: %v (body %q)", err, response.Body.String())
	}
	if body.RefusedBy != refusedByNode {
		t.Fatalf("refusedBy is %q, want %q — a preflight refusal ran nothing and has to say so", body.RefusedBy, refusedByNode)
	}
	if got := fencingCounter(t, fencingPlaneData, fencingDecisionRefusedPreflight) - refusedBefore; got != 1 {
		t.Fatalf("the preflight refusal series moved by %v, want 1", got)
	}
}

// 🔴 The control group. Without it, an implementation that refuses everything
// makes every refusal test above pass.
func TestMatchingExecutionPassesThrough(t *testing.T) {
	const nodeBody = "the node's own answer, verbatim"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(headerExecutionID, executionNewer)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(nodeBody))
	}))
	defer upstream.Close()

	server := newTestServer(t, stubSchedulerClient{
		lookupNodeFunc: boundToRegistry(&schedulerv1.Node{NodeId: "node-a", Endpoint: upstream.URL}, executionNewer),
	}, 5*time.Second, 4<<20, withExecutionFencing(config.GatewayExecutionFencingEnforce))

	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, dataPlaneRequest("sbx-1"))

	if response.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d (body %q)", response.Code, response.Body.String())
	}
	if response.Body.String() != nodeBody {
		t.Fatalf("body is %q, want the node's own %q", response.Body.String(), nodeBody)
	}
}

// 🔴 The second control group, and the one that matters most in production.
//
// A node whose incarnation is newer than the scheduler's is the ordinary
// same-machine changeover: idle pause runs on a one second tick and the data
// plane resumes on demand, so pause→resume on one node happens constantly and
// the centre is routinely a heartbeat behind. An equality comparison would turn
// every one of those into a 409, and TestDataPlaneRequestRefusesWhenNodeEchoes-
// AnOlderExecution would stay green while it happened.
func TestNodeAheadOfTheControlPlanePassesThrough(t *testing.T) {
	const nodeBody = "answered by the incarnation that replaced the expected one"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(headerExecutionID, executionNewer)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(nodeBody))
	}))
	defer upstream.Close()

	aheadBefore := fencingCounter(t, fencingPlaneData, fencingDecisionUnfencedNodeAhead)

	server := newTestServer(t, stubSchedulerClient{
		// The scheduler still names the older incarnation.
		lookupNodeFunc: boundToRegistry(&schedulerv1.Node{NodeId: "node-a", Endpoint: upstream.URL}, executionOlder),
	}, 5*time.Second, 4<<20, withExecutionFencing(config.GatewayExecutionFencingEnforce))

	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, dataPlaneRequest("sbx-1"))

	if response.Code != http.StatusOK {
		t.Fatalf("a node ahead of the scheduler was answered %d (body %q); it must pass", response.Code, response.Body.String())
	}
	if response.Body.String() != nodeBody {
		t.Fatalf("body is %q, want the node's own %q", response.Body.String(), nodeBody)
	}
	if got := fencingCounter(t, fencingPlaneData, fencingDecisionUnfencedNodeAhead) - aheadBefore; got != 1 {
		t.Fatalf("the node-ahead series moved by %v, want 1", got)
	}
}

// 🔴 The same-case rule on the data plane.
//
// The node reports the incarnation it is running, and nothing forces the case it
// reports it in — the registry's own id validator accepts either. Compared as
// they arrive, an upper-cased newer incarnation sorts before a lower-cased older
// one, so the node ahead of the centre reads as the node behind it, and the one
// request that must always pass is refused instead.
func TestIncarnationsAreComparedAfterBeingLowerCased(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(headerExecutionID, strings.ToUpper(executionNewer))
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	aheadBefore := fencingCounter(t, fencingPlaneData, fencingDecisionUnfencedNodeAhead)

	server := newTestServer(t, stubSchedulerClient{
		lookupNodeFunc: boundToRegistry(&schedulerv1.Node{NodeId: "node-a", Endpoint: upstream.URL}, executionOlder),
	}, 5*time.Second, 4<<20, withExecutionFencing(config.GatewayExecutionFencingEnforce))

	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, dataPlaneRequest("sbx-1"))

	if response.Code != http.StatusOK {
		t.Fatalf("an upper-cased newer incarnation was answered %d; case must not decide the ordering", response.Code)
	}
	if got := fencingCounter(t, fencingPlaneData, fencingDecisionUnfencedNodeAhead) - aheadBefore; got != 1 {
		t.Fatalf("the node-ahead series moved by %v, want 1", got)
	}
}

// A pass the gateway could not protect is still a pass, but it is not the same
// event as a protected one and must not be recorded as one. The absolute value
// of this series is the size of the coverage gap.
func TestUnfencedRequestIsCountedNotSilentlyAllowed(t *testing.T) {
	for _, authority := range []schedulerv1.ExecutionAuthority{
		schedulerv1.ExecutionAuthority_EXECUTION_AUTHORITY_UNKNOWN,
		// An older scheduler. It has to behave identically to UNKNOWN, or the
		// fleet gets two behaviours for one situation for a whole rollout.
		schedulerv1.ExecutionAuthority_EXECUTION_AUTHORITY_UNSPECIFIED,
	} {
		t.Run(authority.String(), func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set(headerExecutionID, executionOlder)
				w.WriteHeader(http.StatusOK)
			}))
			defer upstream.Close()

			before := fencingCounter(t, fencingPlaneData, fencingDecisionUnfencedNoAuthority)

			server := newTestServer(t, stubSchedulerClient{
				lookupNodeFunc: lookupNodeWithExecution(
					&schedulerv1.Node{NodeId: "node-a", Endpoint: upstream.URL},
					schedulerv1.SandboxLocation_SANDBOX_LOCATION_BOUND,
					"",
					authority,
				),
			}, 5*time.Second, 4<<20, withExecutionFencing(config.GatewayExecutionFencingEnforce))

			response := httptest.NewRecorder()
			server.Handler().ServeHTTP(response, dataPlaneRequest("sbx-1"))

			if response.Code != http.StatusOK {
				t.Fatalf("expected status 200, got %d (body %q)", response.Code, response.Body.String())
			}
			if got := fencingCounter(t, fencingPlaneData, fencingDecisionUnfencedNoAuthority) - before; got != 1 {
				t.Fatalf("the unfenced series moved by %v, want 1", got)
			}
		})
	}
}

// 🔴 A node that answers without an echo does not implement fencing, and this
// count is the only way anyone finds that out. It has to read zero across the
// fleet before enforce is turned on; without the count, a gateway enforcing
// nothing and a gateway enforcing everything look the same from outside.
func TestNodeWithoutEchoHeaderIsCountedAsUnfenced(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	before := fencingCounter(t, fencingPlaneData, fencingDecisionUnfencedNodeSilent)

	server := newTestServer(t, stubSchedulerClient{
		lookupNodeFunc: boundToRegistry(&schedulerv1.Node{NodeId: "node-a", Endpoint: upstream.URL}, executionNewer),
	}, 5*time.Second, 4<<20, withExecutionFencing(config.GatewayExecutionFencingEnforce))

	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, dataPlaneRequest("sbx-1"))

	if response.Code != http.StatusOK {
		t.Fatalf("a node that does not echo was answered %d; it must pass", response.Code)
	}
	if got := fencingCounter(t, fencingPlaneData, fencingDecisionUnfencedNodeSilent) - before; got != 1 {
		t.Fatalf("the node-silent series moved by %v, want 1", got)
	}
}

// newFencingNode is a node that behaves the way a node carrying the A5 receiving
// end behaves (node design §3.7): it echoes the incarnation it is running on
// every response, pass or refuse, and it answers 412 plus the internal refusal
// header — before doing anything — when it was handed an expect header naming an
// incarnation newer than its own.
//
// 🔴 The second half is what makes the observe tests below mean something. A stub
// that ignored the expect header would answer 200 whether or not the gateway
// stamped one, so every assertion about observe not stamping would hold just as
// well against a gateway that stamps.
func newFencingNode(live string, body string) (*httptest.Server, <-chan http.Header) {
	seen := make(chan http.Header, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case seen <- r.Header.Clone():
		default:
		}
		w.Header().Set(headerExecutionID, live)
		if expect := normalizeExecutionID(r.Header.Get(headerExpectExecutionID)); expect != "" && live < expect {
			w.Header().Set(headerRefusal, refusalCodeExecutionSuperseded)
			w.WriteHeader(http.StatusPreconditionFailed)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body))
	}))
	return server, seen
}

// 🔴 observe is the dry run, and a dry run that can still hand a client a 409 is
// not one.
//
// The mode exists for one step of the rollout — prove the wiring end to end while
// user traffic is untouched — and the gateway's own refusal is the easy half to
// withhold. The node's is not: it is delegated the moment an expect header goes
// out, and a 412 the node has already produced can only be translated, never
// withdrawn. So observe delegates nothing. It stamps no expect header, and takes
// its entire reading off the echo the node sends regardless of what was expected
// of it, which costs it no observability at all.
//
// The node here is the real gate rather than a passthrough, so the mutation this
// test is aimed at — observe stamping again — fails it the way a user would see
// it, as a 409, and not merely as a missing header.
func TestObserveModeCountsTheMismatchWithoutRefusing(t *testing.T) {
	const nodeBody = "served by a superseded incarnation, and delivered anyway"
	// The node is running the older incarnation; the scheduler names the newer
	// one. This is precisely the input enforce refuses.
	upstream, seen := newFencingNode(executionOlder, nodeBody)
	defer upstream.Close()

	echoBefore := fencingCounter(t, fencingPlaneData, fencingDecisionRefusedEcho)
	preflightBefore := fencingCounter(t, fencingPlaneData, fencingDecisionRefusedPreflight)

	logs, logged := observer.New(zap.WarnLevel)
	server := newTestServerWithLogger(t, zap.New(logs), stubSchedulerClient{
		lookupNodeFunc: boundToRegistry(&schedulerv1.Node{NodeId: "node-a", Endpoint: upstream.URL}, executionNewer),
	}, 5*time.Second, 4<<20, withExecutionFencing(config.GatewayExecutionFencingObserve))

	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, dataPlaneRequest("sbx-1"))

	// 1. The client is untouched. Not "refused with a friendlier code" — served.
	if response.Code != http.StatusOK {
		t.Fatalf("observe answered %d (body %q); the dry run may not cost a single client a request",
			response.Code, response.Body.String())
	}
	if response.Body.String() != nodeBody {
		t.Fatalf("body is %q, want the node's own %q", response.Body.String(), nodeBody)
	}

	// 2. And it is untouched because nothing was ever delegated. A gateway that
	// stamped would already have failed above, on the 409; this says why.
	var header http.Header
	select {
	case header = <-seen:
	default:
		t.Fatal("the request never reached the node")
	}
	if got := header.Get(headerExpectExecutionID); got != "" {
		t.Fatalf("observe stamped expect header %q; stamping is handing the node a refusal the gateway cannot take back", got)
	}

	// 3. The whole point of the round: the mismatch was seen and written down.
	if got := fencingCounter(t, fencingPlaneData, fencingDecisionRefusedEcho) - echoBefore; got != 1 {
		t.Fatalf("the echo series moved by %v, want 1 — observe measures exactly what enforce would refuse", got)
	}
	// The control: no preflight refusal can exist in observe, because none was
	// armed. If this moves, the 200 above came from somewhere other than the node.
	if got := fencingCounter(t, fencingPlaneData, fencingDecisionRefusedPreflight) - preflightBefore; got != 0 {
		t.Fatalf("the preflight series moved by %v in observe; nothing was stamped, so nothing could have been refused up front", got)
	}

	// 4. The log trail, by field rather than by message. During the observe round
	// the response is identical to a healthy one, so the counter and this line are
	// the only two artifacts an operator has — and the counter alone cannot say
	// which sandbox, which node or which pair of incarnations disagreed.
	entries := logged.FilterField(zap.String("fencing_stage", fencingStageGatewayRoute)).All()
	if len(entries) != 1 {
		t.Fatalf("observe wrote %d gateway_route fencing lines, want exactly 1", len(entries))
	}
	fields := entries[0].ContextMap()
	for name, want := range map[string]any{
		"sandbox_id":            "sbx-1",
		"node_id":               "node-a",
		"expected_execution_id": executionNewer,
		"observed_execution_id": executionOlder,
		"refusal_code":          refusalCodeExecutionSuperseded,
		"refused_by":            refusedByGateway,
	} {
		if got := fields[name]; got != want {
			t.Fatalf("the observe log line carries %s=%v, want %v", name, got, want)
		}
	}
}

// 🔴 The one branch the observe decision leaves unreachable, and why it stays.
//
// observe stamps nothing, so a node that implements the gate has nothing to
// compare against and cannot answer 412: in the fleet as it is wired, this case
// does not arise. It is translated anyway because the two failure costs are not
// symmetric. An unreachable translation costs one comparison. A missing one hands
// a client the internal 412 and the internal x-agentenv-refusal header the first
// time anything else in the fleet produces them — a second gateway on enforce, a
// mode flipped under a request already in flight, a middlebox replaying a
// refusal — and an internal signal on the outside is the one thing §6's code
// table forbids outright.
//
// This test is what stops the branch from being deleted as dead code, and it is
// not in tension with the test above: there, observe produces no refusal because
// none was armed; here, the refusal arrives from outside observe's control and
// the only remaining question is what shape it reaches the client in.
func TestObserveModeStillTranslatesARogueNodeRefusal(t *testing.T) {
	const nodeBody = `{"error":"an internal refusal shape"}`
	// A node refusing without having been asked to — the shape observe cannot
	// cause but also cannot rule out.
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(headerExecutionID, executionOlder)
		w.Header().Set(headerRefusal, refusalCodeExecutionSuperseded)
		w.WriteHeader(http.StatusPreconditionFailed)
		_, _ = w.Write([]byte(nodeBody))
	}))
	defer upstream.Close()

	before := fencingCounter(t, fencingPlaneData, fencingDecisionRefusedPreflight)

	server := newTestServer(t, stubSchedulerClient{
		lookupNodeFunc: boundToRegistry(&schedulerv1.Node{NodeId: "node-a", Endpoint: upstream.URL}, executionNewer),
	}, 5*time.Second, 4<<20, withExecutionFencing(config.GatewayExecutionFencingObserve))

	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, dataPlaneRequest("sbx-1"))

	if response.Code == http.StatusPreconditionFailed {
		t.Fatal("a node's 412 reached the client in observe; 412 and x-agentenv-refusal are internal signals in every mode")
	}
	if response.Code != http.StatusConflict {
		t.Fatalf("the node's 412 arrived as %d, want the one external shape 409", response.Code)
	}
	if strings.Contains(response.Body.String(), "an internal refusal shape") {
		t.Fatalf("the node's own refusal body reached the client: %q", response.Body.String())
	}

	var body executionRefusalBody
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("refusal body is not JSON: %v (body %q)", err, response.Body.String())
	}
	if body.RefusedBy != refusedByNode {
		t.Fatalf("refusedBy is %q, want %q — observe did not refuse this, the node did", body.RefusedBy, refusedByNode)
	}
	if got := fencingCounter(t, fencingPlaneData, fencingDecisionRefusedPreflight) - before; got != 1 {
		t.Fatalf("the preflight series moved by %v, want 1 — a refusal observe cannot explain is the one it most has to record", got)
	}
}

// TestTheObserveLineDoesNotClaimToHaveRefused pins the one thing the log line
// says that the field set does not.
//
// 🔴 Both live modes reach the same line with the same fields, and under observe
// the request is answered 200. A single message saying "refused" therefore turns
// the entire observe round into a log full of refusals that never happened —
// and the observe round is precisely when somebody greps for them, because the
// response is identical to a healthy one and the log is half of what is left.
//
// The upstream here does not implement the node-side gate: it always answers 200
// with an older incarnation. That is deliberate — it forces both modes through
// the *echo* branch, the one whose outcome differs between them, rather than
// through the node's 412, which is refused in either mode.
func TestTheObserveLineDoesNotClaimToHaveRefused(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(headerExecutionID, executionOlder)
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	for _, tc := range []struct {
		name        string
		mode        config.GatewayExecutionFencing
		wantStatus  int
		wantMessage string
		// wantSaysRefused is asserted against the message text rather than
		// against the constant, so collapsing the two constants onto either of
		// the two texts fails one of these rows.
		wantSaysRefused bool
	}{
		{
			name:            "observe measured it and served it",
			mode:            config.GatewayExecutionFencingObserve,
			wantStatus:      http.StatusOK,
			wantMessage:     logMsgExecutionObserved,
			wantSaysRefused: false,
		},
		{
			name:            "enforce ended the request",
			mode:            config.GatewayExecutionFencingEnforce,
			wantStatus:      http.StatusConflict,
			wantMessage:     logMsgExecutionRefused,
			wantSaysRefused: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs, logged := observer.New(zap.WarnLevel)
			server := newTestServerWithLogger(t, zap.New(logs), stubSchedulerClient{
				lookupNodeFunc: boundToRegistry(&schedulerv1.Node{NodeId: "node-a", Endpoint: upstream.URL}, executionNewer),
			}, 5*time.Second, 4<<20, withExecutionFencing(tc.mode))

			response := httptest.NewRecorder()
			server.Handler().ServeHTTP(response, dataPlaneRequest("sbx-1"))

			// The control: without it, a build that refused nothing at all would
			// pass the "observe does not say refused" row for the wrong reason.
			if response.Code != tc.wantStatus {
				t.Fatalf("answered %d, want %d (body %q)", response.Code, tc.wantStatus, response.Body.String())
			}

			entries := logged.FilterField(zap.String("fencing_stage", fencingStageGatewayRoute)).All()
			if len(entries) != 1 {
				t.Fatalf("wrote %d gateway_route fencing lines, want exactly 1", len(entries))
			}
			if entries[0].Message != tc.wantMessage {
				t.Fatalf("the log line reads %q, want %q", entries[0].Message, tc.wantMessage)
			}
			if saysRefused := strings.Contains(entries[0].Message, "refused"); saysRefused != tc.wantSaysRefused {
				t.Fatalf("the log line %q says refused=%v, want %v — a request answered %d must not be written down as the opposite",
					entries[0].Message, saysRefused, tc.wantSaysRefused, response.Code)
			}

			// 🔴 The field set is the frozen half of this contract (the
			// three-stage trail, impl plan §11.1(g)): the message text was split
			// precisely because
			// the fields could not be. Both messages must still carry all six.
			fields := entries[0].ContextMap()
			for _, name := range []string{
				"sandbox_id", "node_id", "expected_execution_id",
				"observed_execution_id", "refusal_code", "refused_by",
			} {
				if _, present := fields[name]; !present {
					t.Fatalf("the %q line dropped field %q; the fields are the part operators join on across the three stages", tc.name, name)
				}
			}
		})
	}

	if logMsgExecutionObserved == logMsgExecutionRefused {
		t.Fatal("the two messages are the same string again; one message for two outcomes is the defect this test exists for")
	}
}

// 🔴 This test's reason for existing is not that 409 looks nicer than the
// alternatives.
//
// The platform decides that a sandbox is gone, and that it may rebuild the
// user's workspace from scratch, on exactly one signal: a 404 from resume. That
// 404 is produced by this service, in writeSchedulerError, without the request
// ever reaching a node. A fencing refusal that arrives as a 404 is therefore read
// as "the sandbox no longer exists", and the cost of the misreading is a user's
// workspace deleted with nothing logged as an error.
//
// Three layers, and all three are needed: the first covers the paths as they are
// wired today, the second covers the constructor so that a new call site cannot
// escape it, and the third is the reverse control — without it, a change that
// turned every 404 in this file into a 409 would leave the first two green.
func TestFencingRefusalIsNeverFourOhFour(t *testing.T) {
	// Layer 1: every path that can refuse, as wired.
	for _, tc := range []struct {
		name     string
		upstream http.HandlerFunc
	}{
		{
			name: "the node refused before running anything",
			upstream: func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set(headerExecutionID, executionOlder)
				w.Header().Set(headerRefusal, refusalCodeExecutionSuperseded)
				w.WriteHeader(http.StatusPreconditionFailed)
			},
		},
		{
			name: "the gateway caught it on the way back",
			upstream: func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set(headerExecutionID, executionOlder)
				w.WriteHeader(http.StatusOK)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			upstream := httptest.NewServer(tc.upstream)
			defer upstream.Close()

			server := newTestServer(t, stubSchedulerClient{
				lookupNodeFunc: boundToRegistry(&schedulerv1.Node{NodeId: "node-a", Endpoint: upstream.URL}, executionNewer),
			}, 5*time.Second, 4<<20, withExecutionFencing(config.GatewayExecutionFencingEnforce))

			response := httptest.NewRecorder()
			server.Handler().ServeHTTP(response, dataPlaneRequest("sbx-1"))

			switch response.Code {
			case http.StatusNotFound:
				t.Fatal("a fencing refusal answered 404; the platform reads that as permission to rebuild the workspace")
			case http.StatusGone:
				t.Fatal("a fencing refusal answered 410, which the node's own proxy already uses for 'not proxyable'")
			case http.StatusServiceUnavailable:
				t.Fatal("a fencing refusal answered 503, which means 'this may have changed a moment later' — a superseded incarnation has not")
			case http.StatusBadGateway:
				t.Fatal("a fencing refusal answered 502, which says the upstream is broken; it is not, we are declining to use it")
			case http.StatusConflict:
			default:
				t.Fatalf("a fencing refusal answered %d, want 409", response.Code)
			}
		})
	}

	// Layer 2: the constructor itself, so that a call site added later cannot
	// quietly answer with something else.
	t.Run("the refusal constructor", func(t *testing.T) {
		refusal := executionSupersededRefusal("sbx-1", executionNewer, executionOlder, refusedByGateway)
		if refusal.statusCode != http.StatusConflict {
			t.Fatalf("executionSupersededRefusal returns %d, want 409", refusal.statusCode)
		}
		for _, forbidden := range []int{http.StatusNotFound, http.StatusGone, http.StatusServiceUnavailable, http.StatusBadGateway} {
			if refusal.statusCode == forbidden {
				t.Fatalf("executionSupersededRefusal returns %d, which is reserved for another meaning on this path", forbidden)
			}
		}
	})
}

// 🔴 The reverse control for the test above. A scheduler NotFound still has to
// arrive as a 404: it is the signal the platform is entitled to act on, and the
// blanket "no more 404s here" change that would satisfy the previous test must
// fail this one.
func TestSchedulerNotFoundStillMapsToFourOhFour(t *testing.T) {
	server := newTestServer(t, stubSchedulerClient{
		lookupNodeFunc: func(context.Context, *schedulerv1.LookupNodeRequest, ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			return nil, status.Error(codes.NotFound, "sandbox assignment not found")
		},
	}, 5*time.Second, 4<<20, withExecutionFencing(config.GatewayExecutionFencingEnforce))

	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/sandboxes/sbx-1/resume", strings.NewReader("{}")))

	if response.Code != http.StatusNotFound {
		t.Fatalf("a scheduler NotFound arrived as %d, want 404", response.Code)
	}
}

// 🔴 The Scheduler service may never answer PermissionDenied.
//
// writeSchedulerError has no branch for it, so it falls through to the default
// and becomes a 502 — "the upstream is broken", which is a wrong diagnosis of a
// precise refusal. The code is spoken by PausedRegistry, where it means a
// superseded incarnation tried to write, and nodes reach that service directly
// without passing through here.
//
// The method list is frozen deliberately. Moving a refusing method onto this
// service, or adding one, is exactly the change that would turn a fencing
// refusal into a 502 with nothing to notice it, and the only mechanical way to
// require that decision to be made on purpose is to make it break this list.
func TestSchedulerServiceNeverReturnsPermissionDenied(t *testing.T) {
	frozen := map[string]struct{}{
		"Schedule":           {},
		"ListNodes":          {},
		"LookupNode":         {},
		"RecordAssignment":   {},
		"Heartbeat":          {},
		"ListObservedNodes":  {},
		"ReportSandboxEvent": {},
		"ListP2pPeers":       {},
		"RecordP2pArtifact":  {},
		"ForgetP2pArtifact":  {},
		"LookupP2pArtifact":  {},
		"GetNode":            {},
		"UnregisterNode":     {},
		// Read-only listing of the registry. It is on this service rather than
		// on PausedRegistry because it is answered for operators through the
		// gateway, and it cannot refuse a write because it performs none.
		"ListRegistrySandboxes": {},
	}

	for _, method := range schedulerv1.Scheduler_ServiceDesc.Methods {
		if _, ok := frozen[method.MethodName]; !ok {
			t.Fatalf("Scheduler grew the method %q. Before adding it here: it must not answer PermissionDenied, "+
				"because writeSchedulerError has no branch for that code and turns it into a 502.", method.MethodName)
		}
		delete(frozen, method.MethodName)
	}
	for method := range frozen {
		t.Fatalf("Scheduler no longer serves %q; remove it from this list once you have checked where it went — "+
			"if it moved to a service the gateway calls, the PermissionDenied hazard moved with it.", method)
	}

	// And the hazard itself, stated as a fact rather than as a comment: this is
	// what a PermissionDenied from this service costs today.
	server := newTestServer(t, stubSchedulerClient{
		lookupNodeFunc: func(context.Context, *schedulerv1.LookupNodeRequest, ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			return nil, status.Error(codes.PermissionDenied, "a refusal that has no branch here")
		},
	}, 5*time.Second, 4<<20, withExecutionFencing(config.GatewayExecutionFencingEnforce))

	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, dataPlaneRequest("sbx-1"))
	if response.Code != http.StatusBadGateway {
		t.Fatalf("PermissionDenied from the scheduler arrived as %d; this test's premise (that it becomes a 502) "+
			"is stale and the frozen list above needs revisiting", response.Code)
	}
}

// 🔴 The rollback has to be a rollback: off behaves as the gateway did before
// any of this existed. The input is the one that every other mode reacts to —
// an answer and an echo that disagree — so a single surviving branch shows up
// here as a refusal, a header, or a count.
func TestFencingOffMatchesLegacyBehaviour(t *testing.T) {
	const nodeBody = "the node's answer, untouched"
	seen := make(chan http.Header, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen <- r.Header.Clone()
		w.Header().Set(headerExecutionID, executionOlder)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(nodeBody))
	}))
	defer upstream.Close()

	refusedBefore := fencingCounter(t, fencingPlaneData, fencingDecisionRefusedEcho)
	passBefore := fencingCounter(t, fencingPlaneData, fencingDecisionEnforcedPass)
	silentBefore := fencingCounter(t, fencingPlaneData, fencingDecisionUnfencedNodeSilent)
	offBefore := fencingCounter(t, fencingPlaneData, fencingDecisionOff)

	server := newTestServer(t, stubSchedulerClient{
		lookupNodeFunc: boundToRegistry(&schedulerv1.Node{NodeId: "node-a", Endpoint: upstream.URL}, executionNewer),
	}, 5*time.Second, 4<<20, withExecutionFencing(config.GatewayExecutionFencingOff))

	request := dataPlaneRequest("sbx-1")
	request.Header.Set(headerExpectExecutionID, "forged-expect")
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("off answered %d (body %q); it must refuse nothing", response.Code, response.Body.String())
	}
	if response.Body.String() != nodeBody {
		t.Fatalf("body is %q, want the node's own %q", response.Body.String(), nodeBody)
	}

	var header http.Header
	select {
	case header = <-seen:
	default:
		t.Fatal("the request never reached the node")
	}
	if got := header.Get(headerExpectExecutionID); got == executionNewer {
		t.Fatalf("off stamped the expect header %q; the rollback must put nothing new on the wire", got)
	}

	for _, tc := range []struct {
		decision string
		before   float64
	}{
		{fencingDecisionRefusedEcho, refusedBefore},
		{fencingDecisionEnforcedPass, passBefore},
		{fencingDecisionUnfencedNodeSilent, silentBefore},
	} {
		if got := fencingCounter(t, fencingPlaneData, tc.decision) - tc.before; got != 0 {
			t.Fatalf("off recorded %v under %q; it must reach no comparison at all", got, tc.decision)
		}
	}
	// The one thing off does record: that it was off. Without it, a gateway with
	// the switch pulled and a gateway serving no traffic look identical.
	if got := fencingCounter(t, fencingPlaneData, fencingDecisionOff) - offBefore; got != 1 {
		t.Fatalf("the off series moved by %v, want 1", got)
	}
}

// 🔴 The gateway identifies itself to a node on every request it forwards, and
// it overwrites whatever the client sent under that name. "Add it if it is
// missing" would not be a weaker version of this — it would hand any caller the
// ability to present itself as the control plane by setting one header.
func TestGatewayStampsTheControlPlaneTokenOnForwardedRequests(t *testing.T) {
	for _, tc := range []struct {
		name      string
		token     string
		clientSet string
		want      string
	}{
		{name: "configured token overwrites the client's", token: "the-real-token", clientSet: "a-forged-token", want: "the-real-token"},
		{name: "configured token is stamped when the client sent none", token: "the-real-token", want: "the-real-token"},
		// 🔴 Deleted, not left in place: with no token of our own, forwarding
		// the client's would make the gateway a pipe for exactly the identity
		// the node's gate exists to check.
		{name: "no token deletes the client's", clientSet: "a-forged-token", want: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			seen := make(chan string, 1)
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				seen <- r.Header.Get(headerControlPlane)
				w.WriteHeader(http.StatusOK)
			}))
			defer upstream.Close()

			server := newTestServer(t, stubSchedulerClient{
				lookupNodeFunc: boundToRegistry(&schedulerv1.Node{NodeId: "node-a", Endpoint: upstream.URL}, executionNewer),
			}, 5*time.Second, 4<<20, withControlPlaneToken(tc.token))

			request := httptest.NewRequest(http.MethodPost, "/sandboxes/sbx-1/pause", strings.NewReader("{}"))
			if tc.clientSet != "" {
				request.Header.Set(headerControlPlane, tc.clientSet)
			}
			server.Handler().ServeHTTP(httptest.NewRecorder(), request)

			select {
			case got := <-seen:
				if got != tc.want {
					t.Fatalf("node saw control-plane header %q, want %q", got, tc.want)
				}
			default:
				t.Fatal("the request never reached the node")
			}
		})
	}
}

// 🔴 The cluster listing fans out to every node with the gateway's own HTTP
// client, not through the reverse proxy, so it does not inherit the Rewrite
// hook's headers. It is also all-or-nothing: one node refusing takes the whole
// listing with it. Missing this means the listing returns 502 for the fleet the
// moment the node-side gate is switched on, and nothing in the proxy path would
// have shown it.
func TestClusterListFanOutCarriesTheControlPlaneToken(t *testing.T) {
	seen := make(chan http.Header, 1)
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen <- r.Header.Clone()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[]`))
	}))
	defer node.Close()

	server := newTestServer(t, stubSchedulerClient{
		listNodesFunc: func(context.Context, *schedulerv1.ListNodesRequest, ...grpc.CallOption) (*schedulerv1.ListNodesResponse, error) {
			return &schedulerv1.ListNodesResponse{Nodes: []*schedulerv1.Node{{NodeId: "node-a", Endpoint: node.URL}}}, nil
		},
	}, 5*time.Second, 4<<20, withControlPlaneToken("the-real-token"))

	request := httptest.NewRequest(http.MethodGet, "/sandboxes", nil)
	request.Header.Set(headerControlPlane, "a-forged-token")
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d (body %q)", response.Code, response.Body.String())
	}
	select {
	case header := <-seen:
		if got := header.Get(headerControlPlane); got != "the-real-token" {
			t.Fatalf("the fan-out carried control-plane header %q, want the gateway's own token", got)
		}
	default:
		t.Fatal("the fan-out never reached the node")
	}
}

func clusterListNode(t *testing.T, rows ...listedSandbox) *httptest.Server {
	t.Helper()
	payload, err := json.Marshal(rows)
	if err != nil {
		t.Fatalf("encode node rows: %v", err)
	}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(payload)
	}))
}

func fetchClusterListRows(t *testing.T, server *Server, path string) []listedSandbox {
	t.Helper()
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
	if response.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d (body %q)", response.Code, response.Body.String())
	}
	var rows []listedSandbox
	if err := json.Unmarshal(response.Body.Bytes(), &rows); err != nil {
		t.Fatalf("decode listing: %v (body %q)", err, response.Body.String())
	}
	return rows
}

// 🔴 The two rows a split brain produces are identical in everything the sort
// orders on: the same sandbox id, and the same startedAt, because startedAt is
// the sandbox's creation time and only a fork resets it. sort.Slice is not
// stable, so which one survived was whichever the sort happened to leave first,
// and two calls against the same cluster could disagree — with the state, the
// end time and the metadata all read off a run that is over.
//
// Twenty rounds, because a single round passes half the time by luck.
func TestClusterListPrefersTheCurrentExecutionOnDuplicates(t *testing.T) {
	startedAt := time.Date(2026, 8, 19, 10, 0, 0, 0, time.UTC)
	stale := listedSandbox{
		SandboxID:   "11111111-2222-3333-4444-555555555555",
		StartedAt:   startedAt,
		State:       "running-on-the-superseded-node",
		ExecutionID: executionOlder,
	}
	current := listedSandbox{
		SandboxID: "11111111-2222-3333-4444-555555555555",
		StartedAt: startedAt,
		State:     "running-on-the-current-node",
		// 🔴 Upper-cased on purpose. The registry's own id validator accepts
		// either case, so a node may report either, and the ordering is
		// lexicographic: 'A' (0x41) sorts before 'a' (0x61), so a newer
		// incarnation that arrived upper-cased loses to an older lower-cased one
		// unless both are put into one case first. The row that is upper-cased
		// here is the one that has to win.
		ExecutionID: strings.ToUpper(executionNewer),
	}

	for round := 0; round < 20; round++ {
		// The order the two nodes answer in is swapped between rounds, so a
		// keep-first implementation cannot be right by accident.
		first, second := stale, current
		if round%2 == 1 {
			first, second = current, stale
		}

		nodeA := clusterListNode(t, first)
		nodeB := clusterListNode(t, second)

		server := newTestServer(t, stubSchedulerClient{
			listNodesFunc: func(context.Context, *schedulerv1.ListNodesRequest, ...grpc.CallOption) (*schedulerv1.ListNodesResponse, error) {
				return &schedulerv1.ListNodesResponse{Nodes: []*schedulerv1.Node{
					{NodeId: "node-a", Endpoint: nodeA.URL},
					{NodeId: "node-b", Endpoint: nodeB.URL},
				}}, nil
			},
		}, 5*time.Second, 4<<20)

		rows := fetchClusterListRows(t, server, "/v2/sandboxes")
		nodeA.Close()
		nodeB.Close()

		if len(rows) != 1 {
			t.Fatalf("round %d returned %d rows, want 1", round, len(rows))
		}
		if rows[0].State != current.State {
			t.Fatalf("round %d kept the row from %q; the newer incarnation has to win every time", round, rows[0].State)
		}
	}
}

// The duplicate itself is the interesting fact. It used to be swallowed by the
// deduplication, which made the endpoint most likely to be used to find a split
// brain the one endpoint that hid it.
func TestClusterListCountsDuplicates(t *testing.T) {
	startedAt := time.Date(2026, 8, 19, 10, 0, 0, 0, time.UTC)
	row := func(execution string) listedSandbox {
		return listedSandbox{
			SandboxID:   "11111111-2222-3333-4444-555555555555",
			StartedAt:   startedAt,
			ExecutionID: execution,
		}
	}

	for _, tc := range []struct {
		name       string
		second     listedSandbox
		resolution string
	}{
		{name: "resolved by incarnation", second: row(executionOlder), resolution: clusterListDuplicateByExecution},
		// Two nodes that cannot name an incarnation. The old fallback stands,
		// and it is still counted — an uncountable duplicate would be the same
		// silent swallow in a new place.
		{name: "resolved by keeping the first", second: row(""), resolution: clusterListDuplicateKeepFirst},
	} {
		t.Run(tc.name, func(t *testing.T) {
			first := row(executionNewer)
			if tc.resolution == clusterListDuplicateKeepFirst {
				first = row("")
			}

			nodeA := clusterListNode(t, first)
			defer nodeA.Close()
			nodeB := clusterListNode(t, tc.second)
			defer nodeB.Close()

			before := duplicateCounter(t, tc.resolution)
			other := clusterListDuplicateByExecution
			if tc.resolution == clusterListDuplicateByExecution {
				other = clusterListDuplicateKeepFirst
			}
			otherBefore := duplicateCounter(t, other)

			server := newTestServer(t, stubSchedulerClient{
				listNodesFunc: func(context.Context, *schedulerv1.ListNodesRequest, ...grpc.CallOption) (*schedulerv1.ListNodesResponse, error) {
					return &schedulerv1.ListNodesResponse{Nodes: []*schedulerv1.Node{
						{NodeId: "node-a", Endpoint: nodeA.URL},
						{NodeId: "node-b", Endpoint: nodeB.URL},
					}}, nil
				},
			}, 5*time.Second, 4<<20)

			if rows := fetchClusterListRows(t, server, "/sandboxes"); len(rows) != 1 {
				t.Fatalf("returned %d rows, want 1", len(rows))
			}
			if got := duplicateCounter(t, tc.resolution) - before; got != 1 {
				t.Fatalf("the %q series moved by %v, want 1", tc.resolution, got)
			}
			if got := duplicateCounter(t, other) - otherBefore; got != 0 {
				t.Fatalf("the %q series also moved, by %v; the two resolutions have to stay distinguishable", other, got)
			}
		})
	}
}

// 🔴 The listing is decoded into a struct of the gateway's own, and a field the
// struct does not name is discarded without a word. A node reporting the
// incarnation and a gateway dropping it look exactly like a node that never
// reported one: the field is empty, and nothing fails.
func TestClusterListExposesExecutionID(t *testing.T) {
	node := clusterListNode(t, listedSandbox{
		SandboxID:   "11111111-2222-3333-4444-555555555555",
		StartedAt:   time.Date(2026, 8, 19, 10, 0, 0, 0, time.UTC),
		ExecutionID: executionNewer,
	})
	defer node.Close()

	server := newTestServer(t, stubSchedulerClient{
		listNodesFunc: func(context.Context, *schedulerv1.ListNodesRequest, ...grpc.CallOption) (*schedulerv1.ListNodesResponse, error) {
			return &schedulerv1.ListNodesResponse{Nodes: []*schedulerv1.Node{{NodeId: "node-a", Endpoint: node.URL}}}, nil
		},
	}, 5*time.Second, 4<<20)

	for _, path := range []string{"/sandboxes", "/v2/sandboxes"} {
		rows := fetchClusterListRows(t, server, path)
		if len(rows) != 1 {
			t.Fatalf("%s returned %d rows, want 1", path, len(rows))
		}
		if rows[0].ExecutionID != executionNewer {
			t.Fatalf("%s reported executionID %q, want %q", path, rows[0].ExecutionID, executionNewer)
		}
	}
}

// The registry listing is the one place a row's incarnation can be read
// directly, which is what makes it the surface a split brain is reconciled
// against. Same silent-drop hazard as the cluster listing.
func TestRegistryListExposesExecutionID(t *testing.T) {
	server := newTestServer(t, stubSchedulerClient{
		listRegistryFunc: func(context.Context, *schedulerv1.ListRegistrySandboxesRequest, ...grpc.CallOption) (*schedulerv1.ListRegistrySandboxesResponse, error) {
			return &schedulerv1.ListRegistrySandboxesResponse{
				Sandboxes: []*schedulerv1.RegistrySandbox{{
					SandboxId:   "11111111-2222-3333-4444-555555555555",
					State:       "paused",
					ExecutionId: executionNewer,
				}},
			}, nil
		},
	}, 5*time.Second, 4<<20)

	request := httptest.NewRequest(http.MethodGet, "/registry/sandboxes", nil)
	request.Header.Set(headerRegistryAPIKey, "any-non-empty-key")
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d (body %q)", response.Code, response.Body.String())
	}
	var body registryListResponse
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode listing: %v (body %q)", err, response.Body.String())
	}
	if len(body.Sandboxes) != 1 {
		t.Fatalf("returned %d rows, want 1", len(body.Sandboxes))
	}
	if body.Sandboxes[0].ExecutionID != executionNewer {
		t.Fatalf("reported executionID %q, want %q", body.Sandboxes[0].ExecutionID, executionNewer)
	}
}

// 🔴 The incarnation may not be a filter. A parameter that selects by it is one
// step away from a caller supplying one, and an incarnation held by a caller is
// stale by construction — which is the whole reason the gateway asks the
// scheduler rather than the client.
func TestRegistryListDoesNotAcceptAnExecutionFilter(t *testing.T) {
	server := newTestServer(t, stubSchedulerClient{}, 5*time.Second, 4<<20)

	request := httptest.NewRequest(http.MethodGet, "/registry/sandboxes?executionID="+executionNewer, nil)
	request.Header.Set(headerRegistryAPIKey, "any-non-empty-key")
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("filtering by executionID was answered %d, want 400", response.Code)
	}
}

// The incarnation the node reports on a fresh create is carried into the
// assignment, so the binding is authoritative from the first request rather than
// from the first heartbeat.
func TestRecordedAssignmentCarriesTheNodesExecution(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(headerSandboxID, "sbx-1")
		w.Header().Set(headerExecutionID, executionNewer)
		w.WriteHeader(http.StatusCreated)
	}))
	defer upstream.Close()

	assignments := make(chan *schedulerv1.RecordAssignmentRequest, 1)
	server := newTestServer(t, stubSchedulerClient{
		scheduleFunc: func(context.Context, *schedulerv1.ScheduleRequest, ...grpc.CallOption) (*schedulerv1.ScheduleResponse, error) {
			return &schedulerv1.ScheduleResponse{Node: &schedulerv1.Node{NodeId: "node-a", Endpoint: upstream.URL}}, nil
		},
		recordAssignmentFunc: func(_ context.Context, req *schedulerv1.RecordAssignmentRequest, _ ...grpc.CallOption) (*schedulerv1.RecordAssignmentResponse, error) {
			assignments <- req
			return &schedulerv1.RecordAssignmentResponse{}, nil
		},
	}, 5*time.Second, 4<<20)

	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/sandboxes", strings.NewReader("{}")))
	if response.Code != http.StatusCreated {
		t.Fatalf("expected status 201, got %d (body %q)", response.Code, response.Body.String())
	}

	select {
	case assignment := <-assignments:
		if assignment.GetExecutionId() != executionNewer {
			t.Fatalf("assignment recorded incarnation %q, want %q", assignment.GetExecutionId(), executionNewer)
		}
	default:
		t.Fatal("no assignment was recorded")
	}
}

// 🔴 An unrecognised mode stops the process. Falling back to a default would let
// one mistyped letter switch fencing off with nothing to say it happened, and
// the result would be indistinguishable from the value having been meant.
func TestAnUnrecognisedFencingModeRefusesToStart(t *testing.T) {
	if _, err := newServerWithFencing("enfroce"); err == nil {
		t.Fatal("a mistyped mode was accepted; it has to stop the process")
	}

	// The control: the three real values, and the absent one, are all accepted.
	for _, mode := range []string{"", "off", "observe", "enforce"} {
		if _, err := newServerWithFencing(mode); err != nil {
			t.Fatalf("mode %q was rejected: %v", mode, err)
		}
	}
}

func newServerWithFencing(mode string) (*Server, error) {
	return NewServer(zap.NewNop(), stubSchedulerClient{}, ServerOptions{ExecutionFencing: mode})
}

// 🔴 A WebSocket handshake is refused too, and it has to be refused before the
// upgrade is handled rather than after.
//
// The refusal is returned as an error from ModifyResponse instead of being
// written over the response in place, and this is the case that forces that
// shape: the reverse proxy decides whether a response is an upgrade before
// ModifyResponse runs, so mutating the status there would leave the upgrade path
// still taken and hand the client a hijacked connection to a superseded
// incarnation — with the refusal never reaching anyone.
func TestWebSocketHandshakeAgainstASupersededExecutionIsRefused(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			http.Error(w, "hijacking unsupported", http.StatusInternalServerError)
			return
		}
		conn, buffered, err := hijacker.Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = fmt.Fprintf(buffered,
			"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n%s: %s\r\n\r\n",
			headerExecutionID, executionOlder)
		_ = buffered.Flush()
		// Whatever the superseded incarnation would have streamed. None of it
		// may reach the client.
		_, _ = conn.Write([]byte("frames from a superseded incarnation"))
	}))
	defer upstream.Close()

	server := newTestServer(t, stubSchedulerClient{
		lookupNodeFunc: boundToRegistry(&schedulerv1.Node{NodeId: "node-a", Endpoint: upstream.URL}, executionNewer),
	}, 5*time.Second, 4<<20, withExecutionFencing(config.GatewayExecutionFencingEnforce))

	gateway := httptest.NewServer(server.Handler())
	defer gateway.Close()

	request, err := http.NewRequest(http.MethodGet, gateway.URL+"/anything", nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	request.Header.Set(headerSandboxID, "sbx-1")
	request.Header.Set("Upgrade", "websocket")
	request.Header.Set("Connection", "Upgrade")
	request.Header.Set("Sec-WebSocket-Version", "13")
	request.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("perform request: %v", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusConflict {
		t.Fatalf("the handshake was answered %d, want 409 — an upgrade against a superseded incarnation must not complete", response.StatusCode)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if strings.Contains(string(body), "frames from a superseded incarnation") {
		t.Fatalf("the superseded incarnation's stream reached the client: %q", body)
	}
}
