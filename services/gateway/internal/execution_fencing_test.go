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
// TestGatewayStripsClientSuppliedExecutionHeaders's "off" case, a gateway that
// stamped in every mode would pass here; without this one, a gateway that
// stamped in no mode would pass there. Neither is worth anything alone. (A
// third mode, observe, used to occupy the middle of that pair — compares but
// never stamps — until the rollout it existed for finished; see
// arbitration.rs's own note on the Rust twin of this deletion.)
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
// 🔴 The mode axis is load-bearing, not thoroughness. Off is the one mode left
// that stamps nothing, which is exactly what makes a forged header dangerous
// there: it would let a caller reach past a rolled-back gateway into whatever
// gate the nodes still carry. Stripping is what makes "this mode refuses
// nothing" a property of the gateway rather than a hope about its clients. (A
// second such mode, observe, used to sit here too — authoritative but never
// stamping — until the rollout it existed for finished and the mode was
// deleted; its case in the table below went with it.)
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

// 🔴 TestControlPlaneRequestIsNeverRefusedOnExecutionMismatch used to live
// here: pause and resume routed straight to a node by this gateway (via
// `newTestServer`'s unconfigured, restUpstream=="" fixture) and asserted that
// neither was ever stamped or refused. That routing no longer exists —
// handleProxy's isUserFacingRestRequest branch forwards every control-plane
// call to the api half before decideFencing is ever reached for it, so the
// property "the control plane is never refused" is enforced structurally now
// (no fencing plan is computed for a control-plane call that reaches the api
// half — see forwardToRestUpstream's own doc comment) rather than by a decision
// this test needed to pin.
//
// 🔴 The note that stood here added "there is no live call site left that
// resolves a fencingPlaneControl plan through a real request", and that part
// was wrong. isUserFacingRestRequest bails out on any request carrying a
// routing header, so a control-plane path sent with x-agentenv-sandbox-id set
// — which is what a client that stamps the header on everything produces —
// still reaches decideFencing, with routeSourcePath and therefore
// fencingPlaneControl. TestControlPlaneRequestsAreCountedUnderTheObservedLabel
// at the bottom of this file drives exactly that request and reads the
// resulting series off /metrics.

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

// 🔴 Three tests used to live here, all exercised through a real request under
// GatewayExecutionFencingObserve:
// TestObserveModeCountsTheMismatchWithoutRefusing (the dry run measures a
// mismatch without touching the response), TestObserveModeStillTranslatesA
// RogueNodeRefusal (the preflight-translation branch stays correct even in
// the one mode that cannot arm it), and part of the table below. Observe is
// retired — the rollout it existed for finished
// (deploy/k8s/base/kustomization.yaml's execution-fencing-config comment
// records the cluster reaching enforce) — and all three went with it, not
// merely edited: the mode they exercised can no longer be configured, so
// there is no request to send that reaches their assertions.
//
// Two of the three lost no coverage: TestDataPlaneRequestTranslatesNodePrecon
// ditionRefusal already exercises the preflight-translation branch under
// enforce (the mode that can actually arm it — observe's own version of that
// test existed only to prove the branch survives in a mode that can never
// reach it, a defensive test with no primary-path counterpart to lose), and
// TestGatewayStripsClientSuppliedExecutionHeaders/TestDataPlaneRequestCarries
// TheExpectedExecutionHeader together still pin "off and enforce, and only
// those two, decide what gets stamped." The third — the log message split —
// lost its subject: the second message and the parameter that picked it were
// deleted with the last thing that could produce a non-refusing mismatch. What
// replaces it below pins the half of that line that was never about the mode,
// its frozen field set.

// TestLogExecutionMismatchWritesTheFrozenFieldSet pins the half of the mismatch
// log line that is a contract rather than prose: the six fields an operator
// joins the three-stage trail on (impl plan §11.1(g)), which the node and the
// registry write out under the same names.
//
// 🔴 This used to be a two-row table over a `refused` parameter that picked
// between logMsgExecutionRefused and a second message, "…and let it through".
// That parameter and that message existed for observe, which measured a
// mismatch and then served the response; observe is deleted, both remaining
// call sites refuse, and the type can no longer express a plan that compares
// without refusing — so the selection this test pinned no longer exists to be
// pinned. What survives is the part that was never about the mode: the fields,
// and that the one message an operator greps for says "refused" because every
// line it now writes describes a refusal.
func TestLogExecutionMismatchWritesTheFrozenFieldSet(t *testing.T) {
	for _, refusedBy := range []string{refusedByNode, refusedByGateway} {
		t.Run(refusedBy, func(t *testing.T) {
			logs, logged := observer.New(zap.WarnLevel)
			server := newTestServerWithLogger(t, zap.New(logs), stubSchedulerClient{}, 5*time.Second, 4<<20)

			server.logExecutionMismatch(
				"sbx-1", &schedulerv1.Node{NodeId: "node-a"},
				executionNewer, executionOlder, refusedBy,
			)

			entries := logged.FilterField(zap.String("fencing_stage", fencingStageGatewayRoute)).All()
			if len(entries) != 1 {
				t.Fatalf("wrote %d gateway_route fencing lines, want exactly 1", len(entries))
			}
			if entries[0].Message != logMsgExecutionRefused {
				t.Fatalf("the log line reads %q, want %q", entries[0].Message, logMsgExecutionRefused)
			}
			// Asserted against the text, not the constant: every line this
			// function writes now describes a refusal, and an operator finds
			// them by grepping for that word. Rewording the constant to
			// something that does not say it is the regression.
			if !strings.Contains(entries[0].Message, "refused") {
				t.Fatalf("the log line %q does not say refused; it is the line an incident is "+
					"triaged with and every one of them is now a refusal", entries[0].Message)
			}

			// 🔴 The field set is the frozen half of this contract (the
			// three-stage trail, impl plan §11.1(g)): the message text was split
			// from it precisely because the fields could not be.
			fields := entries[0].ContextMap()
			for _, name := range []string{
				"sandbox_id", "node_id", "expected_execution_id",
				"observed_execution_id", "refusal_code", "refused_by",
			} {
				if _, present := fields[name]; !present {
					t.Fatalf("the %q line dropped field %q; the fields are the part operators join on across the three stages", refusedBy, name)
				}
			}
			// refused_by is the one field the two call sites still disagree on,
			// and the reason it stayed a parameter when `refused` did not.
			if got := fields["refused_by"]; got != refusedBy {
				t.Fatalf("refused_by came out as %v, want %q", got, refusedBy)
			}
		})
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

// 🔴 TestSchedulerNotFoundStillMapsToFourOhFour used to live here, driving a
// scheduler NotFound through `POST /sandboxes/sbx-1/resume` against an
// unconfigured (restUpstream=="") fixture to pin that writeSchedulerError's
// NotFound branch still answers 404. Resume is a routeSourcePath call and is
// now always forwarded to the api half before that branch is reached, so the
// specific request this test sent no longer exercises writeSchedulerError at
// all. The mapping itself is not uncovered: it is the same NotFound→404
// branch `TestProjectionMissOnAnUnknownSandboxStillAnswers404`
// (projection_test.go) exercises through a genuine, still-live data-plane
// LookupNode failure.

// 🔴 The Scheduler service may never answer PermissionDenied.
//
// writeSchedulerError has no branch for it, so it falls through to the default
// and becomes a 502 — "the upstream is broken", which is a wrong diagnosis of a
// precise refusal. The code means a superseded incarnation tried to write;
// aenv-api's own in-process paused registry enforces that fencing directly,
// never through a gRPC service the gateway calls.
//
// The method list is frozen deliberately. Moving a refusing method onto this
// service, or adding one, is exactly the change that would turn a fencing
// refusal into a 502 with nothing to notice it, and the only mechanical way to
// require that decision to be made on purpose is to make it break this list.
func TestSchedulerServiceNeverReturnsPermissionDenied(t *testing.T) {
	frozen := map[string]struct{}{
		"Schedule":           {},
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
		// Read-only listing of the registry. It cannot refuse a write because
		// it performs none, so it belongs on this service, answered for
		// operators through the gateway.
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
//
// 🔴 Converted to a data-plane request. This used to drive a pause (POST
// /sandboxes/sbx-1/pause) against an unconfigured (restUpstream=="")
// fixture, which is dead for the reason given throughout this file: pause is
// a routeSourcePath call and is now always forwarded to the api half before
// any node is resolved. stampOutboundGatewayHeaders is unconditional and
// shared by every forwarding path in this package (proxyRequest.Rewrite,
// used by both data-plane proxying and forwardToRestUpstream), so this table
// still pins the same three-way behaviour through the one call site left that
// exercises it via a real LookupNode/proxy round trip.
// TestASandboxControlPlaneCallGoesToTheApiHalfWithoutResolvingANode
// (rest_upstream_test.go) additionally pins that the api-bound forward is
// stamped too, but only for the "configured, no client value" case; this
// table is what still exercises the forged-value overwrite and the
// no-token-configured deletion.
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

			request := dataPlaneRequest("sbx-1")
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

// 🔴 TestRecordedAssignmentCarriesTheNodesExecution used to live here: a
// `POST /sandboxes` create, scheduled by this gateway against an
// unconfigured (restUpstream=="") fixture, whose recorded assignment had to
// carry the node's reported incarnation. Create is a routeSourceSchedule
// call and is now always forwarded to the api half — which records its own
// placements — before the gateway ever schedules or records anything for
// it, so this specific request no longer reaches recordAssignmentFromResponse
// at all. The property this pinned — a recorded assignment carries whatever
// incarnation the response named, not a placeholder — survives for the one
// case that still writes an assignment from this package: a data-plane
// request to a PLACED/PINNED sandbox. See
// TestPlacedSandboxDataPlaneRequestRecordsTheAssignment in server_test.go,
// which asserts the recorded execution id the same way this test did.

// 🔴 An unrecognised mode stops the process. Falling back to a default would let
// one mistyped letter switch fencing off with nothing to say it happened, and
// the result would be indistinguishable from the value having been meant.
//
// 🔴 "observe" is in the refused list, not the accepted one, deliberately. It
// used to be a recognised third mode and is not any more — the rollout it
// existed for finished (deploy/k8s/base/kustomization.yaml's
// execution-fencing-config comment records the cluster reaching enforce) —
// so a manifest that still names it must stop the process the same way a
// typo does, rather than being silently accepted as enforce or off. This is
// the regression guard for that: without it, deleting
// GatewayExecutionFencingObserve from config.go but leaving some fallback
// path in place would go unnoticed here.
func TestAnUnrecognisedFencingModeRefusesToStart(t *testing.T) {
	for _, mode := range []string{"enfroce", "observe"} {
		if _, err := newServerWithFencing(mode); err == nil {
			t.Fatalf("mode %q was accepted; it has to stop the process", mode)
		}
	}

	// The control: the two real values, and the absent one, are all accepted.
	for _, mode := range []string{"", "off", "enforce"} {
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

// 🔴 fencingDecisionObserved is a live metric label and nothing pinned its
// value.
//
// It is what the routing layer records for control-plane traffic: the plane
// that resolves an incarnation, logs it, and deliberately never stamps it. A
// mutation run changed the constant to fencingDecisionOff and the entire Go
// suite stayed green — the series carried on counting control-plane requests,
// under the one label that means "the switch is off and nothing was resolved".
// Any alert or dashboard reading agentenv_gateway_execution_fencing_total would
// have been reading a different fact, in the direction that hides a gap rather
// than inventing one, and no test said a word.
//
// The name is the other half of the reason for this guard. It collides with
// Observe, the deleted third fencing mode, so it reads like that mode's
// leftover and invites deletion — but Observe was a *mode* the operator chose,
// and this is a *plane's* decision that no configuration reaches. The pin is
// what makes that difference visible to whoever reaches for the constant next.
//
// Pinned at both ends, against the literal string rather than against the
// constant: what decideFencing puts in the plan, and what reaches /metrics.
// recordExecutionFencing is the only emission — no log line carries a decision
// label (logExecutionMismatch's frozen six-field set does not include one, and
// it is reached only by refusals, which the control plane never produces).
func TestControlPlaneRequestsAreCountedUnderTheObservedLabel(t *testing.T) {
	// The plan half. Pure, so the whole decision is one call.
	resolved := &schedulerv1.LookupNodeResponse{
		Node:               &schedulerv1.Node{NodeId: "node-a", Endpoint: "http://node-a"},
		Location:           schedulerv1.SandboxLocation_SANDBOX_LOCATION_BOUND,
		ExecutionId:        executionNewer,
		ExecutionAuthority: schedulerv1.ExecutionAuthority_EXECUTION_AUTHORITY_REGISTRY,
	}
	plan := decideFencing(fencingEnforce, fencingPlaneControl, resolved)
	if plan.decision != "observed" {
		t.Fatalf("the control plane's decision label is %q, want \"observed\"", plan.decision)
	}
	if plan.fenced {
		t.Fatal("a control-plane plan came back fenced; the label would then be the least of it")
	}

	// The emission half, through the exporter Prometheus reads. A control-plane
	// request reaches decideFencing when it carries a routing header as well as
	// a control-plane path — the header is what keeps isUserFacingRestRequest
	// from forwarding it to the api half first.
	stamped := make(chan string, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		stamped <- r.Header.Get(headerExpectExecutionID)
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	observedBefore := fencingCounter(t, fencingPlaneControl, "observed")
	offBefore := fencingCounter(t, fencingPlaneControl, "off")

	server := newTestServer(t, stubSchedulerClient{
		lookupNodeFunc: boundToRegistry(&schedulerv1.Node{NodeId: "node-a", Endpoint: upstream.URL}, executionNewer),
	}, 5*time.Second, 4<<20, withExecutionFencing(config.GatewayExecutionFencingEnforce))

	request := httptest.NewRequest(http.MethodPost, "/sandboxes/sbx-1/pause", nil)
	request.Header.Set(headerSandboxID, "sbx-1")
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d (body %q)", response.Code, response.Body.String())
	}

	// Proves the request really took the control plane rather than the data
	// plane: the expect header is the one thing only a fenced, data-plane
	// request carries.
	select {
	case got := <-stamped:
		if got != "" {
			t.Fatalf("a control-plane request carried expect header %q; this exercised the data plane", got)
		}
	default:
		t.Fatal("the request never reached the node")
	}

	if got := fencingCounter(t, fencingPlaneControl, "observed") - observedBefore; got != 1 {
		t.Fatalf(`agentenv_gateway_execution_fencing_total{plane="control",decision="observed"} moved by %v, want 1`, got)
	}
	if got := fencingCounter(t, fencingPlaneControl, "off") - offBefore; got != 0 {
		t.Fatalf(`the control plane was counted under "off" (moved by %v); that label means the switch is off `+
			`and nothing was resolved, which is not what happened`, got)
	}
}
