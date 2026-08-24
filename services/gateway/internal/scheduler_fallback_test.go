package gateway

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc"
)

// withSchedulerFallback sets the two knobs that bound the query-only-scheduler
// LookupNode fallback: whether it is skipped entirely, and how long it may run
// on its own before lookupNodeFallback gives up on it.
func withSchedulerFallback(disabled bool, timeout time.Duration) testServerOption {
	return func(options *ServerOptions) {
		options.SchedulerFallbackDisabled = disabled
		options.SchedulerFallbackTimeout = timeout
	}
}

// sandboxRoutedRequest is TestLookupNodeUsesQueryOnlySchedulerClient's request
// shape: a header-routed GET that reaches the sandbox lookup branch of
// handleProxy (hasSandbox = true) without a projection reader or a resume
// client configured, so resp stays nil all the way to lookupNodeFallback.
func sandboxRoutedRequest(t *testing.T, gatewayURL string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, gatewayURL+"/health", nil)
	if err != nil {
		t.Fatalf("build request failed: %v", err)
	}
	req.Header.Set(headerSandboxID, "sbx-1")
	return req
}

// TestSchedulerFallbackDisabledMakesNoNetworkCall is control #1: with the
// fallback switched off, lookupNodeFallback must return a classified error
// without ever calling the query-only scheduler client. The stub counts calls
// rather than merely erroring, so removing the disabled short-circuit (and
// falling through to a real call that then fails) would still be caught even
// if the failure shape happened to look similar.
func TestSchedulerFallbackDisabledMakesNoNetworkCall(t *testing.T) {
	var calls int32
	queryScheduler := stubSchedulerClient{
		lookupNodeFunc: func(context.Context, *schedulerv1.LookupNodeRequest, ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			atomic.AddInt32(&calls, 1)
			return nil, fmt.Errorf("lookupNodeFallback must not call the scheduler when disabled")
		},
	}

	before := testutil.ToFloat64(gatewaySchedulerFallback.WithLabelValues(schedulerFallbackOutcomeDisabled))

	server := newTestServer(t, stubSchedulerClient{}, 5*time.Second, 1024,
		withQueryOnlyScheduler(queryScheduler),
		withSchedulerFallback(true, time.Second),
	)
	gatewayServer := httptest.NewServer(server.Handler())
	defer gatewayServer.Close()

	resp, err := http.DefaultClient.Do(sandboxRoutedRequest(t, gatewayServer.URL))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusServiceUnavailable)
	}

	if got := atomic.LoadInt32(&calls); got != 0 {
		t.Fatalf("query-only scheduler LookupNode was called %d times, want 0", got)
	}

	if got := testutil.ToFloat64(gatewaySchedulerFallback.WithLabelValues(schedulerFallbackOutcomeDisabled)) - before; got != 1 {
		t.Fatalf("agentenv_gateway_scheduler_fallback_total{outcome=disabled} increased by %v, want 1", got)
	}
}

// TestSchedulerFallbackTimesOutWithinConfiguredCap is control #2, and the one
// that carries the mutation-detection weight for the timeout half of this
// change. The stub blocks until its context is cancelled — exactly what an
// unreachable scheduler (a Service with no ready endpoints, or a black-holed
// route) looks like to a caller — so the request can only return quickly if
// lookupNodeFallback is actually applying its own, shorter deadline. Without
// that, the call would block until routingCtx's own timeout, set here to 5s
// specifically so the two are an order of magnitude apart and cannot be
// confused by scheduling jitter.
func TestSchedulerFallbackTimesOutWithinConfiguredCap(t *testing.T) {
	const fallbackTimeout = 200 * time.Millisecond
	const routingTimeout = 5 * time.Second

	blockedCalls := make(chan struct{}, 1)
	queryScheduler := stubSchedulerClient{
		lookupNodeFunc: func(ctx context.Context, _ *schedulerv1.LookupNodeRequest, _ ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			select {
			case blockedCalls <- struct{}{}:
			default:
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	before := testutil.ToFloat64(gatewaySchedulerFallback.WithLabelValues(schedulerFallbackOutcomeTimeout))

	server := newTestServer(t, stubSchedulerClient{}, routingTimeout, 1024,
		withQueryOnlyScheduler(queryScheduler),
		withSchedulerFallback(false, fallbackTimeout),
	)
	gatewayServer := httptest.NewServer(server.Handler())
	defer gatewayServer.Close()

	start := time.Now()
	resp, err := http.DefaultClient.Do(sandboxRoutedRequest(t, gatewayServer.URL))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	elapsed := time.Since(start)

	// 🔴 The load-bearing assertion. 1s is generous above fallbackTimeout
	// (200ms) to absorb scheduling jitter, and it is 5x below routingTimeout
	// (5s) so a mutant that deletes the fallback-specific context.WithTimeout
	// — leaving the call bound only by routingCtx — cannot pass by accident:
	// it would take ~5s, not <1s.
	if elapsed >= time.Second {
		t.Fatalf("fallback took %s to fail, want well under the 5s routing timeout (fallback cap = %s)", elapsed, fallbackTimeout)
	}
	// And the lower bound: it must not have failed instantly either (which
	// would suggest the disabled short-circuit, or something else entirely,
	// fired instead of the timeout).
	if elapsed < fallbackTimeout/2 {
		t.Fatalf("fallback took only %s, want at least roughly %s (the configured cap)", elapsed, fallbackTimeout)
	}

	select {
	case <-blockedCalls:
	default:
		t.Fatal("query-only scheduler LookupNode was never called")
	}

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusServiceUnavailable)
	}

	if got := testutil.ToFloat64(gatewaySchedulerFallback.WithLabelValues(schedulerFallbackOutcomeTimeout)) - before; got != 1 {
		t.Fatalf("agentenv_gateway_scheduler_fallback_total{outcome=timeout} increased by %v, want 1", got)
	}
}

// TestSchedulerFallbackReachableBehaviorIsUnchanged is control #3: with the
// scheduler reachable and answering immediately, the response, its latency
// and the fallback outcome counter must all look exactly as they did before
// this change existed — nobody may see the new switches unless they fire.
func TestSchedulerFallbackReachableBehaviorIsUnchanged(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer upstream.Close()

	queryScheduler := stubSchedulerClient{
		lookupNodeFunc: func(_ context.Context, req *schedulerv1.LookupNodeRequest, _ ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			if req.GetSandboxId() != "sbx-1" {
				return nil, fmt.Errorf("lookup sandbox id = %q, want %q", req.GetSandboxId(), "sbx-1")
			}
			return &schedulerv1.LookupNodeResponse{Node: &schedulerv1.Node{NodeId: "node-1", Endpoint: upstream.URL}}, nil
		},
	}

	disabledBefore := testutil.ToFloat64(gatewaySchedulerFallback.WithLabelValues(schedulerFallbackOutcomeDisabled))
	timeoutBefore := testutil.ToFloat64(gatewaySchedulerFallback.WithLabelValues(schedulerFallbackOutcomeTimeout))

	server := newTestServer(t, stubSchedulerClient{}, 5*time.Second, 1024,
		withQueryOnlyScheduler(queryScheduler),
		withSchedulerFallback(false, 200*time.Millisecond),
	)
	gatewayServer := httptest.NewServer(server.Handler())
	defer gatewayServer.Close()

	start := time.Now()
	resp, err := http.DefaultClient.Do(sandboxRoutedRequest(t, gatewayServer.URL))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	elapsed := time.Since(start)

	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}
	// Well under the 200ms fallback cap: a reachable, instantly-answering
	// scheduler must not be made to wait for anything this change introduced.
	if elapsed >= 100*time.Millisecond {
		t.Fatalf("reachable lookup took %s, want well under the 200ms fallback cap", elapsed)
	}

	if got := testutil.ToFloat64(gatewaySchedulerFallback.WithLabelValues(schedulerFallbackOutcomeDisabled)) - disabledBefore; got != 0 {
		t.Fatalf("outcome=disabled increased by %v on a reachable success, want 0", got)
	}
	if got := testutil.ToFloat64(gatewaySchedulerFallback.WithLabelValues(schedulerFallbackOutcomeTimeout)) - timeoutBefore; got != 0 {
		t.Fatalf("outcome=timeout increased by %v on a reachable success, want 0", got)
	}
}

// TestSchedulerFallbackDisabledMessageIsClassified pins the exact wording an
// operator sees so it stays distinguishable from an ordinary "scheduler
// unreachable" (dial failure) message and from a sandbox-side error — the
// two things this is easiest to confuse with during an incident.
func TestSchedulerFallbackDisabledMessageIsClassified(t *testing.T) {
	server := newTestServer(t, stubSchedulerClient{}, 5*time.Second, 1024,
		withSchedulerFallback(true, time.Second),
	)
	gatewayServer := httptest.NewServer(server.Handler())
	defer gatewayServer.Close()

	resp, err := http.DefaultClient.Do(sandboxRoutedRequest(t, gatewayServer.URL))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	body := readAll(t, resp)
	if !strings.Contains(body, "disabled") {
		t.Fatalf("body does not name the disabled fallback: %q", body)
	}
}

// TestSchedulerFallbackTimeoutMessageIsClassified is
// TestSchedulerFallbackDisabledMessageIsClassified's sibling for the timeout
// branch: the body must read as an infrastructure problem with the fallback
// path, not as anything the sandbox did.
func TestSchedulerFallbackTimeoutMessageIsClassified(t *testing.T) {
	queryScheduler := stubSchedulerClient{
		lookupNodeFunc: func(ctx context.Context, _ *schedulerv1.LookupNodeRequest, _ ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	server := newTestServer(t, stubSchedulerClient{}, 5*time.Second, 1024,
		withQueryOnlyScheduler(queryScheduler),
		withSchedulerFallback(false, 200*time.Millisecond),
	)
	gatewayServer := httptest.NewServer(server.Handler())
	defer gatewayServer.Close()

	resp, err := http.DefaultClient.Do(sandboxRoutedRequest(t, gatewayServer.URL))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	body := readAll(t, resp)
	if !strings.Contains(body, "not the sandbox's fault") {
		t.Fatalf("body does not disclaim the sandbox: %q", body)
	}
	if !strings.Contains(body, "scaled down") {
		t.Fatalf("body does not point at the likely cause: %q", body)
	}
}

func readAll(t *testing.T, resp *http.Response) string {
	t.Helper()
	buf := make([]byte, 4096)
	n, _ := resp.Body.Read(buf)
	return string(buf[:n])
}
