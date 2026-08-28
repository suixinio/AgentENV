package gateway

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc"
)

// withColdLookupTimeout sets the cap that bounds lookupNodeColdPath's
// LookupNode call on its own, separately from routingCtx's own deadline.
//
// 🔴 This file used to be scheduler_fallback_test.go and also had a
// withSchedulerFallback helper that could disable the call entirely
// (SchedulerFallbackDisabled) and point it at a second client
// (QueryOnlySchedulerClient). Both are deleted along with the Go scheduler
// they existed to decommission — the timeout is the only knob left, and the
// only one of the two that was ever an ordinary safety net rather than
// decommissioning scaffolding. See ColdLookupTimeout's own doc.
func withColdLookupTimeout(timeout time.Duration) testServerOption {
	return func(options *ServerOptions) {
		options.ColdLookupTimeout = timeout
	}
}

// sandboxRoutedRequest is a header-routed GET that reaches the sandbox
// lookup branch of handleProxy (hasSandbox = true) without a projection
// reader or a resume client configured, so resp stays nil all the way to
// lookupNodeColdPath.
func sandboxRoutedRequest(t *testing.T, gatewayURL string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, gatewayURL+"/health", nil)
	if err != nil {
		t.Fatalf("build request failed: %v", err)
	}
	req.Header.Set(headerSandboxID, "sbx-1")
	return req
}

// TestColdLookupTimesOutWithinConfiguredCap is the load-bearing control for
// the timeout: the stub blocks until its context is cancelled — exactly what
// an unreachable target (a Service with no ready endpoints, or a
// black-holed route) looks like to a caller — so the request can only
// return quickly if lookupNodeColdPath is actually applying its own,
// shorter deadline. Without that, the call would block until routingCtx's
// own timeout, set here to 5s specifically so the two are an order of
// magnitude apart and cannot be confused by scheduling jitter.
//
// This replaces TestSchedulerFallbackTimesOutWithinConfiguredCap, which
// exercised the same cap under its old name (schedulerFallbackTimeout) and
// through a second, separately configured client
// (QueryOnlySchedulerClient) that no longer exists.
func TestColdLookupTimesOutWithinConfiguredCap(t *testing.T) {
	const coldLookupTimeout = 200 * time.Millisecond
	const routingTimeout = 5 * time.Second

	blockedCalls := make(chan struct{}, 1)
	scheduler := stubSchedulerClient{
		lookupNodeFunc: func(ctx context.Context, _ *schedulerv1.LookupNodeRequest, _ ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			select {
			case blockedCalls <- struct{}{}:
			default:
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	before := testutil.ToFloat64(gatewayColdLookupTimeout)

	server := newTestServer(t, scheduler, routingTimeout, 1024,
		withColdLookupTimeout(coldLookupTimeout),
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

	// 🔴 The load-bearing assertion. 1s is generous above coldLookupTimeout
	// (200ms) to absorb scheduling jitter, and it is 5x below routingTimeout
	// (5s) so a mutant that deletes the cold-path-specific
	// context.WithTimeout — leaving the call bound only by routingCtx —
	// cannot pass by accident: it would take ~5s, not <1s.
	if elapsed >= time.Second {
		t.Fatalf("cold lookup took %s to fail, want well under the 5s routing timeout (cold-path cap = %s)", elapsed, coldLookupTimeout)
	}
	// And the lower bound: it must not have failed instantly either (which
	// would suggest something other than the timeout fired).
	if elapsed < coldLookupTimeout/2 {
		t.Fatalf("cold lookup took only %s, want at least roughly %s (the configured cap)", elapsed, coldLookupTimeout)
	}

	select {
	case <-blockedCalls:
	default:
		t.Fatal("scheduler LookupNode was never called")
	}

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusServiceUnavailable)
	}

	if got := testutil.ToFloat64(gatewayColdLookupTimeout) - before; got != 1 {
		t.Fatalf("agentenv_gateway_cold_lookup_timeout_total increased by %v, want 1", got)
	}
}

// TestColdLookupReachableBehaviorIsUnchanged is the control that pairs with
// the timeout test above: with the scheduler reachable and answering
// immediately, the response, its latency and the timeout counter must all
// look exactly as they did before the cap existed — nobody sees the cap
// unless it fires.
//
// This replaces TestSchedulerFallbackReachableBehaviorIsUnchanged, dropping
// the half of it that asserted the (now-deleted) disabled switch's outcome
// counter stayed at zero.
func TestColdLookupReachableBehaviorIsUnchanged(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer upstream.Close()

	scheduler := stubSchedulerClient{
		lookupNodeFunc: func(_ context.Context, req *schedulerv1.LookupNodeRequest, _ ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			if req.GetSandboxId() != "sbx-1" {
				return nil, fmt.Errorf("lookup sandbox id = %q, want %q", req.GetSandboxId(), "sbx-1")
			}
			return &schedulerv1.LookupNodeResponse{Node: &schedulerv1.Node{NodeId: "node-1", Endpoint: upstream.URL}}, nil
		},
	}

	before := testutil.ToFloat64(gatewayColdLookupTimeout)

	server := newTestServer(t, scheduler, 5*time.Second, 1024,
		withColdLookupTimeout(200*time.Millisecond),
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
	// Well under the 200ms cap: a reachable, instantly-answering scheduler
	// must not be made to wait for anything this timeout introduced.
	if elapsed >= 100*time.Millisecond {
		t.Fatalf("reachable lookup took %s, want well under the 200ms cold-path cap", elapsed)
	}

	if got := testutil.ToFloat64(gatewayColdLookupTimeout) - before; got != 0 {
		t.Fatalf("cold-path timeout counter increased by %v on a reachable success, want 0", got)
	}
}

// TestColdLookupTimeoutMessageIsClassified pins the exact wording an
// operator sees so it stays distinguishable from an ordinary "scheduler
// unreachable" (dial failure) message and from a sandbox-side error — the
// two things this is easiest to confuse with during an incident.
//
// This replaces TestSchedulerFallbackTimeoutMessageIsClassified;
// TestSchedulerFallbackDisabledMessageIsClassified has no replacement, since
// the disabled switch it pinned wording for is deleted outright.
func TestColdLookupTimeoutMessageIsClassified(t *testing.T) {
	scheduler := stubSchedulerClient{
		lookupNodeFunc: func(ctx context.Context, _ *schedulerv1.LookupNodeRequest, _ ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	server := newTestServer(t, scheduler, 5*time.Second, 1024,
		withColdLookupTimeout(200*time.Millisecond),
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
