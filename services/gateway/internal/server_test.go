package gateway

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"
	"agentenv/services/shared/routing"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type testServerOption func(*ServerOptions)

func newTestServer(t *testing.T, timeout time.Duration, maxRespSize int64, opts ...testServerOption) *Server {
	t.Helper()
	return newTestServerWithLogger(t, zap.NewNop(), timeout, maxRespSize, opts...)
}

// newTestServerWithLogger is newTestServer for the tests that assert on what the
// gateway wrote down rather than only on what it answered. The distinction
// matters for the modes that deliberately change nothing on the wire: with the
// response identical either way, the log line and the counter are the entire
// observable output.
func newTestServerWithLogger(
	t *testing.T,
	logger *zap.Logger,
	timeout time.Duration,
	maxRespSize int64,
	opts ...testServerOption,
) *Server {
	t.Helper()

	options := ServerOptions{
		RequestTimeout:  timeout,
		MaxResponseSize: maxRespSize,
	}
	for _, opt := range opts {
		opt(&options)
	}

	server, err := NewServer(logger, options)
	if err != nil {
		t.Fatalf("new gateway server failed: %v", err)
	}
	return server
}

func withSandboxProxyDomains(domains ...string) testServerOption {
	return func(options *ServerOptions) {
		options.SandboxProxyDomains = domains
	}
}

func withDebugMode(enabled bool) testServerOption {
	return func(options *ServerOptions) {
		options.DebugMode = enabled
	}
}

// routedTo answers the projection read for sandboxID with one node and no
// incarnation: the ordinary data-plane hit before fencing has anything to say.
func routedTo(sandboxID string, nodeID string, endpoint string) testServerOption {
	return routedToExecution(sandboxID, nodeID, endpoint, "")
}

// routedToExecution is routedTo with the incarnation the projection names,
// which is what decideFencing reads.
func routedToExecution(sandboxID string, nodeID string, endpoint string, executionID string) testServerOption {
	return withProjectionReader(&stubProjectionReader{records: map[string]routing.Record{
		sandboxID: {Node: routing.Node{ID: nodeID, Endpoint: endpoint}, ExecutionID: executionID},
	}})
}

func TestSandboxIDFromHeadersPrimary(t *testing.T) {
	h := http.Header{}
	h.Set("x-agentenv-sandbox-id", "abc123")
	id, ok := sandboxIDFromHeaders(h)
	if !ok || id != "abc123" {
		t.Fatalf("expected abc123, got %q (ok=%v)", id, ok)
	}
}

func TestSandboxIDFromHeadersE2B(t *testing.T) {
	h := http.Header{}
	h.Set("e2b-sandbox-id", "xyz")
	id, ok := sandboxIDFromHeaders(h)
	if !ok || id != "xyz" {
		t.Fatalf("expected xyz, got %q (ok=%v)", id, ok)
	}
}

func TestSandboxIDFromHeadersMissing(t *testing.T) {
	_, ok := sandboxIDFromHeaders(http.Header{})
	if ok {
		t.Fatal("expected no sandbox id when headers absent")
	}
}

func TestHasProxyRoutingHeaders(t *testing.T) {
	agentenvSandboxHeader := http.Header{}
	agentenvSandboxHeader.Set(headerSandboxID, "sbx-1")

	e2bPortHeader := http.Header{}
	e2bPortHeader.Set(headerE2BTargetPort, "49983")

	tests := []struct {
		name    string
		headers http.Header
		want    bool
	}{
		{
			name:    "agentenv sandbox header",
			headers: agentenvSandboxHeader,
			want:    true,
		},
		{
			name:    "e2b port header",
			headers: e2bPortHeader,
			want:    true,
		},
		{
			name:    "no proxy headers",
			headers: http.Header{},
			want:    false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := hasProxyRoutingHeaders(tc.headers); got != tc.want {
				t.Fatalf("hasProxyRoutingHeaders() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestSandboxIDFromPath(t *testing.T) {
	tests := []struct {
		name string
		path string
		want string
		ok   bool
	}{
		{name: "base sandbox path", path: "/sandboxes/sbx-123", want: "sbx-123", ok: true},
		{name: "sandbox pause path", path: "/sandboxes/sbx-123/pause", want: "sbx-123", ok: true},
		{name: "sandbox timeout path", path: "/sandboxes/sbx-123/timeout", want: "sbx-123", ok: true},
		{name: "sandbox list path", path: "/sandboxes", ok: false},
		{name: "cold sandbox create path", path: "/sandboxes-cold", ok: false},
		{name: "v2 sandbox list path", path: "/v2/sandboxes", ok: false},
		{name: "other resource", path: "/templates/abc", ok: false},
		{name: "empty segment", path: "/sandboxes//pause", ok: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := sandboxIDFromPath(tc.path)
			if ok != tc.ok || got != tc.want {
				t.Fatalf("path %q expected (%q, %v), got (%q, %v)", tc.path, tc.want, tc.ok, got, ok)
			}
		})
	}
}

func TestIsSandboxControlPlaneRequest(t *testing.T) {
	tests := []struct {
		name   string
		method string
		path   string
		want   bool
	}{
		{name: "sandbox detail", method: http.MethodGet, path: "/sandboxes/sbx-123", want: true},
		{name: "sandbox delete", method: http.MethodDelete, path: "/sandboxes/sbx-123", want: true},
		{name: "sandbox pause", method: http.MethodPost, path: "/sandboxes/sbx-123/pause", want: true},
		{name: "sandbox fork", method: http.MethodPost, path: "/sandboxes/sbx-123/fork", want: true},
		{name: "sandbox network update", method: http.MethodPut, path: "/sandboxes/sbx-123/network", want: true},
		{name: "network update wrong method", method: http.MethodPost, path: "/sandboxes/sbx-123/network", want: false},
		{name: "sandbox custom extension params get", method: http.MethodGet, path: "/sandboxes/sbx-123/custom-extension-params", want: true},
		{name: "sandbox custom extension params patch", method: http.MethodPatch, path: "/sandboxes/sbx-123/custom-extension-params", want: true},
		{name: "sandbox custom extension params post", method: http.MethodPost, path: "/sandboxes/sbx-123/custom-extension-params", want: false},
		{name: "data-plane shaped path", method: http.MethodGet, path: "/sandboxes/sbx-123/files", want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			request := httptest.NewRequest(tc.method, "http://gateway.test"+tc.path, nil)
			if got := isSandboxControlPlaneRequest(request); got != tc.want {
				t.Fatalf("isSandboxControlPlaneRequest(%s %s) = %v, want %v", tc.method, tc.path, got, tc.want)
			}
		})
	}
}

// serve runs one request through the server's own handler chain, which is what
// makes a test a statement about routing rather than about one handler.
func serve(t *testing.T, server *Server, req *http.Request) *http.Response {
	t.Helper()
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)
	return rec.Result()
}

func TestARoutedHealthRequestReachesTheNode(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer upstream.Close()

	server := newTestServer(t, time.Second, 1024, routedTo("sbx-1", "node-1", upstream.URL))

	gatewayServer := httptest.NewServer(server.Handler())
	defer gatewayServer.Close()

	req, err := http.NewRequest(http.MethodGet, gatewayServer.URL+"/health", nil)
	if err != nil {
		t.Fatalf("build request failed: %v", err)
	}
	req.Header.Set(headerSandboxID, "sbx-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}
}

// TestDebugModeExposesBackendNodeIDOnResponse pins debugMode's
// ModifyResponse hook (proxyRequest, in server.go) on a data-plane request.
// The debug header is generic — stamped in proxyRequest.ModifyResponse for
// whatever node served the exchange — and this is its only test.
func TestDebugModeExposesBackendNodeIDOnResponse(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	server := newTestServer(t, time.Second, 1024, routedTo("sbx-1", "node-1", upstream.URL), withDebugMode(true))

	req := httptest.NewRequest(http.MethodGet, "/anything", nil)
	req.Header.Set(headerSandboxID, "sbx-1")
	resp := serve(t, server, req)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if got := resp.Header.Get(headerNodeID); got != "node-1" {
		t.Fatalf("response %s = %q, want %q", headerNodeID, got, "node-1")
	}
}

func TestSandboxIDExtractionPathPreferredOverHeader(t *testing.T) {
	h := http.Header{}
	h.Set("x-agentenv-sandbox-id", "from-header")

	id, ok := sandboxIDFromPath("/sandboxes/from-path/pause")
	if !ok {
		id, ok = sandboxIDFromHeaders(h)
	}

	if !ok || id != "from-path" {
		t.Fatalf("expected path sandbox id to win, got %q (ok=%v)", id, ok)
	}
}

func TestSandboxControlPlaneRequestWithE2BHeadersUsesPathRoute(t *testing.T) {
	type upstreamRequestSnapshot struct {
		path      string
		sandboxID string
	}

	requests := make(chan upstreamRequestSnapshot, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- upstreamRequestSnapshot{
			path:      r.URL.Path,
			sandboxID: r.Header.Get(headerE2BSandboxID),
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"sandboxID":"sbx-path"}`))
	}))
	defer upstream.Close()

	server := newTestServer(t, time.Second, 1024, routedTo("sbx-path", "node-1", upstream.URL))

	gatewayServer := httptest.NewServer(server.Handler())
	defer gatewayServer.Close()

	req, err := http.NewRequest(http.MethodPost, gatewayServer.URL+"/sandboxes/sbx-path/connect", strings.NewReader(`{"timeout":60}`))
	if err != nil {
		t.Fatalf("build connect request failed: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(headerE2BSandboxID, "sbx-path")
	req.Header.Set(headerE2BTargetPort, "49983")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("connect request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("connect status = %d, want %d", resp.StatusCode, http.StatusCreated)
	}

	upstreamReq := <-requests
	if upstreamReq.path != "/sandboxes/sbx-path/connect" {
		t.Fatalf("upstream path = %q, want %q", upstreamReq.path, "/sandboxes/sbx-path/connect")
	}
	if upstreamReq.sandboxID != "sbx-path" {
		t.Fatalf("forwarded e2b sandbox id = %q, want %q", upstreamReq.sandboxID, "sbx-path")
	}
}

func TestSandboxIDFromHeadersOnResponse(t *testing.T) {
	h := http.Header{}
	h.Set("X-Agentenv-Sandbox-Id", "resp-sbx-1")
	id, ok := sandboxIDFromHeaders(h)
	if !ok || id != "resp-sbx-1" {
		t.Fatalf("expected sandbox id from response header, got %q (ok=%v)", id, ok)
	}
}

func TestUpstreamTargetPath(t *testing.T) {
	tests := []struct {
		name        string
		routeSource routeSource
		path        string
		want        string
	}{
		{
			name:        "header route prefixes /proxy",
			routeSource: routeSourceHeader,
			path:        "/sandboxes/sbx-1/files",
			want:        "/proxy/sandboxes/sbx-1/files",
		},
		{
			name:        "host route prefixes /proxy",
			routeSource: routeSourceHost,
			path:        "/readyz",
			want:        "/proxy/readyz",
		},
		{
			name:        "header route root path",
			routeSource: routeSourceHeader,
			path:        "/",
			want:        "/proxy/",
		},
		{
			name:        "path route unchanged",
			routeSource: routeSourcePath,
			path:        "/sandboxes/sbx-1/pause",
			want:        "/sandboxes/sbx-1/pause",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := upstreamTargetPath(tc.routeSource, tc.path)
			if got != tc.want {
				t.Fatalf("upstreamTargetPath(%q, %q) = %q, want %q", string(tc.routeSource), tc.path, got, tc.want)
			}
		})
	}
}

func TestUpstreamTargetEscapedPath(t *testing.T) {
	tests := []struct {
		name        string
		routeSource routeSource
		path        string
		want        string
	}{
		{
			name:        "header route prefixes /proxy",
			routeSource: routeSourceHeader,
			path:        "/sandboxes/a%2Fb/%2525",
			want:        "/proxy/sandboxes/a%2Fb/%2525",
		},
		{
			name:        "host route prefixes /proxy",
			routeSource: routeSourceHost,
			path:        "/api/foo%2Fbar",
			want:        "/proxy/api/foo%2Fbar",
		},
		{
			name:        "path route unchanged",
			routeSource: routeSourcePath,
			path:        "/sandboxes/a%2Fb/%2525",
			want:        "/sandboxes/a%2Fb/%2525",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := upstreamTargetEscapedPath(tc.routeSource, tc.path)
			if got != tc.want {
				t.Fatalf("upstreamTargetEscapedPath(%q, %q) = %q, want %q", string(tc.routeSource), tc.path, got, tc.want)
			}
		})
	}
}

func TestRequestEscapedPath(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "http://example.test/sandboxes/a%2Fb/%2525?x=1", nil)
	if got := requestEscapedPath(req); got != "/sandboxes/a%2Fb/%2525" {
		t.Fatalf("requestEscapedPath() = %q, want %q", got, "/sandboxes/a%2Fb/%2525")
	}
}

func TestJoinUpstreamPreservesRawEscapedPath(t *testing.T) {
	target, err := joinUpstream(
		"http://upstream:8000/base",
		"/proxy/sandboxes/a/b/%25",
		"/proxy/sandboxes/a%2Fb/%2525",
		"x=1",
	)
	if err != nil {
		t.Fatalf("joinUpstream() error = %v", err)
	}
	parsed, err := url.Parse(target)
	if err != nil {
		t.Fatalf("url.Parse() error = %v", err)
	}
	if parsed.EscapedPath() != "/base/proxy/sandboxes/a%2Fb/%2525" {
		t.Fatalf("EscapedPath() = %q, want %q", parsed.EscapedPath(), "/base/proxy/sandboxes/a%2Fb/%2525")
	}
}

func equalStrings(got []string, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func TestIsStreamingRequest(t *testing.T) {
	tests := []struct {
		name    string
		headers http.Header
		want    bool
	}{
		{
			name: "grpc content type",
			headers: http.Header{
				"Content-Type": []string{"application/grpc+proto"},
			},
			want: true,
		},
		{
			name: "connect content type",
			headers: http.Header{
				"Content-Type": []string{"application/connect+proto"},
			},
			want: true,
		},
		{
			name: "connect protocol version header",
			headers: http.Header{
				"Connect-Protocol-Version": []string{"1"},
			},
			want: true,
		},
		{
			name: "grpc-web content type",
			headers: http.Header{
				"Content-Type": []string{"application/grpc-web+proto"},
			},
			want: true,
		},
		{
			name: "sse accept header",
			headers: http.Header{
				"Accept": []string{"text/event-stream"},
			},
			want: true,
		},
		{
			name: "te trailers",
			headers: http.Header{
				"Te": []string{"trailers"},
			},
			want: true,
		},
		{
			name: "normal json request",
			headers: http.Header{
				"Content-Type": []string{"application/json"},
			},
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodPost, "/proxy", nil)
			if err != nil {
				t.Fatalf("build request failed: %v", err)
			}
			req.Header = tc.headers
			if got := isStreamingRequest(req); got != tc.want {
				t.Fatalf("isStreamingRequest() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestIsWebSocketRequest(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "/proxy", nil)
	if err != nil {
		t.Fatalf("build request failed: %v", err)
	}
	req.Header.Set("Connection", "keep-alive, Upgrade")
	req.Header.Set("Upgrade", "websocket")

	if !isWebSocketRequest(req) {
		t.Fatal("expected request to be detected as websocket upgrade")
	}

	req.Header.Del("Upgrade")
	if isWebSocketRequest(req) {
		t.Fatal("expected request without upgrade header to be non-websocket")
	}
}

func TestRequestContextForProxy(t *testing.T) {
	t.Run("streaming reuses request context", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodPost, "/proxy", nil)
		if err != nil {
			t.Fatalf("build request failed: %v", err)
		}
		routingCtx, cancelRouting := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancelRouting()
		ctx, cancel := requestContextForProxy(req, routingCtx, true)
		defer cancel()
		if req.Context() != ctx {
			t.Fatal("expected streaming request to reuse original context")
		}
	})

	t.Run("non-streaming reuses routing context", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodGet, "/health", nil)
		if err != nil {
			t.Fatalf("build request failed: %v", err)
		}
		routingCtx, cancelRouting := context.WithTimeout(context.Background(), 5*time.Millisecond)
		defer cancelRouting()
		ctx, cancel := requestContextForProxy(req, routingCtx, false)
		defer cancel()
		if routingCtx != ctx {
			t.Fatal("expected non-streaming request to share routing context")
		}
		<-ctx.Done()
		if ctx.Err() == nil {
			t.Fatal("expected routing timeout context to be canceled")
		}
	})
}

func TestRequestContextNotCanceledWhenStreamingCancelCalled(t *testing.T) {
	baseCtx, baseCancel := context.WithCancel(context.Background())
	defer baseCancel()
	req, err := http.NewRequestWithContext(baseCtx, http.MethodPost, "/proxy", nil)
	if err != nil {
		t.Fatalf("build request failed: %v", err)
	}
	routingCtx, cancelRouting := context.WithTimeout(context.Background(), time.Second)
	defer cancelRouting()
	ctx, cancel := requestContextForProxy(req, routingCtx, true)
	cancel()
	if err := ctx.Err(); err != nil {
		t.Fatalf("streaming cancel function should be no-op, got context err: %v", err)
	}
}

func TestSetXForwardedFor(t *testing.T) {
	h := http.Header{}
	setXForwardedFor(h, "10.0.0.2:12345")
	if got := h.Get("X-Forwarded-For"); got != "10.0.0.2" {
		t.Fatalf("X-Forwarded-For = %q, want %q", got, "10.0.0.2")
	}

	setXForwardedFor(h, "10.0.0.3:8080")
	if got := h.Get("X-Forwarded-For"); got != "10.0.0.3" {
		t.Fatalf("X-Forwarded-For overwrite = %q, want %q", got, "10.0.0.3")
	}
}

func TestInjectForwardedHeadersSetsXForwardedFor(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "http://gateway.test/sandboxes", nil)
	if err != nil {
		t.Fatalf("build request failed: %v", err)
	}
	req.RemoteAddr = "192.168.1.10:5000"
	req.Host = "gateway.test"

	h := http.Header{}
	h.Set("X-Forwarded-For", "10.0.0.1")
	injectForwardedHeaders(h, req)

	if got := h.Get("X-Forwarded-For"); got != "192.168.1.10" {
		t.Fatalf("X-Forwarded-For = %q, want %q", got, "192.168.1.10")
	}
}

func TestFlushInterval(t *testing.T) {
	if got := flushInterval(true); got != -1 {
		t.Fatalf("flushInterval(true) = %s, want -1ns", got)
	}
	if got := flushInterval(false); got != 0 {
		t.Fatalf("flushInterval(false) = %s, want 0s", got)
	}
}

func TestMetricsEndpointReturnsNotFoundWithoutProxyRouting(t *testing.T) {
	server := newTestServer(t, time.Second, 1024)
	gatewayServer := httptest.NewServer(server.Handler())
	defer gatewayServer.Close()

	resp, err := http.Get(gatewayServer.URL + "/metrics")
	if err != nil {
		t.Fatalf("gateway metrics request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("gateway metrics status = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
}

func TestHealthEndpointReturnsGatewayHealthWithoutProxyHeaders(t *testing.T) {
	server := newTestServer(t, time.Second, 1024)
	gatewayServer := httptest.NewServer(server.Handler())
	defer gatewayServer.Close()

	resp, err := http.Get(gatewayServer.URL + "/health")
	if err != nil {
		t.Fatalf("gateway health request failed: %v", err)
	}
	defer resp.Body.Close()

	_, err = io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read gateway health body failed: %v", err)
	}

	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("gateway health status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}
}

func TestHealthAndMetricsEndpointsWithSandboxHeadersProxyToSandbox(t *testing.T) {
	type upstreamRequestSnapshot struct {
		path         string
		targetPort   string
		forwardedURI string
	}

	requests := make(chan upstreamRequestSnapshot, 2)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- upstreamRequestSnapshot{
			path:         r.URL.Path,
			targetPort:   r.Header.Get(headerTargetPort),
			forwardedURI: r.Header.Get("X-Forwarded-URI"),
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer upstream.Close()

	server := newTestServer(t, time.Second, 1024, routedTo("sbx-service", "node-1", upstream.URL))

	gatewayServer := httptest.NewServer(server.Handler())
	defer gatewayServer.Close()

	for _, path := range []string{"/health", "/metrics"} {
		t.Run(path, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodGet, gatewayServer.URL+path, nil)
			if err != nil {
				t.Fatalf("build proxy request failed: %v", err)
			}
			req.Header.Set(headerSandboxID, "sbx-service")
			req.Header.Set(headerTargetPort, "49983")

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("proxy request failed: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusNoContent {
				t.Fatalf("proxy status = %d, want %d", resp.StatusCode, http.StatusNoContent)
			}

			upstreamReq := <-requests
			if upstreamReq.path != "/proxy"+path {
				t.Fatalf("upstream path = %q, want %q", upstreamReq.path, "/proxy"+path)
			}
			if upstreamReq.targetPort != "49983" {
				t.Fatalf("target port header = %q, want %q", upstreamReq.targetPort, "49983")
			}
			if upstreamReq.forwardedURI != path {
				t.Fatalf("X-Forwarded-URI = %q, want %q", upstreamReq.forwardedURI, path)
			}
		})
	}
}

func TestHealthAndMetricsEndpointsWithProxyHeadersMissingSandboxIDReturnBadRequest(t *testing.T) {
	server := newTestServer(t, time.Second, 1024)
	gatewayServer := httptest.NewServer(server.Handler())
	defer gatewayServer.Close()

	for _, path := range []string{"/health", "/metrics"} {
		t.Run(path, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodGet, gatewayServer.URL+path, nil)
			if err != nil {
				t.Fatalf("build malformed proxy request failed: %v", err)
			}
			req.Header.Set(headerTargetPort, "49983")

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("malformed proxy request failed: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("malformed proxy status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
			}
		})
	}
}

func TestHealthAndMetricsEndpointsWithHostRoutingProxyToSandbox(t *testing.T) {
	type upstreamRequestSnapshot struct {
		path       string
		sandboxID  string
		targetPort string
	}

	requests := make(chan upstreamRequestSnapshot, 2)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- upstreamRequestSnapshot{
			path:       r.URL.Path,
			sandboxID:  r.Header.Get(headerSandboxID),
			targetPort: r.Header.Get(headerTargetPort),
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer upstream.Close()

	server := newTestServer(t, time.Second, 1024, routedTo("sbx-service", "node-1", upstream.URL), withSandboxProxyDomains("sandbox-proxy.example.invalid"))
	gatewayServer := httptest.NewServer(server.Handler())
	defer gatewayServer.Close()

	for _, path := range []string{"/health", "/metrics"} {
		t.Run(path, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodGet, gatewayServer.URL+path, nil)
			if err != nil {
				t.Fatalf("build host-routed request failed: %v", err)
			}
			req.Host = "40988-sbx-service.sandbox-proxy.example.invalid"

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("host-routed request failed: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusAccepted {
				t.Fatalf("proxy status = %d, want %d", resp.StatusCode, http.StatusAccepted)
			}

			upstreamReq := <-requests
			if upstreamReq.path != "/proxy"+path {
				t.Fatalf("upstream path = %q, want %q", upstreamReq.path, "/proxy"+path)
			}
			if upstreamReq.sandboxID != "sbx-service" {
				t.Fatalf("sandbox routing header = %q, want %q", upstreamReq.sandboxID, "sbx-service")
			}
			if upstreamReq.targetPort != "40988" {
				t.Fatalf("target port header = %q, want 40988", upstreamReq.targetPort)
			}
		})
	}
}

func TestHandleProxyHostBasedRoutingForwardsToSandboxProxy(t *testing.T) {
	const sandboxID = "018ff4b7-8c0d-7f7c-9a99-123456789abc"

	type upstreamRequestSnapshot struct {
		path          string
		rawQuery      string
		host          string
		sandboxID     string
		targetPort    string
		forwardedHost string
		forwardedURI  string
	}

	requests := make(chan upstreamRequestSnapshot, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- upstreamRequestSnapshot{
			path:          r.URL.Path,
			rawQuery:      r.URL.RawQuery,
			host:          r.Host,
			sandboxID:     r.Header.Get(headerSandboxID),
			targetPort:    r.Header.Get(headerTargetPort),
			forwardedHost: r.Header.Get("X-Forwarded-Host"),
			forwardedURI:  r.Header.Get("X-Forwarded-URI"),
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()

	server := newTestServer(t, time.Second, 1024, routedTo(sandboxID, "node-1", upstream.URL), withSandboxProxyDomains("sandbox-proxy.example.invalid"))

	gatewayServer := httptest.NewServer(server.Handler())
	defer gatewayServer.Close()

	req, err := http.NewRequest(http.MethodGet, gatewayServer.URL+"/readyz?x=1", nil)
	if err != nil {
		t.Fatalf("build host-routed request failed: %v", err)
	}
	req.Host = "40988-" + sandboxID + ".sandbox-proxy.example.invalid"
	req.Header.Set(headerSandboxID, "sbx-from-header")
	req.Header.Set(headerTargetPort, "1234")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("host-routed request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200; body = %s", resp.StatusCode, string(body))
	}

	upstreamReq := <-requests
	if upstreamReq.path != "/proxy/readyz" {
		t.Fatalf("upstream path = %q, want %q", upstreamReq.path, "/proxy/readyz")
	}
	if upstreamReq.rawQuery != "x=1" {
		t.Fatalf("raw query = %q, want x=1", upstreamReq.rawQuery)
	}
	if upstreamReq.host != req.Host {
		t.Fatalf("upstream host = %q, want %q", upstreamReq.host, req.Host)
	}
	if upstreamReq.sandboxID != sandboxID {
		t.Fatalf("sandbox routing header = %q, want %q", upstreamReq.sandboxID, sandboxID)
	}
	if upstreamReq.targetPort != "40988" {
		t.Fatalf("target port header = %q, want 40988", upstreamReq.targetPort)
	}
	if upstreamReq.forwardedHost != req.Host {
		t.Fatalf("X-Forwarded-Host = %q, want %q", upstreamReq.forwardedHost, req.Host)
	}
	if upstreamReq.forwardedURI != "/readyz?x=1" {
		t.Fatalf("X-Forwarded-URI = %q, want %q", upstreamReq.forwardedURI, "/readyz?x=1")
	}
}

func TestHandleProxyHostBasedRoutingRejectsInvalidHost(t *testing.T) {
	server := newTestServer(t, time.Second, 1024, withSandboxProxyDomains("sandbox-proxy.example.invalid"))
	gatewayServer := httptest.NewServer(server.Handler())
	defer gatewayServer.Close()

	req, err := http.NewRequest(http.MethodGet, gatewayServer.URL+"/readyz", nil)
	if err != nil {
		t.Fatalf("build request failed: %v", err)
	}
	req.Host = "40988-sbx_bad.sandbox-proxy.example.invalid"

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body failed: %v", err)
	}
	if got, want := strings.TrimSpace(string(body)), "invalid sandbox data-plane host: sandbox id is invalid"; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
}

func TestHandleProxyWebSocketForwarding(t *testing.T) {
	type upstreamRequestSnapshot struct {
		path            string
		rawQuery        string
		host            string
		sandboxID       string
		upgrade         string
		connection      string
		forwardedHost   string
		forwardedProto  string
		forwardedMethod string
		forwardedURI    string
	}

	const websocketKey = "dGhlIHNhbXBsZSBub25jZQ=="

	requests := make(chan upstreamRequestSnapshot, 1)
	upstreamDone := make(chan error, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- upstreamRequestSnapshot{
			path:            r.URL.Path,
			rawQuery:        r.URL.RawQuery,
			host:            r.Host,
			sandboxID:       r.Header.Get(headerSandboxID),
			upgrade:         r.Header.Get("Upgrade"),
			connection:      r.Header.Get("Connection"),
			forwardedHost:   r.Header.Get("X-Forwarded-Host"),
			forwardedProto:  r.Header.Get("X-Forwarded-Proto"),
			forwardedMethod: r.Header.Get("X-Forwarded-Method"),
			forwardedURI:    r.Header.Get("X-Forwarded-URI"),
		}

		if !isWebSocketRequest(r) {
			upstreamDone <- fmt.Errorf("expected websocket upgrade request")
			http.Error(w, "expected websocket", http.StatusBadRequest)
			return
		}

		hj, ok := w.(http.Hijacker)
		if !ok {
			upstreamDone <- fmt.Errorf("response writer does not support hijacking")
			http.Error(w, "hijacking unsupported", http.StatusInternalServerError)
			return
		}

		conn, bufrw, err := hj.Hijack()
		if err != nil {
			upstreamDone <- err
			return
		}
		defer conn.Close()

		if _, err := fmt.Fprintf(bufrw, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: %s\r\n\r\n", websocketAccept(websocketKey)); err != nil {
			upstreamDone <- err
			return
		}
		if err := bufrw.Flush(); err != nil {
			upstreamDone <- err
			return
		}

		payload := make([]byte, 4)
		if _, err := io.ReadFull(conn, payload); err != nil {
			upstreamDone <- err
			return
		}
		if string(payload) != "ping" {
			upstreamDone <- fmt.Errorf("unexpected websocket payload %q", string(payload))
			return
		}
		if _, err := conn.Write([]byte("pong")); err != nil {
			upstreamDone <- err
			return
		}

		upstreamDone <- nil
	}))
	defer upstream.Close()

	server := newTestServer(t, time.Second, 1024, routedTo("sbx-1", "node-1", upstream.URL))

	gatewayServer := httptest.NewServer(server.Handler())
	defer gatewayServer.Close()

	gatewayURL, err := url.Parse(gatewayServer.URL)
	if err != nil {
		t.Fatalf("parse gateway url failed: %v", err)
	}

	conn, err := net.Dial("tcp", gatewayURL.Host)
	if err != nil {
		t.Fatalf("dial gateway failed: %v", err)
	}
	defer conn.Close()

	if _, err := fmt.Fprintf(conn, "GET /socket?token=abc HTTP/1.1\r\nHost: gateway.test\r\nConnection: keep-alive, Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n%s: sbx-1\r\n\r\n", websocketKey, headerSandboxID); err != nil {
		t.Fatalf("write websocket handshake failed: %v", err)
	}

	handshakeReq, err := http.NewRequest(http.MethodGet, gatewayServer.URL+"/socket?token=abc", nil)
	if err != nil {
		t.Fatalf("build handshake request failed: %v", err)
	}
	respReader := bufio.NewReader(conn)
	resp, err := http.ReadResponse(respReader, handshakeReq)
	if err != nil {
		t.Fatalf("read websocket handshake failed: %v", err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("unexpected handshake status: %d", resp.StatusCode)
	}
	if got := resp.Header.Get("Sec-WebSocket-Accept"); got != websocketAccept(websocketKey) {
		t.Fatalf("unexpected websocket accept header: %q", got)
	}

	if _, err := conn.Write([]byte("ping")); err != nil {
		t.Fatalf("write websocket payload failed: %v", err)
	}
	reply := make([]byte, 4)
	if _, err := io.ReadFull(respReader, reply); err != nil {
		t.Fatalf("read websocket payload failed: %v", err)
	}
	if string(reply) != "pong" {
		t.Fatalf("unexpected websocket reply %q", string(reply))
	}

	req := <-requests
	if req.path != "/proxy/socket" {
		t.Fatalf("upstream path = %q, want %q", req.path, "/proxy/socket")
	}
	if req.rawQuery != "token=abc" {
		t.Fatalf("upstream raw query = %q, want %q", req.rawQuery, "token=abc")
	}
	if req.host != "gateway.test" {
		t.Fatalf("upstream host = %q, want %q", req.host, "gateway.test")
	}
	if req.sandboxID != "sbx-1" {
		t.Fatalf("sandbox routing header = %q, want %q", req.sandboxID, "sbx-1")
	}
	if !strings.EqualFold(req.upgrade, "websocket") {
		t.Fatalf("upgrade header = %q, want websocket", req.upgrade)
	}
	if !strings.Contains(strings.ToLower(req.connection), "upgrade") {
		t.Fatalf("connection header = %q, want token upgrade", req.connection)
	}
	if req.forwardedHost != "gateway.test" {
		t.Fatalf("X-Forwarded-Host = %q, want %q", req.forwardedHost, "gateway.test")
	}
	if req.forwardedProto != "http" {
		t.Fatalf("X-Forwarded-Proto = %q, want %q", req.forwardedProto, "http")
	}
	if req.forwardedMethod != http.MethodGet {
		t.Fatalf("X-Forwarded-Method = %q, want %q", req.forwardedMethod, http.MethodGet)
	}
	if req.forwardedURI != "/socket?token=abc" {
		t.Fatalf("X-Forwarded-URI = %q, want %q", req.forwardedURI, "/socket?token=abc")
	}

	if err := <-upstreamDone; err != nil {
		t.Fatal(err)
	}
}

func TestHandleProxyPreservesEncodedPathSegments(t *testing.T) {
	type upstreamRequestSnapshot struct {
		rawURI      string
		rawQuery    string
		escapedPath string
	}

	requests := make(chan upstreamRequestSnapshot, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- upstreamRequestSnapshot{
			rawURI:      r.RequestURI,
			rawQuery:    r.URL.RawQuery,
			escapedPath: r.URL.EscapedPath(),
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()

	server := newTestServer(t, time.Second, 1024, routedTo("sbx-enc", "node-1", upstream.URL))

	gatewayServer := httptest.NewServer(server.Handler())
	defer gatewayServer.Close()

	tests := []struct {
		name            string
		rawRequestURI   string
		wantEscapedPath string
		wantRawQuery    string
	}{
		{
			name:            "encoded slash in path segment",
			rawRequestURI:   "/v0/files/%2F?list=true",
			wantEscapedPath: "/proxy/v0/files/%2F",
			wantRawQuery:    "list=true",
		},
		{
			name:            "double encoded percent",
			rawRequestURI:   "/v0/files/%252Fhome%252Fuser?list=true",
			wantEscapedPath: "/proxy/v0/files/%252Fhome%252Fuser",
			wantRawQuery:    "list=true",
		},
		{
			name:            "encoded space in path",
			rawRequestURI:   "/v0/files/my%20file.txt",
			wantEscapedPath: "/proxy/v0/files/my%20file.txt",
			wantRawQuery:    "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Use raw TCP to ensure the encoded path is sent exactly as-is,
			// since http.NewRequest decodes %2F into / in URL.Path.
			gatewayURL, err := url.Parse(gatewayServer.URL)
			if err != nil {
				t.Fatalf("parse gateway url: %v", err)
			}

			conn, err := net.Dial("tcp", gatewayURL.Host)
			if err != nil {
				t.Fatalf("dial gateway: %v", err)
			}
			defer conn.Close()

			rawHTTP := fmt.Sprintf(
				"GET %s HTTP/1.1\r\nHost: gateway.test\r\n%s: sbx-enc\r\nConnection: close\r\n\r\n",
				tc.rawRequestURI,
				headerSandboxID,
			)
			if _, err := conn.Write([]byte(rawHTTP)); err != nil {
				t.Fatalf("write request: %v", err)
			}

			resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
			if err != nil {
				t.Fatalf("read response: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				body, _ := io.ReadAll(resp.Body)
				t.Fatalf("status = %d, want 200; body = %s", resp.StatusCode, string(body))
			}

			upstreamReq := <-requests
			if upstreamReq.escapedPath != tc.wantEscapedPath {
				t.Fatalf("upstream escaped path = %q, want %q", upstreamReq.escapedPath, tc.wantEscapedPath)
			}
			if upstreamReq.rawQuery != tc.wantRawQuery {
				t.Fatalf("upstream raw query = %q, want %q", upstreamReq.rawQuery, tc.wantRawQuery)
			}
		})
	}
}

func websocketAccept(key string) string {
	sum := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	return base64.StdEncoding.EncodeToString(sum[:])
}

func TestProxyRequestContextDeadlineReturnsGatewayTimeout(t *testing.T) {
	server := newTestServer(t, time.Second, 1024)

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	proxyReq := httptest.NewRequest(http.MethodGet, "/sandboxes/sbx-timeout", nil).WithContext(ctx)
	recorder := httptest.NewRecorder()

	server.proxyRequest(
		recorder,
		proxyReq,
		"http://127.0.0.1:1/sandboxes/sbx-timeout",
		&schedulerv1.Node{NodeId: "node-1", Endpoint: "http://127.0.0.1:1"},
		proxyRequestOptions{},
	)

	if recorder.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusGatewayTimeout)
	}
}

func TestGatewayClassifiesClientCanceledProxyErrors(t *testing.T) {
	streamInputReq := httptest.NewRequest(http.MethodPost, "/process.Process/StreamInput", nil)
	if !isStreamInputProxyRequest(streamInputReq) {
		t.Fatalf("POST StreamInput should be classified as stream input")
	}

	otherReq := httptest.NewRequest(http.MethodPost, "/process.Process/Connect", nil)
	if isStreamInputProxyRequest(otherReq) {
		t.Fatalf("POST Connect should not be classified as stream input")
	}

	getReq := httptest.NewRequest(http.MethodGet, "/process.Process/StreamInput", nil)
	if isStreamInputProxyRequest(getReq) {
		t.Fatalf("GET StreamInput should not be classified as stream input")
	}
}

// lookupNodeReturning builds a stub whose LookupNode answers with one node and
// one location. Every other RPC is left unset, so any call the gateway makes
// beyond the single lookup fails the test by itself.
// 🔴 A 503 is right; the gRPC client's own dial error is not a body.
//
// Measured on the dev cluster with the control plane scaled to zero replicas:
// the body was `dial tcp 10.43.165.31:9090: connect: connection refused`,
// handing every caller the cluster's internal addressing and a Go transport
// string neither this service nor the api half wrote. The refusal is fed
// through the resume client, the one RPC left, as a FailedPrecondition — the
// code whose reason is forwarded to the caller.
func TestTransportTextInAResumeRefusalIsNotHandedToTheClient(t *testing.T) {
	for _, tc := range []struct {
		name    string
		message string
	}{
		{
			name:    "the dial failure measured on the cluster",
			message: `connection error: desc = "transport: Error while dialing dial tcp 10.43.165.31:9090: connect: connection refused"`,
		},
		{
			name:    "a name that does not resolve",
			message: `connection error: desc = "transport: Error while dialing dial tcp: lookup scheduler.agentenv.svc: no such host"`,
		},
		{
			name:    "a resolver that produced nothing",
			message: `name resolver error: produced zero addresses`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service := &stubResumeService{err: status.Error(codes.FailedPrecondition, tc.message)}
			server := newTestServer(t, 5*time.Second, 4<<20,
				withProjectionReader(missingProjection()),
				withResumeClient(service),
			)

			request := httptest.NewRequest(http.MethodGet, "/anything", nil)
			request.Header.Set(headerSandboxID, "sbx-1")
			response := httptest.NewRecorder()
			server.Handler().ServeHTTP(response, request)

			if response.Code != http.StatusServiceUnavailable {
				t.Fatalf("expected status 503, got %d (body %q)", response.Code, response.Body.String())
			}
			body := response.Body.String()
			for _, leaked := range []string{"dial tcp", "transport:", "10.43.165.31", "9090", "connection refused", "no such host", "resolver"} {
				if strings.Contains(body, leaked) {
					t.Fatalf("the body leaks %q to the caller: %q", leaked, body)
				}
			}
			if !strings.Contains(body, "the scheduler could not be reached") {
				t.Fatalf("the body has to say what happened: %q", body)
			}
		})
	}
}

// 🔴 The backstop, for a phrasing the list above has not seen. Whatever wrapped
// it, an endpoint does not go in a response body — a future grpc-go, a proxy or
// a mesh may word its failure any way it likes.
func TestAnEndpointInAResumeRefusalIsRedacted(t *testing.T) {
	service := &stubResumeService{err: status.Error(codes.FailedPrecondition,
		"upstream 10.43.165.31:9090 said no, and so did [fd00::1]:8443")}
	server := newTestServer(t, 5*time.Second, 4<<20,
		withProjectionReader(missingProjection()),
		withResumeClient(service),
	)

	request := httptest.NewRequest(http.MethodGet, "/anything", nil)
	request.Header.Set(headerSandboxID, "sbx-1")
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected status 503, got %d (body %q)", response.Code, response.Body.String())
	}
	body := response.Body.String()
	for _, leaked := range []string{"10.43.165.31", "9090", "fd00::1", "8443"} {
		if strings.Contains(body, leaked) {
			t.Fatalf("the body leaks %q to the caller: %q", leaked, body)
		}
	}
	// 🔴 And only the endpoint goes. Redacting the whole message would take the
	// reason with it, which is the failure the test above this one exists to
	// prevent.
	if !strings.Contains(body, "upstream") || !strings.Contains(body, "said no") {
		t.Fatalf("redaction ate the reason: %q", body)
	}
}

// Routing headers that name no sandbox are refused where the /health branch
// refuses them, not carried into the resolver to fail on an empty upstream.
func TestRoutingHeadersNamingNoSandboxAreRefusedNotProxied(t *testing.T) {
	server := newTestServer(t, 5*time.Second, 4<<20,
		withResumeClient(refusingResume(t, "nothing named a sandbox to look up")))

	request := httptest.NewRequest(http.MethodGet, "/anything", nil)
	request.Header.Set(headerTargetPort, "8080")
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", response.Code)
	}
	if body := strings.TrimSpace(response.Body.String()); body != "sandbox id header required" {
		t.Fatalf("body = %q, want the same refusal the /health branch gives", body)
	}
	if got := response.Header().Get(headerAllowOrigin); got != "*" {
		t.Fatalf("%s = %q, want %q", headerAllowOrigin, got, "*")
	}
}
