package gateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"google.golang.org/grpc"
)

const headerAllowOrigin = "Access-Control-Allow-Origin"

func TestASynthesizedRefusalIsReadableByBrowserJS(t *testing.T) {
	server := newTestServer(t, stubSchedulerClient{}, 5*time.Second, 4<<20)

	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/sandboxes", nil))

	if response.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", response.Code)
	}
	if got := response.Header().Get(headerAllowOrigin); got != "*" {
		t.Fatalf("%s = %q, want %q", headerAllowOrigin, got, "*")
	}
	// The value is constant, so varying on it only fragments caches, and "*"
	// is mutually exclusive with credentials.
	if got := response.Header().Get("Vary"); strings.Contains(got, "Origin") {
		t.Fatalf("Vary = %q; the allowed origin does not depend on the request", got)
	}
	if got := response.Header().Get("Access-Control-Allow-Credentials"); got != "" {
		t.Fatalf("Access-Control-Allow-Credentials = %q; it cannot be combined with %q", got, "*")
	}
}

func TestAPreflightIsAnsweredWhereNoUpstreamCan(t *testing.T) {
	server := newTestServer(t, stubSchedulerClient{}, 5*time.Second, 4<<20)

	request := httptest.NewRequest(http.MethodOptions, "/sandboxes", nil)
	request.Header.Set("Access-Control-Request-Method", "POST")
	request.Header.Set("Access-Control-Request-Headers", "x-api-key, content-type")
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", response.Code)
	}
	for header, want := range map[string]string{
		headerAllowOrigin: "*",
		// Without Allow-Methods a browser refuses to dispatch the real request
		// for any method outside the CORS safelist.
		"Access-Control-Allow-Methods": "*",
		"Access-Control-Allow-Headers": "x-api-key, content-type",
		"Access-Control-Max-Age":       "86400",
	} {
		if got := response.Header().Get(header); got != want {
			t.Fatalf("%s = %q, want %q", header, got, want)
		}
	}
}

// A bare OPTIONS is an ordinary request. Answering it as a preflight would
// invent a 204 for a method the caller asked nothing about.
func TestABareOptionsIsNotAPreflight(t *testing.T) {
	server := newTestServer(t, stubSchedulerClient{}, 5*time.Second, 4<<20)

	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodOptions, "/sandboxes", nil))

	if response.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", response.Code)
	}
	if got := response.Header().Get("Access-Control-Allow-Methods"); got != "" {
		t.Fatalf("Access-Control-Allow-Methods = %q on a response that answered no preflight", got)
	}
}

// 🔴 The load-bearing property. CORS on a sandbox's own responses belongs to
// envd or to whatever the user is running in there; a header added here would
// tell a browser that a server which never agreed to it accepts cross-origin
// reads.
func TestAProxiedResponseIsNeverGivenCORSHeaders(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	server := newTestServer(t, stubSchedulerClient{
		lookupNodeFunc: func(context.Context, *schedulerv1.LookupNodeRequest, ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			return &schedulerv1.LookupNodeResponse{
				Node: &schedulerv1.Node{NodeId: "node-a", Endpoint: upstream.URL},
			}, nil
		},
	}, 5*time.Second, 4<<20)

	// A plain data-plane request, and a preflight addressed the same way: both
	// have an upstream, so both are the sandbox's to answer.
	preflight := httptest.NewRequest(http.MethodOptions, "/anything", nil)
	preflight.Header.Set(headerSandboxID, "sbx-1")
	preflight.Header.Set("Access-Control-Request-Method", "POST")

	for name, request := range map[string]*http.Request{
		"plain":     dataPlaneRequest("sbx-1"),
		"preflight": preflight,
	} {
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, request)

		if response.Code != http.StatusOK {
			t.Fatalf("%s: status = %d, want the sandbox's 200", name, response.Code)
		}
		if got := response.Header().Get(headerAllowOrigin); got != "" {
			t.Fatalf("%s: the gateway added %s = %q to a response the sandbox produced",
				name, headerAllowOrigin, got)
		}
	}
}

// Every refusal this package writes has to go through the cors helpers, or a
// browser sees an opaque failure instead of the status and body.
func TestNoRawHTTPErrorInTheGatewayPackage(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package directory: %v", err)
	}

	scanned := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		content, err := os.ReadFile(filepath.Join(".", name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		scanned++

		// Comment lines are dropped first: a comment naming the call is a
		// description of this rule, not a violation of it.
		for number, line := range strings.Split(string(content), "\n") {
			if strings.HasPrefix(strings.TrimSpace(line), "//") {
				continue
			}
			for _, call := range []string{"http.Error(", "http.NotFound("} {
				if strings.Contains(line, call) {
					t.Errorf("%s:%d calls %s; use the cors package so a browser can read the response",
						name, number+1, call)
				}
			}
		}
	}
	if scanned == 0 {
		t.Fatal("the scan read no source files, so it proves nothing")
	}
}
