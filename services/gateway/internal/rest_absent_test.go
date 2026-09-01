package gateway

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// 🔴 The gateway carries the sandbox data plane and nothing else. A request
// that names no sandbox — no proxy host name, no routing header — has no
// upstream here, so it is answered 404 rather than resolved, scheduled or
// forwarded.
//
// The scheduler stub fails every call it is given: a route that reaches any RPC
// shows up as a status other than 404, so this also pins that the refusal
// happens before anything is asked.
func TestARequestThatNamesNoSandboxIsNotFound(t *testing.T) {
	for _, tc := range []struct {
		name   string
		method string
		target string
	}{
		{name: "sandbox create", method: http.MethodPost, target: "/sandboxes"},
		{name: "sandbox listing", method: http.MethodGet, target: "/v2/sandboxes"},
		{name: "sandbox pause", method: http.MethodPost, target: "/sandboxes/sbx-1/pause"},
		{name: "sandbox resume", method: http.MethodPost, target: "/sandboxes/sbx-1/resume"},
		{name: "snapshot listing", method: http.MethodGet, target: "/snapshots"},
		{name: "template build", method: http.MethodPost, target: "/v3/templates"},
		{name: "node listing", method: http.MethodGet, target: "/nodes"},
		{name: "node detail", method: http.MethodPost, target: "/nodes/node-a"},
		{name: "registry listing", method: http.MethodGet, target: "/registry/sandboxes"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := newTestServer(t, 5*time.Second, 4<<20,
				withResumeClient(refusingResume(t, "a request that names no sandbox has nothing to ask about")))

			response := httptest.NewRecorder()
			server.Handler().ServeHTTP(response, httptest.NewRequest(tc.method, tc.target, nil))

			if response.Code != http.StatusNotFound {
				t.Fatalf("%s %s answered %d, want 404 (body %q)",
					tc.method, tc.target, response.Code, response.Body.String())
			}
		})
	}
}

// The control for the table above: the same server, the same paths, still
// route to a node when a routing header names a sandbox. Without this, a
// gateway that 404'd everything would pass.
func TestTheSameRequestWithARoutingHeaderStillReachesANode(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	}))
	defer upstream.Close()

	server := newTestServer(t, 5*time.Second, 4<<20, routedTo("sbx-1", "node-a", upstream.URL))

	for _, target := range []string{"/sandboxes/sbx-1/pause", "/nodes", "/anything"} {
		request := httptest.NewRequest(http.MethodPost, target, nil)
		request.Header.Set(headerSandboxID, "sbx-1")
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, request)

		if response.Code != http.StatusTeapot {
			t.Fatalf("%s with a routing header answered %d, want the node's 418", target, response.Code)
		}
	}
}
