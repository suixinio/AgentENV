package gateway

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"agentenv/services/shared/routing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// stubProjectionReader answers from a map and counts what it was asked.
type stubProjectionReader struct {
	mu      sync.Mutex
	records map[string]routing.Record
	err     error
	calls   int
}

func (r *stubProjectionReader) Get(_ context.Context, sandboxID string) (routing.Record, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	if r.err != nil {
		return routing.Record{}, false, r.err
	}
	record, ok := r.records[sandboxID]
	return record, ok, nil
}

func (r *stubProjectionReader) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

func withProjectionReader(reader projectionReader) testServerOption {
	return func(options *ServerOptions) {
		options.ProjectionReader = reader
	}
}

func newUpstream(t *testing.T) (*httptest.Server, *int32Counter) {
	t.Helper()
	hits := &int32Counter{}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.inc()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(upstream.Close)
	return upstream, hits
}

type int32Counter struct {
	mu sync.Mutex
	n  int
}

func (c *int32Counter) inc() {
	c.mu.Lock()
	c.n++
	c.mu.Unlock()
}

func (c *int32Counter) get() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

func serveDataPlaneRequest(t *testing.T, handler http.Handler, sandboxID string) *http.Response {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/anything", nil)
	req.Header.Set(headerSandboxID, sandboxID)
	req.Header.Set(headerTargetPort, "8080")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec.Result()
}

// TestProjectionHitDoesNotAskTheApiHalf is the whole point of the read
// switch: with a record present, the request is served with the control plane
// untouched.
func TestProjectionHitDoesNotAskTheApiHalf(t *testing.T) {
	upstream, hits := newUpstream(t)
	reader := &stubProjectionReader{records: map[string]routing.Record{
		"sbx-1": {Node: routing.Node{ID: "node-a", Endpoint: upstream.URL}, ExecutionID: "0198b7cc-1111-7000-8000-000000000001"},
	}}

	server := newTestServer(t, 5*time.Second, 1<<20,
		withProjectionReader(reader),
		withResumeClient(refusingResume(t, "a projection hit must not reach the api half")),
	)
	resp := serveDataPlaneRequest(t, server.Handler(), "sbx-1")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if hits.get() != 1 {
		t.Fatalf("upstream hits = %d, want 1", hits.get())
	}
	if reader.callCount() != 1 {
		t.Fatalf("projection reads = %d, want 1", reader.callCount())
	}
}

// TestProjectionMissAsksTheApiHalf is the assertion that keeps the roster and
// registry paths alive.
//
// 🔴 A miss must reach the api half, which walks the binding, then the
// heartbeat roster, then the paused registry, and answers a running sandbox
// as it stands. Those last two cover the window a heartbeat is late for and
// the window another node's reconciliation dropped a binding this node still
// lists. A gateway that answered its own 404 on a miss would remove both, and
// the symptom — a resume answering 404 — is the end of that sandbox as far as
// any client is concerned.
func TestProjectionMissAsksTheApiHalf(t *testing.T) {
	upstream, hits := newUpstream(t)
	reader := &stubProjectionReader{records: map[string]routing.Record{}}
	service := runningAt("node-a", upstream.URL)

	server := newTestServer(t, 5*time.Second, 1<<20, withProjectionReader(reader), withResumeClient(service))
	resp := serveDataPlaneRequest(t, server.Handler(), "sbx-1")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200: a miss must be served by the api half's answer", resp.StatusCode)
	}
	if service.gotSandboxID != "sbx-1" {
		t.Fatalf("asked about %q", service.gotSandboxID)
	}
	if service.calls != 1 || hits.get() != 1 {
		t.Fatalf("resume calls = %d, upstream hits = %d, want 1 and 1", service.calls, hits.get())
	}
}

// TestProjectionReadErrorAsksTheApiHalf: Redis must not become a second thing
// that can kill the data plane. The whole stage is about removing a dependency
// from the request path, and a read failure that refused the request would be
// the opposite.
func TestProjectionReadErrorAsksTheApiHalf(t *testing.T) {
	upstream, hits := newUpstream(t)
	reader := &stubProjectionReader{err: errors.New("redis is down")}
	service := runningAt("node-a", upstream.URL)

	server := newTestServer(t, 5*time.Second, 1<<20, withProjectionReader(reader), withResumeClient(service))
	resp := serveDataPlaneRequest(t, server.Handler(), "sbx-1")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200: a projection read failure must not be visible to a client", resp.StatusCode)
	}
	if service.calls != 1 || hits.get() != 1 {
		t.Fatalf("resume calls = %d, upstream hits = %d, want 1 and 1", service.calls, hits.get())
	}
}

// TestProjectionMissOnAnUnknownSandboxStillAnswers404 is the control face.
//
// Without it, "a miss falls through" would be satisfied by an implementation
// that simply let everything through, and the two are indistinguishable from a
// test that only ever asks about sandboxes that exist.
func TestProjectionMissOnAnUnknownSandboxStillAnswers404(t *testing.T) {
	reader := &stubProjectionReader{records: map[string]routing.Record{}}
	service := &stubResumeService{err: status.Error(codes.NotFound, "sandbox never-existed not found")}

	server := newTestServer(t, 5*time.Second, 1<<20, withProjectionReader(reader), withResumeClient(service))
	resp := serveDataPlaneRequest(t, server.Handler(), "never-existed")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

// TestProjectionReadSwitchOffNeverReads: nil reader, every request through the
// api half.
func TestProjectionReadSwitchOffNeverReads(t *testing.T) {
	upstream, _ := newUpstream(t)
	service := runningAt("node-a", upstream.URL)

	server := newTestServer(t, 5*time.Second, 1<<20, withResumeClient(service))
	if server.projectionReader != nil {
		t.Fatal("the read switch defaults on")
	}
	resp := serveDataPlaneRequest(t, server.Handler(), "sbx-1")
	defer resp.Body.Close()
	if service.calls != 1 {
		t.Fatalf("resume calls = %d, want 1", service.calls)
	}
}

// TestProjectionHitCarriesTheIncarnationIntoFencing: the synthesized answer has
// to drive the fencing decision exactly as the scheduler's would, or turning on
// the direct read silently turns off the refusal.
func TestProjectionHitCarriesTheIncarnationIntoFencing(t *testing.T) {
	const executionID = "0198b7cc-1111-7000-8000-000000000001"

	var observed string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The gateway stamps what it expects, never what it observed: the
		// observed-direction header is deleted on the way out so a client
		// cannot seed the node's view of an exchange it is not part of.
		observed = r.Header.Get(headerExpectExecutionID)
		w.Header().Set(headerExecutionID, executionID)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(upstream.Close)

	reader := &stubProjectionReader{records: map[string]routing.Record{
		"sbx-1": {Node: routing.Node{ID: "node-a", Endpoint: upstream.URL}, ExecutionID: executionID},
	}}
	server := newTestServer(t, 5*time.Second, 1<<20, withProjectionReader(reader))

	resp := serveDataPlaneRequest(t, server.Handler(), "sbx-1")
	defer resp.Body.Close()
	if observed != executionID {
		t.Fatalf("forwarded incarnation = %q, want %q: a direct read must fence exactly as a scheduler answer does", observed, executionID)
	}
}
