package gateway

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"
	"agentenv/services/shared/routing"

	"google.golang.org/grpc"
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

func withProjectionAuthoritative(on bool) testServerOption {
	return func(options *ServerOptions) {
		options.ProjectionAuthoritative = on
	}
}

func sandboxIDsOf(assignments []sandboxAssignment) []string {
	ids := make([]string, 0, len(assignments))
	for _, assignment := range assignments {
		ids = append(ids, assignment.sandboxID)
	}
	return ids
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

// TestProjectionHitDoesNotCallTheScheduler is the whole point of the read
// switch: with a record present, the request is served with the control plane
// untouched.
func TestProjectionHitDoesNotCallTheScheduler(t *testing.T) {
	upstream, hits := newUpstream(t)
	reader := &stubProjectionReader{records: map[string]routing.Record{
		"sbx-1": {Node: routing.Node{ID: "node-a", Endpoint: upstream.URL}, ExecutionID: "0198b7cc-1111-7000-8000-000000000001"},
	}}
	scheduler := stubSchedulerClient{
		lookupNodeFunc: func(context.Context, *schedulerv1.LookupNodeRequest, ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			t.Fatal("a projection hit must not reach the scheduler")
			return nil, nil
		},
	}

	server := newTestServer(t, scheduler, 5*time.Second, 1<<20, withProjectionReader(reader))
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

// TestProjectionMissFallsBackToTheScheduler is the assertion that keeps the
// roster fallback alive.
//
// 🔴 A miss must reach LookupNode, which walks the binding, then the heartbeat
// roster, then the paused registry. Those last two cover the window a heartbeat
// is late for and the window another node's reconciliation dropped a binding
// this node still lists. A gateway that answered its own 404 on a miss would
// remove both, and the symptom — a resume answering 404 — is the end of that
// sandbox as far as any client is concerned.
func TestProjectionMissFallsBackToTheScheduler(t *testing.T) {
	upstream, hits := newUpstream(t)
	reader := &stubProjectionReader{records: map[string]routing.Record{}}
	lookups := 0
	scheduler := stubSchedulerClient{
		lookupNodeFunc: func(_ context.Context, req *schedulerv1.LookupNodeRequest, _ ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			lookups++
			if req.GetSandboxId() != "sbx-1" {
				t.Fatalf("looked up %q", req.GetSandboxId())
			}
			return &schedulerv1.LookupNodeResponse{
				Node:     &schedulerv1.Node{NodeId: "node-a", Endpoint: upstream.URL},
				Location: schedulerv1.SandboxLocation_SANDBOX_LOCATION_BOUND,
			}, nil
		},
	}

	server := newTestServer(t, scheduler, 5*time.Second, 1<<20, withProjectionReader(reader))
	resp := serveDataPlaneRequest(t, server.Handler(), "sbx-1")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200: a miss must be served by the fallback", resp.StatusCode)
	}
	if lookups != 1 || hits.get() != 1 {
		t.Fatalf("lookups = %d, upstream hits = %d, want 1 and 1", lookups, hits.get())
	}
}

// TestProjectionReadErrorFallsBackToTheScheduler: Redis must not become a
// second thing that can kill the data plane. The whole stage is about removing
// a dependency from the request path, and a read failure that refused the
// request would be the opposite.
func TestProjectionReadErrorFallsBackToTheScheduler(t *testing.T) {
	upstream, hits := newUpstream(t)
	reader := &stubProjectionReader{err: errors.New("redis is down")}
	lookups := 0
	scheduler := stubSchedulerClient{
		lookupNodeFunc: func(context.Context, *schedulerv1.LookupNodeRequest, ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			lookups++
			return &schedulerv1.LookupNodeResponse{
				Node:     &schedulerv1.Node{NodeId: "node-a", Endpoint: upstream.URL},
				Location: schedulerv1.SandboxLocation_SANDBOX_LOCATION_BOUND,
			}, nil
		},
	}

	server := newTestServer(t, scheduler, 5*time.Second, 1<<20, withProjectionReader(reader))
	resp := serveDataPlaneRequest(t, server.Handler(), "sbx-1")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200: a projection read failure must not be visible to a client", resp.StatusCode)
	}
	if lookups != 1 || hits.get() != 1 {
		t.Fatalf("lookups = %d, upstream hits = %d, want 1 and 1", lookups, hits.get())
	}
}

// TestProjectionMissOnAnUnknownSandboxStillAnswers404 is the control face.
//
// Without it, "a miss falls through" would be satisfied by an implementation
// that simply let everything through, and the two are indistinguishable from a
// test that only ever asks about sandboxes that exist.
func TestProjectionMissOnAnUnknownSandboxStillAnswers404(t *testing.T) {
	reader := &stubProjectionReader{records: map[string]routing.Record{}}
	scheduler := stubSchedulerClient{
		lookupNodeFunc: func(context.Context, *schedulerv1.LookupNodeRequest, ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			return nil, status.Error(codes.NotFound, "sandbox assignment not found")
		},
	}

	server := newTestServer(t, scheduler, 5*time.Second, 1<<20, withProjectionReader(reader))
	resp := serveDataPlaneRequest(t, server.Handler(), "never-existed")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

// TestProjectionReadSwitchOffNeverReads: nil reader, every request through the
// scheduler, which is what shipped before this existed.
func TestProjectionReadSwitchOffNeverReads(t *testing.T) {
	upstream, _ := newUpstream(t)
	lookups := 0
	scheduler := stubSchedulerClient{
		lookupNodeFunc: func(context.Context, *schedulerv1.LookupNodeRequest, ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			lookups++
			return &schedulerv1.LookupNodeResponse{
				Node:     &schedulerv1.Node{NodeId: "node-a", Endpoint: upstream.URL},
				Location: schedulerv1.SandboxLocation_SANDBOX_LOCATION_BOUND,
			}, nil
		},
	}

	server := newTestServer(t, scheduler, 5*time.Second, 1<<20)
	if server.projectionReader != nil {
		t.Fatal("the read switch defaults on")
	}
	resp := serveDataPlaneRequest(t, server.Handler(), "sbx-1")
	defer resp.Body.Close()
	if lookups != 1 {
		t.Fatalf("lookups = %d, want 1", lookups)
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
	server := newTestServer(t, stubSchedulerClient{}, 5*time.Second, 1<<20, withProjectionReader(reader))

	resp := serveDataPlaneRequest(t, server.Handler(), "sbx-1")
	defer resp.Body.Close()
	if observed != executionID {
		t.Fatalf("forwarded incarnation = %q, want %q: a direct read must fence exactly as a scheduler answer does", observed, executionID)
	}
}

// TestProjectionTTLSecsFromHeaders pins the parse.
//
// 🔴 Zero is "not offered" and lands on the receiver's binding_ttl. Nothing may
// produce a value a store could read as "keep this key until somebody deletes
// it", which is what an omitted expiry is.
func TestProjectionTTLSecsFromHeaders(t *testing.T) {
	cases := map[string]uint32{
		"":                     0,
		"   ":                  0,
		"0":                    0,
		"-1":                   0,
		"-86400":               0,
		"not-a-number":         0,
		"3600":                 3600,
		" 86460 ":              86460,
		"4294967296":           4294967295, // above uint32, clamped rather than wrapped to a tiny value
		"99999999999999999999": 0,          // unparseable as int64
	}
	for raw, want := range cases {
		h := http.Header{}
		if raw != "" {
			h.Set(headerProjectionTTLSecs, raw)
		}
		if got := projectionTTLSecsFromHeaders(h); got != want {
			t.Fatalf("projectionTTLSecsFromHeaders(%q) = %d, want %d", raw, got, want)
		}
	}
}

// TestExtractSandboxAssignmentsFromForkArray is the E2 fix.
//
// Fork's 201 is a bare top-level JSON array of per-fork results. The previous
// implementation unmarshalled into a map first and returned nil on anything
// else, so fork's projection write had never happened in any build.
func TestExtractSandboxAssignmentsFromForkArray(t *testing.T) {
	body := []byte(`[
		{"sandbox":{"sandboxID":"sbx-1","executionID":"exec-1"},"projectionTtlSecs":3600},
		{"error":{"code":500,"message":"fork failed"}},
		{"sandbox":{"sandboxID":"sbx-2","executionID":"exec-2"},"projectionTtlSecs":86460},
		{"sandbox":{"sandboxID":"sbx-3"}}
	]`)

	got := extractSandboxAssignmentsFromResponse(body)
	want := []sandboxAssignment{
		{sandboxID: "sbx-1", executionID: "exec-1", projectionTTLSecs: 3600},
		{sandboxID: "sbx-2", executionID: "exec-2", projectionTTLSecs: 86460},
		// 🔴 An element with no incarnation of its own is recorded without one.
		// Borrowing a neighbour's would name an incarnation that never ran
		// there, which is why the body path used to record none at all.
		{sandboxID: "sbx-3"},
	}
	if len(got) != len(want) {
		t.Fatalf("got %#v, want %#v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("element %d: got %#v, want %#v", i, got[i], want[i])
		}
	}
}

func TestExtractSandboxAssignmentsShapes(t *testing.T) {
	cases := []struct {
		name string
		body string
		want []sandboxAssignment
	}{
		{
			name: "create response object",
			body: `{"sandboxID":"sbx-1","executionID":"exec-1"}`,
			want: []sandboxAssignment{{sandboxID: "sbx-1", executionID: "exec-1"}},
		},
		{
			name: "data envelope",
			body: `{"data":{"sandboxID":"sbx-1"}}`,
			want: []sandboxAssignment{{sandboxID: "sbx-1"}},
		},
		{
			name: "duplicate ids keep the first spelling",
			body: `[{"sandbox":{"sandboxID":"sbx-1","executionID":"exec-1"}},{"sandbox":{"sandboxID":"sbx-1","executionID":"exec-2"}}]`,
			want: []sandboxAssignment{{sandboxID: "sbx-1", executionID: "exec-1"}},
		},
		{
			// 🔴 A negative budget in the body is "not offered", never
			// "forever".
			name: "a negative budget is dropped",
			body: `[{"sandbox":{"sandboxID":"sbx-1"},"projectionTtlSecs":-5}]`,
			want: []sandboxAssignment{{sandboxID: "sbx-1"}},
		},
		{name: "empty array", body: `[]`, want: nil},
		{name: "array of non-objects", body: `[1,2,3]`, want: nil},
		{name: "not json", body: `nope`, want: nil},
		{name: "elements with no sandbox id", body: `[{"error":{"code":500}}]`, want: nil},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := extractSandboxAssignmentsFromResponse([]byte(tc.body))
			if len(got) != len(tc.want) {
				t.Fatalf("got %#v, want %#v", got, tc.want)
			}
			for i := range tc.want {
				if got[i] != tc.want[i] {
					t.Fatalf("element %d: got %#v, want %#v", i, got[i], tc.want[i])
				}
			}
		})
	}
}

// 🔴 Six tests used to live here, testing the gateway's write-side
// projection switch (ServerOptions.ProjectionAuthoritative) end to end:
// TestForkRecordsEveryChildAssignment, TestResumeRecordsTheRoutedSandbox,
// TestResumeRecordsNothingWhileTheWriteSwitchIsOff,
// TestCreateSendsNoBudgetWhileTheWriteSwitchIsOff,
// TestForkSendsNoBudgetWhileTheWriteSwitchIsOff, and
// TestCreateForwardsTheBudgetWithoutBufferingTheBody. All six drove a create,
// a fork, a resume or a connect against an unconfigured (restUpstream=="")
// fixture — scheduled or looked up by this gateway itself — and inspected the
// resulting RecordAssignment call.
//
// All four of those request shapes are routeSourcePath or routeSourceSchedule
// calls and are now always forwarded to the api half by handleProxy's
// isUserFacingRestRequest branch, which records no assignment of its own — see
// forwardToRestUpstream's doc comment: placement and its assignment are now the
// api half's `NodePlacement::record_placement`, not this package's. None of
// the six requests these tests sent reaches assignmentRouteFor,
// recordAssignmentFromResponse or the scheduler's RecordAssignment RPC from
// this package any more, so there is nothing left here for them to pin.
//
// The read-path tests above this comment — TestProjectionHitDoesNotCallThe-
// Scheduler through TestProjectionHitCarriesTheIncarnationIntoFencing — are all
// data-plane requests (serveDataPlaneRequest) and are unaffected: they do not
// touch ProjectionAuthoritative or assignmentRouteFor at all.
//
// Every extraction-shape assertion these tests also exercised end to end
// (single object, {"data":...} envelope, top-level array, dedup by sandbox id,
// a negative budget dropped) remains pinned at the unit level by
// TestExtractSandboxAssignmentsFromForkArray and TestExtractSandboxAssignments-
// Shapes, above. What is not covered any more is end-to-end wiring — a real
// HTTP response reaching recordAssignmentFromResponse's array-body branch —
// because nothing in production reaches that branch either: fork is the only
// route that ever answered with an array, and fork records no assignment from
// this package now. The one assignment this package still writes from a
// response — a data-plane request to a PLACED/PINNED sandbox — is exercised
// end to end by TestPlacedSandboxDataPlaneRequestRecordsTheAssignment in
// server_test.go, including the incarnation this file's create/resume tests
// used to check.
