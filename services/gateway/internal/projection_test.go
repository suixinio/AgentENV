package gateway

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
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

// TestForkRecordsEveryChildAssignment drives the fix end to end: a fork answers
// with an array, and every child in it gets a binding written against the node
// that answered.
func TestForkRecordsEveryChildAssignment(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`[
			{"sandbox":{"sandboxID":"child-1","executionID":"exec-1"},"projectionTtlSecs":3600},
			{"sandbox":{"sandboxID":"child-2","executionID":"exec-2"},"projectionTtlSecs":3600}
		]`))
	}))
	t.Cleanup(upstream.Close)

	recorded := make(chan *schedulerv1.RecordAssignmentRequest, 4)
	scheduler := stubSchedulerClient{
		lookupNodeFunc: func(context.Context, *schedulerv1.LookupNodeRequest, ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			return &schedulerv1.LookupNodeResponse{
				Node:     &schedulerv1.Node{NodeId: "node-a", Endpoint: upstream.URL},
				Location: schedulerv1.SandboxLocation_SANDBOX_LOCATION_BOUND,
			}, nil
		},
		recordAssignmentFunc: func(_ context.Context, req *schedulerv1.RecordAssignmentRequest, _ ...grpc.CallOption) (*schedulerv1.RecordAssignmentResponse, error) {
			recorded <- req
			return &schedulerv1.RecordAssignmentResponse{}, nil
		},
	}

	server := newTestServer(t, scheduler, 5*time.Second, 1<<20, withProjectionAuthoritative(true))
	req := httptest.NewRequest(http.MethodPost, "/sandboxes/sbx-parent/fork", strings.NewReader(`{"count":2}`))
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201", rec.Code)
	}

	seen := map[string]*schedulerv1.RecordAssignmentRequest{}
	for i := 0; i < 2; i++ {
		select {
		case got := <-recorded:
			seen[got.GetSandboxId()] = got
		case <-time.After(2 * time.Second):
			t.Fatalf("only %d child assignments were recorded; before this fix the answer was 0", len(seen))
		}
	}

	for id, executionID := range map[string]string{"child-1": "exec-1", "child-2": "exec-2"} {
		got, ok := seen[id]
		if !ok {
			t.Fatalf("no assignment recorded for %s", id)
		}
		if got.GetExecutionId() != executionID {
			t.Fatalf("%s recorded incarnation %q, want %q", id, got.GetExecutionId(), executionID)
		}
		if got.GetProjectionTtlSecs() != 3600 {
			t.Fatalf("%s recorded ttl %d, want 3600", id, got.GetProjectionTtlSecs())
		}
	}
}

// TestResumeRecordsTheRoutedSandbox is ①.2 end to end: resume's 201 carries no
// sandbox-id header, and the routed id is used without buffering the body.
func TestResumeRecordsTheRoutedSandbox(t *testing.T) {
	const executionID = "0198b7cc-1111-7000-8000-000000000001"

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(headerExecutionID, executionID)
		w.Header().Set(headerProjectionTTLSecs, "86460")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"templateID":"tpl","sandboxID":"sbx-1","clientID":"c","envdVersion":"1"}`))
	}))
	t.Cleanup(upstream.Close)

	recorded := make(chan *schedulerv1.RecordAssignmentRequest, 2)
	scheduler := stubSchedulerClient{
		lookupNodeFunc: func(context.Context, *schedulerv1.LookupNodeRequest, ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			return &schedulerv1.LookupNodeResponse{
				Node:     &schedulerv1.Node{NodeId: "node-a", Endpoint: upstream.URL},
				Location: schedulerv1.SandboxLocation_SANDBOX_LOCATION_BOUND,
			}, nil
		},
		recordAssignmentFunc: func(_ context.Context, req *schedulerv1.RecordAssignmentRequest, _ ...grpc.CallOption) (*schedulerv1.RecordAssignmentResponse, error) {
			recorded <- req
			return &schedulerv1.RecordAssignmentResponse{}, nil
		},
	}

	for _, path := range []string{"/sandboxes/sbx-1/resume", "/sandboxes/sbx-1/connect"} {
		t.Run(path, func(t *testing.T) {
			server := newTestServer(t, scheduler, 5*time.Second, 1<<20, withProjectionAuthoritative(true))
			req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"timeout":3600}`))
			rec := httptest.NewRecorder()
			server.Handler().ServeHTTP(rec, req)

			if rec.Code != http.StatusCreated {
				t.Fatalf("status = %d, want 201", rec.Code)
			}
			select {
			case got := <-recorded:
				if got.GetSandboxId() != "sbx-1" {
					t.Fatalf("recorded sandbox %q, want sbx-1", got.GetSandboxId())
				}
				// 🔴 Without the incarnation this write is silently refused by
				// the scheduler's arbitration: a challenger naming none cannot
				// displace an incumbent that does, and a resume mints a new
				// incarnation every time.
				if got.GetExecutionId() != executionID {
					t.Fatalf("recorded incarnation %q, want %q", got.GetExecutionId(), executionID)
				}
				if got.GetProjectionTtlSecs() != 86460 {
					t.Fatalf("recorded ttl %d, want 86460", got.GetProjectionTtlSecs())
				}
			case <-time.After(2 * time.Second):
				t.Fatal("no assignment was recorded for the resumed sandbox")
			}
		})
	}
}

// TestResumeRecordsNothingWhileTheWriteSwitchIsOff keeps "off" equal to today.
//
// 🔴 It asserts on the calls, not on the status code. The version of this test
// that shipped delegated its whole claim to a stub returning an error — and
// Server.recordAssignment swallows a failed RecordAssignment with a Warn,
// deliberately, because a projection write must never fail a client's request.
// So the stub could fire on every request and the test would still have gone
// green on its 201. A write that must not happen has to be counted.
func TestResumeRecordsNothingWhileTheWriteSwitchIsOff(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(headerExecutionID, "0198b7cc-1111-7000-8000-000000000001")
		w.Header().Set(headerProjectionTTLSecs, "86460")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(upstream.Close)

	recorded := make(chan *schedulerv1.RecordAssignmentRequest, 4)
	scheduler := stubSchedulerClient{
		lookupNodeFunc: func(context.Context, *schedulerv1.LookupNodeRequest, ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			return &schedulerv1.LookupNodeResponse{
				Node:     &schedulerv1.Node{NodeId: "node-a", Endpoint: upstream.URL},
				Location: schedulerv1.SandboxLocation_SANDBOX_LOCATION_BOUND,
			}, nil
		},
		recordAssignmentFunc: func(_ context.Context, req *schedulerv1.RecordAssignmentRequest, _ ...grpc.CallOption) (*schedulerv1.RecordAssignmentResponse, error) {
			recorded <- req
			return &schedulerv1.RecordAssignmentResponse{}, nil
		},
	}

	// Both entry points, for the reason assignmentRouteFor covers both: the
	// node routes connect into the same resume path, so a switch that governed
	// only one would leave the identical write under a different name.
	for _, path := range []string{"/sandboxes/sbx-1/resume", "/sandboxes/sbx-1/connect"} {
		t.Run(path, func(t *testing.T) {
			server := newTestServer(t, scheduler, 5*time.Second, 1<<20)
			req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{}`))
			rec := httptest.NewRecorder()
			server.Handler().ServeHTTP(rec, req)
			if rec.Code != http.StatusCreated {
				t.Fatalf("status = %d, want 201", rec.Code)
			}
			// The write is made inside ModifyResponse, before the response is
			// written back, so anything that was going to arrive already has.
			select {
			case got := <-recorded:
				t.Fatalf("the switch is off and an assignment was recorded anyway: %v", got)
			default:
			}
		})
	}
}

// TestCreateSendsNoBudgetWhileTheWriteSwitchIsOff is the gateway's half of the
// write switch, which nothing exercised.
//
// 🔴 Create records either way — the switch was never about *whether* a create
// is recorded, only about the TTL it carries — so "off" here is a zero in one
// field of a request that still has to be sent, with the incarnation still on
// it. That is what makes the projection write byte-identical to the one that
// shipped before any of this, and what lets the two halves of the write switch
// be flipped in either order.
func TestCreateSendsNoBudgetWhileTheWriteSwitchIsOff(t *testing.T) {
	const executionID = "0198b7cc-1111-7000-8000-000000000001"

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(headerSandboxID, "sbx-new")
		w.Header().Set(headerExecutionID, executionID)
		// The node stamps the budget whatever the gateway's switch says: it
		// knows nothing about the gateway's configuration, and the header is
		// the same one the switched-on case reads.
		w.Header().Set(headerProjectionTTLSecs, "86460")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"sandboxID":"sbx-new"}`))
	}))
	t.Cleanup(upstream.Close)

	recorded := make(chan *schedulerv1.RecordAssignmentRequest, 2)
	scheduler := stubSchedulerClient{
		scheduleFunc: func(context.Context, *schedulerv1.ScheduleRequest, ...grpc.CallOption) (*schedulerv1.ScheduleResponse, error) {
			return &schedulerv1.ScheduleResponse{Node: &schedulerv1.Node{NodeId: "node-a", Endpoint: upstream.URL}}, nil
		},
		recordAssignmentFunc: func(_ context.Context, req *schedulerv1.RecordAssignmentRequest, _ ...grpc.CallOption) (*schedulerv1.RecordAssignmentResponse, error) {
			recorded <- req
			return &schedulerv1.RecordAssignmentResponse{}, nil
		},
	}

	server := newTestServer(t, scheduler, 5*time.Second, 1<<20)
	req := httptest.NewRequest(http.MethodPost, "/sandboxes", strings.NewReader(`{"templateID":"tpl"}`))
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201", rec.Code)
	}
	select {
	case got := <-recorded:
		if got.GetSandboxId() != "sbx-new" {
			t.Fatalf("recorded sandbox %q, want sbx-new", got.GetSandboxId())
		}
		if got.GetProjectionTtlSecs() != 0 {
			t.Fatalf("recorded ttl %d with the switch off, want 0 so the scheduler uses binding_ttl", got.GetProjectionTtlSecs())
		}
		// 🔴 The incarnation is not gated. Forwarding it is behaviour that
		// already shipped, and withholding it here would refuse the write at
		// the scheduler's arbitration rather than shorten its TTL.
		if got.GetExecutionId() != executionID {
			t.Fatalf("recorded incarnation %q, want %q", got.GetExecutionId(), executionID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no assignment was recorded for the created sandbox")
	}
}

// TestForkSendsNoBudgetWhileTheWriteSwitchIsOff is the same for the body path,
// where the budget comes off each element rather than off a header.
func TestForkSendsNoBudgetWhileTheWriteSwitchIsOff(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`[
			{"sandbox":{"sandboxID":"sbx-child","executionID":"0198b7cc-1111-7000-8000-000000000001"},"projectionTtlSecs":3600}
		]`))
	}))
	t.Cleanup(upstream.Close)

	recorded := make(chan *schedulerv1.RecordAssignmentRequest, 4)
	scheduler := stubSchedulerClient{
		lookupNodeFunc: func(context.Context, *schedulerv1.LookupNodeRequest, ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			return &schedulerv1.LookupNodeResponse{
				Node:     &schedulerv1.Node{NodeId: "node-a", Endpoint: upstream.URL},
				Location: schedulerv1.SandboxLocation_SANDBOX_LOCATION_BOUND,
			}, nil
		},
		recordAssignmentFunc: func(_ context.Context, req *schedulerv1.RecordAssignmentRequest, _ ...grpc.CallOption) (*schedulerv1.RecordAssignmentResponse, error) {
			recorded <- req
			return &schedulerv1.RecordAssignmentResponse{}, nil
		},
	}

	server := newTestServer(t, scheduler, 5*time.Second, 1<<20)
	req := httptest.NewRequest(http.MethodPost, "/sandboxes/sbx-parent/fork", strings.NewReader(`{"count":1}`))
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201", rec.Code)
	}
	select {
	case got := <-recorded:
		if got.GetSandboxId() != "sbx-child" {
			t.Fatalf("recorded sandbox %q, want sbx-child", got.GetSandboxId())
		}
		if got.GetProjectionTtlSecs() != 0 {
			t.Fatalf("recorded ttl %d with the switch off, want 0", got.GetProjectionTtlSecs())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no assignment was recorded for the forked child")
	}
}

// TestCreateForwardsTheBudgetWithoutBufferingTheBody pins the property §3.1
// asks for: create's fast path reads the sandbox id off a header and must go on
// doing so once the TTL header is added beside it.
func TestCreateForwardsTheBudgetWithoutBufferingTheBody(t *testing.T) {
	recorded := make(chan *schedulerv1.RecordAssignmentRequest, 2)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(headerSandboxID, "sbx-new")
		w.Header().Set(headerExecutionID, "0198b7cc-1111-7000-8000-000000000001")
		w.Header().Set(headerProjectionTTLSecs, "86460")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"sandboxID":"sbx-new"}`))
	}))
	t.Cleanup(upstream.Close)

	scheduler := stubSchedulerClient{
		scheduleFunc: func(context.Context, *schedulerv1.ScheduleRequest, ...grpc.CallOption) (*schedulerv1.ScheduleResponse, error) {
			return &schedulerv1.ScheduleResponse{Node: &schedulerv1.Node{NodeId: "node-a", Endpoint: upstream.URL}}, nil
		},
		recordAssignmentFunc: func(_ context.Context, req *schedulerv1.RecordAssignmentRequest, _ ...grpc.CallOption) (*schedulerv1.RecordAssignmentResponse, error) {
			recorded <- req
			return &schedulerv1.RecordAssignmentResponse{}, nil
		},
	}

	server := newTestServer(t, scheduler, 5*time.Second, 1<<20, withProjectionAuthoritative(true))
	req := httptest.NewRequest(http.MethodPost, "/sandboxes", strings.NewReader(`{"templateID":"tpl"}`))
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201", rec.Code)
	}
	select {
	case got := <-recorded:
		if got.GetSandboxId() != "sbx-new" || got.GetProjectionTtlSecs() != 86460 {
			t.Fatalf("recorded %+v", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no assignment was recorded for the created sandbox")
	}
}
