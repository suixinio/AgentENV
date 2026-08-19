package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestIsRegistryListRequest(t *testing.T) {
	cases := []struct {
		method string
		path   string
		want   bool
	}{
		{http.MethodGet, "/registry/sandboxes", true},
		{http.MethodGet, "/registry/sandboxes/", true},
		{http.MethodGet, "/registry/sandboxes/extra", false},
		{http.MethodGet, "/registry", false},
		{http.MethodPost, "/registry/sandboxes", false},
		{http.MethodGet, "/sandboxes", false},
		{http.MethodGet, "/nodes", false},
	}

	for _, tc := range cases {
		r := httptest.NewRequest(tc.method, tc.path, nil)
		if got := isRegistryListRequest(r); got != tc.want {
			t.Fatalf("%s %s: expected %v, got %v", tc.method, tc.path, tc.want, got)
		}
	}
}

func TestRegistryListRendersRowsWithNullLeases(t *testing.T) {
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	var captured *schedulerv1.ListRegistrySandboxesRequest
	client := stubSchedulerClient{
		listRegistryFunc: func(_ context.Context, req *schedulerv1.ListRegistrySandboxesRequest, _ ...grpc.CallOption) (*schedulerv1.ListRegistrySandboxesResponse, error) {
			captured = req
			return &schedulerv1.ListRegistrySandboxesResponse{
				DatabaseNowUnixMs: now.UnixMilli(),
				NextPageToken:     "s2",
				Sandboxes: []*schedulerv1.RegistrySandbox{
					{
						SandboxId:            "s1",
						ClusterId:            "cluster-a",
						State:                "resuming",
						Generation:           9,
						OriginNodeId:         "node-a",
						ClaimedByNodeId:      "node-b",
						SnapshotId:           "snap-1",
						HolderNodeId:         "node-b",
						PausedAtUnixMs:       now.Add(-time.Hour).UnixMilli(),
						UpdatedAtUnixMs:      now.UnixMilli(),
						LeaseExpiresAtUnixMs: now.Add(time.Minute).UnixMilli(),
					},
					{
						SandboxId:       "s2",
						ClusterId:       "cluster-a",
						State:           "local_only",
						OriginNodeId:    "node-b",
						HolderNodeId:    "node-b",
						PausedAtUnixMs:  now.Add(-2 * time.Hour).UnixMilli(),
						UpdatedAtUnixMs: now.Add(-2 * time.Hour).UnixMilli(),
					},
				},
			}, nil
		},
	}
	server := newTestServer(t, client, time.Second, 1<<20)

	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/registry/sandboxes?state=resuming&nodeID=node-b&limit=2&nextToken=s0", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", recorder.Code, recorder.Body.String())
	}
	if captured.GetState() != "resuming" || captured.GetNodeId() != "node-b" {
		t.Fatalf("filters were not forwarded: %v", captured)
	}
	if captured.GetPageSize() != 2 || captured.GetPageToken() != "s0" {
		t.Fatalf("pagination was not forwarded: %v", captured)
	}

	var body registryListResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response failed: %v", err)
	}
	if len(body.Sandboxes) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(body.Sandboxes))
	}
	if body.NextToken != "s2" {
		t.Fatalf("unexpected next token %q", body.NextToken)
	}
	if body.DatabaseTimeUnixMs != now.UnixMilli() {
		t.Fatalf("unexpected database time %d", body.DatabaseTimeUnixMs)
	}
	if body.Sandboxes[0].HolderNodeID != "node-b" {
		t.Fatalf("unexpected holder %q", body.Sandboxes[0].HolderNodeID)
	}
	if body.Sandboxes[0].LeaseExpiresAtUnixMs == nil || *body.Sandboxes[0].LeaseExpiresAtUnixMs != now.Add(time.Minute).UnixMilli() {
		t.Fatalf("unexpected lease timestamp %v", body.Sandboxes[0].LeaseExpiresAtUnixMs)
	}
	// A NULL column has to stay null: rendering it as 0 would make "already
	// expired" and "never expires" indistinguishable.
	if body.Sandboxes[1].LeaseExpiresAtUnixMs != nil {
		t.Fatalf("expected a null lease, got %v", *body.Sandboxes[1].LeaseExpiresAtUnixMs)
	}
	if body.Sandboxes[1].SandboxExpiresAtUnixMs != nil {
		t.Fatalf("expected a null deadline, got %v", *body.Sandboxes[1].SandboxExpiresAtUnixMs)
	}
}

func TestRegistryListErrorMapping(t *testing.T) {
	cases := []struct {
		name string
		code codes.Code
		want int
	}{
		// Never configured here: permanent, and distinguishable from a database
		// that happens to be down.
		{"not configured", codes.FailedPrecondition, http.StatusNotImplemented},
		{"registry unreadable", codes.Unavailable, http.StatusServiceUnavailable},
		{"bad argument", codes.InvalidArgument, http.StatusBadRequest},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client := stubSchedulerClient{
				listRegistryFunc: func(context.Context, *schedulerv1.ListRegistrySandboxesRequest, ...grpc.CallOption) (*schedulerv1.ListRegistrySandboxesResponse, error) {
					return nil, status.Error(tc.code, "boom")
				},
			}
			server := newTestServer(t, client, time.Second, 1<<20)

			recorder := httptest.NewRecorder()
			server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/registry/sandboxes", nil))

			if recorder.Code != tc.want {
				t.Fatalf("expected %d, got %d (%s)", tc.want, recorder.Code, recorder.Body.String())
			}
		})
	}
}

func TestRegistryListRejectsBadLimit(t *testing.T) {
	client := stubSchedulerClient{
		listRegistryFunc: func(context.Context, *schedulerv1.ListRegistrySandboxesRequest, ...grpc.CallOption) (*schedulerv1.ListRegistrySandboxesResponse, error) {
			t.Fatal("expected the scheduler not to be called for a bad limit")
			return nil, nil
		},
	}
	server := newTestServer(t, client, time.Second, 1<<20)

	for _, limit := range []string{"abc", "-1"} {
		recorder := httptest.NewRecorder()
		server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/registry/sandboxes?limit="+limit, nil))
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("limit %q: expected 400, got %d", limit, recorder.Code)
		}
	}
}

// The control-plane paths only apply when the request is not addressed to a
// sandbox; a routing header has to win, or a sandbox could be shadowed by a
// gateway path.
func TestRegistryListYieldsToSandboxRouting(t *testing.T) {
	client := stubSchedulerClient{
		listRegistryFunc: func(context.Context, *schedulerv1.ListRegistrySandboxesRequest, ...grpc.CallOption) (*schedulerv1.ListRegistrySandboxesResponse, error) {
			t.Fatal("expected a sandbox-routed request not to reach the registry handler")
			return nil, nil
		},
		lookupNodeFunc: func(context.Context, *schedulerv1.LookupNodeRequest, ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			return nil, status.Error(codes.NotFound, "sandbox assignment not found")
		},
	}
	server := newTestServer(t, client, time.Second, 1<<20)

	request := httptest.NewRequest(http.MethodGet, "/registry/sandboxes", nil)
	request.Header.Set(headerSandboxID, "sandbox-1")
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("expected the sandbox path to answer, got %d (%s)", recorder.Code, recorder.Body.String())
	}
}
