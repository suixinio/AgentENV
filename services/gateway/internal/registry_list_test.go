package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// registryListRequest builds an authorised request to the listing. Every test
// below goes through it, so the one place the credential is spelled out is the
// same place a test that means to omit it has to opt out of.
func registryListRequest(target string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, target, nil)
	r.Header.Set(headerRegistryAPIKey, "an-operator-key")
	return r
}

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
	server.Handler().ServeHTTP(recorder, registryListRequest("/registry/sandboxes?state=resuming&nodeID=node-b&limit=2&nextToken=s0"))

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
			server.Handler().ServeHTTP(recorder, registryListRequest("/registry/sandboxes"))

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
		server.Handler().ServeHTTP(recorder, registryListRequest("/registry/sandboxes?limit="+limit))
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

	request := registryListRequest("/registry/sandboxes")
	request.Header.Set(headerSandboxID, "sandbox-1")
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("expected the sandbox path to answer, got %d (%s)", recorder.Code, recorder.Body.String())
	}
}

// 🔴 F1: the listing does not go out unauthenticated.
//
// The gateway has no authentication middleware — /nodes is served by the
// gateway itself and is checked by nobody, and the paths that do get checked
// are checked by the node they are proxied to. This endpoint publishes every
// sandbox id in the cluster together with the node holding it, which is a
// bigger thing to leave open than a node list, so it carries its own check
// until the gateway grows a real one.
func TestRegistryListRequiresAnAPIKey(t *testing.T) {
	for _, tc := range []struct {
		name   string
		key    string
		absent bool
	}{
		{name: "no key at all", absent: true},
		{name: "empty key", key: ""},
		{name: "whitespace key", key: "   "},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := stubSchedulerClient{
				listRegistryFunc: func(context.Context, *schedulerv1.ListRegistrySandboxesRequest, ...grpc.CallOption) (*schedulerv1.ListRegistrySandboxesResponse, error) {
					t.Fatal("expected an unauthenticated request never to reach the scheduler")
					return nil, nil
				},
			}
			server := newTestServer(t, client, time.Second, 1<<20)

			request := httptest.NewRequest(http.MethodGet, "/registry/sandboxes", nil)
			if !tc.absent {
				request.Header.Set(headerRegistryAPIKey, tc.key)
			}
			recorder := httptest.NewRecorder()
			server.Handler().ServeHTTP(recorder, request)

			if recorder.Code != http.StatusUnauthorized {
				t.Fatalf("expected 401, got %d (%s)", recorder.Code, recorder.Body.String())
			}
		})
	}

	// And the same request with a key is served, so the check is a door rather
	// than a wall.
	t.Run("with a key", func(t *testing.T) {
		client := stubSchedulerClient{
			listRegistryFunc: func(context.Context, *schedulerv1.ListRegistrySandboxesRequest, ...grpc.CallOption) (*schedulerv1.ListRegistrySandboxesResponse, error) {
				return &schedulerv1.ListRegistrySandboxesResponse{}, nil
			},
		}
		server := newTestServer(t, client, time.Second, 1<<20)

		recorder := httptest.NewRecorder()
		server.Handler().ServeHTTP(recorder, registryListRequest("/registry/sandboxes"))
		if recorder.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d (%s)", recorder.Code, recorder.Body.String())
		}
	})

	// 🔴 And the check stays on this one endpoint. /nodes is read by
	// Agent-Console and AENV-Panel without any credential today; extending the
	// check to it would break both, which is a different decision from this one
	// and is not being made here.
	t.Run("does not spread to the node list", func(t *testing.T) {
		client := stubSchedulerClient{
			listObservedFunc: func(context.Context, *schedulerv1.ListObservedNodesRequest, ...grpc.CallOption) (*schedulerv1.ListObservedNodesResponse, error) {
				return &schedulerv1.ListObservedNodesResponse{}, nil
			},
		}
		server := newTestServer(t, client, time.Second, 1<<20)

		recorder := httptest.NewRecorder()
		server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/nodes", nil))
		if recorder.Code != http.StatusOK {
			t.Fatalf("expected the node list to stay open, got %d (%s)", recorder.Code, recorder.Body.String())
		}
	})
}

// 🔴 F3: a query parameter this endpoint does not know is an error, not a
// no-op.
//
// `?nodeId=` — the same word with a lower-case d — used to be dropped and the
// caller got every row in the cluster back with a 200. Nothing about that
// answer says the filter did not apply, so it reads as "this node holds all of
// them". An unknown parameter is far more often a typo in a real one than
// something the caller meant to send.
func TestRegistryListRejectsUnknownQueryParameters(t *testing.T) {
	client := stubSchedulerClient{
		listRegistryFunc: func(context.Context, *schedulerv1.ListRegistrySandboxesRequest, ...grpc.CallOption) (*schedulerv1.ListRegistrySandboxesResponse, error) {
			t.Fatal("expected an unknown query parameter never to reach the scheduler")
			return nil, nil
		},
	}
	server := newTestServer(t, client, time.Second, 1<<20)

	for _, tc := range []struct {
		query string
		names string
	}{
		{query: "nodeId=node-a", names: "nodeId"},
		{query: "State=paused", names: "State"},
		{query: "pageToken=s1", names: "pageToken"},
		{query: "nodeID=node-a&zzz=1&aaa=2", names: "aaa, zzz"},
	} {
		recorder := httptest.NewRecorder()
		server.Handler().ServeHTTP(recorder, registryListRequest("/registry/sandboxes?"+tc.query))
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("%s: expected 400, got %d (%s)", tc.query, recorder.Code, recorder.Body.String())
		}
		// The message names what was wrong. A 400 that does not is a caller
		// re-reading the source to find a typo.
		if !strings.Contains(recorder.Body.String(), tc.names) {
			t.Fatalf("%s: expected the message to name %q, got %q", tc.query, tc.names, recorder.Body.String())
		}
	}
}

// The four documented parameters keep working, spelled exactly as documented.
// Without this the rejection above could be satisfied by refusing everything.
func TestRegistryListAcceptsEveryDocumentedParameter(t *testing.T) {
	client := stubSchedulerClient{
		listRegistryFunc: func(context.Context, *schedulerv1.ListRegistrySandboxesRequest, ...grpc.CallOption) (*schedulerv1.ListRegistrySandboxesResponse, error) {
			return &schedulerv1.ListRegistrySandboxesResponse{}, nil
		},
	}
	server := newTestServer(t, client, time.Second, 1<<20)

	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, registryListRequest("/registry/sandboxes?state=paused&nodeID=node-a&limit=1&nextToken=s0"))
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", recorder.Code, recorder.Body.String())
	}
}

// The refusal message advertises exactly the set the check enforces. Spelling
// the list out a second time in the message is how a 400 ends up naming a
// parameter that is itself refused.
func TestRegistryListRefusalNamesTheSetItEnforces(t *testing.T) {
	client := stubSchedulerClient{
		listRegistryFunc: func(context.Context, *schedulerv1.ListRegistrySandboxesRequest, ...grpc.CallOption) (*schedulerv1.ListRegistrySandboxesResponse, error) {
			return &schedulerv1.ListRegistrySandboxesResponse{}, nil
		},
	}
	server := newTestServer(t, client, time.Second, 1<<20)

	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, registryListRequest("/registry/sandboxes?nope=1"))
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", recorder.Code)
	}
	_, advertised, found := strings.Cut(recorder.Body.String(), "supported: ")
	if !found {
		t.Fatalf("expected the message to advertise the supported set, got %q", recorder.Body.String())
	}
	for _, name := range strings.Split(strings.TrimSpace(advertised), ", ") {
		accepted := httptest.NewRecorder()
		server.Handler().ServeHTTP(accepted, registryListRequest("/registry/sandboxes?"+name+"="))
		if accepted.Code != http.StatusOK {
			t.Fatalf("the message advertises %q but the endpoint answers %d for it (%s)",
				name, accepted.Code, accepted.Body.String())
		}
	}
}
