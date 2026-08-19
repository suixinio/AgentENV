package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"
	pausedregistry "agentenv/services/scheduler/internal/registry"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func registryTestListing(now time.Time) pausedregistry.Listing {
	return pausedregistry.Listing{
		Now: now,
		Sandboxes: []pausedregistry.Sandbox{
			{
				SandboxID:      "s3",
				ClusterID:      "cluster-a",
				State:          pausedregistry.StatePaused,
				Generation:     4,
				OriginNodeID:   "node-a",
				SnapshotID:     "snap-3",
				PausedAt:       now.Add(-time.Hour),
				UpdatedAt:      now.Add(-time.Minute),
				LeaseExpiresAt: at(now, time.Minute),
			},
			{
				SandboxID:       "s1",
				ClusterID:       "cluster-a",
				State:           pausedregistry.StateResuming,
				Generation:      9,
				OriginNodeID:    "node-a",
				ClaimedByNodeID: "node-b",
				SnapshotID:      "snap-1",
				PausedAt:        now.Add(-2 * time.Hour),
				UpdatedAt:       now,
			},
			{
				SandboxID:    "s2",
				ClusterID:    "cluster-a",
				State:        pausedregistry.StateLocalOnly,
				Generation:   2,
				OriginNodeID: "node-b",
				PausedAt:     now.Add(-3 * time.Hour),
				UpdatedAt:    now.Add(-3 * time.Hour),
			},
		},
	}
}

func newRegistryTestService(t *testing.T, reader pausedregistry.Reader) *Service {
	t.Helper()

	opts := []ServiceOption{}
	if reader != nil {
		opts = append(opts, WithPausedRegistry(reader, testReportTTL, testReportTTL))
	}
	return NewService(
		zap.NewNop(),
		NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, defaultObservedReportTTL),
		NewStrategy("round_robin"),
		NewInMemoryBindingStore(time.Second),
		opts...,
	)
}

func requireCode(t *testing.T, err error, want codes.Code) {
	t.Helper()

	if err == nil {
		t.Fatalf("expected %s, got no error", want)
	}
	if got := status.Code(err); got != want {
		t.Fatalf("expected %s, got %s (%v)", want, got, err)
	}
}

// A scheduler that was never pointed at a registry says so, permanently. It
// must not answer with an empty list, which would read as "the registry holds
// nothing".
func TestListRegistrySandboxesReportsAMissingRegistry(t *testing.T) {
	svc := newRegistryTestService(t, nil)

	resp, err := svc.ListRegistrySandboxes(context.Background(), &schedulerv1.ListRegistrySandboxesRequest{})
	if resp != nil {
		t.Fatalf("expected no response, got %v", resp)
	}
	requireCode(t, err, codes.FailedPrecondition)
}

// 🔴 The single most important behaviour here: an unreadable registry is
// Unavailable, never NotFound and never an empty success. Both of the latter
// would be read downstream as an authoritative absence.
func TestListRegistrySandboxesIsUnavailableWhenTheReadFails(t *testing.T) {
	reader := &stubRegistryReader{err: errors.New("connection refused")}
	svc := newRegistryTestService(t, reader)

	resp, err := svc.ListRegistrySandboxes(context.Background(), &schedulerv1.ListRegistrySandboxesRequest{})
	if resp != nil {
		t.Fatalf("expected no response, got %v", resp)
	}
	requireCode(t, err, codes.Unavailable)
}

func TestListRegistrySandboxesReturnsRowsSortedWithDerivedHolder(t *testing.T) {
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	svc := newRegistryTestService(t, &stubRegistryReader{listing: registryTestListing(now)})

	resp, err := svc.ListRegistrySandboxes(context.Background(), &schedulerv1.ListRegistrySandboxesRequest{})
	if err != nil {
		t.Fatalf("list failed: %v", err)
	}
	if got := len(resp.GetSandboxes()); got != 3 {
		t.Fatalf("expected 3 rows, got %d", got)
	}
	if resp.GetNextPageToken() != "" {
		t.Fatalf("expected no next page token, got %q", resp.GetNextPageToken())
	}
	if resp.GetDatabaseNowUnixMs() != now.UnixMilli() {
		t.Fatalf("expected the database clock to be carried through, got %d", resp.GetDatabaseNowUnixMs())
	}

	ids := []string{}
	for _, sandbox := range resp.GetSandboxes() {
		ids = append(ids, sandbox.GetSandboxId())
	}
	if ids[0] != "s1" || ids[1] != "s2" || ids[2] != "s3" {
		t.Fatalf("expected rows sorted by sandbox id, got %v", ids)
	}

	resuming := resp.GetSandboxes()[0]
	if resuming.GetHolderNodeId() != "node-b" {
		t.Fatalf("expected a resuming row to be held by its claimer, got %q", resuming.GetHolderNodeId())
	}
	if resuming.GetOriginNodeId() != "node-a" {
		t.Fatalf("expected origin to be reported unchanged, got %q", resuming.GetOriginNodeId())
	}
	// A NULL lease column renders as 0, which the field comment defines as NULL.
	if resuming.GetLeaseExpiresAtUnixMs() != 0 {
		t.Fatalf("expected a NULL lease to render as 0, got %d", resuming.GetLeaseExpiresAtUnixMs())
	}
	if resuming.GetUpdatedAtUnixMs() != now.UnixMilli() {
		t.Fatalf("unexpected updated_at %d", resuming.GetUpdatedAtUnixMs())
	}

	paused := resp.GetSandboxes()[2]
	if paused.GetLeaseExpiresAtUnixMs() != now.Add(time.Minute).UnixMilli() {
		t.Fatalf("unexpected lease timestamp %d", paused.GetLeaseExpiresAtUnixMs())
	}
	if paused.GetGeneration() != 4 {
		t.Fatalf("unexpected generation %d", paused.GetGeneration())
	}
}

func TestListRegistrySandboxesFilters(t *testing.T) {
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	svc := newRegistryTestService(t, &stubRegistryReader{listing: registryTestListing(now)})
	ctx := context.Background()

	byState, err := svc.ListRegistrySandboxes(ctx, &schedulerv1.ListRegistrySandboxesRequest{State: "LOCAL_ONLY"})
	if err != nil {
		t.Fatalf("list by state failed: %v", err)
	}
	if len(byState.GetSandboxes()) != 1 || byState.GetSandboxes()[0].GetSandboxId() != "s2" {
		t.Fatalf("unexpected rows for the state filter: %v", byState.GetSandboxes())
	}

	// The node filter matches the holder, so a resuming row belongs to its
	// claimer and not to the node that still has the artifacts.
	byNode, err := svc.ListRegistrySandboxes(ctx, &schedulerv1.ListRegistrySandboxesRequest{NodeId: "node-b"})
	if err != nil {
		t.Fatalf("list by node failed: %v", err)
	}
	ids := []string{}
	for _, sandbox := range byNode.GetSandboxes() {
		ids = append(ids, sandbox.GetSandboxId())
	}
	if len(ids) != 2 || ids[0] != "s1" || ids[1] != "s2" {
		t.Fatalf("unexpected rows for the node filter: %v", ids)
	}
}

func TestListRegistrySandboxesPaginates(t *testing.T) {
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	svc := newRegistryTestService(t, &stubRegistryReader{listing: registryTestListing(now)})
	ctx := context.Background()

	seen := []string{}
	token := ""
	for page := 0; page < 5; page++ {
		resp, err := svc.ListRegistrySandboxes(ctx, &schedulerv1.ListRegistrySandboxesRequest{
			PageSize:  2,
			PageToken: token,
		})
		if err != nil {
			t.Fatalf("page %d failed: %v", page, err)
		}
		for _, sandbox := range resp.GetSandboxes() {
			seen = append(seen, sandbox.GetSandboxId())
		}
		token = resp.GetNextPageToken()
		if token == "" {
			break
		}
	}

	if len(seen) != 3 || seen[0] != "s1" || seen[1] != "s2" || seen[2] != "s3" {
		t.Fatalf("expected every row exactly once in order, got %v", seen)
	}
	if token != "" {
		t.Fatalf("expected pagination to terminate, got token %q", token)
	}
}

func TestListRegistrySandboxesRejectsNegativePageSize(t *testing.T) {
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	svc := newRegistryTestService(t, &stubRegistryReader{listing: registryTestListing(now)})

	_, err := svc.ListRegistrySandboxes(context.Background(), &schedulerv1.ListRegistrySandboxesRequest{PageSize: -1})
	requireCode(t, err, codes.InvalidArgument)
}

// 🔴 Regression guard for the trap that would make every later phase a no-op:
// a gateway configured with query_only_scheduler_addr sends its sandbox
// data-plane lookups here, so this replica has to carry the registry too.
func TestQueryOnlyServiceCarriesTheRegistry(t *testing.T) {
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	store := NewInMemoryBindingStore(time.Second)
	ctx := context.Background()

	unwired := NewQueryOnlyService(zap.NewNop(), store)
	_, err := unwired.ListRegistrySandboxes(ctx, &schedulerv1.ListRegistrySandboxesRequest{})
	requireCode(t, err, codes.FailedPrecondition)

	wired := NewQueryOnlyService(zap.NewNop(), store,
		WithQueryOnlyPausedRegistry(&stubRegistryReader{listing: registryTestListing(now)}))
	resp, err := wired.ListRegistrySandboxes(ctx, &schedulerv1.ListRegistrySandboxesRequest{})
	if err != nil {
		t.Fatalf("list on the query-only replica failed: %v", err)
	}
	if len(resp.GetSandboxes()) != 3 {
		t.Fatalf("expected 3 rows on the query-only replica, got %d", len(resp.GetSandboxes()))
	}

	failing := NewQueryOnlyService(zap.NewNop(), store,
		WithQueryOnlyPausedRegistry(&stubRegistryReader{err: errors.New("connection refused")}))
	_, err = failing.ListRegistrySandboxes(ctx, &schedulerv1.ListRegistrySandboxesRequest{})
	requireCode(t, err, codes.Unavailable)
}
