package scheduler

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"
	"agentenv/services/scheduler/internal/catalog"
	pausedregistry "agentenv/services/scheduler/internal/registry"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The service is a translation layer, so what is tested here is the
// translation: which requests it refuses before they reach a statement, which
// refusals travel in the response rather than as a status code, and — with a
// real database — that the paused registry's half of a catalog write really
// does share the catalog's transaction.

// ─────────────────────────────────────────────────────────────────────────────
// A store that answers whatever the test needs
// ─────────────────────────────────────────────────────────────────────────────

type stubCatalogStore struct {
	beginOut  catalog.BeginOutcome
	beginErr  error
	commitOut catalog.CommitOutcome
	commitErr error
	failOut   catalog.FailOutcome
	failErr   error
	getRow    *catalog.SnapshotRow
	getErr    error
	page      catalog.ListPage
	pageErr   error
	deleted   catalog.Deleted
	deleteErr error
	alias     *catalog.AliasTarget
	aliasErr  error
	build     catalog.StartBuildOutcome
	buildErr  error
	live      bool
	liveErr   error
	buildRow  *catalog.BuildRow
	buildRErr error
	reaped    []catalog.ReapedBuild
	reapErr   error

	// What the store was actually asked for, so a test can assert the
	// translation rather than only the answer.
	lastBegin  catalog.BeginInput
	lastCommit catalog.CommitInput
	lastFail   catalog.FailInput
	lastList   catalog.ListInput
	lastRead   catalog.ReadOptions
}

func (s *stubCatalogStore) BeginSnapshot(_ context.Context, in catalog.BeginInput) (catalog.BeginOutcome, error) {
	s.lastBegin = in
	return s.beginOut, s.beginErr
}

func (s *stubCatalogStore) CommitSnapshot(_ context.Context, in catalog.CommitInput) (catalog.CommitOutcome, error) {
	s.lastCommit = in
	return s.commitOut, s.commitErr
}

func (s *stubCatalogStore) FailSnapshot(_ context.Context, in catalog.FailInput) (catalog.FailOutcome, error) {
	s.lastFail = in
	return s.failOut, s.failErr
}

func (s *stubCatalogStore) GetSnapshot(_ context.Context, _, _ string, opts catalog.ReadOptions) (*catalog.SnapshotRow, error) {
	s.lastRead = opts
	return s.getRow, s.getErr
}

func (s *stubCatalogStore) ListSnapshots(_ context.Context, in catalog.ListInput) (catalog.ListPage, error) {
	s.lastList = in
	return s.page, s.pageErr
}

func (s *stubCatalogStore) DeleteSnapshot(_ context.Context, _, _ string, _ int64) (catalog.Deleted, error) {
	return s.deleted, s.deleteErr
}

func (s *stubCatalogStore) ResolveAlias(_ context.Context, _, _ string, _ bool) (*catalog.AliasTarget, error) {
	return s.alias, s.aliasErr
}

func (s *stubCatalogStore) StartBuild(_ context.Context, _ catalog.StartBuildInput) (catalog.StartBuildOutcome, error) {
	return s.build, s.buildErr
}

func (s *stubCatalogStore) RenewBuildLease(_ context.Context, _ catalog.RenewBuildLeaseInput) (bool, error) {
	return s.live, s.liveErr
}

func (s *stubCatalogStore) GetBuild(_ context.Context, _, _ string) (*catalog.BuildRow, error) {
	return s.buildRow, s.buildRErr
}

func (s *stubCatalogStore) ReapExpiredBuilds(_ context.Context, _ catalog.ReapInput) ([]catalog.ReapedBuild, error) {
	return s.reaped, s.reapErr
}

func (s *stubCatalogStore) Close() {}

type stubGate struct{ err error }

func (g stubGate) Require() error { return g.err }

const serviceCluster = "11111111-1111-1111-1111-111111111111"

func newCatalogService(t *testing.T, store catalog.Store, gate catalogGate) *SnapshotCatalogService {
	t.Helper()
	return NewSnapshotCatalogService(zap.NewNop(), store, gate, serviceCluster)
}

func serviceUUID(t *testing.T) string {
	t.Helper()

	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("generate a uuid: %v", err)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// ─────────────────────────────────────────────────────────────────────────────
// Admission
// ─────────────────────────────────────────────────────────────────────────────

// TestCatalogRefusesEverythingBeforeItsSchemaIsThere is the one thing the
// service must do that a working store cannot: say it cannot answer.
//
// 🔴 An empty page is the alternative shape of "not ready", and a caller reads
// that as "this cluster has no such snapshot" — on the strength of which it
// collects artifacts.
func TestCatalogRefusesEverythingBeforeItsSchemaIsThere(t *testing.T) {
	svc := newCatalogService(t, &stubCatalogStore{}, stubGate{err: pausedregistry.ErrNotReady})

	if _, err := svc.ListSnapshots(context.Background(), &schedulerv1.ListSnapshotsRequest{ClusterId: serviceCluster}); err == nil {
		t.Fatal("a listing before the migration answered instead of refusing")
	} else if status.Code(err) != codes.Unavailable {
		t.Fatalf("code = %s, want Unavailable", status.Code(err))
	}

	if _, err := svc.GetSnapshot(context.Background(), &schedulerv1.GetSnapshotRequest{ClusterId: serviceCluster}); status.Code(err) != codes.Unavailable {
		t.Fatalf("code = %s, want Unavailable", status.Code(err))
	}
}

func TestCatalogRefusesAClusterItDoesNotOwn(t *testing.T) {
	svc := newCatalogService(t, &stubCatalogStore{}, stubGate{})

	_, err := svc.GetSnapshot(context.Background(), &schedulerv1.GetSnapshotRequest{
		ClusterId: "22222222-2222-2222-2222-222222222222",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %s, want InvalidArgument", status.Code(err))
	}

	_, err = svc.GetSnapshot(context.Background(), &schedulerv1.GetSnapshotRequest{})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("a request naming no cluster gave %s", status.Code(err))
	}

	// The scope is compared without regard to case, because a uuid rendered by
	// two builds may differ only in that.
	if _, err := svc.GetSnapshot(context.Background(), &schedulerv1.GetSnapshotRequest{
		ClusterId: strings.ToUpper(serviceCluster),
	}); err != nil {
		t.Fatalf("the owning cluster in upper case was refused: %v", err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Refusals are answers, failures are codes
// ─────────────────────────────────────────────────────────────────────────────

func TestRefusalsTravelInTheResponse(t *testing.T) {
	cases := []struct {
		name string
		in   catalog.Rejection
		want schedulerv1.CatalogRejection
	}{
		{"not found", catalog.RejectionNotFound, schedulerv1.CatalogRejection_CATALOG_REJECTION_NOT_FOUND},
		{"status", catalog.RejectionStatusMismatch, schedulerv1.CatalogRejection_CATALOG_REJECTION_STATUS_MISMATCH},
		{"alias", catalog.RejectionAliasTaken, schedulerv1.CatalogRejection_CATALOG_REJECTION_ALIAS_TAKEN},
		{"generation", catalog.RejectionGenerationMismatch, schedulerv1.CatalogRejection_CATALOG_REJECTION_GENERATION_MISMATCH},
		{"superseded", catalog.RejectionExecutionSuperseded, schedulerv1.CatalogRejection_CATALOG_REJECTION_EXECUTION_SUPERSEDED},
		{"build in progress", catalog.RejectionBuildInProgress, schedulerv1.CatalogRejection_CATALOG_REJECTION_BUILD_IN_PROGRESS},
		{"queue full", catalog.RejectionBuildQueueFull, schedulerv1.CatalogRejection_CATALOG_REJECTION_BUILD_QUEUE_FULL},
		{"already exists", catalog.RejectionAlreadyExists, schedulerv1.CatalogRejection_CATALOG_REJECTION_ALREADY_EXISTS},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := &stubCatalogStore{beginOut: catalog.BeginOutcome{Rejected: &catalog.Rejected{Reason: tc.in}}}
			svc := newCatalogService(t, store, stubGate{})

			resp, err := svc.BeginSnapshot(context.Background(), &schedulerv1.BeginSnapshotRequest{
				ClusterId: serviceCluster, SnapshotId: serviceUUID(t), SourceKind: "template", Status: "waiting",
			})
			// 🔴 A refusal is a successful RPC. A status code cannot carry
			// which row won, what the status is now, or who holds the alias —
			// and all three are what the caller branches on.
			if err != nil {
				t.Fatalf("a refusal was reported as an error: %v", err)
			}
			if resp.GetRejected().GetReason() != tc.want {
				t.Fatalf("reason = %v, want %v", resp.GetRejected().GetReason(), tc.want)
			}
			if resp.GetBegan() != nil {
				t.Fatal("a refusal carried a row as well")
			}
		})
	}

	// 🔴 A reason this build does not know maps to UNSPECIFIED and not to a
	// plausible neighbour: the caller's response to "the alias is taken" and to
	// "you are superseded" are opposite.
	store := &stubCatalogStore{beginOut: catalog.BeginOutcome{Rejected: &catalog.Rejected{Reason: catalog.Rejection("something-new")}}}
	svc := newCatalogService(t, store, stubGate{})
	resp, err := svc.BeginSnapshot(context.Background(), &schedulerv1.BeginSnapshotRequest{ClusterId: serviceCluster})
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if resp.GetRejected().GetReason() != schedulerv1.CatalogRejection_CATALOG_REJECTION_UNSPECIFIED {
		t.Fatalf("an unknown reason mapped to %v", resp.GetRejected().GetReason())
	}
}

func TestRefusalsCarryWhatTheCallerActsOn(t *testing.T) {
	generation := int64(7)
	store := &stubCatalogStore{commitOut: catalog.CommitOutcome{Rejected: &catalog.Rejected{
		Reason:             catalog.RejectionGenerationMismatch,
		ObservedStatus:     "building",
		AliasHolder:        "aaaaaaaa-0000-4000-8000-000000000001",
		ActiveBuildID:      "bbbbbbbb-0000-4000-8000-000000000002",
		ObservedGeneration: &generation,
	}}}
	svc := newCatalogService(t, store, stubGate{})

	resp, err := svc.CommitSnapshot(context.Background(), &schedulerv1.CommitSnapshotRequest{
		ClusterId: serviceCluster, SnapshotId: serviceUUID(t), CommittedPayload: []byte{1}, Published: true,
	})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	got := resp.GetRejected()
	if got.GetObservedStatus() != "building" || got.GetAliasHolderSnapshotId() == "" || got.GetActiveBuildId() == "" {
		t.Fatalf("the refusal lost its detail: %+v", got)
	}
	if got.GetObservedGeneration() != generation {
		t.Fatalf("observed generation = %d, want %d", got.GetObservedGeneration(), generation)
	}
}

func TestFailuresBecomeCodesTheCallerCanTellApart(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want codes.Code
	}{
		{"a malformed request", fmt.Errorf("%w: bad id", catalog.ErrInvalidArgument), codes.InvalidArgument},
		// A row this build cannot make sense of wants an operator, not a
		// retry. Left to the default it would read as "unavailable" and send
		// every node into a loop over a write that can never succeed.
		{"a row nothing can repair", fmt.Errorf("%w: ready with no payload", catalog.ErrInvalidRecord), codes.FailedPrecondition},
		{"no registry to write to", catalog.ErrNoPausedHalf, codes.FailedPrecondition},
		{"the schema is not there", pausedregistry.ErrNotReady, codes.Unavailable},
		{"a cancelled caller", context.Canceled, codes.Canceled},
		{"a deadline", context.DeadlineExceeded, codes.DeadlineExceeded},
		{"anything else", errors.New("connection reset"), codes.Unavailable},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc := newCatalogService(t, &stubCatalogStore{getErr: tc.err}, stubGate{})
			_, err := svc.GetSnapshot(context.Background(), &schedulerv1.GetSnapshotRequest{
				ClusterId: serviceCluster, IdOrAlias: "x",
			})
			if status.Code(err) != tc.want {
				t.Fatalf("code = %s, want %s", status.Code(err), tc.want)
			}
		})
	}
}

// TestAStoreThatAnswersNeitherWayIsAnError is the rule that no failure is ever
// answered with an empty success.
func TestAStoreThatAnswersNeitherWayIsAnError(t *testing.T) {
	svc := newCatalogService(t, &stubCatalogStore{}, stubGate{})

	if _, err := svc.BeginSnapshot(context.Background(), &schedulerv1.BeginSnapshotRequest{ClusterId: serviceCluster}); err == nil {
		t.Fatal("a store answering neither a row nor a refusal produced a successful begin")
	}
	if _, err := svc.CommitSnapshot(context.Background(), &schedulerv1.CommitSnapshotRequest{
		ClusterId: serviceCluster, CommittedPayload: []byte{1},
	}); err == nil {
		t.Fatal("a store answering neither a row nor a refusal produced a successful commit")
	}
	if _, err := svc.StartBuild(context.Background(), &schedulerv1.StartBuildRequest{ClusterId: serviceCluster}); err == nil {
		t.Fatal("a build was reported started with no row to show for it")
	}
}

// TestAbsenceIsAnAnswerAndNotAnError is the other side of the same rule: "there
// is no such snapshot" is a fact, and reporting it as a failure would make
// every ordinary miss look like an outage.
func TestAbsenceIsAnAnswerAndNotAnError(t *testing.T) {
	svc := newCatalogService(t, &stubCatalogStore{}, stubGate{})
	ctx := context.Background()

	resp, err := svc.GetSnapshot(ctx, &schedulerv1.GetSnapshotRequest{ClusterId: serviceCluster, IdOrAlias: "nothing"})
	if err != nil || resp.Row != nil {
		t.Fatalf("get: %v %v", resp, err)
	}
	alias, err := svc.ResolveAlias(ctx, &schedulerv1.ResolveAliasRequest{ClusterId: serviceCluster, Alias: "nothing"})
	if err != nil || alias.GetSnapshotId() != "" {
		t.Fatalf("resolve: %v %v", alias, err)
	}
	build, err := svc.GetBuild(ctx, &schedulerv1.GetBuildRequest{ClusterId: serviceCluster, BuildId: "nothing"})
	if err != nil || build.Build != nil {
		t.Fatalf("get build: %v %v", build, err)
	}
	deleted, err := svc.DeleteSnapshot(ctx, &schedulerv1.DeleteSnapshotRequest{ClusterId: serviceCluster, IdOrAlias: "nothing"})
	if err != nil || deleted.GetDeleted() {
		t.Fatalf("delete: %v %v", deleted, err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// The registry half of a request
// ─────────────────────────────────────────────────────────────────────────────

// TestATransitionFieldThatMeansNothingIsRefused follows TransitionSandbox's
// rule: a field the requested kind never writes is refused rather than dropped,
// because both mistakes are invisible from the caller's side — the response
// says the write succeeded, because it did.
func TestATransitionFieldThatMeansNothingIsRefused(t *testing.T) {
	svc := newCatalogService(t, &stubCatalogStore{}, stubGate{})
	ctx := context.Background()
	generation := int64(3)
	deadline := int64(1700000000000000)

	beginCases := []struct {
		name string
		t    *schedulerv1.CatalogPausedTransition
		want string
	}{
		{
			name: "the wrong kind",
			t:    &schedulerv1.CatalogPausedTransition{Kind: schedulerv1.TransitionKind_TRANSITION_KIND_COMPLETE_PAUSE},
			want: "begin_pause",
		},
		{
			name: "a generation to quote",
			t: &schedulerv1.CatalogPausedTransition{
				Kind: schedulerv1.TransitionKind_TRANSITION_KIND_BEGIN_PAUSE, ExpectGeneration: &generation,
			},
			want: "quotes no generation",
		},
		{
			name: "no metadata",
			t:    &schedulerv1.CatalogPausedTransition{Kind: schedulerv1.TransitionKind_TRANSITION_KIND_BEGIN_PAUSE},
			want: "no metadata",
		},
		{
			name: "no incarnation",
			t: &schedulerv1.CatalogPausedTransition{
				Kind: schedulerv1.TransitionKind_TRANSITION_KIND_BEGIN_PAUSE, MetadataJson: []byte(`{"a":1}`),
			},
			want: "execution id",
		},
		{
			// mark_running and the lease renewal record a deadline; none of
			// these three do. A caller that sent one would believe it stored
			// while reclamation went on treating the sandbox as having none.
			name: "a sandbox deadline",
			t: &schedulerv1.CatalogPausedTransition{
				Kind: schedulerv1.TransitionKind_TRANSITION_KIND_BEGIN_PAUSE, MetadataJson: []byte(`{"a":1}`),
				ExecutionId: serviceUUID(t), SandboxExpiresAtUnixMicros: &deadline,
			},
			want: "deadline",
		},
	}

	for _, tc := range beginCases {
		t.Run("begin/"+tc.name, func(t *testing.T) {
			_, err := svc.BeginSnapshot(ctx, &schedulerv1.BeginSnapshotRequest{
				ClusterId: serviceCluster, PausedTransition: tc.t,
			})
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("code = %s, want InvalidArgument", status.Code(err))
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not mention %q", err, tc.want)
			}
		})
	}

	commitCases := []struct {
		name string
		t    *schedulerv1.CatalogPausedTransition
		want string
	}{
		{
			name: "the wrong kind",
			t:    &schedulerv1.CatalogPausedTransition{Kind: schedulerv1.TransitionKind_TRANSITION_KIND_MARK_RUNNING},
			want: "complete_pause or mark_local_only",
		},
		{
			name: "no generation",
			t:    &schedulerv1.CatalogPausedTransition{Kind: schedulerv1.TransitionKind_TRANSITION_KIND_COMPLETE_PAUSE},
			want: "quotes no generation",
		},
		{
			name: "metadata only begin_pause records",
			t: &schedulerv1.CatalogPausedTransition{
				Kind:             schedulerv1.TransitionKind_TRANSITION_KIND_COMPLETE_PAUSE,
				ExpectGeneration: &generation, MetadataJson: []byte(`{"a":1}`),
			},
			want: "carries metadata",
		},
		{
			name: "an incarnation only begin_pause is fenced on",
			t: &schedulerv1.CatalogPausedTransition{
				Kind:             schedulerv1.TransitionKind_TRANSITION_KIND_COMPLETE_PAUSE,
				ExpectGeneration: &generation, ExecutionId: serviceUUID(t),
			},
			want: "names an incarnation",
		},
	}

	for _, tc := range commitCases {
		t.Run("commit/"+tc.name, func(t *testing.T) {
			_, err := svc.CommitSnapshot(ctx, &schedulerv1.CommitSnapshotRequest{
				ClusterId: serviceCluster, CommittedPayload: []byte{1}, Published: true, PausedTransition: tc.t,
			})
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("code = %s, want InvalidArgument", status.Code(err))
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not mention %q", err, tc.want)
			}
		})
	}

	// A failed snapshot can only park its sandbox: complete_pause names a
	// snapshot the sandbox can come back from, and this call is the statement
	// that there is none.
	_, err := svc.FailSnapshot(ctx, &schedulerv1.FailSnapshotRequest{
		ClusterId: serviceCluster, BuildErrorJson: []byte(`{"message":"x"}`),
		PausedTransition: &schedulerv1.CatalogPausedTransition{
			Kind: schedulerv1.TransitionKind_TRANSITION_KIND_COMPLETE_PAUSE, ExpectGeneration: &generation,
		},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %s, want InvalidArgument", status.Code(err))
	}
}

// TestTheTwoHalvesMustAgreeAboutWhoCanStartTheSandbox refuses the pair of
// combinations that would leave the catalog and the registry disagreeing.
//
// 🔴 complete_pause with published=false is the dangerous one: the registry
// hands the row to any node while the catalog says only the origin has the
// bytes, so a resume elsewhere is granted a claim it cannot honour.
func TestTheTwoHalvesMustAgreeAboutWhoCanStartTheSandbox(t *testing.T) {
	svc := newCatalogService(t, &stubCatalogStore{}, stubGate{})
	generation := int64(1)

	cases := []struct {
		name      string
		kind      schedulerv1.TransitionKind
		published bool
		ok        bool
	}{
		{"complete and published", schedulerv1.TransitionKind_TRANSITION_KIND_COMPLETE_PAUSE, true, true},
		{"local only and unpublished", schedulerv1.TransitionKind_TRANSITION_KIND_MARK_LOCAL_ONLY, false, true},
		{"complete but unpublished", schedulerv1.TransitionKind_TRANSITION_KIND_COMPLETE_PAUSE, false, false},
		{"local only but published", schedulerv1.TransitionKind_TRANSITION_KIND_MARK_LOCAL_ONLY, true, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := svc.CommitSnapshot(context.Background(), &schedulerv1.CommitSnapshotRequest{
				ClusterId: serviceCluster, SnapshotId: serviceUUID(t),
				CommittedPayload: []byte{1}, Published: tc.published, OriginNodeId: "node-a",
				PausedTransition: &schedulerv1.CatalogPausedTransition{
					Kind: tc.kind, SandboxId: serviceUUID(t), ExpectGeneration: &generation,
				},
			})
			if tc.ok {
				// The stub answers neither way, so a request that got past the
				// checks fails for that reason and not for InvalidArgument.
				if status.Code(err) == codes.InvalidArgument {
					t.Fatalf("a coherent pair was refused: %v", err)
				}
				return
			}
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("code = %s, want InvalidArgument", status.Code(err))
			}
			if !strings.Contains(err.Error(), "disagree") {
				t.Fatalf("error %q does not say what disagrees", err)
			}
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Translation
// ─────────────────────────────────────────────────────────────────────────────

func TestListingTranslatesItsCursorAndFilterBothWays(t *testing.T) {
	store := &stubCatalogStore{page: catalog.ListPage{
		Rows: []catalog.SnapshotRow{{SnapshotID: "aaaaaaaa-0000-4000-8000-000000000001", CreatedAtMs: 5}},
		Next: &catalog.Cursor{CreatedAtMs: 5, SnapshotID: "aaaaaaaa-0000-4000-8000-000000000001"},
	}}
	svc := newCatalogService(t, store, stubGate{})

	prefix := "pre"
	resp, err := svc.ListSnapshots(context.Background(), &schedulerv1.ListSnapshotsRequest{
		ClusterId: serviceCluster,
		Cursor:    &schedulerv1.SnapshotCursor{CreatedAtUnixMs: 99, SnapshotId: "bbbbbbbb-0000-4000-8000-000000000002"},
		Filter: &schedulerv1.SnapshotFilter{
			SourceKinds: []string{"sandbox"},
			AliasPrefix: &prefix,
		},
		Limit:     25,
		OnlyReady: true,
		WithBuild: true,
	})
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	// 🔴 Milliseconds through, in both directions. The public pagination token
	// is rendered from an i64 of milliseconds, and a unit conversion anywhere
	// on this path moves a page boundary onto the wrong row.
	if store.lastList.Cursor == nil || store.lastList.Cursor.CreatedAtMs != 99 {
		t.Fatalf("the cursor did not reach the store intact: %+v", store.lastList.Cursor)
	}
	if store.lastList.Cursor.SnapshotID != "bbbbbbbb-0000-4000-8000-000000000002" {
		t.Fatalf("cursor id = %q", store.lastList.Cursor.SnapshotID)
	}
	if store.lastList.Limit != 25 || !store.lastList.OnlyReady || !store.lastList.WithBuild {
		t.Fatalf("the read options did not travel: %+v", store.lastList)
	}
	if len(store.lastList.Filter.SourceKinds) != 1 || store.lastList.Filter.AliasPrefix == nil {
		t.Fatalf("the filter did not travel: %+v", store.lastList.Filter)
	}
	if resp.GetNextCursor().GetCreatedAtUnixMs() != 5 || resp.GetNextCursor().GetSnapshotId() == "" {
		t.Fatalf("the next cursor did not come back: %+v", resp.GetNextCursor())
	}
}

// TestAbsentAndPresentAreNotTheSameOnTheWire checks the optional fields that a
// zero value would misreport.
func TestAbsentAndPresentAreNotTheSameOnTheWire(t *testing.T) {
	schema := uint32(2)
	started := int64(11)
	store := &stubCatalogStore{getRow: &catalog.SnapshotRow{
		SnapshotID:       "aaaaaaaa-0000-4000-8000-000000000001",
		Status:           "ready",
		CommittedPayload: []byte{0x00, 0xff},
		CommittedSchema:  &schema,
		BuildStartedAtMs: &started,
		Published:        false,
		OriginNodeID:     "node-a",
	}}
	svc := newCatalogService(t, store, stubGate{})

	resp, err := svc.GetSnapshot(context.Background(), &schedulerv1.GetSnapshotRequest{
		ClusterId: serviceCluster, IdOrAlias: "x", OnlyReady: true, WithBuild: true,
	})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	row := resp.GetRow()
	if string(row.GetCommittedPayload()) != string([]byte{0x00, 0xff}) {
		t.Fatalf("the payload was altered: %x", row.GetCommittedPayload())
	}
	if row.CommittedSchema == nil || *row.CommittedSchema != 2 {
		t.Fatalf("committed schema = %v", row.CommittedSchema)
	}
	// 🔴 Absent, not zero. A sandbox that never started and one that started at
	// the epoch are different facts, and the second is what a zero would say.
	if row.SandboxStartedAtUnixMs != nil {
		t.Fatalf("an absent timestamp came back as %v", *row.SandboxStartedAtUnixMs)
	}
	if row.BuildStartedAtUnixMs == nil || *row.BuildStartedAtUnixMs != 11 {
		t.Fatalf("build started = %v", row.BuildStartedAtUnixMs)
	}
	if row.GetPublished() || row.GetOriginNodeId() != "node-a" {
		t.Fatalf("the origin block did not travel: %+v", row)
	}
	if !store.lastRead.OnlyReady || !store.lastRead.WithBuild {
		t.Fatalf("the read options did not reach the store: %+v", store.lastRead)
	}
}

func TestRenewBuildLeaseReportsTheReapedBuild(t *testing.T) {
	svc := newCatalogService(t, &stubCatalogStore{live: false}, stubGate{})

	// 🔴 False is the only notice a builder gets that its template has been
	// handed to somebody else. An error here would be retried; a true would let
	// two builders publish into one template.
	resp, err := svc.RenewBuildLease(context.Background(), &schedulerv1.RenewBuildLeaseRequest{
		ClusterId: serviceCluster, NodeId: "node-a", BuildId: serviceUUID(t),
	})
	if err != nil {
		t.Fatalf("renew: %v", err)
	}
	if resp.GetLive() {
		t.Fatal("a reaped build was reported live")
	}
}

func TestFailSnapshotPassesTheReasonThroughUntouched(t *testing.T) {
	store := &stubCatalogStore{failOut: catalog.FailOutcome{Row: &catalog.SnapshotRow{Status: "error"}}}
	svc := newCatalogService(t, store, stubGate{})

	// A TemplateBuildErrorReason with a shape this build has never heard of.
	// It is stored and handed back; nothing here has an opinion about it.
	reason := []byte(`{"message":"x","step":{"nested":[1,2,3]},"unknown_field":true}`)
	if _, err := svc.FailSnapshot(context.Background(), &schedulerv1.FailSnapshotRequest{
		ClusterId: serviceCluster, SnapshotId: serviceUUID(t), BuildErrorJson: reason,
	}); err != nil {
		t.Fatalf("fail: %v", err)
	}
	if string(store.lastFail.BuildError) != string(reason) {
		t.Fatalf("the reason was rewritten on the way in:\n got %s\nwant %s", store.lastFail.BuildError, reason)
	}
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(store.lastFail.BuildError, &probe); err != nil {
		t.Fatalf("the reason stopped being JSON: %v", err)
	}
	if _, ok := probe["unknown_field"]; !ok {
		t.Fatal("a field this build has not heard of was dropped")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// The real paused half
// ─────────────────────────────────────────────────────────────────────────────
//
// 🔴 The tests above use a stand-in for `paused_sandboxes`; these use the table
// itself, through the adapter that runs the registry's own statements inside
// the catalog's transaction. That adapter is the seam the whole design rests
// on — it is what makes "the sandbox is paused" and "here is the snapshot it
// paused into" one commit — and it is the one piece a stand-in cannot check,
// because a stand-in would be agreeing with itself.

func requireServiceTestDSN(t *testing.T) string {
	t.Helper()

	dsn := os.Getenv("SCHEDULER_REGISTRY_TEST_DSN")
	if dsn == "" {
		if os.Getenv("SCHEDULER_REGISTRY_TEST_REQUIRED") != "" {
			t.Fatal("SCHEDULER_REGISTRY_TEST_REQUIRED is set but SCHEDULER_REGISTRY_TEST_DSN is not: these tests would have been skipped")
		}
		t.Skip("SCHEDULER_REGISTRY_TEST_DSN is not set; skipping the real PostgreSQL test")
	}
	return dsn
}

// pausedHalfFixture is both schemas on one pool, wired the way main.go wires
// them.
type pausedHalfFixture struct {
	t        *testing.T
	ctx      context.Context
	registry *pausedregistry.PostgresStore
	store    *catalog.PostgresStore
	svc      *SnapshotCatalogService
	pool     *pgxpool.Pool
}

func newPausedHalfFixture(t *testing.T) *pausedHalfFixture {
	t.Helper()

	dsn := requireServiceTestDSN(t)
	ctx := context.Background()

	// Each test gets its own schema, so two of them can read an unfiltered
	// table without seeing each other's rows.
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = admin.Close(ctx) }()

	schema := serviceTestSchema(t)
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+quoted); err != nil {
		t.Fatalf("create schema %s: %v", schema, err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		conn, err := pgx.Connect(cleanup, dsn)
		if err != nil {
			t.Logf("reconnect to drop %s: %v", schema, err)
			return
		}
		defer func() { _ = conn.Close(cleanup) }()
		if _, err := conn.Exec(cleanup, "DROP SCHEMA "+quoted+" CASCADE"); err != nil {
			t.Logf("drop %s: %v", schema, err)
		}
	})

	scoped := serviceDSNInSchema(t, dsn, schema)
	// 🔴 Write fencing on, which is what the deployment runs. With it off the
	// begin_pause statement installs an incarnation instead of comparing one,
	// and the refusal these tests are about cannot happen.
	generic, err := pausedregistry.NewStore(ctx, pausedregistry.StoreConfig{DSN: scoped, WriteFencing: true})
	if err != nil {
		t.Fatalf("create the registry store: %v", err)
	}
	registryStore, ok := generic.(*pausedregistry.PostgresStore)
	if !ok {
		t.Fatalf("the registry store is a %T, and the adapter needs the postgres one", generic)
	}
	t.Cleanup(registryStore.Close)

	if err := registryStore.Migrate(ctx); err != nil {
		t.Fatalf("migrate the registry: %v", err)
	}
	if err := catalog.Migrate(ctx, registryStore.Pool()); err != nil {
		t.Fatalf("migrate the catalog: %v", err)
	}

	// The same pool for both, which is the arrangement the two halves need.
	store := catalog.NewStoreWithPool(registryStore.Pool(), catalog.StoreConfig{
		Logger: zap.NewNop(),
		Paused: NewPausedHalfAdapter(registryStore),
	})
	return &pausedHalfFixture{
		t:        t,
		ctx:      ctx,
		registry: registryStore,
		store:    store,
		svc:      NewSnapshotCatalogService(zap.NewNop(), store, stubGate{}, serviceCluster),
		pool:     registryStore.Pool(),
	}
}

func serviceTestSchema(t *testing.T) string {
	t.Helper()

	name := []rune(t.Name())
	if len(name) > 24 {
		name = name[:24]
	}
	cleaned := make([]rune, 0, len(name))
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			cleaned = append(cleaned, r)
		default:
			cleaned = append(cleaned, '_')
		}
	}
	var unique [8]byte
	if _, err := rand.Read(unique[:]); err != nil {
		t.Fatalf("generate a schema suffix: %v", err)
	}
	// Lower case throughout: search_path carries the name unquoted and
	// PostgreSQL folds it before resolving, so a capital creates one schema and
	// then looks for another.
	return strings.ToLower(fmt.Sprintf("cattx_%s_%x", string(cleaned), unique))
}

func serviceDSNInSchema(t *testing.T, dsn, schema string) string {
	t.Helper()

	if !strings.HasPrefix(dsn, "postgres://") && !strings.HasPrefix(dsn, "postgresql://") {
		return dsn + " search_path=" + schema
	}
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse the test DSN: %v", err)
	}
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func (f *pausedHalfFixture) sandboxState(sandboxID string) (string, *string, bool) {
	f.t.Helper()

	var (
		state    string
		snapshot *string
	)
	err := f.pool.QueryRow(f.ctx,
		`SELECT state, snapshot_id::text FROM paused_sandboxes WHERE sandbox_id = $1::uuid`,
		sandboxID).Scan(&state, &snapshot)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil, false
	}
	if err != nil {
		f.t.Fatalf("read paused_sandboxes: %v", err)
	}
	return state, snapshot, true
}

func (f *pausedHalfFixture) beginPause(sandboxID, snapshotID, executionID, alias string) *schedulerv1.BeginSnapshotResponse {
	f.t.Helper()

	resp, err := f.svc.BeginSnapshot(f.ctx, &schedulerv1.BeginSnapshotRequest{
		ClusterId:       serviceCluster,
		NodeId:          "node-a",
		SnapshotId:      snapshotID,
		SourceKind:      "sandbox",
		SourceSandboxId: sandboxID,
		CpuCount:        2,
		MemoryMib:       512,
		DiskSizeMib:     2048,
		Alias:           alias,
		CreatedAtUnixMs: time.Now().UnixMilli(),
		Status:          "building",
		Published:       false,
		OriginNodeId:    "node-a",
		PausedTransition: &schedulerv1.CatalogPausedTransition{
			Kind:         schedulerv1.TransitionKind_TRANSITION_KIND_BEGIN_PAUSE,
			SandboxId:    sandboxID,
			MetadataJson: []byte(`{"sandbox_id":"x","state":"paused"}`),
			ExecutionId:  executionID,
		},
	})
	if err != nil {
		f.t.Fatalf("begin a pause: %v", err)
	}
	return resp
}

func TestARealPauseWritesBothTablesInOneTransaction(t *testing.T) {
	f := newPausedHalfFixture(t)

	sandbox := serviceUUID(t)
	snapshot := serviceUUID(t)
	resp := f.beginPause(sandbox, snapshot, serviceUUID(t), "")
	if resp.GetRejected() != nil {
		t.Fatalf("refused: %v", resp.GetRejected())
	}
	if resp.GetBegan().GetGeneration() != 1 {
		t.Fatalf("generation = %d, want 1", resp.GetBegan().GetGeneration())
	}
	if resp.GetBegan().GetRow().GetPublished() {
		t.Fatal("a pause opens its row unpublished")
	}

	state, snapshotID, ok := f.sandboxState(sandbox)
	if !ok || state != "publishing" {
		t.Fatalf("paused_sandboxes says %q (%v), want publishing", state, ok)
	}
	if snapshotID != nil {
		// begin_pause leaves the row pointing at the previous snapshot, which
		// is what keeps a pause whose upload then fails recoverable from where
		// it was rather than from nowhere.
		t.Fatalf("a first pause already names a snapshot: %v", *snapshotID)
	}
}

func TestARealCommitClosesBothHalvesTogether(t *testing.T) {
	f := newPausedHalfFixture(t)

	sandbox := serviceUUID(t)
	snapshot := serviceUUID(t)
	began := f.beginPause(sandbox, snapshot, serviceUUID(t), "")
	generation := began.GetBegan().GetGeneration()

	resp, err := f.svc.CommitSnapshot(f.ctx, &schedulerv1.CommitSnapshotRequest{
		ClusterId: serviceCluster, NodeId: "node-a", SnapshotId: snapshot,
		CommittedPayload: []byte{0x01, 0x02}, CommittedSchema: 1,
		Alias: "live", UpdatedAtUnixMs: time.Now().UnixMilli(),
		Published: true, OriginNodeId: "node-a",
		PausedTransition: &schedulerv1.CatalogPausedTransition{
			Kind:      schedulerv1.TransitionKind_TRANSITION_KIND_COMPLETE_PAUSE,
			SandboxId: sandbox, ExpectGeneration: &generation,
		},
	})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if resp.GetRejected() != nil {
		t.Fatalf("refused: %v", resp.GetRejected())
	}
	if resp.GetCommitted().GetStatus() != "ready" {
		t.Fatalf("catalog status = %q", resp.GetCommitted().GetStatus())
	}

	state, pointed, _ := f.sandboxState(sandbox)
	if state != "paused" {
		t.Fatalf("the sandbox is %q, want paused", state)
	}
	if pointed == nil || *pointed != snapshot {
		t.Fatalf("the sandbox points at %v, want %s", pointed, snapshot)
	}
}

// TestARealRefusedCommitRollsBackPausedSandboxes is the property the whole
// arrangement exists for, against the real table.
//
// 🔴 Before this, the two writes were a gRPC call apart: the registry moved to
// `paused` and the catalog was written separately, so a failure between them
// left a sandbox recorded as paused with nothing to bring it back. Here the
// catalog half refuses — the alias is held — and the registry row must not have
// moved at all.
func TestARealRefusedCommitRollsBackPausedSandboxes(t *testing.T) {
	f := newPausedHalfFixture(t)

	// Somebody else already holds the name.
	holder := serviceUUID(t)
	if _, err := f.store.BeginSnapshot(f.ctx, catalog.BeginInput{
		ClusterID: serviceCluster, SnapshotID: holder, SourceKind: catalog.SourceKindTemplate,
		Status: catalog.StatusWaiting, Alias: "contested", CPUCount: 1, MemoryMiB: 1, DiskSizeMiB: 1,
		Published: true, CreatedAtMs: time.Now().UnixMilli(),
	}); err != nil {
		t.Fatalf("seed the holder: %v", err)
	}

	sandbox := serviceUUID(t)
	snapshot := serviceUUID(t)
	began := f.beginPause(sandbox, snapshot, serviceUUID(t), "")
	generation := began.GetBegan().GetGeneration()

	resp, err := f.svc.CommitSnapshot(f.ctx, &schedulerv1.CommitSnapshotRequest{
		ClusterId: serviceCluster, NodeId: "node-a", SnapshotId: snapshot,
		CommittedPayload: []byte{0x01}, CommittedSchema: 1,
		Alias: "contested", UpdatedAtUnixMs: time.Now().UnixMilli(),
		Published: true, OriginNodeId: "node-a",
		PausedTransition: &schedulerv1.CatalogPausedTransition{
			Kind:      schedulerv1.TransitionKind_TRANSITION_KIND_COMPLETE_PAUSE,
			SandboxId: sandbox, ExpectGeneration: &generation,
		},
	})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if resp.GetRejected().GetReason() != schedulerv1.CatalogRejection_CATALOG_REJECTION_ALIAS_TAKEN {
		t.Fatalf("outcome = %v, want ALIAS_TAKEN", resp)
	}
	if resp.GetRejected().GetAliasHolderSnapshotId() != holder {
		t.Fatalf("the refusal named %q as the holder, want %q", resp.GetRejected().GetAliasHolderSnapshotId(), holder)
	}

	state, pointed, _ := f.sandboxState(sandbox)
	if state != "publishing" {
		t.Fatalf("the sandbox moved to %q on a refused commit", state)
	}
	if pointed != nil {
		t.Fatalf("the sandbox was pointed at %v by a commit that was refused", *pointed)
	}
}

// TestARealStaleGenerationRollsBackTheCatalog is the same coupling from the
// other side, and the more dangerous one: a catalog row flipped to `ready`
// while the sandbox stayed `publishing` is a snapshot the cluster would hand to
// any node while its bytes are still going up.
func TestARealStaleGenerationRollsBackTheCatalog(t *testing.T) {
	f := newPausedHalfFixture(t)

	sandbox := serviceUUID(t)
	snapshot := serviceUUID(t)
	began := f.beginPause(sandbox, snapshot, serviceUUID(t), "")
	stale := began.GetBegan().GetGeneration() - 1

	resp, err := f.svc.CommitSnapshot(f.ctx, &schedulerv1.CommitSnapshotRequest{
		ClusterId: serviceCluster, NodeId: "node-a", SnapshotId: snapshot,
		CommittedPayload: []byte{0x01}, CommittedSchema: 1,
		UpdatedAtUnixMs: time.Now().UnixMilli(), Published: true, OriginNodeId: "node-a",
		PausedTransition: &schedulerv1.CatalogPausedTransition{
			Kind:      schedulerv1.TransitionKind_TRANSITION_KIND_COMPLETE_PAUSE,
			SandboxId: sandbox, ExpectGeneration: &stale,
		},
	})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if resp.GetRejected().GetReason() != schedulerv1.CatalogRejection_CATALOG_REJECTION_GENERATION_MISMATCH {
		t.Fatalf("outcome = %v, want GENERATION_MISMATCH", resp)
	}
	// The number to re-read against, rather than leaving the caller blind.
	if resp.GetRejected().GetObservedGeneration() != began.GetBegan().GetGeneration() {
		t.Fatalf("observed generation = %d, want %d",
			resp.GetRejected().GetObservedGeneration(), began.GetBegan().GetGeneration())
	}

	row, err := f.store.GetSnapshot(f.ctx, serviceCluster, snapshot, catalog.ReadOptions{})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if row.Status != catalog.StatusBuilding {
		t.Fatalf("the catalog row is %q after a commit the registry refused: the two halves are not one transaction", row.Status)
	}
	if row.CommittedPayload != nil {
		t.Fatalf("a rolled-back commit left a payload behind: %x", row.CommittedPayload)
	}
}

// TestARealSupersededIncarnationIsRefusedTerminally is the refusal the caller
// must never retry — and the one a re-read walks straight around.
func TestARealSupersededIncarnationIsRefusedTerminally(t *testing.T) {
	f := newPausedHalfFixture(t)

	sandbox := serviceUUID(t)
	f.beginPause(sandbox, serviceUUID(t), serviceUUID(t), "")

	// A second pause of the same sandbox under a different incarnation: the VM
	// that is asking has already been replaced.
	second := serviceUUID(t)
	resp := f.beginPause(sandbox, second, serviceUUID(t), "")
	if resp.GetRejected().GetReason() != schedulerv1.CatalogRejection_CATALOG_REJECTION_EXECUTION_SUPERSEDED {
		t.Fatalf("outcome = %v, want EXECUTION_SUPERSEDED", resp)
	}

	// 🔴 And it left nothing behind. A catalog row from a superseded
	// incarnation would meet the live one's next pause as an id that already
	// exists.
	row, err := f.store.GetSnapshot(f.ctx, serviceCluster, second, catalog.ReadOptions{})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if row != nil {
		t.Fatalf("a fenced pause left a catalog row behind: %+v", row)
	}
}

// TestARealLocalOnlyKeepsBothRows is transaction C against both tables.
func TestARealLocalOnlyKeepsBothRows(t *testing.T) {
	f := newPausedHalfFixture(t)

	sandbox := serviceUUID(t)
	snapshot := serviceUUID(t)
	began := f.beginPause(sandbox, snapshot, serviceUUID(t), "")
	generation := began.GetBegan().GetGeneration()

	resp, err := f.svc.CommitSnapshot(f.ctx, &schedulerv1.CommitSnapshotRequest{
		ClusterId: serviceCluster, NodeId: "node-a", SnapshotId: snapshot,
		CommittedPayload: []byte{0x01}, CommittedSchema: 1,
		UpdatedAtUnixMs: time.Now().UnixMilli(),
		Published:       false, OriginNodeId: "node-a",
		PausedTransition: &schedulerv1.CatalogPausedTransition{
			Kind:      schedulerv1.TransitionKind_TRANSITION_KIND_MARK_LOCAL_ONLY,
			SandboxId: sandbox, ExpectGeneration: &generation,
		},
	})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if resp.GetRejected() != nil {
		t.Fatalf("refused: %v", resp.GetRejected())
	}

	// 🔴 `ready` and unpublished: the snapshot is complete and node-a can start
	// it. Recording it as anything else would tell the user a snapshot they can
	// still resume does not exist.
	committed := resp.GetCommitted()
	if committed.GetStatus() != "ready" || committed.GetPublished() {
		t.Fatalf("catalog row = %q published=%v", committed.GetStatus(), committed.GetPublished())
	}
	if allowed, pin := catalog.PinOriginIfUnpublished(catalog.SnapshotRow{
		Published: committed.GetPublished(), OriginNodeID: committed.GetOriginNodeId(),
	}, "node-b"); allowed || pin != "node-a" {
		t.Fatalf("a resume elsewhere was allowed: allowed=%v pin=%q", allowed, pin)
	}

	// 🔴 And the registry keeps its row. Deleting it would make "parked on its
	// own node" indistinguishable from "resumed elsewhere, or destroyed", and
	// reconciliation answers the second by throwing away the only copy.
	state, _, ok := f.sandboxState(sandbox)
	if !ok || state != "local_only" {
		t.Fatalf("the sandbox is %q (%v), want local_only and still there", state, ok)
	}
}

// TestThePausedHalfRunsInsideTheCallersTransaction asserts the adapter's one
// job directly, without going through the catalog at all.
//
// 🔴 Everything above shows the two halves agreeing; this shows *why*. Roll the
// caller's transaction back and the registry row must be gone with it. An
// adapter that reached for the pool instead — which is what every one of these
// statements does in its ordinary form — would leave the row behind, and every
// other test here would still pass, because none of them has a catalog failure
// after the registry half has run.
func TestThePausedHalfRunsInsideTheCallersTransaction(t *testing.T) {
	f := newPausedHalfFixture(t)

	adapter := NewPausedHalfAdapter(f.registry)
	sandbox := serviceUUID(t)

	tx, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	began, err := adapter.Begin(f.ctx, tx, catalog.PausedBegin{
		ClusterID:    serviceCluster,
		OriginNodeID: "node-a",
		SandboxID:    sandbox,
		Metadata:     json.RawMessage(`{"sandbox_id":"x"}`),
		ExecutionID:  serviceUUID(t),
	})
	if err != nil {
		t.Fatalf("the paused half refused: %v", err)
	}
	if began.Generation != 1 {
		t.Fatalf("generation = %d", began.Generation)
	}

	// Visible to the transaction that wrote it...
	var inside int
	if err := tx.QueryRow(f.ctx, `SELECT count(*) FROM paused_sandboxes WHERE sandbox_id = $1::uuid`, sandbox).Scan(&inside); err != nil {
		t.Fatalf("read inside the transaction: %v", err)
	}
	if inside != 1 {
		t.Fatalf("the row is not visible to its own transaction: %d", inside)
	}

	// ...and gone when that transaction goes.
	if err := tx.Rollback(f.ctx); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if _, _, ok := f.sandboxState(sandbox); ok {
		t.Fatal("the registry row survived the rollback of the transaction that wrote it: " +
			"the paused half is not running inside the caller's transaction, and a catalog write " +
			"that fails after it would leave the two tables disagreeing")
	}
}

// TestAdapterWithoutARegistryIsNil keeps "this process serves no registry" a
// state the catalog can refuse rather than one it half-applies.
func TestAdapterWithoutARegistryIsNil(t *testing.T) {
	if half := NewPausedHalfAdapter(nil); half != nil {
		t.Fatalf("a nil registry produced %T", half)
	}
}
