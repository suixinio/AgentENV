package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"
	pausedregistry "agentenv/services/scheduler/internal/registry"

	"github.com/jackc/pgx/v5/pgconn"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	rsCluster = "11111111-aaaa-4aaa-8aaa-111111111111"
	rsOther   = "22222222-bbbb-4bbb-8bbb-222222222222"
	rsSandbox = "aaaaaaaa-0000-4000-8000-000000000001"
	rsSnap    = "cccccccc-0000-4000-8000-000000000001"
	rsNode    = "node-a"

	// rsFloor is well under every TTL these tests report, so only the cases
	// that mean to be clamped are.
	rsFloor = time.Second
)

// fakeStore records what the handler asked for and answers what the test told
// it to. It never touches a database: the point of these tests is the
// translation, and the translation's failure mode is answering a failure with
// something that reads like a fact about the table.
type fakeStore struct {
	mu sync.Mutex

	entries map[string]pausedregistry.Entry
	claim   pausedregistry.ResumeClaim
	freed   pausedregistry.ReleasedHoldings
	renewed uint64
	// markRunning is what MarkRunning answers. The zero value is untracked,
	// which is the common case.
	markRunning pausedregistry.MarkRunningOutcome
	// released and removed are whether the two conditional writes matched.
	released bool
	removed  bool
	// now is the database clock GetMany reports.
	now time.Time
	// lastExpiresAt is the deadline the last MarkRunning carried.
	lastExpiresAt *time.Time
	began         pausedregistry.BeganPause
	err           error

	leaseTTL time.Duration

	calls             []string
	lastHeld          []pausedregistry.HeldSandbox
	lastParkedHolders []pausedregistry.ParkedLeaseHolder
	lastLiveHolders   []pausedregistry.ParkedLeaseHolder
	lastGen           int64
	lastMeta          json.RawMessage
	// lastExecution is what the handler passed down as the incarnation, so a
	// test can assert the value reached the store rather than only that the
	// call was accepted.
	lastExecution string
	// lastNodeID and lastHolderNodeID are MarkRunning's two identities, kept
	// apart so a test can prove the handler forwards the wire's holder_node_id
	// as its own argument rather than collapsing it back onto node_id.
	lastNodeID       string
	lastHolderNodeID string
	reclaims         int
	// deadlineRenewal is what RenewSandboxDeadline answers. The zero value is
	// not_tracked, matching MarkRunning's zero value being the same kind of
	// "no row" answer.
	deadlineRenewal pausedregistry.DeadlineRenewalOutcome
}

func (f *fakeStore) record(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, name)
}

func (f *fakeStore) called(name string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.calls {
		if c == name {
			return true
		}
	}
	return false
}

// rsExecution is the incarnation the fenced transitions carry. Its shape is
// what the node sends: canonical, lower case, v7.
const rsExecution = "00000001-0000-7000-8000-00000000000a"

func (f *fakeStore) BeginPause(_ context.Context, in pausedregistry.BeginPauseInput) (pausedregistry.BeganPause, error) {
	f.record("BeginPause")
	f.lastMeta = in.Metadata
	f.lastExecution = in.ExecutionID
	return f.began, f.err
}

func (f *fakeStore) CompletePause(_ context.Context, _, _ string, gen int64, _ string) error {
	f.record("CompletePause")
	f.lastGen = gen
	return f.err
}

func (f *fakeStore) MarkLocalOnly(_ context.Context, _, _ string, gen int64) error {
	f.record("MarkLocalOnly")
	f.lastGen = gen
	return f.err
}

func (f *fakeStore) Get(_ context.Context, _, id string) (pausedregistry.Entry, bool, error) {
	f.record("Get")
	entry, ok := f.entries[id]
	return entry, ok, f.err
}

func (f *fakeStore) GetMany(_ context.Context, _ string, ids []string) (pausedregistry.Rows, error) {
	f.record("GetMany")
	if f.err != nil {
		return pausedregistry.Rows{}, f.err
	}
	return pausedregistry.Rows{Entries: f.entries, Covered: ids, Now: f.now}, nil
}

func (f *fakeStore) ClaimForResume(_ context.Context, _, _, _, executionID string) (pausedregistry.ResumeClaim, error) {
	f.record("ClaimForResume")
	f.lastExecution = executionID
	if f.err != nil {
		return pausedregistry.ResumeClaim{}, f.err
	}
	return f.claim, nil
}

func (f *fakeStore) ReleaseClaim(_ context.Context, _, _ string, gen int64) (bool, error) {
	f.record("ReleaseClaim")
	f.lastGen = gen
	return f.released, f.err
}

func (f *fakeStore) RenewLease(_ context.Context, _, _ string, held []pausedregistry.HeldSandbox) (uint64, error) {
	f.record("RenewLease")
	f.lastHeld = held
	if f.err != nil {
		return 0, f.err
	}
	return f.renewed, nil
}

func (f *fakeStore) RenewParkedLeases(_ context.Context, _ string, holders []pausedregistry.ParkedLeaseHolder) (uint64, error) {
	f.record("RenewParkedLeases")
	f.mu.Lock()
	f.lastParkedHolders = holders
	f.mu.Unlock()
	if f.err != nil {
		return 0, f.err
	}
	return f.renewed, nil
}

func (f *fakeStore) RenewLiveLeases(_ context.Context, _ string, holders []pausedregistry.ParkedLeaseHolder) (uint64, error) {
	f.record("RenewLiveLeases")
	f.mu.Lock()
	f.lastLiveHolders = holders
	f.mu.Unlock()
	if f.err != nil {
		return 0, f.err
	}
	return f.renewed, nil
}

func (f *fakeStore) MarkRunning(_ context.Context, _, _, nodeID, holderNodeID, executionID string, expiresAt *time.Time) (pausedregistry.MarkRunningOutcome, error) {
	f.record("MarkRunning")
	f.lastNodeID = nodeID
	f.lastHolderNodeID = holderNodeID
	f.lastExecution = executionID
	f.lastExpiresAt = expiresAt
	if f.err != nil {
		return pausedregistry.MarkRunningUntracked, f.err
	}
	return f.markRunning, nil
}

func (f *fakeStore) RenewSandboxDeadline(_ context.Context, _, _, executionID string, expiresAt *time.Time) (pausedregistry.DeadlineRenewalOutcome, error) {
	f.record("RenewSandboxDeadline")
	f.lastExecution = executionID
	f.lastExpiresAt = expiresAt
	if f.err != nil {
		return pausedregistry.DeadlineRenewalNotTracked, f.err
	}
	return f.deadlineRenewal, nil
}

func (f *fakeStore) ReleaseNodeHoldings(_ context.Context, _, _ string) (pausedregistry.ReleasedHoldings, error) {
	f.record("ReleaseNodeHoldings")
	if f.err != nil {
		return pausedregistry.ReleasedHoldings{}, f.err
	}
	return f.freed, nil
}

func (f *fakeStore) Remove(_ context.Context, _, _ string, gen int64) (bool, error) {
	f.record("Remove")
	f.lastGen = gen
	return f.removed, f.err
}

func (f *fakeStore) ReclaimExpiredHoldings(_ context.Context, _ string) (pausedregistry.ReleasedHoldings, error) {
	f.record("ReclaimExpiredHoldings")
	f.mu.Lock()
	f.reclaims++
	f.mu.Unlock()
	if f.err != nil {
		return pausedregistry.ReleasedHoldings{}, f.err
	}
	return f.freed, nil
}

func (f *fakeStore) Migrate(context.Context) error { f.record("Migrate"); return f.err }

func (f *fakeStore) WithLeaseTTL(ttl time.Duration) pausedregistry.Store {
	f.mu.Lock()
	f.leaseTTL = ttl
	f.mu.Unlock()
	return f
}

func (f *fakeStore) Close() {}

func (f *fakeStore) reclaimCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.reclaims
}

// openExtender opens a Grace without a database.
type openExtender struct{}

func (openExtender) ExtendLeases(context.Context, string, time.Duration) (time.Duration, int64, error) {
	return time.Minute, 0, nil
}

// newTestRegistryService returns a service whose gate is fully open.
func newTestRegistryService(t *testing.T, store *fakeStore) *PausedRegistryService {
	t.Helper()
	// A nil gate is an open one, which is what a store built without the
	// controller's start-up sequence gets.
	return NewPausedRegistryService(zap.NewNop(), store, nil, rsCluster, 90*time.Second, rsFloor)
}

// newGatedRegistryService returns a service still inside its grace window.
func newGatedRegistryService(t *testing.T, store *fakeStore, window time.Duration) (*PausedRegistryService, *pausedregistry.Grace) {
	t.Helper()

	grace := pausedregistry.NewGrace(window, zap.NewNop())
	if _, err := grace.Enter(context.Background(), openExtender{}, rsCluster); err != nil {
		t.Fatalf("open the gate: %v", err)
	}
	return NewPausedRegistryService(zap.NewNop(), store, grace, rsCluster, 90*time.Second, rsFloor), grace
}

func codeOf(err error) codes.Code { return status.Code(err) }

// ─────────────────────────────────────────────────────────────────────────────
// Guard §3.1: a failure never looks like a fact about the table
// ─────────────────────────────────────────────────────────────────────────────

// TestNothingIsAnsweredBeforeTheGateOpens is the guard at its narrowest point.
//
// 🔴 The dangerous answer is not the wrong error, it is a successful response
// with nothing in it. GetSandboxes answering an empty list means "none of those
// sandboxes have rows", and the node deletes their local artifacts and tears
// down their VMs on the strength of it.
func TestNothingIsAnsweredBeforeTheGateOpens(t *testing.T) {
	store := &fakeStore{entries: map[string]pausedregistry.Entry{}}
	// A gate that has never run its restart pass.
	svc := NewPausedRegistryService(zap.NewNop(), store, pausedregistry.NewGrace(time.Minute, zap.NewNop()), rsCluster, 90*time.Second, rsFloor)
	ctx := context.Background()

	resp, err := svc.GetSandboxes(ctx, &schedulerv1.GetSandboxesRequest{ClusterId: rsCluster, SandboxIds: []string{rsSandbox}})
	if err == nil {
		t.Fatalf("a cold registry answered a read: %+v", resp)
	}
	if resp != nil {
		t.Fatalf("a refused read must carry no response at all, got %+v", resp)
	}
	if codeOf(err) != codes.Unavailable {
		t.Fatalf("expected Unavailable, got %s", codeOf(err))
	}

	if _, err := svc.TransitionSandbox(ctx, &schedulerv1.TransitionSandboxRequest{
		ClusterId: rsCluster, SandboxId: rsSandbox, NodeId: rsNode,
		Kind: schedulerv1.TransitionKind_TRANSITION_KIND_MARK_RUNNING,
	}); codeOf(err) != codes.Unavailable {
		t.Fatalf("expected Unavailable, got %s (%v)", codeOf(err), err)
	}
	if _, err := svc.AcquireSandbox(ctx, &schedulerv1.AcquireSandboxRequest{
		ClusterId: rsCluster, SandboxId: rsSandbox, NodeId: rsNode,
	}); codeOf(err) != codes.Unavailable {
		t.Fatalf("expected Unavailable, got %s (%v)", codeOf(err), err)
	}
	if _, err := svc.RenewNodeLease(ctx, &schedulerv1.RenewNodeLeaseRequest{ClusterId: rsCluster, NodeId: rsNode}); codeOf(err) != codes.Unavailable {
		t.Fatalf("expected Unavailable, got %s (%v)", codeOf(err), err)
	}
	if _, err := svc.ReleaseNodeHoldings(ctx, &schedulerv1.ReleaseNodeHoldingsRequest{ClusterId: rsCluster, NodeId: rsNode}); codeOf(err) != codes.Unavailable {
		t.Fatalf("expected Unavailable, got %s (%v)", codeOf(err), err)
	}

	if len(store.calls) != 0 {
		t.Fatalf("a cold registry reached the store: %v", store.calls)
	}
}

// TestAFailedReadIsNeverAnEmptyList is the same property for the store's own
// failures.
func TestAFailedReadIsNeverAnEmptyList(t *testing.T) {
	store := &fakeStore{err: errors.New("connection refused"), entries: map[string]pausedregistry.Entry{}}
	svc := newTestRegistryService(t, store)

	resp, err := svc.GetSandboxes(context.Background(), &schedulerv1.GetSandboxesRequest{
		ClusterId: rsCluster, SandboxIds: []string{rsSandbox},
	})
	if err == nil {
		t.Fatalf("a backend failure was answered successfully: %+v", resp)
	}
	if resp != nil {
		t.Fatalf("a failed read must carry no response, got %+v", resp)
	}
	if codeOf(err) != codes.Unavailable {
		t.Fatalf("an unclassified backend failure must be Unavailable, got %s", codeOf(err))
	}
}

// TestStoreErrorsKeepTheirDistinctions.
//
// Aborted and FailedPrecondition are deliberately different. A generation
// conflict says somebody wrote first and the caller's view is stale, which its
// own code answers by re-reading; an invalid record says a row this build
// cannot make sense of, which nothing on the node can repair. Collapsing them
// sends a node into a re-read loop over a row that will never change.
func TestStoreErrorsKeepTheirDistinctions(t *testing.T) {
	cases := map[string]struct {
		err  error
		want codes.Code
	}{
		"generation conflict": {pausedregistry.ErrGenerationConflict, codes.Aborted},
		"invalid record":      {pausedregistry.ErrInvalidRecord, codes.FailedPrecondition},
		"invalid argument":    {pausedregistry.ErrInvalidArgument, codes.InvalidArgument},
		"not ready":           {pausedregistry.ErrNotReady, codes.Unavailable},
		"grace period":        {pausedregistry.ErrGracePeriod, codes.Unavailable},
		"deadline":            {context.DeadlineExceeded, codes.DeadlineExceeded},
		"cancelled":           {context.Canceled, codes.Canceled},
		"anything else":       {errors.New("boom"), codes.Unavailable},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			store := &fakeStore{err: tc.err}
			svc := newTestRegistryService(t, store)

			gen := int64(4)
			_, err := svc.TransitionSandbox(context.Background(), &schedulerv1.TransitionSandboxRequest{
				ClusterId: rsCluster, SandboxId: rsSandbox, NodeId: rsNode,
				Kind:             schedulerv1.TransitionKind_TRANSITION_KIND_COMPLETE_PAUSE,
				ExpectGeneration: &gen, SnapshotId: rsSnap,
			})
			if codeOf(err) != tc.want {
				t.Fatalf("got %s, want %s (%v)", codeOf(err), tc.want, err)
			}
		})
	}
}

// TestRequestsForAnotherClusterAreRefused.
//
// The restart grace pass extends one cluster's leases. Serving a second
// cluster's writes would serve them with none of theirs extended, which is the
// state the whole gate exists to prevent — and reclamation would delete rows
// this controller was never given.
func TestRequestsForAnotherClusterAreRefused(t *testing.T) {
	store := &fakeStore{entries: map[string]pausedregistry.Entry{}}
	svc := newTestRegistryService(t, store)

	_, err := svc.GetSandboxes(context.Background(), &schedulerv1.GetSandboxesRequest{ClusterId: rsOther})
	if codeOf(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %s (%v)", codeOf(err), err)
	}
	if _, err := svc.GetSandboxes(context.Background(), &schedulerv1.GetSandboxesRequest{}); codeOf(err) != codes.InvalidArgument {
		t.Fatalf("a request with no cluster scope must be refused, got %s", codeOf(err))
	}
	if len(store.calls) != 0 {
		t.Fatalf("another cluster's request reached the store: %v", store.calls)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// GetSandboxes
// ─────────────────────────────────────────────────────────────────────────────

func testEntry(state pausedregistry.State) pausedregistry.Entry {
	entry := pausedregistry.Entry{Metadata: json.RawMessage(`{"id":"s","big":9007199254740993}`)}
	entry.SandboxID = rsSandbox
	entry.ClusterID = rsCluster
	entry.State = state
	entry.Generation = 7
	entry.OriginNodeID = rsNode
	entry.ClaimedByNodeID = "node-b"
	entry.SnapshotID = rsSnap
	entry.PausedAt = time.Unix(1_700_000_000, 123_456_000).UTC()
	entry.UpdatedAt = time.Unix(1_700_000_500, 654_321_000).UTC()
	return entry
}

// TestABulkReadCarriesNoMetadata: no bulk consumer reads it — the node's two
// batch callers look only at the state, the two node ids and the generation —
// and it is the largest column in the table.
func TestABulkReadCarriesNoMetadata(t *testing.T) {
	store := &fakeStore{entries: map[string]pausedregistry.Entry{rsSandbox: testEntry(pausedregistry.StatePaused)}}
	svc := newTestRegistryService(t, store)

	resp, err := svc.GetSandboxes(context.Background(), &schedulerv1.GetSandboxesRequest{
		ClusterId: rsCluster, SandboxIds: []string{rsSandbox},
	})
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if len(resp.GetSandboxes()) != 1 {
		t.Fatalf("expected one row, got %d", len(resp.GetSandboxes()))
	}
	got := resp.GetSandboxes()[0]
	if got.GetState() != "paused" || got.GetGeneration() != 7 || got.GetOriginNodeId() != rsNode {
		t.Fatalf("unexpected entry: %+v", got)
	}
	if got.GetClaimedByNodeId() != "node-b" || got.GetSnapshotId() != rsSnap {
		t.Fatalf("unexpected entry: %+v", got)
	}
}

// TestTimesCrossTheWireAsMicroseconds.
//
// PostgreSQL stores timestamptz to the microsecond and the node's clock type is
// nanoseconds, so nanoseconds would round-trip a write as a silently truncated
// read; seconds would make two transitions within the same second unorderable.
func TestTimesCrossTheWireAsMicroseconds(t *testing.T) {
	entry := testEntry(pausedregistry.StatePaused)
	store := &fakeStore{entries: map[string]pausedregistry.Entry{rsSandbox: entry}}
	svc := newTestRegistryService(t, store)

	resp, err := svc.GetSandboxes(context.Background(), &schedulerv1.GetSandboxesRequest{
		ClusterId: rsCluster, SandboxIds: []string{rsSandbox},
	})
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	got := resp.GetSandboxes()[0]
	if got.GetPausedAtUnixMicros() != entry.PausedAt.UnixMicro() {
		t.Fatalf("paused_at: got %d, want %d", got.GetPausedAtUnixMicros(), entry.PausedAt.UnixMicro())
	}
	if got.GetUpdatedAtUnixMicros() != entry.UpdatedAt.UnixMicro() {
		t.Fatalf("updated_at: got %d, want %d", got.GetUpdatedAtUnixMicros(), entry.UpdatedAt.UnixMicro())
	}
	// Round-tripping through the wire value must land on the same instant to
	// the microsecond, which is the resolution both sides can hold.
	if back := time.UnixMicro(got.GetUpdatedAtUnixMicros()).UTC(); !back.Equal(entry.UpdatedAt.Truncate(time.Microsecond)) {
		t.Fatalf("updated_at did not survive the round trip: %s -> %s", entry.UpdatedAt, back)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// TransitionSandbox
// ─────────────────────────────────────────────────────────────────────────────

// TestATransitionRefusesFieldsItsKindNeverWrites.
//
// 🔴 Refused, not dropped. A caller that quotes a generation believes the write
// is fenced; one that sends metadata believes it was stored. Neither is true
// for these kinds, and both mistakes are invisible from the caller's side,
// because the response says the transition succeeded — and it did.
func TestATransitionRefusesFieldsItsKindNeverWrites(t *testing.T) {
	gen := int64(4)
	metadata := json.RawMessage(`{"id":"s"}`)

	cases := []struct {
		name string
		req  *schedulerv1.TransitionSandboxRequest
	}{
		{"remove with metadata", &schedulerv1.TransitionSandboxRequest{
			Kind: schedulerv1.TransitionKind_TRANSITION_KIND_REMOVE, ExpectGeneration: &gen,
			MetadataJson: metadata}},
		{"remove without a generation", &schedulerv1.TransitionSandboxRequest{
			Kind: schedulerv1.TransitionKind_TRANSITION_KIND_REMOVE}},
		// 🔴 6 is the reserved number the unconditional delete used to have. It
		// is refused outright rather than served as a compatibility case: a
		// node still asking for it is a node still deciding for itself whether
		// somebody else's sandbox may be destroyed. The constant is gone from
		// the proto, so the number is quoted directly — the assertion has to
		// outlive the enum entry, or "one branch fewer" quietly becomes "one
		// gate fewer".
		{"the unconditional remove (reserved 6)", &schedulerv1.TransitionSandboxRequest{
			Kind: schedulerv1.TransitionKind(6)}},
		{"mark_running with a generation", &schedulerv1.TransitionSandboxRequest{
			Kind: schedulerv1.TransitionKind_TRANSITION_KIND_MARK_RUNNING, ExpectGeneration: &gen}},
		{"complete_pause with metadata", &schedulerv1.TransitionSandboxRequest{
			Kind: schedulerv1.TransitionKind_TRANSITION_KIND_COMPLETE_PAUSE, ExpectGeneration: &gen,
			SnapshotId: rsSnap, MetadataJson: metadata}},
		{"begin_pause with a snapshot", &schedulerv1.TransitionSandboxRequest{
			Kind: schedulerv1.TransitionKind_TRANSITION_KIND_BEGIN_PAUSE, MetadataJson: metadata, SnapshotId: rsSnap}},
		{"begin_pause with a generation", &schedulerv1.TransitionSandboxRequest{
			Kind: schedulerv1.TransitionKind_TRANSITION_KIND_BEGIN_PAUSE, MetadataJson: metadata, ExpectGeneration: &gen}},
		{"release_claim with a snapshot", &schedulerv1.TransitionSandboxRequest{
			Kind: schedulerv1.TransitionKind_TRANSITION_KIND_RELEASE_CLAIM, ExpectGeneration: &gen, SnapshotId: rsSnap}},
		{"mark_local_only with metadata", &schedulerv1.TransitionSandboxRequest{
			Kind: schedulerv1.TransitionKind_TRANSITION_KIND_MARK_LOCAL_ONLY, ExpectGeneration: &gen, MetadataJson: metadata}},
		{"renew_deadline with a generation", &schedulerv1.TransitionSandboxRequest{
			Kind: schedulerv1.TransitionKind_TRANSITION_KIND_RENEW_DEADLINE, ExecutionId: rsExecution, ExpectGeneration: &gen}},
		{"renew_deadline with metadata", &schedulerv1.TransitionSandboxRequest{
			Kind: schedulerv1.TransitionKind_TRANSITION_KIND_RENEW_DEADLINE, ExecutionId: rsExecution, MetadataJson: metadata}},
		{"renew_deadline with a snapshot", &schedulerv1.TransitionSandboxRequest{
			Kind: schedulerv1.TransitionKind_TRANSITION_KIND_RENEW_DEADLINE, ExecutionId: rsExecution, SnapshotId: rsSnap}},
		{"renew_deadline with a holder", &schedulerv1.TransitionSandboxRequest{
			Kind: schedulerv1.TransitionKind_TRANSITION_KIND_RENEW_DEADLINE, ExecutionId: rsExecution, HolderNodeId: "aenv-master-01"}},
		{"a kind this build does not serve", &schedulerv1.TransitionSandboxRequest{
			Kind: schedulerv1.TransitionKind_TRANSITION_KIND_UNSPECIFIED}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeStore{}
			svc := newTestRegistryService(t, store)

			tc.req.ClusterId = rsCluster
			tc.req.SandboxId = rsSandbox
			tc.req.NodeId = rsNode

			_, err := svc.TransitionSandbox(context.Background(), tc.req)
			if codeOf(err) != codes.InvalidArgument {
				t.Fatalf("expected InvalidArgument, got %s (%v)", codeOf(err), err)
			}
			if len(store.calls) != 0 {
				t.Fatalf("a refused transition reached the store: %v", store.calls)
			}
		})
	}
}

// TestAConditionalTransitionMustQuoteAGeneration is the other half: a caller
// that forgot to fence a write it believes is fenced.
func TestAConditionalTransitionMustQuoteAGeneration(t *testing.T) {
	for _, kind := range []schedulerv1.TransitionKind{
		schedulerv1.TransitionKind_TRANSITION_KIND_COMPLETE_PAUSE,
		schedulerv1.TransitionKind_TRANSITION_KIND_MARK_LOCAL_ONLY,
		schedulerv1.TransitionKind_TRANSITION_KIND_RELEASE_CLAIM,
	} {
		t.Run(kind.String(), func(t *testing.T) {
			store := &fakeStore{}
			svc := newTestRegistryService(t, store)

			req := &schedulerv1.TransitionSandboxRequest{
				ClusterId: rsCluster, SandboxId: rsSandbox, NodeId: rsNode, Kind: kind,
			}
			if kind == schedulerv1.TransitionKind_TRANSITION_KIND_COMPLETE_PAUSE {
				req.SnapshotId = rsSnap
			}
			_, err := svc.TransitionSandbox(context.Background(), req)
			if codeOf(err) != codes.InvalidArgument {
				t.Fatalf("expected InvalidArgument, got %s (%v)", codeOf(err), err)
			}
			if len(store.calls) != 0 {
				t.Fatalf("an unfenced conditional write reached the store: %v", store.calls)
			}
		})
	}
}

func TestBeginPauseCarriesMetadataThroughUntouched(t *testing.T) {
	store := &fakeStore{began: pausedregistry.BeganPause{Generation: 3, PreviousSnapshotID: rsSnap}}
	svc := newTestRegistryService(t, store)

	// Deliberately awkward: a key order no marshaller would choose, and an
	// integer no float can hold.
	metadata := json.RawMessage(`{"z":1,"a":{"big":9007199254740993},"unknown":true}`)

	resp, err := svc.TransitionSandbox(context.Background(), &schedulerv1.TransitionSandboxRequest{
		ClusterId: rsCluster, SandboxId: rsSandbox, NodeId: rsNode,
		Kind: schedulerv1.TransitionKind_TRANSITION_KIND_BEGIN_PAUSE, MetadataJson: metadata,
		ExecutionId: rsExecution,
	})
	if err != nil {
		t.Fatalf("begin_pause failed: %v", err)
	}
	if resp.GetGeneration() != 3 || resp.GetPreviousSnapshotId() != rsSnap {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if string(store.lastMeta) != string(metadata) {
		t.Fatalf("metadata was rewritten on the way through:\n got %s\nwant %s", store.lastMeta, metadata)
	}
}

func TestBeginPauseWithoutMetadataIsRefused(t *testing.T) {
	store := &fakeStore{}
	svc := newTestRegistryService(t, store)

	_, err := svc.TransitionSandbox(context.Background(), &schedulerv1.TransitionSandboxRequest{
		ClusterId: rsCluster, SandboxId: rsSandbox, NodeId: rsNode,
		Kind: schedulerv1.TransitionKind_TRANSITION_KIND_BEGIN_PAUSE,
	})
	if codeOf(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %s (%v)", codeOf(err), err)
	}
	if store.called("BeginPause") {
		t.Fatal("a pause with no metadata reached the store; the column is NOT NULL and the resume reads it")
	}
}

// TestAnUntrackedSandboxIsASuccessfulAnswer.
//
// 🔴 Untracked says the cluster does not track this sandbox — which is what a
// sandbox that has never been paused looks like, and by far the common case.
// Reporting it as an error would make a node treat its own healthy sandboxes as
// an outage.
func TestAnUntrackedSandboxIsASuccessfulAnswer(t *testing.T) {
	store := &fakeStore{markRunning: pausedregistry.MarkRunningUntracked}
	svc := newTestRegistryService(t, store)

	markRunning := func(t *testing.T) *schedulerv1.TransitionSandboxResponse {
		t.Helper()
		resp, err := svc.TransitionSandbox(context.Background(), &schedulerv1.TransitionSandboxRequest{
			ClusterId: rsCluster, SandboxId: rsSandbox, NodeId: rsNode,
			Kind: schedulerv1.TransitionKind_TRANSITION_KIND_MARK_RUNNING, ExecutionId: rsExecution,
		})
		if err != nil {
			t.Fatalf("mark_running was reported as a failure: %v", err)
		}
		return resp
	}

	resp := markRunning(t)
	if resp.GetTracked() {
		t.Fatal("the response says the cluster tracks a sandbox it does not")
	}
	if got := resp.GetMarkRunningOutcome(); got != schedulerv1.MarkRunningOutcome_MARK_RUNNING_OUTCOME_UNTRACKED {
		t.Fatalf("an untracked sandbox must say so on the wire: got %v", got)
	}

	store.markRunning = pausedregistry.MarkRunningAdopted
	resp = markRunning(t)
	if !resp.GetTracked() {
		t.Fatalf("a tracked sandbox was not reported as tracked: %+v", resp)
	}
	if got := resp.GetMarkRunningOutcome(); got != schedulerv1.MarkRunningOutcome_MARK_RUNNING_OUTCOME_ADOPTED {
		t.Fatalf("an adopted sandbox must say so on the wire: got %v", got)
	}

	// 🔴 The case the bool could not carry. Both of these are `tracked: false`,
	// and one of them means two nodes are bringing the same sandbox up.
	store.markRunning = pausedregistry.MarkRunningHeldElsewhere
	resp = markRunning(t)
	if resp.GetTracked() {
		t.Fatal("a sandbox held by another node must not be reported as adopted")
	}
	if got := resp.GetMarkRunningOutcome(); got != schedulerv1.MarkRunningOutcome_MARK_RUNNING_OUTCOME_HELD_ELSEWHERE {
		t.Fatalf("held-elsewhere must be distinguishable from untracked on the wire: got %v", got)
	}
}

// TestMarkRunningForwardsBothIdentitiesSeparately proves the wire plumbing
// this fix depends on: node_id and holder_node_id must reach the store as two
// distinct arguments, in the order MarkRunning documents them (node_id, then
// holder_node_id) — collapsing them back onto one is exactly a0487f0's
// mistake, moved one layer up.
func TestMarkRunningForwardsBothIdentitiesSeparately(t *testing.T) {
	store := &fakeStore{markRunning: pausedregistry.MarkRunningAdopted}
	svc := newTestRegistryService(t, store)

	const holder = "aenv-master-01"
	if _, err := svc.TransitionSandbox(context.Background(), &schedulerv1.TransitionSandboxRequest{
		ClusterId: rsCluster, SandboxId: rsSandbox, NodeId: rsNode,
		Kind: schedulerv1.TransitionKind_TRANSITION_KIND_MARK_RUNNING, ExecutionId: rsExecution,
		HolderNodeId: holder,
	}); err != nil {
		t.Fatalf("mark_running was reported as a failure: %v", err)
	}

	if store.lastNodeID != rsNode {
		t.Fatalf("node_id: got %q, want %q — the guard identity must reach the store unchanged", store.lastNodeID, rsNode)
	}
	if store.lastHolderNodeID != holder {
		t.Fatalf("holder_node_id: got %q, want %q — quoting node_id here is the exact regression this test exists to catch", store.lastHolderNodeID, holder)
	}
	if store.lastHolderNodeID == store.lastNodeID {
		t.Fatalf("node_id and holder_node_id must not have collapsed onto the same value: both were %q", store.lastNodeID)
	}
}

// TestHolderNodeIDIsRefusedOnEveryOtherKind mirrors the execution_id and
// generation checks: a field only mark_running writes must be refused, not
// silently dropped, everywhere else — a caller that sent it believes it was
// recorded.
func TestHolderNodeIDIsRefusedOnEveryOtherKind(t *testing.T) {
	store := &fakeStore{}
	svc := newTestRegistryService(t, store)

	kinds := []schedulerv1.TransitionKind{
		schedulerv1.TransitionKind_TRANSITION_KIND_BEGIN_PAUSE,
		schedulerv1.TransitionKind_TRANSITION_KIND_COMPLETE_PAUSE,
		schedulerv1.TransitionKind_TRANSITION_KIND_MARK_LOCAL_ONLY,
		schedulerv1.TransitionKind_TRANSITION_KIND_RELEASE_CLAIM,
		schedulerv1.TransitionKind_TRANSITION_KIND_REMOVE,
		schedulerv1.TransitionKind_TRANSITION_KIND_RENEW_DEADLINE,
	}
	for _, kind := range kinds {
		_, err := svc.TransitionSandbox(context.Background(), &schedulerv1.TransitionSandboxRequest{
			ClusterId: rsCluster, SandboxId: rsSandbox, NodeId: rsNode,
			Kind: kind, HolderNodeId: "aenv-master-01",
		})
		if codeOf(err) != codes.InvalidArgument {
			t.Fatalf("%v: expected InvalidArgument for a holder_node_id it does not record, got %s (%v)", kind, codeOf(err), err)
		}
	}
}

// TestRenewDeadlineForwardsTheDeadlineItWasGiven is the wire-translation half
// of deadline propagation: whatever the caller put on the request is what the
// store is asked to write, not merely "something non-empty". A test that only
// checked "the column is non-null" cannot tell a clamped, correct deadline
// from the caller's raw, unclamped one — see keep_alive_for's own clamp — so
// this pins the exact value, in both directions (a real deadline, and an
// explicit absence of one).
func TestRenewDeadlineForwardsTheDeadlineItWasGiven(t *testing.T) {
	deadline := time.Unix(1_700_000_000, 654_000_000).UTC()
	micros := deadline.UnixMicro()

	t.Run("a deadline", func(t *testing.T) {
		store := &fakeStore{deadlineRenewal: pausedregistry.DeadlineRenewalRenewed}
		svc := newTestRegistryService(t, store)
		if _, err := svc.TransitionSandbox(context.Background(), &schedulerv1.TransitionSandboxRequest{
			ClusterId: rsCluster, SandboxId: rsSandbox, NodeId: rsNode,
			Kind: schedulerv1.TransitionKind_TRANSITION_KIND_RENEW_DEADLINE, ExecutionId: rsExecution,
			SandboxExpiresAtUnixMicros: &micros,
		}); err != nil {
			t.Fatalf("renew_deadline failed: %v", err)
		}
		if store.lastExpiresAt == nil || !store.lastExpiresAt.Equal(deadline) {
			t.Fatalf("deadline: got %v, want %s", store.lastExpiresAt, deadline)
		}
	})

	// Absent is not zero: absent means the sandbox was asked never to expire,
	// and reclamation must leave it alone forever — zero is a deadline in
	// 1970, which reclamation acts on immediately.
	t.Run("no deadline", func(t *testing.T) {
		store := &fakeStore{deadlineRenewal: pausedregistry.DeadlineRenewalRenewed}
		svc := newTestRegistryService(t, store)
		if _, err := svc.TransitionSandbox(context.Background(), &schedulerv1.TransitionSandboxRequest{
			ClusterId: rsCluster, SandboxId: rsSandbox, NodeId: rsNode,
			Kind: schedulerv1.TransitionKind_TRANSITION_KIND_RENEW_DEADLINE, ExecutionId: rsExecution,
		}); err != nil {
			t.Fatalf("renew_deadline failed: %v", err)
		}
		if store.lastExpiresAt != nil {
			t.Fatalf("an absent deadline became %v; that sandbox is now reclaimable", store.lastExpiresAt)
		}
	})
}

// TestRenewDeadlineOutcomesReachTheWire is DeadlineRenewalOutcome's own
// version of TestAllFourClaimOutcomesReachTheWire: none of the three answers
// is an error, and a caller reading the wrong one off the wire would either
// trust a deadline that was never written (superseded/not_tracked read as
// renewed) or warn about a healthy, ordinary case (renewed or not_tracked
// read as superseded).
func TestRenewDeadlineOutcomesReachTheWire(t *testing.T) {
	cases := []struct {
		outcome pausedregistry.DeadlineRenewalOutcome
		want    schedulerv1.DeadlineRenewalOutcome
	}{
		{pausedregistry.DeadlineRenewalRenewed, schedulerv1.DeadlineRenewalOutcome_DEADLINE_RENEWAL_OUTCOME_RENEWED},
		{pausedregistry.DeadlineRenewalNotTracked, schedulerv1.DeadlineRenewalOutcome_DEADLINE_RENEWAL_OUTCOME_NOT_TRACKED},
		{pausedregistry.DeadlineRenewalSuperseded, schedulerv1.DeadlineRenewalOutcome_DEADLINE_RENEWAL_OUTCOME_SUPERSEDED},
	}

	for _, tc := range cases {
		t.Run(string(tc.outcome), func(t *testing.T) {
			store := &fakeStore{deadlineRenewal: tc.outcome}
			svc := newTestRegistryService(t, store)

			resp, err := svc.TransitionSandbox(context.Background(), &schedulerv1.TransitionSandboxRequest{
				ClusterId: rsCluster, SandboxId: rsSandbox, NodeId: rsNode,
				Kind: schedulerv1.TransitionKind_TRANSITION_KIND_RENEW_DEADLINE, ExecutionId: rsExecution,
			})
			// None of the three is an error: the api half's own record of the
			// timeout extension already succeeded by the time this call is
			// made, and this write is only a best-effort mirror.
			if err != nil {
				t.Fatalf("renew_deadline(%s) was reported as a failure: %v", tc.outcome, err)
			}
			if got := resp.GetDeadlineRenewalOutcome(); got != tc.want {
				t.Fatalf("outcome: got %s, want %s", got, tc.want)
			}
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// AcquireSandbox
// ─────────────────────────────────────────────────────────────────────────────

func TestAllFourClaimOutcomesReachTheWire(t *testing.T) {
	entry := testEntry(pausedregistry.StateResuming)

	cases := []struct {
		name  string
		claim pausedregistry.ResumeClaim
		check func(*testing.T, *schedulerv1.AcquireSandboxResponse)
	}{
		{
			name: "claimed",
			claim: pausedregistry.ResumeClaim{
				Outcome: pausedregistry.ClaimOutcomeClaimed, Entry: &entry,
				PreviousState: pausedregistry.StatePaused,
			},
			check: func(t *testing.T, resp *schedulerv1.AcquireSandboxResponse) {
				claimed := resp.GetClaimed()
				if claimed == nil {
					t.Fatalf("expected a claim, got %+v", resp.GetOutcome())
				}
				if claimed.GetPreviousState() != "paused" {
					t.Fatalf("previous_state: got %q", claimed.GetPreviousState())
				}
				if string(claimed.GetMetadataJson()) != string(entry.Metadata) {
					t.Fatalf("metadata was rewritten: %s", claimed.GetMetadataJson())
				}
				if claimed.GetEntry().GetState() != "resuming" {
					t.Fatalf("entry: %+v", claimed.GetEntry())
				}
			},
		},
		{
			name:  "not found",
			claim: pausedregistry.ResumeClaim{Outcome: pausedregistry.ClaimOutcomeNotFound},
			check: func(t *testing.T, resp *schedulerv1.AcquireSandboxResponse) {
				if resp.GetNotFound() == nil {
					t.Fatalf("expected not_found, got %+v", resp.GetOutcome())
				}
			},
		},
		{
			name:  "not ready",
			claim: pausedregistry.ResumeClaim{Outcome: pausedregistry.ClaimOutcomeNotReady, OriginNodeID: rsNode},
			check: func(t *testing.T, resp *schedulerv1.AcquireSandboxResponse) {
				if resp.GetNotReady() == nil || resp.GetNotReady().GetOriginNodeId() != rsNode {
					t.Fatalf("expected not_ready naming the origin, got %+v", resp.GetOutcome())
				}
			},
		},
		{
			name:  "conflict",
			claim: pausedregistry.ResumeClaim{Outcome: pausedregistry.ClaimOutcomeConflict, OriginNodeID: "node-b"},
			check: func(t *testing.T, resp *schedulerv1.AcquireSandboxResponse) {
				if resp.GetConflict() == nil || resp.GetConflict().GetOriginNodeId() != "node-b" {
					t.Fatalf("expected conflict naming the holder, got %+v", resp.GetOutcome())
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeStore{claim: tc.claim}
			svc := newTestRegistryService(t, store)

			resp, err := svc.AcquireSandbox(context.Background(), &schedulerv1.AcquireSandboxRequest{
				ClusterId: rsCluster, SandboxId: rsSandbox, NodeId: rsNode, ExecutionId: rsExecution,
			})
			if err != nil {
				t.Fatalf("acquire failed: %v", err)
			}
			tc.check(t, resp)
		})
	}
}

// TestAClaimWithNoEntryIsAFailure: a claimed message with no entry would be
// read as "granted" by anything that checks the oneof and not its contents.
func TestAClaimWithNoEntryIsAFailure(t *testing.T) {
	store := &fakeStore{claim: pausedregistry.ResumeClaim{Outcome: pausedregistry.ClaimOutcomeClaimed}}
	svc := newTestRegistryService(t, store)

	resp, err := svc.AcquireSandbox(context.Background(), &schedulerv1.AcquireSandboxRequest{
		ClusterId: rsCluster, SandboxId: rsSandbox, NodeId: rsNode, ExecutionId: rsExecution,
	})
	if err == nil {
		t.Fatalf("a claim with no entry was granted: %+v", resp)
	}
	if codeOf(err) != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition, got %s", codeOf(err))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Leases
// ─────────────────────────────────────────────────────────────────────────────

// TestAnAbsentDeadlineStaysAbsent.
//
// Absent means the sandbox was asked never to expire and reclamation leaves it
// alone forever. Zero would be a deadline in 1970, which reclamation acts on.
func TestAnAbsentDeadlineStaysAbsent(t *testing.T) {
	store := &fakeStore{renewed: 2}
	svc := newTestRegistryService(t, store)

	deadline := time.Unix(1_700_000_000, 123_456_000).UTC()
	micros := deadline.UnixMicro()

	resp, err := svc.RenewNodeLease(context.Background(), &schedulerv1.RenewNodeLeaseRequest{
		ClusterId: rsCluster, NodeId: rsNode,
		Held: []*schedulerv1.HeldSandbox{
			{SandboxId: rsSandbox},
			{SandboxId: rsSnap, ExpiresAtUnixMicros: &micros},
		},
	})
	if err != nil {
		t.Fatalf("renew failed: %v", err)
	}
	if resp.GetRenewed() != 2 {
		t.Fatalf("renewed: got %d", resp.GetRenewed())
	}
	if len(store.lastHeld) != 2 {
		t.Fatalf("held: got %d", len(store.lastHeld))
	}
	if store.lastHeld[0].ExpiresAt != nil {
		t.Fatalf("an absent deadline became %v; that sandbox is now reclaimable", store.lastHeld[0].ExpiresAt)
	}
	if store.lastHeld[1].ExpiresAt == nil || !store.lastHeld[1].ExpiresAt.Equal(deadline) {
		t.Fatalf("deadline: got %v, want %s", store.lastHeld[1].ExpiresAt, deadline)
	}
}

// TestTheCallersLeaseLengthReachesTheStore covers the TTL travelling with every
// transition that stamps a lease.
func TestTheCallersLeaseLengthReachesTheStore(t *testing.T) {
	store := &fakeStore{
		began: pausedregistry.BeganPause{Generation: 1},
		claim: pausedregistry.ResumeClaim{Outcome: pausedregistry.ClaimOutcomeNotFound},
	}
	svc := newTestRegistryService(t, store)

	if _, err := svc.TransitionSandbox(context.Background(), &schedulerv1.TransitionSandboxRequest{
		ClusterId: rsCluster, SandboxId: rsSandbox, NodeId: rsNode,
		Kind:         schedulerv1.TransitionKind_TRANSITION_KIND_BEGIN_PAUSE,
		MetadataJson: json.RawMessage(`{"id":"s"}`), LeaseTtlMillis: 120_000, ExecutionId: rsExecution,
	}); err != nil {
		t.Fatalf("begin_pause failed: %v", err)
	}
	if store.leaseTTL != 2*time.Minute {
		t.Fatalf("the node's lease length did not reach the store: %s", store.leaseTTL)
	}

	store.leaseTTL = 0
	if _, err := svc.AcquireSandbox(context.Background(), &schedulerv1.AcquireSandboxRequest{
		ClusterId: rsCluster, SandboxId: rsSandbox, NodeId: rsNode, LeaseTtlMillis: 45_000, ExecutionId: rsExecution,
	}); err != nil {
		t.Fatalf("acquire failed: %v", err)
	}
	if store.leaseTTL != 45*time.Second {
		t.Fatalf("the node's lease length did not reach the claim: %s", store.leaseTTL)
	}

	// And a request that reports none leaves the store's own default alone.
	store.leaseTTL = 0
	if _, err := svc.RenewNodeLease(context.Background(), &schedulerv1.RenewNodeLeaseRequest{
		ClusterId: rsCluster, NodeId: rsNode,
	}); err != nil {
		t.Fatalf("renew failed: %v", err)
	}
	if store.leaseTTL != 0 {
		t.Fatalf("a request reporting no lease length overrode the store's: %s", store.leaseTTL)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// The grace window, from the service's side
// ─────────────────────────────────────────────────────────────────────────────

// TestTheGraceWindowStillServesANodeThatJustRestarted.
//
// 🔴 ReleaseNodeHoldings is served through the window, unlike reclamation. Its
// evidence is not a clock: a row saying "running on this node", read on behalf
// of a process that has just started and holds nothing, can only have been
// written by a previous process on that same machine. Nothing about this
// controller having been away weakens that, and withholding it would strand a
// restarting node's sandboxes for a full lease.
func TestTheGraceWindowStillServesANodeThatJustRestarted(t *testing.T) {
	store := &fakeStore{freed: pausedregistry.ReleasedHoldings{Released: 2, Discarded: 1}, entries: map[string]pausedregistry.Entry{}}
	svc, _ := newGatedRegistryService(t, store, time.Hour)

	resp, err := svc.ReleaseNodeHoldings(context.Background(), &schedulerv1.ReleaseNodeHoldingsRequest{
		ClusterId: rsCluster, NodeId: rsNode,
	})
	if err != nil {
		t.Fatalf("a restarting node was refused during the grace window: %v", err)
	}
	if resp.GetReleased() != 2 || resp.GetDiscarded() != 1 {
		t.Fatalf("unexpected counts: %+v", resp)
	}

	// Reads and ordinary writes are served too.
	if _, err := svc.GetSandboxes(context.Background(), &schedulerv1.GetSandboxesRequest{ClusterId: rsCluster}); err != nil {
		t.Fatalf("a read was refused during the grace window: %v", err)
	}
	if _, err := svc.RenewNodeLease(context.Background(), &schedulerv1.RenewNodeLeaseRequest{ClusterId: rsCluster, NodeId: rsNode}); err != nil {
		t.Fatalf("a renewal was refused during the grace window: %v", err)
	}
}

// TestReclamationIsHeldBackUntilTheWindowCloses.
//
// Nothing about that pass is urgent — every row it collects has been stranded
// for at least a sandbox lifetime — and the leases it reads are ones this
// process is the reason nobody renewed.
func TestReclamationIsHeldBackUntilTheWindowCloses(t *testing.T) {
	store := &fakeStore{}
	svc, _ := newGatedRegistryService(t, store, 150*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go svc.RunReclaim(ctx, 20*time.Millisecond)

	time.Sleep(100 * time.Millisecond)
	if n := store.reclaimCount(); n != 0 {
		t.Fatalf("reclamation ran %d times inside the grace window", n)
	}

	time.Sleep(150 * time.Millisecond)
	if store.reclaimCount() == 0 {
		t.Fatal("reclamation never ran after the grace window closed")
	}
}

// TestReclamationSurvivesATrippedBreaker: the pass is refused, not the timer.
func TestReclamationSurvivesATrippedBreaker(t *testing.T) {
	store := &fakeStore{err: pausedregistry.ErrDiscardBreakerTripped}
	svc := newTestRegistryService(t, store)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go svc.RunReclaim(ctx, 20*time.Millisecond)

	time.Sleep(120 * time.Millisecond)
	if store.reclaimCount() < 2 {
		t.Fatalf("the timer stopped after a refused pass: %d runs", store.reclaimCount())
	}
}

// TestALeaseTooShortToBeMeantIsRaisedNotRefused.
//
// 🔴 The floor exists because the invariant that keeps a lease longer than the
// cadence renewing it — three reconcile intervals, so two missed renewals are
// survivable — is checked in the *node's* configuration, which is the only
// place that knows both numbers. Once the TTL travels per-call, nothing here
// can re-check it.
//
// 🔴 Raised, never refused, and the asymmetry is the point. A longer lease is
// harder to take over, which is the fail-safe direction; refusing would fail
// the pause or resume the request belongs to over a number that was only ever
// advisory.
func TestALeaseTooShortToBeMeantIsRaisedNotRefused(t *testing.T) {
	const floor = 30 * time.Second

	cases := map[string]struct {
		reported int64
		want     time.Duration
	}{
		// The bugs this is for: an unset field, and milliseconds confused with
		// seconds.
		"one millisecond":      {reported: 1, want: floor},
		"ninety milliseconds":  {reported: 90, want: floor},
		"just under the floor": {reported: floor.Milliseconds() - 1, want: floor},
		"exactly the floor":    {reported: floor.Milliseconds(), want: floor},
		"a real lease":         {reported: 90_000, want: 90 * time.Second},
		"a generous lease":     {reported: 3_600_000, want: time.Hour},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			store := &fakeStore{began: pausedregistry.BeganPause{Generation: 1}}
			svc := NewPausedRegistryService(zap.NewNop(), store, nil, rsCluster, 90*time.Second, floor)

			if _, err := svc.TransitionSandbox(context.Background(), &schedulerv1.TransitionSandboxRequest{
				ClusterId: rsCluster, SandboxId: rsSandbox, NodeId: rsNode,
				Kind:           schedulerv1.TransitionKind_TRANSITION_KIND_BEGIN_PAUSE,
				MetadataJson:   json.RawMessage(`{"id":"s"}`),
				LeaseTtlMillis: tc.reported,
				ExecutionId:    rsExecution,
			}); err != nil {
				t.Fatalf("a reported lease of %dms was refused: %v", tc.reported, err)
			}
			if store.leaseTTL != tc.want {
				t.Fatalf("stamped %s, want %s", store.leaseTTL, tc.want)
			}
		})
	}
}

// TestAnUnreportedLeaseIsNotClamped: reporting nothing means "use yours", and
// the store's own default is a controller setting that has already been
// validated. Clamping it would be this side second-guessing itself.
func TestAnUnreportedLeaseIsNotClamped(t *testing.T) {
	store := &fakeStore{renewed: 1}
	svc := NewPausedRegistryService(zap.NewNop(), store, nil, rsCluster, 90*time.Second, time.Hour)

	if _, err := svc.RenewNodeLease(context.Background(), &schedulerv1.RenewNodeLeaseRequest{
		ClusterId: rsCluster, NodeId: rsNode,
	}); err != nil {
		t.Fatalf("renew failed: %v", err)
	}
	if store.leaseTTL != 0 {
		t.Fatalf("an unreported lease was clamped to %s", store.leaseTTL)
	}
}

// TestTheFloorAppliesToEveryPathThatStampsALease. A floor that covers only the
// path somebody remembered is not a floor.
func TestTheFloorAppliesToEveryPathThatStampsALease(t *testing.T) {
	const floor = 30 * time.Second
	gen := int64(4)

	transitions := []*schedulerv1.TransitionSandboxRequest{
		{Kind: schedulerv1.TransitionKind_TRANSITION_KIND_BEGIN_PAUSE, MetadataJson: json.RawMessage(`{"id":"s"}`), ExecutionId: rsExecution},
		{Kind: schedulerv1.TransitionKind_TRANSITION_KIND_COMPLETE_PAUSE, ExpectGeneration: &gen, SnapshotId: rsSnap},
		{Kind: schedulerv1.TransitionKind_TRANSITION_KIND_MARK_LOCAL_ONLY, ExpectGeneration: &gen},
		{Kind: schedulerv1.TransitionKind_TRANSITION_KIND_RELEASE_CLAIM, ExpectGeneration: &gen},
		{Kind: schedulerv1.TransitionKind_TRANSITION_KIND_MARK_RUNNING, ExecutionId: rsExecution},
	}
	for _, req := range transitions {
		t.Run(req.GetKind().String(), func(t *testing.T) {
			store := &fakeStore{began: pausedregistry.BeganPause{Generation: 1}}
			svc := NewPausedRegistryService(zap.NewNop(), store, nil, rsCluster, 90*time.Second, floor)

			req.ClusterId, req.SandboxId, req.NodeId = rsCluster, rsSandbox, rsNode
			req.LeaseTtlMillis = 1
			if _, err := svc.TransitionSandbox(context.Background(), req); err != nil {
				t.Fatalf("transition failed: %v", err)
			}
			if store.leaseTTL != floor {
				t.Fatalf("%v stamped %s, want the floor %s", req.GetKind(), store.leaseTTL, floor)
			}
		})
	}

	t.Run("AcquireSandbox", func(t *testing.T) {
		store := &fakeStore{claim: pausedregistry.ResumeClaim{Outcome: pausedregistry.ClaimOutcomeNotFound}}
		svc := NewPausedRegistryService(zap.NewNop(), store, nil, rsCluster, 90*time.Second, floor)

		if _, err := svc.AcquireSandbox(context.Background(), &schedulerv1.AcquireSandboxRequest{
			ClusterId: rsCluster, SandboxId: rsSandbox, NodeId: rsNode, LeaseTtlMillis: 1, ExecutionId: rsExecution,
		}); err != nil {
			t.Fatalf("acquire failed: %v", err)
		}
		if store.leaseTTL != floor {
			t.Fatalf("acquire stamped %s, want the floor %s", store.leaseTTL, floor)
		}
	})

	t.Run("RenewNodeLease", func(t *testing.T) {
		store := &fakeStore{renewed: 1}
		svc := NewPausedRegistryService(zap.NewNop(), store, nil, rsCluster, 90*time.Second, floor)

		if _, err := svc.RenewNodeLease(context.Background(), &schedulerv1.RenewNodeLeaseRequest{
			ClusterId: rsCluster, NodeId: rsNode, LeaseTtlMillis: 1,
			Held: []*schedulerv1.HeldSandbox{{SandboxId: rsSandbox}},
		}); err != nil {
			t.Fatalf("renew failed: %v", err)
		}
		if store.leaseTTL != floor {
			t.Fatalf("renew stamped %s, want the floor %s", store.leaseTTL, floor)
		}
	})
}

// TestAZeroFloorFallsBackToTheDefault: a floor of zero is no floor, and the one
// value it has to catch is exactly zero.
func TestAZeroFloorFallsBackToTheDefault(t *testing.T) {
	store := &fakeStore{began: pausedregistry.BeganPause{Generation: 1}}
	svc := NewPausedRegistryService(zap.NewNop(), store, nil, rsCluster, 90*time.Second, 0)

	if _, err := svc.TransitionSandbox(context.Background(), &schedulerv1.TransitionSandboxRequest{
		ClusterId: rsCluster, SandboxId: rsSandbox, NodeId: rsNode,
		Kind:         schedulerv1.TransitionKind_TRANSITION_KIND_BEGIN_PAUSE,
		MetadataJson: json.RawMessage(`{"id":"s"}`), LeaseTtlMillis: 1, ExecutionId: rsExecution,
	}); err != nil {
		t.Fatalf("begin_pause failed: %v", err)
	}
	if store.leaseTTL != defaultLeaseTTLFloor {
		t.Fatalf("stamped %s, want the default floor %s", store.leaseTTL, defaultLeaseTTLFloor)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// The identity axis at the contract layer
// ─────────────────────────────────────────────────────────────────────────────

// TestATransitionWithoutAnExecutionIsRefused — T-A3-7.
//
// 🔴 The two fenced kinds require the field; the other four refuse it. Stated
// kind by kind rather than as "checked when present", because an optional
// fencing token needs a "missing means allowed" branch and that branch is the
// whole attack surface.
func TestATransitionWithoutAnExecutionIsRefused(t *testing.T) {
	gen := int64(4)

	cases := []struct {
		name string
		req  *schedulerv1.TransitionSandboxRequest
	}{
		{"begin_pause without an execution", &schedulerv1.TransitionSandboxRequest{
			Kind:         schedulerv1.TransitionKind_TRANSITION_KIND_BEGIN_PAUSE,
			MetadataJson: json.RawMessage(`{"id":"s"}`)}},
		{"mark_running without an execution", &schedulerv1.TransitionSandboxRequest{
			Kind: schedulerv1.TransitionKind_TRANSITION_KIND_MARK_RUNNING}},
		{"renew_deadline without an execution", &schedulerv1.TransitionSandboxRequest{
			Kind: schedulerv1.TransitionKind_TRANSITION_KIND_RENEW_DEADLINE}},
		// The other four refuse the field outright: it means nothing for them,
		// and a caller that sent it would believe a write was fenced that never
		// was.
		{"complete_pause with an execution", &schedulerv1.TransitionSandboxRequest{
			Kind:             schedulerv1.TransitionKind_TRANSITION_KIND_COMPLETE_PAUSE,
			ExpectGeneration: &gen, SnapshotId: rsSnap, ExecutionId: rsExecution}},
		{"mark_local_only with an execution", &schedulerv1.TransitionSandboxRequest{
			Kind:             schedulerv1.TransitionKind_TRANSITION_KIND_MARK_LOCAL_ONLY,
			ExpectGeneration: &gen, ExecutionId: rsExecution}},
		{"release_claim with an execution", &schedulerv1.TransitionSandboxRequest{
			Kind:             schedulerv1.TransitionKind_TRANSITION_KIND_RELEASE_CLAIM,
			ExpectGeneration: &gen, ExecutionId: rsExecution}},
		{"remove with an execution", &schedulerv1.TransitionSandboxRequest{
			Kind:             schedulerv1.TransitionKind_TRANSITION_KIND_REMOVE,
			ExpectGeneration: &gen, ExecutionId: rsExecution}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeStore{began: pausedregistry.BeganPause{Generation: 1}}
			svc := newTestRegistryService(t, store)

			tc.req.ClusterId, tc.req.SandboxId, tc.req.NodeId = rsCluster, rsSandbox, rsNode
			_, err := svc.TransitionSandbox(context.Background(), tc.req)
			if codeOf(err) != codes.InvalidArgument {
				t.Fatalf("expected InvalidArgument, got %s (%v)", codeOf(err), err)
			}
			if len(store.calls) != 0 {
				t.Fatalf("the request reached the store anyway: %v", store.calls)
			}
		})
	}
}

// TestTheFencedKindsPassTheExecutionDown is the control for the test above: the
// field is not merely demanded, it arrives.
func TestTheFencedKindsPassTheExecutionDown(t *testing.T) {
	t.Run("begin_pause", func(t *testing.T) {
		store := &fakeStore{began: pausedregistry.BeganPause{Generation: 1}}
		svc := newTestRegistryService(t, store)
		if _, err := svc.TransitionSandbox(context.Background(), &schedulerv1.TransitionSandboxRequest{
			ClusterId: rsCluster, SandboxId: rsSandbox, NodeId: rsNode,
			Kind:         schedulerv1.TransitionKind_TRANSITION_KIND_BEGIN_PAUSE,
			MetadataJson: json.RawMessage(`{"id":"s"}`), ExecutionId: rsExecution,
		}); err != nil {
			t.Fatalf("begin_pause failed: %v", err)
		}
		if store.lastExecution != rsExecution {
			t.Fatalf("the store was given %q, want %q", store.lastExecution, rsExecution)
		}
	})

	t.Run("mark_running", func(t *testing.T) {
		store := &fakeStore{markRunning: pausedregistry.MarkRunningAdopted}
		svc := newTestRegistryService(t, store)
		if _, err := svc.TransitionSandbox(context.Background(), &schedulerv1.TransitionSandboxRequest{
			ClusterId: rsCluster, SandboxId: rsSandbox, NodeId: rsNode,
			Kind: schedulerv1.TransitionKind_TRANSITION_KIND_MARK_RUNNING, ExecutionId: rsExecution,
		}); err != nil {
			t.Fatalf("mark_running failed: %v", err)
		}
		if store.lastExecution != rsExecution {
			t.Fatalf("the store was given %q, want %q", store.lastExecution, rsExecution)
		}
	})

	t.Run("renew_deadline", func(t *testing.T) {
		store := &fakeStore{deadlineRenewal: pausedregistry.DeadlineRenewalRenewed}
		svc := newTestRegistryService(t, store)
		if _, err := svc.TransitionSandbox(context.Background(), &schedulerv1.TransitionSandboxRequest{
			ClusterId: rsCluster, SandboxId: rsSandbox, NodeId: rsNode,
			Kind: schedulerv1.TransitionKind_TRANSITION_KIND_RENEW_DEADLINE, ExecutionId: rsExecution,
		}); err != nil {
			t.Fatalf("renew_deadline failed: %v", err)
		}
		if store.lastExecution != rsExecution {
			t.Fatalf("the store was given %q, want %q", store.lastExecution, rsExecution)
		}
	})

	// 🔴 The claim is where an incarnation is allocated, so it is required
	// there too — and it is the value the node reads back and starts the VM
	// under.
	t.Run("acquire", func(t *testing.T) {
		store := &fakeStore{claim: pausedregistry.ResumeClaim{Outcome: pausedregistry.ClaimOutcomeNotFound}}
		svc := newTestRegistryService(t, store)
		if _, err := svc.AcquireSandbox(context.Background(), &schedulerv1.AcquireSandboxRequest{
			ClusterId: rsCluster, SandboxId: rsSandbox, NodeId: rsNode, ExecutionId: rsExecution,
		}); err != nil {
			t.Fatalf("acquire failed: %v", err)
		}
		if store.lastExecution != rsExecution {
			t.Fatalf("the store was given %q, want %q", store.lastExecution, rsExecution)
		}
	})

	t.Run("acquire without one", func(t *testing.T) {
		store := &fakeStore{claim: pausedregistry.ResumeClaim{Outcome: pausedregistry.ClaimOutcomeNotFound}}
		svc := newTestRegistryService(t, store)
		_, err := svc.AcquireSandbox(context.Background(), &schedulerv1.AcquireSandboxRequest{
			ClusterId: rsCluster, SandboxId: rsSandbox, NodeId: rsNode,
		})
		if codeOf(err) != codes.InvalidArgument {
			t.Fatalf("expected InvalidArgument, got %s (%v)", codeOf(err), err)
		}
		if len(store.calls) != 0 {
			t.Fatalf("the claim reached the store with no incarnation to allocate: %v", store.calls)
		}
	})
}

// TestExecutionFencedIsPermissionDenied — T-A3-6.
//
// 🔴 Never Aborted. The node's handler for Aborted is to re-read and try
// again, and a re-read after a fenced write hands it the live incarnation's
// generation — with which the same write goes straight through.
func TestExecutionFencedIsPermissionDenied(t *testing.T) {
	got := registryErrorCode(fmt.Errorf("wrapped: %w", pausedregistry.ErrExecutionFenced))
	if got != codes.PermissionDenied {
		t.Fatalf("ErrExecutionFenced maps to %s, want %s", got, codes.PermissionDenied)
	}
	if got == codes.Aborted {
		t.Fatal("a fenced write maps to the code the node retries around")
	}
	// The control: the version axis still maps to the retryable code, so this
	// is not a build that collapsed both onto one answer.
	if generation := registryErrorCode(pausedregistry.ErrGenerationConflict); generation != codes.Aborted {
		t.Fatalf("ErrGenerationConflict maps to %s, want %s", generation, codes.Aborted)
	}
}

// TestAFencedWriteReachesTheWireAsPermissionDenied is the same statement made
// through the handler, so a mapping that exists but is never consulted fails
// here.
func TestAFencedWriteReachesTheWireAsPermissionDenied(t *testing.T) {
	store := &fakeStore{err: fmt.Errorf("sandbox belongs to another incarnation: %w", pausedregistry.ErrExecutionFenced)}
	svc := newTestRegistryService(t, store)

	_, err := svc.TransitionSandbox(context.Background(), &schedulerv1.TransitionSandboxRequest{
		ClusterId: rsCluster, SandboxId: rsSandbox, NodeId: rsNode,
		Kind:         schedulerv1.TransitionKind_TRANSITION_KIND_BEGIN_PAUSE,
		MetadataJson: json.RawMessage(`{"id":"s"}`), ExecutionId: rsExecution,
	})
	if codeOf(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %s (%v)", codeOf(err), err)
	}
}

// TestACheckViolationIsNotReportedAsUnavailable.
//
// The execution CHECK is the one constraint a statement of ours can trip. It
// falls to FailedPrecondition — an operator with a psql prompt — rather than to
// the default arm, which reads as "try again later" and would put every node
// into a retry loop over a write that can never succeed.
func TestACheckViolationIsNotReportedAsUnavailable(t *testing.T) {
	violation := &pgconn.PgError{Code: "23514", ConstraintName: "paused_sandboxes_execution_check"}
	got := registryErrorCode(fmt.Errorf("registry begin_pause: %w", violation))
	if got != codes.FailedPrecondition {
		t.Fatalf("a CHECK violation maps to %s, want %s", got, codes.FailedPrecondition)
	}

	// The control: an unrecognised database error is still Unavailable, so this
	// is not a build that stopped retrying everything.
	other := &pgconn.PgError{Code: "08006"}
	if got := registryErrorCode(fmt.Errorf("registry begin_pause: %w", other)); got != codes.Unavailable {
		t.Fatalf("a connection failure maps to %s, want %s", got, codes.Unavailable)
	}
}
