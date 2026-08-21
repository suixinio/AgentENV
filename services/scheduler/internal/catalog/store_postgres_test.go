package catalog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

// The store against a real database.
//
// Everything here needs one: the properties worth asserting are the ones the
// database enforces — a fenced update matching nothing, a partial unique index
// refusing a second live build, a transaction rolling both of its halves back —
// and a fake that answered them would only be asserting itself.
//
// 🔴 Which is why a run without SCHEDULER_REGISTRY_TEST_DSN skips instead of
// passing quietly, and why a runner that is supposed to have a database sets
// SCHEDULER_REGISTRY_TEST_REQUIRED and gets a failure rather than a file of
// green no-ops.

const testCluster = "11111111-1111-1111-1111-111111111111"

// ─────────────────────────────────────────────────────────────────────────────
// Fixture
// ─────────────────────────────────────────────────────────────────────────────

type fixture struct {
	t       *testing.T
	pool    *pgxpool.Pool
	store   *PostgresStore
	paused  *scratchPausedHalf
	cluster string
	ctx     context.Context
}

func newFixture(t *testing.T, opts ...func(*StoreConfig)) *fixture {
	t.Helper()

	pool := migratedPool(t)
	ctx := context.Background()

	// A stand-in for `paused_sandboxes`, in this test's own schema.
	//
	// 🔴 A real table and not a mock. What these tests are about is that the
	// catalog row and the registry row move together, and a mock that recorded
	// calls could not tell a rollback from a call that was never made — which
	// is the whole difference the transaction buys.
	if _, err := pool.Exec(ctx, `
CREATE TABLE scratch_paused (
    sandbox_id  UUID   PRIMARY KEY,
    cluster_id  UUID   NOT NULL,
    generation  BIGINT NOT NULL,
    state       TEXT   NOT NULL,
    snapshot_id UUID   NULL
)`); err != nil {
		t.Fatalf("create the paused-half stand-in: %v", err)
	}

	paused := &scratchPausedHalf{}
	cfg := StoreConfig{Logger: zap.NewNop(), Paused: paused}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &fixture{
		t:       t,
		pool:    pool,
		store:   NewStoreWithPool(pool, cfg),
		paused:  paused,
		cluster: testCluster,
		ctx:     ctx,
	}
}

func withoutPausedHalf(cfg *StoreConfig) { cfg.Paused = nil }

func withBuildCeiling(n int) func(*StoreConfig) {
	return func(cfg *StoreConfig) { cfg.MaxConcurrentBuilds = n }
}

// scratchPausedHalf is the registry's side of a catalog write, written against
// the scratch table above with the same conditional shapes the real one has.
type scratchPausedHalf struct {
	mu    sync.Mutex
	calls []string

	beginErr     error
	completeErr  error
	localOnlyErr error
}

func (h *scratchPausedHalf) record(call string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls = append(h.calls, call)
}

func (h *scratchPausedHalf) Begin(ctx context.Context, tx pgx.Tx, in PausedBegin) (PausedBegan, error) {
	h.record("begin:" + in.SandboxID)
	if h.beginErr != nil {
		return PausedBegan{}, h.beginErr
	}
	var (
		generation int64
		previous   *string
	)
	err := tx.QueryRow(ctx, `
WITH previous AS (
    SELECT snapshot_id FROM scratch_paused WHERE sandbox_id = $1::uuid
), upserted AS (
    INSERT INTO scratch_paused (sandbox_id, cluster_id, generation, state)
    VALUES ($1::uuid, $2::uuid, 1, 'publishing')
    ON CONFLICT (sandbox_id) DO UPDATE
        SET generation = scratch_paused.generation + 1, state = 'publishing'
    RETURNING generation
)
SELECT upserted.generation, previous.snapshot_id::text
  FROM upserted LEFT JOIN previous ON TRUE`, in.SandboxID, in.ClusterID).Scan(&generation, &previous)
	if err != nil {
		return PausedBegan{}, err
	}
	began := PausedBegan{Generation: generation}
	if previous != nil {
		began.PreviousSnapshotID = *previous
	}
	return began, nil
}

func (h *scratchPausedHalf) Complete(ctx context.Context, tx pgx.Tx, in PausedFinish, snapshotID string) error {
	h.record("complete:" + in.SandboxID)
	if h.completeErr != nil {
		return h.completeErr
	}
	tag, err := tx.Exec(ctx, `
UPDATE scratch_paused SET state = 'paused', snapshot_id = $3::uuid
 WHERE sandbox_id = $1::uuid AND generation = $2 AND state = 'publishing'`,
		in.SandboxID, in.ExpectGeneration, snapshotID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: generation %d", ErrPausedGenerationMismatch, in.ExpectGeneration)
	}
	return nil
}

func (h *scratchPausedHalf) MarkLocalOnly(ctx context.Context, tx pgx.Tx, in PausedFinish) error {
	h.record("local_only:" + in.SandboxID)
	if h.localOnlyErr != nil {
		return h.localOnlyErr
	}
	tag, err := tx.Exec(ctx, `
UPDATE scratch_paused SET state = 'local_only'
 WHERE sandbox_id = $1::uuid AND generation = $2 AND state = 'publishing'`,
		in.SandboxID, in.ExpectGeneration)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: generation %d", ErrPausedGenerationMismatch, in.ExpectGeneration)
	}
	return nil
}

func (h *scratchPausedHalf) ObserveGeneration(ctx context.Context, tx pgx.Tx, clusterID, sandboxID string) (int64, bool, error) {
	var generation int64
	err := tx.QueryRow(ctx, `SELECT generation FROM scratch_paused WHERE sandbox_id = $1::uuid`, sandboxID).Scan(&generation)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	return generation, err == nil, err
}

func (f *fixture) pausedState(sandboxID string) (string, bool) {
	f.t.Helper()

	var state string
	err := f.pool.QueryRow(f.ctx, `SELECT state FROM scratch_paused WHERE sandbox_id = $1::uuid`, sandboxID).Scan(&state)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false
	}
	if err != nil {
		f.t.Fatalf("read the paused-half stand-in: %v", err)
	}
	return state, true
}

// ─────────────────────────────────────────────────────────────────────────────
// Fixture conveniences
// ─────────────────────────────────────────────────────────────────────────────

func (f *fixture) beginTemplate(alias string) *SnapshotRow {
	f.t.Helper()
	return f.begin(BeginInput{
		SnapshotID:  newUUID(f.t),
		SourceKind:  SourceKindTemplate,
		Status:      StatusWaiting,
		Alias:       alias,
		Published:   true,
		CreatedAtMs: time.Now().UnixMilli(),
	})
}

func (f *fixture) begin(in BeginInput) *SnapshotRow {
	f.t.Helper()

	out, err := f.beginRaw(in)
	if err != nil {
		f.t.Fatalf("begin a snapshot: %v", err)
	}
	if out.Rejected != nil {
		f.t.Fatalf("begin a snapshot was refused: %s", out.Rejected.Reason)
	}
	return out.Row
}

func (f *fixture) beginRaw(in BeginInput) (BeginOutcome, error) {
	f.t.Helper()

	if in.ClusterID == "" {
		in.ClusterID = f.cluster
	}
	if in.SnapshotID == "" {
		in.SnapshotID = newUUID(f.t)
	}
	if in.SourceKind == "" {
		in.SourceKind = SourceKindTemplate
	}
	if in.Status == "" {
		in.Status = StatusWaiting
	}
	if in.CPUCount == 0 {
		in.CPUCount = 2
	}
	if in.MemoryMiB == 0 {
		in.MemoryMiB = 512
	}
	if in.DiskSizeMiB == 0 {
		in.DiskSizeMiB = 2048
	}
	if in.CreatedAtMs == 0 {
		in.CreatedAtMs = time.Now().UnixMilli()
	}
	if !in.Published && in.OriginNodeID == "" {
		in.Published = true
	}
	return f.store.BeginSnapshot(f.ctx, in)
}

// beginWithoutADiskSize opens a row the way a v3 template create does: with the
// disk size the build has not produced yet, which the node writes as 0.
//
// 🔴 Deliberately not routed through beginRaw, which fills a 0 in with 2048.
// That convenience is what makes every other test readable and it is exactly
// the value these tests are about, so they go straight to the store.
func (f *fixture) beginWithoutADiskSize(status string) *SnapshotRow {
	f.t.Helper()

	out, err := f.store.BeginSnapshot(f.ctx, BeginInput{
		ClusterID:   f.cluster,
		SnapshotID:  newUUID(f.t),
		SourceKind:  SourceKindTemplate,
		Status:      status,
		CPUCount:    2,
		MemoryMiB:   512,
		DiskSizeMiB: 0,
		Published:   true,
		CreatedAtMs: time.Now().UnixMilli(),
	})
	if err != nil {
		f.t.Fatalf("open a row that does not know its disk size yet: %v", err)
	}
	if out.Rejected != nil {
		f.t.Fatalf("open a row that does not know its disk size yet was refused: %s", out.Rejected.Reason)
	}
	return out.Row
}

func (f *fixture) commit(in CommitInput) *SnapshotRow {
	f.t.Helper()

	out, err := f.commitRaw(in)
	if err != nil {
		f.t.Fatalf("commit a snapshot: %v", err)
	}
	if out.Rejected != nil {
		f.t.Fatalf("commit was refused: %s (%s)", out.Rejected.Reason, out.Rejected.ObservedStatus)
	}
	return out.Row
}

func (f *fixture) commitRaw(in CommitInput) (CommitOutcome, error) {
	f.t.Helper()

	if in.ClusterID == "" {
		in.ClusterID = f.cluster
	}
	if in.CommittedPayload == nil {
		in.CommittedPayload = []byte{0x01, 0x02, 0x03}
	}
	if in.CommittedSchema == 0 {
		in.CommittedSchema = 1
	}
	if in.UpdatedAtMs == 0 {
		in.UpdatedAtMs = time.Now().UnixMilli()
	}
	if !in.Published && in.OriginNodeID == "" {
		in.Published = true
	}
	return f.store.CommitSnapshot(f.ctx, in)
}

// readyTemplate is a committed template row, which is what most reads are about.
func (f *fixture) readyTemplate(alias string) *SnapshotRow {
	f.t.Helper()

	row := f.begin(BeginInput{Status: StatusBuilding, Alias: alias})
	return f.commit(CommitInput{SnapshotID: row.SnapshotID, Published: true})
}

func mustJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()

	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return raw
}

// ─────────────────────────────────────────────────────────────────────────────
// BeginSnapshot
// ─────────────────────────────────────────────────────────────────────────────

func TestBeginSnapshotOpensATemplateWaiting(t *testing.T) {
	f := newFixture(t)

	row := f.beginTemplate("base")
	if row.Status != StatusWaiting {
		t.Fatalf("status = %q, want %q", row.Status, StatusWaiting)
	}
	// 🔴 The status group came from the trigger, not from the insert: the
	// statement writes 'pending' unconditionally, so a `building` row reading
	// back 'in_progress' is the trigger and nothing else. Asserted again below
	// where the two would differ.
	if row.StatusGroup != StatusGroupPending {
		t.Fatalf("status group = %q, want %q", row.StatusGroup, StatusGroupPending)
	}
	if row.Alias != "base" {
		t.Fatalf("alias = %q", row.Alias)
	}
	if row.CommittedPayload != nil {
		t.Fatalf("a row with no bytes yet carries a payload: %v", row.CommittedPayload)
	}
	if !row.Published {
		t.Fatal("a template is born published: nothing is on any node yet")
	}

	// The template half of the row exists too, and its soft-delete flag follows
	// the snapshot's.
	var count int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM templates WHERE id = $1::uuid`, row.SnapshotID).Scan(&count); err != nil {
		t.Fatalf("read templates: %v", err)
	}
	if count != 1 {
		t.Fatalf("templates rows = %d, want 1", count)
	}
}

func TestBeginSnapshotDerivesTheStatusGroupFromTheStatus(t *testing.T) {
	f := newFixture(t)

	// The insert sends 'pending' for every row. A `building` row that reads
	// back 'in_progress' can only have got there through the trigger, which is
	// what the partial indexes are built on.
	row := f.begin(BeginInput{
		SourceKind:      SourceKindSandbox,
		SourceSandboxID: "sbx-1",
		Status:          StatusBuilding,
		Published:       false,
		OriginNodeID:    "node-a",
	})
	if row.StatusGroup != StatusGroupInProgress {
		t.Fatalf("status group = %q, want %q: the trigger did not derive it", row.StatusGroup, StatusGroupInProgress)
	}
}

func TestBeginSnapshotRefusesRowsNothingCanInterpret(t *testing.T) {
	f := newFixture(t)

	// A row that satisfies everything, so each case can break exactly one
	// thing and the failure names the thing it broke.
	valid := func() BeginInput {
		return BeginInput{
			ClusterID:   testCluster,
			SnapshotID:  newUUID(t),
			SourceKind:  SourceKindTemplate,
			Status:      StatusWaiting,
			CPUCount:    2,
			MemoryMiB:   512,
			DiskSizeMiB: 2048,
			Published:   true,
			CreatedAtMs: time.Now().UnixMilli(),
		}
	}

	cases := []struct {
		name   string
		break_ func(*BeginInput)
		want   string
	}{
		{
			// 🔴 `ready` requires a payload and there is none yet. Refused here
			// rather than by the CHECK, so the caller learns which field is
			// wrong instead of which constraint fired.
			name:   "born ready",
			break_: func(in *BeginInput) { in.Status = StatusReady },
			want:   "'waiting' or 'building'",
		},
		{
			name:   "a template naming a sandbox",
			break_: func(in *BeginInput) { in.SourceSandboxID = "sbx-1" },
			want:   "disagree",
		},
		{
			name:   "a sandbox naming none",
			break_: func(in *BeginInput) { in.SourceKind = SourceKindSandbox },
			want:   "disagree",
		},
		{
			name:   "an unknown source kind",
			break_: func(in *BeginInput) { in.SourceKind = "snapshot" },
			want:   "source kind",
		},
		{
			// The one rule the origin block does enforce: an unpublished row
			// must say where its bytes are, or nothing can ever start it.
			name:   "unpublished with no origin",
			break_: func(in *BeginInput) { in.Published = false },
			want:   "name the node",
		},
		{
			name:   "no cpu",
			break_: func(in *BeginInput) { in.CPUCount = 0 },
			want:   "cpu_count",
		},
		{
			name:   "no memory",
			break_: func(in *BeginInput) { in.MemoryMiB = 0 },
			want:   "memory_mib",
		},
		// 🔴 There is deliberately no "no disk" case here. 0 is what a v3
		// template legitimately opens with — see
		// TestBeginSnapshotAcceptsATemplateThatDoesNotKnowItsDiskSizeYet — and
		// the rule that a disk size must be positive lives on the commit
		// instead, where the row becomes launchable.
		{
			name:   "an id that is not a uuid",
			break_: func(in *BeginInput) { in.SnapshotID = "not-a-uuid" },
			want:   "not a uuid",
		},
		{
			name:   "a cluster that is not a uuid",
			break_: func(in *BeginInput) { in.ClusterID = "cluster-one" },
			want:   "not a uuid",
		},
		{
			// Written but never checked in this phase, and still refused when
			// it is not a uuid: the column is one, and a value that cannot go
			// in is better named here than in a cast error.
			name:   "an execution id that is not a uuid",
			break_: func(in *BeginInput) { in.PublishingExecutionID = "exec-1" },
			want:   "not a uuid",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := valid()
			tc.break_(&in)
			_, err := f.store.BeginSnapshot(f.ctx, in)
			if err == nil {
				t.Fatal("the row was accepted")
			}
			if !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("error is not ErrInvalidArgument: %v", err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not mention %q", err, tc.want)
			}
		})
	}

	// And the unbroken row goes in, so the cases above are refusals rather than
	// a builder that never worked.
	if _, err := f.store.BeginSnapshot(f.ctx, valid()); err != nil {
		t.Fatalf("the valid row was refused too: %v", err)
	}
}

// TestBeginSnapshotAcceptsATemplateThatDoesNotKnowItsDiskSizeYet is the defect
// migration 0003 exists for, run.
//
// 🔴 A v3 template genuinely has no disk size when its row opens. The
// E2B-compatible create request carries name, tags, cpuCount and memoryMB and
// has no disk field at all; the real figure is the virtual size of a rootfs the
// build has not produced yet, and the node's encoding for "not known yet" is 0
// (src/template/build_spec.rs). While a positive disk size was demanded here,
// every v3 template create on the cluster was refused with InvalidArgument
// before a build could ever produce the number being demanded.
func TestBeginSnapshotAcceptsATemplateThatDoesNotKnowItsDiskSizeYet(t *testing.T) {
	f := newFixture(t)

	// Not f.beginRaw: the fixture fills a 0 disk size in with 2048, and 0 is
	// the value under test.
	out, err := f.store.BeginSnapshot(f.ctx, BeginInput{
		ClusterID:   f.cluster,
		SnapshotID:  newUUID(t),
		SourceKind:  SourceKindTemplate,
		Status:      StatusWaiting,
		CPUCount:    2,
		MemoryMiB:   512,
		DiskSizeMiB: 0,
		Published:   true,
		CreatedAtMs: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("a template that does not know its disk size yet was refused: %v", err)
	}
	if out.Rejected != nil {
		t.Fatalf("begin was refused: %s", out.Rejected.Reason)
	}
	if out.Row == nil {
		t.Fatal("begin returned neither a row nor a refusal")
	}
	// Kept as sent rather than corrected to something plausible: 0 is the
	// statement that the build has not run.
	if out.Row.DiskSizeMiB != 0 {
		t.Fatalf("disk_size_mib = %d, want 0", out.Row.DiskSizeMiB)
	}
	// And it is not launchable, which is why 0 is safe here: no resolving query
	// can return a row outside the ready group.
	if out.Row.StatusGroup == StatusGroupReady {
		t.Fatalf("a row with no disk size landed in the ready group")
	}
}

// TestABuildFillsInTheDiskSizeItsTemplateOpenedWithout is the round trip: open
// at 0, build, commit the size the build produced.
func TestABuildFillsInTheDiskSizeItsTemplateOpenedWithout(t *testing.T) {
	f := newFixture(t)

	opened := f.beginWithoutADiskSize(StatusBuilding)
	if opened.DiskSizeMiB != 0 {
		t.Fatalf("the row opened at %d MiB, want 0", opened.DiskSizeMiB)
	}

	built := uint32(4096)
	row := f.commit(CommitInput{SnapshotID: opened.SnapshotID, Published: true, DiskSizeMiB: &built})
	if row.Status != StatusReady {
		t.Fatalf("status = %q, want %q", row.Status, StatusReady)
	}
	if row.DiskSizeMiB != built {
		t.Fatalf("disk_size_mib = %d, want %d", row.DiskSizeMiB, built)
	}

	// Read back, because what a launch gets is the row and not the commit's
	// return value.
	after, err := f.store.GetSnapshot(f.ctx, f.cluster, opened.SnapshotID, ReadOptions{})
	if err != nil {
		t.Fatalf("read the committed row: %v", err)
	}
	if after == nil || after.DiskSizeMiB != built {
		t.Fatalf("the ready row carries %+v, want disk_size_mib %d", after, built)
	}
}

func TestBeginSnapshotRefusesAnIdThatIsAlreadyThere(t *testing.T) {
	f := newFixture(t)

	row := f.beginTemplate("")
	out, err := f.beginRaw(BeginInput{SnapshotID: row.SnapshotID})
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if out.Rejected == nil || out.Rejected.Reason != RejectionAlreadyExists {
		t.Fatalf("outcome = %+v, want ALREADY_EXISTS", out)
	}
}

func TestBeginSnapshotRefusesAnAliasSomebodyElseHolds(t *testing.T) {
	f := newFixture(t)

	first := f.beginTemplate("shared")
	out, err := f.beginRaw(BeginInput{Alias: "shared"})
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if out.Rejected == nil || out.Rejected.Reason != RejectionAliasTaken {
		t.Fatalf("outcome = %+v, want ALIAS_TAKEN", out)
	}
	if out.Rejected.AliasHolder != first.SnapshotID {
		t.Fatalf("holder = %q, want %q", out.Rejected.AliasHolder, first.SnapshotID)
	}

	// 🔴 The whole write is refused, not committed without the alias. A row
	// that appeared anyway would be a snapshot the user cannot reach by the
	// name they asked for, which is what the object-store backend does today.
	var rows int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM snapshots`).Scan(&rows); err != nil {
		t.Fatalf("count: %v", err)
	}
	if rows != 1 {
		t.Fatalf("snapshots = %d, want 1: the refused begin left a row behind", rows)
	}
}

func TestBeginSnapshotRefusesAPausedTransitionItCannotApply(t *testing.T) {
	f := newFixture(t, withoutPausedHalf)

	_, err := f.beginRaw(BeginInput{
		NodeID: "node-a",
		Paused: &PausedBegin{SandboxID: newUUID(t), Metadata: mustJSON(t, map[string]string{"a": "b"}), ExecutionID: newUUID(t)},
	})
	if !errors.Is(err, ErrNoPausedHalf) {
		t.Fatalf("error = %v, want ErrNoPausedHalf: applying half a pause is worse than refusing it", err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// CommitSnapshot
// ─────────────────────────────────────────────────────────────────────────────

func TestCommitSnapshotFlipsTheRowAndKeepsThePayloadIntact(t *testing.T) {
	f := newFixture(t)

	opened := f.begin(BeginInput{
		SourceKind:      SourceKindSandbox,
		SourceSandboxID: "sbx-7",
		Status:          StatusBuilding,
		Published:       false,
		OriginNodeID:    "node-a",
	})

	// Bytes that are not valid UTF-8 and not valid JSON: the payload is a
	// CommittedSnapshot as the node serialised it, and nothing on this side may
	// assume anything about it.
	payload := []byte{0x00, 0xff, 0xfe, 0x7f, 0x80, 0x00}
	row := f.commit(CommitInput{
		SnapshotID:       opened.SnapshotID,
		CommittedPayload: payload,
		CommittedSchema:  3,
		Alias:            "prod",
		Published:        true,
		OriginNodeID:     "node-a",
	})

	if row.Status != StatusReady || row.StatusGroup != StatusGroupReady {
		t.Fatalf("status = %q/%q, want ready/ready", row.Status, row.StatusGroup)
	}
	if string(row.CommittedPayload) != string(payload) {
		t.Fatalf("payload came back as %x, want %x", row.CommittedPayload, payload)
	}
	if row.CommittedSchema == nil || *row.CommittedSchema != 3 {
		t.Fatalf("committed schema = %v, want 3", row.CommittedSchema)
	}
	if row.Alias != "prod" {
		t.Fatalf("alias = %q", row.Alias)
	}
	if !row.Published {
		t.Fatal("published = false after a commit that published")
	}
	if row.OriginNodeID != "node-a" {
		t.Fatalf("origin hint = %q, want it kept as a placement hint", row.OriginNodeID)
	}
}

// TestCommitIsFencedOnTheRowStillBuilding is the predicate §5.2 calls
// load-bearing, exercised from every state it has to refuse.
func TestCommitIsFencedOnTheRowStillBuilding(t *testing.T) {
	f := newFixture(t)

	t.Run("a waiting row is not committable", func(t *testing.T) {
		row := f.beginTemplate("")
		out, err := f.commitRaw(CommitInput{SnapshotID: row.SnapshotID})
		if err != nil {
			t.Fatalf("commit: %v", err)
		}
		if out.Rejected == nil || out.Rejected.Reason != RejectionStatusMismatch {
			t.Fatalf("outcome = %+v, want STATUS_MISMATCH", out)
		}
		if out.Rejected.ObservedStatus != StatusWaiting {
			t.Fatalf("observed status = %q, want %q", out.Rejected.ObservedStatus, StatusWaiting)
		}
	})

	t.Run("committing twice refuses the second", func(t *testing.T) {
		row := f.begin(BeginInput{Status: StatusBuilding})
		f.commit(CommitInput{SnapshotID: row.SnapshotID})

		out, err := f.commitRaw(CommitInput{SnapshotID: row.SnapshotID, CommittedPayload: []byte{0x09}})
		if err != nil {
			t.Fatalf("commit: %v", err)
		}
		if out.Rejected == nil || out.Rejected.Reason != RejectionStatusMismatch {
			t.Fatalf("outcome = %+v, want STATUS_MISMATCH", out)
		}
		if out.Rejected.ObservedStatus != StatusReady {
			t.Fatalf("observed status = %q, want %q", out.Rejected.ObservedStatus, StatusReady)
		}

		// And the first commit's payload is still the one on the row.
		after, err := f.store.GetSnapshot(f.ctx, f.cluster, row.SnapshotID, ReadOptions{})
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if len(after.CommittedPayload) != 3 {
			t.Fatalf("the refused commit overwrote the payload: %x", after.CommittedPayload)
		}
	})

	t.Run("a deleted row is not committable", func(t *testing.T) {
		row := f.begin(BeginInput{Status: StatusBuilding})
		if _, err := f.store.DeleteSnapshot(f.ctx, f.cluster, row.SnapshotID, time.Now().UnixMilli()); err != nil {
			t.Fatalf("delete: %v", err)
		}
		out, err := f.commitRaw(CommitInput{SnapshotID: row.SnapshotID})
		if err != nil {
			t.Fatalf("commit: %v", err)
		}
		if out.Rejected == nil || out.Rejected.Reason != RejectionNotFound {
			t.Fatalf("outcome = %+v, want NOT_FOUND", out)
		}
	})

	t.Run("an id nothing ever had", func(t *testing.T) {
		out, err := f.commitRaw(CommitInput{SnapshotID: newUUID(t)})
		if err != nil {
			t.Fatalf("commit: %v", err)
		}
		if out.Rejected == nil || out.Rejected.Reason != RejectionNotFound {
			t.Fatalf("outcome = %+v, want NOT_FOUND", out)
		}
	})
}

func TestCommitRefusesAnEmptyPayload(t *testing.T) {
	f := newFixture(t)

	row := f.begin(BeginInput{Status: StatusBuilding})
	_, err := f.store.CommitSnapshot(f.ctx, CommitInput{
		ClusterID:  f.cluster,
		SnapshotID: row.SnapshotID,
		Published:  true,
	})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("error = %v, want ErrInvalidArgument", err)
	}
}

// TestCommitRefusesADiskSizeOfZero is the other end of the split: 0 means "not
// known yet", and the commit is where that stops being an answer anything can
// use. Refused in the store rather than by the CHECK, so the caller is told
// which field is wrong.
func TestCommitRefusesADiskSizeOfZero(t *testing.T) {
	f := newFixture(t)

	row := f.begin(BeginInput{Status: StatusBuilding})
	zero := uint32(0)
	_, err := f.commitRaw(CommitInput{
		SnapshotID:  row.SnapshotID,
		Published:   true,
		DiskSizeMiB: &zero,
	})
	if err == nil {
		t.Fatal("a commit stating the finished snapshot has no disk size was accepted")
	}
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("error = %v, want ErrInvalidArgument", err)
	}
	if !strings.Contains(err.Error(), "disk_size_mib") {
		t.Fatalf("error %q does not name the field", err)
	}

	// And the row did not move: a refused commit leaves a retryable build, not
	// a half-flipped row.
	after, err := f.store.GetSnapshot(f.ctx, f.cluster, row.SnapshotID, ReadOptions{})
	if err != nil {
		t.Fatalf("read the row back: %v", err)
	}
	if after == nil || after.Status != StatusBuilding {
		t.Fatalf("row = %+v, want it still building", after)
	}

	// 🔴 And it is this guard doing it, not the table's CHECK catching the
	// same value one layer down. A stated 0 is wrong about the *argument*, so
	// it is refused before the row is looked at — against an id that does not
	// exist, the caller is still told which field is wrong rather than being
	// handed a NOT_FOUND and sent looking for a row. With the guard removed
	// this is the case the CHECK cannot cover: the update matches nothing, so
	// nothing is ever checked.
	out, err := f.commitRaw(CommitInput{
		SnapshotID:  newUUID(t),
		Published:   true,
		DiskSizeMiB: &zero,
	})
	if err == nil {
		t.Fatalf("a commit with disk_size_mib = 0 against a missing row was not refused: %+v", out)
	}
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("error = %v, want ErrInvalidArgument", err)
	}
}

// TestCommitRefusesLeavingAReadyRowWithNoDiskSize is the residual case, and the
// one worth a test of its own.
//
// 🔴 The resource fields are pointers and COALESCEd, so nil means "keep what
// the begin recorded" — and when that is 0, a commit sending nothing asks for a
// launchable row with no disk size. Nothing in the store can see it coming; the
// table's CHECK is what catches it. What this pins is that the refusal arrives
// as ErrInvalidArgument and not as the bare pg error, because everything
// unclassified maps to UNAVAILABLE and the node would retry a write that can
// never succeed.
func TestCommitRefusesLeavingAReadyRowWithNoDiskSize(t *testing.T) {
	f := newFixture(t)

	row := f.beginWithoutADiskSize(StatusBuilding)
	_, err := f.commitRaw(CommitInput{SnapshotID: row.SnapshotID, Published: true})
	if err == nil {
		t.Fatal("a row with no disk size was made ready")
	}
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("the table's refusal reached the caller unclassified, as %v: "+
			"catalogErrorCode maps that to UNAVAILABLE, which tells the node to retry forever", err)
	}
	if !strings.Contains(err.Error(), "disk_size_mib") {
		t.Fatalf("error %q does not name the field", err)
	}

	after, err := f.store.GetSnapshot(f.ctx, f.cluster, row.SnapshotID, ReadOptions{})
	if err != nil {
		t.Fatalf("read the row back: %v", err)
	}
	if after == nil || after.Status != StatusBuilding {
		t.Fatalf("row = %+v, want it still building", after)
	}
}

// TestCommitLeavesTheRowBuildingWhenTheAliasIsTaken is the rollback that makes
// "all three or none" true.
func TestCommitLeavesTheRowBuildingWhenTheAliasIsTaken(t *testing.T) {
	f := newFixture(t)

	f.readyTemplate("taken")
	row := f.begin(BeginInput{Status: StatusBuilding})

	out, err := f.commitRaw(CommitInput{SnapshotID: row.SnapshotID, Alias: "taken"})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if out.Rejected == nil || out.Rejected.Reason != RejectionAliasTaken {
		t.Fatalf("outcome = %+v, want ALIAS_TAKEN", out)
	}

	after, err := f.store.GetSnapshot(f.ctx, f.cluster, row.SnapshotID, ReadOptions{})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if after.Status != StatusBuilding {
		t.Fatalf("status = %q after a refused commit, want it left at %q", after.Status, StatusBuilding)
	}
	if after.CommittedPayload != nil {
		t.Fatalf("a refused commit left a payload behind: %x", after.CommittedPayload)
	}
}

func TestCommitCanRenameAnAliasItAlreadyHolds(t *testing.T) {
	f := newFixture(t)

	opened := f.begin(BeginInput{Status: StatusBuilding, Alias: "old"})
	if opened.Alias != "old" {
		t.Fatalf("alias = %q", opened.Alias)
	}

	// 🔴 A snapshot has at most one alias, so this is a rename. The unique
	// index over snapshot_id would otherwise refuse it — and that refusal is
	// not the alias conflict the API reports, so reporting it as one would tell
	// the user their own rename collided with a stranger.
	row := f.commit(CommitInput{SnapshotID: opened.SnapshotID, Alias: "new"})
	if row.Alias != "new" {
		t.Fatalf("alias = %q, want the rename to have taken", row.Alias)
	}

	// And the old name is free again.
	other := f.begin(BeginInput{Alias: "old"})
	if other.Alias != "old" {
		t.Fatalf("the released alias was not rebindable: %q", other.Alias)
	}
}

func TestCommitBindingTheSameAliasTwiceIsIdempotent(t *testing.T) {
	f := newFixture(t)

	opened := f.begin(BeginInput{Status: StatusBuilding, Alias: "same"})
	row := f.commit(CommitInput{SnapshotID: opened.SnapshotID, Alias: "same"})
	if row.Alias != "same" {
		t.Fatalf("alias = %q", row.Alias)
	}
}

// TestCommitLeavesAPublishThatFailedRunnableOnItsOrigin is §3.1's rule, which
// is the reason `published` is not folded into the status.
func TestCommitLeavesAPublishThatFailedRunnableOnItsOrigin(t *testing.T) {
	f := newFixture(t)

	opened := f.begin(BeginInput{
		SourceKind: SourceKindSandbox, SourceSandboxID: "sbx-9",
		Status: StatusBuilding, Published: false, OriginNodeID: "node-a",
	})
	row := f.commit(CommitInput{
		SnapshotID:   opened.SnapshotID,
		Published:    false,
		OriginNodeID: "node-a",
	})

	// 🔴 `ready`, not stuck at building. The snapshot is complete and node-a
	// can start it; saying otherwise would tell the user a snapshot they can
	// still resume does not exist.
	if row.Status != StatusReady {
		t.Fatalf("status = %q, want %q", row.Status, StatusReady)
	}
	if row.Published {
		t.Fatal("published = true after a publish that never reached shared storage")
	}

	allowed, pin := PinOriginIfUnpublished(*row, "node-b")
	if allowed || pin != "node-a" {
		t.Fatalf("a node other than the origin was allowed: allowed=%v pin=%q", allowed, pin)
	}
	allowed, _ = PinOriginIfUnpublished(*row, "node-a")
	if !allowed {
		t.Fatal("the origin node was refused its own snapshot")
	}
}

func TestCommitRefusesAnUnpublishedRowWithNoOrigin(t *testing.T) {
	f := newFixture(t)

	row := f.begin(BeginInput{Status: StatusBuilding})
	_, err := f.store.CommitSnapshot(f.ctx, CommitInput{
		ClusterID:        f.cluster,
		SnapshotID:       row.SnapshotID,
		CommittedPayload: []byte{1},
		CommittedSchema:  1,
		Published:        false,
	})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("error = %v, want ErrInvalidArgument", err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// FailSnapshot
// ─────────────────────────────────────────────────────────────────────────────

func TestFailSnapshotRecordsTheReason(t *testing.T) {
	f := newFixture(t)

	row := f.beginTemplate("")
	reason := mustJSON(t, map[string]any{"message": "build failed", "step": "RUN apt-get"})
	out, err := f.store.FailSnapshot(f.ctx, FailInput{
		ClusterID:   f.cluster,
		SnapshotID:  row.SnapshotID,
		BuildError:  reason,
		UpdatedAtMs: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("fail: %v", err)
	}
	if out.Rejected != nil {
		t.Fatalf("refused: %s", out.Rejected.Reason)
	}
	if out.Row.Status != StatusError || out.Row.StatusGroup != StatusGroupFailed {
		t.Fatalf("status = %q/%q", out.Row.Status, out.Row.StatusGroup)
	}
	// The reason comes back as the caller wrote it. JSONB reorders keys, so
	// this compares the decoded document rather than the bytes — what must not
	// change is the content, and nothing here has an opinion about it.
	var got, want map[string]any
	if err := json.Unmarshal(out.Row.BuildError, &got); err != nil {
		t.Fatalf("decode the reason that came back: %v", err)
	}
	if err := json.Unmarshal(reason, &want); err != nil {
		t.Fatalf("decode the reason sent: %v", err)
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("reason = %v, want %v", got, want)
	}
}

func TestFailSnapshotRefusesACommittedRow(t *testing.T) {
	f := newFixture(t)

	row := f.readyTemplate("")
	out, err := f.store.FailSnapshot(f.ctx, FailInput{
		ClusterID:  f.cluster,
		SnapshotID: row.SnapshotID,
		BuildError: mustJSON(t, map[string]string{"message": "no"}),
	})
	if err != nil {
		t.Fatalf("fail: %v", err)
	}
	// 🔴 A committed snapshot has a payload and can be started. Recording it as
	// a failure would be a lie the user acts on.
	if out.Rejected == nil || out.Rejected.Reason != RejectionStatusMismatch {
		t.Fatalf("outcome = %+v, want STATUS_MISMATCH", out)
	}
	if out.Rejected.ObservedStatus != StatusReady {
		t.Fatalf("observed = %q", out.Rejected.ObservedStatus)
	}
}

func TestFailSnapshotIsRetryable(t *testing.T) {
	f := newFixture(t)

	row := f.beginTemplate("")
	reason := mustJSON(t, map[string]string{"message": "first"})
	for i := 0; i < 2; i++ {
		out, err := f.store.FailSnapshot(f.ctx, FailInput{ClusterID: f.cluster, SnapshotID: row.SnapshotID, BuildError: reason})
		if err != nil {
			t.Fatalf("fail %d: %v", i, err)
		}
		if out.Rejected != nil {
			t.Fatalf("a retry after a lost response was refused: %s", out.Rejected.Reason)
		}
	}
}

func TestFailSnapshotRequiresAReason(t *testing.T) {
	f := newFixture(t)

	row := f.beginTemplate("")
	for _, bad := range []json.RawMessage{nil, json.RawMessage(`null`), json.RawMessage(`"boom"`)} {
		_, err := f.store.FailSnapshot(f.ctx, FailInput{ClusterID: f.cluster, SnapshotID: row.SnapshotID, BuildError: bad})
		if !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("reason %q was accepted: %v", bad, err)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Reads
// ─────────────────────────────────────────────────────────────────────────────

func TestGetSnapshotTakesAnIdOrAnAlias(t *testing.T) {
	f := newFixture(t)

	row := f.readyTemplate("by-name")

	byID, err := f.store.GetSnapshot(f.ctx, f.cluster, row.SnapshotID, ReadOptions{OnlyReady: true})
	if err != nil || byID == nil {
		t.Fatalf("get by id: %v %v", byID, err)
	}
	byAlias, err := f.store.GetSnapshot(f.ctx, f.cluster, "by-name", ReadOptions{OnlyReady: true})
	if err != nil || byAlias == nil {
		t.Fatalf("get by alias: %v %v", byAlias, err)
	}
	if byID.SnapshotID != byAlias.SnapshotID {
		t.Fatalf("the two reads found different rows: %s and %s", byID.SnapshotID, byAlias.SnapshotID)
	}

	missing, err := f.store.GetSnapshot(f.ctx, f.cluster, newUUID(t), ReadOptions{OnlyReady: true})
	if err != nil {
		t.Fatalf("get a missing id: %v", err)
	}
	if missing != nil {
		t.Fatal("an id nothing has returned a row")
	}
}

// TestGetSnapshotReachesAnAliasThatLooksLikeAUUID is why the store tries the id
// first and then the alias rather than choosing by shape.
//
// A snapshot alias is ASCII letters, digits, hyphens and underscores, so a
// perfectly legal alias can be spelled exactly like a uuid. Dispatching on the
// shape of the string would make that alias unreachable and the caller would be
// told the snapshot does not exist.
func TestGetSnapshotReachesAnAliasThatLooksLikeAUUID(t *testing.T) {
	f := newFixture(t)

	aliasThatLooksLikeAnID := newUUID(t)
	row := f.readyTemplate(aliasThatLooksLikeAnID)

	got, err := f.store.GetSnapshot(f.ctx, f.cluster, aliasThatLooksLikeAnID, ReadOptions{OnlyReady: true})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got == nil || got.SnapshotID != row.SnapshotID {
		t.Fatalf("a uuid-shaped alias did not resolve: %v", got)
	}
}

// TestReadyPredicateHidesASnapshotStillUploading is §5.3's rule seen from the
// outside: it is what stops a half-uploaded snapshot from starting a VM.
func TestReadyPredicateHidesASnapshotStillUploading(t *testing.T) {
	f := newFixture(t)

	building := f.begin(BeginInput{Status: StatusBuilding, Alias: "half-done"})

	for _, probe := range []struct {
		name string
		read func(onlyReady bool) (bool, error)
	}{
		{
			name: "get by id",
			read: func(onlyReady bool) (bool, error) {
				row, err := f.store.GetSnapshot(f.ctx, f.cluster, building.SnapshotID, ReadOptions{OnlyReady: onlyReady})
				return row != nil, err
			},
		},
		{
			name: "get by alias",
			read: func(onlyReady bool) (bool, error) {
				row, err := f.store.GetSnapshot(f.ctx, f.cluster, "half-done", ReadOptions{OnlyReady: onlyReady})
				return row != nil, err
			},
		},
		{
			name: "resolve alias",
			read: func(onlyReady bool) (bool, error) {
				target, err := f.store.ResolveAlias(f.ctx, f.cluster, "half-done", onlyReady)
				return target != nil, err
			},
		},
		{
			name: "list",
			read: func(onlyReady bool) (bool, error) {
				page, err := f.store.ListSnapshots(f.ctx, ListInput{ClusterID: f.cluster, ReadOptions: ReadOptions{OnlyReady: onlyReady}})
				return len(page.Rows) > 0, err
			},
		},
	} {
		t.Run(probe.name, func(t *testing.T) {
			visible, err := probe.read(true)
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			if visible {
				t.Fatal("a snapshot still uploading resolved under the ready predicate")
			}
			// 🔴 The control. Without it, "not visible" would also pass if the
			// read were broken, the alias wrong, or the row never written.
			visible, err = probe.read(false)
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			if !visible {
				t.Fatal("the row is invisible without the predicate too: this probe proves nothing")
			}
		})
	}
}

func TestResolveAliasProjectsTheOriginBlock(t *testing.T) {
	f := newFixture(t)

	opened := f.begin(BeginInput{
		SourceKind: SourceKindSandbox, SourceSandboxID: "sbx-1",
		Status: StatusBuilding, Published: false, OriginNodeID: "node-a", Alias: "pinned",
	})
	f.commit(CommitInput{SnapshotID: opened.SnapshotID, Alias: "pinned", Published: false, OriginNodeID: "node-a"})

	// 🔴 It resolves. An unpublished snapshot is a complete snapshot, and the
	// alias naming it must not stop working just because only one node can
	// start it.
	target, err := f.store.ResolveAlias(f.ctx, f.cluster, "pinned", true)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if target == nil {
		t.Fatal("an alias on an unpublished snapshot resolved to nothing")
	}
	if target.Published || target.OriginNodeID != "node-a" {
		t.Fatalf("the origin block did not travel: %+v", target)
	}
	if allowed, pin := PinAliasTarget(*target, "node-b"); allowed || pin != "node-a" {
		t.Fatalf("a resume elsewhere was allowed: allowed=%v pin=%q", allowed, pin)
	}

	missing, err := f.store.ResolveAlias(f.ctx, f.cluster, "nothing-here", true)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if missing != nil {
		t.Fatal("an alias nothing holds resolved")
	}
}

func TestReadsAreScopedToTheirCluster(t *testing.T) {
	f := newFixture(t)

	row := f.readyTemplate("shared-name")
	other := "22222222-2222-2222-2222-222222222222"

	got, err := f.store.GetSnapshot(f.ctx, other, row.SnapshotID, ReadOptions{})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got != nil {
		t.Fatal("a snapshot was readable from another cluster")
	}
	target, err := f.store.ResolveAlias(f.ctx, other, "shared-name", true)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if target != nil {
		t.Fatal("an alias was resolvable from another cluster")
	}
	page, err := f.store.ListSnapshots(f.ctx, ListInput{ClusterID: other, ReadOptions: ReadOptions{OnlyReady: true}})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(page.Rows) != 0 {
		t.Fatalf("another cluster listed %d rows", len(page.Rows))
	}
}

func TestWithBuildProjectsTheNewestBuild(t *testing.T) {
	f := newFixture(t)

	template := f.beginTemplate("")
	started := time.Now().UnixMilli()
	out, err := f.store.StartBuild(f.ctx, StartBuildInput{
		ClusterID: f.cluster, NodeID: "node-a",
		BuildID: template.SnapshotID, TemplateID: template.SnapshotID,
		StartedAtMs: started,
	})
	if err != nil || out.Rejected != nil {
		t.Fatalf("start build: %v %+v", err, out.Rejected)
	}

	without, err := f.store.GetSnapshot(f.ctx, f.cluster, template.SnapshotID, ReadOptions{})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if without.BuildStartedAtMs != nil {
		t.Fatal("a read that did not ask for the build got one")
	}

	with, err := f.store.GetSnapshot(f.ctx, f.cluster, template.SnapshotID, ReadOptions{WithBuild: true})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if with.BuildStartedAtMs == nil || *with.BuildStartedAtMs != started {
		t.Fatalf("build started = %v, want %d", with.BuildStartedAtMs, started)
	}
	if with.BuildFinishedAtMs != nil {
		t.Fatalf("a running build reported a finish: %v", with.BuildFinishedAtMs)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Keyset pagination
// ─────────────────────────────────────────────────────────────────────────────

// seedPage writes n ready snapshots across a small number of distinct
// timestamps.
//
// 🔴 Few timestamps on purpose. The keyset comparison's whole difficulty is the
// tie-break: rows sharing a created_at are ordered by id in the *opposite*
// direction, and a comparison written the obvious way still returns rows — it
// returns a page that has quietly skipped some of them. Spreading n rows over
// n timestamps would never exercise it.
func (f *fixture) seedPage(n int, distinctTimestamps int) []SnapshotRow {
	f.t.Helper()

	base := time.Now().UnixMilli()
	rows := make([]SnapshotRow, 0, n)
	for i := 0; i < n; i++ {
		row := f.begin(BeginInput{
			Status:      StatusBuilding,
			CreatedAtMs: base + int64(i%distinctTimestamps),
		})
		committed := f.commit(CommitInput{SnapshotID: row.SnapshotID, Published: true, UpdatedAtMs: row.CreatedAtMs})
		rows = append(rows, *committed)
	}
	return rows
}

// expectedOrder is created_at descending, id ascending as text — the order the
// public pagination cursor defines.
func expectedOrder(rows []SnapshotRow) []string {
	sorted := append([]SnapshotRow(nil), rows...)
	sortRows(sorted)
	ids := make([]string, 0, len(sorted))
	for _, r := range sorted {
		ids = append(ids, r.SnapshotID)
	}
	return ids
}

func sortRows(rows []SnapshotRow) {
	for i := 1; i < len(rows); i++ {
		for j := i; j > 0 && less(rows[j], rows[j-1]); j-- {
			rows[j], rows[j-1] = rows[j-1], rows[j]
		}
	}
}

func less(a, b SnapshotRow) bool {
	if a.CreatedAtMs != b.CreatedAtMs {
		return a.CreatedAtMs > b.CreatedAtMs
	}
	return a.SnapshotID < b.SnapshotID
}

// drain walks every page and returns the ids in the order they arrived.
func (f *fixture) drain(limit uint32, between func(page int)) []string {
	f.t.Helper()

	var (
		ids    []string
		cursor *Cursor
		pages  int
	)
	for {
		page, err := f.store.ListSnapshots(f.ctx, ListInput{
			ClusterID:   f.cluster,
			Cursor:      cursor,
			Limit:       limit,
			ReadOptions: ReadOptions{OnlyReady: true},
		})
		if err != nil {
			f.t.Fatalf("list: %v", err)
		}
		for _, row := range page.Rows {
			ids = append(ids, row.SnapshotID)
		}
		pages++
		if between != nil {
			between(pages)
		}
		if page.Next == nil {
			return ids
		}
		if pages > 1000 {
			f.t.Fatal("pagination did not terminate")
		}
		cursor = page.Next
	}
}

func TestKeysetPaginationVisitsEveryRowExactlyOnce(t *testing.T) {
	f := newFixture(t)

	rows := f.seedPage(300, 5)
	got := f.drain(7, nil)

	want := expectedOrder(rows)
	if len(got) != len(want) {
		t.Fatalf("walked %d rows, want %d", len(got), len(want))
	}
	seen := make(map[string]int, len(got))
	for _, id := range got {
		seen[id]++
	}
	for id, count := range seen {
		if count != 1 {
			t.Fatalf("snapshot %s appeared %d times", id, count)
		}
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("at position %d the page walk gave %s, want %s: "+
				"the keyset order and created_at DESC, id ASC have come apart", i, got[i], want[i])
		}
	}
}

// TestPaginationDoesNotSeeRowsWrittenBehindIt is the property offset pagination
// does not have, and the reason keyset is worth the trouble.
func TestPaginationDoesNotSeeRowsWrittenBehindIt(t *testing.T) {
	f := newFixture(t)

	base := time.Now().UnixMilli()
	original := make([]string, 0, 40)
	for i := 0; i < 40; i++ {
		row := f.begin(BeginInput{Status: StatusBuilding, CreatedAtMs: base + int64(i)})
		f.commit(CommitInput{SnapshotID: row.SnapshotID, Published: true})
		original = append(original, row.SnapshotID)
	}

	var inserted []string
	got := f.drain(10, func(page int) {
		if page != 1 {
			return
		}
		// One row inside the region already walked — offset pagination misses a
		// row here — and one newer than anything — offset pagination repeats a
		// row there.
		behind := f.begin(BeginInput{Status: StatusBuilding, CreatedAtMs: base + 35})
		f.commit(CommitInput{SnapshotID: behind.SnapshotID, Published: true})
		ahead := f.begin(BeginInput{Status: StatusBuilding, CreatedAtMs: base + 1000})
		f.commit(CommitInput{SnapshotID: ahead.SnapshotID, Published: true})
		inserted = append(inserted, behind.SnapshotID, ahead.SnapshotID)
	})

	if len(got) != len(original) {
		t.Fatalf("the walk returned %d rows, want the %d that existed when it started", len(got), len(original))
	}
	for _, id := range inserted {
		for _, seen := range got {
			if seen == id {
				t.Fatalf("a row written during the walk (%s) appeared in it", id)
			}
		}
	}
}

func TestPaginationEndsWithoutACursorAndNotEarly(t *testing.T) {
	f := newFixture(t)

	f.seedPage(20, 3)

	first, err := f.store.ListSnapshots(f.ctx, ListInput{ClusterID: f.cluster, Limit: 10, ReadOptions: ReadOptions{OnlyReady: true}})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(first.Rows) != 10 || first.Next == nil {
		t.Fatalf("first page: %d rows, next=%v", len(first.Rows), first.Next)
	}

	// 🔴 The second page is exactly full and is the last one. Deciding "there
	// is more" by comparing the page size to the limit ends every listing whose
	// total is a multiple of the limit one page early, silently.
	second, err := f.store.ListSnapshots(f.ctx, ListInput{ClusterID: f.cluster, Cursor: first.Next, Limit: 10, ReadOptions: ReadOptions{OnlyReady: true}})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(second.Rows) != 10 {
		t.Fatalf("second page: %d rows, want 10", len(second.Rows))
	}
	if second.Next != nil {
		t.Fatalf("a full last page reported another one: %+v", second.Next)
	}
}

func TestListingCapsWhatACallerMayAskFor(t *testing.T) {
	f := newFixture(t)

	// 🔴 One row more than the default page, and that is the point of the
	// number. Twelve rows could not tell "the default is a hundred" from "no
	// limit at all" — every listing returned all twelve either way — so the
	// property that stops one request pulling the whole catalog into memory was
	// asserted by a page smaller than any limit it could have hit.
	f.seedPage(int(defaultListLimit)+1, 3)

	// Zero is the default, not "every row".
	page, err := f.store.ListSnapshots(f.ctx, ListInput{ClusterID: f.cluster, ReadOptions: ReadOptions{OnlyReady: true}})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(page.Rows) != int(defaultListLimit) {
		t.Fatalf("the default page held %d rows, want %d: a request for no limit is not a request for all of them",
			len(page.Rows), defaultListLimit)
	}
	if page.Next == nil {
		t.Fatal("the default page reported no continuation, with a row left over")
	}

	page, err = f.store.ListSnapshots(f.ctx, ListInput{ClusterID: f.cluster, Limit: 5, ReadOptions: ReadOptions{OnlyReady: true}})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(page.Rows) != 5 {
		t.Fatalf("limit 5 returned %d rows", len(page.Rows))
	}
	if page.Next == nil {
		t.Fatal("a limited page reported no continuation")
	}

	// 🔴 And the ceiling this test is named for, which nothing above it went
	// anywhere near. Seeding a thousand and one rows to watch a thousand come
	// back would buy the assertion at the price of the slowest test in the
	// package; the ceiling is arithmetic, and the statement carries the result
	// of it — `LIMIT` is bound as the page size plus the one row that says
	// there is another page.
	if got := clampLimit(maxListLimit + 1); got != maxListLimit {
		t.Fatalf("a request for %d rows was resolved to %d, want the ceiling %d",
			maxListLimit+1, got, maxListLimit)
	}
	if got := clampLimit(maxListLimit * 10); got != maxListLimit {
		t.Fatalf("a request for %d rows was resolved to %d, want the ceiling %d",
			maxListLimit*10, got, maxListLimit)
	}
	_, args, err := listSnapshotsSQL(
		ListInput{ClusterID: f.cluster, ReadOptions: ReadOptions{OnlyReady: true}},
		clampLimit(maxListLimit*10))
	if err != nil {
		t.Fatalf("build the listing statement: %v", err)
	}
	if got := args[len(args)-1]; got != int64(maxListLimit)+1 {
		t.Fatalf("the capped statement binds LIMIT %v, want %d", got, int64(maxListLimit)+1)
	}
}

// TestListingShowsSnapshotsThatFailedToPublish is rule V5 seen from the
// outside. Filtering them out would tell a user a snapshot they can resume on
// its origin node does not exist.
func TestListingShowsSnapshotsThatFailedToPublish(t *testing.T) {
	f := newFixture(t)

	opened := f.begin(BeginInput{
		SourceKind: SourceKindSandbox, SourceSandboxID: "sbx-1",
		Status: StatusBuilding, Published: false, OriginNodeID: "node-a",
	})
	f.commit(CommitInput{SnapshotID: opened.SnapshotID, Published: false, OriginNodeID: "node-a"})

	page, err := f.store.ListSnapshots(f.ctx, ListInput{
		ClusterID:   f.cluster,
		Filter:      Filter{SourceKinds: []string{SourceKindSandbox}},
		ReadOptions: ReadOptions{OnlyReady: true},
	})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(page.Rows) != 1 {
		t.Fatalf("an unpublished snapshot was hidden from the listing: %d rows", len(page.Rows))
	}
	if page.Rows[0].Published || page.Rows[0].OriginNodeID != "node-a" {
		t.Fatalf("the origin block did not travel with the listing: %+v", page.Rows[0])
	}
}

func TestListingFilters(t *testing.T) {
	f := newFixture(t)

	sandbox := f.begin(BeginInput{
		SourceKind: SourceKindSandbox, SourceSandboxID: "sbx-alpha",
		Status: StatusBuilding, Alias: "alpha-one",
	})
	f.commit(CommitInput{SnapshotID: sandbox.SnapshotID, Published: true})

	otherSandbox := f.begin(BeginInput{
		SourceKind: SourceKindSandbox, SourceSandboxID: "sbx-beta",
		Status: StatusBuilding, Alias: "beta-one",
	})
	f.commit(CommitInput{SnapshotID: otherSandbox.SnapshotID, Published: true})

	template := f.begin(BeginInput{Status: StatusBuilding, Alias: "alpha-two"})
	f.commit(CommitInput{SnapshotID: template.SnapshotID, Published: true})

	list := func(filter Filter, onlyReady bool) []string {
		page, err := f.store.ListSnapshots(f.ctx, ListInput{
			ClusterID: f.cluster, Filter: filter, ReadOptions: ReadOptions{OnlyReady: onlyReady},
		})
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		ids := make([]string, 0, len(page.Rows))
		for _, row := range page.Rows {
			ids = append(ids, row.SnapshotID)
		}
		return ids
	}

	if got := list(Filter{SourceKinds: []string{SourceKindTemplate}}, true); len(got) != 1 || got[0] != template.SnapshotID {
		t.Fatalf("source kind filter gave %v", got)
	}
	sandboxID := "sbx-alpha"
	if got := list(Filter{SourceSandboxID: &sandboxID}, true); len(got) != 1 || got[0] != sandbox.SnapshotID {
		t.Fatalf("source sandbox filter gave %v", got)
	}
	prefix := "alpha"
	got := list(Filter{AliasPrefix: &prefix}, true)
	if len(got) != 2 {
		t.Fatalf("alias prefix filter gave %v, want the two alpha aliases", got)
	}
	if got := list(Filter{SnapshotIDs: []string{sandbox.SnapshotID, template.SnapshotID}}, true); len(got) != 2 {
		t.Fatalf("id filter gave %v", got)
	}
	exact := "beta-one"
	if got := list(Filter{SnapshotIDOrAlias: &exact}, true); len(got) != 1 || got[0] != otherSandbox.SnapshotID {
		t.Fatalf("alias union filter gave %v", got)
	}
	byID := template.SnapshotID
	if got := list(Filter{SnapshotIDOrAlias: &byID}, true); len(got) != 1 || got[0] != template.SnapshotID {
		t.Fatalf("id union filter gave %v", got)
	}

	// 🔴 Template statuses only mean anything without the ready predicate:
	// asking for `waiting` rows under it is a filter that can never match, and
	// a caller reading the empty page as "there are none" is wrong.
	waiting := f.beginTemplate("")
	if got := list(Filter{TemplateStatuses: []string{StatusWaiting}}, false); len(got) != 1 || got[0] != waiting.SnapshotID {
		t.Fatalf("template status filter gave %v", got)
	}
	if got := list(Filter{TemplateStatuses: []string{StatusWaiting}}, true); len(got) != 0 {
		t.Fatalf("a waiting row matched under the ready predicate: %v", got)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Delete
// ─────────────────────────────────────────────────────────────────────────────

func TestDeleteSnapshotRetiresTheRowAndReleasesItsName(t *testing.T) {
	f := newFixture(t)

	row := f.readyTemplate("retire-me")
	at := time.Now().UnixMilli()

	deleted, err := f.store.DeleteSnapshot(f.ctx, f.cluster, row.SnapshotID, at)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if !deleted.Deleted || deleted.Row == nil {
		t.Fatalf("delete reported %+v", deleted)
	}
	// The row that comes back is the one before the delete, so the caller can
	// still find the artifacts to collect from it.
	if deleted.Row.Alias != "retire-me" || len(deleted.Row.CommittedPayload) == 0 {
		t.Fatalf("the returned row lost what the caller needs: %+v", deleted.Row)
	}

	for _, probe := range []struct {
		name string
		seen func() bool
	}{
		{"get by id", func() bool {
			got, err := f.store.GetSnapshot(f.ctx, f.cluster, row.SnapshotID, ReadOptions{})
			if err != nil {
				t.Fatalf("get: %v", err)
			}
			return got != nil
		}},
		{"resolve alias", func() bool {
			target, err := f.store.ResolveAlias(f.ctx, f.cluster, "retire-me", false)
			if err != nil {
				t.Fatalf("resolve: %v", err)
			}
			return target != nil
		}},
		{"list", func() bool {
			page, err := f.store.ListSnapshots(f.ctx, ListInput{ClusterID: f.cluster})
			if err != nil {
				t.Fatalf("list: %v", err)
			}
			return len(page.Rows) > 0
		}},
	} {
		if probe.seen() {
			t.Fatalf("%s still sees a deleted snapshot", probe.name)
		}
	}

	// 🔴 The alias row is gone, not merely hidden. The foreign key's cascade
	// fires on a hard delete and this one is soft, so a name reserved by a row
	// no read can reach would otherwise never be released.
	rebound := f.begin(BeginInput{Alias: "retire-me"})
	if rebound.Alias != "retire-me" {
		t.Fatalf("the deleted snapshot's alias was not released: %q", rebound.Alias)
	}

	// And the template half followed. Two soft-delete flags that can disagree
	// is how a reader consulting the wrong one shows a deleted template.
	var templateDeleted *int64
	if err := f.pool.QueryRow(f.ctx, `SELECT deleted_at_ms FROM templates WHERE id = $1::uuid`, row.SnapshotID).Scan(&templateDeleted); err != nil {
		t.Fatalf("read templates: %v", err)
	}
	if templateDeleted == nil || *templateDeleted != at {
		t.Fatalf("the templates row was not soft-deleted with the snapshot: %v", templateDeleted)
	}
	// The view hides it, so nothing reading through the view can forget.
	var visible int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM active_templates WHERE id = $1::uuid`, row.SnapshotID).Scan(&visible); err != nil {
		t.Fatalf("read active_templates: %v", err)
	}
	if visible != 0 {
		t.Fatal("active_templates still shows a deleted template")
	}
}

// 🔴 What a raw `count(*) FROM templates` is, and is not, evidence of.
//
// Reported from the dev cluster as a possible leak: 17 rows in `templates`
// while object storage held 2 templates and every test template had been
// deleted. It is not a leak. The catalog's delete is *soft* — a hard delete
// would take the alias, and through it an image reference somebody exported to
// a registry, with nothing left to trace — so a deleted template keeps its row
// with `deleted_at_ms` set. The raw table therefore counts every template the
// cluster has ever had; `active_templates` is the live view, and it is what a
// count meant as "how many templates are there" has to read.
//
// The foreign key makes the other half of the answer structural: a `templates`
// row cannot exist without its `snapshots` row, so there are no orphans to
// leak. This holds all of that still, so the same reading does not get
// re-reported as a defect.
func TestDeletedTemplatesStayCountedInTheRawTableAndNotInTheView(t *testing.T) {
	f := newFixture(t)

	live := []*SnapshotRow{f.readyTemplate("kept-one"), f.readyTemplate("kept-two")}
	retired := []*SnapshotRow{
		f.readyTemplate("gone-one"),
		f.readyTemplate("gone-two"),
		f.readyTemplate("gone-three"),
	}
	// A sandbox snapshot for contrast: it never gets a `templates` row at all.
	f.begin(BeginInput{SourceKind: SourceKindSandbox, SourceSandboxID: "sbx-1"})

	for _, row := range retired {
		if _, err := f.store.DeleteSnapshot(f.ctx, f.cluster, row.SnapshotID, time.Now().UnixMilli()); err != nil {
			t.Fatalf("delete: %v", err)
		}
	}

	count := func(query string) int {
		t.Helper()
		var n int
		if err := f.pool.QueryRow(f.ctx, query).Scan(&n); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		return n
	}

	if raw := count(`SELECT count(*) FROM templates`); raw != len(live)+len(retired) {
		t.Fatalf("templates rows = %d, want %d: the raw table keeps the history", raw, len(live)+len(retired))
	}
	if active := count(`SELECT count(*) FROM active_templates`); active != len(live) {
		t.Fatalf("active_templates rows = %d, want %d", active, len(live))
	}

	// 🔴 No orphan, and no disagreement. Two soft-delete flags for one entity
	// can drift, and the reader consulting the wrong one shows a deleted
	// template; `snapshots.deleted_at_ms` is the authoritative one and this is
	// what says the other still follows it.
	if orphans := count(`
SELECT count(*)
  FROM templates t
  LEFT JOIN snapshots s ON s.id = t.id
 WHERE s.id IS NULL
    OR (s.deleted_at_ms IS NULL) <> (t.deleted_at_ms IS NULL)`); orphans != 0 {
		t.Fatalf("%d templates rows have no snapshots row or disagree with it about deletion", orphans)
	}

	// And a template row exists for exactly the template snapshots, so the raw
	// count is the all-time template count rather than anything else.
	if mismatched := count(`
SELECT count(*)
  FROM snapshots s
 WHERE s.source_kind = 'template'
   AND NOT EXISTS (SELECT 1 FROM templates t WHERE t.id = s.id)`); mismatched != 0 {
		t.Fatalf("%d template snapshots have no templates row", mismatched)
	}
	if sandboxes := count(`
SELECT count(*)
  FROM templates t
  JOIN snapshots s ON s.id = t.id
 WHERE s.source_kind <> 'template'`); sandboxes != 0 {
		t.Fatalf("%d templates rows belong to snapshots that are not templates", sandboxes)
	}
}

func TestDeleteSnapshotIsIdempotent(t *testing.T) {
	f := newFixture(t)

	row := f.readyTemplate("gone")
	if _, err := f.store.DeleteSnapshot(f.ctx, f.cluster, row.SnapshotID, time.Now().UnixMilli()); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// 🔴 A success reporting nothing done, not a refusal: the state the caller
	// wanted is the state the table is in.
	again, err := f.store.DeleteSnapshot(f.ctx, f.cluster, row.SnapshotID, time.Now().UnixMilli())
	if err != nil {
		t.Fatalf("delete again: %v", err)
	}
	if again.Deleted || again.Row != nil {
		t.Fatalf("the second delete reported %+v", again)
	}

	missing, err := f.store.DeleteSnapshot(f.ctx, f.cluster, newUUID(t), time.Now().UnixMilli())
	if err != nil {
		t.Fatalf("delete a missing id: %v", err)
	}
	if missing.Deleted {
		t.Fatal("deleting an id nothing has reported a deletion")
	}
}

func TestDeleteSnapshotTakesAnAlias(t *testing.T) {
	f := newFixture(t)

	row := f.readyTemplate("by-alias")
	deleted, err := f.store.DeleteSnapshot(f.ctx, f.cluster, "by-alias", time.Now().UnixMilli())
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if !deleted.Deleted || deleted.Row.SnapshotID != row.SnapshotID {
		t.Fatalf("delete by alias reported %+v", deleted)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Build admission
// ─────────────────────────────────────────────────────────────────────────────

func (f *fixture) startBuild(templateID, buildID, node string, at int64) (StartBuildOutcome, error) {
	f.t.Helper()

	if buildID == "" {
		buildID = templateID
	}
	if at == 0 {
		at = time.Now().UnixMilli()
	}
	return f.store.StartBuild(f.ctx, StartBuildInput{
		ClusterID: f.cluster, NodeID: node,
		BuildID: buildID, TemplateID: templateID,
		StartedAtMs: at,
	})
}

// ageBuildHeartbeat pushes a build's last heartbeat into the past.
//
// 🔴 This is how a test makes a build look stranded, and there is deliberately
// no other way. The heartbeat and the reaper's threshold both come from the
// database's clock, so nothing a caller passes can move either — which is the
// property under test as much as it is an inconvenience here. A stranded build
// *is* a row whose heartbeat is old, and that is what this writes.
func (f *fixture) ageBuildHeartbeat(buildID string, by time.Duration) {
	f.t.Helper()

	tag, err := f.pool.Exec(f.ctx,
		`UPDATE builds SET heartbeat_at_ms = heartbeat_at_ms - $2 WHERE id = $1::uuid`,
		buildID, by.Milliseconds())
	if err != nil {
		f.t.Fatalf("age the heartbeat of build %s: %v", buildID, err)
	}
	if tag.RowsAffected() != 1 {
		f.t.Fatalf("aging build %s touched %d rows", buildID, tag.RowsAffected())
	}
}

func TestStartBuildAdmitsOneAndMovesTheTemplate(t *testing.T) {
	f := newFixture(t)

	template := f.beginTemplate("")
	out, err := f.startBuild(template.SnapshotID, "", "node-a", 0)
	if err != nil {
		t.Fatalf("start build: %v", err)
	}
	if out.Rejected != nil {
		t.Fatalf("refused: %+v", out.Rejected)
	}
	if out.Build.Status != StatusBuilding || out.Build.StatusGroup != StatusGroupInProgress {
		t.Fatalf("build status = %q/%q", out.Build.Status, out.Build.StatusGroup)
	}
	if out.Build.NodeID != "node-a" {
		t.Fatalf("node = %q", out.Build.NodeID)
	}
	if out.Build.HeartbeatAtMs == nil {
		t.Fatal("a build was admitted without a heartbeat: nothing could ever reap it")
	}
	if out.Snapshot.Status != StatusBuilding {
		t.Fatalf("the template row is %q, want %q", out.Snapshot.Status, StatusBuilding)
	}
}

func TestStartBuildRefusesASecondLiveBuildForOneTemplate(t *testing.T) {
	f := newFixture(t)

	template := f.beginTemplate("")
	first, err := f.startBuild(template.SnapshotID, "", "node-a", 0)
	if err != nil || first.Rejected != nil {
		t.Fatalf("first build: %v %+v", err, first.Rejected)
	}

	second, err := f.startBuild(template.SnapshotID, newUUID(t), "node-b", 0)
	if err != nil {
		t.Fatalf("second build: %v", err)
	}
	if second.Rejected == nil || second.Rejected.Reason != RejectionBuildInProgress {
		t.Fatalf("outcome = %+v, want BUILD_IN_PROGRESS", second)
	}
	if second.Rejected.ActiveBuildID != first.Build.BuildID {
		t.Fatalf("the refusal named build %q, want %q", second.Rejected.ActiveBuildID, first.Build.BuildID)
	}
}

// TestStartBuildIsExclusiveUnderConcurrency is the read-modify-write the
// object-store backend still has, closed here by a partial unique index and an
// advisory lock rather than by hoping.
func TestStartBuildIsExclusiveUnderConcurrency(t *testing.T) {
	f := newFixture(t)

	template := f.beginTemplate("")

	const racers = 16
	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		admitted  int
		inProgres int
		failures  []error
	)
	start := make(chan struct{})
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			out, err := f.store.StartBuild(context.Background(), StartBuildInput{
				ClusterID: f.cluster, NodeID: fmt.Sprintf("node-%d", i),
				BuildID: newUUID(t), TemplateID: template.SnapshotID,
				StartedAtMs: time.Now().UnixMilli(),
			})
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err != nil:
				failures = append(failures, err)
			case out.Rejected == nil:
				admitted++
			case out.Rejected.Reason == RejectionBuildInProgress:
				inProgres++
			default:
				failures = append(failures, fmt.Errorf("unexpected refusal %s", out.Rejected.Reason))
			}
		}(i)
	}
	close(start)
	wg.Wait()

	if len(failures) > 0 {
		t.Fatalf("errors during the race: %v", failures)
	}
	if admitted != 1 {
		t.Fatalf("%d of %d builds were admitted, want exactly 1", admitted, racers)
	}
	if inProgres != racers-1 {
		t.Fatalf("%d refusals said BUILD_IN_PROGRESS, want %d", inProgres, racers-1)
	}

	var live int
	if err := f.pool.QueryRow(f.ctx,
		`SELECT count(*) FROM builds WHERE template_id = $1::uuid AND status_group IN ('pending','in_progress')`,
		template.SnapshotID).Scan(&live); err != nil {
		t.Fatalf("count: %v", err)
	}
	if live != 1 {
		t.Fatalf("%d live builds for one template", live)
	}
}

func TestStartBuildRefusesATemplateThatIsNotStartable(t *testing.T) {
	f := newFixture(t)

	ready := f.readyTemplate("")
	out, err := f.startBuild(ready.SnapshotID, newUUID(t), "node-a", 0)
	if err != nil {
		t.Fatalf("start build: %v", err)
	}
	// A published template is rebuilt under a new id, not in place.
	if out.Rejected == nil || out.Rejected.Reason != RejectionStatusMismatch {
		t.Fatalf("outcome = %+v, want STATUS_MISMATCH", out)
	}
	if out.Rejected.ObservedStatus != StatusReady {
		t.Fatalf("observed = %q", out.Rejected.ObservedStatus)
	}

	missing, err := f.startBuild(newUUID(t), newUUID(t), "node-a", 0)
	if err != nil {
		t.Fatalf("start build: %v", err)
	}
	if missing.Rejected == nil || missing.Rejected.Reason != RejectionNotFound {
		t.Fatalf("outcome = %+v, want NOT_FOUND", missing)
	}
}

// TestStartBuildRetriesATemplateWhoseLastBuildFailed is the other half of the
// fence: `error` is a state a build starts from, and refusing it would leave a
// template that failed once unbuildable forever.
func TestStartBuildRetriesATemplateWhoseLastBuildFailed(t *testing.T) {
	f := newFixture(t)

	template := f.beginTemplate("")
	if out, err := f.startBuild(template.SnapshotID, "", "node-a", 0); err != nil || out.Rejected != nil {
		t.Fatalf("first build: %v %+v", err, out.Rejected)
	}
	failed, err := f.store.FailSnapshot(f.ctx, FailInput{
		ClusterID: f.cluster, SnapshotID: template.SnapshotID,
		BuildError:      mustJSON(t, map[string]string{"message": "step failed"}),
		FailActiveBuild: true,
	})
	if err != nil || failed.Rejected != nil {
		t.Fatalf("fail: %v %+v", err, failed.Rejected)
	}

	out, err := f.startBuild(template.SnapshotID, newUUID(t), "node-a", 0)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if out.Rejected != nil {
		t.Fatalf("a retry after a failed build was refused: %+v", out.Rejected)
	}
	// The retry cleared the previous reason, so the row does not carry an error
	// it is no longer in.
	if out.Snapshot.BuildError != nil {
		t.Fatalf("the retried template still carries %s", out.Snapshot.BuildError)
	}
}

func TestStartBuildStopsAtTheClusterCeiling(t *testing.T) {
	f := newFixture(t, withBuildCeiling(2))

	for i := 0; i < 2; i++ {
		template := f.beginTemplate("")
		if out, err := f.startBuild(template.SnapshotID, "", "node-a", 0); err != nil || out.Rejected != nil {
			t.Fatalf("build %d: %v %+v", i, err, out.Rejected)
		}
	}

	third := f.beginTemplate("")
	out, err := f.startBuild(third.SnapshotID, "", "node-a", 0)
	if err != nil {
		t.Fatalf("third build: %v", err)
	}
	if out.Rejected == nil || out.Rejected.Reason != RejectionBuildQueueFull {
		t.Fatalf("outcome = %+v, want BUILD_QUEUE_FULL", out)
	}
	// 🔴 Refused, and nothing left behind: the template must not be sitting at
	// `building` with no build to move it on.
	after, err := f.store.GetSnapshot(f.ctx, f.cluster, third.SnapshotID, ReadOptions{})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if after.Status != StatusWaiting {
		t.Fatalf("a refused admission left the template at %q", after.Status)
	}
}

// TestNoAdmittedBuildCanBeOneNothingCouldEverReap is the guard that used to be
// a check on an input field, made structural.
//
// 🔴 The row it keeps out is the worst one this table can hold: a build with no
// heartbeat is invisible to the reaper for ever, and `builds_one_active_per_
// template` turns it into a template nobody can build again for as long as the
// database lives. It used to be refused when a caller left the field unset,
// which meant the guarantee was only as good as every future caller. The
// admitting statement stamps the heartbeat itself now, so there is no input
// that can produce the row at all — this asserts that, over the surface a
// caller actually has.
// TestTheClusterCeilingHoldsUnderConcurrency is what the advisory lock is for,
// and the only test that can tell whether it is there.
//
// 🔴 Under READ COMMITTED two admissions cannot see each other's uncommitted
// rows, so "insert, then count" lets both through and a ceiling of two admits
// three. The sequential test above passes either way — it is this one that
// fails when the lock goes.
func TestTheClusterCeilingHoldsUnderConcurrency(t *testing.T) {
	const ceiling = 3
	f := newFixture(t, withBuildCeiling(ceiling))

	const racers = 12
	templates := make([]string, 0, racers)
	for i := 0; i < racers; i++ {
		templates = append(templates, f.beginTemplate("").SnapshotID)
	}

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		admitted int
		full     int
		failures []error
	)
	start := make(chan struct{})
	for i, template := range templates {
		wg.Add(1)
		go func(i int, template string) {
			defer wg.Done()
			<-start
			out, err := f.store.StartBuild(context.Background(), StartBuildInput{
				ClusterID: f.cluster, NodeID: fmt.Sprintf("node-%d", i),
				BuildID: newUUID(t), TemplateID: template,
				StartedAtMs: time.Now().UnixMilli(),
			})
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err != nil:
				failures = append(failures, err)
			case out.Rejected == nil:
				admitted++
			case out.Rejected.Reason == RejectionBuildQueueFull:
				full++
			default:
				failures = append(failures, fmt.Errorf("unexpected refusal %s", out.Rejected.Reason))
			}
		}(i, template)
	}
	close(start)
	wg.Wait()

	if len(failures) > 0 {
		t.Fatalf("errors during the race: %v", failures)
	}
	if admitted != ceiling {
		t.Fatalf("%d builds were admitted against a ceiling of %d: the count is not being taken under a lock", admitted, ceiling)
	}
	if full != racers-ceiling {
		t.Fatalf("%d admissions were refused for the ceiling, want %d", full, racers-ceiling)
	}
}

func TestNoAdmittedBuildCanBeOneNothingCouldEverReap(t *testing.T) {
	f := newFixture(t)

	for _, at := range []int64{0, 1, -1, time.Now().UnixMilli()} {
		template := f.beginTemplate("")
		out, err := f.store.StartBuild(f.ctx, StartBuildInput{
			ClusterID: f.cluster, NodeID: "node-a",
			BuildID: template.SnapshotID, TemplateID: template.SnapshotID,
			StartedAtMs: at,
		})
		if err != nil || out.Rejected != nil {
			t.Fatalf("started_at_ms=%d: %v %+v", at, err, out.Rejected)
		}
		if out.Build.HeartbeatAtMs == nil {
			t.Fatalf("started_at_ms=%d admitted a build with no heartbeat: nothing could ever reap it", at)
		}
		// And it is this machine's clock, not whatever the caller's timeline
		// said — the reaper judges it against the same one.
		if drift := time.Since(time.UnixMilli(*out.Build.HeartbeatAtMs)); drift > time.Minute || drift < -time.Minute {
			t.Fatalf("started_at_ms=%d stamped a heartbeat %s away from now", at, drift)
		}
	}

	template := f.beginTemplate("")
	_, err := f.store.StartBuild(f.ctx, StartBuildInput{
		ClusterID: f.cluster, BuildID: template.SnapshotID, TemplateID: template.SnapshotID,
		StartedAtMs: 1,
	})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("a build with no node was accepted: %v", err)
	}
}

// TestTheHeartbeatAxisIsOneClock is the reason the two build statements stopped
// taking a timestamp.
//
// 🔴 The bug it locks out. A heartbeat stamped by the node and a staleness
// threshold computed by whoever runs the reaping pass are two different
// machines' clocks, and their difference is indistinguishable from elapsed
// time. A node running five minutes slow had every build it ran reaped while it
// was still running — repeatedly, with the error saying its heartbeat lapsed
// when it had never missed one. Nothing on either side compares the clocks, so
// the only symptom is builds dying. Both ends come from the database now, and
// the surface no longer has anywhere to put a second clock.
func TestTheHeartbeatAxisIsOneClock(t *testing.T) {
	f := newFixture(t)

	template := f.beginTemplate("")
	// A caller whose whole timeline is a week behind everyone else's.
	behind := time.Now().Add(-7 * 24 * time.Hour).UnixMilli()
	out, err := f.startBuild(template.SnapshotID, "", "node-slow", behind)
	if err != nil || out.Rejected != nil {
		t.Fatalf("start: %v %+v", err, out.Rejected)
	}

	// The reaper would end anything unheard from for a minute. This build is a
	// week old by its own account and was admitted a moment ago by the
	// database's, and the database's is the one that counts.
	reaped, err := f.store.ReapExpiredBuilds(f.ctx, ReapInput{ClusterID: f.cluster, TTLMs: 60_000})
	if err != nil {
		t.Fatalf("reap: %v", err)
	}
	if len(reaped) != 0 {
		t.Fatalf("a build from a node whose clock is a week slow was reaped while it was still running: %+v", reaped)
	}

	// Same again for the renewal: a builder that reports a stale instant is
	// still a builder that just spoke.
	f.ageBuildHeartbeat(out.Build.BuildID, 10*time.Minute)
	live, err := f.store.RenewBuildLease(f.ctx, RenewBuildLeaseInput{
		ClusterID: f.cluster, NodeID: "node-slow", BuildID: out.Build.BuildID,
	})
	if err != nil || !live {
		t.Fatalf("renew: live=%v err=%v", live, err)
	}
	reaped, err = f.store.ReapExpiredBuilds(f.ctx, ReapInput{ClusterID: f.cluster, TTLMs: 60_000})
	if err != nil {
		t.Fatalf("reap: %v", err)
	}
	if len(reaped) != 0 {
		t.Fatalf("a build that had just renewed was reaped: %+v", reaped)
	}
}

// TestASuccessfulBuildComesOffTheQueue is the half of admission that only ever
// shows up as an outage, and only after twenty successes.
//
// 🔴 What it locks. Both the per-template exclusion and the cluster ceiling
// count exactly the `pending`/`in_progress` build rows. Nothing but this moves
// a build out of that group when it *works* — the reaper only touches rows
// whose heartbeat has lapsed, and the failure path is a different statement. So
// without it, every successful build in the cluster's life goes on holding a
// slot, the count creeps up as a monotone function of how well things are
// going, and one day every build in the cluster is refused with the queue full
// while nothing is building at all.
func TestASuccessfulBuildComesOffTheQueue(t *testing.T) {
	const ceiling = 3
	f := newFixture(t, withBuildCeiling(ceiling))

	// One more successful build than the ceiling. Every one of them starts,
	// commits, and must leave the queue empty behind it.
	for i := 0; i < ceiling+1; i++ {
		template := f.beginTemplate("")
		out, err := f.startBuild(template.SnapshotID, "", "node-a", 0)
		if err != nil {
			t.Fatalf("build %d: %v", i, err)
		}
		if out.Rejected != nil {
			t.Fatalf("build %d was refused with %s: a successful build is still holding its slot",
				i, out.Rejected.Reason)
		}

		size := uint32(10)
		f.commit(CommitInput{SnapshotID: template.SnapshotID, DiskSizeMiB: &size})

		build, err := f.store.GetBuild(f.ctx, f.cluster, out.Build.BuildID)
		if err != nil {
			t.Fatalf("get build %d: %v", i, err)
		}
		if build.StatusGroup != StatusGroupReady {
			t.Fatalf("build %d is %q/%q after its snapshot committed", i, build.Status, build.StatusGroup)
		}
		// The finish has to be recorded too — `builds_finished_axis` states the
		// equivalence, so a terminal group without one cannot be written at all
		// and this would have failed above; asserted anyway, because the column
		// is what an operator reads to see how long the build took.
		if build.FinishedAtMs == nil {
			t.Fatalf("build %d ended without recording when", i)
		}
	}

	var active int64
	if err := f.pool.QueryRow(f.ctx, countActiveBuildsSQL, f.cluster).Scan(&active); err != nil {
		t.Fatalf("count active builds: %v", err)
	}
	if active != 0 {
		t.Fatalf("%d builds are still on the queue with nothing building", active)
	}
}

// TestACommitWithNoBuildOfItsOwnLeavesOtherBuildsAlone is the refusal half of
// the statement above. A pause commits a snapshot too, and it has no build.
func TestACommitWithNoBuildOfItsOwnLeavesOtherBuildsAlone(t *testing.T) {
	f := newFixture(t)

	// A build running for one template…
	building := f.beginTemplate("")
	out, err := f.startBuild(building.SnapshotID, "", "node-a", 0)
	if err != nil || out.Rejected != nil {
		t.Fatalf("start: %v %+v", err, out.Rejected)
	}

	// …and an unrelated snapshot committing.
	other := f.begin(BeginInput{
		SnapshotID:      newUUID(t),
		SourceKind:      SourceKindSandbox,
		SourceSandboxID: "sbx-unrelated",
		Status:          StatusBuilding,
		CPUCount:        2,
		MemoryMiB:       512,
		DiskSizeMiB:     4,
		Published:       true,
		CreatedAtMs:     time.Now().UnixMilli(),
	})
	f.commit(CommitInput{SnapshotID: other.SnapshotID})

	still, err := f.store.GetBuild(f.ctx, f.cluster, out.Build.BuildID)
	if err != nil {
		t.Fatalf("get build: %v", err)
	}
	if still.StatusGroup != StatusGroupInProgress {
		t.Fatalf("another snapshot's commit ended a running build: %+v", still)
	}
}

func TestRenewBuildLease(t *testing.T) {
	f := newFixture(t)

	template := f.beginTemplate("")
	out, err := f.startBuild(template.SnapshotID, "", "node-a", 1000)
	if err != nil || out.Rejected != nil {
		t.Fatalf("start: %v %+v", err, out.Rejected)
	}

	// Aged first, so that "the renewal moved it" is a change this test can see
	// rather than two reads of the same second.
	f.ageBuildHeartbeat(out.Build.BuildID, time.Hour)
	before, err := f.store.GetBuild(f.ctx, f.cluster, out.Build.BuildID)
	if err != nil {
		t.Fatalf("get build: %v", err)
	}

	live, err := f.store.RenewBuildLease(f.ctx, RenewBuildLeaseInput{
		ClusterID: f.cluster, NodeID: "node-a", BuildID: out.Build.BuildID,
	})
	if err != nil || !live {
		t.Fatalf("renew: live=%v err=%v", live, err)
	}
	after, err := f.store.GetBuild(f.ctx, f.cluster, out.Build.BuildID)
	if err != nil {
		t.Fatalf("get build: %v", err)
	}
	if after.HeartbeatAtMs == nil || *after.HeartbeatAtMs <= *before.HeartbeatAtMs {
		t.Fatalf("the renewal did not move the heartbeat: %v -> %v", before.HeartbeatAtMs, after.HeartbeatAtMs)
	}

	// 🔴 A process that is not the one running this build must not keep it
	// alive — that is exactly what would happen after the reaper freed the
	// template and somebody else took it.
	live, err = f.store.RenewBuildLease(f.ctx, RenewBuildLeaseInput{
		ClusterID: f.cluster, NodeID: "node-b", BuildID: out.Build.BuildID,
	})
	if err != nil {
		t.Fatalf("renew: %v", err)
	}
	if live {
		t.Fatal("a node that is not running this build renewed its lease")
	}

	live, err = f.store.RenewBuildLease(f.ctx, RenewBuildLeaseInput{
		ClusterID: f.cluster, NodeID: "node-a", BuildID: newUUID(t),
	})
	if err != nil {
		t.Fatalf("renew: %v", err)
	}
	if live {
		t.Fatal("a build that does not exist reported itself live")
	}
}

// TestGetBuildSeesBuildsNoResolvingQueryWould is the rule the two query files
// exist to keep apart.
func TestGetBuildSeesBuildsNoResolvingQueryWould(t *testing.T) {
	f := newFixture(t)

	template := f.beginTemplate("")
	out, err := f.startBuild(template.SnapshotID, "", "node-a", 0)
	if err != nil || out.Rejected != nil {
		t.Fatalf("start: %v %+v", err, out.Rejected)
	}

	// Running.
	running, err := f.store.GetBuild(f.ctx, f.cluster, out.Build.BuildID)
	if err != nil || running == nil {
		t.Fatalf("a running build was invisible: %v %v", running, err)
	}
	if running.StatusGroup != StatusGroupInProgress {
		t.Fatalf("status group = %q", running.StatusGroup)
	}

	// Failed.
	if _, err := f.store.FailSnapshot(f.ctx, FailInput{
		ClusterID: f.cluster, SnapshotID: template.SnapshotID,
		BuildError: mustJSON(t, map[string]string{"message": "nope"}), FailActiveBuild: true,
	}); err != nil {
		t.Fatalf("fail: %v", err)
	}
	failed, err := f.store.GetBuild(f.ctx, f.cluster, out.Build.BuildID)
	if err != nil || failed == nil {
		t.Fatalf("a failed build was invisible: %v %v", failed, err)
	}
	if failed.StatusGroup != StatusGroupFailed || failed.FinishedAtMs == nil {
		t.Fatalf("failed build = %+v", failed)
	}
	if failed.ErrorReason == nil {
		t.Fatal("a failed build carries no reason")
	}

	missing, err := f.store.GetBuild(f.ctx, f.cluster, newUUID(t))
	if err != nil {
		t.Fatalf("get build: %v", err)
	}
	if missing != nil {
		t.Fatal("a build nothing has was returned")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// The reaper
// ─────────────────────────────────────────────────────────────────────────────

func TestReaperEndsBothHalvesOfAStrandedBuild(t *testing.T) {
	f := newFixture(t)

	template := f.beginTemplate("")
	out, err := f.startBuild(template.SnapshotID, "", "node-a", 0)
	if err != nil || out.Rejected != nil {
		t.Fatalf("start: %v %+v", err, out.Rejected)
	}

	f.ageBuildHeartbeat(out.Build.BuildID, 10*time.Minute)
	reaped, err := f.store.ReapExpiredBuilds(f.ctx, ReapInput{ClusterID: f.cluster, TTLMs: 5 * 60 * 1000})
	if err != nil {
		t.Fatalf("reap: %v", err)
	}
	if len(reaped) != 1 || reaped[0].BuildID != out.Build.BuildID {
		t.Fatalf("reaped %+v", reaped)
	}
	if reaped[0].NodeID != "node-a" {
		t.Fatalf("the reaped build named node %q", reaped[0].NodeID)
	}

	build, err := f.store.GetBuild(f.ctx, f.cluster, out.Build.BuildID)
	if err != nil {
		t.Fatalf("get build: %v", err)
	}
	if build.StatusGroup != StatusGroupFailed || build.FinishedAtMs == nil {
		t.Fatalf("the build row was not ended: %+v", build)
	}

	// 🔴 And the template row, which is the half that is easy to leave out.
	// The unique index stops blocking the template the moment the build leaves
	// the active group — but a template still sitting at `building` is refused
	// by the admission statement, so it stays unbuildable through the other
	// door.
	snapshot, err := f.store.GetSnapshot(f.ctx, f.cluster, template.SnapshotID, ReadOptions{})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if snapshot.Status != StatusError {
		t.Fatalf("the template row is %q, want %q", snapshot.Status, StatusError)
	}
	if snapshot.BuildError == nil {
		t.Fatal("the reaped template carries no reason")
	}

	// And the builder finds out, which is the only notice it gets.
	live, err := f.store.RenewBuildLease(f.ctx, RenewBuildLeaseInput{
		ClusterID: f.cluster, NodeID: "node-a", BuildID: out.Build.BuildID,
	})
	if err != nil {
		t.Fatalf("renew: %v", err)
	}
	if live {
		t.Fatal("a reaped build still reported itself live: two builders would publish into one template")
	}
}

// TestReaperIsWhatKeepsTheExclusionFromBecomingAnOutage is P6: the index and
// the reaper are one change, and this is the pair of runs that shows it.
func TestReaperIsWhatKeepsTheExclusionFromBecomingAnOutage(t *testing.T) {
	f := newFixture(t)

	strand := func() string {
		template := f.beginTemplate("")
		out, err := f.startBuild(template.SnapshotID, "", "node-a", 0)
		if err != nil || out.Rejected != nil {
			f.t.Fatalf("start: %v %+v", err, out.Rejected)
		}
		f.ageBuildHeartbeat(out.Build.BuildID, 10*time.Minute)
		return template.SnapshotID
	}

	// 🔴 The control first: without a reaping pass, the template is blocked and
	// stays blocked. If this half passed too, the test below would prove
	// nothing about the reaper.
	blocked := strand()
	out, err := f.startBuild(blocked, newUUID(t), "node-b", 0)
	if err != nil {
		t.Fatalf("retry without reaping: %v", err)
	}
	if out.Rejected == nil || out.Rejected.Reason != RejectionBuildInProgress {
		t.Fatalf("without a reaping pass the stranded build did not block the template: %+v", out)
	}

	// And now with one.
	reaped := strand()
	if _, err := f.store.ReapExpiredBuilds(f.ctx, ReapInput{ClusterID: f.cluster, TTLMs: 5 * 60 * 1000}); err != nil {
		t.Fatalf("reap: %v", err)
	}
	out, err = f.startBuild(reaped, newUUID(t), "node-b", 0)
	if err != nil {
		t.Fatalf("retry after reaping: %v", err)
	}
	if out.Rejected != nil {
		t.Fatalf("a template whose stranded build was reaped is still unbuildable: %+v", out.Rejected)
	}
}

// TestReaperLeavesAliveAndUnevidencedBuildsAlone is the refusal half, and it is
// the one that matters most in this file.
//
// 🔴 A reaper that leaks a row costs an operator a query. A reaper that ends a
// build still running costs a user the VM-minutes it had spent, tells them
// their heartbeat lapsed when it did not, and — because the template row is
// failed alongside it — hands them an error for a build that was working. The
// two directions are not the same size, so this is the test to break first when
// checking whether these tests actually hold anything.
func TestReaperLeavesAliveAndUnevidencedBuildsAlone(t *testing.T) {
	f := newFixture(t)

	// Alive: last heard from a minute ago, well inside the TTL.
	alive := f.beginTemplate("")
	aliveBuild, err := f.startBuild(alive.SnapshotID, "", "node-a", 0)
	if err != nil || aliveBuild.Rejected != nil {
		t.Fatalf("start: %v %+v", err, aliveBuild.Rejected)
	}
	f.ageBuildHeartbeat(aliveBuild.Build.BuildID, time.Minute)

	// 🔴 Just inside, too: a build one millisecond short of the TTL is still a
	// build that is running. Without this the boundary could be > instead of >=
	// — or the TTL could be ignored altogether — and the test above would not
	// notice.
	fresh := f.beginTemplate("")
	freshBuild, err := f.startBuild(fresh.SnapshotID, "", "node-a", 0)
	if err != nil || freshBuild.Rejected != nil {
		t.Fatalf("start: %v %+v", err, freshBuild.Rejected)
	}
	f.ageBuildHeartbeat(freshBuild.Build.BuildID, 5*time.Minute-2*time.Second)

	// 🔴 No heartbeat at all. The reaper leaves it alone forever, which is
	// exactly why the admitting statement stamps one — this row is the shape
	// nothing can clean up, and it can only be written by going round the
	// store.
	unevidenced := f.beginTemplate("")
	unevidencedBuild := newUUID(t)
	if _, err := f.pool.Exec(f.ctx, `
INSERT INTO builds (id, template_id, cluster_id, status, status_group, node_id, created_at_ms, started_at_ms)
VALUES ($1::uuid, $2::uuid, $3::uuid, 'building', 'in_progress', 'node-a', $4, $4)`,
		unevidencedBuild, unevidenced.SnapshotID, f.cluster, time.Now().Add(-time.Hour).UnixMilli()); err != nil {
		t.Fatalf("insert a heartbeatless build: %v", err)
	}

	reaped, err := f.store.ReapExpiredBuilds(f.ctx, ReapInput{ClusterID: f.cluster, TTLMs: 5 * 60 * 1000})
	if err != nil {
		t.Fatalf("reap: %v", err)
	}
	if len(reaped) != 0 {
		t.Fatalf("the pass reaped %+v, want nothing", reaped)
	}

	for _, id := range []string{aliveBuild.Build.BuildID, freshBuild.Build.BuildID, unevidencedBuild} {
		build, err := f.store.GetBuild(f.ctx, f.cluster, id)
		if err != nil {
			t.Fatalf("get build: %v", err)
		}
		if build.StatusGroup != StatusGroupInProgress {
			t.Fatalf("build %s was ended: %+v", id, build)
		}
	}

	// 🔴 And their templates. A pass that left the build rows alone but failed
	// the template rows anyway would still have taken a working build away from
	// its user — the reaper writes both halves, so the refusal has to cover
	// both halves too.
	for _, id := range []string{alive.SnapshotID, fresh.SnapshotID, unevidenced.SnapshotID} {
		row, err := f.store.GetSnapshot(f.ctx, f.cluster, id, ReadOptions{})
		if err != nil {
			t.Fatalf("get snapshot: %v", err)
		}
		if row.Status == StatusError {
			t.Fatalf("template %s was failed under a build that is still running: %+v", id, row.BuildError)
		}
	}
}

// TestTheReaperDoesNotFailATemplateThatHasAlreadyPublished is the second half
// of the refusal, on the half of the pass that writes to `snapshots`.
//
// 🔴 The reaper fails the template alongside the build, and that is right for a
// template stuck at `building` — it is what stops the exclusion becoming an
// outage. It is very wrong for a template that has published: the snapshot is
// complete, users are starting sandboxes from it, and marking it `error` tells
// every one of them it failed. The predicate that keeps the two apart is a
// single `AND status = 'building'`, which removes cleanly and breaks nothing
// else.
func TestTheReaperDoesNotFailATemplateThatHasAlreadyPublished(t *testing.T) {
	f := newFixture(t)

	template := f.beginTemplate("")
	out, err := f.startBuild(template.SnapshotID, "", "node-a", 0)
	if err != nil || out.Rejected != nil {
		t.Fatalf("start: %v %+v", err, out.Rejected)
	}
	size := uint32(12)
	f.commit(CommitInput{SnapshotID: template.SnapshotID, DiskSizeMiB: &size})

	// A second build row for a template that has already published. The store
	// cannot produce one — the admitting statement refuses a `ready` template —
	// so this is written round it, which is the point: the predicate has to
	// hold against a row it did not create.
	stale := newUUID(t)
	if _, err := f.pool.Exec(f.ctx, `
INSERT INTO builds (id, template_id, cluster_id, status, status_group, node_id, heartbeat_at_ms, created_at_ms, started_at_ms)
VALUES ($1::uuid, $2::uuid, $3::uuid, 'building', 'in_progress', 'node-a', $4, $4, $4)`,
		stale, template.SnapshotID, f.cluster, time.Now().Add(-time.Hour).UnixMilli()); err != nil {
		t.Fatalf("plant a stale build under a published template: %v", err)
	}

	reaped, err := f.store.ReapExpiredBuilds(f.ctx, ReapInput{ClusterID: f.cluster, TTLMs: 60_000})
	if err != nil {
		t.Fatalf("reap: %v", err)
	}
	if len(reaped) != 1 || reaped[0].BuildID != stale {
		t.Fatalf("reaped %+v, want just the stale build", reaped)
	}

	row, err := f.store.GetSnapshot(f.ctx, f.cluster, template.SnapshotID, ReadOptions{OnlyReady: true})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if row == nil {
		t.Fatal("the reaper took a published template out of every resolving query")
	}
	if row.Status != StatusReady || row.BuildError != nil {
		t.Fatalf("a published template was failed by the reaper: status=%q error=%s", row.Status, row.BuildError)
	}
}

func TestReaperRefusesAPassWithNoTTL(t *testing.T) {
	f := newFixture(t)

	// A pass with no TTL would end every live build in the cluster.
	if _, err := f.store.ReapExpiredBuilds(f.ctx, ReapInput{ClusterID: f.cluster, TTLMs: 0}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("error = %v, want ErrInvalidArgument", err)
	}
}

func TestReaperIsScopedToItsCluster(t *testing.T) {
	f := newFixture(t)

	template := f.beginTemplate("")
	out, err := f.startBuild(template.SnapshotID, "", "node-a", 0)
	if err != nil || out.Rejected != nil {
		t.Fatalf("start: %v %+v", err, out.Rejected)
	}
	f.ageBuildHeartbeat(out.Build.BuildID, time.Hour)

	reaped, err := f.store.ReapExpiredBuilds(f.ctx, ReapInput{
		ClusterID: "22222222-2222-2222-2222-222222222222", TTLMs: 1000,
	})
	if err != nil {
		t.Fatalf("reap: %v", err)
	}
	if len(reaped) != 0 {
		t.Fatalf("another cluster's pass reaped %+v", reaped)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// The paused half, and the transaction that holds it to the catalog row
// ─────────────────────────────────────────────────────────────────────────────
//
// 🔴 These are the reason the catalog is served from this process rather than
// read by the node. A pause writes two facts — the sandbox is parked, and here
// is the snapshot it parked into — and until now they landed in two systems
// with nothing between them, so a crash in the middle left a sandbox recorded
// as paused with nothing in the catalog to bring it back.
//
// What is asserted is not that the two writes happen. It is that neither
// happens without the other, in both directions.

func (f *fixture) beginPause(sandboxID string) (BeginOutcome, string) {
	f.t.Helper()

	snapshotID := newUUID(f.t)
	out, err := f.beginRaw(BeginInput{
		SnapshotID:      snapshotID,
		NodeID:          "node-a",
		SourceKind:      SourceKindSandbox,
		SourceSandboxID: sandboxID,
		Status:          StatusBuilding,
		Published:       false,
		OriginNodeID:    "node-a",
		Paused: &PausedBegin{
			SandboxID:   sandboxID,
			Metadata:    mustJSON(f.t, map[string]string{"kind": "sandbox"}),
			ExecutionID: newUUID(f.t),
		},
	})
	if err != nil {
		f.t.Fatalf("begin a pause: %v", err)
	}
	return out, snapshotID
}

func TestBeginSnapshotAndBeginPauseCommitTogether(t *testing.T) {
	f := newFixture(t)

	sandbox := newUUID(t)
	out, snapshotID := f.beginPause(sandbox)
	if out.Rejected != nil {
		t.Fatalf("refused: %+v", out.Rejected)
	}
	if out.Generation == nil || *out.Generation != 1 {
		t.Fatalf("generation = %v, want 1", out.Generation)
	}
	if state, ok := f.pausedState(sandbox); !ok || state != "publishing" {
		t.Fatalf("paused state = %q (%v), want publishing", state, ok)
	}

	row, err := f.store.GetSnapshot(f.ctx, f.cluster, snapshotID, ReadOptions{})
	if err != nil || row == nil {
		t.Fatalf("the catalog row is not there: %v %v", row, err)
	}
	if row.Published {
		t.Fatal("a pause opens its row unpublished: nothing has left the node yet")
	}
}

// TestBeginSnapshotRefusesAPublishedPause pins §5.2①: a pause opens its row
// unpublished.
//
// 🔴 The two halves cannot be allowed to disagree at birth. This one statement
// writes both — the catalog row and `paused_sandboxes` going to `publishing` on
// this node — and step ② has not happened yet, so there are no bytes anywhere
// but here. `published=true` says any node can start it. CommitSnapshot already
// refuses the same disagreement on the way out (the transition/published XOR in
// catalog_service.go); this is the entry that could otherwise create it, and
// nothing between the two would have noticed: the row reads as a perfectly
// ordinary published snapshot, and a resume scheduled anywhere finds nothing.
func TestBeginSnapshotRefusesAPublishedPause(t *testing.T) {
	f := newFixture(t)

	sandbox := newUUID(t)
	published := func() BeginInput {
		return BeginInput{
			SnapshotID:      newUUID(t),
			NodeID:          "node-a",
			SourceKind:      SourceKindSandbox,
			SourceSandboxID: sandbox,
			Status:          StatusBuilding,
			Published:       true,
			OriginNodeID:    "node-a",
			Paused: &PausedBegin{
				SandboxID:   sandbox,
				Metadata:    mustJSON(t, map[string]string{"kind": "sandbox"}),
				ExecutionID: newUUID(t),
			},
		}
	}

	_, err := f.beginRaw(published())
	if err == nil {
		t.Fatal("a pause was allowed to open its row published")
	}
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("error is not ErrInvalidArgument: %v", err)
	}
	if !strings.Contains(err.Error(), "unpublished") {
		t.Fatalf("the refusal does not say what is wrong with the row: %v", err)
	}

	// Neither half moved. A refusal that had already written one of them is the
	// state this whole transaction exists to make impossible.
	var rows int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM snapshots`).Scan(&rows); err != nil {
		t.Fatalf("count: %v", err)
	}
	if rows != 0 {
		t.Fatalf("snapshots = %d, want 0: the refused begin left a row behind", rows)
	}
	if state, ok := f.pausedState(sandbox); ok {
		t.Fatalf("the paused half moved to %q on a begin that was refused", state)
	}

	// 🔴 The control. Everything else about this input is valid, so the refusal
	// above is about `published` and not about some other field the builder got
	// wrong — and the same call with published=false goes through.
	in := published()
	in.Published = false
	out, err := f.beginRaw(in)
	if err != nil {
		t.Fatalf("the same pause, unpublished, was refused too: %v", err)
	}
	if out.Rejected != nil {
		t.Fatalf("refused: %+v", out.Rejected)
	}
	if state, ok := f.pausedState(sandbox); !ok || state != "publishing" {
		t.Fatalf("paused state = %q (%v), want publishing", state, ok)
	}
}

// TestARefusedCatalogWriteLeavesTheRegistryUntouched is the direction the
// object-store design could not have: the catalog half refuses, and the
// sandbox's own row does not move.
func TestARefusedCatalogWriteLeavesTheRegistryUntouched(t *testing.T) {
	f := newFixture(t)

	f.readyTemplate("contested")

	sandbox := newUUID(t)
	out, snapshotID := f.beginPause(sandbox)
	if out.Rejected != nil {
		t.Fatalf("begin: %+v", out.Rejected)
	}

	commit, err := f.commitRaw(CommitInput{
		SnapshotID:   snapshotID,
		Alias:        "contested",
		Published:    true,
		OriginNodeID: "node-a",
		Paused:       &PausedFinish{SandboxID: sandbox, ExpectGeneration: *out.Generation},
	})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if commit.Rejected == nil || commit.Rejected.Reason != RejectionAliasTaken {
		t.Fatalf("outcome = %+v, want ALIAS_TAKEN", commit)
	}

	// 🔴 Both halves are as they were. Before this, a pause that lost its alias
	// would still have completed in the registry — the sandbox would read as
	// paused, pointing at a snapshot the catalog never made ready.
	if state, _ := f.pausedState(sandbox); state != "publishing" {
		t.Fatalf("the registry row moved to %q on a refused commit", state)
	}
	row, err := f.store.GetSnapshot(f.ctx, f.cluster, snapshotID, ReadOptions{})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if row.Status != StatusBuilding {
		t.Fatalf("the catalog row moved to %q on a refused commit", row.Status)
	}
}

// TestARefusedRegistryWriteLeavesTheCatalogUntouched is the same property from
// the other side, and it is the one that matters most: a catalog row flipped to
// `ready` while the sandbox stayed `publishing` is a snapshot the cluster would
// hand to any node while its bytes are still going up.
func TestARefusedRegistryWriteLeavesTheCatalogUntouched(t *testing.T) {
	f := newFixture(t)

	sandbox := newUUID(t)
	out, snapshotID := f.beginPause(sandbox)
	if out.Rejected != nil {
		t.Fatalf("begin: %+v", out.Rejected)
	}

	stale := *out.Generation - 1
	commit, err := f.commitRaw(CommitInput{
		SnapshotID:   snapshotID,
		Published:    true,
		OriginNodeID: "node-a",
		Paused:       &PausedFinish{SandboxID: sandbox, ExpectGeneration: stale},
	})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if commit.Rejected == nil || commit.Rejected.Reason != RejectionGenerationMismatch {
		t.Fatalf("outcome = %+v, want GENERATION_MISMATCH", commit)
	}
	// The refusal carries the number to re-read against, rather than leaving
	// the caller to guess.
	if commit.Rejected.ObservedGeneration == nil || *commit.Rejected.ObservedGeneration != *out.Generation {
		t.Fatalf("observed generation = %v, want %d", commit.Rejected.ObservedGeneration, *out.Generation)
	}

	row, err := f.store.GetSnapshot(f.ctx, f.cluster, snapshotID, ReadOptions{})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	// The catalog's own statement succeeded and was then rolled back by the
	// registry half's refusal. `ready` here means the two are not in one
	// transaction.
	if row.Status != StatusBuilding {
		t.Fatalf("the catalog row is %q after a commit the registry refused; "+
			"it should have been rolled back to %q, and `ready` would mean the two halves are not one transaction",
			row.Status, StatusBuilding)
	}
	if row.CommittedPayload != nil {
		t.Fatalf("a rolled-back commit left a payload behind: %x", row.CommittedPayload)
	}
	if state, _ := f.pausedState(sandbox); state != "publishing" {
		t.Fatalf("the registry row is %q", state)
	}
}

func TestCommitSnapshotAndCompletePauseCommitTogether(t *testing.T) {
	f := newFixture(t)

	sandbox := newUUID(t)
	out, snapshotID := f.beginPause(sandbox)
	row := f.commit(CommitInput{
		SnapshotID:   snapshotID,
		Published:    true,
		OriginNodeID: "node-a",
		Paused:       &PausedFinish{SandboxID: sandbox, ExpectGeneration: *out.Generation},
	})

	if row.Status != StatusReady || !row.Published {
		t.Fatalf("catalog row = %q published=%v", row.Status, row.Published)
	}
	if state, _ := f.pausedState(sandbox); state != "paused" {
		t.Fatalf("the sandbox is %q, want paused", state)
	}
	var snapshot *string
	if err := f.pool.QueryRow(f.ctx, `SELECT snapshot_id::text FROM scratch_paused WHERE sandbox_id = $1::uuid`, sandbox).Scan(&snapshot); err != nil {
		t.Fatalf("read the paused half: %v", err)
	}
	if snapshot == nil || *snapshot != snapshotID {
		t.Fatalf("the sandbox points at %v, want %s", snapshot, snapshotID)
	}
}

// TestAPublishThatNeverLeftTheNodeParksBothHalves is §5.2's transaction C.
func TestAPublishThatNeverLeftTheNodeParksBothHalves(t *testing.T) {
	f := newFixture(t)

	sandbox := newUUID(t)
	out, snapshotID := f.beginPause(sandbox)
	row := f.commit(CommitInput{
		SnapshotID:   snapshotID,
		Published:    false,
		OriginNodeID: "node-a",
		Paused:       &PausedFinish{SandboxID: sandbox, ExpectGeneration: *out.Generation, LocalOnly: true},
	})

	// 🔴 `ready`, not stuck at building. The bytes are complete; only their
	// location is limited, and that is what `published` says.
	if row.Status != StatusReady {
		t.Fatalf("catalog status = %q, want %q", row.Status, StatusReady)
	}
	if row.Published || row.OriginNodeID != "node-a" {
		t.Fatalf("the origin block is %v/%q", row.Published, row.OriginNodeID)
	}
	// 🔴 And the registry keeps the row rather than deleting it. Deleting would
	// make "parked on its own node" indistinguishable from "resumed elsewhere,
	// or destroyed", and reconciliation answers the second by throwing away
	// what is by then the only copy.
	if state, ok := f.pausedState(sandbox); !ok || state != "local_only" {
		t.Fatalf("the sandbox is %q (%v), want local_only and still there", state, ok)
	}
}

func TestFailSnapshotParksTheSandboxItCouldNotCapture(t *testing.T) {
	f := newFixture(t)

	sandbox := newUUID(t)
	out, snapshotID := f.beginPause(sandbox)

	failed, err := f.store.FailSnapshot(f.ctx, FailInput{
		ClusterID:  f.cluster,
		SnapshotID: snapshotID,
		BuildError: mustJSON(t, map[string]string{"message": "capture produced nothing"}),
		Paused:     &PausedFinish{SandboxID: sandbox, ExpectGeneration: *out.Generation, LocalOnly: true},
	})
	if err != nil {
		t.Fatalf("fail: %v", err)
	}
	if failed.Rejected != nil {
		t.Fatalf("refused: %+v", failed.Rejected)
	}
	if failed.Row.Status != StatusError {
		t.Fatalf("catalog status = %q", failed.Row.Status)
	}
	if state, _ := f.pausedState(sandbox); state != "local_only" {
		t.Fatalf("the sandbox is %q", state)
	}
}

func TestFailSnapshotRefusesToCompleteAPause(t *testing.T) {
	f := newFixture(t)

	sandbox := newUUID(t)
	out, snapshotID := f.beginPause(sandbox)

	// complete_pause names a snapshot the sandbox can come back from, and this
	// call is the statement that there is none.
	_, err := f.store.FailSnapshot(f.ctx, FailInput{
		ClusterID:  f.cluster,
		SnapshotID: snapshotID,
		BuildError: mustJSON(t, map[string]string{"message": "x"}),
		Paused:     &PausedFinish{SandboxID: sandbox, ExpectGeneration: *out.Generation},
	})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("error = %v, want ErrInvalidArgument", err)
	}
}

// TestASupersededIncarnationIsRefusedTerminally is the refusal a caller must
// never retry: a re-read after it hands the caller the live incarnation's
// generation, and the same write then walks straight around the fence.
func TestASupersededIncarnationIsRefusedTerminally(t *testing.T) {
	f := newFixture(t)

	f.paused.beginErr = fmt.Errorf("%w: incarnation replaced", ErrPausedExecutionFenced)

	out, err := f.beginRaw(BeginInput{
		NodeID:          "node-a",
		SourceKind:      SourceKindSandbox,
		SourceSandboxID: newUUID(t),
		Status:          StatusBuilding,
		Published:       false,
		OriginNodeID:    "node-a",
		Paused: &PausedBegin{
			SandboxID:   newUUID(t),
			Metadata:    mustJSON(t, map[string]string{"a": "b"}),
			ExecutionID: newUUID(t),
		},
	})
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if out.Rejected == nil || out.Rejected.Reason != RejectionExecutionSuperseded {
		t.Fatalf("outcome = %+v, want EXECUTION_SUPERSEDED", out)
	}

	// 🔴 And nothing was written. A superseded incarnation that left a catalog
	// row behind would have the live one's next pause refused for an id that
	// already exists.
	var rows int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM snapshots`).Scan(&rows); err != nil {
		t.Fatalf("count: %v", err)
	}
	if rows != 0 {
		t.Fatalf("a fenced pause left %d catalog rows behind", rows)
	}
}

func TestARolledBackBeginLeavesNoRegistryRow(t *testing.T) {
	f := newFixture(t)

	f.readyTemplate("held")

	sandbox := newUUID(t)
	out, err := f.beginRaw(BeginInput{
		NodeID:          "node-a",
		SourceKind:      SourceKindSandbox,
		SourceSandboxID: sandbox,
		Status:          StatusBuilding,
		Published:       false,
		OriginNodeID:    "node-a",
		Alias:           "held",
		Paused: &PausedBegin{
			SandboxID:   sandbox,
			Metadata:    mustJSON(t, map[string]string{"a": "b"}),
			ExecutionID: newUUID(t),
		},
	})
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if out.Rejected == nil || out.Rejected.Reason != RejectionAliasTaken {
		t.Fatalf("outcome = %+v, want ALIAS_TAKEN", out)
	}
	if state, ok := f.pausedState(sandbox); ok {
		t.Fatalf("a refused begin left the sandbox at %q", state)
	}
}

// TestSuccessivePausesCarryTheSnapshotTheySupersede is what lets the caller
// collect the previous snapshot's artifacts once the new pause completes.
func TestSuccessivePausesCarryTheSnapshotTheySupersede(t *testing.T) {
	f := newFixture(t)

	sandbox := newUUID(t)
	first, firstSnapshot := f.beginPause(sandbox)
	f.commit(CommitInput{
		SnapshotID:   firstSnapshot,
		Published:    true,
		OriginNodeID: "node-a",
		Paused:       &PausedFinish{SandboxID: sandbox, ExpectGeneration: *first.Generation},
	})

	second, _ := f.beginPause(sandbox)
	if second.Rejected != nil {
		t.Fatalf("the second pause was refused: %+v", second.Rejected)
	}
	if second.PreviousSnapshotID != firstSnapshot {
		t.Fatalf("the second pause reported %q as the snapshot it supersedes, want %q",
			second.PreviousSnapshotID, firstSnapshot)
	}
	if second.Generation == nil || *second.Generation != 2 {
		t.Fatalf("generation = %v, want 2", second.Generation)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Alias uniqueness under concurrency
// ─────────────────────────────────────────────────────────────────────────────

// TestOnlyOneOfTwoRacingCommitsTakesAnAlias is what replaces the object-store
// backend's read-modify-write-reread, which describes itself as "weaker than a
// true CAS".
func TestOnlyOneOfTwoRacingCommitsTakesAnAlias(t *testing.T) {
	f := newFixture(t)

	const racers = 8
	opened := make([]string, 0, racers)
	for i := 0; i < racers; i++ {
		row := f.begin(BeginInput{Status: StatusBuilding})
		opened = append(opened, row.SnapshotID)
	}

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		won      int
		conflict int
		failures []error
	)
	start := make(chan struct{})
	for _, id := range opened {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			<-start
			out, err := f.store.CommitSnapshot(context.Background(), CommitInput{
				ClusterID:        f.cluster,
				SnapshotID:       id,
				CommittedPayload: []byte{0x01},
				CommittedSchema:  1,
				Alias:            "one-name",
				Published:        true,
				UpdatedAtMs:      time.Now().UnixMilli(),
			})
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err != nil:
				failures = append(failures, err)
			case out.Rejected == nil:
				won++
			case out.Rejected.Reason == RejectionAliasTaken:
				conflict++
			default:
				failures = append(failures, fmt.Errorf("unexpected refusal %s", out.Rejected.Reason))
			}
		}(id)
	}
	close(start)
	wg.Wait()

	if len(failures) > 0 {
		t.Fatalf("errors during the race: %v", failures)
	}
	if won != 1 || conflict != racers-1 {
		t.Fatalf("%d commits took the alias and %d were refused, want 1 and %d", won, conflict, racers-1)
	}

	var holders int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM aliases WHERE alias = 'one-name'`).Scan(&holders); err != nil {
		t.Fatalf("count: %v", err)
	}
	if holders != 1 {
		t.Fatalf("%d rows hold the alias", holders)
	}

	// 🔴 And the losers are still building — refused entirely rather than
	// committed without the name they asked for.
	ready := 0
	for _, id := range opened {
		row, err := f.store.GetSnapshot(f.ctx, f.cluster, id, ReadOptions{})
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if row.Status == StatusReady {
			ready++
		}
	}
	if ready != 1 {
		t.Fatalf("%d snapshots went ready, want the one that took the alias", ready)
	}
}

// TestDeletingATemplateEndsTheBuildItWasHolding is the cascade the foreign key
// does not perform.
//
// 🔴 `builds_template_fk` is declared ON DELETE CASCADE, and a snapshot delete
// here is *soft* — so the cascade never fires. A template deleted mid-build
// left its `builds` row in `in_progress`, where `countActiveBuildsSQL` goes on
// counting it against the cluster-wide ceiling and
// `builds_one_active_per_template` goes on holding a template that no longer
// exists. Nothing released it but the heartbeat reaper, a TTL later, and only
// once the builder stopped renewing; a builder still running would have held
// the slot for as long as it ran.
func TestDeletingATemplateEndsTheBuildItWasHolding(t *testing.T) {
	f := newFixture(t, withBuildCeiling(1))

	template := f.beginTemplate("")
	started, err := f.startBuild(template.SnapshotID, "", "node-a", 0)
	if err != nil || started.Rejected != nil {
		t.Fatalf("the build was not admitted: %v %+v", err, started.Rejected)
	}

	deleted, err := f.store.DeleteSnapshot(f.ctx, f.cluster, template.SnapshotID, time.Now().UnixMilli())
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if !deleted.Deleted {
		t.Fatalf("the template was not deleted: %+v", deleted)
	}

	build, err := f.store.GetBuild(f.ctx, f.cluster, started.Build.BuildID)
	if err != nil {
		t.Fatalf("get build: %v", err)
	}
	if build == nil {
		t.Fatal("the build row is gone entirely, which loses the record of what happened")
	}
	if build.StatusGroup == StatusGroupPending || build.StatusGroup == StatusGroupInProgress {
		t.Fatalf("the deleted template's build is still %q/%q, so it still holds its slot",
			build.Status, build.StatusGroup)
	}
	if len(build.ErrorReason) == 0 {
		t.Fatal("nothing says why the build ended, so an operator reading the row cannot tell " +
			"a deleted template from a build that crashed")
	}

	// 🔴 The assertion that matters to the cluster rather than to the row: the
	// ceiling is one, and a build on a *different* template has to fit through
	// it now. Before, this was refused as BUILD_QUEUE_FULL by a template that
	// had been deleted.
	other := f.beginTemplate("")
	out, err := f.startBuild(other.SnapshotID, "", "node-a", 0)
	if err != nil {
		t.Fatalf("second build: %v", err)
	}
	if out.Rejected != nil {
		t.Fatalf("a deleted template's build is still holding the cluster ceiling: %+v", out.Rejected)
	}
}
