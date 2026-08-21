package catalog

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
)

// Store is the snapshot catalog's query layer: every statement that reads or
// writes `snapshots`, `templates`, `builds` and `aliases`, behind one process.
//
// 🔴 Why it lives here at all, rather than on the node. A pause writes two
// facts — the sandbox is parked, and here is the snapshot it parked into — and
// today they land in two systems with nothing between them. Moving the catalog
// into PostgreSQL only fixes that if the *other* fact is in the same database
// and reachable by the same transaction, which is true exactly here. A node
// holding its own connection would have had two sockets and the window would
// have stayed open. Every write below therefore takes an optional paused half
// and runs it in its own transaction; that is the whole reason for the seam.
//
// 🔴 What this layer is not allowed to know. `CommittedPayload` is a
// CommittedSnapshot as the node serialised it and `BuildError` is a
// TemplateBuildErrorReason with a hand-written deserialiser on the other side.
// Both travel as bytes, are stored as bytes, and come back as bytes. Nothing
// here defines a Go type for either, branches on their contents, or validates
// them beyond "is this a JSON object" where a malformed one would poison the
// column. This process is the table's DBA, not the domain's model.
type Store interface {
	// BeginSnapshot creates the row before any bytes exist — transaction A.
	//
	// A template is born `waiting`; a pause opens its row `building` and
	// unpublished, naming the node the bytes are going to. Carries the paused
	// half's begin_pause when there is one, so that "the sandbox is pausing"
	// and "here is the row it will publish into" commit together.
	BeginSnapshot(ctx context.Context, in BeginInput) (BeginOutcome, error)

	// CommitSnapshot flips a row to `ready` — transaction B, and the only
	// writer that produces a `ready` row.
	//
	// 🔴 Fenced on the row still being `building`. Not defensive: it is where
	// execution fencing lands on the catalog side, and it is what makes a
	// crash between the bytes and the commit leave a row that every resolving
	// query already refuses to see.
	//
	// 🔴 It also takes the template's build off the queue, in the same
	// transaction. Nothing else does on the success path, and a build that
	// stays on the queue after it has succeeded holds a slot under the
	// cluster ceiling for ever.
	CommitSnapshot(ctx context.Context, in CommitInput) (CommitOutcome, error)

	// FailSnapshot moves a row to `error` with a reason — transaction C.
	//
	// 🔴 Not the same thing as a publish that failed. A publish that produced
	// bytes the node still holds ends `ready` + unpublished (see
	// CommitInput.Published); this is for a capture or a build that produced
	// nothing to run at all.
	FailSnapshot(ctx context.Context, in FailInput) (FailOutcome, error)

	// GetSnapshot reads one row by id or alias. A nil row is "no such
	// snapshot", which is an answer and not an error.
	GetSnapshot(ctx context.Context, clusterID, idOrAlias string, opts ReadOptions) (*SnapshotRow, error)

	// ListSnapshots reads one keyset page.
	ListSnapshots(ctx context.Context, in ListInput) (ListPage, error)

	// DeleteSnapshot soft-deletes one row and drops the alias that pointed at
	// it. Idempotent: deleting what is already deleted is a success reporting
	// Deleted=false.
	DeleteSnapshot(ctx context.Context, clusterID, idOrAlias string, deletedAtMs int64) (Deleted, error)

	// ResolveAlias answers which snapshot an alias names, projecting the
	// origin block so a resume can be pinned without a second read.
	ResolveAlias(ctx context.Context, clusterID, alias string, onlyReady bool) (*AliasTarget, error)

	// StartBuild admits one build: the cluster-wide cap, the per-template
	// exclusion and the transition of the template row, in one transaction.
	StartBuild(ctx context.Context, in StartBuildInput) (StartBuildOutcome, error)

	// RenewBuildLease records that the builder is still alive. False means the
	// build is no longer the live one and the builder must stop.
	RenewBuildLease(ctx context.Context, in RenewBuildLeaseInput) (bool, error)

	// GetBuild reads one build row.
	//
	// 🔴 Deliberately without the ready predicate every resolving query
	// carries: this is the one read whose purpose is to see a build that is
	// still running or has failed.
	GetBuild(ctx context.Context, clusterID, buildID string) (*BuildRow, error)

	// ReapExpiredBuilds fails builds whose heartbeat has lapsed, and the
	// template rows they were holding.
	//
	// 🔴 Both halves, and that is not tidying. `builds_one_active_per_template`
	// turns a stranded build into a template nobody can ever build again, and
	// the template row stranded at `building` does the same on the other side.
	// Freeing one without the other leaves the template blocked either way.
	ReapExpiredBuilds(ctx context.Context, in ReapInput) ([]ReapedBuild, error)

	// Close releases the underlying resources.
	Close()
}

// ─────────────────────────────────────────────────────────────────────────────
// Rows
// ─────────────────────────────────────────────────────────────────────────────

// SnapshotRow is one catalog row: the scalar columns, plus two opaque blobs.
type SnapshotRow struct {
	SnapshotID      string
	ClusterID       string
	SourceKind      string
	SourceSandboxID string

	CPUCount    uint32
	MemoryMiB   uint32
	DiskSizeMiB uint32

	Status      string
	StatusGroup string

	// Alias is empty when nothing points here. At most one thing does —
	// `aliases_one_per_snapshot` holds it to one, because a second row would
	// make the listing join return this snapshot twice inside one page.
	Alias string

	CreatedAtMs int64
	UpdatedAtMs int64
	// SandboxStartedAtMs is nil for template rows and for rows written before
	// the column was populated.
	SandboxStartedAtMs *int64

	// CommittedPayload is a CommittedSnapshot as the node serialised it.
	// Nil means the row has no payload, which the table pins to "not ready".
	CommittedPayload []byte
	CommittedSchema  *uint32

	// BuildError is a TemplateBuildErrorReason as JSON, passed through
	// untouched.
	BuildError json.RawMessage

	// BuildStartedAtMs and BuildFinishedAtMs are projected from the `builds`
	// row when the read asked for it.
	BuildStartedAtMs  *int64
	BuildFinishedAtMs *int64

	// ── origin pinning ──────────────────────────────────────────────────
	//
	// 🔴 Projected, never filtered on. See PinOriginIfUnpublished, which is
	// the only place either value decides anything, and the schema's rule V5.
	Published    bool
	OriginNodeID string
}

// BuildRow is one row of the build queue.
type BuildRow struct {
	BuildID     string
	TemplateID  string
	ClusterID   string
	Status      string
	StatusGroup string
	NodeID      string

	HeartbeatAtMs *int64
	CreatedAtMs   int64
	StartedAtMs   *int64
	FinishedAtMs  *int64
	ErrorReason   json.RawMessage
}

// AliasTarget is what an alias resolves to, with the origin block beside it.
type AliasTarget struct {
	SnapshotID   string
	Published    bool
	OriginNodeID string
}

// Deleted is what a delete did.
type Deleted struct {
	// Deleted is false when there was nothing left to delete. Still a success.
	Deleted bool
	// Row is the row as it stood before the delete, so the caller can find the
	// artifacts to collect. Nil when Deleted is false.
	Row *SnapshotRow
}

// ReapedBuild names one build the reaper ended.
type ReapedBuild struct {
	BuildID    string
	TemplateID string
	NodeID     string
}

// ListPage is one keyset page and the position to resume from.
type ListPage struct {
	Rows []SnapshotRow
	// Next is nil on the last page.
	//
	// 🔴 Decided by reading one row more than the caller asked for, not by
	// comparing the page size to the limit. A page that happens to be exactly
	// full is not a last page, and treating it as one silently truncates every
	// listing whose total is a multiple of the limit.
	Next *Cursor
}

// Cursor is a keyset position: the last row of the previous page.
//
// 🔴 Not the public pagination token. That one is a base64url string with an
// RFC3339 rendering inside it and it is an OpenAPI response header, so its
// shape may not change. It is parsed and rendered on the node; only the two
// values it decodes to reach this side, in the units the node holds them in.
type Cursor struct {
	// CreatedAtMs is milliseconds, because the public cursor is rendered from
	// an i64 of milliseconds. Carrying it in any other unit would round-trip
	// a page boundary through a resolution it does not have.
	CreatedAtMs int64
	// SnapshotID is compared as text, never as a uuid.
	//
	// 🔴 The public cursor orders by the id's string form, and a uuid's binary
	// order is not its text order. Comparing the wrong one does not fail: it
	// skips rows at a page boundary, silently, on about half of them.
	SnapshotID string
}

// ─────────────────────────────────────────────────────────────────────────────
// Inputs
// ─────────────────────────────────────────────────────────────────────────────

// ReadOptions are the two axes every catalog read has.
type ReadOptions struct {
	// OnlyReady applies the `status_group = 'ready'` predicate.
	//
	// 🔴 Required of the caller rather than defaulted, because there is no safe
	// default. True is what stops a snapshot whose bytes are still uploading
	// from starting a VM; false is what lets the build-status endpoint see a
	// build that is running or has failed. Guessing either way breaks the other.
	OnlyReady bool
	// WithBuild joins the build row's timestamps into the answer.
	WithBuild bool
}

// BeginInput opens a catalog row.
type BeginInput struct {
	ClusterID string
	NodeID    string

	SnapshotID      string
	SourceKind      string
	SourceSandboxID string

	CPUCount    uint32
	MemoryMiB   uint32
	DiskSizeMiB uint32

	// Alias is bound in the same transaction. Empty means none. An alias a
	// live snapshot already holds refuses the whole call.
	Alias string

	// CreatedAtMs is the node's clock, not the database's.
	//
	// 🔴 Stored as the node reported it so that a row and its object-store
	// mirror carry the same instant while both exist. A server-side now()
	// would make every mirrored row differ from its twin by one RPC's latency,
	// and the double-write check is per row.
	CreatedAtMs        int64
	SandboxStartedAtMs *int64

	// Status is `waiting` or `building`. A row cannot be born `ready`: that
	// state requires a payload and there is none yet.
	Status string

	// PublishingExecutionID is written and never checked in this phase. The
	// column exists now so the phase with several writers can add the predicate
	// without backfilling a column onto rows already in flight.
	PublishingExecutionID string

	Published    bool
	OriginNodeID string

	// Paused is the registry half, absent for a template build.
	Paused *PausedBegin
}

// CommitInput flips a row to `ready`.
type CommitInput struct {
	ClusterID  string
	NodeID     string
	SnapshotID string

	// CommittedPayload is required and non-empty: the table refuses a `ready`
	// row without one, and this is the only call that produces one.
	CommittedPayload []byte
	CommittedSchema  uint32

	// The three resource fields are nil to leave the row's own values alone.
	CPUCount    *uint32
	MemoryMiB   *uint32
	DiskSizeMiB *uint32

	// Alias binds in the same transaction. Empty leaves whatever the row has,
	// which is not the same as unbinding — there is no unbind here.
	Alias       string
	UpdatedAtMs int64

	PublishingExecutionID string

	// Published false is the publish that never reached shared storage. The
	// row still goes `ready`, because the snapshot is complete and its origin
	// can start it; only the origin can.
	Published    bool
	OriginNodeID string

	// Paused is the registry half: complete_pause, or mark_local_only.
	Paused *PausedFinish
}

// FailInput moves a row to `error`.
type FailInput struct {
	ClusterID  string
	NodeID     string
	SnapshotID string
	// BuildError is required: the table refuses an `error` row without one, so
	// that "it failed" and "why" cannot come apart.
	BuildError  json.RawMessage
	UpdatedAtMs int64

	// FailActiveBuild ends the build row too, when one is in flight.
	FailActiveBuild bool

	// Paused is the registry half, mark_local_only only.
	Paused *PausedFinish
}

// ListInput is one keyset page request.
type ListInput struct {
	ClusterID string
	Filter    Filter
	// Cursor nil starts at the newest row.
	Cursor *Cursor
	// Limit zero asks for the server's default. Every value is capped.
	Limit uint32
	ReadOptions
}

// Filter is the conjunction of everything a listing may narrow by.
//
// 🔴 There is deliberately no field for `published` or `origin_node_id`.
// Filtering on them would hide a snapshot the user can still resume on its
// origin node behind "no such snapshot" — and would make the day those columns
// are dropped a day that edits every query. See the schema's rule V5.
type Filter struct {
	SourceKinds       []string
	AliasPrefix       *string
	SnapshotIDs       []string
	SnapshotIDOrAlias *string
	SourceSandboxID   *string
	// TemplateStatuses is meaningful only when OnlyReady is false. Asking for
	// `building` rows under the ready predicate is a filter that can never
	// match.
	TemplateStatuses []string
}

// StartBuildInput admits one build.
//
// 🔴 There is no heartbeat field. The first heartbeat is stamped by the
// statement that admits the build, from the database's clock — a build admitted
// without one is the row nothing can ever reap, and the partial unique index
// would hold its template shut for as long as the database lives, so it must
// not be possible to ask for one.
type StartBuildInput struct {
	ClusterID  string
	NodeID     string
	BuildID    string
	TemplateID string

	StartedAtMs int64
}

// RenewBuildLeaseInput is one heartbeat.
//
// 🔴 It carries no timestamp either, and for the reason that matters most in
// this package: what is recorded is when this process heard from the builder.
// A heartbeat stamped by the node and judged by the reaper is a comparison
// between two machines' clocks, and a node running slow would have every one of
// its builds ended while they were still running.
type RenewBuildLeaseInput struct {
	ClusterID string
	NodeID    string
	BuildID   string
}

// ReapInput bounds one reaping pass.
type ReapInput struct {
	ClusterID string
	// TTLMs is how long a build may go without being heard from. It is a
	// duration and not a deadline: "now" comes from the database, in the same
	// statement that reads the heartbeats it is compared against, so there is
	// no clock a caller could supply that would be the right one.
	TTLMs int64
}

// ─────────────────────────────────────────────────────────────────────────────
// Outcomes
// ─────────────────────────────────────────────────────────────────────────────

// Rejection is a refusal the caller has to act on, as opposed to a failure it
// can only report. It is never a gRPC status code: a code cannot carry which
// row won or what the status is now.
type Rejection string

const (
	// RejectionNotFound means no row with that id, or none not soft-deleted.
	RejectionNotFound Rejection = "not_found"
	// RejectionStatusMismatch means the fencing predicate did not match.
	RejectionStatusMismatch Rejection = "status_mismatch"
	// RejectionAliasTaken means another live snapshot holds the alias. The
	// whole write is refused rather than committed without it.
	RejectionAliasTaken Rejection = "alias_taken"
	// RejectionGenerationMismatch means the paused half moved since the caller
	// last read it. Re-read and try again.
	RejectionGenerationMismatch Rejection = "generation_mismatch"
	// RejectionExecutionSuperseded is terminal: a superseded incarnation tried
	// to write. Never retry, never publish behind it.
	RejectionExecutionSuperseded Rejection = "sandbox_execution_superseded"
	// RejectionBuildInProgress means another build holds this template.
	RejectionBuildInProgress Rejection = "build_in_progress"
	// RejectionBuildQueueFull means the cluster is at its build ceiling.
	// Retryable, unlike the one above.
	RejectionBuildQueueFull Rejection = "build_queue_full"
	// RejectionAlreadyExists means a row with this id is already there.
	RejectionAlreadyExists Rejection = "already_exists"
)

// Rejected carries the refusal plus whatever the caller needs to act on it.
type Rejected struct {
	Reason Rejection
	// ObservedStatus is set for RejectionStatusMismatch.
	ObservedStatus string
	// AliasHolder is set for RejectionAliasTaken.
	AliasHolder string
	// ActiveBuildID is set for RejectionBuildInProgress.
	ActiveBuildID string
	// ObservedGeneration is set for RejectionGenerationMismatch when the row
	// could still be read.
	ObservedGeneration *int64
}

// BeginOutcome is either a row or a refusal, never both and never neither.
type BeginOutcome struct {
	Row      *SnapshotRow
	Rejected *Rejected

	// Generation and PreviousSnapshotID come from the paused half, and only
	// from begin_pause.
	Generation         *int64
	PreviousSnapshotID string
}

// CommitOutcome is either the committed row or a refusal.
type CommitOutcome struct {
	Row      *SnapshotRow
	Rejected *Rejected
}

// FailOutcome is either the failed row or a refusal.
type FailOutcome struct {
	Row      *SnapshotRow
	Rejected *Rejected
}

// StartBuildOutcome is either the admitted build with its template row, or a
// refusal.
type StartBuildOutcome struct {
	Build    *BuildRow
	Snapshot *SnapshotRow
	Rejected *Rejected
}

// ─────────────────────────────────────────────────────────────────────────────
// The paused half
// ─────────────────────────────────────────────────────────────────────────────

// PausedHalf applies the `paused_sandboxes` side of a catalog write inside the
// catalog's own transaction.
//
// 🔴 An interface rather than a direct call, and it is not for testing. The
// statements that move a registry row have exactly one owner and it is the
// registry package; a second copy here would be two builds' worth of drift
// away from a sandbox that is paused according to one table and running
// according to the other. What the catalog owns is the transaction — the two
// halves have to be in one, which is the entire reason the catalog is served
// beside the registry rather than read by the node.
//
// A nil PausedHalf means this process serves no registry, and a write carrying
// a paused transition is refused rather than half-applied.
type PausedHalf interface {
	// Begin is begin_pause, fenced on the incarnation.
	Begin(ctx context.Context, tx pgx.Tx, in PausedBegin) (PausedBegan, error)
	// Complete is complete_pause, fenced on the generation.
	Complete(ctx context.Context, tx pgx.Tx, in PausedFinish, snapshotID string) error
	// MarkLocalOnly parks the sandbox on its origin node, keeping the row.
	MarkLocalOnly(ctx context.Context, tx pgx.Tx, in PausedFinish) error
	// ObserveGeneration reads the generation a row carries now, so a
	// conflicting caller can re-read against a number rather than blind. The
	// bool is false when the row is gone.
	ObserveGeneration(ctx context.Context, tx pgx.Tx, clusterID, sandboxID string) (int64, bool, error)
}

// PausedBegin is begin_pause's half of BeginSnapshot.
//
// ClusterID and OriginNodeID are filled in by the store from the catalog
// write's own scope rather than by the caller. 🔴 That is the invariant, not
// convenience: the registry row a pause moves and the catalog row it publishes
// into must belong to the same cluster and name the same node, and a caller
// able to state them separately is a caller able to state them differently.
type PausedBegin struct {
	ClusterID    string
	OriginNodeID string

	SandboxID string
	// Metadata is the node's own sandbox description, stored and never decoded.
	Metadata json.RawMessage
	// ExecutionID is compared, not installed: a pause is the same VM being
	// stopped, so the row must already name this incarnation.
	ExecutionID string
	// LeaseTTLMillis is the node's, zero meaning the store's default.
	LeaseTTLMillis int64
}

// PausedFinish is complete_pause's and mark_local_only's half. ClusterID is
// filled in by the store; see PausedBegin.
type PausedFinish struct {
	ClusterID        string
	SandboxID        string
	ExpectGeneration int64
	LeaseTTLMillis   int64
	// LocalOnly selects mark_local_only over complete_pause on a commit.
	LocalOnly bool
}

// PausedBegan is what the paused half hands back.
type PausedBegan struct {
	Generation int64
	// PreviousSnapshotID is the snapshot the row pointed at before, empty when
	// there was none. Nothing references it once this pause completes.
	PreviousSnapshotID string
}

// ─────────────────────────────────────────────────────────────────────────────
// Errors
// ─────────────────────────────────────────────────────────────────────────────

var (
	// ErrInvalidArgument means the caller's request was malformed — an id that
	// is not a uuid, a status this table does not have, a missing scope.
	ErrInvalidArgument = errors.New("catalog argument is invalid")

	// ErrInvalidRecord means a row exists but this build cannot make sense of
	// it. Never softened into an absence: absence is what makes a caller throw
	// artifacts away.
	ErrInvalidRecord = errors.New("catalog record is invalid")

	// ErrNoPausedHalf means a write carried a registry transition and this
	// process has no registry to apply it to. Refused rather than applied by
	// halves.
	ErrNoPausedHalf = errors.New("catalog write carries a paused transition but this process serves no paused registry")

	// ErrPausedGenerationMismatch is what a PausedHalf returns when its
	// conditional write matched no row.
	ErrPausedGenerationMismatch = errors.New("paused registry generation conflict")

	// ErrPausedExecutionFenced is what a PausedHalf returns when the row names
	// a different incarnation. 🔴 Never to be retried — a re-read hands the
	// caller the live incarnation's generation and the same write then walks
	// straight around the fence.
	ErrPausedExecutionFenced = errors.New("paused registry execution fenced")
)
