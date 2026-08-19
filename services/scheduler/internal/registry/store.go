package registry

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"go.uber.org/zap"
)

// Store is the write side of the paused registry: the whole of what the nodes
// used to do against PostgreSQL themselves, moved behind one process.
//
// It deliberately mirrors the node-side Rust trait method for method
// (`src/orchestrator/paused_registry/mod.rs`) rather than the five RPCs that
// carry it. The wire contract is allowed to be narrower — several of these
// collapse into one `TransitionSandbox` — but the semantics are not, because
// the tests that prove this translation correct are the node's own tests, and
// they are written against these operations. A Go interface shaped like the
// RPCs would leave those tests with nothing to attach to.
//
// Every method takes clusterID explicitly. The node scopes each statement with
// a `WHERE cluster_id = $n` rather than scoping the connection, and one cluster
// reaching another's sandboxes is a case its tests cover; carrying the scope in
// the call signature is what keeps that property from being an implementation
// detail somebody can lose.
type Store interface {
	// BeginPause records that a sandbox is being paused, returning the
	// generation the caller must quote to finish and the snapshot the row
	// pointed at before, which is now nobody's.
	BeginPause(ctx context.Context, in BeginPauseInput) (BeganPause, error)

	// CompletePause publishes the snapshot a pause produced. The row moves to
	// `paused` and becomes claimable by any node.
	CompletePause(ctx context.Context, clusterID, sandboxID string, expectGeneration int64, snapshotID string) error

	// MarkLocalOnly records that the snapshot never reached the repository, so
	// only the origin node can bring the sandbox back.
	//
	// A no-op update is an error, not a success: it means the row moved on
	// while the pause was failing, and the caller's idea of what it holds is
	// wrong.
	MarkLocalOnly(ctx context.Context, clusterID, sandboxID string, expectGeneration int64) error

	// Get returns one row. The bool is false only when the statement ran and
	// matched nothing.
	Get(ctx context.Context, clusterID, sandboxID string) (Entry, bool, error)

	// GetMany returns the rows that exist among the requested ids.
	//
	// 🔴 A sandbox missing from the returned map has no row — the same answer
	// Get gives as false, and never "we did not look". The caller deletes local
	// artifacts on the strength of that absence, so any failure at all must be
	// an error rather than a shorter map.
	GetMany(ctx context.Context, clusterID string, sandboxIDs []string) (map[string]Entry, error)

	// ClaimForResume takes ownership of a sandbox so nodeID can bring it back.
	//
	// The three-way test lives here (see the ResumeClaim doc): a published
	// snapshot may be taken by anybody, an unpublished one only after its
	// holder's lease lapses, and a live sandbox never.
	ClaimForResume(ctx context.Context, clusterID, sandboxID, nodeID string) (ResumeClaim, error)

	// ReleaseClaim hands a claimed sandbox back without resuming it.
	ReleaseClaim(ctx context.Context, clusterID, sandboxID string, expectGeneration int64) error

	// RenewLease extends the lease on every sandbox nodeID reports holding, and
	// records each one's current deadline. Returns how many rows it renewed.
	//
	// The deadline travels with the renewal rather than being read off the row
	// because the two disagree in exactly the case that matters: a resume may
	// set a different timeout than the pause recorded, and callers extend
	// timeouts on live sandboxes all the time.
	RenewLease(ctx context.Context, clusterID, nodeID string, held []HeldSandbox) (uint64, error)

	// MarkRunning records that a sandbox is live on nodeID.
	//
	// 🔴 Never creates a row. A sandbox that has never been paused has no row
	// by design, and the false return says exactly that: this node holds it,
	// but the cluster is not tracking it. It also refuses when another node
	// holds the claim, which is the difference between "not tracked" and
	// "somebody else's" — and the caller needs both.
	MarkRunning(ctx context.Context, clusterID, sandboxID, nodeID string) (bool, error)

	// ReleaseNodeHoldings frees the rows a previous process on this same
	// machine was holding when it died.
	//
	// 🔴 The evidence is positional, not temporal: a row saying "running on
	// this node" being read by a process that has just started and holds
	// nothing can only have been written by a previous process on this machine,
	// and its sandboxes were its children.
	ReleaseNodeHoldings(ctx context.Context, clusterID, nodeID string) (ReleasedHoldings, error)

	// Remove deletes a row outright.
	Remove(ctx context.Context, clusterID, sandboxID string) error

	// ReclaimExpiredHoldings frees rows whose holder has stopped renewing *and*
	// whose sandbox has outlived its own deadline.
	//
	// 🔴 Both conditions, never one. The deadline is what makes this safe: a
	// lapsed lease alone says only that the holder cannot reach the database,
	// which a partitioned node — still running every sandbox it has — satisfies
	// exactly as well as a dead one.
	//
	// This is the cluster's backstop and belongs to whoever owns the database,
	// so it is driven by a timer here rather than exposed over the wire.
	ReclaimExpiredHoldings(ctx context.Context, clusterID string) (ReleasedHoldings, error)

	// Migrate brings the table to the shape this build expects.
	//
	// Separate from construction, and construction does not connect, because
	// this process does more than serve the registry: a database that is down
	// must not stop it from routing traffic. The registry surface answers
	// UNAVAILABLE until this has succeeded — never an empty result, which would
	// read as "the cluster knows of no such sandbox".
	Migrate(ctx context.Context) error

	// WithLeaseTTL returns a view of this store that stamps leases with the
	// given duration, sharing the underlying pool.
	//
	// The view's Close is a no-op: it borrows the pool rather than owning it,
	// and a per-request view closing the pool out from under the store it came
	// from is the failure this note exists to prevent.
	//
	// 🔴 The lease TTL belongs to the node, not to this process. The node
	// renews on its own cadence and its configuration enforces that the TTL
	// leaves room for two missed renewals; a TTL chosen here instead would let
	// rows expire underneath a node that is renewing exactly as it was told to.
	// So each call applies the TTL its caller reported, and the store's own
	// value is only the default for callers that report none.
	WithLeaseTTL(ttl time.Duration) Store

	// Close releases the underlying resources.
	Close()
}

// Entry is a registry row as the write path sees it: every column the read
// model carries, plus the opaque metadata blob.
//
// Metadata is deliberately json.RawMessage and is never decoded here. It is the
// node's own sandbox description, written by one Rust process and read by
// another, and the control plane's only job is to hand back the bytes it was
// given. Decoding it into a Go type would drop whatever fields this build has
// not heard of — silently, because the Rust type does not deny unknown fields —
// and a sandbox missing one of them can afterwards be neither read nor claimed
// by anybody.
type Entry struct {
	Sandbox
	Metadata json.RawMessage
}

// BeginPauseInput is everything a pause needs to record about itself.
//
// SnapshotID is absent: a pause that has only begun has not produced a snapshot
// yet, and the row keeps pointing at the previous one until CompletePause
// replaces it. That is what makes a failed pause leave the sandbox recoverable
// from where it was rather than from nowhere.
type BeginPauseInput struct {
	ClusterID    string
	SandboxID    string
	OriginNodeID string
	Metadata     json.RawMessage
}

// BeganPause is what BeginPause hands back.
type BeganPause struct {
	// Generation the caller must quote when completing or abandoning.
	Generation int64
	// PreviousSnapshotID is the snapshot the row pointed at before this pause
	// replaced it, empty when there was none. Nothing references it once the
	// new pause completes, so deleting it is the caller's job — deferred to
	// there rather than done at resume time so a sandbox always has one durable
	// snapshot behind it while it runs.
	PreviousSnapshotID string
}

// HeldSandbox is one sandbox a node reports holding, and when that sandbox is
// currently due to end.
//
// A nil ExpiresAt means the sandbox has no deadline at all, which is not the
// same as unknown: it was asked never to expire, and reclamation leaves it
// alone forever.
type HeldSandbox struct {
	SandboxID string
	ExpiresAt *time.Time
}

// ReleasedHoldings counts what a release or reclamation did.
//
// The two are reported apart because they mean different things to an operator:
// Released rows are recoverable and come back on the next resume, Discarded
// ones are gone — no snapshot was ever published for them, so nothing remains
// to rebuild the sandbox from.
type ReleasedHoldings struct {
	Released  uint64
	Discarded uint64
}

// ClaimOutcome is which of the four answers a claim got.
type ClaimOutcome string

const (
	// ClaimOutcomeClaimed means the caller owns the sandbox and must either
	// resume it or release the claim.
	ClaimOutcomeClaimed ClaimOutcome = "claimed"
	// ClaimOutcomeNotFound means no row: the sandbox is unknown to the cluster.
	ClaimOutcomeNotFound ClaimOutcome = "not_found"
	// ClaimOutcomeNotReady means the snapshot is still uploading, so only the
	// origin node can serve this resume.
	ClaimOutcomeNotReady ClaimOutcome = "not_ready"
	// ClaimOutcomeConflict means somebody else has it — either live elsewhere
	// or a claim this caller lost.
	ClaimOutcomeConflict ClaimOutcome = "conflict"
)

// ResumeClaim is the outcome of trying to take a sandbox for a resume.
type ResumeClaim struct {
	Outcome ClaimOutcome

	// Entry is set only for ClaimOutcomeClaimed.
	Entry *Entry

	// PreviousState is what the row said *before* the claim moved it to
	// `resuming`. Set only for ClaimOutcomeClaimed.
	//
	// 🔴 It cannot be read off Entry. The claim is a single conditional UPDATE
	// and RETURNING hands back the row as the statement left it, so the state
	// there is always `resuming` whatever it was a moment earlier. Reading it
	// from the returned row is how this claim came to report every ordinary
	// resume as a lease takeover for months.
	//
	// The distinction is not cosmetic. StatePaused means the snapshot was
	// durable and nothing was lost. StatePublishing and StateLocalOnly mean the
	// claim overrode a node that never finished uploading, so the sandbox comes
	// back one snapshot behind and the work since then is gone — the one event
	// on this path an operator has to be able to find.
	PreviousState State

	// OriginNodeID is set for ClaimOutcomeNotReady and ClaimOutcomeConflict, so
	// the caller can say where the sandbox actually is.
	OriginNodeID string
}

var (
	// ErrGenerationConflict means a conditional write matched no row: the
	// generation the caller quoted is not the one the row carries any more.
	//
	// It is distinct from "no such row" on purpose. Both leave the table
	// unchanged, but one says another writer got there first and the caller's
	// view is stale, while the other says there is nothing to write to at all.
	ErrGenerationConflict = errors.New("registry generation conflict")

	// ErrInvalidRecord means a row exists but this build cannot make sense of
	// it — an unknown state, or a `paused` row with no snapshot to resume from.
	//
	// It is never softened into an absence. A row that cannot be decoded is a
	// row whose meaning is unknown, and the caller's response to absence is to
	// delete things.
	ErrInvalidRecord = errors.New("registry record is invalid")
)

// StoreConfig configures a Store.
type StoreConfig struct {
	DSN string
	// Logger receives the events on this path that an operator has to be able
	// to find — above all the claim that overrode a node which never finished
	// uploading, which is the one place a resume silently costs the user the
	// work since their last durable snapshot. Nil is allowed and discards them.
	Logger *zap.Logger
	// LeaseTTL is the default lease duration, used for callers that report none
	// of their own. See Store.WithLeaseTTL for why the caller's value wins.
	LeaseTTL       time.Duration
	MaxConnections int32
	QueryTimeout   time.Duration
}

// The constructor's shape is part of this seam, and is fixed here rather than
// beside its implementation because the tests that prove this translation
// correct are written against it without reading that implementation:
//
//	func NewStore(ctx context.Context, cfg StoreConfig) (Store, error)
//
// It does not connect and it does not migrate; call Migrate for that. There is
// exactly one owner of this table's shape and it is this process; a node
// migrating it as well is the mixed-mode hazard the switchover notes describe.
