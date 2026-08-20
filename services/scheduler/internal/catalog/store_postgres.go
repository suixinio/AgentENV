package catalog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

const (
	// defaultQueryTimeout bounds a statement that would otherwise run until the
	// caller's own deadline. A backstop, not a policy — the caller sets the
	// deadline that matters and context.WithTimeout keeps whichever comes
	// first.
	defaultQueryTimeout = 30 * time.Second

	// defaultMaxConcurrentBuilds is the cluster-wide ceiling, matching the
	// default e2b hangs off its tier table. We have no tenants to hang it off,
	// so the subject of the quota is the cluster.
	defaultMaxConcurrentBuilds = 20
)

// uniqueViolation is PostgreSQL's SQLSTATE for a unique index refusing a row.
//
// On this schema it can mean two quite different things, and they must not be
// reported as each other: `builds_one_active_per_template` means somebody else
// is building this template, while `aliases_pkey` means somebody else holds
// this name. The store reads the constraint name rather than guessing.
const uniqueViolation = "23505"

// StoreConfig configures a PostgresStore.
type StoreConfig struct {
	DSN string
	// Logger receives what an operator has to be able to find later: a reaped
	// build, an admission refused by the ceiling. Nil discards them.
	Logger *zap.Logger
	// MaxConnections bounds the pool. Zero takes pgx's default.
	MaxConnections int32
	QueryTimeout   time.Duration
	// MaxConcurrentBuilds is the cluster-wide ceiling. Zero takes the default;
	// a negative value removes the ceiling, and with it the advisory lock and
	// the count that only exist to enforce one.
	MaxConcurrentBuilds int
	// Paused is the registry half of every catalog write that has one. Nil
	// means this process serves no registry, and such a write is refused rather
	// than applied by halves.
	Paused PausedHalf
}

// PostgresStore is the catalog's query layer.
//
// Every fenced write carries its precondition in its own WHERE clause, so two
// callers racing on one row resolve through the database rather than through
// application locking: exactly one statement matches and the other is told what
// the row says now.
type PostgresStore struct {
	pool                *pgxpool.Pool
	log                 *zap.Logger
	queryTimeout        time.Duration
	maxConcurrentBuilds int
	paused              PausedHalf
	ownsPool            bool
}

var _ Store = (*PostgresStore)(nil)

// NewStore builds the catalog store over the given DSN.
//
// It does not connect and it does not migrate — both are the caller's to
// sequence, for the same reason the paused registry's are: this process routes
// traffic and serves bindings, and a database that is down must not stop it.
// The catalog RPCs answer UNAVAILABLE until the migration has run, which is a
// refusal a caller can act on where an empty answer would not be.
func NewStore(ctx context.Context, cfg StoreConfig) (*PostgresStore, error) {
	dsn := strings.TrimSpace(cfg.DSN)
	if dsn == "" {
		return nil, errors.New("catalog store dsn is required")
	}
	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse catalog store dsn: %w", err)
	}
	if cfg.MaxConnections > 0 {
		poolCfg.MaxConns = cfg.MaxConnections
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("create catalog store pool: %w", err)
	}
	store := NewStoreWithPool(pool, cfg)
	store.ownsPool = true
	return store, nil
}

// NewStoreWithPool builds a store over a pool somebody else owns.
//
// 🔴 The ordinary case, not a test hook. The catalog lives in the same database
// as `paused_sandboxes` and shares its pool, because the two halves of a pause
// have to be able to reach one transaction; a second pool would be a second
// connection and the window this whole move exists to close would still be
// open.
func NewStoreWithPool(pool *pgxpool.Pool, cfg StoreConfig) *PostgresStore {
	log := cfg.Logger
	if log == nil {
		log = zap.NewNop()
	}
	timeout := cfg.QueryTimeout
	if timeout <= 0 {
		timeout = defaultQueryTimeout
	}
	max := cfg.MaxConcurrentBuilds
	if max == 0 {
		max = defaultMaxConcurrentBuilds
	}
	return &PostgresStore{
		pool:                pool,
		log:                 log,
		queryTimeout:        timeout,
		maxConcurrentBuilds: max,
		paused:              cfg.Paused,
	}
}

// Pool exposes the underlying pool so the caller can migrate through it.
func (s *PostgresStore) Pool() *pgxpool.Pool { return s.pool }

// Close drains the pool, unless the pool belongs to somebody else.
func (s *PostgresStore) Close() {
	if s.ownsPool && s.pool != nil {
		s.pool.Close()
	}
}

func (s *PostgresStore) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, s.queryTimeout)
}

// querier is the sliver of *pgxpool.Pool and pgx.Tx every statement here needs.
// It exists so a read can be made to run inside the transaction that wrote —
// the difference between "the row my statement did not match" and "whatever the
// table holds by now".
type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// begin opens a transaction whose rollback is detached from the caller.
//
// 🔴 Detached because the usual way to reach a rollback is a cancelled context,
// and a rollback that inherits the cancellation does not happen — it leaves the
// transaction open until the connection is reaped, holding whatever locks it
// took, including the build admission lock.
func (s *PostgresStore) begin(ctx context.Context, what string) (pgx.Tx, func(), error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("catalog %s: %w", what, err)
	}
	return tx, func() {
		rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(rollbackCtx)
	}, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Transaction A — BeginSnapshot
// ─────────────────────────────────────────────────────────────────────────────

// BeginSnapshot creates the catalog row before any bytes exist.
//
// 🔴 One transaction, and this is the reason the catalog is served here at all.
// The row and the paused registry's `publishing` transition commit together, so
// a pause cannot end up recorded in one table and absent from the other. There
// is no cross-process transaction anywhere in this — the node makes one call
// and this side runs both statements — which is the shape e2b arrived at as
// well, by way of a single multi-CTE statement. A transaction rather than one
// statement here because a refusal has to be *classified*, and the second read
// that classifies it must see the same table the write did.
func (s *PostgresStore) BeginSnapshot(ctx context.Context, in BeginInput) (BeginOutcome, error) {
	cluster, err := requireUUID("cluster_id", in.ClusterID)
	if err != nil {
		return BeginOutcome{}, err
	}
	snapshot, err := requireUUID("snapshot_id", in.SnapshotID)
	if err != nil {
		return BeginOutcome{}, err
	}
	if !knownSourceKind(in.SourceKind) {
		return BeginOutcome{}, fmt.Errorf("%w: source kind %q is not one this table has", ErrInvalidArgument, in.SourceKind)
	}
	sandboxSource := nilIfEmpty(in.SourceSandboxID)
	if (in.SourceKind == SourceKindSandbox) != (sandboxSource != nil) {
		// Stated as an equivalence for the same reason the table's CHECK is:
		// a template row carrying a sandbox id and a sandbox row that lost one
		// are both rows nothing downstream can interpret.
		return BeginOutcome{}, fmt.Errorf(
			"%w: source kind %q and source_sandbox_id %q disagree; a sandbox snapshot names its sandbox and a template does not",
			ErrInvalidArgument, in.SourceKind, in.SourceSandboxID)
	}
	// 🔴 A row cannot be born `ready`: that state requires a payload and there
	// is none yet. Refused rather than corrected, so a caller that believes it
	// published something finds out here rather than from a CHECK violation.
	if in.Status != StatusWaiting && in.Status != StatusBuilding {
		return BeginOutcome{}, fmt.Errorf("%w: a snapshot row opens at 'waiting' or 'building', not %q", ErrInvalidArgument, in.Status)
	}
	if err := requirePositiveResources(in.CPUCount, in.MemoryMiB, in.DiskSizeMiB); err != nil {
		return BeginOutcome{}, err
	}
	origin := nilIfEmpty(in.OriginNodeID)
	if !in.Published && origin == nil {
		// The table says the same thing with snapshots_origin_axis; saying it
		// here first means the caller is told which field is missing rather
		// than which constraint fired.
		return BeginOutcome{}, fmt.Errorf("%w: an unpublished snapshot must name the node its bytes are on", ErrInvalidArgument)
	}
	var execution *string
	if trimmed := strings.TrimSpace(in.PublishingExecutionID); trimmed != "" {
		id, err := requireUUID("publishing_execution_id", trimmed)
		if err != nil {
			return BeginOutcome{}, err
		}
		execution = &id
	}
	if in.Paused != nil {
		if s.paused == nil {
			return BeginOutcome{}, ErrNoPausedHalf
		}
		if strings.TrimSpace(in.NodeID) == "" {
			return BeginOutcome{}, fmt.Errorf("%w: a pause names the node its bytes are going to", ErrInvalidArgument)
		}
		// Scoped from the catalog write rather than from the caller: the two
		// rows this transaction touches belong to one cluster and one node by
		// construction, and there is no way to state otherwise.
		in.Paused.ClusterID = cluster
		in.Paused.OriginNodeID = strings.TrimSpace(in.NodeID)
	}

	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	tx, rollback, err := s.begin(ctx, "begin_snapshot")
	if err != nil {
		return BeginOutcome{}, err
	}
	defer rollback()

	tag, err := tx.Exec(ctx, insertSnapshotSQL,
		snapshot, cluster, in.SourceKind, sandboxSource,
		int32(in.CPUCount), int32(in.MemoryMiB), int32(in.DiskSizeMiB),
		in.Status,
		in.Published, origin,
		in.SandboxStartedAtMs, in.CreatedAtMs,
		execution,
	)
	if err != nil {
		return BeginOutcome{}, fmt.Errorf("catalog begin_snapshot: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return BeginOutcome{Rejected: &Rejected{Reason: RejectionAlreadyExists}}, nil
	}

	if in.SourceKind == SourceKindTemplate {
		if _, err := tx.Exec(ctx, insertTemplateSQL, snapshot, cluster, in.CreatedAtMs); err != nil {
			return BeginOutcome{}, fmt.Errorf("catalog begin_snapshot: %w", err)
		}
	}

	if rejected, err := s.bindAlias(ctx, tx, cluster, snapshot, in.Alias, in.CreatedAtMs); err != nil {
		return BeginOutcome{}, err
	} else if rejected != nil {
		return BeginOutcome{Rejected: rejected}, nil
	}

	out := BeginOutcome{}
	if in.Paused != nil {
		began, rejected, err := s.beginPausedHalf(ctx, tx, cluster, *in.Paused)
		if err != nil {
			return BeginOutcome{}, err
		}
		if rejected != nil {
			return BeginOutcome{Rejected: rejected}, nil
		}
		generation := began.Generation
		out.Generation = &generation
		out.PreviousSnapshotID = began.PreviousSnapshotID
	}

	row, err := readSnapshot(ctx, tx, cluster, snapshot, ReadOptions{})
	if err != nil {
		return BeginOutcome{}, err
	}
	if row == nil {
		return BeginOutcome{}, fmt.Errorf("%w: snapshot %s vanished between its insert and its read", ErrInvalidRecord, snapshot)
	}
	if err := tx.Commit(ctx); err != nil {
		return BeginOutcome{}, fmt.Errorf("catalog begin_snapshot: %w", err)
	}

	out.Row = row
	return out, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Transaction B — CommitSnapshot
// ─────────────────────────────────────────────────────────────────────────────

// CommitSnapshot flips a row to `ready`, binds its alias and finishes the
// pause, all in one transaction.
//
// 🔴 The three of them together or none. Before this existed, "the sandbox is
// paused" and "here is the snapshot it paused into" were two writes to two
// systems, and a crash between them left a sandbox recorded as paused with
// nothing in the catalog to resume it from. That window is the thing phase 2
// was for, and this method is where it closes.
func (s *PostgresStore) CommitSnapshot(ctx context.Context, in CommitInput) (CommitOutcome, error) {
	cluster, err := requireUUID("cluster_id", in.ClusterID)
	if err != nil {
		return CommitOutcome{}, err
	}
	snapshot, err := requireUUID("snapshot_id", in.SnapshotID)
	if err != nil {
		return CommitOutcome{}, err
	}
	// 🔴 Required, and refused here rather than by the CHECK. `ready` is
	// exactly the state in which a payload exists, this is the only statement
	// that produces a `ready` row, and a caller that sent nothing believes it
	// published something.
	if len(in.CommittedPayload) == 0 {
		return CommitOutcome{}, fmt.Errorf("%w: a commit carries the payload that makes the row ready, and this one is empty", ErrInvalidArgument)
	}
	origin := nilIfEmpty(in.OriginNodeID)
	if !in.Published && origin == nil {
		return CommitOutcome{}, fmt.Errorf("%w: an unpublished snapshot must name the node its bytes are on", ErrInvalidArgument)
	}
	var execution *string
	if trimmed := strings.TrimSpace(in.PublishingExecutionID); trimmed != "" {
		id, err := requireUUID("publishing_execution_id", trimmed)
		if err != nil {
			return CommitOutcome{}, err
		}
		execution = &id
	}
	if in.Paused != nil {
		if s.paused == nil {
			return CommitOutcome{}, ErrNoPausedHalf
		}
		in.Paused.ClusterID = cluster
	}

	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	tx, rollback, err := s.begin(ctx, "commit_snapshot")
	if err != nil {
		return CommitOutcome{}, err
	}
	defer rollback()

	// Bound before the flip so that a name somebody else holds refuses the
	// whole commit. Committed first and bound afterwards, a conflict would
	// leave a published snapshot the user cannot reach by the name they asked
	// for — which is what the object-store backend does today.
	if rejected, err := s.bindAlias(ctx, tx, cluster, snapshot, in.Alias, in.UpdatedAtMs); err != nil {
		return CommitOutcome{}, err
	} else if rejected != nil {
		return CommitOutcome{Rejected: rejected}, nil
	}

	tag, err := tx.Exec(ctx, commitSnapshotSQL,
		snapshot, cluster,
		in.CommittedPayload, int32(in.CommittedSchema),
		in.Published, origin,
		in.UpdatedAtMs,
		int32Ptr(in.CPUCount), int32Ptr(in.MemoryMiB), int32Ptr(in.DiskSizeMiB),
		execution,
	)
	if err != nil {
		return CommitOutcome{}, fmt.Errorf("catalog commit_snapshot: %w", err)
	}
	if tag.RowsAffected() == 0 {
		rejected, err := s.classifyMiss(ctx, tx, cluster, snapshot)
		if err != nil {
			return CommitOutcome{}, err
		}
		return CommitOutcome{Rejected: rejected}, nil
	}

	if in.Paused != nil {
		rejected, err := s.finishPausedHalf(ctx, tx, cluster, snapshot, *in.Paused)
		if err != nil {
			return CommitOutcome{}, err
		}
		if rejected != nil {
			return CommitOutcome{Rejected: rejected}, nil
		}
	}

	row, err := readSnapshot(ctx, tx, cluster, snapshot, ReadOptions{})
	if err != nil {
		return CommitOutcome{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return CommitOutcome{}, fmt.Errorf("catalog commit_snapshot: %w", err)
	}
	return CommitOutcome{Row: row}, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Transaction C — FailSnapshot
// ─────────────────────────────────────────────────────────────────────────────

// FailSnapshot records that a capture or a build produced nothing to run.
func (s *PostgresStore) FailSnapshot(ctx context.Context, in FailInput) (FailOutcome, error) {
	cluster, err := requireUUID("cluster_id", in.ClusterID)
	if err != nil {
		return FailOutcome{}, err
	}
	snapshot, err := requireUUID("snapshot_id", in.SnapshotID)
	if err != nil {
		return FailOutcome{}, err
	}
	if err := requireJSONObject("build_error", in.BuildError); err != nil {
		return FailOutcome{}, err
	}
	if in.Paused != nil {
		if s.paused == nil {
			return FailOutcome{}, ErrNoPausedHalf
		}
		if !in.Paused.LocalOnly {
			// complete_pause names a snapshot the sandbox can come back from,
			// and this call is the statement that there is none.
			return FailOutcome{}, fmt.Errorf("%w: a failed snapshot can only park its sandbox as local-only", ErrInvalidArgument)
		}
		in.Paused.ClusterID = cluster
	}

	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	tx, rollback, err := s.begin(ctx, "fail_snapshot")
	if err != nil {
		return FailOutcome{}, err
	}
	defer rollback()

	tag, err := tx.Exec(ctx, failSnapshotSQL, snapshot, cluster, []byte(in.BuildError), in.UpdatedAtMs)
	if err != nil {
		return FailOutcome{}, fmt.Errorf("catalog fail_snapshot: %w", err)
	}
	if tag.RowsAffected() == 0 {
		rejected, err := s.classifyMiss(ctx, tx, cluster, snapshot)
		if err != nil {
			return FailOutcome{}, err
		}
		return FailOutcome{Rejected: rejected}, nil
	}

	if in.FailActiveBuild {
		if _, err := tx.Exec(ctx, failActiveBuildSQL, snapshot, cluster, in.UpdatedAtMs, []byte(in.BuildError)); err != nil {
			return FailOutcome{}, fmt.Errorf("catalog fail_snapshot: %w", err)
		}
	}

	if in.Paused != nil {
		rejected, err := s.finishPausedHalf(ctx, tx, cluster, snapshot, *in.Paused)
		if err != nil {
			return FailOutcome{}, err
		}
		if rejected != nil {
			return FailOutcome{Rejected: rejected}, nil
		}
	}

	row, err := readSnapshot(ctx, tx, cluster, snapshot, ReadOptions{})
	if err != nil {
		return FailOutcome{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return FailOutcome{}, fmt.Errorf("catalog fail_snapshot: %w", err)
	}
	return FailOutcome{Row: row}, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Reads
// ─────────────────────────────────────────────────────────────────────────────

// GetSnapshot reads one row by id or alias.
//
// 🔴 The id is tried first and the alias second, rather than one being chosen
// by the shape of the string. An alias is ASCII letters, digits, hyphens and
// underscores, so a perfectly legal alias can look exactly like a uuid;
// dispatching on shape would make such an alias unreachable, and dispatching on
// a union in one statement would cost a sequential scan on the far larger
// table.
func (s *PostgresStore) GetSnapshot(ctx context.Context, clusterID, idOrAlias string, opts ReadOptions) (*SnapshotRow, error) {
	cluster, err := requireUUID("cluster_id", clusterID)
	if err != nil {
		return nil, err
	}
	value := strings.TrimSpace(idOrAlias)
	if value == "" {
		return nil, fmt.Errorf("%w: id_or_alias is required", ErrInvalidArgument)
	}

	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	if isCanonicalUUID(value) {
		row, err := queryOneSnapshot(ctx, s.pool, selectSnapshotSQL(byIDPredicate, opts), cluster, strings.ToLower(value))
		if err != nil || row != nil {
			return row, err
		}
	}
	return queryOneSnapshot(ctx, s.pool, selectSnapshotSQL(byAliasPredicate, opts), cluster, value)
}

// ResolveAlias answers which snapshot an alias names.
func (s *PostgresStore) ResolveAlias(ctx context.Context, clusterID, alias string, onlyReady bool) (*AliasTarget, error) {
	cluster, err := requireUUID("cluster_id", clusterID)
	if err != nil {
		return nil, err
	}
	name := strings.TrimSpace(alias)
	if name == "" {
		return nil, fmt.Errorf("%w: alias is required", ErrInvalidArgument)
	}

	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	var (
		target AliasTarget
		origin *string
	)
	err = s.pool.QueryRow(ctx, resolveAliasSQL(onlyReady), cluster, name).
		Scan(&target.SnapshotID, &target.Published, &origin)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("catalog resolve_alias: %w", err)
	}
	if origin != nil {
		target.OriginNodeID = *origin
	}
	return &target, nil
}

// ListSnapshots reads one keyset page.
func (s *PostgresStore) ListSnapshots(ctx context.Context, in ListInput) (ListPage, error) {
	cluster, err := requireUUID("cluster_id", in.ClusterID)
	if err != nil {
		return ListPage{}, err
	}
	in.ClusterID = cluster

	limit := clampLimit(in.Limit)
	sql, args, err := listSnapshotsSQL(in, limit)
	if err != nil {
		return ListPage{}, err
	}

	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return ListPage{}, fmt.Errorf("catalog list_snapshots: %w", err)
	}
	defer rows.Close()

	page := ListPage{Rows: make([]SnapshotRow, 0, limit)}
	for rows.Next() {
		row, err := scanSnapshot(rows)
		if err != nil {
			return ListPage{}, err
		}
		page.Rows = append(page.Rows, row)
	}
	if err := rows.Err(); err != nil {
		return ListPage{}, fmt.Errorf("catalog list_snapshots: %w", err)
	}

	// The extra row is the answer to "is there another page", and it is not
	// part of this one.
	if uint32(len(page.Rows)) > limit {
		last := page.Rows[limit-1]
		page.Rows = page.Rows[:limit]
		page.Next = &Cursor{CreatedAtMs: last.CreatedAtMs, SnapshotID: last.SnapshotID}
	}
	return page, nil
}

// GetBuild reads one build row.
func (s *PostgresStore) GetBuild(ctx context.Context, clusterID, buildID string) (*BuildRow, error) {
	cluster, err := requireUUID("cluster_id", clusterID)
	if err != nil {
		return nil, err
	}
	build, err := requireUUID("build_id", buildID)
	if err != nil {
		return nil, err
	}

	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	rows, err := s.pool.Query(ctx, getBuildSQL, cluster, build)
	if err != nil {
		return nil, fmt.Errorf("catalog get_build: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("catalog get_build: %w", err)
		}
		return nil, nil
	}
	row, err := scanBuild(rows)
	if err != nil {
		return nil, err
	}
	return &row, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Delete
// ─────────────────────────────────────────────────────────────────────────────

// DeleteSnapshot soft-deletes one row, its template half, and the alias that
// named it.
//
// All three in one transaction. `templates` carries its own deleted_at_ms and
// two flags for one entity can disagree; the alias must go by hand because the
// foreign key's cascade only fires on a hard delete, and a name reserved by a
// row no read can reach would never be released.
func (s *PostgresStore) DeleteSnapshot(ctx context.Context, clusterID, idOrAlias string, deletedAtMs int64) (Deleted, error) {
	cluster, err := requireUUID("cluster_id", clusterID)
	if err != nil {
		return Deleted{}, err
	}
	value := strings.TrimSpace(idOrAlias)
	if value == "" {
		return Deleted{}, fmt.Errorf("%w: id_or_alias is required", ErrInvalidArgument)
	}

	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	tx, rollback, err := s.begin(ctx, "delete_snapshot")
	if err != nil {
		return Deleted{}, err
	}
	defer rollback()

	// Read first, and inside the transaction: the caller uses the row that
	// comes back to find the artifacts to collect, and after the alias is gone
	// there is nothing left to read it from.
	row, err := resolveSnapshotTx(ctx, tx, cluster, value)
	if err != nil {
		return Deleted{}, err
	}
	if row == nil {
		return Deleted{Deleted: false}, nil
	}

	tag, err := tx.Exec(ctx, softDeleteSnapshotSQL, row.SnapshotID, cluster, deletedAtMs)
	if err != nil {
		return Deleted{}, fmt.Errorf("catalog delete_snapshot: %w", err)
	}
	if tag.RowsAffected() == 0 {
		// Somebody deleted it between the read and the write. That is the state
		// this call was trying to reach, so it is a success reporting nothing
		// done rather than a conflict.
		return Deleted{Deleted: false}, nil
	}
	if _, err := tx.Exec(ctx, dropAliasesOfSnapshotSQL, cluster, row.SnapshotID); err != nil {
		return Deleted{}, fmt.Errorf("catalog delete_snapshot: %w", err)
	}
	if _, err := tx.Exec(ctx, softDeleteTemplateSQL, row.SnapshotID, cluster, deletedAtMs); err != nil {
		return Deleted{}, fmt.Errorf("catalog delete_snapshot: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Deleted{}, fmt.Errorf("catalog delete_snapshot: %w", err)
	}
	return Deleted{Deleted: true, Row: row}, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Build admission
// ─────────────────────────────────────────────────────────────────────────────

// StartBuild admits one build.
//
// Three gates in one transaction: the cluster-wide ceiling under an advisory
// lock, the per-template exclusion, and the template row's own transition. The
// exclusion is a partial unique index rather than a query, which is strictly
// stronger than what e2b can do — it allows several tags per template and so
// has to look first — and it closes the read-modify-write in the object-store
// backend where two concurrent POSTs both saw `waiting` and both started a VM.
func (s *PostgresStore) StartBuild(ctx context.Context, in StartBuildInput) (StartBuildOutcome, error) {
	cluster, err := requireUUID("cluster_id", in.ClusterID)
	if err != nil {
		return StartBuildOutcome{}, err
	}
	build, err := requireUUID("build_id", in.BuildID)
	if err != nil {
		return StartBuildOutcome{}, err
	}
	template, err := requireUUID("template_id", in.TemplateID)
	if err != nil {
		return StartBuildOutcome{}, err
	}
	if strings.TrimSpace(in.NodeID) == "" {
		return StartBuildOutcome{}, fmt.Errorf("%w: node_id is required: a build nobody is running cannot be reaped", ErrInvalidArgument)
	}
	if in.HeartbeatAtMs <= 0 {
		// 🔴 Refused rather than defaulted. The reaper only touches rows with a
		// heartbeat, so a build admitted without one holds its template behind
		// the unique index for as long as the database lives.
		return StartBuildOutcome{}, fmt.Errorf("%w: a build is admitted with its first heartbeat, and this one has none", ErrInvalidArgument)
	}

	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	tx, rollback, err := s.begin(ctx, "start_build")
	if err != nil {
		return StartBuildOutcome{}, err
	}
	defer rollback()

	if s.maxConcurrentBuilds > 0 {
		if _, err := tx.Exec(ctx, takeBuildAdmissionLockSQL, buildAdmissionKey); err != nil {
			return StartBuildOutcome{}, fmt.Errorf("catalog start_build: %w", err)
		}
		var active int64
		if err := tx.QueryRow(ctx, countActiveBuildsSQL, cluster).Scan(&active); err != nil {
			return StartBuildOutcome{}, fmt.Errorf("catalog start_build: %w", err)
		}
		if active >= int64(s.maxConcurrentBuilds) {
			s.log.Info("build admission refused: the cluster is at its concurrent-build ceiling",
				zap.String("cluster_id", cluster),
				zap.String("template_id", template),
				zap.Int64("active", active),
				zap.Int("ceiling", s.maxConcurrentBuilds),
			)
			return StartBuildOutcome{Rejected: &Rejected{Reason: RejectionBuildQueueFull}}, nil
		}
	}

	var holder string
	err = tx.QueryRow(ctx, activeBuildForTemplateSQL, cluster, template).Scan(&holder)
	switch {
	case err == nil:
		return StartBuildOutcome{Rejected: &Rejected{
			Reason:        RejectionBuildInProgress,
			ActiveBuildID: holder,
		}}, nil
	case errors.Is(err, pgx.ErrNoRows):
	default:
		return StartBuildOutcome{}, fmt.Errorf("catalog start_build: %w", err)
	}

	tag, err := tx.Exec(ctx, markSnapshotBuildingSQL, template, cluster, in.StartedAtMs)
	if err != nil {
		return StartBuildOutcome{}, fmt.Errorf("catalog start_build: %w", err)
	}
	if tag.RowsAffected() == 0 {
		rejected, err := s.classifyMiss(ctx, tx, cluster, template)
		if err != nil {
			return StartBuildOutcome{}, err
		}
		return StartBuildOutcome{Rejected: rejected}, nil
	}

	if _, err := tx.Exec(ctx, insertBuildSQL,
		build, template, cluster, strings.TrimSpace(in.NodeID), in.HeartbeatAtMs, in.StartedAtMs,
	); err != nil {
		// Reachable only against a writer that did not take the admission lock.
		// The index is the enforcement and the read above is only what makes
		// the refusal informative, so losing the id here is acceptable where
		// losing the refusal would not be.
		if constraint, ok := uniqueViolationOn(err); ok && constraint == "builds_one_active_per_template" {
			return StartBuildOutcome{Rejected: &Rejected{Reason: RejectionBuildInProgress}}, nil
		}
		if constraint, ok := uniqueViolationOn(err); ok && constraint == "builds_pkey" {
			return StartBuildOutcome{Rejected: &Rejected{Reason: RejectionAlreadyExists}}, nil
		}
		return StartBuildOutcome{}, fmt.Errorf("catalog start_build: %w", err)
	}

	snapshot, err := readSnapshot(ctx, tx, cluster, template, ReadOptions{})
	if err != nil {
		return StartBuildOutcome{}, err
	}
	buildRow, err := readBuild(ctx, tx, cluster, build)
	if err != nil {
		return StartBuildOutcome{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return StartBuildOutcome{}, fmt.Errorf("catalog start_build: %w", err)
	}
	return StartBuildOutcome{Build: buildRow, Snapshot: snapshot}, nil
}

// RenewBuildLease records that the builder is still alive.
//
// 🔴 False is the only thing that tells a builder it has been reaped. Without
// it the reaper frees the template, a second build starts, and two builders
// publish into the same one.
func (s *PostgresStore) RenewBuildLease(ctx context.Context, in RenewBuildLeaseInput) (bool, error) {
	cluster, err := requireUUID("cluster_id", in.ClusterID)
	if err != nil {
		return false, err
	}
	build, err := requireUUID("build_id", in.BuildID)
	if err != nil {
		return false, err
	}
	node := strings.TrimSpace(in.NodeID)
	if node == "" {
		return false, fmt.Errorf("%w: node_id is required", ErrInvalidArgument)
	}

	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	tag, err := s.pool.Exec(ctx, renewBuildLeaseSQL, cluster, node, build, in.HeartbeatAtMs)
	if err != nil {
		return false, fmt.Errorf("catalog renew_build_lease: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}

// ReapExpiredBuilds ends builds whose heartbeat has lapsed, and frees the
// template rows they were holding.
func (s *PostgresStore) ReapExpiredBuilds(ctx context.Context, in ReapInput) ([]ReapedBuild, error) {
	cluster, err := requireUUID("cluster_id", in.ClusterID)
	if err != nil {
		return nil, err
	}
	if in.TTLMs <= 0 {
		return nil, fmt.Errorf("%w: a reaping pass with no TTL would end every live build", ErrInvalidArgument)
	}

	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	tx, rollback, err := s.begin(ctx, "reap_builds")
	if err != nil {
		return nil, err
	}
	defer rollback()

	rows, err := tx.Query(ctx, reapBuildsSQL, cluster, in.NowMs, in.NowMs-in.TTLMs, []byte(reapedBuildError))
	if err != nil {
		return nil, fmt.Errorf("catalog reap_builds: %w", err)
	}
	reaped := make([]ReapedBuild, 0, 4)
	for rows.Next() {
		var (
			r    ReapedBuild
			node *string
		)
		if err := rows.Scan(&r.BuildID, &r.TemplateID, &node); err != nil {
			rows.Close()
			return nil, fmt.Errorf("catalog reap_builds: %w", err)
		}
		if node != nil {
			r.NodeID = *node
		}
		reaped = append(reaped, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("catalog reap_builds: %w", err)
	}
	if len(reaped) == 0 {
		return nil, nil
	}

	templates := make([]string, 0, len(reaped))
	for _, r := range reaped {
		templates = append(templates, r.TemplateID)
	}
	if _, err := tx.Exec(ctx, failReapedTemplatesSQL, cluster, in.NowMs, []byte(reapedBuildError), templates); err != nil {
		return nil, fmt.Errorf("catalog reap_builds: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("catalog reap_builds: %w", err)
	}

	for _, r := range reaped {
		s.log.Warn("build reaped: its heartbeat lapsed",
			zap.String("cluster_id", cluster),
			zap.String("build_id", r.BuildID),
			zap.String("template_id", r.TemplateID),
			zap.String("node_id", r.NodeID),
		)
	}
	return reaped, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Shared pieces
// ─────────────────────────────────────────────────────────────────────────────

// bindAlias claims a name inside the caller's transaction, or refuses.
//
// Binding a name this snapshot already holds is a no-op success: the two
// callers that bind — opening a row and committing it — are both allowed to
// name the same alias, and a retry of either must not become a conflict with
// itself.
func (s *PostgresStore) bindAlias(ctx context.Context, tx pgx.Tx, cluster, snapshot, alias string, atMs int64) (*Rejected, error) {
	name := strings.TrimSpace(alias)
	if name == "" {
		return nil, nil
	}

	// A rename is one row moving, not two rows existing: the unique index over
	// snapshot_id refuses the second, and that refusal is not the conflict the
	// API reports.
	if _, err := tx.Exec(ctx, releaseOtherAliasesSQL, cluster, snapshot, name); err != nil {
		return nil, fmt.Errorf("catalog bind_alias: %w", err)
	}

	tag, err := tx.Exec(ctx, bindAliasSQL, cluster, name, snapshot, atMs)
	if err != nil {
		return nil, fmt.Errorf("catalog bind_alias: %w", err)
	}
	if tag.RowsAffected() > 0 {
		return nil, nil
	}

	var holder string
	if err := tx.QueryRow(ctx, aliasHolderSQL, cluster, name).Scan(&holder); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// The row that made the insert conflict is gone already. Nothing to
			// report a conflict against and nothing bound either, so say so
			// rather than reporting a holder that does not exist.
			return nil, fmt.Errorf("%w: alias %q refused an insert and then vanished", ErrInvalidRecord, name)
		}
		return nil, fmt.Errorf("catalog bind_alias: %w", err)
	}
	if strings.EqualFold(holder, snapshot) {
		return nil, nil
	}
	return &Rejected{Reason: RejectionAliasTaken, AliasHolder: holder}, nil
}

// beginPausedHalf runs begin_pause inside the catalog's transaction.
func (s *PostgresStore) beginPausedHalf(ctx context.Context, tx pgx.Tx, cluster string, in PausedBegin) (PausedBegan, *Rejected, error) {
	began, err := s.paused.Begin(ctx, tx, in)
	switch {
	case err == nil:
		return began, nil, nil
	case errors.Is(err, ErrPausedExecutionFenced):
		return PausedBegan{}, &Rejected{Reason: RejectionExecutionSuperseded}, nil
	case errors.Is(err, ErrPausedGenerationMismatch):
		return PausedBegan{}, s.observeGeneration(ctx, tx, cluster, in.SandboxID), nil
	default:
		return PausedBegan{}, nil, err
	}
}

// finishPausedHalf runs complete_pause or mark_local_only inside the catalog's
// transaction.
//
// 🔴 Which of the two it is stays a fact about the sandbox and is not copied
// into the catalog. The catalog answers one question — can anybody else start
// this — and `publishing` and `local_only` give it the same answer. Copying the
// distinction here would put one state machine in two tables.
func (s *PostgresStore) finishPausedHalf(ctx context.Context, tx pgx.Tx, cluster, snapshot string, in PausedFinish) (*Rejected, error) {
	var err error
	if in.LocalOnly {
		err = s.paused.MarkLocalOnly(ctx, tx, in)
	} else {
		err = s.paused.Complete(ctx, tx, in, snapshot)
	}
	switch {
	case err == nil:
		return nil, nil
	case errors.Is(err, ErrPausedExecutionFenced):
		return &Rejected{Reason: RejectionExecutionSuperseded}, nil
	case errors.Is(err, ErrPausedGenerationMismatch):
		return s.observeGeneration(ctx, tx, cluster, in.SandboxID), nil
	default:
		return nil, err
	}
}

// observeGeneration turns a generation conflict into a refusal the caller can
// re-read against, rather than one it has to guess at.
func (s *PostgresStore) observeGeneration(ctx context.Context, tx pgx.Tx, cluster, sandbox string) *Rejected {
	rejected := &Rejected{Reason: RejectionGenerationMismatch}
	generation, ok, err := s.paused.ObserveGeneration(ctx, tx, cluster, sandbox)
	if err != nil {
		// The conflict is the answer; failing to enrich it is not a reason to
		// turn a refusal the caller can act on into an error it cannot.
		s.log.Warn("could not read the generation behind a catalog conflict",
			zap.String("sandbox_id", sandbox), zap.Error(err))
		return rejected
	}
	if ok {
		rejected.ObservedGeneration = &generation
	}
	return rejected
}

// classifyMiss says why a fenced write matched nothing.
//
// 🔴 The three answers are different actions. "No such row" means the caller is
// naming something that never existed or has been deleted; "the status is X"
// means somebody else moved it and the caller can decide whether X is a state
// it can work from. Collapsing them into one refusal makes both undecidable.
func (s *PostgresStore) classifyMiss(ctx context.Context, q querier, cluster, snapshot string) (*Rejected, error) {
	var (
		status  string
		deleted *int64
	)
	err := q.QueryRow(ctx, observedSnapshotStatusSQL, snapshot, cluster).Scan(&status, &deleted)
	if errors.Is(err, pgx.ErrNoRows) {
		return &Rejected{Reason: RejectionNotFound}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("catalog classify: %w", err)
	}
	if deleted != nil {
		return &Rejected{Reason: RejectionNotFound, ObservedStatus: status}, nil
	}
	return &Rejected{Reason: RejectionStatusMismatch, ObservedStatus: status}, nil
}

// resolveSnapshotTx reads one row by id or alias inside a transaction.
func resolveSnapshotTx(ctx context.Context, q querier, cluster, idOrAlias string) (*SnapshotRow, error) {
	if isCanonicalUUID(idOrAlias) {
		row, err := queryOneSnapshot(ctx, q, selectSnapshotSQL(byIDPredicate, ReadOptions{}), cluster, strings.ToLower(idOrAlias))
		if err != nil || row != nil {
			return row, err
		}
	}
	return queryOneSnapshot(ctx, q, selectSnapshotSQL(byAliasPredicate, ReadOptions{}), cluster, idOrAlias)
}

// readSnapshot re-reads a row the caller just wrote, so what comes back is what
// a later read would see rather than what this process believes it wrote.
func readSnapshot(ctx context.Context, q querier, cluster, snapshot string, opts ReadOptions) (*SnapshotRow, error) {
	return queryOneSnapshot(ctx, q, selectSnapshotSQL(byIDPredicate, opts), cluster, snapshot)
}

func queryOneSnapshot(ctx context.Context, q querier, sql, cluster, value string) (*SnapshotRow, error) {
	rows, err := q.Query(ctx, sql, cluster, value)
	if err != nil {
		return nil, fmt.Errorf("catalog read snapshot: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("catalog read snapshot: %w", err)
		}
		return nil, nil
	}
	row, err := scanSnapshot(rows)
	if err != nil {
		return nil, err
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("catalog read snapshot: %w", err)
	}
	return &row, nil
}

func readBuild(ctx context.Context, q querier, cluster, build string) (*BuildRow, error) {
	rows, err := q.Query(ctx, getBuildSQL, cluster, build)
	if err != nil {
		return nil, fmt.Errorf("catalog read build: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("catalog read build: %w", err)
		}
		return nil, nil
	}
	row, err := scanBuild(rows)
	if err != nil {
		return nil, err
	}
	return &row, nil
}

// scanSnapshot decodes one row of snapshotColumns followed by the two build
// timestamps.
//
// 🔴 The status is checked against the four this build knows and *kept* when it
// is not one of them, rather than being flattened. A value this build has not
// heard of came from a database whose CHECK constraint has moved on, and the
// caller can render it; turning it into something plausible would report a
// state that is not the row's.
func scanSnapshot(rows pgx.Rows) (SnapshotRow, error) {
	var (
		row             SnapshotRow
		sourceSandboxID *string
		alias           *string
		cpu             int32
		memory          int32
		disk            int32
		committedSchema *int32
		originNodeID    *string
		committed       []byte
		buildError      []byte
	)
	if err := rows.Scan(
		&row.SnapshotID,
		&row.ClusterID,
		&row.SourceKind,
		&sourceSandboxID,
		&cpu,
		&memory,
		&disk,
		&row.Status,
		&row.StatusGroup,
		&alias,
		&row.CreatedAtMs,
		&row.UpdatedAtMs,
		&row.SandboxStartedAtMs,
		&committed,
		&committedSchema,
		&buildError,
		&row.Published,
		&originNodeID,
		&row.BuildStartedAtMs,
		&row.BuildFinishedAtMs,
	); err != nil {
		return SnapshotRow{}, fmt.Errorf("decode catalog row: %w", err)
	}

	if sourceSandboxID != nil {
		row.SourceSandboxID = *sourceSandboxID
	}
	if alias != nil {
		row.Alias = *alias
	}
	if originNodeID != nil {
		row.OriginNodeID = *originNodeID
	}
	row.CPUCount = uint32(cpu)
	row.MemoryMiB = uint32(memory)
	row.DiskSizeMiB = uint32(disk)
	if committedSchema != nil {
		schema := uint32(*committedSchema)
		row.CommittedSchema = &schema
	}
	if len(committed) > 0 {
		row.CommittedPayload = committed
	}
	if len(buildError) > 0 {
		row.BuildError = json.RawMessage(buildError)
	}

	// The one invariant worth refusing a row over: `ready` with no payload
	// means something started a VM from nothing. The table's CHECK says the
	// same, and this catches a row that predates it.
	if row.Status == StatusReady && len(row.CommittedPayload) == 0 {
		return SnapshotRow{}, fmt.Errorf("%w: snapshot %s is ready and carries no payload", ErrInvalidRecord, row.SnapshotID)
	}
	return row, nil
}

func scanBuild(rows pgx.Rows) (BuildRow, error) {
	var (
		row         BuildRow
		nodeID      *string
		errorReason []byte
	)
	if err := rows.Scan(
		&row.BuildID,
		&row.TemplateID,
		&row.ClusterID,
		&row.Status,
		&row.StatusGroup,
		&nodeID,
		&row.HeartbeatAtMs,
		&row.CreatedAtMs,
		&row.StartedAtMs,
		&row.FinishedAtMs,
		&errorReason,
	); err != nil {
		return BuildRow{}, fmt.Errorf("decode catalog build row: %w", err)
	}
	if nodeID != nil {
		row.NodeID = *nodeID
	}
	if len(errorReason) > 0 {
		row.ErrorReason = json.RawMessage(errorReason)
	}
	return row, nil
}

func requirePositiveResources(cpu, memory, disk uint32) error {
	switch {
	case cpu == 0:
		return fmt.Errorf("%w: cpu_count must be positive", ErrInvalidArgument)
	case memory == 0:
		return fmt.Errorf("%w: memory_mib must be positive", ErrInvalidArgument)
	case disk == 0:
		return fmt.Errorf("%w: disk_size_mib must be positive", ErrInvalidArgument)
	default:
		return nil
	}
}

func int32Ptr(v *uint32) *int32 {
	if v == nil {
		return nil
	}
	converted := int32(*v)
	return &converted
}

// uniqueViolationOn names the index a unique violation came from.
//
// The name matters: on this schema `builds_one_active_per_template` means
// somebody else is building this template and `aliases_pkey` means somebody
// else holds this name, and a caller told the wrong one goes looking in the
// wrong place.
func uniqueViolationOn(err error) (string, bool) {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != uniqueViolation {
		return "", false
	}
	return pgErr.ConstraintName, true
}
