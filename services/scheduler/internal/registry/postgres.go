package registry

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultMaxConnections = 4
	defaultQueryTimeout   = 5 * time.Second

	// invalidTextRepresentation is what PostgreSQL answers when a string is
	// cast to uuid and is not one. No row can carry a non-uuid sandbox id, so
	// the honest translation of that error is "no such row".
	invalidTextRepresentation = "22P02"
)

// selectColumns is the read model, cast to text where the column is a uuid so
// the decoder never depends on which uuid codec the driver happens to register.
//
// It is deliberately wider than the node's own ENTRY_COLUMNS: the two lease
// columns are the whole point of reading this table centrally.
//
// 🔴 This is the second of two column lists over the same table — the write
// path has its own in store_postgres.go. Adding a column to one and not the
// other does not fail: it makes that column read as empty on whichever paths
// use the list that was missed, which for the incarnation means fencing that
// silently passes everything.
const selectColumns = `sandbox_id::text,
       cluster_id::text,
       state,
       generation,
       origin_node_id,
       claimed_by_node_id,
       snapshot_id::text,
       paused_at,
       updated_at,
       lease_expires_at,
       sandbox_expires_at,
       execution_id::text,
       execution_started_at`

// Config configures a PostgresReader. A zero DSN is not valid here; callers
// that may be switched off should use Disabled instead.
type Config struct {
	DSN string
	// ClusterID scopes every query. Empty means no filter, which is only
	// correct when this database serves exactly one cluster — two clusters
	// sharing a database and no filter would have each reconciling the
	// other's rows.
	ClusterID      string
	MaxConnections int32
	QueryTimeout   time.Duration
}

// PostgresReader reads `paused_sandboxes` and nothing else.
type PostgresReader struct {
	pool         *pgxpool.Pool
	clusterID    string
	queryTimeout time.Duration
	ready        atomic.Bool
}

// New builds a reader over the given DSN.
//
// It does not connect: a database that is down at start-up must not stop the
// scheduler from coming up, because everything else the scheduler does works
// without it. Only a DSN that cannot be parsed is an error here.
//
// `ctx` bounds the pool's background warm-up only, not the reader's lifetime;
// pass a long-lived context.
func New(ctx context.Context, cfg Config) (*PostgresReader, error) {
	dsn := strings.TrimSpace(cfg.DSN)
	if dsn == "" {
		return nil, errors.New("registry dsn is required")
	}

	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse registry dsn: %w", err)
	}
	if cfg.MaxConnections > 0 {
		poolCfg.MaxConns = cfg.MaxConnections
	} else {
		poolCfg.MaxConns = defaultMaxConnections
	}
	// Enforced at the database rather than by review: a write issued from this
	// package fails with a PostgreSQL error instead of quietly succeeding.
	// The node owns every write to this table, and a control-plane process
	// writing it behind the node's back is the one failure mode that would be
	// both silent and unrecoverable.
	poolCfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, "SET default_transaction_read_only = on")
		return err
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("create registry pool: %w", err)
	}

	queryTimeout := cfg.QueryTimeout
	if queryTimeout <= 0 {
		queryTimeout = defaultQueryTimeout
	}

	return &PostgresReader{
		pool:         pool,
		clusterID:    strings.TrimSpace(cfg.ClusterID),
		queryTimeout: queryTimeout,
	}, nil
}

// Ready reports whether any read has ever succeeded.
func (r *PostgresReader) Ready() bool { return r.ready.Load() }

// ClusterID is the cluster every query is scoped to, empty when unscoped.
func (r *PostgresReader) ClusterID() string { return r.clusterID }

// Close drains the pool.
func (r *PostgresReader) Close() {
	if r.pool != nil {
		r.pool.Close()
	}
}

// List reads every row in scope inside one read-only transaction, alongside the
// transaction's `now()`.
//
// The two come from the same transaction on purpose. Every lease judgement is a
// comparison against the database clock, and taking that clock from a second
// round trip — or worse, from this process — would compare rows against a time
// they were never written against.
func (r *PostgresReader) List(ctx context.Context) (Listing, error) {
	ctx, cancel := context.WithTimeout(ctx, r.queryTimeout)
	defer cancel()

	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return Listing{}, fmt.Errorf("begin registry read: %w", err)
	}
	defer func() {
		// Read-only and already finished either way; nothing to salvage from a
		// rollback error.
		_ = tx.Rollback(ctx)
	}()

	var dbNow time.Time
	if err := tx.QueryRow(ctx, "SELECT now()").Scan(&dbNow); err != nil {
		return Listing{}, fmt.Errorf("read registry clock: %w", err)
	}

	query := "SELECT " + selectColumns + " FROM paused_sandboxes"
	args := []any(nil)
	if r.clusterID != "" {
		query += " WHERE cluster_id = $1::uuid"
		args = append(args, r.clusterID)
	}
	query += " ORDER BY sandbox_id"

	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return Listing{}, fmt.Errorf("query registry rows: %w", err)
	}
	defer rows.Close()

	sandboxes := make([]Sandbox, 0, 64)
	for rows.Next() {
		sandbox, scanErr := scanSandbox(rows)
		if scanErr != nil {
			return Listing{}, scanErr
		}
		sandboxes = append(sandboxes, sandbox)
	}
	if err := rows.Err(); err != nil {
		return Listing{}, fmt.Errorf("read registry rows: %w", err)
	}

	r.ready.Store(true)
	return Listing{Sandboxes: sandboxes, Now: dbNow}, nil
}

// Get reads one row by sandbox id.
func (r *PostgresReader) Get(ctx context.Context, sandboxID string) (Sandbox, bool, error) {
	sandboxID = strings.TrimSpace(sandboxID)
	if sandboxID == "" {
		return Sandbox{}, false, errors.New("sandbox_id is required")
	}

	ctx, cancel := context.WithTimeout(ctx, r.queryTimeout)
	defer cancel()

	query := "SELECT " + selectColumns + " FROM paused_sandboxes WHERE sandbox_id = $1::uuid"
	args := []any{sandboxID}
	if r.clusterID != "" {
		query += " AND cluster_id = $2::uuid"
		args = append(args, r.clusterID)
	}

	sandbox, found, err := r.queryOne(ctx, query, args...)
	if err != nil {
		if isInvalidTextRepresentation(err) {
			// The id is not a uuid, so no row can match it. Answering "no row"
			// rather than an error keeps a malformed id out of the failure
			// counters that are supposed to mean the database is unhappy.
			//
			// 🔴 It does not mark the reader ready. This read failed — the
			// statement never ran — and Ready is what tells a caller it may
			// treat a missing row as the table's answer. Letting a malformed id
			// set it means one probe with a bad id unlocks authoritative
			// "no such sandbox" answers from a reader that has never seen the
			// table, and downstream that answer deletes a workspace.
			return Sandbox{}, false, nil
		}
		return Sandbox{}, false, fmt.Errorf("query registry row: %w", err)
	}

	r.ready.Store(true)
	return sandbox, found, nil
}

// queryOne runs a statement expected to match at most one row. pgx surfaces a
// query's error either from Query or from the first Next, so both are checked
// before anything is concluded from an empty result.
func (r *PostgresReader) queryOne(ctx context.Context, query string, args ...any) (Sandbox, bool, error) {
	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return Sandbox{}, false, err
	}
	defer rows.Close()

	if !rows.Next() {
		return Sandbox{}, false, rows.Err()
	}

	sandbox, err := scanSandbox(rows)
	if err != nil {
		return Sandbox{}, false, err
	}
	rows.Close()
	return sandbox, true, rows.Err()
}

func scanSandbox(rows pgx.Rows) (Sandbox, error) {
	var (
		sandbox         Sandbox
		state           string
		claimedByNodeID *string
		snapshotID      *string
		executionID     *string
	)
	err := rows.Scan(
		&sandbox.SandboxID,
		&sandbox.ClusterID,
		&state,
		&sandbox.Generation,
		&sandbox.OriginNodeID,
		&claimedByNodeID,
		&snapshotID,
		&sandbox.PausedAt,
		&sandbox.UpdatedAt,
		&sandbox.LeaseExpiresAt,
		&sandbox.SandboxExpiresAt,
		&executionID,
		&sandbox.ExecutionStartedAt,
	)
	if err != nil {
		return Sandbox{}, fmt.Errorf("decode registry row: %w", err)
	}

	sandbox.State = State(state)
	if claimedByNodeID != nil {
		sandbox.ClaimedByNodeID = *claimedByNodeID
	}
	if snapshotID != nil {
		sandbox.SnapshotID = *snapshotID
	}
	if executionID != nil {
		sandbox.ExecutionID = *executionID
	}
	return sandbox, nil
}

func isInvalidTextRepresentation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == invalidTextRepresentation
}
