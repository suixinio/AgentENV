package registry

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// SchemaDDL is this process's schema for `paused_sandboxes`.
//
// It began as the node's own bootstrap, copied verbatim from SCHEMA_DDL in
// src/orchestrator/paused_registry/postgres.rs. That file is gone — the node's
// `postgres` backend went with D11 — so this process is now the only writer and
// the only thing that applies DDL here. The historical ALTER statements are
// kept below the CREATE TABLE anyway: they are no-ops on a table that is
// already current, and deleting them would remove the only path a table an old
// node built has to reach this shape. When the operator forgot the runbook's
// DROP TABLE we want a *refusal* (see checkExecutionAxis), not a table that
// cannot grow its columns.
//
// 🔴 What is not lifted is the ban on a NOT NULL column without a default: it
// still fails every insert an older writer issues. The execution axis below is
// nullable for a stronger reason than that, though — see
// paused_sandboxes_execution_check.
const SchemaDDL = `
CREATE TABLE IF NOT EXISTS paused_sandboxes (
    sandbox_id           UUID        PRIMARY KEY,
    cluster_id           UUID        NOT NULL,
    state                TEXT        NOT NULL,
    generation           BIGINT      NOT NULL,
    origin_node_id       TEXT        NOT NULL,
    snapshot_id          UUID,
    metadata             JSONB       NOT NULL,
    paused_at            TIMESTAMPTZ NOT NULL,
    updated_at           TIMESTAMPTZ NOT NULL,
    claimed_by_node_id   TEXT,
    lease_expires_at     TIMESTAMPTZ,
    sandbox_expires_at   TIMESTAMPTZ,
    execution_id         UUID,
    execution_started_at TIMESTAMPTZ
);
ALTER TABLE paused_sandboxes ADD COLUMN IF NOT EXISTS claimed_by_node_id   TEXT;
ALTER TABLE paused_sandboxes ADD COLUMN IF NOT EXISTS lease_expires_at     TIMESTAMPTZ;
ALTER TABLE paused_sandboxes ADD COLUMN IF NOT EXISTS sandbox_expires_at   TIMESTAMPTZ;
ALTER TABLE paused_sandboxes ADD COLUMN IF NOT EXISTS execution_id         UUID;
ALTER TABLE paused_sandboxes ADD COLUMN IF NOT EXISTS execution_started_at TIMESTAMPTZ;
ALTER TABLE paused_sandboxes DROP CONSTRAINT IF EXISTS paused_sandboxes_state_check;
ALTER TABLE paused_sandboxes ADD CONSTRAINT paused_sandboxes_state_check
    CHECK (state IN ('publishing', 'paused', 'resuming', 'local_only', 'running'));
ALTER TABLE paused_sandboxes DROP CONSTRAINT IF EXISTS paused_sandboxes_execution_check;
-- The identity axis, stated as an equivalence rather than as NOT NULL.
--
-- 🔴 'resuming' is on the live side. A claim allocates the incarnation the
-- resume will run under and writes it in the same statement that takes the
-- claim, so mark_running can check the incarnation rather than only the
-- claimant. Leaving it off — the shape this constraint had before that was
-- settled — makes every cross-node resume's claim fail with 23514.
--
-- 🔴 Nullable rather than NOT NULL because "no live incarnation" is a real
-- state a parked row has to be able to express. A blanket NOT NULL forces a
-- sentinel uuid into those rows, and a sentinel is a value somebody eventually
-- compares for equality — at which point the fencing is gone, silently.
ALTER TABLE paused_sandboxes ADD  CONSTRAINT paused_sandboxes_execution_check
    CHECK ( (state IN ('running', 'publishing', 'resuming')) = (execution_id IS NOT NULL)
        AND (execution_id IS NULL) = (execution_started_at IS NULL) );
CREATE INDEX IF NOT EXISTS paused_sandboxes_origin_node_idx ON paused_sandboxes (origin_node_id);
CREATE INDEX IF NOT EXISTS paused_sandboxes_updated_at_idx ON paused_sandboxes (updated_at);
-- Serves the two reclamation statements, which run on a timer against the whole
-- cluster and until now had no index to stand on: they filter by cluster, by
-- two live states, by a lapsed lease and by a deadline that has passed, and
-- PostgreSQL answered that with a sequential scan. Affordable on a small table
-- and not what this table will be.
--
-- Partial, because the rows reclamation must never miss are a minority of a
-- large table: it only ever acts on live rows carrying a deadline. Indexing the
-- rest would cost a write on every pause for entries these statements skip.
CREATE INDEX IF NOT EXISTS paused_sandboxes_reclaim_idx
    ON paused_sandboxes (cluster_id, sandbox_expires_at)
    WHERE state IN ('running', 'resuming') AND sandbox_expires_at IS NOT NULL;
-- Serves reclaimReleasedResumingSQL, which the row above's index cannot: a
-- stuck 'resuming' row is exactly the one claim_for_resume never wrote
-- sandbox_expires_at for (see that statement's own doc), so it is NULL and
-- excluded from paused_sandboxes_reclaim_idx by that index's own predicate.
-- Narrow on purpose, the same reasoning as the index above: 'resuming' is an
-- in-flight claim, a small slice of a large table, and this only has to get a
-- reclaim pass to the handful of rows worth filtering by lease deadline
-- afterwards.
CREATE INDEX IF NOT EXISTS paused_sandboxes_resuming_reclaim_idx
    ON paused_sandboxes (cluster_id)
    WHERE state = 'resuming';
`

// executionAxisViolationsSQL counts the rows paused_sandboxes_execution_check
// would refuse. It is phrased as the constraint's own negation so the two
// cannot drift apart.
//
// The whole negation, not the "live row with no incarnation" half: a table that
// has been rolled forward, back and forward again can carry either direction,
// and a row this misses is a row that comes back as a raw constraint violation
// from the ALTER — which is the message this check exists to replace.
const executionAxisViolationsSQL = `
SELECT count(*) FROM paused_sandboxes
 WHERE (state IN ('running', 'publishing', 'resuming')) IS DISTINCT FROM (execution_id IS NOT NULL)
    OR (execution_id IS NULL) IS DISTINCT FROM (execution_started_at IS NULL)`

// anyRowSQL is the fallback for a table that predates the execution columns,
// where the statement above cannot even be parsed. Every row in such a table
// violates the constraint by construction, so the count is the whole table.
const anyRowSQL = `SELECT count(*) FROM paused_sandboxes`

const (
	// undefinedTable is what PostgreSQL answers for a table that is not there:
	// a fresh database, which has nothing to check.
	undefinedTable = "42P01"
	// undefinedColumn is what it answers for a table that exists without the
	// execution columns — a pre-phase-3 table, which is the shape this check
	// exists for.
	//
	// 🔴 Handling only 42P01 would let that shape through as an unhandled
	// driver error, which is exactly the "an error nobody recognises" outcome
	// the check is here to replace.
	undefinedColumn = "42703"
)

// preflight refuses to migrate a table carrying rows this build's CHECK
// constraint would reject.
//
// 🔴 Why it exists. ADD CONSTRAINT scans the whole table, so without this the
// operator who skipped the runbook's DROP TABLE gets
// `check constraint "paused_sandboxes_execution_check" is violated by some row`
// — a constraint born a second ago, on a machine whose own configuration is
// unchanged, with nothing anywhere saying what to do about it. That is the same
// lesson the state CHECK taught, seen from the other side.
//
// 🔴 Why it does not backfill. The only backfill that satisfies the constraint
// is turning live rows into `paused` ones, and `paused` means "any node may
// claim this" — so the repair would hand every live sandbox to the next resume
// that came along. That is manufacturing the double-live this release is about.
// Refusing is cheaper by a wide margin, and the runbook has the one command
// that fixes it.
func preflight(ctx context.Context, conn interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}) error {
	var offending int64
	err := conn.QueryRow(ctx, executionAxisViolationsSQL).Scan(&offending)
	if err != nil {
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) {
			return fmt.Errorf("inspect paused_sandboxes before migrating: %w", err)
		}
		switch pgErr.Code {
		case undefinedTable:
			// Nothing there yet. The CREATE TABLE below builds it correct.
			return nil
		case undefinedColumn:
			if err := conn.QueryRow(ctx, anyRowSQL).Scan(&offending); err != nil {
				return fmt.Errorf("inspect paused_sandboxes before migrating: %w", err)
			}
		default:
			return fmt.Errorf("inspect paused_sandboxes before migrating: %w", err)
		}
	}
	if offending == 0 {
		return nil
	}
	return fmt.Errorf(
		"paused_sandboxes 里有 %d 行是阶段 3 之前的形状（活状态的行没有 execution_id，或停放的行带着一个）。"+
			"本 build 不做自动回填 —— 把 running 行降级成 paused 会让它们变成可抢，等于人为制造双活。"+
			"按 runbook 处置（dev/test：DROP TABLE paused_sandboxes 后重启本进程）", offending)
}

// schemaLockKey is the advisory lock the node used to take around its own
// bootstrap. The node no longer applies DDL, but the key is kept: two
// controllers rolling over each other are the pair that has to serialise now,
// and CREATE TABLE IF NOT EXISTS is not atomic against a concurrent creator —
// both pass the existence check and then collide inside the system catalogs,
// which is fatal for whichever one loses.
const schemaLockKey int64 = 0x0A6E_7653_4348_4D41

// unlockTimeout bounds the advisory unlock, which runs on a context detached
// from the caller's.
const unlockTimeout = 5 * time.Second

// Migrate creates the table and indexes, serialised cluster-wide.
//
// The lock is taken on one pinned connection because advisory locks are
// session-scoped: taking it from a pool and releasing it on a different
// connection would leave it held until the first session ends.
//
// 🔴 The preflight runs inside the lock and before the DDL. Inside, so two
// controllers cannot both look at a table one of them is halfway through
// changing; before, because the statement it is protecting against is the
// ADD CONSTRAINT further down the same script.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	if pool == nil {
		return errors.New("migrate paused registry schema: no pool")
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection for registry schema bootstrap: %w", err)
	}
	released := false
	defer func() {
		if !released {
			conn.Release()
		}
	}()

	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", schemaLockKey); err != nil {
		return fmt.Errorf("lock paused registry schema: %w", err)
	}

	applyErr := preflight(ctx, conn)
	if applyErr == nil {
		_, applyErr = conn.Exec(ctx, SchemaDDL)
	}

	// Released before the DDL outcome is reported: holding the lock through an
	// error path would block every other writer until this session drops.
	//
	// The unlock runs on a context detached from the caller's, because the most
	// likely reason to be here with a failed DDL is that the caller's context
	// was cancelled — and an unlock skipped for that reason returns a connection
	// to the pool still holding a cluster-wide lock, which is a deadlock nobody
	// can see. When even the detached unlock fails the connection is destroyed
	// rather than reused: ending the session is what releases the lock.
	unlockCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), unlockTimeout)
	defer cancel()
	if _, err := conn.Exec(unlockCtx, "SELECT pg_advisory_unlock($1)", schemaLockKey); err != nil {
		hijacked := conn.Hijack()
		released = true
		_ = hijacked.Close(unlockCtx)
		if applyErr != nil {
			return fmt.Errorf("ensure paused registry schema: %w (releasing the schema lock also failed: %v)", applyErr, err)
		}
		return fmt.Errorf("release paused registry schema lock: %w", err)
	}

	if applyErr != nil {
		return fmt.Errorf("ensure paused registry schema: %w", applyErr)
	}
	return nil
}
