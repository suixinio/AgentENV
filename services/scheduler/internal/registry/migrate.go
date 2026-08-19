package registry

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// SchemaDDL is the node's own schema bootstrap, copied verbatim from
// SCHEMA_DDL in src/orchestrator/paused_registry/postgres.rs.
//
// 🔴 Verbatim, and it has to stay verbatim for as long as any node can still be
// configured with the `postgres` backend. That node runs this same script on
// every start, and two of its statements are unconditional:
//
//	ALTER TABLE ... DROP CONSTRAINT IF EXISTS paused_sandboxes_state_check;
//	ALTER TABLE ... ADD CONSTRAINT paused_sandboxes_state_check CHECK (state IN (...));
//
// So the moment this copy admits a state the node's copy does not — and one row
// is written carrying it — the next node to start cannot add its constraint
// back, and that node never comes up. The failure lands on a machine whose own
// configuration is unchanged and whose logs name a constraint nobody there
// touched, which is the worst combination a schema owner can hand somebody.
//
// A column is the same story one step removed: a new NOT NULL column without a
// default makes every insert the node still issues fail. Extending this belongs
// to the release that removes the `postgres` backend, not to this one.
//
// Everything after the CREATE TABLE brings an already-deployed table up to the
// current shape, because CREATE TABLE IF NOT EXISTS silently does nothing when
// the table exists — including when its columns and constraints are a version
// behind. Each statement is a no-op on a table that is already current, so this
// runs unchanged against a fresh database and one seeded by an earlier build.
const SchemaDDL = `
CREATE TABLE IF NOT EXISTS paused_sandboxes (
    sandbox_id     UUID        PRIMARY KEY,
    cluster_id     UUID        NOT NULL,
    state          TEXT        NOT NULL,
    generation     BIGINT      NOT NULL,
    origin_node_id TEXT        NOT NULL,
    snapshot_id    UUID,
    metadata       JSONB       NOT NULL,
    paused_at      TIMESTAMPTZ NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL
);
ALTER TABLE paused_sandboxes ADD COLUMN IF NOT EXISTS claimed_by_node_id TEXT;
ALTER TABLE paused_sandboxes DROP CONSTRAINT IF EXISTS paused_sandboxes_state_check;
ALTER TABLE paused_sandboxes ADD CONSTRAINT paused_sandboxes_state_check
    CHECK (state IN ('publishing', 'paused', 'resuming', 'local_only', 'running'));
ALTER TABLE paused_sandboxes ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ;
ALTER TABLE paused_sandboxes ADD COLUMN IF NOT EXISTS sandbox_expires_at TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS paused_sandboxes_origin_node_idx ON paused_sandboxes (origin_node_id);
CREATE INDEX IF NOT EXISTS paused_sandboxes_updated_at_idx ON paused_sandboxes (updated_at);
`

// schemaLockKey is the advisory lock the node takes around its own bootstrap,
// reused here so the two serialise against each other rather than only against
// themselves.
//
// CREATE TABLE IF NOT EXISTS is not atomic against a concurrent creator: two
// writers booting together both pass the existence check and then collide
// inside the system catalogs, which is fatal for whichever one loses. During
// the changeover a node and this controller are exactly that pair, so sharing
// the key is not tidiness — it is the only thing standing between a rollout and
// a node that will not start.
const schemaLockKey int64 = 0x0A6E_7653_4348_4D41

// unlockTimeout bounds the advisory unlock, which runs on a context detached
// from the caller's.
const unlockTimeout = 5 * time.Second

// Migrate creates the table and indexes, serialised cluster-wide.
//
// The lock is taken on one pinned connection because advisory locks are
// session-scoped: taking it from a pool and releasing it on a different
// connection would leave it held until the first session ends.
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

	_, applyErr := conn.Exec(ctx, SchemaDDL)

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
