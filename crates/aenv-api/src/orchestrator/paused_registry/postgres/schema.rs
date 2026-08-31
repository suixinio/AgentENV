//! Idempotent paused-registry DDL guarded by the shared schema advisory lock.
//! Preflight refuses identity-axis violations instead of backfilling states
//! that could make a live sandbox claimable.

use anyhow::{Context, Result};
use sqlx::PgPool;
use tracing::{info, warn};

use crate::pg::GO_SCHEMA_LOCK_KEY;

/// Idempotent `paused_sandboxes` schema and indexes.
pub const SCHEMA_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS paused_sandboxes (
    sandbox_id           UUID        PRIMARY KEY,
    cluster_id           UUID        NOT NULL,
    state                TEXT        NOT NULL,
    generation           BIGINT      NOT NULL,
    origin_node_id       TEXT        NOT NULL,
    snapshot_id          UUID,
    metadata             JSONB       NOT NULL,
    paused_at            TIMESTAMPTZ NOT NULL,
    updated_at            TIMESTAMPTZ NOT NULL,
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
ALTER TABLE paused_sandboxes ADD  CONSTRAINT paused_sandboxes_execution_check
    CHECK ( (state IN ('running', 'publishing', 'resuming')) = (execution_id IS NOT NULL)
        AND (execution_id IS NULL) = (execution_started_at IS NULL) );
CREATE INDEX IF NOT EXISTS paused_sandboxes_origin_node_idx ON paused_sandboxes (origin_node_id);
CREATE INDEX IF NOT EXISTS paused_sandboxes_updated_at_idx ON paused_sandboxes (updated_at);
CREATE INDEX IF NOT EXISTS paused_sandboxes_reclaim_idx
    ON paused_sandboxes (cluster_id, sandbox_expires_at)
    WHERE state IN ('running', 'resuming') AND sandbox_expires_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS paused_sandboxes_resuming_reclaim_idx
    ON paused_sandboxes (cluster_id)
    WHERE state = 'resuming';
"#;

/// Persisted restart-grace state shared by independent leaders.
pub const GRACE_STATE_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS paused_registry_grace (
    cluster_id      UUID PRIMARY KEY,
    grace_until     TIMESTAMPTZ NOT NULL,
    downtime_secs   DOUBLE PRECISION NOT NULL,
    leases_extended BIGINT NOT NULL,
    entered_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
"#;

// Keep this the exact null-safe negation of `paused_sandboxes_execution_check`.
const EXECUTION_AXIS_VIOLATIONS_SQL: &str = "SELECT count(*) FROM paused_sandboxes \
     WHERE (state IN ('running', 'publishing', 'resuming')) IS DISTINCT FROM (execution_id IS NOT NULL) \
        OR (execution_id IS NULL) IS DISTINCT FROM (execution_started_at IS NULL)";

const ANY_ROW_SQL: &str = "SELECT count(*) FROM paused_sandboxes";

const UNDEFINED_TABLE: &str = "42P01";
const UNDEFINED_COLUMN: &str = "42703";

// Refuses rows that violate the identity axis before applying its constraint.
async fn preflight(conn: &mut sqlx::PgConnection) -> Result<()> {
    let outcome: std::result::Result<i64, sqlx::Error> =
        sqlx::query_scalar(EXECUTION_AXIS_VIOLATIONS_SQL)
            .fetch_one(&mut *conn)
            .await;

    let offending = match outcome {
        Ok(count) => count,
        Err(sqlx::Error::Database(db_err)) => {
            match db_err.code().as_deref() {
                Some(UNDEFINED_TABLE) => {
                    // A missing table is created by the DDL.
                    return Ok(());
                }
                Some(UNDEFINED_COLUMN) => {
                    // A table without identity columns predates the constraint.
                    sqlx::query_scalar(ANY_ROW_SQL)
                        .fetch_one(&mut *conn)
                        .await
                        .context("inspect paused_sandboxes before migrating")?
                }
                _ => {
                    return Err(sqlx::Error::Database(db_err))
                        .context("inspect paused_sandboxes before migrating")
                }
            }
        }
        Err(err) => return Err(err).context("inspect paused_sandboxes before migrating"),
    };

    if offending == 0 {
        return Ok(());
    }

    anyhow::bail!(
        "paused_sandboxes has {offending} row(s) that predate the identity axis (a live row \
         with no execution_id, or a parked one carrying one). This build does not backfill \
         automatically -- demoting a running row to paused would make it claimable, which is \
         manufacturing a live duplicate. Handle it per runbook (dev/test: DROP TABLE \
         paused_sandboxes, then restart this process)"
    );
}

/// Applies preflight and DDL under one session-scoped advisory lock.
///
/// Safe for concurrent startup calls and closes connections on unlock failure.
pub async fn migrate(pool: &PgPool) -> Result<()> {
    let mut conn = pool
        .acquire()
        .await
        .context("acquire a connection for the paused registry schema bootstrap")?;

    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(GO_SCHEMA_LOCK_KEY)
        .execute(&mut *conn)
        .await
        .context("lock the paused registry schema")?;

    let apply_result = apply(&mut conn).await;

    // Never return a connection with uncertain advisory-lock state to the pool.
    match sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(GO_SCHEMA_LOCK_KEY)
        .execute(&mut *conn)
        .await
    {
        Ok(_) => apply_result
            .context("ensure paused registry schema")
            .map(|()| {
                info!(target: "agentenv", "paused registry schema ready");
            }),
        Err(unlock_err) => {
            warn!(
                target: "agentenv",
                error = %unlock_err,
                "failed to release the paused registry schema lock; closing the connection \
                 instead of returning it to the pool with the lock's state uncertain"
            );
            conn.close().await.ok();
            match apply_result {
                Ok(()) => Err(unlock_err).context("release the paused registry schema lock"),
                Err(apply_err) => Err(apply_err.context(format!(
                    "ensure paused registry schema (releasing the schema lock also failed: \
                     {unlock_err})"
                ))),
            }
        }
    }
}

async fn apply(conn: &mut sqlx::PgConnection) -> Result<()> {
    preflight(conn).await?;
    // Multi-statement DDL requires the simple-query protocol.
    sqlx::raw_sql(SCHEMA_DDL)
        .execute(&mut *conn)
        .await
        .context("apply the paused_sandboxes schema")?;
    sqlx::raw_sql(GRACE_STATE_DDL)
        .execute(&mut *conn)
        .await
        .context("apply the paused_registry_grace schema")?;
    Ok(())
}

#[cfg(test)]
mod pg {
    use uuid::Uuid;

    use super::*;
    use crate::pg::harness::isolated_schema_pool_or_skip;

    #[tokio::test]
    async fn migrating_an_empty_database_creates_every_table() {
        let pool = isolated_schema_pool_or_skip!("migrating_an_empty_database_creates_every_table");
        migrate(&pool).await.expect("migration should succeed");

        let sandbox_count: i64 = sqlx::query_scalar("SELECT count(*) FROM paused_sandboxes")
            .fetch_one(&pool)
            .await
            .expect("paused_sandboxes should exist");
        assert_eq!(sandbox_count, 0);

        let grace_count: i64 = sqlx::query_scalar("SELECT count(*) FROM paused_registry_grace")
            .fetch_one(&pool)
            .await
            .expect("paused_registry_grace should exist");
        assert_eq!(grace_count, 0);
    }

    #[tokio::test]
    async fn migrating_twice_is_idempotent() {
        let pool = isolated_schema_pool_or_skip!("migrating_twice_is_idempotent");
        migrate(&pool)
            .await
            .expect("first migration should succeed");
        migrate(&pool)
            .await
            .expect("second migration should succeed");
    }

    #[tokio::test]
    async fn two_replicas_migrating_the_same_schema_concurrently_both_succeed() {
        let pool_a = isolated_schema_pool_or_skip!(
            "two_replicas_migrating_the_same_schema_concurrently_both_succeed"
        );
        let pool_b = pool_a.clone();

        let (a, b) = tokio::join!(migrate(&pool_a), migrate(&pool_b));
        a.expect("replica a's migration should succeed");
        b.expect("replica b's migration should succeed");
    }

    #[tokio::test]
    async fn preflight_refuses_a_table_with_pre_axis_violations() {
        let pool =
            isolated_schema_pool_or_skip!("preflight_refuses_a_table_with_pre_axis_violations");

        // Build the pre-identity-axis table shape.
        sqlx::raw_sql(
            "CREATE TABLE paused_sandboxes (
                sandbox_id         UUID PRIMARY KEY,
                cluster_id         UUID NOT NULL,
                state              TEXT NOT NULL,
                generation         BIGINT NOT NULL,
                origin_node_id     TEXT NOT NULL,
                snapshot_id        UUID,
                metadata           JSONB NOT NULL,
                paused_at          TIMESTAMPTZ NOT NULL,
                updated_at         TIMESTAMPTZ NOT NULL,
                claimed_by_node_id TEXT,
                lease_expires_at   TIMESTAMPTZ,
                sandbox_expires_at TIMESTAMPTZ
            )",
        )
        .execute(&pool)
        .await
        .expect("seeding the legacy-shaped table should succeed");

        sqlx::query(
            "INSERT INTO paused_sandboxes (
                sandbox_id, cluster_id, state, generation, origin_node_id, metadata,
                paused_at, updated_at
             ) VALUES ($1, $2, 'running', 1, 'node-a', '{}'::jsonb, now(), now())",
        )
        .bind(Uuid::new_v4())
        .bind(Uuid::new_v4())
        .execute(&pool)
        .await
        .expect("seeding a pre-axis running row should succeed");

        let error = migrate(&pool)
            .await
            .expect_err("a pre-axis violation must refuse the migration");
        let message = format!("{error:#}");
        assert!(
            message.contains("predate the identity axis"),
            "refusal should name the reason: {message}"
        );

        // Scope `pg_constraint` because isolated test schemas share one database.
        let has_constraint: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                SELECT 1 FROM pg_constraint c
                JOIN pg_class t ON t.oid = c.conrelid
                JOIN pg_namespace n ON n.oid = t.relnamespace
                WHERE c.conname = 'paused_sandboxes_execution_check'
                  AND n.nspname = current_schema()
             )",
        )
        .fetch_one(&pool)
        .await
        .expect("checking for the constraint should succeed");
        assert!(
            !has_constraint,
            "a refused migration must not have applied the constraint it refused over"
        );

        assert!(
            !message.contains("paused_sandboxes_execution_check"),
            "the refusal must be preflight's own message, not the raw constraint's: {message}"
        );
    }

    #[tokio::test]
    async fn preflight_refuses_a_table_that_only_violates_the_second_conjunct() {
        let pool = isolated_schema_pool_or_skip!(
            "preflight_refuses_a_table_that_only_violates_the_second_conjunct"
        );

        // Include both axis columns but omit the constraint.
        sqlx::raw_sql(
            "CREATE TABLE paused_sandboxes (
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
            )",
        )
        .execute(&pool)
        .await
        .expect("seeding the axis-columns-present table should succeed");

        sqlx::query(
            "INSERT INTO paused_sandboxes (
                sandbox_id, cluster_id, state, generation, origin_node_id, metadata,
                paused_at, updated_at, execution_id, execution_started_at
             ) VALUES ($1, $2, 'paused', 1, 'node-a', '{}'::jsonb, now(), now(), NULL, now())",
        )
        .bind(Uuid::new_v4())
        .bind(Uuid::new_v4())
        .execute(&pool)
        .await
        .expect("seeding a second-conjunct-only violation should succeed");

        let error = migrate(&pool)
            .await
            .expect_err("a second-conjunct-only violation must refuse the migration");
        let message = format!("{error:#}");
        assert!(
            message.contains("predate the identity axis"),
            "the refusal must be preflight's own message, naming the reason, not a raw \
             constraint-violation error from the ALTER TABLE that follows it: {message}"
        );
        assert!(
            !message.contains("paused_sandboxes_execution_check"),
            "preflight must have caught this before the ALTER ran at all: {message}"
        );
    }
}
