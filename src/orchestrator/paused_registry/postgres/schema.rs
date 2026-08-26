//! `paused_sandboxes` schema bootstrap — a Rust port of
//! `services/scheduler/internal/registry/migrate.go`.
//!
//! 🔴 Unlike Stage B's catalog migrator
//! (`crate::snapshot::repository::backends::postgres::migrate`), this table
//! has **no** migrations-applied ledger table on the Go side, and this port
//! does not invent one. `services/scheduler/internal/registry/migrate.go`
//! never tracked "which migrations have run" — it just re-applies one
//! idempotent DDL string (`CREATE TABLE IF NOT EXISTS`,
//! `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`, `DROP CONSTRAINT IF EXISTS`
//! and `ADD CONSTRAINT`) on every call, guarded by the same session-scoped
//! advisory lock catalog's own migrator uses
//! (`crate::pg::GO_SCHEMA_LOCK_KEY`, `services/scheduler/internal/registry/
//! migrate.go:183` and `services/scheduler/internal/catalog/migrate.go:113`
//! independently declare the identical literal on purpose — "two schedulers
//! rolling over each other must not migrate concurrently... the stronger
//! reason is local: both migrations run from the same goroutine in the same
//! process, one after the other"). Stage B's Rust catalog port and this one
//! now make that three appliers sharing one key, for the same reason: any of
//! them might run against the same physical database concurrently mid-rollout.
//!
//! `preflight` is ported too: a database this build has never migrated but
//! that already carries pre-identity-axis rows (a live row with no
//! `execution_id`, or a parked row carrying one) is refused rather than
//! silently patched — see `executionAxisViolationsSQL` below and its Go
//! twin's doc comment on why an automatic backfill would let two incarnations
//! of the same sandbox run at once.

use anyhow::{Context, Result};
use sqlx::PgPool;
use tracing::{info, warn};

use crate::pg::GO_SCHEMA_LOCK_KEY;

/// `SchemaDDL` in `services/scheduler/internal/registry/migrate.go:30-96`,
/// verbatim (including the two partial indexes Fix B added:
/// `paused_sandboxes_reclaim_idx` and `paused_sandboxes_resuming_reclaim_idx`).
pub(crate) const SCHEMA_DDL: &str = r#"
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

/// Stage C's own addition, absent from the Go schema: the write surface's
/// persisted restart-grace phase. Go never needed this — `Grace` lived in the
/// one scheduler process's memory, and `RunReclaim`'s
/// `s.grace.RequireServing()` check was a plain in-process read. `--role api`
/// is N replicas: `Grace.Enter`'s equivalent runs on whichever replica newly
/// wins the reconcile leader lock, but the *reclaim* leader lock is a
/// **separate** election (`AdvisoryLockKey::PausedRegistryReclaim` vs
/// `PausedRegistryReconcile`) that may land on a different replica — so
/// "is this cluster still inside its restart grace window" has to be a fact
/// both leaders can observe, not a flag one of them holds in memory the other
/// cannot see. See `super::grace`'s own module doc for the full design.
pub(crate) const GRACE_STATE_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS paused_registry_grace (
    cluster_id      UUID PRIMARY KEY,
    grace_until     TIMESTAMPTZ NOT NULL,
    downtime_secs   DOUBLE PRECISION NOT NULL,
    leases_extended BIGINT NOT NULL,
    entered_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
"#;

/// `executionAxisViolationsSQL` (`migrate.go:106-109`), verbatim: a row whose
/// identity axis disagrees with the CHECK constraint this DDL is about to add
/// — a live state with no `execution_id`, or a parked one carrying one.
const EXECUTION_AXIS_VIOLATIONS_SQL: &str = "SELECT count(*) FROM paused_sandboxes \
     WHERE (state IN ('running', 'publishing', 'resuming')) != (execution_id IS NOT NULL)";

/// `anyRowSQL` (`migrate.go:114`).
const ANY_ROW_SQL: &str = "SELECT count(*) FROM paused_sandboxes";

/// PostgreSQL error codes `preflight` treats specially — `migrate.go:117-128`.
const UNDEFINED_TABLE: &str = "42P01";
const UNDEFINED_COLUMN: &str = "42703";

/// `preflight` (`migrate.go:146-175`), ported: refuses to migrate a database
/// that already holds rows violating the identity axis this DDL is about to
/// enforce, rather than silently deciding for the operator whether those rows
/// are safe to reinterpret. `conn` is a `&mut PgConnection` (a live
/// transaction-free session), matching the pinned connection [`migrate`]
/// already holds the schema lock on.
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
                    // Nothing there yet. The DDL below builds it correct.
                    return Ok(());
                }
                Some(UNDEFINED_COLUMN) => {
                    // The table exists but predates the identity axis (no
                    // `execution_id` column yet) -- every row is, by
                    // definition, pre-axis and therefore not a violation of a
                    // constraint that did not exist for it.
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

/// `Migrate()` (`migrate.go:199-249`), ported: a pinned connection (advisory
/// locks are session-scoped, so lock and unlock must run on the same
/// session), `pg_advisory_lock`/`pg_advisory_unlock` around `preflight` +
/// the DDL, unlocking even on failure and closing the connection outright if
/// the unlock itself fails (never returning a connection to the pool with an
/// uncertain lock state).
///
/// Idempotent and safe to call from every `--role api` replica on every
/// startup, concurrently -- matching the Go doc's own claim ("two controllers
/// rolling over each other are the pair that has to serialise now").
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

    // 🔴 Released before the outcome is reported, same as Go's applier and
    // Stage B's own `migrate::migrate` -- whatever failed above may have left
    // the connection's server-side state in a way that makes even the unlock
    // fail; if so, close the connection outright rather than let a
    // lock-holding session drift back into the pool for some other borrower
    // to inherit.
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

/// `preflight` then the DDL -- the paused_sandboxes table, then Stage C's own
/// grace-state table (see [`GRACE_STATE_DDL`]'s doc).
async fn apply(conn: &mut sqlx::PgConnection) -> Result<()> {
    preflight(conn).await?;
    // `sqlx::raw_sql`, not `sqlx::query`: `SCHEMA_DDL` is several
    // semicolon-separated statements sent as one simple-query message --
    // matching Stage B's own `apply_one` precedent
    // (`postgres::migrate::apply_one`) for the identical reason (the
    // extended/prepared-statement protocol `sqlx::query` uses refuses a
    // multi-statement body).
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

    /// Every `--role api` replica runs this at startup: it has to be safe to
    /// call twice against the same schema, whether from the same pool or two
    /// independent ones racing each other (see below).
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

    /// `6713e13 test(registry): retry a migration the test suite deadlocked
    /// itself into` is real Go history: two controllers racing to migrate the
    /// same schema is the exact scenario `pg_advisory_lock` exists to
    /// serialise, and it is worth proving directly rather than trusting the
    /// primitive by inference. Two pools sharing one physical schema (not two
    /// independent `isolated_schema_pool`s, which would not contend at all)
    /// racing `migrate()` concurrently must both succeed, serialised rather
    /// than deadlocked.
    #[tokio::test]
    async fn two_replicas_migrating_the_same_schema_concurrently_both_succeed() {
        let pool_a = isolated_schema_pool_or_skip!(
            "two_replicas_migrating_the_same_schema_concurrently_both_succeed"
        );
        // `PgPool::clone` shares the same underlying pool (and therefore the
        // same schema): each `migrate()` call still does its own independent
        // `pool.acquire()`, so this is two distinct PostgreSQL sessions
        // racing the same advisory lock and the same DDL -- exactly two
        // `--role api` replicas starting at once against one database.
        let pool_b = pool_a.clone();

        let (a, b) = tokio::join!(migrate(&pool_a), migrate(&pool_b));
        a.expect("replica a's migration should succeed");
        b.expect("replica b's migration should succeed");
    }

    /// `preflight`'s refusal: a table that predates the identity axis (no
    /// `execution_id` column at all) with a `running` row is exactly the
    /// shape it exists to catch, once the column has been retrofitted with
    /// pre-existing rows still carrying no value.
    #[tokio::test]
    async fn preflight_refuses_a_table_with_pre_axis_violations() {
        let pool =
            isolated_schema_pool_or_skip!("preflight_refuses_a_table_with_pre_axis_violations");

        // Build the pre-identity-axis shape by hand: every column this DDL's
        // `ADD COLUMN IF NOT EXISTS` list would otherwise add, minus
        // `execution_id`/`execution_started_at` -- i.e. the exact shape
        // `preflight`'s `42703` (undefined_column) branch exists for.
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

        // 🔴 And the refusal must not have left the schema half-applied: the
        // constraint that would make this row illegal must not exist yet,
        // or a later successful migration attempt would find a table it
        // cannot add that constraint to without the operator's manual
        // intervention this refusal exists to force in the first place.
        //
        // Scoped to `current_schema()` explicitly: `pg_constraint` is a
        // system catalog, not schema-scoped by `search_path` the way a plain
        // `SELECT` from a user table is -- an unscoped query here would find
        // the *other* isolated-schema tests' own
        // `paused_sandboxes_execution_check` constraints too, which was a
        // real false failure this test hit once (the harness's per-test
        // schemas share one physical database).
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
    }
}
