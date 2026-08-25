//! Brings the catalog schema in a PostgreSQL database to the shape this build
//! expects — a Rust port of `services/scheduler/internal/catalog/migrate.go`.
//!
//! Follows the same two conventions Go's applier does, on purpose: a version
//! table (`catalog_schema_migrations`) rather than a migration framework
//! (Go's own file header explains why: four files that change roughly once a
//! month do not earn a build-time tool), and a session-scoped PostgreSQL
//! advisory lock so that two processes racing to migrate the same database —
//! two `--role api` replicas starting at once, or this build racing a Go
//! `scheduler` process during a rollout — serialize rather than corrupt the
//! ledger.
//!
//! # Rolling it back
//!
//! There is no down migration. A rollback is dropping what was created:
//!
//! ```text
//! DROP TABLE IF EXISTS aliases, builds, templates, snapshots, catalog_migration_state CASCADE;
//! DROP TABLE IF EXISTS catalog_schema_migrations;
//! ```
//!
//! Both lines. The second is the one an operator forgets — it is not one of
//! the tables the change was about — and forgetting it is silent: the next
//! start believes every version is already applied, creates nothing, and
//! every catalog query then fails on a missing relation with no way forward
//! short of re-running this file's `DROP` by hand.
//!
//! # Deliberately not ported from `migrate.go`
//!
//! Go's applier also runs a `preflight`/`verifyApplied` pair that refuses to
//! start against a `snapshots` table this ledger did not create, and a ledger
//! naming relations this build no longer recognises. That is real defense
//! against a database shared with something else, but Stage B's tables are
//! new to this schema (Go's `catalog` package is this file's only ancestor,
//! and it is not deployed against the same database as this Rust build in any
//! configuration Stage B ships), so the extra guard is not load-bearing here
//! yet. Left for whoever first needs it — flagged in the Stage B report as a
//! deliberate gap, not an oversight.

use anyhow::{Context, Result};
use sqlx::postgres::PgPool;
use std::collections::HashSet;

use crate::pg::GO_SCHEMA_LOCK_KEY;

struct Migration {
    version: i32,
    name: &'static str,
    body: &'static str,
}

/// Every migration this build carries, in the order they must apply.
///
/// 🔴 Order here is asserted by a test (`migrations_are_ordered_and_versions_are_dense`)
/// rather than merely hoped for — `apply` trusts this order and does not sort
/// it, unlike Go's applier, which reads a directory and sorts by parsed
/// version. A `const` array read top to bottom is the whole directory in this
/// build, so sorting it a second time at runtime would only hide a mistake in
/// this list instead of catching it at compile-adjacent test time.
const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        name: "0001_snapshots.sql",
        body: include_str!("migrations/0001_snapshots.sql"),
    },
    Migration {
        version: 2,
        name: "0002_templates_builds_aliases.sql",
        body: include_str!("migrations/0002_templates_builds_aliases.sql"),
    },
    Migration {
        version: 3,
        name: "0003_disk_size_known_at_ready.sql",
        body: include_str!("migrations/0003_disk_size_known_at_ready.sql"),
    },
    Migration {
        version: 4,
        name: "0004_catalog_migration_state.sql",
        body: include_str!("migrations/0004_catalog_migration_state.sql"),
    },
];

/// The highest version this build carries.
pub fn latest_version() -> i32 {
    MIGRATIONS
        .last()
        .expect("MIGRATIONS is never empty")
        .version
}

const VERSION_TABLE_DDL: &str = "
CREATE TABLE IF NOT EXISTS catalog_schema_migrations (
    version       INTEGER PRIMARY KEY,
    applied_at_ms BIGINT  NOT NULL
)";

/// Brings the catalog schema to the shape this build expects.
///
/// Takes the schema lock on one pinned connection (advisory locks are
/// session-scoped, so the lock and the unlock must be the same session), runs
/// every migration this database has not recorded, and releases the lock
/// before returning either way.
///
/// 🔴 Callers should treat a failure here as a reason to refuse to serve the
/// catalog rather than to crash the process outright wherever that choice is
/// available to them — the same posture `catalog_service.go`'s `catalogGate`
/// takes. Stage B's own caller (`PostgresSnapshotCatalog::connect`) runs this
/// synchronously during backend assembly, before anything is wired to serve
/// traffic, so there is no analogous "already serving, now the schema turns
/// out to be broken" state to guard against the way Go's background goroutine
/// has to.
pub async fn migrate(pool: &PgPool) -> Result<()> {
    let mut conn = pool
        .acquire()
        .await
        .context("acquire a connection to migrate the snapshot catalog schema")?;

    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(GO_SCHEMA_LOCK_KEY)
        .execute(&mut *conn)
        .await
        .context("lock catalog schema")?;

    let apply_result = apply(&mut conn).await;

    // 🔴 Released before the outcome is reported, same as Go's applier —
    // whatever failed above may have left the connection's server-side state
    // in a way that makes even the unlock fail; if so, close the connection
    // outright rather than let a lock-holding session drift back into the
    // pool for some other borrower to inherit.
    match sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(GO_SCHEMA_LOCK_KEY)
        .execute(&mut *conn)
        .await
    {
        Ok(_) => apply_result.context("ensure catalog schema"),
        Err(unlock_err) => {
            conn.close().await.ok();
            match apply_result {
                Ok(()) => Err(unlock_err).context("release catalog schema lock"),
                Err(apply_err) => Err(apply_err.context(format!(
                    "ensure catalog schema (releasing the schema lock also failed: {unlock_err})"
                ))),
            }
        }
    }
}

async fn apply(conn: &mut sqlx::PgConnection) -> Result<()> {
    sqlx::query(VERSION_TABLE_DDL)
        .execute(&mut *conn)
        .await
        .context("ensure catalog migration ledger")?;

    let applied: HashSet<i32> = sqlx::query_scalar("SELECT version FROM catalog_schema_migrations")
        .fetch_all(&mut *conn)
        .await
        .context("read catalog migration ledger")?
        .into_iter()
        .collect();

    for migration in MIGRATIONS {
        if applied.contains(&migration.version) {
            continue;
        }
        apply_one(conn, migration).await?;
    }
    Ok(())
}

async fn apply_one(conn: &mut sqlx::PgConnection, migration: &Migration) -> Result<()> {
    use sqlx::Connection;

    let mut tx = conn
        .begin()
        .await
        .with_context(|| format!("begin transaction for catalog migration {}", migration.name))?;

    // 🔴 `sqlx::raw_sql`, not `sqlx::query`. The extended (prepared-statement)
    // protocol `query()` uses accepts exactly one statement; these files are
    // several, some containing PL/pgSQL bodies with their own internal
    // semicolons inside `$$ ... $$` dollar-quoting (0001's triggers, 0003's
    // `DO` block). `raw_sql` sends the file as one simple-query message and
    // lets PostgreSQL's own parser split it, which is the same thing `psql`
    // and Go's `pgx.Conn.Exec` do for a multi-statement body.
    sqlx::raw_sql(migration.body)
        .execute(&mut *tx)
        .await
        .with_context(|| format!("apply catalog migration {}", migration.name))?;

    sqlx::query("INSERT INTO catalog_schema_migrations (version, applied_at_ms) VALUES ($1, $2)")
        .bind(migration.version)
        .bind(now_ms())
        .execute(&mut *tx)
        .await
        .with_context(|| format!("record catalog migration {}", migration.name))?;

    tx.commit()
        .await
        .with_context(|| format!("commit catalog migration {}", migration.name))?;

    tracing::info!(
        target: "agentenv",
        version = migration.version,
        file = migration.name,
        "applied catalog schema migration"
    );
    Ok(())
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod pg {
    use super::*;
    use crate::pg::harness::isolated_schema_pool_or_skip;

    #[test]
    fn migrations_are_ordered_and_versions_are_dense() {
        let versions: Vec<i32> = MIGRATIONS.iter().map(|m| m.version).collect();
        let expected: Vec<i32> = (1..=MIGRATIONS.len() as i32).collect();
        assert_eq!(
            versions, expected,
            "migrations must be listed in order 1..N with no gaps, since `apply` trusts this \
             array's order instead of sorting it"
        );
    }

    #[test]
    fn latest_version_is_the_last_entry() {
        assert_eq!(latest_version(), MIGRATIONS.last().unwrap().version);
    }

    /// `information_schema.tables` is not filtered by `search_path` on its
    /// own, so two isolated-schema tests both creating a `snapshots` table
    /// would each see the other's row here unless the query names its own
    /// schema explicitly. `current_schema()` is what `isolated_schema_pool`'s
    /// `after_connect` hook set on this exact connection.
    async fn table_exists_in_current_schema(pool: &sqlx::PgPool, table: &str) -> bool {
        sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables \
             WHERE table_name = $1 AND table_schema = current_schema())",
        )
        .bind(table)
        .fetch_one(pool)
        .await
        .expect("querying information_schema should succeed")
    }

    const OWNED_TABLES: [&str; 6] = [
        "snapshots",
        "templates",
        "builds",
        "aliases",
        "catalog_migration_state",
        "catalog_schema_migrations",
    ];

    #[tokio::test]
    async fn migrating_an_empty_database_creates_every_table() {
        let pool = isolated_schema_pool_or_skip!("migrating_an_empty_database_creates_every_table");
        migrate(&pool).await.expect("migration should succeed");

        for table in OWNED_TABLES {
            assert!(
                table_exists_in_current_schema(&pool, table).await,
                "migration should have created table {table}"
            );
        }

        let recorded: Vec<i32> =
            sqlx::query_scalar("SELECT version FROM catalog_schema_migrations ORDER BY version")
                .fetch_all(&pool)
                .await
                .expect("reading the ledger should succeed");
        assert_eq!(recorded, vec![1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn migrating_twice_is_idempotent() {
        let pool = isolated_schema_pool_or_skip!("migrating_twice_is_idempotent");
        migrate(&pool)
            .await
            .expect("first migration should succeed");
        migrate(&pool)
            .await
            .expect("second migration should succeed and apply nothing new");

        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM catalog_schema_migrations")
            .fetch_one(&pool)
            .await
            .expect("counting the ledger should succeed");
        assert_eq!(count, MIGRATIONS.len() as i64);
    }

    /// The rollback command this module's doc comment quotes actually works,
    /// against a database this module actually migrated — not asserted only
    /// in prose.
    #[tokio::test]
    async fn the_documented_rollback_command_actually_rolls_back() {
        let pool =
            isolated_schema_pool_or_skip!("the_documented_rollback_command_actually_rolls_back");
        migrate(&pool).await.expect("migration should succeed");

        sqlx::raw_sql(
            "DROP TABLE IF EXISTS aliases, builds, templates, snapshots, catalog_migration_state CASCADE;\
             DROP TABLE IF EXISTS catalog_schema_migrations;",
        )
        .execute(&pool)
        .await
        .expect("the documented rollback command should succeed");

        for table in OWNED_TABLES {
            assert!(
                !table_exists_in_current_schema(&pool, table).await,
                "rollback should have dropped table {table}"
            );
        }

        // And migrating again from a rolled-back database works, the same as
        // a first start would.
        migrate(&pool)
            .await
            .expect("re-migrating after rollback should succeed");
    }

    /// Two competitors racing to migrate the same schema must not corrupt the
    /// ledger — the whole reason for the advisory lock. Both competitors
    /// share one isolated-schema pool (rather than one pool each) precisely
    /// so they race over the *same* tables: `pool.acquire()` still hands each
    /// concurrent `migrate()` call its own physical connection, which is
    /// exactly what two `--role api` replicas each holding their own pool
    /// would look like, without racing every *other* concurrently-running
    /// test over the shared `public` schema's table names.
    #[tokio::test]
    async fn two_concurrent_migrators_do_not_corrupt_the_ledger() {
        let pool =
            isolated_schema_pool_or_skip!("two_concurrent_migrators_do_not_corrupt_the_ledger");

        let (result_a, result_b) = tokio::join!(migrate(&pool), migrate(&pool));
        result_a.expect("first competitor should succeed");
        result_b.expect("second competitor should succeed");

        let recorded: Vec<i32> =
            sqlx::query_scalar("SELECT version FROM catalog_schema_migrations ORDER BY version")
                .fetch_all(&pool)
                .await
                .expect("reading the ledger should succeed");
        assert_eq!(
            recorded,
            vec![1, 2, 3, 4],
            "no duplicate or missing ledger rows"
        );
    }
}
