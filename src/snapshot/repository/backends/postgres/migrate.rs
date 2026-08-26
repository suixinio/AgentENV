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
//! # `preflight` / `verify_applied`, ported after all
//!
//! An earlier version of this module left Go's `preflight`/`verifyApplied`
//! pair out, on the theory that Stage B's tables are new to whatever database
//! this build touches. That theory does not hold on every cluster this build
//! ships to: a cluster already running `services/scheduler` has these exact
//! tables — `snapshots`/`templates`/`builds`/`aliases`, plus the
//! `catalog_schema_migrations` ledger itself — created by *Go's* migrate.go,
//! against the *same* PostgreSQL database this build's `[pg]` now points at.
//! `migrate()` runs unconditionally at `--role api`/`--role all` startup
//! whenever `[pg]` is configured (`build_pg_pool` in `src/bin/aenv-api.rs`), so
//! this is not a hypothetical shared-database scenario to defend against —
//! it is the ordinary shape of a cluster mid-migration off `services/scheduler`.
//!
//! Versions 1-3 are copied verbatim from Go's own migration files, so a
//! database Go already migrated reads as "already applied" here for those —
//! the two ledgers agree because the SQL is identical. What neither
//! `preflight` nor `verifyApplied` in Go was written to catch is a version
//! number the *two* migration sets disagree about (a future Go migration and
//! this build's `0004_catalog_migration_state.sql` both claiming version 4
//! but creating different things) — `verify_applied` below still catches
//! that, generalised rather than narrowed to rollbacks: it checks every
//! recorded version's *own* relations exist, not only versions this build
//! just tried to skip.

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
///
/// 🔴 Not called from production code yet — `migrate` itself walks
/// `MIGRATIONS` directly and never needs to ask its own ceiling. Kept `pub`
/// for whoever first needs a preflight check against it (see this module's
/// own "Deliberately not ported from `migrate.go`" doc on the
/// `preflight`/`verifyApplied` pair Go's applier runs and this one does
/// not), and exercised today only by
/// `migrations_are_ordered_and_versions_are_dense`.
#[allow(dead_code)]
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

    preflight(conn, &applied).await?;
    verify_applied(conn, &applied).await?;

    for migration in MIGRATIONS {
        if applied.contains(&migration.version) {
            continue;
        }
        apply_one(conn, migration).await?;
    }
    Ok(())
}

/// The relations each migration version is expected to have created, in
/// version order — a Rust port of Go's `relationsByVersion`. Version 3
/// creates no new relation (it only moves a CHECK constraint), matching Go
/// exactly; version 4 is this build's own addition, absent from Go's set.
const RELATIONS_BY_VERSION: &[(i32, &[&str])] = &[
    (1, &["snapshots"]),
    (2, &["templates", "builds", "aliases", "active_templates"]),
    (3, &[]),
    (4, &["catalog_migration_state"]),
];

fn owned_relations() -> Vec<&'static str> {
    RELATIONS_BY_VERSION
        .iter()
        .flat_map(|(_, relations)| relations.iter().copied())
        .collect()
}

/// `to_regclass` resolves against the connection's `search_path`, same as
/// Go's identical query — which is what keeps this consistent between a
/// production connection (the `public` schema) and this module's own
/// `pg::` tests (each running in its own schema-scoped connection via
/// `isolated_schema_pool`).
async fn relation_exists(conn: &mut sqlx::PgConnection, relation: &str) -> Result<bool> {
    let found: Option<String> = sqlx::query_scalar("SELECT to_regclass($1)::text")
        .bind(relation)
        .fetch_one(&mut *conn)
        .await
        .context("inspect the catalog schema")?;
    Ok(found.is_some())
}

/// Refuses to create a table that already exists and was not created by this
/// ledger — a Rust port of Go's `preflight`. Only fires on an empty ledger:
/// once anything is recorded, these relations are this ledger's by
/// construction and their existence is expected.
async fn preflight(conn: &mut sqlx::PgConnection, applied: &HashSet<i32>) -> Result<()> {
    if !applied.is_empty() {
        return Ok(());
    }

    let mut existing = Vec::new();
    for relation in owned_relations() {
        if relation_exists(conn, relation).await? {
            existing.push(relation);
        }
    }
    if existing.is_empty() {
        return Ok(());
    }

    anyhow::bail!(
        "catalog table(s) {} already exist, but catalog_schema_migrations has no rows recorded — \
         these tables were not created by this ledger. Refusing to continue: every migration \
         statement is IF NOT EXISTS, so continuing would silently leave a schema that looks \
         migrated but rejects every write on a column this ledger's migrations never added. \
         Confirm what created these tables before proceeding (a `services/scheduler` deployment \
         against the same database is one live possibility, not a hypothetical one); in dev/test, \
         DROP them and restart this process.",
        existing.join(", ")
    )
}

/// Refuses a ledger that claims work the database no longer has — a Rust
/// port of Go's `verifyApplied`, generalised the same way that function's own
/// doc already frames it: checked against every recorded version's relations,
/// not only the ones a half-finished rollback would touch. Versions this
/// build does not recognise are skipped, same as Go: that is a database a
/// newer (or differently versioned) migrator touched, and this one has no
/// idea what those files created.
async fn verify_applied(conn: &mut sqlx::PgConnection, applied: &HashSet<i32>) -> Result<()> {
    if applied.is_empty() {
        return Ok(());
    }

    let mut missing = Vec::new();
    let mut claimed = Vec::new();
    for (version, relations) in RELATIONS_BY_VERSION {
        if !applied.contains(version) {
            continue;
        }
        let mut gone = false;
        for relation in *relations {
            if !relation_exists(conn, relation).await? {
                missing.push(*relation);
                gone = true;
            }
        }
        if gone {
            claimed.push(version.to_string());
        }
    }
    if missing.is_empty() {
        return Ok(());
    }

    anyhow::bail!(
        "catalog_schema_migrations records version(s) {} as applied, but the relation(s) {} that \
         version is supposed to have created are not in the database — both cannot be true at \
         once. The usual cause is a rollback that dropped the tables without dropping \
         catalog_schema_migrations itself: the next start is then told every version is applied, \
         creates nothing, and every catalog query then fails on a missing relation, permanently. \
         Refusing to continue in this state. Finish the rollback, then restart:\n\
         \x20   DROP TABLE IF EXISTS aliases, builds, templates, snapshots, catalog_migration_state CASCADE;\n\
         \x20   DROP TABLE IF EXISTS catalog_schema_migrations;",
        claimed.join(", "),
        missing.join(", ")
    )
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

    /// The scenario this pair was reinstated for: a database already holding
    /// these tables under an empty ledger — exactly what a cluster running
    /// `services/scheduler`'s own `catalog.Migrate` looks like from this
    /// build's side, before `catalog_schema_migrations` has a single row in
    /// it that this build wrote. `migrate()` must refuse rather than march
    /// ahead over somebody else's table.
    #[tokio::test]
    async fn preflight_refuses_a_snapshots_table_that_predates_the_ledger() {
        let pool = isolated_schema_pool_or_skip!(
            "preflight_refuses_a_snapshots_table_that_predates_the_ledger"
        );
        // A stand-in for a table some other migrator created — no columns
        // this build would recognize, deliberately, since preflight only
        // checks existence, never shape.
        sqlx::query("CREATE TABLE snapshots (id INT)")
            .execute(&pool)
            .await
            .expect("creating the stand-in table should succeed");

        let error = migrate(&pool)
            .await
            .expect_err("migrate must refuse a pre-existing table under an empty ledger");
        let message = format!("{error:#}");
        assert!(
            message.contains("snapshots") && message.contains("not created by this ledger"),
            "got: {message}"
        );

        // And it did not quietly create anything else either — the ledger is
        // still empty, not partially populated.
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM catalog_schema_migrations")
            .fetch_one(&pool)
            .await
            .expect("counting the ledger should succeed");
        assert_eq!(
            count, 0,
            "a refused preflight must leave the ledger untouched"
        );
    }

    /// The half-finished-rollback shape `verifyApplied` exists for: the four
    /// owned tables dropped, `catalog_schema_migrations` left behind. A
    /// second `migrate()` must refuse rather than believe the ledger and
    /// silently create nothing.
    #[tokio::test]
    async fn verify_applied_refuses_a_ledger_whose_tables_are_gone() {
        let pool =
            isolated_schema_pool_or_skip!("verify_applied_refuses_a_ledger_whose_tables_are_gone");
        migrate(&pool).await.expect("migration should succeed");

        // The rollback command's first line, without its second — the exact
        // mistake this guard exists to catch.
        sqlx::raw_sql(
            "DROP TABLE IF EXISTS aliases, builds, templates, snapshots, catalog_migration_state CASCADE;",
        )
        .execute(&pool)
        .await
        .expect("dropping the four owned tables should succeed");

        let error = migrate(&pool)
            .await
            .expect_err("migrate must refuse a ledger whose claimed relations are gone");
        let message = format!("{error:#}");
        assert!(
            message.contains("cannot be true") || message.contains("not in the database"),
            "got: {message}"
        );
    }

    /// A version this build's own `RELATIONS_BY_VERSION` has no entry for —
    /// standing in for a migration a *different* migrator applied under a
    /// version number this build has never heard of — must not be refused.
    /// Refusing it would mean two independently-versioned migrators (this
    /// build and a future Go or Rust one) could never share a database
    /// without this build failing every start the moment the other one gets
    /// ahead.
    #[tokio::test]
    async fn an_unrecognized_version_in_the_ledger_is_skipped_not_refused() {
        let pool = isolated_schema_pool_or_skip!(
            "an_unrecognized_version_in_the_ledger_is_skipped_not_refused"
        );
        migrate(&pool).await.expect("migration should succeed");

        sqlx::query(
            "INSERT INTO catalog_schema_migrations (version, applied_at_ms) VALUES (99, 0)",
        )
        .execute(&pool)
        .await
        .expect("seeding an unrecognized version should succeed");

        migrate(&pool)
            .await
            .expect("a ledger row this build does not recognize must not block a start");
    }
}
