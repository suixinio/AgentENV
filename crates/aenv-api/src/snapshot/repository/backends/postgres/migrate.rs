//! Brings the catalog schema in a PostgreSQL database to the shape this build
//! expects.
//!
//! Two conventions, both deliberate: a version table
//! (`catalog_schema_migrations`) rather than a migration framework — a
//! directory that changes roughly once a release does not earn a build-time
//! tool — and a session-scoped PostgreSQL advisory lock so that two processes
//! racing to migrate the same database (two `aenv-api` replicas starting at
//! once) serialize rather than corrupt the ledger.
//!
//! # One version today, and the machinery for the next one
//!
//! `MIGRATIONS` carries a single entry: `0001_initial_schema.sql`, the squash
//! of the five incremental files this catalog was built up through. The
//! database they migrated was never released, so there was nothing to preserve
//! and no second migrator to agree with about version numbers.
//!
//! 🔴 That does **not** make this an "apply one file" script, and it must not
//! become one. Everything a real version 2 needs is here and stays here: the
//! ledger, the ordering/density check, `preflight`, `verify_applied`,
//! `RELATIONS_BY_VERSION`, and `apply_one`'s per-migration transaction. Adding
//! a version is appending to two `const` arrays and dropping a `.sql` file
//! beside this one.
//!
//! # Rolling it back
//!
//! There is no down migration. A rollback is dropping what was created:
//!
//! ```text
//! DROP TABLE IF EXISTS aliases, builds, templates, snapshots CASCADE;
//! DROP TABLE IF EXISTS catalog_schema_migrations;
//! ```
//!
//! Both lines. The second is the one an operator forgets — it is not one of
//! the tables the change was about — and forgetting it is silent: the next
//! start believes every version is already applied, creates nothing, and
//! every catalog query then fails on a missing relation with no way forward
//! short of re-running this file's `DROP` by hand.
//!
//! # Why `preflight` and `verify_applied` both exist
//!
//! `preflight` refuses to create a table that is already there under an *empty*
//! ledger — somebody else's table, whoever they are — because every statement
//! in a migration file is `IF NOT EXISTS`, so marching ahead would leave a
//! schema that looks migrated and rejects writes on a column this ledger never
//! added. `verify_applied` refuses the opposite shape: a ledger that claims
//! work the database no longer has, which is what a rollback that dropped the
//! tables and forgot the ledger looks like. It checks every recorded version's
//! own relations rather than only the ones a rollback would touch, and skips
//! versions it does not recognise so that a database some future migrator got
//! ahead on does not fail every start here.

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
/// it. A `const` array read top to bottom is the whole directory in this build,
/// so sorting it a second time at runtime would only hide a mistake in this
/// list instead of catching it at compile-adjacent test time. The check keeps
/// meaning something the day a second entry lands: it is what makes appending
/// a version 3 while a version 2 is still on a branch fail here rather than in
/// a half-migrated database.
const MIGRATIONS: &[Migration] = &[Migration {
    version: 1,
    name: "0001_initial_schema.sql",
    body: include_str!("migrations/0001_initial_schema.sql"),
}];

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
/// available to them. The only caller (`PostgresSnapshotCatalog::connect`)
/// runs this synchronously during backend assembly, before anything is wired
/// to serve traffic, so there is no "already serving, now the schema turns out
/// to be broken" state to guard against.
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

    // 🔴 Released before the outcome is reported: whatever failed above may
    // have left the connection's server-side state
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
/// version order — what `preflight` refuses to overwrite and what
/// `verify_applied` demands still exists.
///
/// 🔴 `active_templates` is a view and is listed anyway: `to_regclass`
/// resolves a view the same as a table, and a rollback that took the view
/// without the ledger is the same broken state as one that took a table.
const RELATIONS_BY_VERSION: &[(i32, &[&str])] = &[(
    1,
    &[
        "snapshots",
        "templates",
        "builds",
        "aliases",
        "active_templates",
    ],
)];

fn owned_relations() -> Vec<&'static str> {
    RELATIONS_BY_VERSION
        .iter()
        .flat_map(|(_, relations)| relations.iter().copied())
        .collect()
}

/// `to_regclass` resolves against the connection's `search_path`, which is
/// what keeps this consistent between a
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
/// ledger. Only fires on an empty ledger:
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
         Confirm what created these tables before proceeding; in dev/test, DROP them and \
         restart this process.",
        existing.join(", ")
    )
}

/// Refuses a ledger that claims work the database no longer has: checked
/// against every recorded version's relations, not only the ones a
/// half-finished rollback would touch. Versions this build does not recognise
/// are skipped — that is a database a newer (or differently versioned)
/// migrator touched, and this one has no idea what those files created.
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
         \x20   DROP TABLE IF EXISTS aliases, builds, templates, snapshots CASCADE;\n\
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
    // semicolons inside `$$ ... $$` dollar-quoting (0001's two trigger
    // functions). `raw_sql` sends the file as one simple-query message and
    // lets PostgreSQL's own parser split it, which is the same thing `psql`
    // does for a multi-statement body.
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

    const OWNED_TABLES: [&str; 5] = [
        "snapshots",
        "templates",
        "builds",
        "aliases",
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
        assert_eq!(recorded, vec![1]);
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
            "DROP TABLE IF EXISTS aliases, builds, templates, snapshots CASCADE;\
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
    /// exactly what two `aenv-api` replicas each holding their own pool
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
        assert_eq!(recorded, vec![1], "no duplicate or missing ledger rows");
    }

    /// A database already holding these tables under an empty ledger — some
    /// other tool's `snapshots`, or a rollback that dropped the ledger and
    /// nothing else. `migrate()` must refuse rather than march ahead over a
    /// table it did not create, because every statement in the file is
    /// `IF NOT EXISTS` and would quietly do nothing.
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

    /// The half-finished-rollback shape `verify_applied` exists for: the four
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
        sqlx::raw_sql("DROP TABLE IF EXISTS aliases, builds, templates, snapshots CASCADE;")
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
    /// Refusing it would mean two independently-versioned migrators could
    /// never share a database without this build failing every start the
    /// moment the other one gets ahead.
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

    // ─────────────────────────────────────────────────────────────────────
    // What the schema actually is
    //
    // 🔴 These four lists are the reason a squashed migration is safe to edit.
    // Every statement in `0001_initial_schema.sql` is `IF NOT EXISTS` /
    // `OR REPLACE`, so *deleting* one is silent: the file still applies, the
    // ledger still records version 1, every other test here still passes, and
    // the constraint or index that was supposed to stop a bad row is simply
    // gone. Nothing else in this crate reads `pg_constraint`. So these
    // assertions are spelled as whole sorted sets rather than as `contains`
    // checks — a set comparison fails on a deletion, which is the direction
    // that matters, and a `contains` check does not.
    //
    // A deliberate change to the schema is meant to fail these and be updated
    // here in the same commit. That is the point: the update is where somebody
    // states, in the diff, which rule they are removing.
    // ─────────────────────────────────────────────────────────────────────

    /// Everything in the test connection's own schema, so two isolated-schema
    /// tests cannot see each other's tables.
    async fn schema_facts(pool: &sqlx::PgPool, sql: &str) -> Vec<String> {
        sqlx::query_scalar(sql)
            .fetch_all(pool)
            .await
            .expect("inspecting the applied schema should succeed")
    }

    #[tokio::test]
    async fn the_applied_schema_has_exactly_these_relations() {
        let pool = isolated_schema_pool_or_skip!("the_applied_schema_has_exactly_these_relations");
        migrate(&pool).await.expect("migration should succeed");

        let found = schema_facts(
            &pool,
            "SELECT relname || ' (' || relkind::text || ')' \
               FROM pg_class \
              WHERE relnamespace = current_schema()::regnamespace \
                AND relkind IN ('r', 'v') \
              ORDER BY 1",
        )
        .await;

        assert_eq!(
            found,
            vec![
                "active_templates (v)",
                "aliases (r)",
                "builds (r)",
                "catalog_schema_migrations (r)",
                "snapshots (r)",
                "templates (r)",
            ],
            "the applied schema is not the set of relations this build expects"
        );
    }

    /// 🔴 The auto-named column CHECKs (`snapshots_cpu_count_check`,
    /// `snapshots_status_check`, …) are listed beside the hand-named ones on
    /// purpose. PostgreSQL derives those names from the table and column, so
    /// they are stable, and pinning them is what catches an inline `CHECK`
    /// being dropped out of a `CREATE TABLE` — which no named-constraint list
    /// would notice.
    ///
    /// 🔴 There is deliberately no `snapshots_disk_size_mib_check` here.
    /// `disk_size_mib` carries no inline positivity CHECK: 0 means "not known
    /// until the build produces a rootfs", and an insert-time `> 0` refused
    /// every v3 template create. The two named rules below
    /// (`snapshots_disk_size_floor`, `snapshots_ready_has_disk_size`) are what
    /// replaced it, and this assertion fails if somebody puts the inline one
    /// back.
    #[tokio::test]
    async fn the_applied_schema_has_exactly_these_constraints() {
        let pool =
            isolated_schema_pool_or_skip!("the_applied_schema_has_exactly_these_constraints");
        migrate(&pool).await.expect("migration should succeed");

        let found = schema_facts(
            &pool,
            "SELECT t.relname || '.' || c.conname || ' (' || c.contype::text || ')' \
               FROM pg_constraint c \
               JOIN pg_class t ON t.oid = c.conrelid \
              WHERE t.relnamespace = current_schema()::regnamespace \
                AND c.contype IN ('c', 'f', 'p', 'u') \
              ORDER BY 1",
        )
        .await;

        assert_eq!(
            found,
            vec![
                "aliases.aliases_pkey (p)",
                "aliases.aliases_snapshot_fk (f)",
                "builds.builds_finished_axis (c)",
                "builds.builds_pkey (p)",
                "builds.builds_started_axis (c)",
                "builds.builds_status_check (c)",
                "builds.builds_status_group_check (c)",
                "builds.builds_template_fk (f)",
                "catalog_schema_migrations.catalog_schema_migrations_pkey (p)",
                "snapshots.snapshots_committed_axis (c)",
                "snapshots.snapshots_cpu_count_check (c)",
                "snapshots.snapshots_disk_size_floor (c)",
                "snapshots.snapshots_error_axis (c)",
                "snapshots.snapshots_memory_mib_check (c)",
                "snapshots.snapshots_origin_axis (c)",
                "snapshots.snapshots_pkey (p)",
                "snapshots.snapshots_ready_has_disk_size (c)",
                "snapshots.snapshots_ready_is_committed (c)",
                "snapshots.snapshots_source_axis (c)",
                "snapshots.snapshots_source_kind_check (c)",
                "snapshots.snapshots_status_check (c)",
                "snapshots.snapshots_status_group_check (c)",
                "templates.templates_id_fk (f)",
                "templates.templates_pkey (p)",
            ],
            "the applied schema is not the set of constraints this build expects"
        );
    }

    /// 🔴 `unique` and `partial` are asserted, not just the names. Both carry
    /// the rule rather than the performance: `builds_one_active_per_template`
    /// is what makes "one live build per template" an impossibility instead of
    /// a race two concurrent POSTs can lose, and it only means that while it is
    /// UNIQUE *and* predicated on the active status groups. An index recreated
    /// without either half still answers every query and enforces nothing.
    #[tokio::test]
    async fn the_applied_schema_has_exactly_these_indexes() {
        let pool = isolated_schema_pool_or_skip!("the_applied_schema_has_exactly_these_indexes");
        migrate(&pool).await.expect("migration should succeed");

        let found = schema_facts(
            &pool,
            "SELECT c.relname \
                 || ' unique=' || i.indisunique::text \
                 || ' partial=' || (i.indpred IS NOT NULL)::text \
               FROM pg_index i \
               JOIN pg_class c ON c.oid = i.indexrelid \
               JOIN pg_class t ON t.oid = i.indrelid \
              WHERE t.relnamespace = current_schema()::regnamespace \
              ORDER BY 1",
        )
        .await;

        assert_eq!(
            found,
            vec![
                "aliases_one_per_snapshot unique=true partial=false",
                "aliases_pkey unique=true partial=false",
                "builds_active_idx unique=false partial=true",
                "builds_one_active_per_template unique=true partial=true",
                "builds_pkey unique=true partial=false",
                "catalog_schema_migrations_pkey unique=true partial=false",
                "snapshots_list_idx unique=false partial=true",
                "snapshots_pkey unique=true partial=false",
                "snapshots_source_sandbox_idx unique=false partial=true",
                "snapshots_unpublished_idx unique=false partial=true",
                "templates_cluster_live_idx unique=false partial=true",
                "templates_pkey unique=true partial=false",
            ],
            "the applied schema is not the set of indexes this build expects"
        );
    }

    /// 🔴 The function each trigger calls is part of the assertion. `status_group`
    /// is never written by a caller — every read path's partial indexes are
    /// predicated on it — so a trigger left pointing at the wrong function, or
    /// dropped from one of the two tables that carry the column, produces rows
    /// whose `status_group` disagrees with their `status` and which the listing
    /// index therefore cannot see.
    #[tokio::test]
    async fn the_applied_schema_has_exactly_these_triggers() {
        let pool = isolated_schema_pool_or_skip!("the_applied_schema_has_exactly_these_triggers");
        migrate(&pool).await.expect("migration should succeed");

        let found = schema_facts(
            &pool,
            "SELECT t.relname || '.' || g.tgname || ' -> ' || p.proname \
               FROM pg_trigger g \
               JOIN pg_class t ON t.oid = g.tgrelid \
               JOIN pg_proc p ON p.oid = g.tgfoid \
              WHERE t.relnamespace = current_schema()::regnamespace \
                AND NOT g.tgisinternal \
              ORDER BY 1",
        )
        .await;

        assert_eq!(
            found,
            vec![
                "builds.builds_status_group_trg -> catalog_status_group_trg",
                "snapshots.snapshots_status_group_trg -> catalog_status_group_trg",
                "snapshots.snapshots_updated_at_trg -> snapshots_touch_updated_at_trg",
            ],
            "the applied schema is not the set of triggers this build expects"
        );
    }
}
