//! Versioned catalog migrations serialized by a session-scoped schema lock.
//! Preflight refuses relations of unrecorded versions; verification refuses
//! recorded versions whose relations disappeared.
//! Each migration and ledger write commit atomically.

use anyhow::{Context, Result};
use sqlx::postgres::PgPool;
use std::collections::HashSet;

use crate::pg::GO_SCHEMA_LOCK_KEY;

struct Migration {
    version: i32,
    name: &'static str,
    body: &'static str,
}

// Ordered, dense migration list; tests enforce the sequence.
const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        name: "0001_initial_schema.sql",
        body: include_str!("migrations/0001_initial_schema.sql"),
    },
    Migration {
        version: 2,
        name: "0002_secret_refs.sql",
        body: include_str!("migrations/0002_secret_refs.sql"),
    },
    Migration {
        version: 3,
        name: "0003_secret_values.sql",
        body: include_str!("migrations/0003_secret_values.sql"),
    },
    Migration {
        version: 4,
        name: "0004_one_pause_per_sandbox.sql",
        body: include_str!("migrations/0004_one_pause_per_sandbox.sql"),
    },
];

// Every table these migrations create, in the order the rollback drops
// them. The verification error prints this list and the test that claims to
// run the documented command runs this list: a table left out of it survives
// the rollback and blocks the next start as an unrecorded relation.
const CATALOG_TABLES: &str =
    "aliases, builds, templates, snapshots, secret_values, secret_grants, secret_refs";

const VERSION_TABLE_DDL: &str = "
CREATE TABLE IF NOT EXISTS catalog_schema_migrations (
    version       INTEGER PRIMARY KEY,
    applied_at_ms BIGINT  NOT NULL
)";

/// Applies every unrecorded catalog migration on one lock-holding connection.
///
/// Unlocks before returning and closes the connection if lock state is uncertain.
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

    // Never return a connection with uncertain advisory-lock state to the pool.
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

// Relations required by each recorded migration version, including views.
const RELATIONS_BY_VERSION: &[(i32, &[&str])] = &[
    (
        1,
        &[
            "snapshots",
            "templates",
            "builds",
            "aliases",
            "active_templates",
        ],
    ),
    (2, &["secret_refs"]),
    (3, &["secret_values", "secret_grants"]),
    // An index, not a table: it is the whole point of the version, and
    // `to_regclass` resolves it the same way.
    (4, &["snapshots_one_pause_per_sandbox"]),
];

// `to_regclass` resolves against this connection's search path.
async fn relation_exists(conn: &mut sqlx::PgConnection, relation: &str) -> Result<bool> {
    let found: Option<String> = sqlx::query_scalar("SELECT to_regclass($1)::text")
        .bind(relation)
        .fetch_one(&mut *conn)
        .await
        .context("inspect the catalog schema")?;
    Ok(found.is_some())
}

// Refuses relations belonging to a version the ledger has not recorded, so a
// partially recorded ledger cannot adopt a table this build never created.
async fn preflight(conn: &mut sqlx::PgConnection, applied: &HashSet<i32>) -> Result<()> {
    let mut existing = Vec::new();
    for (version, relations) in RELATIONS_BY_VERSION {
        if applied.contains(version) {
            continue;
        }
        for relation in *relations {
            if relation_exists(conn, relation).await? {
                existing.push(*relation);
            }
        }
    }
    if existing.is_empty() {
        return Ok(());
    }

    anyhow::bail!(
        "catalog table(s) {} already exist, but catalog_schema_migrations has not recorded the \
         migration that creates them — these tables were not created by this ledger. Refusing to \
         continue: every migration statement is IF NOT EXISTS, so continuing would silently \
         leave a schema that looks migrated but rejects every write on a column this ledger's \
         migrations never added. Confirm what created these tables before proceeding; in \
         dev/test, DROP them and restart this process.",
        existing.join(", ")
    )
}

// Refuses recorded migrations whose expected relations are missing.
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
         \x20   DROP TABLE IF EXISTS {CATALOG_TABLES} CASCADE;\n\
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

    // Migration files require the simple-query protocol for multiple statements
    // and dollar-quoted PL/pgSQL bodies.
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

    // Scope information_schema to the test connection's current schema.
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

    const OWNED_TABLES: [&str; 8] = [
        "snapshots",
        "templates",
        "builds",
        "aliases",
        "secret_refs",
        "secret_values",
        "secret_grants",
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

    #[tokio::test]
    async fn the_documented_rollback_command_actually_rolls_back() {
        let pool =
            isolated_schema_pool_or_skip!("the_documented_rollback_command_actually_rolls_back");
        migrate(&pool).await.expect("migration should succeed");

        sqlx::raw_sql(&format!(
            "DROP TABLE IF EXISTS {CATALOG_TABLES} CASCADE;\
             DROP TABLE IF EXISTS catalog_schema_migrations;"
        ))
        .execute(&pool)
        .await
        .expect("the documented rollback command should succeed");

        for table in OWNED_TABLES {
            assert!(
                !table_exists_in_current_schema(&pool, table).await,
                "rollback should have dropped table {table}"
            );
        }

        migrate(&pool)
            .await
            .expect("re-migrating after rollback should succeed");
    }

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

    #[tokio::test]
    async fn preflight_refuses_a_snapshots_table_that_predates_the_ledger() {
        let pool = isolated_schema_pool_or_skip!(
            "preflight_refuses_a_snapshots_table_that_predates_the_ledger"
        );
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

        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM catalog_schema_migrations")
            .fetch_one(&pool)
            .await
            .expect("counting the ledger should succeed");
        assert_eq!(
            count, 0,
            "a refused preflight must leave the ledger untouched"
        );
    }

    #[tokio::test]
    async fn preflight_refuses_a_table_of_a_version_the_ledger_never_recorded() {
        let pool = isolated_schema_pool_or_skip!(
            "preflight_refuses_a_table_of_a_version_the_ledger_never_recorded"
        );
        migrate(&pool).await.expect("migration should succeed");

        sqlx::query("DELETE FROM catalog_schema_migrations WHERE version = 2")
            .execute(&pool)
            .await
            .expect("forgetting one recorded version should succeed");

        let error = migrate(&pool)
            .await
            .expect_err("migrate must refuse an unrecorded version's table under a live ledger");
        let message = format!("{error:#}");
        assert!(
            message.contains("secret_refs") && message.contains("not created by this ledger"),
            "got: {message}"
        );

        let recorded: Vec<i32> =
            sqlx::query_scalar("SELECT version FROM catalog_schema_migrations ORDER BY version")
                .fetch_all(&pool)
                .await
                .expect("reading the ledger should succeed");
        assert_eq!(
            recorded,
            vec![1, 3, 4],
            "a refused preflight must not record the version it refused"
        );
    }

    #[tokio::test]
    async fn verify_applied_refuses_a_ledger_whose_tables_are_gone() {
        let pool =
            isolated_schema_pool_or_skip!("verify_applied_refuses_a_ledger_whose_tables_are_gone");
        migrate(&pool).await.expect("migration should succeed");

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

    // Inserts a committed sandbox-source row directly, bypassing the write path,
    // so a database that predates one-pause-per-sandbox can be reproduced.
    async fn seed_committed_sandbox_row(
        pool: &sqlx::PgPool,
        cluster_id: uuid::Uuid,
        source_sandbox_id: &str,
        created_at_ms: i64,
        payload_json: &str,
    ) -> uuid::Uuid {
        let id = uuid::Uuid::now_v7();
        sqlx::query(
            "INSERT INTO snapshots (
                id, cluster_id, source_kind, source_sandbox_id,
                cpu_count, memory_mib, disk_size_mib,
                status, status_group, published,
                created_at_ms, updated_at_ms,
                committed_payload, committed_schema
             ) VALUES (
                $1, $2, 'sandbox', $3,
                1, 512, 1024,
                'ready', 'pending', true,
                $4, $4,
                convert_to($5, 'UTF8'), 1
             )",
        )
        .bind(id)
        .bind(cluster_id)
        .bind(source_sandbox_id)
        .bind(created_at_ms)
        .bind(payload_json)
        .execute(pool)
        .await
        .expect("seeding a pre-migration row should succeed");
        id
    }

    // The trigger reads the column, so it goes with it or every later insert
    // raises instead of reproducing a database that never had either.
    async fn rewind_to_the_pre_migration_schema(pool: &sqlx::PgPool) {
        sqlx::raw_sql(
            "DROP TRIGGER IF EXISTS snapshots_pause_axis_trg ON snapshots;\
             DROP INDEX IF EXISTS snapshots_one_pause_per_sandbox;\
             ALTER TABLE snapshots DROP CONSTRAINT IF EXISTS snapshots_pause_axis;\
             ALTER TABLE snapshots DROP COLUMN IF EXISTS is_pause;\
             DELETE FROM catalog_schema_migrations WHERE version = 4;",
        )
        .execute(pool)
        .await
        .expect("rewinding to the pre-migration schema should succeed");
    }

    async fn is_live(pool: &sqlx::PgPool, id: uuid::Uuid) -> bool {
        sqlx::query_scalar::<_, bool>("SELECT deleted_at_ms IS NULL FROM snapshots WHERE id = $1")
            .bind(id)
            .fetch_one(pool)
            .await
            .expect("the seeded row should still exist")
    }

    #[tokio::test]
    async fn the_pause_migration_keeps_the_newest_ready_duplicate_and_retires_the_rest() {
        let pool = isolated_schema_pool_or_skip!(
            "the_pause_migration_keeps_the_newest_ready_duplicate_and_retires_the_rest"
        );
        migrate(&pool).await.expect("migration should succeed");

        // Rewind to the state a database that never ran this version is in.
        rewind_to_the_pre_migration_schema(&pool).await;

        let cluster_id = uuid::Uuid::new_v4();
        let older = seed_committed_sandbox_row(
            &pool,
            cluster_id,
            "sbx-dup",
            1_000,
            r#"{"paused_sandbox":{}}"#,
        )
        .await;
        let newer = seed_committed_sandbox_row(
            &pool,
            cluster_id,
            "sbx-dup",
            2_000,
            r#"{"paused_sandbox":{}}"#,
        )
        .await;
        let checkpoint = seed_committed_sandbox_row(
            &pool,
            cluster_id,
            "sbx-dup",
            3_000,
            r#"{"rootfs_layers":[]}"#,
        )
        .await;

        migrate(&pool)
            .await
            .expect("a database that already holds duplicates must still migrate");

        assert!(
            is_live(&pool, newer).await,
            "the newest ready pause is the one a resume would have read, so it survives"
        );
        assert!(
            !is_live(&pool, older).await,
            "a duplicate the constraint cannot hold is retired, not left to block the upgrade"
        );
        assert!(
            is_live(&pool, checkpoint).await,
            "a checkpoint of the same sandbox is not a pause and is not a duplicate"
        );

        let pauses: Vec<bool> =
            sqlx::query_scalar("SELECT is_pause FROM snapshots ORDER BY created_at_ms")
                .fetch_all(&pool)
                .await
                .expect("reading the backfilled column should succeed");
        assert_eq!(
            pauses,
            vec![true, true, false],
            "the backfill reads the payload's key, not the row's source kind"
        );
    }

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
                "secret_grants (r)",
                "secret_refs (r)",
                "secret_values (r)",
                "snapshots (r)",
                "templates (r)",
            ],
            "the applied schema is not the set of relations this build expects"
        );
    }

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
                "secret_grants.secret_grants_pkey (p)",
                "secret_refs.secret_refs_name_unique (u)",
                "secret_refs.secret_refs_pkey (p)",
                "secret_refs.secret_refs_version_nonnegative (c)",
                "secret_values.secret_values_kind (c)",
                "secret_values.secret_values_name_fk (f)",
                "secret_values.secret_values_pkey (p)",
                "secret_values.secret_values_version_positive (c)",
                "snapshots.snapshots_committed_axis (c)",
                "snapshots.snapshots_cpu_count_check (c)",
                "snapshots.snapshots_disk_size_floor (c)",
                "snapshots.snapshots_error_axis (c)",
                "snapshots.snapshots_memory_mib_check (c)",
                "snapshots.snapshots_origin_axis (c)",
                "snapshots.snapshots_pause_axis (c)",
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
                "secret_grants_pkey unique=true partial=false",
                "secret_refs_name_unique unique=true partial=false",
                "secret_refs_pkey unique=true partial=false",
                "secret_values_pkey unique=true partial=false",
                "snapshots_list_idx unique=false partial=true",
                "snapshots_one_pause_per_sandbox unique=true partial=true",
                "snapshots_pkey unique=true partial=false",
                "snapshots_source_sandbox_idx unique=false partial=true",
                "snapshots_unpublished_idx unique=false partial=true",
                "templates_cluster_live_idx unique=false partial=true",
                "templates_pkey unique=true partial=false",
            ],
            "the applied schema is not the set of indexes this build expects"
        );
    }

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
                "snapshots.snapshots_pause_axis_trg -> catalog_snapshots_pause_axis_trg",
                "snapshots.snapshots_status_group_trg -> catalog_status_group_trg",
                "snapshots.snapshots_updated_at_trg -> snapshots_touch_updated_at_trg",
            ],
            "the applied schema is not the set of triggers this build expects"
        );
    }
}
