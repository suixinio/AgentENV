//! `aenv-core`'s repository backends, plus the PostgreSQL catalog.
//!
//! The shared assembly (`build_snapshot_backend`, `build_central_catalog`,
//! `build_catalog_only_storage`) is `aenv-core`'s and is re-exported here
//! unchanged; what this crate adds is the catalog that answers over `[pg]` and
//! the two bridges the binary calls into it (`migrate_catalog_schema`,
//! `spawn_catalog_build_reaper`), plus `pg_catalog_parts`, which builds what
//! the shared assembly takes.

pub use aenv_core::snapshot::repository::backends::*;

pub mod postgres;

pub use postgres::{migrate_catalog_schema, pg_catalog_parts, spawn_catalog_build_reaper};

/// `build_central_catalog`'s own pg-vs-grpc choice, against a real database.
/// Named `pg::` (not folded into `mod tests` above) so `make test-with-postgres`
/// — a name filter, not a feature check — actually selects it; see Stage B's
/// own proposal doc §7 step 12 on this exact trap.
#[cfg(test)]
mod pg {
    use super::*;
    use crate::cfg::{AppConfig, SnapshotCatalogWrite};
    use crate::pg::harness::isolated_schema_pool_or_skip;
    use crate::snapshot::repository::backends::postgres::migrate::migrate;
    use crate::snapshot::repository::interfaces::CatalogReadScope;
    use crate::snapshot::types::{SnapshotId, SnapshotRecord};
    use crate::types::SandboxResources;

    fn resources() -> SandboxResources {
        SandboxResources {
            cpu_count: 1,
            memory_mib: 512,
            disk_size_mib: 1024,
        }
    }

    /// The write axis alone decides `Some`/`None`; `write = "object_store"`
    /// must return `None` even when a pool is handed to it — a caller that
    /// stopped checking `pg_pool.is_some()` first and started trusting this
    /// function's `Some`-ness alone would otherwise silently double-write
    /// object-store-only deployments.
    #[tokio::test]
    async fn object_store_write_returns_none_even_with_a_pool() {
        let pool =
            isolated_schema_pool_or_skip!("object_store_write_returns_none_even_with_a_pool");
        migrate(&pool).await.expect("migration should succeed");

        let config = AppConfig::default();
        assert_eq!(
            config.snapshot.catalog.write,
            SnapshotCatalogWrite::ObjectStore
        );

        let handle =
            build_central_catalog(&config, Some(&pg_catalog_parts(&config, &pool).central))
                .expect("should not error");
        assert!(handle.is_none());
    }

    /// The heart of the wiring this module exists for: with `[pg]`
    /// configured, `write = "both"` must build a `PostgresSnapshotCatalog`
    /// rather than dialling `services/scheduler` over gRPC. Proven two ways,
    /// not just inferred:
    ///
    /// 1. No `scheduler_endpoint` is configured at all — the gRPC branch
    ///    would refuse to build with a `requires a scheduler endpoint` error,
    ///    so a `Some` result here is only possible if the Postgres branch was
    ///    taken.
    /// 2. The returned handle is round-tripped against the real database: a
    ///    `begin` through `writes`, then a `census` listing and a `reads`
    ///    lookup both see it, proving all three faces of the bundle are wired
    ///    to the same live pool rather than to three different or dead
    ///    catalogs.
    #[tokio::test]
    async fn write_both_with_a_pg_pool_builds_a_postgres_central_catalog() {
        let pool = isolated_schema_pool_or_skip!(
            "write_both_with_a_pg_pool_builds_a_postgres_central_catalog"
        );
        migrate(&pool).await.expect("migration should succeed");

        let mut config = AppConfig::default();
        config.snapshot.catalog.write = SnapshotCatalogWrite::Both;
        assert!(
            config.cluster.scheduler_endpoint.is_none(),
            "the default config must carry no scheduler endpoint for this test's proof to hold"
        );

        let handle =
            build_central_catalog(&config, Some(&pg_catalog_parts(&config, &pool).central))
                .expect("building should not error")
                .expect("write = \"both\" with a pool must produce a central catalog");

        let id = SnapshotId::generate();
        let record = SnapshotRecord::template_waiting(id.clone(), None, resources());
        match handle
            .writes
            .begin(&record, "waiting", true)
            .await
            .expect("begin should succeed")
        {
            CatalogWrite::Applied(applied) => assert_eq!(applied.id, id),
            CatalogWrite::Refused(refusal) => panic!("begin was refused: {refusal}"),
        }

        let ids = handle
            .census
            .every_snapshot_id()
            .await
            .expect("census should succeed");
        assert!(
            ids.contains(&id),
            "the census must see the row `begin` just wrote"
        );

        let found = handle
            .reads
            .get_scoped(&id.to_string(), CatalogReadScope::AnyStatus)
            .await
            .expect("read should succeed")
            .expect("the row `begin` just wrote must be readable back");
        assert_eq!(found.id, id);
    }

    /// `write = "postgres"` has no gRPC fallback — with no `[pg]` pool at
    /// all, it must refuse outright rather than silently falling through to
    /// dialling a scheduler (which the pre-Stage-B `bail!` this replaces
    /// never did either, but for a different reason).
    #[test]
    fn write_postgres_without_a_pool_refuses() {
        let mut config = AppConfig::default();
        config.snapshot.catalog.write = SnapshotCatalogWrite::Postgres;

        let error = match build_central_catalog(&config, None) {
            Ok(_) => panic!("write = \"postgres\" without a pool must be refused"),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains("requires [pg] to be configured"),
            "got: {error}"
        );
    }

    /// `write = "postgres"` with a pool takes the same Postgres branch
    /// `write = "both"` does — proven the same way: no scheduler endpoint
    /// configured, and the handle is round-tripped against the database.
    #[tokio::test]
    async fn write_postgres_with_a_pg_pool_builds_a_postgres_central_catalog() {
        let pool = isolated_schema_pool_or_skip!(
            "write_postgres_with_a_pg_pool_builds_a_postgres_central_catalog"
        );
        migrate(&pool).await.expect("migration should succeed");

        let mut config = AppConfig::default();
        config.snapshot.catalog.write = SnapshotCatalogWrite::Postgres;

        let handle =
            build_central_catalog(&config, Some(&pg_catalog_parts(&config, &pool).central))
                .expect("building should not error")
                .expect("write = \"postgres\" with a pool must produce a central catalog");

        let id = SnapshotId::generate();
        let record = SnapshotRecord::template_waiting(id.clone(), None, resources());
        handle
            .writes
            .begin(&record, "waiting", true)
            .await
            .expect("begin should succeed");
        let found = handle
            .reads
            .get_scoped(&id.to_string(), CatalogReadScope::AnyStatus)
            .await
            .expect("read should succeed");
        assert!(found.is_some());
    }

    /// 🔴 P1's actual production path: `config.snapshot.catalog.max_concurrent_builds`
    /// has to reach the `PostgresSnapshotCatalog` [`pg_catalog_parts`]
    /// constructs, not just the test-only constructor in `postgres::mod::pg`.
    /// A ceiling of `1` set on the `AppConfig` passed in here must refuse a
    /// second *different* template's build the same way
    /// `the_cluster_wide_build_ceiling_refuses_once_it_is_reached` proves the
    /// underlying store does — this test is the only one that goes through
    /// `pg_catalog_parts` + `build_central_catalog` to get there, so a
    /// regression that stops the config value from being read (for instance,
    /// `pg_catalog_parts` going back to `PostgresSnapshotCatalog::new` without
    /// the `with_max_concurrent_builds` call) fails only here.
    #[tokio::test]
    async fn max_concurrent_builds_from_config_reaches_admission() {
        let pool =
            isolated_schema_pool_or_skip!("max_concurrent_builds_from_config_reaches_admission");
        migrate(&pool).await.expect("migration should succeed");

        let mut config = AppConfig::default();
        config.snapshot.catalog.write = SnapshotCatalogWrite::Both;
        config.snapshot.catalog.max_concurrent_builds = 1;

        let handle =
            build_central_catalog(&config, Some(&pg_catalog_parts(&config, &pool).central))
                .expect("building should not error")
                .expect("write = \"both\" with a pool must produce a central catalog");

        let first_id = SnapshotId::generate();
        let first = SnapshotRecord::template_waiting(first_id.clone(), None, resources());
        handle
            .writes
            .begin(&first, "waiting", true)
            .await
            .expect("begin should succeed");
        match handle
            .writes
            .start_build(&first_id, &SnapshotId::generate(), central::now_unix_ms())
            .await
            .expect("start_build should not error")
        {
            CatalogWrite::Applied(_) => {}
            CatalogWrite::Refused(refusal) => {
                panic!("the first build must be admitted under the ceiling: {refusal}")
            }
        }

        let second_id = SnapshotId::generate();
        let second = SnapshotRecord::template_waiting(second_id.clone(), None, resources());
        handle
            .writes
            .begin(&second, "waiting", true)
            .await
            .expect("begin should succeed");
        match handle
            .writes
            .start_build(&second_id, &SnapshotId::generate(), central::now_unix_ms())
            .await
            .expect("start_build should not error")
        {
            CatalogWrite::Applied(_) => panic!(
                "a second, different template's build must be refused once the configured \
                 ceiling of 1 is reached — this only happens if `config.snapshot.catalog.\
                 max_concurrent_builds` actually reached the store"
            ),
            CatalogWrite::Refused(CatalogRefusal::BuildQueueFull) => {}
            CatalogWrite::Refused(other) => {
                panic!("expected BuildQueueFull, got a different refusal: {other}")
            }
        }
    }
}
