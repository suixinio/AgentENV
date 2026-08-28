//! `aenv-core`'s repository backends, plus the PostgreSQL catalog.
//!
//! The shared assembly (`build_snapshot_backend`, `build_catalog_only_storage`)
//! is `aenv-core`'s and is re-exported here unchanged; what this crate adds is
//! the catalog that answers over `[pg]` — the only snapshot catalog there is —
//! and the two bridges the binary calls into it (`migrate_catalog_schema`,
//! `spawn_catalog_build_reaper`), plus `pg_snapshot_catalog`, which builds what
//! the shared assembly takes.

pub use aenv_core::snapshot::repository::backends::*;

pub mod postgres;

pub use postgres::{migrate_catalog_schema, pg_snapshot_catalog, spawn_catalog_build_reaper};

/// The production construction of the snapshot catalog, against a real
/// database. Named `pg::` (not `mod tests`) so `make test-with-postgres` — a
/// name filter, not a feature check — actually selects it; see Stage B's own
/// proposal doc §7 step 12 on this exact trap.
#[cfg(test)]
mod pg {
    use super::*;
    use crate::cfg::AppConfig;
    use crate::pg::harness::isolated_schema_pool_or_skip;
    use crate::snapshot::repository::backends::postgres::migrate::migrate;
    use crate::snapshot::repository::interfaces::CatalogReadScope;
    use crate::snapshot::repository::RepositoryError;
    use crate::snapshot::types::{SnapshotId, SnapshotRecord};
    use crate::types::SandboxResources;

    fn resources() -> SandboxResources {
        SandboxResources {
            cpu_count: 1,
            memory_mib: 512,
            disk_size_mib: 1024,
        }
    }

    /// The catalog `pg_snapshot_catalog` hands the shared assembly is live: a
    /// row written through it reads back through it, over the same pool.
    #[tokio::test]
    async fn the_catalog_handed_to_the_assembly_is_wired_to_the_pool() {
        let pool = isolated_schema_pool_or_skip!(
            "the_catalog_handed_to_the_assembly_is_wired_to_the_pool"
        );
        migrate(&pool).await.expect("migration should succeed");

        let config = AppConfig::default();
        let catalog = pg_snapshot_catalog(&config, &pool);

        let id = SnapshotId::generate();
        let record = SnapshotRecord::template_waiting(id.clone(), None, resources());
        catalog.create(record).await.expect("create should succeed");

        let found = catalog
            .get_scoped(&id.to_string(), CatalogReadScope::AnyStatus)
            .await
            .expect("read should succeed")
            .expect("the row `create` just wrote must be readable back");
        assert_eq!(found.id, id);
    }

    /// 🔴 The whole assembly, end to end, over a real database: the byte half
    /// `build_catalog_only_storage` builds carries no catalog, and
    /// `build_snapshot_backend` is what puts this one in front of it. A
    /// regression that dropped the catalog on the floor would leave the
    /// repository answering out of `NoSnapshotCatalog` — which refuses — and
    /// this is the only test that would notice.
    #[tokio::test]
    async fn the_assembled_repository_reads_through_postgresql() {
        let pool =
            isolated_schema_pool_or_skip!("the_assembled_repository_reads_through_postgresql");
        migrate(&pool).await.expect("migration should succeed");

        let dir = tempfile::TempDir::new().expect("tempdir");
        let mut config = AppConfig::default();
        config.snapshot.repository_backend = crate::cfg::SnapshotRepositoryBackendKind::PosixFs;
        config.backend.posix_fs = Some(crate::cfg::PosixFsBackendConfig {
            snapshot_store: dir.path().join("store"),
        });

        let catalog = pg_snapshot_catalog(&config, &pool);
        let id = SnapshotId::generate();
        catalog
            .create(SnapshotRecord::template_waiting(
                id.clone(),
                None,
                resources(),
            ))
            .await
            .expect("create should succeed");

        let assembled = build_snapshot_backend(
            build_catalog_only_storage(&config).expect("the byte half should assemble"),
            Some(catalog),
            CentralCatalogUse::AsConfigured,
        )
        .expect("the assembly should succeed with a catalog");

        let found = assembled
            .repository
            .get_scoped(&id.to_string(), CatalogReadScope::AnyStatus)
            .await
            .expect("the assembled repository must read through PostgreSQL")
            .expect("the row must be there");
        assert_eq!(found.id, id);
    }

    /// 🔴 P1's production path: `config.snapshot.catalog.max_concurrent_builds`
    /// has to reach the `PostgresSnapshotCatalog` [`pg_snapshot_catalog`]
    /// constructs, not just the test-only constructor in `postgres::mod::pg`.
    /// A ceiling of `1` set on the `AppConfig` passed in here must refuse a
    /// second *different* template's build the same way
    /// `the_cluster_wide_build_ceiling_refuses_once_it_is_reached` proves the
    /// underlying store does — this test is the only one that goes through
    /// `pg_snapshot_catalog` to get there, so a regression that stops the
    /// config value from being read (for instance, `pg_snapshot_catalog` going
    /// back to `PostgresSnapshotCatalog::new` without the
    /// `with_max_concurrent_builds` call) fails only here.
    #[tokio::test]
    async fn max_concurrent_builds_from_config_reaches_admission() {
        let pool =
            isolated_schema_pool_or_skip!("max_concurrent_builds_from_config_reaches_admission");
        migrate(&pool).await.expect("migration should succeed");

        let mut config = AppConfig::default();
        config.snapshot.catalog.max_concurrent_builds = 1;
        let catalog = pg_snapshot_catalog(&config, &pool);

        let first_id = SnapshotId::generate();
        catalog
            .create(SnapshotRecord::template_waiting(
                first_id.clone(),
                None,
                resources(),
            ))
            .await
            .expect("create should succeed");
        catalog
            .try_start_build(&first_id)
            .await
            .expect("the first build must be admitted under the ceiling");

        let second_id = SnapshotId::generate();
        catalog
            .create(SnapshotRecord::template_waiting(
                second_id.clone(),
                None,
                resources(),
            ))
            .await
            .expect("create should succeed");
        let error = catalog.try_start_build(&second_id).await.expect_err(
            "a second, different template's build must be refused once the configured ceiling \
             of 1 is reached — this only happens if `config.snapshot.catalog.\
             max_concurrent_builds` actually reached the store",
        );
        assert!(
            matches!(error, RepositoryError::InvalidRequest { .. })
                && error.to_string().contains("concurrent-build ceiling"),
            "the ceiling refusal must surface as a rejection naming the ceiling: {error}"
        );
    }
}
