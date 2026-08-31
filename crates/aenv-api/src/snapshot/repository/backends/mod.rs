//! Shared snapshot backends with the PostgreSQL catalog added.

pub use aenv_core::snapshot::repository::backends::*;

pub mod postgres;

pub use postgres::{migrate_catalog_schema, pg_snapshot_catalog, spawn_catalog_build_reaper};

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
