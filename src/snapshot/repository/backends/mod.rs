pub mod catalog_write;
pub mod common;
pub mod oss;
pub mod posixfs;

use std::sync::Arc;

use anyhow::{Context, Result};

use crate::cfg::{AppConfig, SnapshotImageStoragePolicy, SnapshotRepositoryBackendKind};
use crate::snapshot::repository::interfaces::SnapshotArtifactStore;
use crate::snapshot::repository::interfaces::SnapshotCatalog;
use crate::snapshot::repository::interfaces::SnapshotRuntimeResolver;
use crate::snapshot::repository::SnapshotRepository;
pub use catalog_write::{CatalogRefusal, CatalogWrite};
use posixfs::posixfs_artifacts_only_store;

/// Assembled repository and optional runtime resolver.
pub struct AssembledSnapshotBackend {
    pub repository: Arc<SnapshotRepository>,
    /// Absent on a process that runs no sandbox runtime.
    pub runtime_resolver: Option<Arc<dyn SnapshotRuntimeResolver>>,
}

/// The running half's backend: the byte storage it assembled, carrying the
/// catalog that refuses. Rows are the deciding half's.
pub fn build_node_snapshot_backend(storage: RoleStorage) -> AssembledSnapshotBackend {
    let (repository, runtime_resolver) = storage;
    AssembledSnapshotBackend {
        repository,
        runtime_resolver,
    }
}

/// The deciding half's backend: the shared catalog over the byte store this
/// deployment is configured for, and no runtime resolver, because this half
/// materializes nothing.
pub fn build_catalog_backed_backend(
    config: &AppConfig,
    // PostgreSQL catalog is supplied only by the API process.
    pg: Option<Arc<dyn SnapshotCatalog>>,
) -> Result<AssembledSnapshotBackend> {
    let catalog = pg.context(
        "no snapshot catalog is configured: [pg].dsn is unset and PostgreSQL is the only \
         snapshot catalog there is. Object storage holds byte artifacts alone, so starting \
         without [pg] would leave every snapshot and template request with nowhere to read or \
         write a row. Set [pg].dsn for this half \
         — it is TOML-file-only, with no environment binding (confique cannot descend into \
         AppConfig::pg's Option), so supply it through the file AENV_CONFIG_PATH names or an \
         AENV_CONFIG_OVERLAY_PATH overlay, the way deploy/k8s/base's pg-dsn.toml and \
         deploy/docker-compose.yml's /tmp/agentenv-pg/pg-dsn.toml both do",
    )?;

    let node_id = crate::identity::local_node_id();
    tracing::info!(
        target: "agentenv",
        node_id = %node_id,
        "snapshot catalog is served solely by PostgreSQL; object storage holds no catalog rows"
    );
    Ok(AssembledSnapshotBackend {
        repository: Arc::new(SnapshotRepository::on_node(
            catalog,
            build_artifact_store(config)?,
            node_id,
        )),
        // This half materializes nothing, so it resolves nothing.
        runtime_resolver: None,
    })
}

/// Repository plus optional runtime resolver assembled by the owning binary.
///
/// The repository arrives with the refusing catalog; only the deciding half
/// composes PostgreSQL over the byte store.
pub type RoleStorage = (
    Arc<SnapshotRepository>,
    Option<Arc<dyn SnapshotRuntimeResolver>>,
);

/// The byte store this deployment is configured for, on its own.
///
/// Delete remains available because the origin node may be gone.
pub fn build_artifact_store(config: &AppConfig) -> Result<Arc<dyn SnapshotArtifactStore>> {
    match config.snapshot.repository_backend {
        SnapshotRepositoryBackendKind::PosixFs => {
            let root = config
                .backend
                .posix_fs
                .as_ref()
                .context("backend.posix_fs config is required when repository_backend = posix_fs")?
                .snapshot_store
                .join("repository");
            Ok(posixfs_artifacts_only_store(&root))
        }
        SnapshotRepositoryBackendKind::Oss => {
            let oss_config = config
                .backend
                .oss
                .as_ref()
                .context("backend.oss config is required when repository_backend = oss")?;
            Ok(
                oss::oss_durable_parts(oss_config, snapshot_image_storage_policy(config))?
                    .into_artifacts(),
            )
        }
    }
}

/// Whether committed snapshot rootfs/drive deltas are published back to the
/// source registry or kept as object-storage managed layers.
pub fn snapshot_image_storage_policy(config: &AppConfig) -> SnapshotImageStoragePolicy {
    if config.snapshot.image_publish.enabled {
        SnapshotImageStoragePolicy::SourceRegistry
    } else {
        SnapshotImageStoragePolicy::ObjectStorage
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::mock::MockSnapshotCatalog;
    use crate::snapshot::repository::interfaces::SnapshotArtifactStore;
    use crate::snapshot::types::SnapshotId;

    fn config_with_a_byte_store() -> (tempfile::TempDir, AppConfig) {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let mut config = AppConfig::default();
        config.snapshot.repository_backend = SnapshotRepositoryBackendKind::PosixFs;
        config.backend.posix_fs = Some(crate::cfg::PosixFsBackendConfig {
            snapshot_store: dir.path().join("store"),
        });
        (dir, config)
    }

    fn node_storage() -> RoleStorage {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let root = dir.keep();
        (
            Arc::new(SnapshotRepository::new(
                Arc::new(crate::snapshot::repository::no_catalog::NoSnapshotCatalog),
                posixfs_artifacts_only_store(&root),
            )),
            None::<Arc<dyn SnapshotRuntimeResolver>>,
        )
    }

    #[test]
    fn the_deciding_half_refuses_to_assemble_without_postgresql() {
        let (_dir, config) = config_with_a_byte_store();
        let Err(error) = build_catalog_backed_backend(&config, None) else {
            panic!("an api half with no [pg] holds no catalog and must not start");
        };
        let rendered = format!("{error:#}");
        assert!(
            rendered.contains("[pg]"),
            "the error must name the setting an operator has to add: {rendered}"
        );
    }

    #[test]
    fn the_refusal_names_no_environment_variable_that_does_not_exist() {
        let (_dir, config) = config_with_a_byte_store();
        let Err(error) = build_catalog_backed_backend(&config, None) else {
            panic!("an api half with no [pg] holds no catalog and must not start");
        };
        let rendered = format!("{error:#}");
        assert!(
            !rendered.contains("AENV_PG_DSN"),
            "there is no such environment variable: {rendered}"
        );
        assert!(rendered.contains("[pg].dsn"), "{rendered}");
        assert!(rendered.contains("TOML-file-only"), "{rendered}");
        assert!(rendered.contains("AENV_CONFIG_OVERLAY_PATH"), "{rendered}");
        assert!(rendered.contains("pg-dsn.toml"), "{rendered}");
    }

    #[tokio::test]
    async fn the_deciding_half_reads_the_catalog_it_was_handed() {
        let (_dir, config) = config_with_a_byte_store();
        let catalog = Arc::new(MockSnapshotCatalog::default());
        assert_eq!(catalog.get_calls(), 0);

        let assembled = build_catalog_backed_backend(
            &config,
            Some(Arc::clone(&catalog) as Arc<dyn SnapshotCatalog>),
        )
        .expect("an api half with a catalog assembles");

        assert!(assembled.runtime_resolver.is_none());
        let _ = assembled
            .repository
            .get(&SnapshotId::generate().to_string())
            .await;
        assert_eq!(
            catalog.get_calls(),
            1,
            "the assembled repository read through some other catalog than the one it was handed"
        );
    }

    #[tokio::test]
    async fn the_running_half_assembles_with_a_catalog_that_refuses() {
        let assembled = build_node_snapshot_backend(node_storage());

        let error = assembled
            .repository
            .get("anything")
            .await
            .expect_err("a node must not answer a catalog read");
        assert!(
            matches!(
                error,
                crate::snapshot::repository::RepositoryError::Unsupported { .. }
            ),
            "a node's catalog read must refuse, never report absence: {error}"
        );

        // Node byte storage remains real even though catalog operations refuse.
        let _: Arc<dyn SnapshotArtifactStore> = assembled.repository.artifacts();
    }
}
