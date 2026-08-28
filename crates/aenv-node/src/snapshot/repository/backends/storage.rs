//! The byte half: turning a configured repository backend into something a VM
//! can actually mmap.
//!
//! # 🔴 This module is the compile-time gate
//!
//! Everything here reaches the node-local overlaybd layer store
//! (`crate::image::cache`), the node-local artifact cache
//! (`crate::snapshot::artifact_cache`) and, through the two backends'
//! importing halves, overlaybd itself. A process that boots no microVMs must
//! not link any of it.
//!
//! That used to be a runtime gate — `build_storage_for_role` took the
//! now-deleted `ServerRole` and returned early when it said this process runs
//! no sandbox runtime, and a source-scanning test asserted it was the only
//! call site. The gate is now the crate boundary: this module lives
//! in `aenv-node`, `aenv-api` does not depend on it, and
//! `make check-crate-boundaries` is what fails when that stops being true.
//! See `build_catalog_only_storage` for the half `aenv-api` builds instead.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};

/// 🔴 Re-exported here rather than from `backends` itself: both are the byte
/// half. `backends` is shared, this module is not.
pub use super::oss::OssBackend;
pub use super::posixfs::{PosixFsBackend, PosixFsBackendConfig};
use super::{snapshot_image_storage_policy, RoleStorage};
use crate::cfg::{AppConfig, ConfigManager, SnapshotRepositoryBackendKind};
use crate::image::cache::local_image_services_from_app_config;
use crate::p2p::P2pTransport;
use crate::snapshot::artifact_cache::LocalArtifactCache;
use crate::snapshot::repository::interfaces::SnapshotRuntimeResolver;
use crate::snapshot::repository::SnapshotRepository;

/// Both storage halves, for the binary that has somewhere to run a sandbox.
pub fn build_node_storage(
    config: &AppConfig,
    p2p_transport: Option<Arc<dyn P2pTransport>>,
) -> Result<RoleStorage> {
    let (repository, runtime_resolver) = build_storage_backend(config, p2p_transport)?;
    Ok((repository, Some(runtime_resolver)))
}

pub fn build_storage_backend(
    config: &AppConfig,
    p2p_transport: Option<Arc<dyn P2pTransport>>,
) -> Result<(Arc<SnapshotRepository>, Arc<dyn SnapshotRuntimeResolver>)> {
    let shared_cache_root = shared_runtime_cache_root();
    let overlaybd_layers = local_image_services_from_app_config(config).overlaybd_layers;
    match config.snapshot.repository_backend {
        SnapshotRepositoryBackendKind::PosixFs => {
            let root = config
                .backend
                .posix_fs
                .as_ref()
                .context("backend.posix_fs config is required when repository_backend = posix_fs")?
                .snapshot_store
                .join("repository");
            let cache = LocalArtifactCache::new(shared_cache_root.clone(), None)?;
            Ok(PosixFsBackend::from_parts(
                PosixFsBackendConfig {
                    root,
                    cache_root: Some(shared_cache_root.clone()),
                    runtime_cache_root: Some(shared_cache_root.join("runtime")),
                },
                overlaybd_layers,
                cache,
            )
            .into_parts())
        }
        SnapshotRepositoryBackendKind::Oss => {
            let oss_config = config
                .backend
                .oss
                .as_ref()
                .context("backend.oss config is required when repository_backend = oss")?;
            let snapshot_image_storage = snapshot_image_storage_policy(config);
            let cache =
                LocalArtifactCache::new(shared_cache_root.clone(), oss_config.cache_max_size_gb)?;
            Ok(OssBackend::from_parts(
                oss_config,
                snapshot_image_storage,
                cache,
                shared_cache_root.join("runtime"),
                overlaybd_layers,
                p2p_transport,
            )?
            .into_parts())
        }
    }
}

pub fn shared_runtime_cache_root() -> PathBuf {
    ConfigManager::global_config()
        .snapshot
        .local_cache_path
        .clone()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::SnapshotRepositoryBackendKind;
    use crate::snapshot::mock::InMemorySnapshotCatalog;
    use crate::snapshot::repository::backends::build_catalog_only_storage;
    use crate::snapshot::repository::interfaces::CatalogReadScope;
    use crate::snapshot::types::SnapshotId;
    use crate::snapshot::RepositoryError;

    /// 🔴 The step's own acceptance criterion: the half that runs no sandboxes
    /// assembles the byte half's *lifecycle* and none of its
    /// *materialization*.
    ///
    /// Two claims, and the second is why the first is safe:
    ///
    /// 1. no [`SnapshotRuntimeResolver`] is built at all, so nothing on this
    ///    process holds an overlaybd layer store, a shared artifact cache, or
    ///    a runtime cache root on api's behalf; and
    /// 2. `delete` still works over what *is* built — the row goes, and the
    ///    committed bytes go with it. That half must stay on api: a snapshot's
    ///    origin node can be gone, and a delete that had to be dispatched
    ///    there would leave the row removed and the bytes orphaned.
    ///
    /// The bytes are staged through the *node*-assembled repository, because
    /// that is where staging happens and because this half now refuses it —
    /// see the third claim below.
    ///
    /// 🔴 One catalog, shared by both halves, and supplied by the test. Neither
    /// assembly carries one: the snapshot catalog is PostgreSQL and both of
    /// these byte halves are built over `NoSnapshotCatalog`. In a real cluster
    /// the shared catalog is the `[pg]` pool `aenv-api` puts in front of its own
    /// byte half; here it is `InMemorySnapshotCatalog`, and it has to be shared
    /// or the delete below would be deleting out of a catalog the publish never
    /// reached.
    ///
    /// 3. `publish` through the api-assembled repository is refused rather
    ///    than silently doing nothing. `aenv-api` never stages: every capture
    ///    that reaches it arrived already staged by the node holding the bytes
    ///    (`SnapshotManager::adopt_staged`). The importing half is what needs
    ///    to read overlaybd layer files, so it is the half api does not build.
    #[tokio::test]
    async fn an_api_role_assembles_no_runtime_resolver_and_still_deletes() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let mut config = AppConfig::default();
        config.snapshot.repository_backend = SnapshotRepositoryBackendKind::PosixFs;
        config.backend.posix_fs = Some(crate::cfg::PosixFsBackendConfig {
            snapshot_store: dir.path().join("store"),
        });

        let (api_bytes, runtime_resolver) = build_catalog_only_storage(&config)
            .expect("the api half should assemble a storage backend");

        let catalog = Arc::new(InMemorySnapshotCatalog::default());
        let repository = Arc::new(SnapshotRepository::new(
            Arc::clone(&catalog) as Arc<dyn crate::snapshot::repository::SnapshotCatalog>,
            api_bytes.artifacts(),
        ));

        assert!(
            runtime_resolver.is_none(),
            "the catalog-only half built a snapshot runtime resolver; it resolves nothing and \
             must hold none of what a resolver drags in"
        );

        let workspace = tempfile::TempDir::new().expect("tempdir");
        let (_, _, manifest) = crate::snapshot::mock::write_mock_built_artifacts(workspace.path())
            .expect("mock built artifacts should write");
        let id = SnapshotId::generate();
        let metadata = crate::snapshot::SnapshotPublishMetadata {
            id: id.clone(),
            ..crate::snapshot::SnapshotPublishMetadata::mock()
        };

        // The half that holds the bytes writes them.
        let root = dir.path().join("store").join("repository");
        let node_repository = PosixFsBackend::from_parts(
            PosixFsBackendConfig {
                root: root.clone(),
                cache_root: Some(dir.path().join("cache")),
                runtime_cache_root: Some(dir.path().join("cache").join("runtime")),
            },
            crate::image::cache::local_image_services_from_app_config(&config).overlaybd_layers,
            crate::snapshot::artifact_cache::LocalArtifactCache::new(
                dir.path().join("cache"),
                None,
            )
            .expect("artifact cache"),
        )
        .into_parts()
        .0;
        let node_repository = Arc::new(SnapshotRepository::new(
            Arc::clone(&catalog) as Arc<dyn crate::snapshot::repository::SnapshotCatalog>,
            node_repository.artifacts(),
        ));
        node_repository
            .publish(metadata.clone(), manifest.clone())
            .await
            .expect("the node-assembled repository stages the bytes");

        // And this half refuses to, rather than pretending it can.
        let refusal = repository
            .publish(metadata, manifest)
            .await
            .expect_err("aenv-api must refuse to import snapshot artifacts");
        assert!(
            matches!(refusal, RepositoryError::Unsupported { .. }),
            "the refusal must say the feature is unavailable, not fail as a backend error: \
             {refusal:?}"
        );

        let committed_dir = dir
            .path()
            .join("store")
            .join("repository")
            .join("snapshots")
            .join(id.to_string());
        assert!(
            committed_dir.exists(),
            "the committed bytes should be on disk before the delete"
        );

        repository
            .delete(&id.to_string())
            .await
            .expect("delete must work on a process with no runtime resolver");

        assert!(
            repository
                .get_scoped(&id.to_string(), CatalogReadScope::AnyStatus)
                .await
                .expect("the catalog should answer")
                .is_none(),
            "the row survived a delete on aenv-api"
        );
        assert!(
            !committed_dir.exists(),
            "the bytes survived a delete on aenv-api — this is the orphan the whole \
             api-side delete exists to prevent"
        );
    }
}
