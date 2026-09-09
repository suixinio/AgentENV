//! The OSS backend a process that runs sandboxes builds: the durable halves
//! plus the node-local runtime resolver that materializes runnable overlaybd
//! configs.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};

use super::config::NormalizedOssConfig;
use super::durable::{oss_durable_parts, OssDurableParts};
use super::import;
use super::resolver::OssRuntimeResolver;
use crate::cfg::{OssBackendConfig, SnapshotImageStoragePolicy};
use crate::image::cache::{local_image_services_from_global_config, OverlaybdLayerStore};
use crate::p2p::P2pTransport;
use crate::snapshot::artifact_cache::LocalArtifactCache;
use crate::snapshot::repository::interfaces::SnapshotRuntimeResolver;
use crate::snapshot::repository::no_catalog::NoSnapshotCatalog;
use crate::snapshot::repository::SnapshotRepository;

/// OSS-backed snapshot backend.
///
/// Combines the durable committed-state repository (stored in OSS) with a
/// node-local runtime resolver that materializes runnable overlaybd configs.
pub struct OssBackend {
    repository: Arc<SnapshotRepository>,
    runtime_resolver: Arc<dyn SnapshotRuntimeResolver>,
}

impl OssBackend {
    /// Build the OSS backend from config.
    ///
    /// This convenience constructor remains available for tests and direct
    /// callers. The main backend factory constructs a shared cache once and
    /// uses [`OssBackend::from_parts`] instead.
    pub fn new(config: &OssBackendConfig, cache_root: PathBuf) -> Result<Self> {
        let cache = LocalArtifactCache::new(cache_root.clone(), config.cache_max_size_gb)?;
        Self::from_parts(
            config,
            SnapshotImageStoragePolicy::default(),
            cache,
            cache_root.join("runtime"),
            local_image_services_from_global_config().overlaybd_layers,
            None,
        )
    }

    /// Build the OSS backend from config plus a shared node-local cache.
    pub fn from_parts(
        config: &OssBackendConfig,
        snapshot_image_storage: SnapshotImageStoragePolicy,
        cache: Arc<LocalArtifactCache>,
        runtime_root: PathBuf,
        store: Arc<dyn OverlaybdLayerStore>,
        p2p_transport: Option<Arc<dyn P2pTransport>>,
    ) -> Result<Self> {
        let OssDurableParts {
            artifacts: _,
            client,
            managed_layers_repo_blob_url,
        } = oss_durable_parts(config, snapshot_image_storage)?;
        // Compose the importing byte half; neither repository carries a catalog.
        let repository = Arc::new(SnapshotRepository::new(
            Arc::new(NoSnapshotCatalog),
            Arc::new(import::OssSnapshotArtifactImporter::new(
                Arc::clone(&client),
                NormalizedOssConfig::new(config, snapshot_image_storage)?.snapshot_image_storage(),
            )),
        ));

        std::fs::create_dir_all(&runtime_root)
            .with_context(|| format!("create oss runtime root '{}'", runtime_root.display()))?;
        let runtime_resolver: Arc<dyn SnapshotRuntimeResolver> = Arc::new(OssRuntimeResolver::new(
            client,
            cache,
            runtime_root,
            store,
            managed_layers_repo_blob_url,
            p2p_transport,
        )?);

        Ok(Self {
            repository,
            runtime_resolver,
        })
    }

    /// Splits the backend into its repository and runtime-resolution components.
    pub fn into_parts(self) -> (Arc<SnapshotRepository>, Arc<dyn SnapshotRuntimeResolver>) {
        (self.repository, self.runtime_resolver)
    }
}
