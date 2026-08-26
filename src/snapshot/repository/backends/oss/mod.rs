mod artifacts;
mod catalog;
mod client;
mod config;
mod import;
mod layout;
mod resolver;
#[cfg(test)]
mod test_support;

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};

use crate::cfg::{OssBackendConfig, SnapshotImageStoragePolicy};
use crate::image::cache::{local_image_services_from_global_config, OverlaybdLayerStore};
use crate::p2p::P2pTransport;
use crate::snapshot::artifact_cache::LocalArtifactCache;
use crate::snapshot::repository::interfaces::SnapshotRuntimeResolver;
use crate::snapshot::repository::SnapshotRepository;

pub(crate) use self::artifacts::OssSnapshotArtifactStore;
pub(crate) use self::catalog::OssSnapshotCatalog;
pub(crate) use self::client::OssClient;
pub(crate) use self::config::NormalizedOssConfig;
pub(crate) use self::layout::OssSnapshotArtifactLayout;
use self::resolver::OssRuntimeResolver;

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
    pub(crate) fn from_parts(
        config: &OssBackendConfig,
        snapshot_image_storage: SnapshotImageStoragePolicy,
        cache: Arc<LocalArtifactCache>,
        runtime_root: PathBuf,
        store: Arc<dyn OverlaybdLayerStore>,
        p2p_transport: Option<Arc<dyn P2pTransport>>,
    ) -> Result<Self> {
        let OssDurableParts {
            repository: _,
            client,
            managed_layers_repo_blob_url,
        } = Self::durable_parts(config, snapshot_image_storage)?;
        // 🔴 Not `durable_parts`' repository. That one carries the delete-only
        // artifact half; this process holds bytes and has to be able to import
        // them, so it composes the same catalog with the importing half.
        let repository = Arc::new(SnapshotRepository::new(
            Arc::new(OssSnapshotCatalog::new(Arc::clone(&client))),
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

    /// The durable halves on their own: the catalog and the artifact store,
    /// already composed into a [`SnapshotRepository`].
    ///
    /// 🔴 Split out of [`OssBackend::from_parts`] so a process that never turns
    /// a snapshot into local bytes can still *delete* one. Deleting is
    /// `catalog.delete_record` followed by `artifacts.delete_artifacts`, and on
    /// this backend the second half is a pure network operation — an
    /// object-store `DELETE` under the snapshot's prefix, plus
    /// `rollback_publication` against the source registry (see
    /// [`OssSnapshotArtifactStore::delete_artifacts`]). It never needed an
    /// [`OverlaybdLayerStore`], a [`LocalArtifactCache`], or a runtime root; it
    /// was coupled to all three only because `from_parts` took one set of
    /// arguments for both halves and the *other* half — materializing layers
    /// onto local disk — is what actually consumes them.
    ///
    /// The delete has to stay here rather than being shipped to the node that
    /// owns the bytes: that node may be gone (hard death, or rolled), and a
    /// delete with nowhere to go leaves the row removed, the bytes orphaned,
    /// and nobody holding a record of either.
    pub(crate) fn durable_parts(
        config: &OssBackendConfig,
        snapshot_image_storage: SnapshotImageStoragePolicy,
    ) -> Result<OssDurableParts> {
        let config = NormalizedOssConfig::new(config, snapshot_image_storage)?;
        let managed_layers_repo_blob_url = config.managed_layers_repo_blob_url();
        let client = Arc::new(OssClient::new(
            config.bucket().to_string(),
            config.endpoint().to_string(),
            config.region().to_string(),
            config.prefix().to_string(),
            config.credential_source(),
        )?);

        let repository = Arc::new(SnapshotRepository::new(
            Arc::new(OssSnapshotCatalog::new(Arc::clone(&client))),
            Arc::new(OssSnapshotArtifactStore::new(Arc::clone(&client))),
        ));

        Ok(OssDurableParts {
            repository,
            client,
            managed_layers_repo_blob_url,
        })
    }

    /// Splits the backend into its repository and runtime-resolution components.
    pub fn into_parts(self) -> (Arc<SnapshotRepository>, Arc<dyn SnapshotRuntimeResolver>) {
        (self.repository, self.runtime_resolver)
    }
}

/// What [`OssBackend::durable_parts`] hands back: the durable repository, plus
/// the two values the runtime resolver needs on top of it. Only
/// [`OssBackend::from_parts`] consumes the latter two — every other caller
/// wants [`Self::into_repository`].
pub(crate) struct OssDurableParts {
    repository: Arc<SnapshotRepository>,
    client: Arc<OssClient>,
    managed_layers_repo_blob_url: String,
}

impl OssDurableParts {
    /// The repository on its own, dropping what only the resolver would use.
    pub(crate) fn into_repository(self) -> Arc<SnapshotRepository> {
        self.repository
    }
}
