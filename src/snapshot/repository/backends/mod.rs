pub mod central;
pub(crate) mod common;
pub(crate) mod oss;
pub(crate) mod posixfs;

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};

use crate::cfg::{
    AppConfig, ConfigManager, SnapshotCatalogRead, SnapshotCatalogWrite,
    SnapshotImageStoragePolicy, SnapshotRepositoryBackendKind,
};
use crate::image::cache::local_image_services_from_app_config;
use crate::p2p::P2pTransport;
use crate::snapshot::artifact_cache::LocalArtifactCache;
use crate::snapshot::repository::interfaces::SnapshotRuntimeResolver;
use crate::snapshot::repository::mirror::{
    CatalogReadSide, DualWriteCatalog, MirrorBacklog, MirrorCompensator,
};
use crate::snapshot::repository::SnapshotRepository;
pub use central::{CatalogReadScope, CatalogRefusal, CatalogWrite, CentralSnapshotCatalog};
pub use oss::OssBackend;
pub use posixfs::{PosixFsBackend, PosixFsBackendConfig};

/// Everything the snapshot layer needs from storage, assembled.
pub struct AssembledSnapshotBackend {
    pub repository: Arc<SnapshotRepository>,
    pub runtime_resolver: Arc<dyn SnapshotRuntimeResolver>,
    /// Alive only while the catalog is double-written. Dropping it stops the
    /// replay, so it is held for as long as the manager is.
    pub mirror_compensator: Option<Arc<MirrorCompensator>>,
}

/// Builds the storage backend, and puts the catalog behind a double write when
/// the configuration asks for one.
///
/// 🔴 The wrapping is here rather than inside a backend because it is not a
/// property of any backend: it composes whichever catalog the configured
/// backend has with the central one, and the byte half is untouched by it. That
/// is what the trait split bought.
pub async fn build_snapshot_backend(
    p2p_transport: Option<Arc<dyn P2pTransport>>,
) -> Result<AssembledSnapshotBackend> {
    let config = ConfigManager::global_config();
    let (repository, runtime_resolver) = build_storage_backend(config, p2p_transport)?;

    let Some(central) = build_central_catalog(config)? else {
        return Ok(AssembledSnapshotBackend {
            repository,
            runtime_resolver,
            mirror_compensator: None,
        });
    };

    let backlog = MirrorBacklog::open(&config.snapshot.catalog.mirror_backlog_path).await?;
    // 🔴 Before anything is served. A node whose read side has just been moved
    // back onto a store that is behind would answer "absent" for every snapshot
    // the mirror still owes, and absence is an instruction downstream.
    backlog
        .guard_read_side(match config.snapshot.catalog.read {
            SnapshotCatalogRead::ObjectStore => CatalogReadSide::ObjectStore,
            SnapshotCatalogRead::Postgres => CatalogReadSide::Postgres,
        })
        .await?;

    let object_store_catalog = repository.catalog();
    let compensator = MirrorCompensator::spawn(
        Arc::clone(&backlog),
        Arc::clone(&object_store_catalog),
        std::time::Duration::from_secs(config.snapshot.catalog.mirror_compensator_interval_secs),
    );

    let dual = Arc::new(DualWriteCatalog::new(
        central,
        object_store_catalog,
        backlog,
    ));
    let artifacts = repository.artifacts();
    let node_id = crate::identity::local_node_id();
    tracing::info!(
        target: "agentenv",
        catalog_write = "both",
        catalog_read = "object_store",
        node_id = %node_id,
        "snapshot catalog is double-written; object storage still answers reads"
    );

    Ok(AssembledSnapshotBackend {
        repository: Arc::new(SnapshotRepository::on_node(dual, artifacts, node_id)),
        runtime_resolver,
        mirror_compensator: Some(Arc::new(compensator)),
    })
}

/// The central catalog client, when the configuration asks for one.
///
/// 🔴 A missing scheduler endpoint is a startup failure rather than a silent
/// fall back to writing one store. The operator asked for a second copy, and a
/// node that quietly kept only the first would only reveal the difference when
/// somebody tried to read the second.
fn build_central_catalog(config: &AppConfig) -> Result<Option<Arc<CentralSnapshotCatalog>>> {
    match config.snapshot.catalog.write {
        SnapshotCatalogWrite::ObjectStore => return Ok(None),
        SnapshotCatalogWrite::Both => {}
        // Refused by `validate_snapshot_catalog` before this is reached; the
        // arm exists so adding the mode later is a compile error here rather
        // than a silent no-op.
        SnapshotCatalogWrite::Postgres => {
            anyhow::bail!("snapshot.catalog.write = \"postgres\" is not served by this build")
        }
    }

    let endpoint = config
        .cluster
        .scheduler_endpoint
        .as_deref()
        .map(str::trim)
        .filter(|endpoint| !endpoint.is_empty())
        .context(
            "snapshot.catalog.write = \"both\" requires a scheduler endpoint; \
             set AENV_OBSERVABILITY_SCHEDULER_ENDPOINT",
        )?;
    let identity = crate::identity::NodeIdentity::from_config(&config.node_identity);

    Ok(Some(Arc::new(CentralSnapshotCatalog::connect_lazy(
        endpoint,
        identity.cluster_id,
        identity.id,
    )?)))
}

fn build_storage_backend(
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
            let snapshot_image_storage = if config.snapshot.image_publish.enabled {
                SnapshotImageStoragePolicy::SourceRegistry
            } else {
                SnapshotImageStoragePolicy::ObjectStorage
            };
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

pub(crate) fn shared_runtime_cache_root() -> PathBuf {
    ConfigManager::global_config()
        .snapshot
        .local_cache_path
        .clone()
}
