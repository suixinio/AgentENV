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
use crate::snapshot::repository::interfaces::SnapshotCatalog;
use crate::snapshot::repository::interfaces::SnapshotRuntimeResolver;
use crate::snapshot::repository::mirror::{
    admit_read_side, CatalogReadSide, CentralCatalogWrites, DualWriteCatalog, MirrorBacklog,
    MirrorCompensator, MirrorDirection, MirrorTargets, ObjectStoreCensus,
};
use crate::snapshot::repository::SnapshotRepository;
pub use central::{CatalogRefusal, CatalogWrite, CentralSnapshotCatalog};
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
        // 🔴 A node told to read PostgreSQL with no central catalog wired would
        // otherwise serve every read from object storage while its
        // configuration says otherwise — the quietest possible version of the
        // failure the whole matrix exists to prevent.
        if config.snapshot.catalog.read == SnapshotCatalogRead::Postgres {
            anyhow::bail!(
                "snapshot.catalog.read = \"postgres\" was configured but no central catalog was \
                 built, so reads would silently come from object storage. Set \
                 snapshot.catalog.write = \"both\" with a scheduler endpoint, or set read = \
                 \"object_store\"."
            );
        }
        let compensator = drain_a_rolled_back_mirror(
            &config.snapshot.catalog.mirror_backlog_path,
            std::time::Duration::from_secs(
                config.snapshot.catalog.mirror_compensator_interval_secs,
            ),
            repository.catalog(),
        )
        .await?;
        return Ok(AssembledSnapshotBackend {
            mirror_compensator: compensator,
            repository,
            runtime_resolver,
        });
    };

    let backlog = MirrorBacklog::open(&config.snapshot.catalog.mirror_backlog_path).await?;
    let object_store_catalog = repository.catalog();

    // 🔴 Before the guard, because it is what the guard reads. The double write
    // only mirrors writes made after it was turned on, so every snapshot older
    // than the switch is invisible to both gauges — measured on this cluster at
    // the moment of the flip as `lag = 0, diverged = 0`, PostgreSQL 0 rows,
    // object storage 32. Queueing that history as ordinary debt is what makes
    // `lag == 0 && diverged == 0` mean "the two catalogs agree" rather than
    // "nothing is queued".
    //
    // 🔴 A failure here does not stop the node. The history is queued so that a
    // *later* switch is honest, and the switch is what the guard refuses; a
    // node that cannot list its object store right now is a node that should
    // keep serving, and the marker stays unset so the next start tries again.
    if let Err(error) = backlog
        .queue_history_toward_central(object_store_catalog.as_ref())
        .await
    {
        tracing::error!(
            target: "agentenv",
            %error,
            "could not queue the object-store catalog's history for the central catalog; the \
             read side will stay refused from PostgreSQL until a start succeeds at it"
        );
    }

    let configured_read = match config.snapshot.catalog.read {
        SnapshotCatalogRead::ObjectStore => CatalogReadSide::ObjectStore,
        SnapshotCatalogRead::Postgres => CatalogReadSide::Postgres,
    };

    // 🔴 Before anything is served. A node whose read side has just been moved
    // onto a store that does not hold everything would answer "absent" for
    // every snapshot the other one has, and absence is an instruction
    // downstream: callers delete artifacts and refuse resumes on it.
    let populations = admit_read_side(
        configured_read,
        &backlog,
        &ObjectStoreCensus(object_store_catalog.as_ref()),
        central.as_ref(),
    )
    .await?;
    if configured_read == CatalogReadSide::Postgres {
        tracing::info!(
            target: "agentenv",
            catalog_rows = populations.central_rows,
            object_store_rows = populations.object_store_rows,
            mirror_lag = backlog.lag_toward(MirrorDirection::Central),
            mirror_diverged = backlog.diverged_toward(MirrorDirection::Central),
            "reads are served from the central catalog. The admission compared identity, not \
             content: two rows sharing an id and disagreeing about anything else pass it, and \
             rows both stores took on the live path are compared field by field by nothing"
        );
    }

    let compensator = MirrorCompensator::spawn(
        Arc::clone(&backlog),
        MirrorTargets::object_store(Arc::clone(&object_store_catalog))
            .with_central(Arc::clone(&central) as Arc<dyn CentralCatalogWrites>),
        std::time::Duration::from_secs(config.snapshot.catalog.mirror_compensator_interval_secs),
    );

    let dual = DualWriteCatalog::new(
        Arc::clone(&central) as Arc<dyn CentralCatalogWrites>,
        object_store_catalog,
        backlog,
    );
    let dual = Arc::new(match configured_read {
        CatalogReadSide::ObjectStore => dual,
        // 🔴 The whole `SnapshotCatalog` surface, so the resolvable scope the
        // central client already applies — `status_group = 'ready'` — comes
        // with it. Reading through the narrower write trait would have meant
        // restating that predicate here, one layer away from the queries that
        // own it.
        CatalogReadSide::Postgres => {
            dual.reading_from_central(Arc::clone(&central) as Arc<dyn SnapshotCatalog>)
        }
    });
    let artifacts = repository.artifacts();
    let node_id = crate::identity::local_node_id();
    match configured_read {
        CatalogReadSide::ObjectStore => tracing::info!(
            target: "agentenv",
            catalog_write = "both",
            catalog_read = "object_store",
            node_id = %node_id,
            "snapshot catalog is double-written; object storage still answers reads"
        ),
        CatalogReadSide::Postgres => tracing::info!(
            target: "agentenv",
            catalog_write = "both",
            catalog_read = "postgres",
            node_id = %node_id,
            "snapshot catalog is double-written; the central catalog answers reads, and object \
             storage is the way back"
        ),
    }

    Ok(AssembledSnapshotBackend {
        repository: Arc::new(SnapshotRepository::on_node(dual, artifacts, node_id)),
        runtime_resolver,
        mirror_compensator: Some(Arc::new(compensator)),
    })
}

/// Keeps paying off a mirror that the double write was turned off underneath.
///
/// 🔴 The rollback from `write = "both"` is `write = "object_store"`, and it is
/// only lossless if what object storage was still owed gets written. Those
/// entries are publishes that *succeeded* — the caller was told so — and object
/// storage is about to be the only catalog there is, so abandoning them would
/// make those snapshots disappear for good. The replay needs no central
/// catalog, only the object store, so it can run perfectly well after the
/// switch — and the entries owed the *other* way are left alone rather than
/// dropped, because the central catalog is the thing being turned off.
///
/// Nothing is created here: with no backlog on disk there is nothing to drain,
/// which is every node that has never double-written.
async fn drain_a_rolled_back_mirror(
    path: &std::path::Path,
    interval: std::time::Duration,
    object_store: Arc<dyn crate::snapshot::repository::interfaces::SnapshotCatalog>,
) -> Result<Option<Arc<MirrorCompensator>>> {
    if !path.exists() {
        return Ok(None);
    }

    let backlog = MirrorBacklog::open(path).await?;
    // 🔴 Object storage's debt, not the total. The other direction's entries are
    // owed to the catalog this rollback is switching off; spinning a loop that
    // could only skip them would burn a pass every interval for no reason, and
    // dropping them is not this function's call either.
    let owed = backlog.lag_toward(MirrorDirection::ObjectStore);
    if owed == 0 {
        // 🔴 Say what is being left behind. Opening the backlog has already
        // published both gauges for both directions, so the numbers are on the
        // metrics endpoint — but a rolled-back node with central-direction
        // entries still on disk looks, from its logs, exactly like a node that
        // never double-wrote. Those entries do not go away: they are owed to a
        // catalog this configuration is not writing, and the next flip back to
        // `write = "both"` brings them straight back as pinned lag.
        let central_owed = backlog.lag_toward(MirrorDirection::Central);
        let central_diverged = backlog.diverged_toward(MirrorDirection::Central);
        if central_owed > 0 || central_diverged > 0 {
            tracing::warn!(
                target: "agentenv",
                mirror_lag = central_owed,
                mirror_diverged = central_diverged,
                backlog = %path.display(),
                "the snapshot catalog mirror backlog still holds writes the central catalog is \
                 owed, and disagreements nobody settled, from when double writing was on. \
                 Nothing replays them while write = \"object_store\" — the catalog they are \
                 owed to is switched off — and turning double writing back on will surface them \
                 as lag that was there all along"
            );
        }
        return Ok(None);
    }

    tracing::warn!(
        target: "agentenv",
        mirror_lag = owed,
        backlog = %path.display(),
        "snapshot catalog double writing is off, but object storage is still owed writes from \
         when it was on; replaying them, because those snapshots reported success and object \
         storage is now the only catalog that has them"
    );

    Ok(Some(Arc::new(MirrorCompensator::spawn(
        backlog,
        MirrorTargets::object_store(object_store),
        interval,
    ))))
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::snapshot::repository::interfaces::SnapshotCatalog;
    use crate::snapshot::repository::mirror::test_doubles::{record_for, ScriptedCatalog};
    use crate::snapshot::types::SnapshotId;

    /// 🔴 A node that has never double-written must not have a backlog created
    /// underneath it. Creating one here would put a RocksDB directory on every
    /// node in the fleet and make "there is nothing outstanding" a thing that
    /// had to be read rather than a thing that was obvious.
    #[tokio::test]
    async fn a_node_that_never_double_wrote_gets_nothing() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let path = dir.path().join("never-existed");
        let catalog = Arc::new(ScriptedCatalog::default()) as Arc<dyn SnapshotCatalog>;

        let compensator = drain_a_rolled_back_mirror(&path, Duration::from_secs(1), catalog)
            .await
            .expect("the decision should be answerable");

        assert!(compensator.is_none());
        assert!(!path.exists(), "nothing may be created here");
    }

    /// A backlog that is already paid off needs no loop.
    #[tokio::test]
    async fn a_drained_backlog_needs_no_compensator() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let path = dir.path().join("mirror");
        MirrorBacklog::open(&path)
            .await
            .expect("the backlog should open");

        let catalog = Arc::new(ScriptedCatalog::default()) as Arc<dyn SnapshotCatalog>;
        let compensator = drain_a_rolled_back_mirror(&path, Duration::from_secs(1), catalog)
            .await
            .expect("the decision should be answerable");

        assert!(compensator.is_none());
    }

    /// 🔴 The whole "rolling back loses nothing" claim, and it runs at startup.
    ///
    /// The entries left behind are publishes that *succeeded* — the caller was
    /// told so — and object storage is about to be the only catalog there is.
    /// Abandoning them makes those snapshots disappear for good.
    #[tokio::test(start_paused = true)]
    async fn a_rollback_that_still_owes_object_storage_keeps_paying() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let path = dir.path().join("mirror");
        let id = SnapshotId::generate();
        {
            let backlog = MirrorBacklog::open(&path)
                .await
                .expect("the backlog should open");
            crate::snapshot::repository::mirror::test_support::owe_a_create(
                &backlog,
                MirrorDirection::ObjectStore,
                record_for(&id),
            )
            .await;
        }

        let catalog = Arc::new(ScriptedCatalog::default());
        let compensator = drain_a_rolled_back_mirror(
            &path,
            Duration::from_secs(1),
            Arc::clone(&catalog) as Arc<dyn SnapshotCatalog>,
        )
        .await
        .expect("the decision should be answerable")
        .expect("a backlog that is still owed must keep a compensator alive");

        for _ in 0..50 {
            if catalog.calls().contains(&format!("create:{id}")) {
                break;
            }
            tokio::time::advance(Duration::from_millis(1_100)).await;
            for _ in 0..20 {
                tokio::task::yield_now().await;
            }
        }
        drop(compensator);
        assert!(
            catalog.calls().contains(&format!("create:{id}")),
            "the rollback must replay what object storage was owed: {:?}",
            catalog.calls()
        );
    }

    /// 🔴 A debt owed the *other* way does not start a loop. The central catalog
    /// is the thing being switched off, so there is nothing to replay into and a
    /// pass every interval would only skip.
    #[tokio::test]
    async fn a_debt_owed_to_the_catalog_being_switched_off_starts_nothing() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let path = dir.path().join("mirror");
        {
            let backlog = MirrorBacklog::open(&path)
                .await
                .expect("the backlog should open");
            crate::snapshot::repository::mirror::test_support::owe_a_create(
                &backlog,
                MirrorDirection::Central,
                record_for(&SnapshotId::generate()),
            )
            .await;
            assert_eq!(backlog.lag(), 1);
        }

        let catalog = Arc::new(ScriptedCatalog::default()) as Arc<dyn SnapshotCatalog>;
        let compensator = drain_a_rolled_back_mirror(&path, Duration::from_secs(1), catalog)
            .await
            .expect("the decision should be answerable");

        assert!(compensator.is_none());
    }
}
