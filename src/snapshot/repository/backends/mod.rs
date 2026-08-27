pub mod central;
pub mod common;
pub mod oss;
pub mod posixfs;

use std::sync::Arc;

use anyhow::{Context, Result};

use crate::cfg::{
    AppConfig, ConfigManager, SnapshotCatalogRead, SnapshotCatalogWrite,
    SnapshotImageStoragePolicy, SnapshotRepositoryBackendKind,
};
use crate::snapshot::repository::interfaces::SnapshotArtifactStore;
use crate::snapshot::repository::interfaces::SnapshotCatalog;
use crate::snapshot::repository::interfaces::SnapshotRuntimeResolver;
use crate::snapshot::repository::mirror::{
    admit_read_side_with_confirmation, require_read_side_confirmed, CatalogCensus, CatalogReadSide,
    CentralCatalogWrites, DualWriteCatalog, MirrorBacklog, MirrorCompensator, MirrorDirection,
    MirrorTargets, ObjectStoreCensus, ReadSideConfirmationStore,
};
use crate::snapshot::repository::SnapshotRepository;
pub use central::{CatalogRefusal, CatalogWrite, CentralSnapshotCatalog};
use posixfs::posixfs_catalog_only_repository;

/// Everything the snapshot layer needs from storage, assembled.
pub struct AssembledSnapshotBackend {
    pub repository: Arc<SnapshotRepository>,
    /// 🔴 `None` on every role that runs no sandbox runtime — today that is
    /// `--role api`. Resolving a snapshot is not a lookup: it downloads
    /// `vm_state.bin` onto this machine's disk, materializes the memory and
    /// rootfs overlaybd `image.json` files, and leases all of it in this
    /// process's local artifact cache. An api replica boots nothing, so it
    /// builds none of that; see `build_storage_for_role`.
    pub runtime_resolver: Option<Arc<dyn SnapshotRuntimeResolver>>,
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
    // 🔴 Built by the caller — see [`RoleStorage`]. This function composes
    // whichever catalog the configured backend has with the central one, and
    // the byte half is untouched by it; which byte half exists at all is the
    // calling binary's decision, and now its crate's.
    storage: RoleStorage,
    // 🔴 `None` for `--role node` always — see
    // `src/bin/aenv-api.rs::build_pg_pool`. Consumed by the read-side
    // admission's shared confirmation (Stage B step 4,
    // `docs/proposals/_sd-phase4-stageB-catalog.md` §7/§5.1) and, when
    // present, by `build_central_catalog`, which then uses the
    // `PostgresSnapshotCatalog` behind it in place of the gRPC hop to
    // `services/scheduler` for both `write = "both"` and
    // `write = "postgres"`.
    //
    // 🔴 Already-built parts rather than the `sqlx::PgPool` they come from:
    // constructing them is the deciding half's business, and this function is
    // shared. See [`PgCatalogParts`].
    pg: Option<PgCatalogParts>,
    role: crate::role::ServerRole,
) -> Result<AssembledSnapshotBackend> {
    let config = ConfigManager::global_config();
    let (repository, runtime_resolver) = storage;

    // 🔴 P2 (task's own "phase4-close"): `--role node` never queries this
    // catalog at all -- both of its request-time reads
    // (`create`'s `Source::Snapshot` arm, `build_template`'s
    // `Base::BaseSnapshotRef` arm) are pre-resolved by api and sent down
    // with the request, and only ever fall back to `repository.get_scoped`
    // (served by whatever `build_storage_backend` above already built,
    // object-storage-backed) during a mixed-version rolling upgrade
    // window -- never to a central Postgres/gRPC catalog. See
    // `ServerRole::never_constructs_a_central_snapshot_catalog`'s own doc
    // for why this skips *both* the `write = "postgres"` bail below (no
    // `[pg]`, ever, on this role) and the `write = "both"` gRPC client (a
    // live dependency on a scheduler this role has no reason to reach),
    // and why it also skips the `read == "postgres"` refusal a few lines
    // down: that refusal exists to catch reads this backend actually
    // serves silently diverging from the configured read side, and a node
    // role serves none through this catalog at all.
    if role.never_constructs_a_central_snapshot_catalog() {
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
    }

    let Some(central) = build_central_catalog(config, pg.as_ref().map(|parts| &parts.central))?
    else {
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

    // 🔴 `write = "postgres"` drops the object-store copy of the catalog
    // entirely — `validate_snapshot_catalog` only lets this pair reach here
    // with `read = "postgres"` too, so there is nothing for the *ongoing*
    // mirror to reconcile: object storage never receives a catalog write in
    // this mode, ever, so no backlog and no compensator are spawned. Byte
    // storage (`repository.artifacts()`) is untouched — this mode only
    // changes where catalog rows live.
    //
    // 🔴 One check does still run, though: `validate_snapshot_catalog`'s own
    // doc says this pair "is allowed once the read side has been served from
    // PostgreSQL for an observation period and the mirror lag has been 0
    // throughout" — a claim that layer cannot verify (it is a pure config
    // check, no database access). `PgReadSideConfirmation` is precisely the
    // record `write = "both", read = "postgres"` writes once its own guard
    // (`admit_read_side_with_confirmation`'s population comparison) has
    // passed, so refusing to start here unless that record already says
    // "confirmed" is what stops an operator from jumping straight from
    // `object_store`/`object_store` to `postgres`/`postgres` in one step and
    // silently orphaning every snapshot object storage still holds.
    if config.snapshot.catalog.write == SnapshotCatalogWrite::Postgres {
        let confirmation = pg
            .as_ref()
            .map(|parts| Arc::clone(&parts.read_side_confirmation))
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "unreachable: build_central_catalog only returns Some for write = \"postgres\" \
                     when [pg] is configured"
                )
            })?;
        require_read_side_confirmed(confirmation.as_ref()).await?;

        let node_id = crate::identity::local_node_id();
        tracing::info!(
            target: "agentenv",
            catalog_write = "postgres",
            catalog_read = "postgres",
            node_id = %node_id,
            "snapshot catalog is served solely by PostgreSQL; object storage holds no catalog \
             rows"
        );
        return Ok(assemble_postgres_only_backend(
            central.reads,
            repository.artifacts(),
            runtime_resolver,
            node_id,
        ));
    }

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

    let targets = MirrorTargets::object_store(Arc::clone(&object_store_catalog))
        .with_central(Arc::clone(&central.writes));

    // 🔴 Before the guard, because the guard refuses on debt and the only
    // thing that pays debt off used to start after it. A node reading
    // PostgreSQL that is being rolled back to object storage is refused while
    // object storage still owes writes — and the compensator that would settle
    // them is spawned below, on a start that never happens. The rollback was
    // therefore unreachable from precisely the state that needs it.
    //
    // This is bounded and it decides nothing: the guard runs either way, on
    // whatever is left.
    backlog
        .settle_before_reading_from(configured_read, &targets)
        .await;

    // 🔴 Before anything is served. A node whose read side has just been moved
    // onto a store that does not hold everything would answer "absent" for
    // every snapshot the other one has, and absence is an instruction
    // downstream: callers delete artifacts and refuse resumes on it.
    //
    // 🔴 `targets`, because the comparison is allowed to repair before it
    // refuses. What it replays is what the queue already owes the central
    // catalog — including the history queued a few lines above — and that is
    // the difference between an api replica whose `$AENV_HOME` is scratch
    // starting and one that can never start again: it meets this comparison on
    // every start, and the compensator that would close the difference is
    // spawned below, on a start that never happens.
    // 🔴 Only when a `[pg]` pool exists at all — a replica with no pool
    // configured falls back to exactly the pre-Stage-B behaviour (the
    // node-local `MirrorBacklog` question `admit_read_side_with_confirmation`
    // asks when this is `None`), which matters for any `write = "both"`
    // deployment that has not yet been given a `[pg]` DSN.
    let shared_confirmation = pg
        .as_ref()
        .map(|parts| Arc::clone(&parts.read_side_confirmation));
    let populations = admit_read_side_with_confirmation(
        configured_read,
        &backlog,
        &targets,
        &ObjectStoreCensus(object_store_catalog.as_ref()),
        central.census.as_ref(),
        shared_confirmation.as_deref(),
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
        targets,
        std::time::Duration::from_secs(config.snapshot.catalog.mirror_compensator_interval_secs),
    );

    let dual = DualWriteCatalog::new(Arc::clone(&central.writes), object_store_catalog, backlog);
    let dual = Arc::new(match configured_read {
        CatalogReadSide::ObjectStore => dual,
        // 🔴 The whole `SnapshotCatalog` surface, so the resolvable scope the
        // central client already applies — `status_group = 'ready'` — comes
        // with it. Reading through the narrower write trait would have meant
        // restating that predicate here, one layer away from the queries that
        // own it.
        CatalogReadSide::Postgres => dual.reading_from_central(Arc::clone(&central.reads)),
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
/// The three faces `build_snapshot_backend` needs from the central catalog,
/// bundled once so the choice of *which* concrete catalog answers them is
/// made in exactly one place — [`build_central_catalog`] — rather than at
/// every call site that would otherwise have needed its own
/// `Arc<CentralSnapshotCatalog>` vs. `Arc<PostgresSnapshotCatalog>` branch.
#[derive(Clone)]
pub struct CentralCatalogHandle {
    /// The double write's own narrow surface — see [`CentralCatalogWrites`].
    pub writes: Arc<dyn CentralCatalogWrites>,
    /// The unbounded "every id, any status" listing the read-side switch's
    /// population comparison needs — see [`CatalogCensus`].
    pub census: Arc<dyn CatalogCensus>,
    /// The ordinary `SnapshotCatalog` surface, for `write = "postgres"`
    /// (the sole catalog) and for `reading_from_central` under
    /// `write = "both", read = "postgres"`.
    pub reads: Arc<dyn SnapshotCatalog>,
}

/// What a process holding a `[pg]` pool hands [`build_snapshot_backend`].
///
/// # 🔴 Parts, not a pool
///
/// Both halves of the split call [`build_snapshot_backend`], and only one of
/// them is allowed to hold database credentials at all (`src/pg/mod.rs`'s own
/// module doc). Everything this assembly needs from PostgreSQL is these two
/// values — the central catalog's three faces, and the shared read-side
/// confirmation record — so they arrive already built, from the half that
/// owns the pool, rather than as the pool itself.
///
/// Build one with
/// [`pg_catalog_parts`][postgres::pg_catalog_parts].
pub struct PgCatalogParts {
    /// The `PostgresSnapshotCatalog`, in the three faces this assembly uses.
    pub central: CentralCatalogHandle,
    /// Stage B step 4's shared confirmation record, in the same database.
    pub read_side_confirmation: Arc<dyn ReadSideConfirmationStore>,
}

/// The central catalog client, when the configuration asks for one.
///
/// 🔴 A missing scheduler endpoint is a startup failure rather than a silent
/// fall back to writing one store. The operator asked for a second copy, and a
/// node that quietly kept only the first would only reveal the difference when
/// somebody tried to read the second.
///
/// 🔴 Whenever a `[pg]` pool is available, this picks `PostgresSnapshotCatalog`
/// over the gRPC hop to `services/scheduler` — a direct, in-process connection
/// to the same database `services/scheduler` itself would have written,
/// without a network hop or a second process. This is what lets a `--role api`
/// (or `--role all`) replica with `[pg]` configured serve `write = "both"`
/// without ever dialing a scheduler, and it is the *only* way `write =
/// "postgres"` is servable at all — that mode has no gRPC form, because there
/// is nothing left to fall back to once the central catalog is the only copy.
pub fn build_central_catalog(
    config: &AppConfig,
    pg: Option<&CentralCatalogHandle>,
) -> Result<Option<CentralCatalogHandle>> {
    match config.snapshot.catalog.write {
        SnapshotCatalogWrite::ObjectStore => return Ok(None),
        SnapshotCatalogWrite::Both | SnapshotCatalogWrite::Postgres => {}
    }

    if let Some(handle) = pg {
        return Ok(Some(handle.clone()));
    }

    if config.snapshot.catalog.write == SnapshotCatalogWrite::Postgres {
        anyhow::bail!(
            "snapshot.catalog.write = \"postgres\" requires [pg] to be configured; there is no \
             gRPC scheduler fallback for this mode"
        );
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

    let catalog = Arc::new(CentralSnapshotCatalog::connect_hot_reloadable(
        endpoint,
        &config.cluster,
        &config.observability.scheduler_report,
        identity.cluster_id,
        identity.id,
    )?);
    Ok(Some(CentralCatalogHandle {
        writes: Arc::clone(&catalog) as Arc<dyn CentralCatalogWrites>,
        census: Arc::clone(&catalog) as Arc<dyn CatalogCensus>,
        reads: catalog as Arc<dyn SnapshotCatalog>,
    }))
}

/// Assembles the `write = "postgres"` backend: no mirror, no backlog, no
/// object-store catalog — see `build_snapshot_backend`'s own call site for
/// why none of that machinery is needed in this mode. Bytes still flow
/// through whichever artifact store `repository_backend` configured; only
/// the row half is replaced.
///
/// Pulled out of `build_snapshot_backend` because that function reads
/// `ConfigManager::global_config()` directly and so cannot be driven by a
/// unit test with an arbitrary `[snapshot.catalog]`; this half has no such
/// dependency.
fn assemble_postgres_only_backend(
    central_reads: Arc<dyn SnapshotCatalog>,
    artifacts: Arc<dyn SnapshotArtifactStore>,
    runtime_resolver: Option<Arc<dyn SnapshotRuntimeResolver>>,
    node_id: String,
) -> AssembledSnapshotBackend {
    AssembledSnapshotBackend {
        repository: Arc::new(SnapshotRepository::on_node(
            central_reads,
            artifacts,
            node_id,
        )),
        runtime_resolver,
        mirror_compensator: None,
    }
}

/// The two storage halves an assembly hands [`build_snapshot_backend`]: the
/// durable repository, and a runtime resolver only for a process that has
/// somewhere to run a sandbox.
///
/// 🔴 Built by the caller, not here. `aenv-node` builds both halves
/// ([`build_node_storage`][storage::build_node_storage]); `aenv-api` builds
/// the first and passes `None` for the second
/// ([`build_catalog_only_storage`]), because resolving a snapshot is not a
/// lookup — it downloads `vm_state.bin` onto local disk, materializes the
/// memory and rootfs overlaybd `image.json` files and leases all of it in a
/// node-local artifact cache. That machinery, and the overlaybd layer store
/// it drags in, is not linked into the api binary at all.
pub type RoleStorage = (
    Arc<SnapshotRepository>,
    Option<Arc<dyn SnapshotRuntimeResolver>>,
);

/// The durable repository — rows and byte *lifecycle* — with nothing that
/// turns bytes into something a VM can mmap.
///
/// Both backends already had the seam: POSIX's is
/// [`posixfs_catalog_only_repository`], two stores rooted at one directory,
/// and the OSS one is
/// [`oss_durable_parts`][oss::oss_durable_parts]. What each of them *doesn't*
/// build is the resolver, which is the only consumer of the overlaybd layer
/// store, the shared artifact cache, and the runtime cache root.
///
/// 🔴 Delete stays on this side on purpose, and that is why this arm still
/// gets a real artifact store rather than a stub. A snapshot's origin node can
/// be gone — hard death, or simply rolled — and a delete that had to be
/// dispatched there would leave the row removed, the bytes orphaned, and
/// nobody holding a record of either.
pub fn build_catalog_only_storage(config: &AppConfig) -> Result<RoleStorage> {
    Ok((build_catalog_only_repository(config)?, None))
}

fn build_catalog_only_repository(config: &AppConfig) -> Result<Arc<SnapshotRepository>> {
    match config.snapshot.repository_backend {
        SnapshotRepositoryBackendKind::PosixFs => {
            let root = config
                .backend
                .posix_fs
                .as_ref()
                .context("backend.posix_fs config is required when repository_backend = posix_fs")?
                .snapshot_store
                .join("repository");
            Ok(Arc::new(posixfs_catalog_only_repository(&root)))
        }
        SnapshotRepositoryBackendKind::Oss => {
            let oss_config = config
                .backend
                .oss
                .as_ref()
                .context("backend.oss config is required when repository_backend = oss")?;
            Ok(
                oss::oss_durable_parts(oss_config, snapshot_image_storage_policy(config))?
                    .into_repository(),
            )
        }
    }
}

/// Which storage the source-registry publication policy names, for both
/// halves. Read in one place so the two constructors cannot drift.
pub fn snapshot_image_storage_policy(config: &AppConfig) -> SnapshotImageStoragePolicy {
    if config.snapshot.image_publish.enabled {
        SnapshotImageStoragePolicy::SourceRegistry
    } else {
        SnapshotImageStoragePolicy::ObjectStorage
    }
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

    /// `write = "postgres"` wiring, without touching `ConfigManager::global_config()`
    /// or a real database — `assemble_postgres_only_backend` is pure
    /// composition, so a `ScriptedCatalog` standing in for `central.reads`
    /// is enough to prove the two things that matter: no compensator is
    /// spawned (there is no mirror to run in this mode), and the assembled
    /// repository actually reads and writes through the catalog it was
    /// given rather than silently falling back to some other store.
    #[tokio::test]
    async fn postgres_only_backend_has_no_compensator_and_reads_its_own_catalog() {
        let central_reads = Arc::new(ScriptedCatalog::default());
        let record = record_for(&SnapshotId::generate());
        central_reads.seed(record.clone());

        let assembled = assemble_postgres_only_backend(
            Arc::clone(&central_reads) as Arc<dyn SnapshotCatalog>,
            Arc::new(crate::snapshot::mock::MockSnapshotArtifactStore),
            Some(Arc::new(crate::snapshot::mock::MockSnapshotRuntimeResolver)),
            "test-node".to_string(),
        );

        assert!(
            assembled.mirror_compensator.is_none(),
            "write = \"postgres\" has nothing for a compensator to reconcile"
        );

        let found = assembled
            .repository
            .catalog()
            .get(&record.id.to_string())
            .await
            .expect("read should succeed")
            .expect("the record seeded into central_reads must be visible through the assembled repository");
        assert_eq!(found.id, record.id);
    }
}
