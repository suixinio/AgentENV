pub mod central;
pub(crate) mod common;
pub(crate) mod oss;
pub(crate) mod posixfs;
pub(crate) mod postgres;

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
pub use oss::OssBackend;
use posixfs::posixfs_catalog_only_repository;
pub use posixfs::{PosixFsBackend, PosixFsBackendConfig};
use postgres::migration_state::PgReadSideConfirmation;
use postgres::PostgresSnapshotCatalog;

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
    p2p_transport: Option<Arc<dyn P2pTransport>>,
    // 🔴 `None` for `--role node` always — see
    // `src/bin/aenv-api.rs::build_pg_pool`. Consumed by the read-side
    // admission's shared confirmation (Stage B step 4,
    // `docs/proposals/_sd-phase4-stageB-catalog.md` §7/§5.1) and, when
    // present, by `build_central_catalog`, which then builds a
    // `PostgresSnapshotCatalog` in place of the gRPC hop to
    // `services/scheduler` for both `write = "both"` and
    // `write = "postgres"`.
    pg_pool: Option<sqlx::PgPool>,
    role: crate::role::ServerRole,
) -> Result<AssembledSnapshotBackend> {
    let config = ConfigManager::global_config();
    let (repository, runtime_resolver) = build_storage_for_role(config, p2p_transport, role)?;

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

    let Some(central) = build_central_catalog(config, pg_pool.as_ref())? else {
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
        let node_identity = crate::identity::NodeIdentity::from_config(&config.node_identity);
        let pool = pg_pool.as_ref().ok_or_else(|| {
            anyhow::anyhow!(
                "unreachable: build_central_catalog only returns Some for write = \"postgres\" \
                 when [pg] is configured"
            )
        })?;
        let confirmation =
            PgReadSideConfirmation::new(pool, node_identity.cluster_id, &node_identity.id);
        require_read_side_confirmed(&confirmation).await?;

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
    let node_identity = crate::identity::NodeIdentity::from_config(&config.node_identity);
    let shared_confirmation = pg_pool
        .as_ref()
        .map(|pool| PgReadSideConfirmation::new(pool, node_identity.cluster_id, &node_identity.id));
    let populations = admit_read_side_with_confirmation(
        configured_read,
        &backlog,
        &targets,
        &ObjectStoreCensus(object_store_catalog.as_ref()),
        central.census.as_ref(),
        shared_confirmation
            .as_ref()
            .map(|store| store as &dyn ReadSideConfirmationStore),
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
struct CentralCatalogHandle {
    /// The double write's own narrow surface — see [`CentralCatalogWrites`].
    writes: Arc<dyn CentralCatalogWrites>,
    /// The unbounded "every id, any status" listing the read-side switch's
    /// population comparison needs — see [`CatalogCensus`].
    census: Arc<dyn CatalogCensus>,
    /// The ordinary `SnapshotCatalog` surface, for `write = "postgres"`
    /// (the sole catalog) and for `reading_from_central` under
    /// `write = "both", read = "postgres"`.
    reads: Arc<dyn SnapshotCatalog>,
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
fn build_central_catalog(
    config: &AppConfig,
    pg_pool: Option<&sqlx::PgPool>,
) -> Result<Option<CentralCatalogHandle>> {
    match config.snapshot.catalog.write {
        SnapshotCatalogWrite::ObjectStore => return Ok(None),
        SnapshotCatalogWrite::Both | SnapshotCatalogWrite::Postgres => {}
    }

    if let Some(pool) = pg_pool {
        let identity = crate::identity::NodeIdentity::from_config(&config.node_identity);
        let catalog = Arc::new(
            PostgresSnapshotCatalog::new(pool.clone(), identity.cluster_id, identity.id)
                .with_max_concurrent_builds(config.snapshot.catalog.max_concurrent_builds),
        );
        if config.snapshot.catalog.max_concurrent_builds < 0 {
            // 🔴 Legal, and deliberate (see `SnapshotCatalogConfig::max_concurrent_builds`'s
            // own doc for why negative rather than zero means this) — still
            // worth a line at startup, matching
            // `services/scheduler/cmd/main.go::announceBuildQueue`'s own
            // warning: with no ceiling the only thing bounding concurrent
            // builds is how many VMs the fleet can boot, and the first
            // symptom is nodes running out of memory rather than a refusal
            // anybody can read.
            tracing::warn!(
                target: "agentenv",
                max_concurrent_builds = config.snapshot.catalog.max_concurrent_builds,
                "snapshot catalog build queue has no cluster-wide ceiling: nothing but the \
                 fleet's capacity limits how many builds run at once"
            );
        }
        return Ok(Some(CentralCatalogHandle {
            writes: Arc::clone(&catalog) as Arc<dyn CentralCatalogWrites>,
            census: Arc::clone(&catalog) as Arc<dyn CatalogCensus>,
            reads: catalog as Arc<dyn SnapshotCatalog>,
        }));
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

/// Starts the catalog build reaper for this process, when `pool` is `Some`
/// (i.e. `[pg]` is configured) — a thin `pub` bridge so `src/bin/aenv-api.rs`
/// (a separate crate from this library) can reach
/// `postgres::reaper::spawn`, which stays `pub(crate)` like the rest of that
/// module. `None` (no interval/ttl configured, or no pool at all) means
/// nothing was started, matching [`postgres::reaper::spawn`]'s own `None`
/// case.
///
/// 🔴 The returned handle must be shut down through
/// `agentenv::pg::SingletonTaskHandle::shutdown()`, never pushed into a
/// `Vec<tokio::task::JoinHandle<()>>` and `.abort()`-ed — see that type's
/// own documentation on why: this replica's PostgreSQL advisory lock, if it
/// is currently leader, would otherwise leak until the pool itself is torn
/// down.
pub fn spawn_catalog_build_reaper(
    pool: Option<sqlx::PgPool>,
    cluster_id: uuid::Uuid,
    interval: std::time::Duration,
    ttl: std::time::Duration,
) -> Option<crate::pg::SingletonTaskHandle> {
    postgres::reaper::spawn(pool?, cluster_id, interval, ttl)
}

/// Brings the catalog schema in `pool`'s database to the shape this build
/// expects. A thin `pub` bridge to `postgres::migrate::migrate`, which stays
/// `pub(crate)` like the rest of that module — see
/// [`spawn_catalog_build_reaper`]'s own doc for why `src/bin/aenv-api.rs` (a
/// separate crate from this library) needs one of these per function it
/// calls into `postgres::`.
///
/// 🔴 Must run to completion before anything else touches the `snapshots` /
/// `aliases` / `builds` tables through this pool — the catalog build reaper
/// (`spawn_catalog_build_reaper`) and `build_snapshot_backend`'s
/// `PostgresSnapshotCatalog` construction both assume the schema already
/// exists and neither one migrates it itself (see `PostgresSnapshotCatalog`'s
/// own module doc). Idempotent and safe to call on every start — the
/// migration runner's own session-scoped advisory lock (`GO_SCHEMA_LOCK_KEY`)
/// is what lets a fleet of `--role api` replicas call this concurrently
/// without racing each other.
pub async fn migrate_catalog_schema(pool: &sqlx::PgPool) -> Result<()> {
    postgres::migrate::migrate(pool).await
}

/// What [`build_storage_for_role`] hands back: the durable repository, and a
/// runtime resolver only for a role that has somewhere to run a sandbox.
type RoleStorage = (
    Arc<SnapshotRepository>,
    Option<Arc<dyn SnapshotRuntimeResolver>>,
);

/// The storage halves this role actually needs.
///
/// 🔴 The single gate between `--role api` and the byte half. `--role api`
/// keeps a repository — it creates template rows, publishes commits staged on
/// a node, lists, and deletes — but it never materializes a snapshot onto
/// local disk, so it gets no [`SnapshotRuntimeResolver`] and, with it, none of
/// the machinery a resolver drags in: the process-wide overlaybd layer store,
/// the node-local artifact cache, and the runtime cache root they write into.
///
/// The early return below is what makes [`build_storage_backend`] unreachable
/// on that role. It is a runtime gate rather than a compile-time one: the byte
/// half is reached through the same crate, so nothing but this branch stops it
/// today. `only_a_role_that_runs_sandboxes_builds_the_byte_half` is the
/// assertion that keeps it the *only* branch; the compile-time version of this
/// property is the later step that moves the byte half behind its own crate
/// boundary.
///
/// 🔴 Delete stays on this side of the gate on purpose, and that is why the
/// api arm still gets a real artifact store rather than a stub. A snapshot's
/// origin node can be gone — hard death, or simply rolled — and a delete that
/// had to be dispatched there would leave the row removed, the bytes orphaned,
/// and nobody holding a record of either. See
/// [`OssBackend::durable_parts`][oss::OssBackend::durable_parts] for the
/// matching split on the OSS backend, whose byte deletion is a pure network
/// operation and never needed overlaybd at all.
fn build_storage_for_role(
    config: &AppConfig,
    p2p_transport: Option<Arc<dyn P2pTransport>>,
    role: crate::role::ServerRole,
) -> Result<RoleStorage> {
    if !role.runs_sandbox_runtime() {
        return Ok((build_catalog_only_repository(config)?, None));
    }

    let (repository, runtime_resolver) = build_storage_backend(config, p2p_transport)?;
    Ok((repository, Some(runtime_resolver)))
}

/// The durable repository — rows and byte *lifecycle* — with nothing that
/// turns bytes into something a VM can mmap.
///
/// Both backends already had the seam: POSIX's repository is
/// [`posixfs_repository`], two stores rooted at one directory, and the OSS one
/// is [`OssBackend::durable_parts`][oss::OssBackend::durable_parts]. What
/// each of them *doesn't* build is the resolver, which is the only consumer of
/// the overlaybd layer store, the shared artifact cache, and the runtime cache
/// root.
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
            let snapshot_image_storage = if config.snapshot.image_publish.enabled {
                SnapshotImageStoragePolicy::SourceRegistry
            } else {
                SnapshotImageStoragePolicy::ObjectStorage
            };
            Ok(OssBackend::durable_parts(oss_config, snapshot_image_storage)?.into_repository())
        }
    }
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
    use crate::snapshot::RepositoryError;

    use crate::snapshot::repository::interfaces::CatalogReadScope;

    /// 🔴 The step's own acceptance criterion: `--role api` assembles the byte
    /// half's *lifecycle* and none of its *materialization*.
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
    /// 3. `publish` through the api-assembled repository is refused rather
    ///    than silently doing nothing. `--role api` never stages: every capture
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

        let (repository, runtime_resolver) =
            build_storage_for_role(&config, None, crate::role::ServerRole::Api)
                .expect("the api role should assemble a storage backend");

        assert!(
            runtime_resolver.is_none(),
            "--role api built a snapshot runtime resolver; it resolves nothing and must hold \
             none of what a resolver drags in"
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
        node_repository
            .publish(metadata.clone(), manifest.clone())
            .await
            .expect("the node-assembled repository stages the bytes");

        // And this half refuses to, rather than pretending it can.
        let refusal = repository
            .publish(metadata, manifest)
            .await
            .expect_err("--role api must refuse to import snapshot artifacts");
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
            "the row survived a delete on --role api"
        );
        assert!(
            !committed_dir.exists(),
            "the bytes survived a delete on --role api — this is the orphan the whole \
             api-side delete exists to prevent"
        );
    }

    /// 🔴 The byte half must not be on `--role api`'s call graph.
    ///
    /// [`build_storage_for_role`]'s early return is the only thing that makes
    /// that true, and a unit test cannot prove it by running the other arm:
    /// the byte half opens the process-wide image-cache RocksDB rooted at
    /// `home_path`, which a unit test has no business creating. So this reads
    /// this file's own source text, the same way `src/bin/aenv-api.rs`'s
    /// `only_the_split_roles_bind_a_second_listener` does, and asserts three
    /// things at once:
    ///
    /// * there is exactly **one** call site in this file (plus the `fn`
    ///   definition). A second one — for instance putting the call back at the
    ///   top of `build_snapshot_backend`, where it was before this step —
    ///   fails here even though [`build_storage_for_role`] itself would still
    ///   look correct;
    /// * that call site is inside [`build_storage_for_role`]'s body; and
    /// * inside that body, the role gate opens and returns *before* it.
    ///
    /// The count assertion is also what stops the opposite mutation: a gate
    /// that returned `None` for every role would leave zero call sites.
    ///
    /// Matching is on the call expression — the name followed by an opening
    /// parenthesis — rather than on the bare identifier, because this file's
    /// prose names the function in several comments and a bare-identifier
    /// match would count every one of them. The needle is assembled from two
    /// halves so this test's own source does not match itself; a comment that
    /// did spell the call expression would over-count and fail here, which is
    /// a false alarm and never a false pass.
    #[test]
    fn only_a_role_that_runs_sandboxes_builds_the_byte_half() {
        let source = include_str!("mod.rs");

        let body_range = |name: &str| -> std::ops::Range<usize> {
            let start = source
                .find(name)
                .unwrap_or_else(|| panic!("{name} is no longer in this file"));
            let open = source[start..].find('{').expect("a body") + start;
            let mut depth = 0usize;
            for (offset, byte) in source[open..].bytes().enumerate() {
                match byte {
                    b'{' => depth += 1,
                    b'}' => {
                        depth -= 1;
                        if depth == 0 {
                            return open..open + offset;
                        }
                    }
                    _ => {}
                }
            }
            panic!("{name} has no closing brace");
        };

        // 🔴 Spelled in halves so this test's own source text does not count
        // as a call site — it is `include_str!`-ing the file it lives in.
        let needle = concat!("build_storage_", "backend(");
        let definition = source
            .find(&format!("fn {needle}"))
            .expect("the byte half's definition is no longer in this file")
            + "fn ".len();
        let call_sites: Vec<usize> = source
            .match_indices(needle)
            .map(|(at, _)| at)
            .filter(|at| *at != definition)
            .collect();

        assert_eq!(
            call_sites.len(),
            1,
            "the byte half has {} call sites in this file; it must have exactly one, inside \
             build_storage_for_role, or --role api can reach it again",
            call_sites.len()
        );
        let call = call_sites[0];

        let gate = body_range("fn build_storage_for_role(");
        assert!(
            gate.contains(&call),
            "the call to the byte half has moved out of build_storage_for_role, so nothing \
             gates it on the role any more"
        );

        let before = &source[gate.start..call];
        let opened = before.find("if !role.runs_sandbox_runtime() {").expect(
            "build_storage_for_role no longer refuses the byte half for a role that runs no \
             sandbox runtime",
        );
        assert!(
            before[opened..].contains("return "),
            "the role gate no longer returns early, so the byte half is built on every role"
        );
    }

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

/// `build_central_catalog`'s own pg-vs-grpc choice, against a real database.
/// Named `pg::` (not folded into `mod tests` above) so `make test-with-postgres`
/// — a name filter, not a feature check — actually selects it; see Stage B's
/// own proposal doc §7 step 12 on this exact trap.
#[cfg(test)]
mod pg {
    use super::*;
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

        let handle = build_central_catalog(&config, Some(&pool)).expect("should not error");
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

        let handle = build_central_catalog(&config, Some(&pool))
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

        let handle = build_central_catalog(&config, Some(&pool))
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
    /// has to reach the `PostgresSnapshotCatalog` `build_central_catalog`
    /// constructs, not just the test-only constructor in `postgres::mod::pg`.
    /// A ceiling of `1` set on the `AppConfig` passed in here must refuse a
    /// second *different* template's build the same way
    /// `the_cluster_wide_build_ceiling_refuses_once_it_is_reached` proves the
    /// underlying store does — this test is the only one that goes through
    /// `build_central_catalog` itself to get there, so a regression that
    /// stops the config value from being read (for instance, `build_central_catalog`
    /// going back to `PostgresSnapshotCatalog::new` without the
    /// `with_max_concurrent_builds` call) fails only here.
    #[tokio::test]
    async fn max_concurrent_builds_from_config_reaches_admission() {
        let pool =
            isolated_schema_pool_or_skip!("max_concurrent_builds_from_config_reaches_admission");
        migrate(&pool).await.expect("migration should succeed");

        let mut config = AppConfig::default();
        config.snapshot.catalog.write = SnapshotCatalogWrite::Both;
        config.snapshot.catalog.max_concurrent_builds = 1;

        let handle = build_central_catalog(&config, Some(&pool))
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
