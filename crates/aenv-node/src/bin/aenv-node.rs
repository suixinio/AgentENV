#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// jemalloc tuning: purge dirty/muzzy pages after 1s instead of the default
// 10s, and do the purging on a background thread (the `background_threads`
// cargo feature is already enabled). Burst allocations (RocksDB opens, image
// resolution, template builds) otherwise linger as retained RSS long after
// the burst is over.
#[used]
#[allow(non_upper_case_globals)]
#[export_name = "malloc_conf"]
pub static malloc_conf: &[u8] = b"dirty_decay_ms:1000,muzzy_decay_ms:1000,background_thread:true\0";
use std::sync::{Arc, RwLock};
use std::time::Duration;

use aenv_node::api::{server, ApiImpl, PausedSandboxWiring, ResumeWiring};
use aenv_node::cfg::{AppConfig, PausedRegistryBackendKind};
use aenv_node::identity::NodeIdentity;
use aenv_node::image::ImageResolver;
use aenv_node::observability::{ObservabilityReporter, ObservabilityService};
use aenv_node::orchestrator::{
    DisabledPausedSandboxRegistry, FileBackedSandboxPersister, InMemoryMetadataStore, Orchestrator,
    SandboxOrchestration,
};
use aenv_node::overlaybd::OverlaybdP2pRuntime;
use aenv_node::p2p::P2pTransport;
use aenv_node::sandbox::{FirecrackerPool, FirecrackerSandboxFactory, UblkDeviceManager};
use aenv_node::server_main::{self, spawn_grpc_surface, Assembly, ProcessRuntime};
use aenv_node::snapshot::SnapshotManager;
use aenv_node::template::TemplateBuilder;
use anyhow::Context as _;
use clap::Parser;
use tracing::{info, warn};

/// The orchestrator this binary assembles: this node's own ledger, this node's
/// Firecracker, this node's files.
type LocalOrchestrator =
    Orchestrator<InMemoryMetadataStore, FirecrackerSandboxFactory, FileBackedSandboxPersister>;

/// 🔴 No `--role`. This binary *is* the node half, because it is the one that
/// links a sandbox runtime; `aenv-api` is the other. There was a `--role` flag
/// (and an `AENV_ROLE` environment variable) through the transition, accepted
/// and checked rather than obeyed, so that manifests written for the
/// single-process image kept starting. Nothing passes it any more — see
/// `deploy/k8s/base/` — and a flag that can only be confirmed is a flag that
/// cannot select anything.
#[derive(Debug, Parser)]
#[command(name = "aenv-node")]
struct NodeCli {
    /// Run setup/provisioning only, then exit.
    #[arg(long)]
    setup_only: bool,

    /// Provision machine-wide KVM, ublk, and networking prerequisites.
    #[arg(long, conflicts_with = "setup_only")]
    setup_host: bool,

    /// Account that will run AENV after host provisioning.
    #[arg(long, default_value = "aenv", requires = "setup_host")]
    runtime_user: String,

    /// Runtime service group; owns AENV state and receives ublk device access.
    #[arg(long, default_value = "aenv", requires = "setup_host")]
    runtime_group: String,

    /// Path to config file (same as AENV_CONFIG_PATH).
    #[arg(long)]
    config: Option<std::path::PathBuf>,
}

/// Bound for each individual step in [`NodeRuntime::shutdown`].
///
/// 🔴 Deliberately smaller than [`server_main::RUNTIME_SHUTDOWN_TIMEOUT`],
/// which is the backstop for whatever this file did *not* reach explicitly —
/// by the time that one runs, every step below has already had its own bound.
///
/// 🔴 Shared across four unrelated subsystems (two P2P shutdowns, two RocksDB
/// store closes) on purpose: an operator reading shutdown logs across a fleet
/// only has to remember one number, and none of these four steps has ever had
/// a reason to need a materially different bound from the others — they are
/// all "stop background work that is already best-effort, and say so if it
/// didn't finish in time" calls. `crate::local_store::DEFAULT_CLOSE_TIMEOUT`
/// is the same value for the same reason, one module over; this one is
/// separate because it also has to bound the two P2P calls, which know
/// nothing about `local_store`.
const NODE_RUNTIME_SHUTDOWN_STEP_TIMEOUT: Duration = Duration::from_secs(15);

/// The machine-local runtime this binary owns: the two P2P pieces it
/// holds by value, the process-wide Firecracker pool and ublk daemon it
/// reaches through their globals.
///
/// 🔴 Held as a whole rather than as independent handles so that the teardown
/// order — pool, ublk, overlaybd P2P, transport, then the RocksDB store this
/// bundle can reach — stays in one place.
///
/// 🔴 It used to also hold the snapshot manager, for one call: closing the
/// double write's durable mirror backlog. That store is gone with the double
/// write — the snapshot catalog is PostgreSQL, which owns no node-local
/// RocksDB — so the handle and the call went with it rather than being kept as
/// a no-op somebody's shutdown guard would go on asserting.
struct NodeRuntime {
    overlaybd_p2p: OverlaybdP2pRuntime,
    p2p_transport: Arc<dyn P2pTransport>,
}

#[async_trait::async_trait]
impl ProcessRuntime for NodeRuntime {
    async fn shutdown(self: Box<Self>) {
        if let Some(pool) = FirecrackerPool::global() {
            info!(target: "agentenv", "shutting down firecracker pool");
            if let Err(err) = pool.shutdown().await {
                warn!(target: "agentenv", error = %err, "error occurred while shutting down firecracker pool");
            }
        }
        info!(target: "agentenv", "shutting down ublk daemon");
        if let Err(err) = UblkDeviceManager::global().shutdown_daemon().await {
            warn!(target: "agentenv", error = %err, "error occurred while shutting down ublk daemon");
        }

        // 🔴 Bounded, unlike the plain `.await`s these replaced. Both
        // subsystems already treat their own failure as best-effort (`warn!`
        // and move on) — but with P2P enabled, `overlaybd_p2p`'s read facade
        // and `p2p_transport`'s iroh endpoint each sit on top of a downstream
        // RPC wait with no timeout of its own (iroh-blobs' storage actor, in
        // particular), and an unbounded `.await` here is exactly the class of
        // bug the rest of this shutdown path exists to close off. Disabled P2P
        // (`DisabledP2pTransport`, most deployments today) returns instantly
        // either way, so this only changes behaviour where P2P is on.
        info!(target: "agentenv", "shutting down overlaybd p2p runtime");
        match tokio::time::timeout(
            NODE_RUNTIME_SHUTDOWN_STEP_TIMEOUT,
            self.overlaybd_p2p.shutdown(),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                warn!(target: "agentenv", error = %err, "error occurred while shutting down overlaybd p2p runtime");
            }
            Err(_) => {
                warn!(
                    target: "agentenv",
                    timeout_secs = NODE_RUNTIME_SHUTDOWN_STEP_TIMEOUT.as_secs(),
                    "overlaybd p2p runtime did not shut down within timeout; continuing shutdown"
                );
            }
        }
        info!(target: "agentenv", "shutting down p2p transport");
        match tokio::time::timeout(
            NODE_RUNTIME_SHUTDOWN_STEP_TIMEOUT,
            self.p2p_transport.shutdown(),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                warn!(target: "agentenv", error = %err, "error occurred while shutting down p2p transport");
            }
            Err(_) => {
                warn!(
                    target: "agentenv",
                    timeout_secs = NODE_RUNTIME_SHUTDOWN_STEP_TIMEOUT.as_secs(),
                    "p2p transport did not shut down within timeout; continuing shutdown"
                );
            }
        }

        // 🔴 The RocksDB store this bundle can still reach. The call does not
        // drop the underlying store — it only stops its background
        // compaction/flush ahead of time (see `LocalKvStore::close`) — so this
        // is safe even while other clones of the same store (the image cache's
        // shared-instance registry, in particular) are still live elsewhere in
        // the process.
        info!(target: "agentenv", "closing image cache metadata store");
        aenv_node::image::close_image_cache_stores(NODE_RUNTIME_SHUTDOWN_STEP_TIMEOUT).await;
    }
}

/// Everything this binary builds before the question of who owns a paused
/// sandbox comes up.
struct NodeCore {
    orchestrator: Arc<LocalOrchestrator>,
    snapshot_manager: Arc<SnapshotManager>,
    template_builder: Arc<TemplateBuilder>,
    image_resolver: Arc<ImageResolver>,
    observability: Option<Arc<ObservabilityService>>,
    reporter: Option<ObservabilityReporter>,
    /// The identity the paused-registry wiring needs after the observability
    /// service has taken ownership of the original.
    identity: NodeIdentity,
    runtime: NodeRuntime,
}

/// 🔴 Not `#[tokio::main]`. That macro's generated `main` builds the runtime,
/// `block_on`s the async body, then lets the `Runtime` value fall out of
/// scope — and `Runtime`'s `Drop` shuts down its blocking-task pool by calling
/// `BlockingPool::shutdown(None)` (tokio, `runtime/blocking/pool.rs`), and
/// `None` means *no timeout*: it waits forever for every `spawn_blocking`
/// closure that has already started to return.
///
/// Every RocksDB store this process opens (`LocalKvStore`, see
/// `aenv_node::local_store`) does its writes, and its own background
/// compaction/flush, through `spawn_blocking` — so one such closure still
/// running when the async body below returns was enough to make the whole
/// process hang past `terminationGracePeriodSeconds`, long after every log
/// line the graceful shutdown was ever going to print had already printed.
/// (Observed only on nodes that had actually run a VM: an idle node's stores
/// have nothing to compact, so `Drop` there really did return immediately —
/// which is exactly why the hang looked selective rather than universal.)
///
/// The explicit `close()` calls [`NodeRuntime::shutdown`] makes — the image
/// cache metadata store and the snapshot catalog mirror backlog — are meant to
/// make that background work finish, and log whether it did, before any of
/// this runs. [`server_main::RUNTIME_SHUTDOWN_TIMEOUT`] is the backstop for
/// whatever is still outstanding regardless: unlike plain `Drop`, it bounds
/// the same wait, and once it returns, `main` returning ends the process.
///
/// 🔴 This binary is the one that runs VMs, so it is the one the hang was ever
/// observed on — which is why `the_shutdown_bounds_are_still_wired` lives at
/// the bottom of *this* file and not in the shared serve loop.
fn main() -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("build the tokio runtime")?;
    let result = runtime.block_on(async_main());
    runtime.shutdown_timeout(server_main::RUNTIME_SHUTDOWN_TIMEOUT);
    result
}

async fn async_main() -> anyhow::Result<()> {
    aenv_node::logging::init();
    agentenv_observability::init_prometheus_recorder()?;

    // 🔴 Before the configuration is read, because what this refuses is a
    // *manifest* that has not been migrated rather than a value in a file.
    // confique silently ignores an environment variable no field declares, so
    // an un-migrated workload would otherwise start looking perfectly healthy
    // while its operator believes the snapshot catalog is somewhere it is not.
    // Same decision `--role`/`AENV_ROLE` got when one binary became two.
    aenv_node::cfg::refuse_removed_catalog_env_vars()?;

    let cli = NodeCli::parse();
    let config_manager = if let Some(config_path) = cli.config.as_deref() {
        aenv_node::cfg::ConfigManager::init_global_from_path(config_path)?
    } else {
        aenv_node::cfg::ConfigManager::init_global()?
    };
    let config = config_manager.config();

    // 🔴 Security invariant, and a belt over a brace. This binary cannot link
    // `sqlx` at all — `cargo tree -p aenv-node -e normal | grep sqlx` is empty,
    // which is a stronger statement than any check could make. What this still
    // catches is *configuration*: an operator who leaves `[pg].dsn` in a node's
    // ConfigMap has put a database credential on a machine that runs user code,
    // whether or not anything in the process could use it.
    refuse_configured_pg_dsn(config.pg.as_ref().and_then(aenv_node::cfg::PgConfig::dsn))?;

    if cli.setup_only {
        aenv_node::setup::ensure_provisioning(config).await?;
        info!(target: "agentenv", "dependency setup complete (setup-only mode)");
        return Ok(());
    }

    if cli.setup_host {
        aenv_node::setup::ensure_host(config, &cli.runtime_user, &cli.runtime_group)?;
        info!(target: "agentenv", "host setup complete");
        return Ok(());
    }

    info!(target: "agentenv", "assembling the node half");
    let assembly = assemble_node(config).await?;
    server_main::serve(config, assembly).await
}

/// Refuses a node that has been handed a PostgreSQL DSN.
///
/// `dsn` is [`aenv_node::cfg::PgConfig::dsn`]'s output — already trimmed,
/// already `None` for blank — so this only ever sees a value here when one is
/// genuinely configured.
///
/// A hard startup failure rather than a warning, for the same reason the
/// snapshot catalog only ever runs against PostgreSQL from `aenv-api`'s
/// `PostgresSnapshotCatalog` and `PausedRegistryBackendKind::Postgres`
/// (`src/cfg.rs`) already enforces for the paused registry: database
/// credentials, the connection budget and the schema are the deciding half's
/// business, never the machines that run user code.
///
/// 🔴 Not made redundant by the crate split, which is why it outlived the role
/// enum's `check_pg_dsn` it was lifted from. The dependency graph proves this
/// binary *cannot use* a DSN; it says nothing about one being *present* on the
/// machine. A
/// `[pg].dsn` left in a node's ConfigMap would otherwise sit there being
/// ignored — a database credential on a host that runs user code, and an
/// operator who believes the node is configured.
fn refuse_configured_pg_dsn(dsn: Option<&str>) -> anyhow::Result<()> {
    if dsn.is_none() {
        return Ok(());
    }
    anyhow::bail!(
        "aenv-node must not be configured with [pg].dsn: database credentials, the connection \
         budget and the schema belong to the deciding half (aenv-api), never to a machine that \
         runs user code. This binary cannot even link a PostgreSQL client, so the setting does \
         nothing here but leave a credential on a host that runs user code. Remove [pg] from \
         this node's configuration, or from whatever file AENV_CONFIG_OVERLAY_PATH names for it"
    )
}

/// Brings up everything this machine needs to run sandboxes.
///
/// 🔴 Took a `role` and a `pg_pool` while one binary was three roles. Both are
/// gone: this binary is one half, and it holds no PostgreSQL pool — which is
/// now a fact about which crates it links rather than an argument it is
/// trusted to pass `None` for.
async fn assemble_node_core(config: &AppConfig) -> anyhow::Result<NodeCore> {
    aenv_node::privileges::require_runtime_capabilities()?;
    aenv_node::privileges::clear_ambient_capabilities()?;

    // 🔴 First, before anything else this process constructs, and in
    // particular before `setup::ensure_environment` — which unlinks every
    // stale network namespace, and a namespace must not be unlinked while a
    // VMM is still running inside it. The sweep is sound only while this
    // process holds nothing on the machine, and that window is widest here.
    // See `src/node_reclaim/` for the whole argument.
    aenv_node::node_reclaim::run(config).await;

    let identity = NodeIdentity::from_config(&config.node_identity);
    // `identity` is moved into the observability service below; the registry
    // wiring needs the same node/cluster identity afterwards.
    let identity_for_registry = identity.clone();
    let p2p_transport = aenv_node::p2p::transport_from_config(config, &identity).await?;
    let p2p_local_endpoint = p2p_transport.local_endpoint();
    let overlaybd_p2p =
        OverlaybdP2pRuntime::start_from_app_config(config, Arc::clone(&p2p_transport)).await;

    aenv_node::setup::ensure_environment(config, overlaybd_p2p.read_facade_address()).await?;

    // Initialize the global ublk device manager (spawns daemon if configured).
    UblkDeviceManager::init_global_from_config_with_p2p_publish_url(
        config,
        overlaybd_p2p.publish_address(),
    )
    .await?;

    if let Err(err) = FirecrackerPool::prime(std::time::Duration::from_secs(10)).await {
        warn!(target: "agentenv", error = %err, "firecracker pool prime failed; continuing startup");
    }

    let snapshot_p2p_transport = config
        .snapshot
        .p2p_enabled
        .then(|| Arc::clone(&p2p_transport));
    // 🔴 Two handles on the same transport, and they answer different
    // questions. The first is how the *resolver* fetches a snapshot's fixed
    // artifacts from a peer; the second is how this node offers the ones it
    // just wrote. Only the machine that holds bytes has anything to offer, so
    // only this assembly builds an advertiser — `assemble_api` passes `None`.
    let snapshot_advertiser = snapshot_p2p_transport.clone().map(|transport| {
        Arc::new(aenv_node::snapshot::P2pSnapshotAdvertiser::new(transport))
            as Arc<dyn aenv_node::snapshot::SnapshotArtifactAdvertiser>
    });
    // 🔴 `None` for the PostgreSQL catalog, unconditionally and by
    // construction: this half never holds a `[pg]` pool (see
    // `refuse_configured_pg_dsn` above, and the dependency graph it is a belt
    // over), so there is nothing for it to build a catalog out of — and under
    // `CentralCatalogUse::Never` it needs none. A node stages bytes; the row is
    // api's to write.
    let snapshot_backend = aenv_node::snapshot::repository::backends::build_snapshot_backend(
        aenv_node::snapshot::repository::backends::storage::build_node_storage(
            config,
            snapshot_p2p_transport,
        )?,
        None,
        aenv_node::snapshot::repository::backends::CentralCatalogUse::Never,
    )?;
    let snapshot_manager = Arc::new(SnapshotManager::from_assembled(
        snapshot_backend,
        snapshot_advertiser,
    ));
    let cluster_cpu_arc: Arc<RwLock<Option<String>>> = Arc::new(RwLock::new(None));
    // The handle the cold-boot paths read the CPUID intersection from.
    //
    // 🔴 Separate from the one the reporter writes, and only when the setting
    // is off. Reporting keeps running either way — the scheduler still collects
    // this node's CPU config and still computes the cluster intersection, so
    // the observability surface does not go dark and turning the setting back
    // on needs no other change. What stops is applying it to a booting microVM,
    // which is the half a host can refuse: see
    // `FirecrackerConfig::apply_cluster_cpu_template` for the Granite Rapids
    // failure this exists for.
    let applied_cpu_arc: Arc<RwLock<Option<String>>> =
        if config.firecracker.apply_cluster_cpu_template {
            Arc::clone(&cluster_cpu_arc)
        } else {
            warn!(
                target: "agentenv",
                "cluster CPU template will not be applied to cold-booting microVMs \
                 (firecracker.apply_cluster_cpu_template = false); this is only safe \
                 while every node in the cluster has the same CPU"
            );
            Arc::new(RwLock::new(None))
        };
    let template_builder = Arc::new(TemplateBuilder::with_cpu_config(Arc::clone(
        &applied_cpu_arc,
    )));
    let image_resolver = Arc::new(ImageResolver::new(config));
    let factory = FirecrackerSandboxFactory::with_cpu_config(applied_cpu_arc);
    // 🔴 This await, and the `reporter.start()` below it, are in this order on
    // purpose. `Orchestrator::new` restores the persisted paused sandboxes
    // before it returns, so by the time the reporter sends its first heartbeat
    // the roster is already complete.
    //
    // The scheduler deletes every routing binding a node owns when that node
    // reports an empty roster — which is what makes a node's disappearance
    // clear its records rather than leave them pointing at nothing. Start the
    // reporter first and a restart would wipe this node's own records, and do
    // it quietly: the next heartbeat puts them back, so all anyone sees is a
    // few seconds of 404s indistinguishable from a cold cache. Pinned by
    // `the_roster_is_complete_the_moment_new_returns` in
    // `src/orchestrator/tests.rs`.
    // 🔴 The node half's own layer cache, named here rather than defaulted
    // inside the orchestrator. This is the process that has one.
    let image_refs = aenv_node::image::local_runtime_image_refs();
    let orchestrator =
        Orchestrator::with_file_backed_store_and_factory(factory, image_refs).await?;
    let observability_config = &config.observability;
    let observability = if observability_config.enabled {
        Some(Arc::new(
            ObservabilityService::new(
                identity,
                Arc::clone(&orchestrator) as Arc<dyn SandboxOrchestration>,
                config.resolved_cpu_template_helper(),
                cluster_cpu_arc,
            )
            .await,
        ))
    } else {
        None
    };
    let reporter = if let Some(service) = observability.as_ref() {
        let mut reporter = ObservabilityReporter::new(
            Arc::clone(service),
            &observability_config.scheduler_report,
            &config.cluster,
            p2p_local_endpoint,
        )?;
        if let Some(inner) = reporter.as_mut() {
            inner.start();
        }
        reporter
    } else {
        None
    };

    let runtime = NodeRuntime {
        overlaybd_p2p,
        p2p_transport,
    };

    Ok(NodeCore {
        orchestrator,
        snapshot_manager,
        template_builder,
        image_resolver,
        observability,
        reporter,
        identity: identity_for_registry,
        runtime,
    })
}

/// `aenv-node`: the half that runs sandboxes, and decides nothing about who
/// owns them.
///
/// What it drops relative to the pre-split single process is one thing, arrived
/// at from one rule: a node executes, the API decides. So the cluster paused
/// registry, the three startup passes over it and the four upkeep tasks that
/// keep this node's claim on a paused sandbox alive are all gone; the API half
/// holds those records now.
///
/// 🔴 What that leaves is a node that never takes a lease it will not renew.
/// The registry it wires in is the disabled one regardless of configuration,
/// which is the fail-closed direction: a node that claimed rows in a shared
/// registry and then never renewed them would have other nodes waiting out a
/// TTL for sandboxes nobody was coming back for.
///
/// 🔴 The three slices this comment used to list as missing have all landed,
/// and each of them is constructed or asserted a few lines below rather than
/// described here — read the code, not this paragraph:
///
/// - the **node gRPC service** the API half drives it through is bound by
///   `spawn_grpc_surface` before `ApiImpl` takes the snapshot manager, and
///   returned as `grpc: Some(..)`;
/// - the **user-REST gate** that stops the `sandboxes`/`snapshots`/`templates`
///   route groups being served from this port is attached by
///   `server::new(api_impl)` (`src/api/role_gate.rs`), off
///   `ApiImpl::owns_sandboxes`, which the `ResumeWiring::node_local` below
///   makes `false`;
/// - the **startup reclaim of host leftovers** already ran, inside
///   `assemble_node_core`.
///
/// So this half is driven, and what it cannot do is now a property of the API
/// half rather than of this one — see the list on `assemble_api`
/// (`crates/aenv-api/src/bin/aenv-api.rs`).
async fn assemble_node(config: &AppConfig) -> anyhow::Result<Assembly> {
    // 🔴 This binary has no `build_pg_pool` to call and no pool type to name.
    // A node must never hold PostgreSQL credentials — see
    // `crates/aenv-api/src/pg/mod.rs`'s own doc comment — and this is that
    // invariant enforced by construction here, not only by
    // `refuse_configured_pg_dsn` at startup.
    let core = assemble_node_core(config).await?;

    let configured_backend = config.orchestrator.paused_registry.backend;
    let cluster_registry_configured =
        !matches!(configured_backend, PausedRegistryBackendKind::Local);
    // Published either way, so "this node has holdings nobody is going to
    // release" is answerable from a scrape rather than from a log line that
    // scrolled past during startup.
    metrics::gauge!("agentenv_node_unreleased_cluster_holdings").set(
        if cluster_registry_configured {
            1.0
        } else {
            0.0
        },
    );
    if cluster_registry_configured {
        // 🔴 Two consequences, and the second is the one that is easy to miss.
        //
        // Forward: this process claims nothing, so it can never fail to renew
        // a lease. That is the fail-closed direction and it is why the
        // disabled registry is wired in regardless of configuration.
        //
        // Backward: whatever *the previous process on this machine* claimed
        // while it ran the pre-split single-process image stays claimed. That
        // process called `release_stale_node_holdings` at startup to hand those
        // back; this binary does not, and deliberately — releasing rows by node
        // identity is a statement about who owns a paused sandbox, which is the
        // one thing this half does not make. Putting it back here would
        // reintroduce exactly the split this half exists to end.
        //
        // The successor for it is the API half's reconciliation, which has a
        // proof this process does not: it can see from the scheduler that this
        // node is gone. Until that lands, an `all` → `node` switch strands the
        // previous process's rows until their leases lapse.
        warn!(
            target: "agentenv",
            configured = ?configured_backend,
            "aenv-node ignores the configured paused-sandbox registry: cluster-wide records \
             belong to the API half. Paused sandboxes stay resumable on this node, and this \
             process claims nothing new — but anything this machine was holding from a previous \
             single-process AgentENV is not released by this one and stays held until its lease \
             lapses."
        );
    }
    // `ApiImpl` needs a coordinator either way; this one is wired to a registry
    // that answers nothing and records nothing, so every cluster-facing call
    // through it is a no-op.
    //
    // 🔴 Including the publish. This comment used to say that publishing the
    // *bytes* of a pause still happened here — that it was the node's job and
    // only the bookkeeping was dropped — and that is exactly what leaked: the
    // capture went to the shared repository on every pause, and with no row to
    // name it, no resume could find it and no delete could collect it. The
    // coordinator now asks whether anything could reference an upload before
    // making one (`PausedSandboxCoordinator::publish`); a pause here stays
    // durable through the node-local persister, which is what the resume on
    // this half reads anyway.
    let paused_wiring = PausedSandboxWiring::new(
        Arc::new(DisabledPausedSandboxRegistry),
        Arc::clone(&core.snapshot_manager),
        &core.identity,
    );
    core.orchestrator
        .set_paused_publisher(paused_wiring.publisher());
    let orchestration: Arc<dyn SandboxOrchestration> =
        Arc::clone(&core.orchestrator) as Arc<dyn SandboxOrchestration>;

    // The half the API half drives this machine through, bound before the
    // `ApiImpl` below takes ownership of the snapshot manager it needs.
    //
    // 🔴 Bound here rather than inside the task that serves it, so a port
    // already in use stops this process instead of leaving it serving HTTP and
    // unreachable to the control plane — which looks, from the control plane,
    // exactly like a node with nothing on it.
    let grpc = {
        let orchestration = Arc::clone(&orchestration);
        let snapshots = Arc::clone(&core.snapshot_manager);
        let node_id = core.identity.id.clone();
        let image_resolver = Arc::clone(&core.image_resolver);
        let template_builder = Arc::clone(&core.template_builder);
        spawn_grpc_surface(
            &config.cluster.node_service_addr,
            "node sandbox service",
            move |listener, shutdown| {
                aenv_node::node_server::serve_on(
                    listener,
                    orchestration,
                    snapshots,
                    node_id,
                    image_resolver,
                    template_builder,
                    shutdown,
                )
            },
        )
        .await?
    };

    let api_impl = Arc::new(ApiImpl::new(
        Arc::clone(&orchestration),
        core.snapshot_manager,
        core.template_builder,
        core.image_resolver,
        core.observability,
        paused_wiring,
        config.sandbox_proxy.domains.clone(),
        // 🔴 `node_local`, and it carries two facts rather than one. No
        // placement source, which is this half showing through rather than an
        // omission: nothing on this process may consult a placement, because
        // handing it one would be handing it the means to decide something it
        // must not decide. And `WakeSite::Local`, which is what
        // `ApiImpl::runs_sandbox_runtime` reads to know it is in this binary
        // and not the other — see its own doc.
        ResumeWiring::node_local(&core.identity.id),
    ));

    Ok(Assembly {
        // 🔴 `server::new` attaches the user-REST gate off the `ApiImpl` above:
        // the user-facing REST surface is still compiled in and still routed,
        // and is answered with 404 on this half. See `src/api/role_gate.rs`.
        app: server::new(api_impl),
        orchestration,
        // 🔴 No upkeep: renewing a lease and reconciling local records against
        // the cluster are both decisions, and this half takes none.
        upkeep: Vec::new(),
        pg_singleton_tasks: Vec::new(),
        reporter: core.reporter,
        runtime: Some(Box::new(core.runtime)),
        // 🔴 A node is a placement target, so it withdraws itself from
        // scheduling before the shutdown pauses start and waits for the cluster
        // to notice. `aenv-api` passes `false`; see `Assembly`'s own field doc.
        drains_on_shutdown: true,
        grpc: Some(grpc),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    /// 🔴 `--role` is gone, and a manifest that still passes it is stopped at
    /// the door rather than silently ignored.
    ///
    /// clap refuses an unknown `--long` by default, so this is a property of
    /// *not* having declared the argument — which is exactly the kind of thing
    /// that comes back by accident when somebody re-adds a flag "for
    /// compatibility". `AENV_ROLE` gets the same treatment for free: no
    /// argument reads it, so a leftover environment variable does nothing at
    /// all. The two are asserted together because the pair is the contract.
    #[test]
    fn the_node_binary_has_no_role_flag_left() {
        NodeCli::command().debug_assert();

        NodeCli::parse_from(["aenv-node"]);
        for spelling in ["api", "node", "all"] {
            assert!(
                NodeCli::try_parse_from(["aenv-node", "--role", spelling]).is_err(),
                "--role {spelling} must be refused outright: a manifest still passing it is a \
                 manifest that has not been migrated, and starting anyway hides that"
            );
        }
        assert!(
            NodeCli::try_parse_from(["aenv-node", "--config", "/dev/null"]).is_ok(),
            "the control: an argument this binary does declare still parses, so the refusals \
             above are about --role and not about parse_from being broken"
        );
    }

    /// 🔴 Both arms of the refusal `async_main` runs before anything is
    /// assembled, and the reason the check survived the role enum it came from.
    ///
    /// The dependency graph already proves this binary cannot *use* a DSN
    /// (`make check-crate-boundaries`). What it cannot prove is that one is
    /// not *present*, and a present-but-ignored `[pg].dsn` is a database
    /// credential sitting on a machine that runs user code.
    #[test]
    fn a_configured_pg_dsn_is_refused_and_an_absent_one_is_not() {
        let error = refuse_configured_pg_dsn(Some("postgres://user:pw@db.internal:5432/agentenv"))
            .expect_err("a node handed a DSN must not start");
        let message = format!("{error:#}");
        assert!(message.contains("[pg].dsn"), "{message}");
        assert!(message.contains("aenv-node"), "{message}");
        // 🔴 The credential itself must not be echoed into the process's first
        // log line by the refusal that exists to keep it off this machine.
        assert!(!message.contains("pw@"), "{message}");

        // The other arm: no DSN configured at all is the ordinary case, and a
        // refusal that fired here would stop every node in the fleet.
        assert!(refuse_configured_pg_dsn(None).is_ok());
    }

    /// 🔴 Guards the shutdown bounds `NodeRuntime::shutdown` and `main` are
    /// each responsible for. Either of the two calls this asserts on can
    /// be deleted without a single one of this binary's other tests noticing
    /// — nothing exercises the real graceful-shutdown path under test — so
    /// this scans the source text directly.
    ///
    /// 🔴 Kept, and kept *here*, through the crate split and through the
    /// deletion of the role enum. What it guards is a RocksDB `spawn_blocking`
    /// closure outliving the async body and hanging the process past
    /// `terminationGracePeriodSeconds` — the real defect behind "every
    /// DaemonSet rollout waits the full hour". That has nothing to do with
    /// which half this process is, so no dependency graph can prove it; and it
    /// was only ever observed on a node that had actually run a VM, which is
    /// this binary.
    ///
    /// See [`server_main::RUNTIME_SHUTDOWN_TIMEOUT`]'s doc for why `main`'s call matters —
    /// without it a stuck `spawn_blocking` closure (RocksDB background
    /// compaction/flush, observed on nodes that had actually run a VM) hangs
    /// the process well past `terminationGracePeriodSeconds` — and
    /// [`NodeRuntime::shutdown`]'s own doc for why the store close comes
    /// before that backstop rather than relying on it.
    ///
    /// 🔴 This asserted on three calls until the snapshot catalog moved wholly
    /// into PostgreSQL. The third was `SnapshotManager::close_stores`, whose
    /// only real work was closing the double write's mirror backlog; with that
    /// store gone the call was a no-op, and an assertion pinning a no-op is a
    /// guard that reports green whatever happens. Both were deleted together.
    #[test]
    fn the_shutdown_bounds_are_still_wired() {
        let source = include_str!("aenv-node.rs");
        let body = |name: &str| {
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
                            return source[open..open + offset].to_string();
                        }
                    }
                    _ => {}
                }
            }
            panic!("{name} has no closing brace");
        };

        let shutdown = body("async fn shutdown(self: Box<Self>)");
        assert!(
            shutdown.contains("close_image_cache_stores"),
            "NodeRuntime::shutdown no longer closes the image cache metadata store's RocksDB \
             handle before process exit"
        );
        let main = body("fn main() -> anyhow::Result<()>");
        assert!(
            main.contains("shutdown_timeout"),
            "main no longer bounds Runtime::shutdown_timeout after block_on returns — an \
             un-bounded fallback here reintroduces the node-never-exits hang"
        );
    }

    /// 🔴 Kept through the deletion of the role enum, rewritten to match the
    /// call it now guards.
    ///
    /// `refuse_configured_pg_dsn`'s own test just above exercises the pure
    /// function directly — it cannot notice if the one call site that wires it
    /// into the running process disappears. That call site is the entire
    /// enforcement of "a `[pg].dsn` must never reach a node": delete it and
    /// both arms above stay green while the invariant they describe is gone.
    ///
    /// 🔴 And no dependency graph replaces it. `make check-crate-boundaries`
    /// proves `aenv-node` cannot link `sqlx`; it says nothing about whether a
    /// DSN in a node's ConfigMap is *refused*. Without this call the setting
    /// would simply be ignored — a database credential left on a machine that
    /// runs user code, and an operator who believes the node is configured.
    ///
    /// Scans this file's own source text for the call, so deleting the call
    /// site fails a test instead of only a future security review.
    #[test]
    fn async_main_actually_refuses_a_configured_pg_dsn() {
        let source = include_str!("aenv-node.rs");
        let body = |name: &str| {
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
                            return source[open..open + offset].to_string();
                        }
                    }
                    _ => {}
                }
            }
            panic!("{name} has no closing brace");
        };

        let async_main = body("async fn async_main() -> anyhow::Result<()>");
        assert!(
            // 🔴 Matches the call *expression*, not the bare identifier. The
            // function's own name appears in comments and in this file's other
            // test; matching the bare identifier would let somebody delete the
            // call and keep a mention, and a false match on a *positive*
            // assertion hides the invariant's enforcement going missing rather
            // than merely giving a false alarm. `async_main`'s body is the only
            // place `refuse_configured_pg_dsn(config.` is spelled.
            async_main.contains("refuse_configured_pg_dsn(config."),
            "async_main no longer calls refuse_configured_pg_dsn — a [pg].dsn could reach a \
             node with nothing left to refuse it, even though the pure function's own two arms \
             would still report green"
        );
        // 🔴 The mutation control, in the same test: the scan has to be able to
        // fail. A `body()` that returned the whole file, or an empty string
        // that `contains` happened to satisfy, would pass the assertion above
        // for the wrong reason.
        assert!(
            !async_main.contains("refuse_configured_pg_dsn(dsn"),
            "the scan is reading something other than async_main's body — \
             `refuse_configured_pg_dsn(dsn` is the definition's own parameter list, which is \
             outside it"
        );
    }

    /// The body of the named item in this file's own source text.
    ///
    /// Shared by the call-site guards below, which scan rather than execute:
    /// deleting a call site is a change no unit test of the called function
    /// can see.
    fn body_of(source: &str, name: &str) -> String {
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
                        return source[open..open + offset].to_string();
                    }
                }
                _ => {}
            }
        }
        panic!("{name} has no closing brace");
    }

    /// 🔴 This binary actually refuses a manifest that still sets one of the
    /// removed snapshot-catalog switches.
    ///
    /// `refuse_removed_catalog_env_vars` has its own two-direction test in
    /// `src/cfg.rs`, and that test stays green with the call site deleted —
    /// which is the whole failure mode: confique ignores an undeclared
    /// environment variable, so a `AENV_SNAPSHOT_CATALOG_WRITE=both` left in a
    /// manifest would start a process that looks entirely healthy while its
    /// operator believes the catalog is double-written. It is not, and nothing
    /// would say so.
    #[test]
    fn async_main_actually_refuses_the_removed_catalog_switches() {
        let source = include_str!("aenv-node.rs");
        let async_main = body_of(source, "async fn async_main() -> anyhow::Result<()>");
        assert!(
            async_main.contains("cfg::refuse_removed_catalog_env_vars()"),
            "async_main no longer refuses AENV_SNAPSHOT_CATALOG_WRITE / _READ; an un-migrated \
             manifest would then start in silence, and src/cfg.rs's own test of the pure \
             function would still report green"
        );
        // 🔴 The mutation control: the scan has to be able to fail. A `body_of`
        // that returned the whole file would satisfy the assertion above for
        // the wrong reason, and `async fn async_main` sits before the body's
        // opening brace, so a correct extraction never contains it.
        assert!(
            !async_main.contains("async fn async_main"),
            "the scan is reading more than async_main's body, so the assertion above proves \
             nothing about where the call actually is"
        );
    }
}
