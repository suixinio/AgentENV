use std::sync::{Arc, RwLock};
use std::time::Duration;

use agentenv::api::{server, ApiImpl, PausedSandboxWiring, ResumeWiring, StaleReleaseOutcome};
use agentenv::cfg::{AppConfig, MetadataStoreBackendKind, PausedRegistryBackendKind};
use agentenv::identity::NodeIdentity;
use agentenv::image::ImageResolver;
use agentenv::node_client::{RemoteSandboxBackendFactory, SchedulerNodePlacement};
use agentenv::observability::{ObservabilityReporter, ObservabilityService};
use agentenv::orchestrator::{
    build_paused_registry, DisabledPausedSandboxRegistry, DisabledSandboxPersister,
    FileBackedSandboxPersister, InMemoryMetadataStore, Orchestrator, RedisMetadataStore,
    SandboxOrchestration,
};
use agentenv::overlaybd::OverlaybdP2pRuntime;
use agentenv::p2p::P2pTransport;
use agentenv::role::ServerRole;
use agentenv::sandbox::{FirecrackerPool, FirecrackerSandboxFactory, UblkDeviceManager};
use agentenv::snapshot::SnapshotManager;
use agentenv::template::TemplateBuilder;
use anyhow::Context as _;
use axum::serve::ListenerExt;
use clap::Parser;
use tokio::sync::oneshot;
use tracing::{info, warn};

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

/// The orchestrator a machine-local role assembles: this node's own ledger,
/// this node's Firecracker, this node's files.
type LocalOrchestrator =
    Orchestrator<InMemoryMetadataStore, FirecrackerSandboxFactory, FileBackedSandboxPersister>;

#[derive(Debug, Parser)]
#[command(name = "agentenv server")]
struct ServerCli {
    /// Which half of the split this process runs: api, node, or all.
    ///
    /// Defaults to `all` — one process holding both halves, which is what has
    /// always run. Also readable from AENV_ROLE; an explicit --role wins.
    #[arg(long, value_enum)]
    role: Option<ServerRole>,

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

/// What an assembled role hands back to `main`: the router it serves and the
/// four things the shutdown path has to stand down, in that order.
struct Assembly {
    app: axum::Router,
    /// The orchestration surface this process drives. Which concrete
    /// `Orchestrator` is behind it is the role's decision.
    orchestration: Arc<dyn SandboxOrchestration>,
    /// Background tasks to stop before the shutdown pauses start.
    upkeep: Vec<tokio::task::JoinHandle<()>>,
    /// The heartbeat sender, for roles that report themselves as a machine.
    reporter: Option<ObservabilityReporter>,
    /// The machine-local runtime, for roles that brought one up.
    runtime: Option<NodeRuntime>,
    /// The gRPC surface this role serves alongside the HTTP one, already
    /// accepting: the task serving it, and the channel that stops it.
    ///
    /// 🔴 Already bound by the time this is built. A listener that binds inside
    /// a spawned task turns "the port is taken" into a task that quietly ended,
    /// and the process goes on serving HTTP with a gRPC surface nobody can
    /// reach — which is indistinguishable from a surface nobody is calling.
    ///
    /// 🔴 `None` for `--role all`, and that is the rollback showing through
    /// rather than an omission. `all` is defined as the process that ran before
    /// the split, and that process listened on one port.
    grpc: Option<(tokio::task::JoinHandle<()>, oneshot::Sender<()>)>,
}

/// Bound for each individual step in [`NodeRuntime::shutdown`].
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

/// The machine-local runtime a sandbox-running role owns: the two P2P pieces it
/// holds by value, the process-wide Firecracker pool and ublk daemon it
/// reaches through their globals, and a handle onto the snapshot manager kept
/// only so shutdown can close its durable mirror-backlog store.
///
/// 🔴 Held as a whole rather than as independent handles so that the teardown
/// order — pool, ublk, overlaybd P2P, transport, then the two RocksDB stores
/// this bundle can reach — stays in one place.
struct NodeRuntime {
    overlaybd_p2p: OverlaybdP2pRuntime,
    p2p_transport: Arc<dyn P2pTransport>,
    /// Not otherwise used here: every operational use of the manager goes
    /// through the `Arc` clone `assemble_node_core`'s caller wires into
    /// `ApiImpl`. This clone exists only for `close_stores` below.
    snapshot_manager: Arc<SnapshotManager>,
}

impl NodeRuntime {
    async fn shutdown(self) {
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

        // 🔴 The two RocksDB stores this bundle can still reach. Neither call
        // drops the underlying store — both only stop its background
        // compaction/flush ahead of time (see `LocalKvStore::close`) — so this
        // is safe even while other clones of the same store (the image cache's
        // shared-instance registry, in particular) are still live elsewhere in
        // the process.
        info!(target: "agentenv", "closing image cache metadata store");
        agentenv::image::close_image_cache_stores(NODE_RUNTIME_SHUTDOWN_STEP_TIMEOUT).await;
        info!(target: "agentenv", "closing snapshot catalog mirror backlog store");
        self.snapshot_manager
            .close_stores(NODE_RUNTIME_SHUTDOWN_STEP_TIMEOUT)
            .await;
    }
}

/// Everything a machine-local role builds before the question of who owns a
/// paused sandbox comes up.
///
/// 🔴 `--role node` and `--role all` share this, deliberately: it is the part
/// where the two are *supposed* to be identical, and a second copy of it would
/// be a second thing to keep in step with the first. What the two roles differ
/// on comes after, in `assemble_node` and `assemble_all`.
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

/// Bound for the final `Runtime::shutdown_timeout` call in `main`, below.
///
/// 🔴 Deliberately larger than [`NODE_RUNTIME_SHUTDOWN_STEP_TIMEOUT`]: by the
/// time this runs, every shutdown step this file knows to bound has already
/// run and already had its own timeout, so this bound is what is left over
/// for whatever *wasn't* individually bounded — a stuck `spawn_blocking`
/// nothing above reached explicitly. It only needs to be comfortably smaller
/// than Kubernetes' `terminationGracePeriodSeconds` (observed misconfigured at
/// 3600s on the cluster this exists for), not tight.
const RUNTIME_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

/// 🔴 Not `#[tokio::main]`. That macro's generated `main` builds the runtime,
/// `block_on`s the async body, then lets the `Runtime` value fall out of
/// scope — and `Runtime`'s `Drop` shuts down its blocking-task pool by calling
/// `BlockingPool::shutdown(None)` (tokio, `runtime/blocking/pool.rs`), and
/// `None` means *no timeout*: it waits forever for every `spawn_blocking`
/// closure that has already started to return.
///
/// Every RocksDB store this process opens (`LocalKvStore`, see
/// `crate::local_store`) does its writes, and its own background
/// compaction/flush, through `spawn_blocking` — so one such closure still
/// running when the async body below returns was enough to make the whole
/// process hang past `terminationGracePeriodSeconds`, long after every log
/// line the graceful shutdown was ever going to print had already printed.
/// (Observed only on nodes that had actually run a VM: an idle node's stores
/// have nothing to compact, so `Drop` there really did return immediately —
/// which is exactly why the hang looked selective rather than universal.)
///
/// The explicit `close()` calls the shutdown path below makes — the
/// persisted-sandboxes store, the image cache metadata store, the snapshot
/// catalog mirror backlog — are meant to make that background work finish,
/// and log whether it did, before any of this runs. `shutdown_timeout` here is the
/// backstop for whatever is still outstanding regardless: unlike plain `Drop`,
/// it bounds the same wait, and once it returns, `main` returning ends the
/// process — any `spawn_blocking` closure still running at that point keeps
/// running on its own OS thread, but nothing waits on it any more, the same
/// way `storage-util`'s un-joined io_uring worker threads already don't block
/// process exit today.
fn main() -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("build the tokio runtime")?;
    let result = runtime.block_on(async_main());
    runtime.shutdown_timeout(RUNTIME_SHUTDOWN_TIMEOUT);
    result
}

async fn async_main() -> anyhow::Result<()> {
    agentenv::logging::init();
    agentenv_observability::init_prometheus_recorder()?;

    let cli = ServerCli::parse();
    let role = ServerRole::resolve(cli.role)?;
    let config_manager = if let Some(config_path) = cli.config.as_deref() {
        agentenv::cfg::ConfigManager::init_global_from_path(config_path)?
    } else {
        agentenv::cfg::ConfigManager::init_global()?
    };
    let config = config_manager.config();

    // Before either provisioning path runs, not after: the point of the check
    // is that the process was pointed at the wrong workload, and provisioning a
    // host on the way to finding that out helps nobody.
    role.check_setup_flags(cli.setup_only, cli.setup_host)?;

    if cli.setup_only {
        agentenv::setup::ensure_provisioning(config).await?;
        info!(target: "agentenv", "dependency setup complete (setup-only mode)");
        return Ok(());
    }

    if cli.setup_host {
        agentenv::setup::ensure_host(config, &cli.runtime_user, &cli.runtime_group)?;
        info!(target: "agentenv", "host setup complete");
        return Ok(());
    }

    info!(target: "agentenv", role = role.as_str(), "assembling server");
    let Assembly {
        app,
        orchestration,
        upkeep,
        mut reporter,
        runtime,
        grpc,
    } = match role {
        ServerRole::All => assemble_all(config).await?,
        ServerRole::Node => assemble_node(config).await?,
        ServerRole::Api => assemble_api(config).await?,
    };

    // Split so the stop signal can travel into the graceful-shutdown closure
    // while the task stays here to be joined after it.
    let (grpc_task, grpc_shutdown) = match grpc {
        Some((task, shutdown)) => (Some(task), Some(shutdown)),
        None => (None, None),
    };

    let addr = std::env::var("API_ADDR").unwrap_or_else(|_| "0.0.0.0:8000".to_string());
    let shutdown_orchestration = Arc::clone(&orchestration);
    let drain_orchestration = Arc::clone(&orchestration);
    let drains_on_shutdown = role.drains_on_shutdown();
    let drain_propagation =
        Duration::from_secs(config.orchestrator.shutdown_drain_propagation_secs);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    // envd streams a command's lifecycle as a burst of tiny Connect-RPC frames.
    // With Nagle left on, the frame after the first one waits for the client's
    // delayed ACK, adding a ~40ms floor to every short-lived command.
    let listener = tokio::net::TcpListener::bind(&addr).await?.tap_io(|stream| {
        if let Err(err) = stream.set_nodelay(true) {
            warn!(target: "agentenv", error = %err, "failed to set TCP_NODELAY on incoming connection");
        }
    });
    info!(target: "agentenv", addr = %addr, "API server listening");

    let shutdown_cleanup = tokio::spawn(async move {
        if let Ok(()) = shutdown_rx.await {
            if let Some(mut handle) = reporter.take() {
                info!(target: "agentenv", "stopping observability reporter before process exit");
                if let Err(err) = handle.shutdown().await {
                    warn!(target: "agentenv", error = %err, "error occurred while shutting down observability reporter");
                }
            }
            // Stop reconciling before the shutdown pauses start: those write
            // paused records these tasks would otherwise be racing to inspect.
            for task in &upkeep {
                task.abort();
            }
            info!(target: "agentenv", "stopping sandboxes before process exit");
            if let Err(err) = shutdown_orchestration.shutdown().await {
                warn!(target: "agentenv", error = %err, "error occurred while shutting down orchestrator");
            }
            if let Some(runtime) = runtime {
                runtime.shutdown().await;
            }
        }
    });

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown_signal().await;

            // Take the node out of rotation before tearing anything down, and
            // give the scheduler time to hear about it. Without this pause a
            // sandbox can be placed here in the moments after the signal and be
            // paused again before the caller has finished starting it.
            //
            // Isolation set through the admin API is left alone: it is already
            // the state we want, and re-announcing it would reset the timestamp
            // an operator is watching.
            //
            // 🔴 Only a role that can be scheduled onto has anything to
            // withdraw; an API replica is not a placement target.
            if drains_on_shutdown && drain_orchestration.set_scheduling_disabled(true) {
                info!(
                    target: "agentenv",
                    wait_secs = drain_propagation.as_secs(),
                    "isolated the node for shutdown; waiting for the scheduler to notice"
                );
                if !drain_propagation.is_zero() {
                    tokio::time::sleep(drain_propagation).await;
                }
            }

            // Stops accepting on the second listener at the same moment the
            // HTTP one stops: both carry work that the teardown below is about
            // to make impossible to finish.
            if let Some(shutdown) = grpc_shutdown {
                let _ = shutdown.send(());
            }

            let _ = shutdown_tx.send(());
        })
        .await?;

    if let Some(task) = grpc_task {
        if let Err(err) = task.await {
            warn!(target: "agentenv", error = %err, "the gRPC surface did not stop cleanly");
        }
    }

    shutdown_cleanup.await?;

    Ok(())
}

/// Brings up everything a role that runs sandboxes on this machine needs.
async fn assemble_node_core(config: &AppConfig, role: ServerRole) -> anyhow::Result<NodeCore> {
    // Both roles that reach here run the machine and report it as one; the API
    // half never does, and never calls this.
    debug_assert!(role.runs_sandbox_runtime());
    debug_assert!(role.sends_heartbeats());

    agentenv::privileges::require_runtime_capabilities()?;
    agentenv::privileges::clear_ambient_capabilities()?;

    // 🔴 First, before anything else this process constructs, and in
    // particular before `setup::ensure_environment` — which unlinks every
    // stale network namespace, and a namespace must not be unlinked while a
    // VMM is still running inside it. The sweep is sound only while this
    // process holds nothing on the machine, and that window is widest here.
    // See `src/node_reclaim/` for the whole argument.
    agentenv::node_reclaim::run(role, config).await;

    let identity = NodeIdentity::from_config(&config.node_identity);
    // `identity` is moved into the observability service below; the registry
    // wiring needs the same node/cluster identity afterwards.
    let identity_for_registry = identity.clone();
    let p2p_transport = agentenv::p2p::transport_from_config(config, &identity).await?;
    let p2p_local_endpoint = p2p_transport.local_endpoint();
    let overlaybd_p2p =
        OverlaybdP2pRuntime::start_from_app_config(config, Arc::clone(&p2p_transport)).await;

    agentenv::setup::ensure_environment(config, overlaybd_p2p.read_facade_address()).await?;

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
    let snapshot_manager = Arc::new(SnapshotManager::new(snapshot_p2p_transport).await?);
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
    let orchestrator = Orchestrator::with_file_backed_store_and_factory(role, factory).await?;
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

    // Cloned before `snapshot_manager` moves into `NodeCore` below: the
    // shutdown path needs its own handle to close the manager's stores, kept
    // separately from whatever the caller does with the `NodeCore` field (move
    // it into `ApiImpl`, in every role that reaches this function today).
    let runtime = NodeRuntime {
        overlaybd_p2p,
        p2p_transport,
        snapshot_manager: Arc::clone(&snapshot_manager),
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

/// `--role all`: both halves in one process, which is what has always run.
///
/// 🔴 This is the rollback target, so it is defined as *today's behaviour* and
/// not as the union of `api` and `node`. Nothing belongs here that was not
/// here before the split.
async fn assemble_all(config: &AppConfig) -> anyhow::Result<Assembly> {
    let role = ServerRole::All;
    let core = assemble_node_core(config, role).await?;

    debug_assert!(role.arbitrates_paused_sandbox_ownership());
    // The rollback target serves everything it ever served: no RoleGate is
    // attached for this role at all (`crate::api::role_gate::attach`).
    debug_assert!(role.serves_user_facing_rest());
    debug_assert!(!role.reclaims_host_leftovers_at_startup());
    let paused_registry = build_paused_registry(
        &config.orchestrator.paused_registry,
        &config.cluster,
        &core.identity,
    )
    .await?;
    let paused_wiring = PausedSandboxWiring::new(
        paused_registry,
        Arc::clone(&core.snapshot_manager),
        &core.identity,
    );
    // The orchestrator publishes every pause it performs, including the ones no
    // API request asked for (expiry, shutdown).
    core.orchestrator
        .set_paused_publisher(paused_wiring.publisher());
    let orchestration: Arc<dyn SandboxOrchestration> =
        Arc::clone(&core.orchestrator) as Arc<dyn SandboxOrchestration>;
    let api_impl = Arc::new(ApiImpl::new(
        Arc::clone(&orchestration),
        core.snapshot_manager,
        core.template_builder,
        core.image_resolver,
        core.observability,
        paused_wiring,
        config.sandbox_proxy.domains.clone(),
        role,
        // 🔴 The real placement source, even though nothing serves the wake-up
        // gRPC surface on this role today. `all` is the one role that both
        // answers wake decisions and runs the sandboxes, so if that surface is
        // ever exposed here it must arrive with the pin check already wired:
        // an unpublished pause woken on the wrong machine does not fail, it
        // rebuilds from an older snapshot and loses the last pause silently.
        //
        // Costs nothing at startup — `connect_lazy` opens no socket — and the
        // startup-sequence gate accounts for it by name.
        ResumeWiring::from_config(&core.identity.id)?,
    ));
    // All three run before the listener opens, and the order is load-bearing.
    //
    // Releasing goes first, and only here: it hands back every sandbox a
    // previous process on this machine died holding, which is sound precisely
    // because this process holds nothing yet. Once the listener is open that
    // stops being true and the same call would be giving away live sandboxes.
    //
    // Renewing then stops this node's own remaining records from looking
    // abandoned during startup, and reconciling makes sure a resume arriving
    // first does not find a paused record the cluster has already moved past.
    let stale_release = api_impl.release_stale_node_holdings().await;
    api_impl.renew_paused_leases().await;
    api_impl.reconcile_local_records().await;
    let mut paused_upkeep = spawn_paused_record_upkeep(
        Arc::clone(&api_impl),
        config.orchestrator.paused_registry.reconcile_interval(),
    );
    // Only a registry that could not be reached is worth retrying, and only
    // from here: the retry is bounded by the same fence the startup call is,
    // and that fence closes the moment this node takes a sandbox live.
    if stale_release == StaleReleaseOutcome::Failed {
        let retrier = Arc::clone(&api_impl);
        paused_upkeep.push(tokio::spawn(async move {
            retrier.retry_stale_node_holdings_release().await;
        }));
    }

    Ok(Assembly {
        app: server::new(api_impl, role),
        orchestration,
        upkeep: paused_upkeep,
        reporter: core.reporter,
        runtime: Some(core.runtime),
        // 🔴 Not "not yet": never. This role is the rollback target and is
        // defined as the process that ran before the split, which listened on
        // one port. The node service belongs to `--role node`; see
        // `assemble_node`.
        grpc: None,
    })
}

/// `--role node`: the half that runs sandboxes, and decides nothing about who
/// owns them.
///
/// What it drops relative to `all` is one thing, arrived at from one rule: a
/// node executes, the API decides. So the cluster paused registry, the three
/// startup passes over it and the four upkeep tasks that keep this node's claim
/// on a paused sandbox alive are all gone; the API half holds those records now.
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
/// - the **RoleGate** that stops user-facing REST being served from this port
///   is attached by `server::new(api_impl, role)` (`src/api/role_gate.rs`),
///   which is what `debug_assert!(!role.serves_user_facing_rest())` is naming;
/// - the **startup reclaim of host leftovers** already ran, inside
///   `assemble_node_core`, which is what
///   `debug_assert!(role.reclaims_host_leftovers_at_startup())` is naming.
///
/// So this role is driven, and what it cannot do is now a property of the API
/// half rather than of this one — see the list on [`assemble_api`].
async fn assemble_node(config: &AppConfig) -> anyhow::Result<Assembly> {
    let role = ServerRole::Node;
    let core = assemble_node_core(config, role).await?;

    debug_assert!(!role.arbitrates_paused_sandbox_ownership());
    // Both are `role`'s to decide and both are read from it below rather than
    // spelled out again: the user-facing REST surface is refused by the layer
    // `server::new` attaches, and the host sweep already ran inside
    // `assemble_node_core`.
    debug_assert!(!role.serves_user_facing_rest());
    debug_assert!(role.reclaims_host_leftovers_at_startup());
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
        // while it ran as `--role all` stays claimed. `--role all` calls
        // `release_stale_node_holdings` at startup to hand those back; this
        // role does not, and deliberately — releasing rows by node identity is
        // a statement about who owns a paused sandbox, which is the one thing
        // `ServerRole::Node` answers `false` to
        // (`arbitrates_paused_sandbox_ownership`). Putting it back here would
        // reintroduce exactly the split this role exists to end.
        //
        // The successor for it is the API half's reconciliation, which has a
        // proof this process does not: it can see from the scheduler that this
        // node is gone. Until that lands, an `all` → `node` switch strands the
        // previous process's rows until their leases lapse.
        warn!(
            target: "agentenv",
            configured = ?configured_backend,
            "--role node ignores the configured paused-sandbox registry: cluster-wide records \
             belong to the API half. Paused sandboxes stay resumable on this node, and this \
             process claims nothing new — but anything this machine was holding from a previous \
             --role all process is not released by this one and stays held until its lease lapses."
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
        spawn_grpc_surface(
            &config.cluster.node_service_addr,
            "node sandbox service",
            move |listener, shutdown| {
                agentenv::node_server::serve_on(
                    listener,
                    orchestration,
                    snapshots,
                    node_id,
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
        role,
        // 🔴 No placement source, and that is the role showing through rather
        // than an omission: `serves_wake_decisions()` is false here, so nothing
        // on this process may consult a placement. Handing it one would be
        // handing it the means to decide something it must not decide.
        ResumeWiring::node_local(&core.identity.id),
    ));

    Ok(Assembly {
        // 🔴 The role is what attaches the RoleGate: the user-facing REST
        // surface is still compiled in and still routed, and is answered with
        // 404 on this half. See `src/api/role_gate.rs`.
        app: server::new(api_impl, role),
        orchestration,
        // 🔴 No upkeep: renewing a lease and reconciling local records against
        // the cluster are both decisions, and this role takes none.
        upkeep: Vec::new(),
        reporter: core.reporter,
        runtime: Some(core.runtime),
        grpc: Some(grpc),
    })
}

/// `--role api`: the deciding half.
///
/// It owns sandboxes and runs none of them. Everything it constructs is either
/// a decision (the cluster store, the paused registry, placement) or a surface
/// (the full REST route set, the wake-up gRPC); everything a machine needs is
/// absent, and absent because this role answers `false` to
/// [`ServerRole::runs_sandbox_runtime`].
///
/// # 🔴 What it refuses to start without, and why each refusal is loud
///
/// Three settings have no safe default here, and each of them fails startup
/// rather than degrading:
///
/// - **a cluster metadata store.** The in-memory one is a single process's
///   private ledger. A replica using it would hold an opinion about sandboxes
///   no other replica shared, and the two would not disagree visibly — each
///   would simply answer 404 for the other's sandboxes.
/// - **a scheduler endpoint.** A create has to be placed and a wake-up has to
///   be located, and there is no local machine to fall back to. Worse than
///   having nowhere to put a sandbox: with no placement source every placement
///   answers `Unconstrained`, so a sandbox pinned to one machine's disk would
///   be woken on another — which succeeds, by rebuilding it from an older
///   snapshot.
/// - **the wake-up listener's port.** Bound at assembly, because the gateway's
///   cold path is the only way a paused sandbox comes back, and a replica
///   serving HTTP with no gRPC surface refuses every wake-up with a connection
///   error the gateway reads as "try again later".
///
/// # 🔴 What it constructs that a machine-local role does not
///
/// [`RemoteSandboxBackendFactory`], which is what makes an `Orchestrator`
/// written entirely in terms of local backends drive sandboxes on other
/// machines — and which is also the thing that puts this control plane's
/// ownership marker on every create it sends
/// (`SandboxBackendFactory::stamps_control_plane_ownership`).
///
/// # 🔴 What is here and does not work yet
///
/// - **A cold create.** `RemoteSandboxBackendFactory::build` refuses: the build
///   spec it is handed has already been resolved into paths on a local disk and
///   the user's image reference is gone by then. Creating from a snapshot or a
///   template works; `POST /sandboxes-cold` does not. It is now refused at the
///   door instead of failing on the way there: the route reads
///   `ServerRole::runs_sandbox_runtime` before it resolves anything
///   (`crate::api::impls`), because resolution comes first on that path and
///   this Pod has no `regctl` — so what a caller used to be told was that a
///   registry tool was missing, and the factory's refusal, which is the one
///   that says *this half cannot cold-start*, was never reached. The
///   capability is still missing; what changed is that its absence is now
///   something the caller is told.
/// - **Publishing a pause.** Pausing and resuming a sandbox on another machine
///   both work — `Pause` leaves the capture on the node and a reference in the
///   cluster store, `Orchestrator::resume_sandbox` reads that reference back
///   through `MetadataStore::paused_handle`, and `Resume` asks the machine
///   holding the capture to reopen it. What is not served is the *published*
///   arm: staging a captured snapshot on the node is not wired up, so this half
///   sends `publish: false` and a node asked to publish refuses rather than
///   answering with nothing. The consequence worth reading twice: **a sandbox
///   paused through this half is resumable only on the machine that paused
///   it**, so losing that machine loses the sandbox.
///
///   🔴 This bullet carried a second consequence — that deleting a paused
///   sandbox left its capture on the node, "because a delete reaches a backend
///   only through a live handle and a paused sandbox has none". That is no
///   longer true, and it is retired here rather than silently dropped so it is
///   not re-derived from the same reasoning. A delete that finds no local
///   handle now goes through `Orchestrator::absent_handle`, which adopts the
///   sandbox as an attaching `RemoteSandboxStub`; `attach` places the stub even
///   when `Describe` answers `NotFound` — which is exactly what a node answers
///   for a sandbox it is holding paused, because `Describe` reports what is
///   *live* — so `stop` finds a placed, unpaused stub and sends `Delete`. On
///   the node, `fenced` reads the incarnation off the record when there is no
///   live handle, and the delete takes the paused record and its artifacts with
///   it (`delete_record_and_artifacts`).
/// - **A snapshot.** `RemoteSandboxStub::snapshot` sends `Checkpoint` and the
///   node answers `Unimplemented`: *checkpoint is not served yet: staging a
///   captured snapshot on the node is not wired up*. Same missing piece as the
///   published arm above, reached from the other direction — a checkpoint's
///   whole product is the staged snapshot, so there is nothing else the call
///   could return. The refusal is classified non-terminal, so
///   `Orchestrator::capture_snapshot` rolls the sandbox back to `Running`
///   rather than tearing it down: the caller gets an error and keeps the
///   sandbox. Unlike the two door refusals in this list it is not caught here —
///   the request goes to the node and the answer comes back.
/// - **Patching custom extension params — retired.** This bullet used to say
///   `PATCH /sandboxes/{id}/custom-extension-params` answered the caller and
///   updated the store while the running sandbox never learned the new
///   value, because `SandboxBackend::update_custom_extension_params` was
///   infallible by signature and the stub could only forward it from a
///   spawned task, fire-and-forget, into a node that answered `Unimplemented`.
///   Kept here rather than deleted so it is not re-derived from the same
///   reasoning. It no longer holds: the method is now `async ... ->
///   Result<()>` like every other property update on this backend
///   (`RemoteSandboxStub::update_custom_extension_params`, mirroring
///   `update_network_policy`), the node answers for real
///   (`NodeSandboxService::update_params` ->
///   `Orchestrator::replace_sandbox_custom_extension_params`, the same
///   assign-then-persist tail `patch_sandbox_custom_extension_params` already
///   used locally), and a failure on either side is returned to the `PATCH`
///   caller with the metadata store left untouched — see
///   `Orchestrator::apply_custom_extension_params`.
/// - **Building a template.** `TemplateBuilder` drives a `FirecrackerSandbox`
///   directly, outside the orchestrator entirely, so a build here would reach
///   for `/dev/kvm` in a Pod that has none. It is now refused at the door
///   instead: `POST /v2/templates/{id}/builds/{id}` answers the caller rather
///   than accepting the build and losing it in a background task
///   (`crate::api::impls` — the refusal reads `ServerRole::runs_sandbox_runtime`
///   and names where the build can be run). The capability is still missing;
///   what changed is that its absence is now something the caller is told.
async fn assemble_api(config: &AppConfig) -> anyhow::Result<Assembly> {
    let role = ServerRole::Api;
    // The four this role answers `false` to, stated where somebody adding a
    // line to this function will read them.
    debug_assert!(!role.runs_sandbox_runtime());
    debug_assert!(!role.sends_heartbeats());
    debug_assert!(!role.reclaims_host_leftovers_at_startup());
    debug_assert!(!role.drains_on_shutdown());
    // And the three it answers `true` to.
    debug_assert!(role.arbitrates_paused_sandbox_ownership());
    debug_assert!(role.serves_user_facing_rest());
    debug_assert!(role.serves_wake_decisions());

    let identity = NodeIdentity::from_config(&config.node_identity);
    let identity_for_registry = identity.clone();

    // 🔴 Both settings are read and refused *before* anything is connected, and
    // the order is the point rather than tidiness: a replica misconfigured in
    // two ways should be told about the one it can see from its own config
    // rather than about the Redis it could not reach on the way to finding out.
    // It is also what makes each refusal testable without a service running —
    // and an untested refusal branch is the shape this programme has already
    // paid for twice.
    let store_config = cluster_store_config(&config.orchestrator.store)?;
    let placement = cluster_placement(&config.cluster)?;
    let store = RedisMetadataStore::connect(store_config)
        .await
        .context("connect the cluster metadata store")?;
    // 🔴 The persister is `Disabled` and not file-backed. A file-backed one
    // would write paused-sandbox artifacts to this Pod's disk for sandboxes
    // whose bytes are on other machines, and then load them back at startup as
    // sandboxes this replica believes it can resume. The durable record of a
    // paused sandbox is the cluster store's row and the registry's, not a file
    // here.
    //
    // 🔴 And the role is what makes the envd access-token seed mandatory. Two
    // replicas that each invented one would hand users tokens the other cannot
    // verify, and nothing about that is visible until a user's token stops
    // working (`_sd-impl-phase3-role.md` §9.2). Refused here, at construction,
    // before the listener opens.
    let orchestrator = Orchestrator::new(
        role,
        store,
        RemoteSandboxBackendFactory::new(placement),
        DisabledSandboxPersister,
    )
    .await?;
    let orchestration: Arc<dyn SandboxOrchestration> =
        Arc::clone(&orchestrator) as Arc<dyn SandboxOrchestration>;

    // The commit side of snapshots, the resolver, and the builder's scheduling
    // half. None of the three needs a machine; what they need is the
    // repository, which is shared.
    //
    // 🔴 No P2P transport is passed, and `[snapshot].p2p_enabled` is not
    // consulted: P2P moves bytes between machines that hold them, and this
    // process holds none.
    let snapshot_manager = Arc::new(SnapshotManager::new(None).await?);
    let template_builder = Arc::new(TemplateBuilder::new());
    let image_resolver = Arc::new(ImageResolver::new(config));

    // 🔴 The receiving side only. `ObservabilityReporter` is not started: a
    // heartbeat reports a machine, and this replica is not one — reporting
    // itself would put a node in the scheduler's table that can never run
    // anything, and the scheduler would place sandboxes on it.
    //
    // `cpu_template_helper` is `None` rather than the configured path: the
    // helper is one of the downloaded runtime assets, this Pod has none of
    // them, and the CPUID intersection it feeds is about the machines that
    // boot microVMs.
    let observability = if config.observability.enabled {
        Some(Arc::new(
            ObservabilityService::new(
                identity,
                Arc::clone(&orchestration),
                None,
                Arc::new(RwLock::new(None)),
            )
            .await,
        ))
    } else {
        None
    };

    let paused_registry = build_paused_registry(
        &config.orchestrator.paused_registry,
        &config.cluster,
        &identity_for_registry,
    )
    .await?;
    let paused_wiring = PausedSandboxWiring::new(
        paused_registry,
        Arc::clone(&snapshot_manager),
        &identity_for_registry,
    );
    orchestrator.set_paused_publisher(paused_wiring.publisher());

    let api_impl = Arc::new(ApiImpl::new(
        Arc::clone(&orchestration),
        snapshot_manager,
        template_builder,
        image_resolver,
        observability,
        paused_wiring,
        config.sandbox_proxy.domains.clone(),
        role,
        // 🔴 `WakeSite::Remote`: the pin is honoured by the orchestration
        // surface below, which places the wake-up on the machine the paused
        // state names, rather than by a same-machine check this process cannot
        // make. Requires a scheduler endpoint and says so if it has none.
        ResumeWiring::cluster_from_config()?,
    ));

    // The same three passes, in the same order, and for the same reasons as
    // `assemble_all` — with one difference worth naming. There, "this process
    // holds nothing yet" is a statement about a machine; here it is a statement
    // about a replica, and it holds because a replica's identity is its own
    // (`AENV_NODE_ID` is the Pod's name). Two replicas sharing one identity
    // would make the release below hand back the *other* replica's live
    // holdings.
    let stale_release = api_impl.release_stale_node_holdings().await;
    api_impl.renew_paused_leases().await;
    api_impl.reconcile_local_records().await;
    let mut paused_upkeep = spawn_paused_record_upkeep(
        Arc::clone(&api_impl),
        config.orchestrator.paused_registry.reconcile_interval(),
    );
    if stale_release == StaleReleaseOutcome::Failed {
        let retrier = Arc::clone(&api_impl);
        paused_upkeep.push(tokio::spawn(async move {
            retrier.retry_stale_node_holdings_release().await;
        }));
    }

    let grpc = {
        let served = Arc::clone(&api_impl);
        spawn_grpc_surface(
            &config.cluster.api_grpc_addr,
            "sandbox resume service",
            move |listener, shutdown| agentenv::api::grpc::serve_on(listener, served, shutdown),
        )
        .await?
    };

    Ok(Assembly {
        // 🔴 No RoleGate: this half serves the whole user-facing surface. The
        // gate exists to stop a *node* answering it.
        app: server::new(api_impl, role),
        orchestration,
        upkeep: paused_upkeep,
        reporter: None,
        runtime: None,
        grpc: Some(grpc),
    })
}

/// The store settings this replica shares with the others, or why there are
/// none.
///
/// 🔴 The in-memory store is refused rather than accepted with a warning. A
/// warning at startup is read once, by whoever was watching; the failure it
/// would be warning about is two replicas each answering 404 for the other's
/// sandboxes, which is indistinguishable from a sandbox that was deleted.
fn cluster_store_config(
    config: &agentenv::cfg::OrchestratorStoreConfig,
) -> anyhow::Result<agentenv::orchestrator::RedisStoreConfig> {
    if !matches!(config.backend, MetadataStoreBackendKind::Redis) {
        anyhow::bail!(
            "--role api needs [orchestrator.store].backend = \"redis\" \
             (AENV_ORCHESTRATOR_STORE_BACKEND), and this process is configured for {:?}. The \
             in-memory store is one process's private ledger: an API replica using it would hold \
             an opinion about sandboxes no other replica shares, and the two would not disagree \
             visibly — each would simply answer 404 for the other's",
            config.backend.as_str()
        );
    }
    // 🔴 Four settings from configuration and the rest from the store's own
    // defaults, which is a decision and not laziness. `RedisStoreConfig` has
    // around twenty timing parameters whose *relationships* carry correctness
    // — `transition_key_ttl > wait_transition_timeout > lock_ttl`,
    // `stale_cutoff > transition_key_ttl`, `record_ttl_grace >
    // transition_key_ttl` — and `validate` refuses a combination that breaks
    // them. Exposing them individually would let a deployment set one and be
    // refused at startup for a reason about a different one.
    Ok(agentenv::orchestrator::RedisStoreConfig {
        url: config.redis_url.clone(),
        key_prefix: config.redis_key_prefix.clone(),
        distributed_lock_enabled: config.redis_distributed_lock_enabled,
        ..Default::default()
    })
}

/// The scheduler-backed placement this half asks where sandboxes go.
fn cluster_placement(
    config: &agentenv::cfg::ClusterConfig,
) -> anyhow::Result<Arc<dyn agentenv::node_client::NodePlacement>> {
    let endpoint = config
        .scheduler_endpoint
        .as_deref()
        .map(str::trim)
        .filter(|endpoint| !endpoint.is_empty())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "--role api needs [cluster].scheduler_endpoint \
                 (AENV_OBSERVABILITY_SCHEDULER_ENDPOINT): it owns sandboxes it does not run, so \
                 every create has to be placed by the scheduler and there is no machine here to \
                 fall back to"
            )
        })?;
    Ok(Arc::new(SchedulerNodePlacement::connect_lazy(
        endpoint,
        config.node_service_port,
    )?))
}

/// Binds a gRPC listener and spawns the server that answers on it.
///
/// 🔴 The bind happens here, in the assembly, and not inside the spawned task.
/// A `serve(addr, ..)` that binds inside its own future turns "the port is
/// already in use" into a task that ended: the process goes on serving HTTP,
/// the surface is unreachable, and from the outside that is the same picture as
/// a surface nobody is calling. See §15.4 ③ — a reading of zero that means two
/// different things.
async fn spawn_grpc_surface<F, Fut>(
    addr: &str,
    surface: &'static str,
    serve: F,
) -> anyhow::Result<(tokio::task::JoinHandle<()>, oneshot::Sender<()>)>
where
    F: FnOnce(tokio::net::TcpListener, GrpcShutdown) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<()>> + Send + 'static,
{
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("bind the {surface} to {addr}"))?;
    let bound = listener.local_addr().ok();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let serving = serve(
        listener,
        Box::pin(async move {
            let _ = shutdown_rx.await;
        }),
    );
    let task = tokio::spawn(async move {
        if let Err(err) = serving.await {
            // 🔴 `error`, not `warn`. Reaching here means the surface stopped
            // answering while the process kept running, which is the state this
            // whole arrangement exists to make impossible to reach quietly.
            tracing::error!(
                target: "agentenv",
                surface,
                error = %format_args!("{err:#}"),
                "a gRPC surface stopped serving"
            );
        }
    });
    info!(target: "agentenv", surface, addr = ?bound, "gRPC surface listening");
    Ok((task, shutdown_tx))
}

/// The stop signal a gRPC surface waits on.
///
/// Boxed so that [`spawn_grpc_surface`] can hand the same concrete type to
/// every `serve_on`, each of which takes an opaque `impl Future`.
type GrpcShutdown = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>;

/// Keeps this node's standing in the cluster registry current, in both
/// directions.
///
/// Outward, it renews the lease on every sandbox this node holds. That lease is
/// the only evidence the registry has that the node is still there, and letting
/// it lapse is what invites another node to take the sandbox over — so this
/// loop stopping is itself the signal that the node has gone.
///
/// Inward, a node that loses a sandbox to another node is never told about it:
/// the resume happens elsewhere, against a registry row this node does not
/// watch. Until it notices, it keeps the sandbox in its heartbeat roster, the
/// scheduler's binding for that sandbox flaps between the two nodes, and — if
/// the sandbox is still running here — two live copies of it write to their own
/// rootfs layers.
///
/// 🔴 **Two tasks, not one.** Reconciliation tears sandboxes down, and a
/// teardown waits on whatever operation currently holds the sandbox; one that
/// drags on would, in a shared loop, stop the renewals as well. The node would
/// then declare *all* of its own sandboxes abandoned while it was busy standing
/// one of them down, and other nodes would take them over. Renewal must not be
/// able to starve behind anything.
fn spawn_paused_record_upkeep(
    api_impl: Arc<ApiImpl>,
    interval: Duration,
) -> Vec<tokio::task::JoinHandle<()>> {
    let renewer = Arc::clone(&api_impl);
    let renew = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        // The startup pass already ran; skip the immediate first tick.
        ticker.tick().await;
        loop {
            ticker.tick().await;
            renewer.renew_paused_leases().await;
        }
    });

    let reconcile = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.tick().await;
        loop {
            ticker.tick().await;
            api_impl.reconcile_local_records().await;
            // Cluster-wide rather than node-local, and deliberately not on the
            // startup path: the rows it collects have been stranded for at
            // least a sandbox lifetime already, so nothing is gained by making
            // the listener wait for it.
            api_impl.reclaim_expired_sandboxes().await;
        }
    });

    vec![renew, reconcile]
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            info!(target: "agentenv", "received Ctrl+C, starting graceful shutdown");
        }
        _ = terminate => {
            info!(target: "agentenv", "received SIGTERM, starting graceful shutdown");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn the_cli_parses_and_defaults_the_role() {
        ServerCli::command().debug_assert();

        let bare = ServerCli::parse_from(["server"]);
        assert_eq!(bare.role, None, "no --role means fall through to AENV_ROLE");
        assert_eq!(
            ServerRole::from_env(None).unwrap(),
            ServerRole::All,
            "and with no AENV_ROLE set, to the process that has always run"
        );

        for spelling in ["api", "node", "all"] {
            let parsed = ServerCli::parse_from(["server", "--role", spelling]);
            assert_eq!(parsed.role.unwrap().as_str(), spelling);
        }

        assert!(
            ServerCli::try_parse_from(["server", "--role", "gateway"]).is_err(),
            "an unknown role is refused at parse time"
        );
    }

    /// 🔴 The two refusals `--role api` takes on its own configuration, each
    /// pushed up rather than assumed.
    ///
    /// Both are read before anything is connected, which is what makes them
    /// testable at all — and an untested refusal branch is the shape §15.3
    /// records: `guard_read_side`'s three refusals never ran outside a unit
    /// test, and one disjunct in one of them has never run at all.
    ///
    /// A version of this role that quietly fell back to the local orchestrator
    /// would be a process that reaches for `/dev/kvm` on a replica supposed to
    /// hold none of a machine's state — and on a host where that reach
    /// succeeded, it would work well enough to be believed.
    #[test]
    fn the_api_half_refuses_a_ledger_no_other_replica_can_see() {
        let mut config = AppConfig::default();
        assert_eq!(
            config.orchestrator.store.backend,
            MetadataStoreBackendKind::InMemory,
            "the default is the machine-local store, which is what makes this refusal necessary"
        );

        let err = cluster_store_config(&config.orchestrator.store)
            .expect_err("the in-memory store is one process's private ledger");
        let err = err.to_string();
        // The setting, the environment variable that overrides it, and what
        // goes wrong — an operator reading this in a CrashLoopBackOff has the
        // log line and nothing else.
        assert!(err.contains("orchestrator.store"), "{err}");
        assert!(err.contains("AENV_ORCHESTRATOR_STORE_BACKEND"), "{err}");
        assert!(err.contains("in-memory"), "{err}");
        // 🔴 And the spelling a deployment would have to write, quoted from
        // `as_str` rather than from prose. An operator reading this message has
        // to be able to copy the value out of it; a message that named the
        // backend in words only would be telling them what is wrong without
        // telling them what to type.
        assert!(
            err.contains(MetadataStoreBackendKind::InMemory.as_str()),
            "{err}"
        );
        assert_ne!(
            MetadataStoreBackendKind::InMemory.as_str(),
            MetadataStoreBackendKind::Redis.as_str(),
            "the two backends must not answer to the same name"
        );

        // 🔴 The control. Without it this test passes just as well against a
        // function that refuses every configuration, including the right one.
        //
        // 🔴 Every value set here differs from what `RedisStoreConfig::default()`
        // would supply, and that is the point rather than arbitrary. Only three
        // of that struct's ~twenty fields come from configuration; the rest
        // arrive through `..Default::default()`, so a field this function
        // forgot to carry would silently take the default — and if the test
        // used the default value, the assertion would agree with it.
        config.orchestrator.store.backend = MetadataStoreBackendKind::Redis;
        config.orchestrator.store.redis_url = "redis://cluster-redis:6379".to_string();
        config.orchestrator.store.redis_key_prefix = "agentenv:probe".to_string();
        config.orchestrator.store.redis_distributed_lock_enabled = false;

        let defaults = agentenv::orchestrator::RedisStoreConfig::default();
        assert_ne!(defaults.url, config.orchestrator.store.redis_url);
        assert_ne!(
            defaults.key_prefix,
            config.orchestrator.store.redis_key_prefix
        );
        assert!(defaults.distributed_lock_enabled);

        let store = cluster_store_config(&config.orchestrator.store)
            .expect("the cluster store is what this role is for");
        assert_eq!(store.url, "redis://cluster-redis:6379");
        assert_eq!(store.key_prefix, "agentenv:probe");
        assert!(!store.distributed_lock_enabled);
        // The settings that are deliberately *not* configurable still arrive,
        // and arrive at the values whose ordering `validate` checks.
        store
            .validate()
            .expect("the defaults this function leans on must be a valid combination");
    }

    // 🔴 `#[tokio::test]` rather than `#[test]`: the control probe at the end
    // builds a real lazy channel, and `connect_lazy` installs a hyper executor
    // that panics outside a runtime. Without the control the test would pass as
    // a plain `#[test]` — and would pass equally against a function that
    // refused every endpoint.
    #[tokio::test]
    async fn the_api_half_refuses_to_place_sandboxes_with_nothing_to_ask() {
        let mut config = AppConfig::default();
        assert_eq!(
            config.cluster.scheduler_endpoint, None,
            "the default is no endpoint, which is what makes this refusal necessary"
        );

        let err = match cluster_placement(&config.cluster) {
            Ok(_) => panic!("there is no machine here to fall back to"),
            Err(err) => err.to_string(),
        };
        assert!(err.contains("scheduler_endpoint"), "{err}");
        assert!(
            err.contains("AENV_OBSERVABILITY_SCHEDULER_ENDPOINT"),
            "{err}"
        );

        // 🔴 Blank is the same answer as absent, and separately so: a
        // ConfigMap that carries the key with an empty value is not naming an
        // endpoint, and treating it as one would produce a placement source
        // that fails on every call instead of a process that refuses to start.
        config.cluster.scheduler_endpoint = Some("   ".to_string());
        assert!(
            cluster_placement(&config.cluster).is_err(),
            "a blank endpoint is not an endpoint"
        );

        // 🔴 The control, again: a real endpoint resolves, so the two refusals
        // above are about what was missing.
        config.cluster.scheduler_endpoint = Some("http://scheduler:9090".to_string());
        assert!(
            cluster_placement(&config.cluster).is_ok(),
            "a configured endpoint is what this role runs on"
        );
    }

    /// 🔴 `--role all` opens one listener, and this is the assertion that says
    /// so where somebody adding a second one will trip over it.
    ///
    /// The rollback target is defined as the process that ran before the split.
    /// A second socket is not a behaviour that can be argued inert: it is a
    /// port bound on every node in the fleet, and the startup-sequence gate
    /// would have to grow an entry claiming otherwise.
    #[test]
    fn only_the_split_roles_bind_a_second_listener() {
        let source = include_str!("server.rs");
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

        assert!(
            !body("async fn assemble_all(").contains("spawn_grpc_surface"),
            "--role all binds a second listener. It is the rollback target and is defined as \
             today's behaviour verbatim; the node service belongs to --role node"
        );
        // 🔴 The control probe. Both halves of the split do bind one, so the
        // assertion above is about `all` rather than about a helper that has
        // been renamed out from under this test.
        assert!(
            body("async fn assemble_node(").contains("spawn_grpc_surface"),
            "--role node no longer serves the node sandbox service, and nothing else does"
        );
        assert!(
            body("async fn assemble_api(").contains("spawn_grpc_surface"),
            "--role api no longer serves the wake-up surface, and the gateway's cold path has \
             nowhere to ask"
        );
    }

    /// 🔴 Guards the shutdown bounds `NodeRuntime::shutdown` and `main` are
    /// each responsible for. Every one of the three calls this asserts on can
    /// be deleted without a single one of this binary's other tests noticing
    /// — nothing exercises the real graceful-shutdown path under test, the
    /// same gap `only_the_split_roles_bind_a_second_listener` closes for the
    /// `--role` split — so this scans the source text directly, the same way
    /// that test does.
    ///
    /// See [`RUNTIME_SHUTDOWN_TIMEOUT`]'s doc for why `main`'s call matters —
    /// without it a stuck `spawn_blocking` closure (RocksDB background
    /// compaction/flush, observed on nodes that had actually run a VM) hangs
    /// the process well past `terminationGracePeriodSeconds` — and
    /// [`NodeRuntime::shutdown`]'s own doc for why the two store closes come
    /// before that backstop rather than relying on it.
    #[test]
    fn the_shutdown_bounds_are_still_wired() {
        let source = include_str!("server.rs");
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

        let shutdown = body("async fn shutdown(self)");
        assert!(
            shutdown.contains("close_image_cache_stores"),
            "NodeRuntime::shutdown no longer closes the image cache metadata store's RocksDB \
             handle before process exit"
        );
        assert!(
            shutdown.contains("close_stores"),
            "NodeRuntime::shutdown no longer closes the snapshot manager's RocksDB stores \
             before process exit"
        );

        let main = body("fn main() -> anyhow::Result<()>");
        assert!(
            main.contains("shutdown_timeout"),
            "main no longer bounds Runtime::shutdown_timeout after block_on returns — an \
             un-bounded fallback here reintroduces the node-never-exits hang"
        );
    }
}
