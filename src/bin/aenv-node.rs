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

use agentenv::api::{server, ApiImpl, PausedSandboxWiring, ResumeWiring};
use agentenv::cfg::{AppConfig, PausedRegistryBackendKind};
use agentenv::identity::NodeIdentity;
use agentenv::image::ImageResolver;
use agentenv::observability::{ObservabilityReporter, ObservabilityService};
use agentenv::orchestrator::{
    DisabledPausedSandboxRegistry, FileBackedSandboxPersister, InMemoryMetadataStore, Orchestrator,
    SandboxOrchestration,
};
use agentenv::overlaybd::OverlaybdP2pRuntime;
use agentenv::p2p::P2pTransport;
use agentenv::role::ServerRole;
use agentenv::sandbox::{FirecrackerPool, FirecrackerSandboxFactory, UblkDeviceManager};
use agentenv::server_main::{self, spawn_grpc_surface, Assembly, ProcessRuntime};
use agentenv::snapshot::SnapshotManager;
use agentenv::template::TemplateBuilder;
use anyhow::Context as _;
use clap::Parser;
use tracing::{info, warn};

/// The orchestrator a machine-local role assembles: this node's own ledger,
/// this node's Firecracker, this node's files.
type LocalOrchestrator =
    Orchestrator<InMemoryMetadataStore, FirecrackerSandboxFactory, FileBackedSandboxPersister>;

/// 🔴 `--role` is accepted and checked, not obeyed. See
/// [`ServerRole::confirm`]: this binary *is* the node half, because it is the
/// one that links a sandbox runtime, and a `--role api` here names the other
/// binary rather than changing what this one does.
#[derive(Debug, Parser)]
#[command(name = "aenv-node")]
struct NodeCli {
    /// Which half of the split this process runs. Only `node` is accepted
    /// here; also read from AENV_ROLE.
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

/// 🔴 Not `#[tokio::main]`. That macro's generated `main` builds the runtime,
/// `block_on`s the async body, then lets the `Runtime` value fall out of
/// scope — and `Runtime`'s `Drop` shuts down its blocking-task pool by calling
/// `BlockingPool::shutdown(None)` (tokio, `runtime/blocking/pool.rs`), and
/// `None` means *no timeout*: it waits forever for every `spawn_blocking`
/// closure that has already started to return.
///
/// Every RocksDB store this process opens (`LocalKvStore`, see
/// `agentenv::local_store`) does its writes, and its own background
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
    agentenv::logging::init();
    agentenv_observability::init_prometheus_recorder()?;

    let cli = NodeCli::parse();
    let role = ServerRole::Node.confirm(cli.role)?;
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

    // 🔴 Security invariant, and now a belt over a brace. This binary cannot
    // link `sqlx` at all — `cargo tree -p aenv-node -e normal | grep sqlx` is
    // empty, which is a stronger statement than any check could make. What this
    // still catches is *configuration*: an operator who leaves `[pg].dsn` in a
    // node's ConfigMap has put a database credential on a machine that runs
    // user code, whether or not anything in the process could use it.
    role.check_pg_dsn(config.pg.as_ref().and_then(agentenv::cfg::PgConfig::dsn))?;

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
    let assembly = assemble_node(config).await?;
    server_main::serve(role, config, assembly).await
}

/// Brings up everything this machine needs to run sandboxes.
///
/// 🔴 Took a `role` and a `pg_pool` while `--role all` shared it. Both are
/// gone: this binary is one role, and it holds no PostgreSQL pool — which is
/// now a fact about which crates it links rather than an argument it is
/// trusted to pass `None` for.
async fn assemble_node_core(config: &AppConfig) -> anyhow::Result<NodeCore> {
    let role = ServerRole::Node;
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
    // 🔴 Two handles on the same transport, and they answer different
    // questions. The first is how the *resolver* fetches a snapshot's fixed
    // artifacts from a peer; the second is how this node offers the ones it
    // just wrote. Only the machine that holds bytes has anything to offer, so
    // only this assembly builds an advertiser — `assemble_api` passes `None`.
    let snapshot_advertiser = snapshot_p2p_transport.clone().map(|transport| {
        Arc::new(agentenv::snapshot::P2pSnapshotAdvertiser::new(transport))
            as Arc<dyn agentenv::snapshot::SnapshotArtifactAdvertiser>
    });
    // 🔴 `None` for the PostgreSQL parts, unconditionally and by construction:
    // this half never holds a `[pg]` pool (see `agentenv::pg`'s own module
    // doc and `ServerRole::check_pg_dsn`), so there is nothing for it to build
    // a central catalog out of.
    let snapshot_backend = agentenv::snapshot::repository::backends::build_snapshot_backend(
        snapshot_p2p_transport,
        None,
        role,
    )
    .await?;
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
    let image_refs = agentenv::image::local_runtime_image_refs();
    let orchestrator =
        Orchestrator::with_file_backed_store_and_factory(role, factory, image_refs).await?;
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
    // 🔴 This binary has no `build_pg_pool` to call and no pool type to name.
    // `--role node` must never hold PostgreSQL credentials — see `src/pg/mod.rs`'s own doc
    // comment — and this is that invariant enforced by construction here,
    // not only by `ServerRole::check_pg_dsn` at startup.
    let core = assemble_node_core(config).await?;

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
        let image_resolver = Arc::clone(&core.image_resolver);
        let template_builder = Arc::clone(&core.template_builder);
        spawn_grpc_surface(
            &config.cluster.node_service_addr,
            "node sandbox service",
            move |listener, shutdown| {
                agentenv::node_server::serve_on(
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
        pg_singleton_tasks: Vec::new(),
        reporter: core.reporter,
        runtime: Some(Box::new(core.runtime)),
        grpc: Some(grpc),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    /// 🔴 `--role` no longer selects anything; it confirms. See
    /// [`ServerRole::confirm`] and `role::confirm_tests` for the arms.
    #[test]
    fn the_node_binary_is_the_node_half() {
        NodeCli::command().debug_assert();

        let bare = NodeCli::parse_from(["aenv-node"]);
        assert_eq!(bare.role, None, "no --role means this binary's own half");

        for spelling in ["api", "node", "all"] {
            let parsed = NodeCli::parse_from(["aenv-node", "--role", spelling]);
            assert_eq!(
                parsed.role.unwrap().as_str(),
                spelling,
                "every role still parses — it is confirmed, not selected, one layer up"
            );
        }
        assert!(
            NodeCli::try_parse_from(["aenv-node", "--role", "gateway"]).is_err(),
            "an unknown role is refused at parse time"
        );

        assert_eq!(
            ServerRole::Node.confirm_with(bare.role, None).unwrap(),
            ServerRole::Node
        );
        assert!(
            ServerRole::Node
                .confirm_with(Some(ServerRole::Api), None)
                .is_err(),
            "this binary links a sandbox runtime; it cannot be the api half"
        );
    }

    /// 🔴 Guards the shutdown bounds `NodeRuntime::shutdown` and `main` are
    /// each responsible for. Every one of the three calls this asserts on can
    /// be deleted without a single one of this binary's other tests noticing
    /// — nothing exercises the real graceful-shutdown path under test — so
    /// this scans the source text directly.
    ///
    /// 🔴 Kept, and kept *here*, through the crate split. What it guards is a
    /// RocksDB `spawn_blocking` closure outliving the async body and hanging
    /// the process past `terminationGracePeriodSeconds` — the real defect
    /// behind "every DaemonSet rollout waits the full hour". That has nothing
    /// to do with which half this process is, so no dependency graph can prove
    /// it; and it was only ever observed on a node that had actually run a VM,
    /// which is this binary.
    ///
    /// See [`server_main::RUNTIME_SHUTDOWN_TIMEOUT`]'s doc for why `main`'s call matters —
    /// without it a stuck `spawn_blocking` closure (RocksDB background
    /// compaction/flush, observed on nodes that had actually run a VM) hangs
    /// the process well past `terminationGracePeriodSeconds` — and
    /// [`NodeRuntime::shutdown`]'s own doc for why the two store closes come
    /// before that backstop rather than relying on it.
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

    /// `ServerRole::check_pg_dsn`'s own tests (`src/role.rs`) only exercise
    /// the pure function directly — none of them can notice if the one call
    /// site that actually wires it into the running process disappears. That
    /// call site is the entire enforcement of "a `[pg].dsn` must never reach
    /// `--role node`": delete it and every test in `src/role.rs` stays green
    /// while the invariant it guards is gone. Scans this file's own source
    /// text for the call, the same way
    /// `only_the_split_roles_bind_a_second_listener` does for
    /// `spawn_grpc_surface`, so deleting the call site fails a test instead
    /// of only a future security review.
    #[test]
    fn async_main_actually_calls_check_pg_dsn() {
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

        assert!(
            // 🔴 Matches the call *expression* (`role.check_pg_dsn(`), not the
            // bare identifier `check_pg_dsn`. This file's own doc comment on
            // the call site (`See \`agentenv::pg\` and \`ServerRole::check_pg_dsn\`.`)
            // contains that bare identifier too — deleting the call while
            // leaving the comment behind kept a bare-identifier assertion
            // green, which is exactly backwards for a positive assertion:
            // a false match here hides the invariant's enforcement going
            // missing rather than merely giving a false alarm. No comment or
            // string literal in this file spells the call expression itself.
            body("async fn async_main() -> anyhow::Result<()>").contains("role.check_pg_dsn("),
            "async_main no longer calls ServerRole::check_pg_dsn — a [pg].dsn could reach \
             --role node with nothing left to refuse it, even though src/role.rs's own tests \
             of the pure function would still report green"
        );
    }
}
