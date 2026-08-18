use std::sync::{Arc, RwLock};
use std::time::Duration;

use agentenv::api::{server, ApiImpl, PausedSandboxWiring};
use agentenv::identity::NodeIdentity;
use agentenv::image::ImageResolver;
use agentenv::observability::{ObservabilityReporter, ObservabilityService};
use agentenv::orchestrator::{build_paused_registry, Orchestrator};
use agentenv::overlaybd::OverlaybdP2pRuntime;
use agentenv::sandbox::{FirecrackerPool, FirecrackerSandboxFactory, UblkDeviceManager};
use agentenv::snapshot::SnapshotManager;
use agentenv::template::TemplateBuilder;
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

#[derive(Debug, Parser)]
#[command(name = "agentenv server")]
struct ServerCli {
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    agentenv::logging::init();
    agentenv_observability::init_prometheus_recorder()?;

    let cli = ServerCli::parse();
    let config_manager = if let Some(config_path) = cli.config.as_deref() {
        agentenv::cfg::ConfigManager::init_global_from_path(config_path)?
    } else {
        agentenv::cfg::ConfigManager::init_global()?
    };
    let config = config_manager.config();

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

    agentenv::privileges::require_runtime_capabilities()?;
    agentenv::privileges::clear_ambient_capabilities()?;

    let addr = std::env::var("API_ADDR").unwrap_or_else(|_| "0.0.0.0:8000".to_string());
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
    let snapshot_manager = Arc::new(SnapshotManager::new(snapshot_p2p_transport)?);
    let cluster_cpu_arc: Arc<RwLock<Option<String>>> = Arc::new(RwLock::new(None));
    let template_builder = Arc::new(TemplateBuilder::with_cpu_config(Arc::clone(
        &cluster_cpu_arc,
    )));
    let image_resolver = Arc::new(ImageResolver::new(config));
    let factory = FirecrackerSandboxFactory::with_cpu_config(Arc::clone(&cluster_cpu_arc));
    let orchestrator = Orchestrator::with_file_backed_store_and_factory(factory).await?;
    let observability_config = &config.observability;
    let observability = if observability_config.enabled {
        Some(Arc::new(
            ObservabilityService::new(
                identity,
                Arc::clone(&orchestrator),
                config.resolved_cpu_template_helper(),
                cluster_cpu_arc,
            )
            .await,
        ))
    } else {
        None
    };
    let mut reporter = if let Some(service) = observability.as_ref() {
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

    let paused_registry =
        build_paused_registry(&config.orchestrator.paused_registry, &identity_for_registry).await?;
    let paused_wiring = PausedSandboxWiring::new(
        paused_registry,
        Arc::clone(&snapshot_manager),
        &identity_for_registry,
    );
    // The orchestrator publishes every pause it performs, including the ones no
    // API request asked for (expiry, shutdown).
    orchestrator.set_paused_publisher(paused_wiring.publisher());
    let api_impl = Arc::new(ApiImpl::new(
        Arc::clone(&orchestrator),
        snapshot_manager,
        template_builder,
        image_resolver,
        observability,
        paused_wiring,
        config.sandbox_proxy.domains.clone(),
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
    api_impl.release_stale_node_holdings().await;
    api_impl.renew_paused_leases().await;
    api_impl.reconcile_local_records().await;
    let paused_upkeep = spawn_paused_record_upkeep(
        Arc::clone(&api_impl),
        config.orchestrator.paused_registry.reconcile_interval(),
    );

    let app = server::new(api_impl);
    let shutdown_orchestrator = Arc::clone(&orchestrator);
    let drain_orchestrator = Arc::clone(&orchestrator);
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
            for task in &paused_upkeep {
                task.abort();
            }
            info!(target: "agentenv", "stopping sandboxes before process exit");
            if let Err(err) = shutdown_orchestrator.shutdown().await {
                warn!(target: "agentenv", error = %err, "error occurred while shutting down orchestrator");
            }
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
            info!(target: "agentenv", "shutting down overlaybd p2p runtime");
            if let Err(err) = overlaybd_p2p.shutdown().await {
                warn!(target: "agentenv", error = %err, "error occurred while shutting down overlaybd p2p runtime");
            }
            info!(target: "agentenv", "shutting down p2p transport");
            if let Err(err) = p2p_transport.shutdown().await {
                warn!(target: "agentenv", error = %err, "error occurred while shutting down p2p transport");
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
            if drain_orchestrator.set_scheduling_disabled(true) {
                info!(
                    target: "agentenv",
                    wait_secs = drain_propagation.as_secs(),
                    "isolated the node for shutdown; waiting for the scheduler to notice"
                );
                if !drain_propagation.is_zero() {
                    tokio::time::sleep(drain_propagation).await;
                }
            }

            let _ = shutdown_tx.send(());
        })
        .await?;

    shutdown_cleanup.await?;

    Ok(())
}

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
