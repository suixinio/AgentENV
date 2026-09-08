#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// Purge jemalloc dirty/muzzy pages after 1s so burst allocations do not linger
// as retained RSS; background purging is enabled by the crate feature.
#[used]
#[allow(non_upper_case_globals)]
#[export_name = "malloc_conf"]
pub static malloc_conf: &[u8] = b"dirty_decay_ms:1000,muzzy_decay_ms:1000,background_thread:true\0";
use std::sync::{Arc, RwLock};
use std::time::Duration;

use aenv_node::api::{proxy, server, NodeApi};
use aenv_node::cfg::{validate_node_half, AppConfig, NodeConfigExt};
use aenv_node::identity::NodeIdentity;
use aenv_node::image::ImageResolver;
use aenv_node::observability::{ObservabilityReporter, ObservabilityService};
use aenv_node::orchestrator::{
    InMemoryMetadataStore, Orchestrator, SandboxOrchestration, StagingPausePublisher,
};
use aenv_node::overlaybd::OverlaybdP2pRuntime;
use aenv_node::p2p::P2pTransport;
use aenv_node::sandbox::{FirecrackerPool, FirecrackerSandboxFactory, UblkDeviceManager};
use aenv_node::server_main::{
    self, spawn_grpc_surface, Assembly, HeartbeatReporter, ProcessRuntime,
};
use aenv_node::snapshot::SnapshotManager;
use aenv_node::template::TemplateBuilder;
use anyhow::Context as _;
use clap::Parser;
use tracing::{info, warn};

type LocalOrchestrator = Orchestrator<InMemoryMetadataStore, FirecrackerSandboxFactory>;

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

/// Per-step shutdown bound; the runtime shutdown timeout remains the backstop.
const NODE_RUNTIME_SHUTDOWN_STEP_TIMEOUT: Duration = Duration::from_secs(15);

/// Machine-local runtime resources, kept together to preserve teardown order.
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

        // Both P2P shutdowns may wait indefinitely on downstream actors.
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
    }
}

struct NodeCore {
    orchestrator: Arc<LocalOrchestrator>,
    snapshot_manager: Arc<SnapshotManager>,
    template_builder: Arc<TemplateBuilder>,
    image_resolver: Arc<ImageResolver>,
    observability: Option<Arc<ObservabilityService>>,
    reporter: Option<ObservabilityReporter>,
    identity: NodeIdentity,
    runtime: NodeRuntime,
}

// Bound the wait on Tokio's blocking pool: a plain runtime drop waits forever
// for whatever blocking work is still in flight.
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

    let cli = NodeCli::parse();
    let config_manager = if let Some(config_path) = cli.config.as_deref() {
        aenv_node::cfg::ConfigManager::init_global_from_path(config_path, validate_node_half)?
    } else {
        aenv_node::cfg::ConfigManager::init_global(validate_node_half)?
    };
    let config = config_manager.config();

    // Refuse database credentials on a machine that runs user code.
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

/// Refuses PostgreSQL credentials in node configuration.
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

/// Brings up the runtime needed to execute sandboxes on this node.
async fn assemble_node_core(config: &AppConfig) -> anyhow::Result<NodeCore> {
    aenv_node::privileges::require_runtime_capabilities()?;
    aenv_node::privileges::clear_ambient_capabilities()?;

    // Reclaim VMMs before environment setup unlinks stale network namespaces.
    aenv_node::node_reclaim::run(config).await;

    let identity = NodeIdentity::from_config(&config.node_identity);
    let identity_for_registry = identity.clone();
    let p2p_transport = aenv_node::p2p::transport_from_config(config, &identity).await?;
    let p2p_local_endpoint = p2p_transport.local_endpoint();
    let overlaybd_p2p =
        OverlaybdP2pRuntime::start_from_app_config(config, Arc::clone(&p2p_transport)).await;

    aenv_node::setup::ensure_environment(config, overlaybd_p2p.read_facade_address()).await?;

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
    // Resolution and advertisement share a transport, but only this node
    // advertises the snapshot bytes it writes.
    let snapshot_advertiser = snapshot_p2p_transport.clone().map(|transport| {
        Arc::new(aenv_node::snapshot::P2pSnapshotAdvertiser::new(transport))
            as Arc<dyn aenv_node::snapshot::SnapshotArtifactAdvertiser>
    });
    // Nodes stage snapshot bytes; only the API half writes catalog rows.
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
    // Reporting always computes the cluster CPU template; configuration only
    // controls whether cold boots apply it.
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
    let image_refs = aenv_node::image::local_runtime_image_refs();
    let orchestrator = Orchestrator::with_in_memory_store_and_factory(factory, image_refs).await?;
    orchestrator.set_grant_issuer(aenv_core::orchestrator::GrantsIssuedUpstream::shared());
    let observability_config = &config.observability;
    let observability = if observability_config.enabled {
        Some(Arc::new(
            ObservabilityService::new(
                identity,
                Arc::clone(&orchestrator) as Arc<dyn SandboxOrchestration>,
                config.resolved_cpu_template_helper(),
                cluster_cpu_arc,
            )
            .await
            .with_egress_broker_probe(Arc::new(|| {
                aenv_node::sandbox::egress::EgressRuntime::global()
                    .map(|runtime| runtime.state())
                    .unwrap_or(aenv_node::observability::EgressBrokerState::Disabled)
            })),
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

/// Assembles the sandbox-running node half with no cluster ownership decisions.
async fn assemble_node(config: &AppConfig) -> anyhow::Result<Assembly> {
    let core = assemble_node_core(config).await?;

    // A pause stages its capture on this node's repository; the api half
    // commits the staged value.
    core.orchestrator
        .set_pause_publisher(Arc::new(StagingPausePublisher::new(Arc::clone(
            &core.snapshot_manager,
        ))));
    let orchestration: Arc<dyn SandboxOrchestration> =
        Arc::clone(&core.orchestrator) as Arc<dyn SandboxOrchestration>;

    // Bind before spawning so a port conflict fails process assembly.
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

    let node_api = Arc::new(NodeApi::new(
        Arc::clone(&orchestration),
        core.observability,
        config.sandbox_proxy.domains.clone(),
    ));

    Ok(Assembly {
        app: server::new(
            Arc::clone(&node_api),
            proxy::data_plane(Arc::clone(&node_api)),
        ),
        orchestration,
        upkeep: Vec::new(),
        pg_singleton_tasks: Vec::new(),
        reporter: core
            .reporter
            .map(|reporter| Box::new(reporter) as Box<dyn HeartbeatReporter>),
        runtime: Some(Box::new(core.runtime)),
        drains_on_shutdown: true,
        grpc: Some(grpc),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

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

    #[test]
    fn a_configured_pg_dsn_is_refused_and_an_absent_one_is_not() {
        let error = refuse_configured_pg_dsn(Some("postgres://user:pw@db.internal:5432/agentenv"))
            .expect_err("a node handed a DSN must not start");
        let message = format!("{error:#}");
        assert!(message.contains("[pg].dsn"), "{message}");
        assert!(message.contains("aenv-node"), "{message}");
        assert!(!message.contains("pw@"), "{message}");

        assert!(refuse_configured_pg_dsn(None).is_ok());
    }

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

        let main = body("fn main() -> anyhow::Result<()>");
        assert!(
            main.contains("shutdown_timeout"),
            "main no longer bounds Runtime::shutdown_timeout after block_on returns — an \
             un-bounded fallback here reintroduces the node-never-exits hang"
        );
    }

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
            // Match the call expression; the bare identifier appears elsewhere.
            async_main.contains("refuse_configured_pg_dsn(config."),
            "async_main no longer calls refuse_configured_pg_dsn — a [pg].dsn could reach a \
             node with nothing left to refuse it, even though the pure function's own two arms \
             would still report green"
        );
        // Mutation control: the scan must exclude the function definition.
        assert!(
            !async_main.contains("refuse_configured_pg_dsn(dsn"),
            "the scan is reading something other than async_main's body — \
             `refuse_configured_pg_dsn(dsn` is the definition's own parameter list, which is \
             outside it"
        );
    }
}
