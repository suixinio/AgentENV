#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// Purge dirty/muzzy pages after 1s on background threads so burst allocations
// do not linger as retained RSS.
#[used]
#[allow(non_upper_case_globals)]
#[export_name = "malloc_conf"]
pub static malloc_conf: &[u8] = b"dirty_decay_ms:1000,muzzy_decay_ms:1000,background_thread:true\0";
use std::sync::{Arc, RwLock};
use std::time::Duration;

use aenv_api::api::{server, ApiImpl, ResumeWiring};
use aenv_api::binding_store::{
    BindingStore, BindingStoreSettings, RedisBindingStore, RedisBindingStoreConfig,
};
use aenv_api::cfg::{
    AppConfig, BindingStoreConfig, ClusterNodeRegistryStoreConfig, MetadataStoreBackendKind,
    NodeRegistryObservedBackendKind,
};
use aenv_api::identity::NodeIdentity;
use aenv_api::node_client::{NativeNodePlacement, RemoteSandboxBackendFactory};
use aenv_api::node_registry::grpc_service::NodeRegistryGrpcService;
use aenv_api::node_registry::kubernetes_discovery::{
    validate_optional_pod_selector, KubernetesDiscovery, KubernetesDiscoveryConfig,
};
use aenv_api::node_registry::redis::{
    run_shared_observed_sync, SharedObservedStore, SharedObservedStoreConfig, DEFAULT_PULL_INTERVAL,
};
use aenv_api::node_registry::registry::{AtomicNodeRegistry, NodeRegistry};
use aenv_api::node_registry::warmup::WarmupGate;
use aenv_api::observability::ObservabilityService;
use aenv_api::orchestrator::{
    CommittingPausePublisher, Orchestrator, RedisMetadataStore, SandboxOrchestration,
};
use aenv_api::pg::{self, PgPoolSettings};
use aenv_api::server_main::{self, spawn_grpc_surface, Assembly};
use aenv_api::snapshot::SnapshotManager;
use anyhow::Context as _;
use clap::Parser;
use tracing::{info, warn};

#[derive(Debug, Parser)]
#[command(name = "aenv-api")]
struct ApiCli {
    /// Path to config file (same as AENV_CONFIG_PATH).
    #[arg(long)]
    config: Option<std::path::PathBuf>,
}

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
    aenv_api::logging::init();
    agentenv_observability::init_prometheus_recorder()?;

    let cli = ApiCli::parse();
    let config_manager = if let Some(config_path) = cli.config.as_deref() {
        aenv_api::cfg::ConfigManager::init_global_from_path(config_path)?
    } else {
        aenv_api::cfg::ConfigManager::init_global()?
    };
    let config = config_manager.config();

    info!(target: "agentenv", "assembling the api half");
    let assembly = assemble_api(config).await?;
    server_main::serve(config, assembly).await
}

async fn build_pg_pool(config: &AppConfig) -> anyhow::Result<sqlx::PgPool> {
    let settings = PgPoolSettings::from_config(config.pg.as_ref())?.context(
        "[pg] is required for aenv-api: [pg].dsn is unset or blank, and PostgreSQL is the only \
         snapshot catalog there is (object storage holds byte artifacts alone). Without it this \
         replica would start with no catalog, and \
         every snapshot, template and paused-sandbox request would have nowhere to read or write \
         a row. Set [pg].dsn for this half — it is TOML-file-only, with no environment binding \
         (confique cannot descend into AppConfig::pg's Option), so supply it through the file \
         AENV_CONFIG_PATH names or an AENV_CONFIG_OVERLAY_PATH overlay, the way \
         deploy/k8s/base's pg-dsn.toml and deploy/docker-compose.yml's /tmp/agentenv-pg/\
         pg-dsn.toml both do",
    )?;
    let pool = pg::connect(&settings).await?;
    // Catalog tables must exist before reapers or catalog clients start.
    aenv_api::snapshot::repository::backends::migrate_catalog_schema(&pool).await?;
    Ok(pool)
}

fn reaper_cadence(config: &AppConfig) -> (std::time::Duration, std::time::Duration) {
    let heartbeat_interval = config.snapshot.catalog.build_heartbeat_interval_secs.max(1);
    let interval = std::time::Duration::from_secs(30);
    let ttl = std::time::Duration::from_secs(heartbeat_interval.saturating_mul(3));
    (interval, ttl)
}

fn spawn_pg_singleton_tasks(
    config: &AppConfig,
    pg_pool: sqlx::PgPool,
) -> Vec<aenv_api::pg::SingletonTaskHandle> {
    let (interval, ttl) = reaper_cadence(config);
    let identity = aenv_api::identity::NodeIdentity::from_config(&config.node_identity);
    aenv_api::snapshot::repository::backends::spawn_catalog_build_reaper(
        Some(pg_pool),
        identity.cluster_id,
        interval,
        ttl,
    )
    .into_iter()
    .collect()
}

async fn assemble_api(config: &AppConfig) -> anyhow::Result<Assembly> {
    let identity = NodeIdentity::from_config(&config.node_identity);
    let store_config = cluster_store_config(&config.orchestrator.store)?;

    let NativeNodeRegistryBits {
        registry: native_registry_handle,
        warmup: native_warmup_handle,
        grpc_service: node_registry_grpc_service,
        tasks: mut node_registry_upkeep,
    } = start_native_node_registry(&config.cluster).await?;
    let binding_store = build_binding_store(&config.binding_store).await?;
    let binding_store_handle = Arc::clone(&binding_store);
    let max_projection_ttl = Duration::from_secs(config.binding_store.max_projection_ttl_secs);
    let artifact_store: Arc<dyn aenv_api::binding_store::artifact_index::ArtifactStore> = Arc::new(
        aenv_api::binding_store::artifact_index::InMemoryArtifactStore::new(
            config.binding_store.artifact_index_capacity as usize,
        ),
    );
    let node_registry_grpc_service = node_registry_grpc_service
        .with_binding_store(
            binding_store,
            config.binding_store.projection_authoritative,
            max_projection_ttl,
        )
        .with_artifact_store(artifact_store);
    if let Some(task) = wire_shared_node_observed_store(
        &config.cluster.node_registry_store,
        &native_registry_handle,
    )
    .await?
    {
        node_registry_upkeep.push(task);
    }
    let pg_pool = build_pg_pool(config).await?;
    let pg_singleton_tasks = spawn_pg_singleton_tasks(config, pg_pool.clone());
    let placement = cluster_placement(
        &native_registry_handle,
        config.cluster.node_service_port,
        &native_warmup_handle,
        &node_registry_grpc_service,
    );
    let store = RedisMetadataStore::connect(store_config)
        .await
        .context("connect the cluster metadata store")?;
    let orchestrator = Orchestrator::new(
        aenv_api::sandbox::AccessTokenSeedPolicy::MustBeConfigured,
        store,
        RemoteSandboxBackendFactory::new(Arc::clone(&placement)),
        aenv_api::image::DisabledRuntimeImageRefs::shared(),
    )
    .await?;
    let orchestration: Arc<dyn SandboxOrchestration> =
        Arc::clone(&orchestrator) as Arc<dyn SandboxOrchestration>;

    let pg_catalog =
        Some(aenv_api::snapshot::repository::backends::pg_snapshot_catalog(config, &pg_pool));
    let snapshot_backend = aenv_api::snapshot::repository::backends::build_snapshot_backend(
        aenv_api::snapshot::repository::backends::build_catalog_only_storage(config)?,
        pg_catalog,
        aenv_api::snapshot::repository::backends::CentralCatalogUse::AsConfigured,
    )?;
    let snapshot_manager = Arc::new(SnapshotManager::from_assembled(snapshot_backend, None));
    // API replicas receive metrics but must not report themselves as schedulable nodes.
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

    // A pause's staged snapshot is committed here, making the sandbox
    // resumable anywhere.
    orchestrator.set_pause_publisher(Arc::new(CommittingPausePublisher::new(Arc::clone(
        &snapshot_manager,
    ))));

    let api_impl = Arc::new(
        ApiImpl::new(
            Arc::clone(&orchestration),
            snapshot_manager,
            observability,
            config.sandbox_proxy.domains.clone(),
            // Clone only after the binding and artifact builders.
            ResumeWiring::cluster_in_process(node_registry_grpc_service.clone()),
        )
        .with_node_placement(placement)
        // This half observes the cluster, so `/nodes` reports the fleet rather
        // than the one replica answering the request.
        .with_node_fleet(
            Arc::clone(&native_registry_handle) as Arc<dyn NodeRegistry>,
            config.cluster.node_service_port,
        ),
    );

    let mut upkeep = node_registry_upkeep;
    if config.binding_store.sweep_enabled {
        let cluster_id = config.node_identity.cluster_id.clone().unwrap_or_default();
        let sweeper = Arc::new(aenv_api::binding_store::sweep::BindingSweeper::new(
            cluster_id,
            Duration::from_secs(config.binding_store.sweep_silence_secs),
        ));
        let registry: Arc<dyn NodeRegistry> =
            Arc::clone(&native_registry_handle) as Arc<dyn NodeRegistry>;
        let store = Arc::clone(&binding_store_handle);
        let interval = Duration::from_secs(config.binding_store.sweep_interval_secs);
        upkeep.push(tokio::spawn(async move {
            sweeper.run(registry, store, interval).await;
        }));
    }

    let grpc = {
        let served = Arc::clone(&api_impl);
        spawn_grpc_surface(
            &config.cluster.api_grpc_addr,
            "sandbox resume service",
            move |listener, shutdown| {
                aenv_api::api::grpc::serve_on(
                    listener,
                    served,
                    node_registry_grpc_service,
                    shutdown,
                )
            },
        )
        .await?
    };

    // Start the warm-up timeout when the heartbeat listener is actually bound.
    native_warmup_handle.rebase_deadline(
        std::time::SystemTime::now(),
        Duration::from_secs(config.cluster.native_warmup_timeout_secs),
    );

    Ok(Assembly {
        app: server::new_control_plane_only(api_impl),
        orchestration,
        upkeep,
        pg_singleton_tasks,
        reporter: None,
        runtime: None,
        drains_on_shutdown: false,
        grpc: Some(grpc),
    })
}

fn cluster_store_config(
    config: &aenv_api::cfg::OrchestratorStoreConfig,
) -> anyhow::Result<aenv_api::orchestrator::RedisStoreConfig> {
    if !matches!(config.backend, MetadataStoreBackendKind::Redis) {
        anyhow::bail!(
            "aenv-api needs [orchestrator.store].backend = \"redis\" \
             (AENV_ORCHESTRATOR_STORE_BACKEND), and this process is configured for {:?}. The \
             in-memory store is one process's private ledger: an API replica using it would hold \
             an opinion about sandboxes no other replica shares, and the two would not disagree \
             visibly — each would simply answer 404 for the other's",
            config.backend.as_str()
        );
    }
    // Store-owned timing defaults preserve `RedisStoreConfig::validate` ordering invariants.
    Ok(aenv_api::orchestrator::RedisStoreConfig {
        url: config.redis_url.clone(),
        key_prefix: config.redis_key_prefix.clone(),
        distributed_lock_enabled: config.redis_distributed_lock_enabled,
        response_timeout: Duration::from_millis(config.redis_response_timeout_ms),
        connect_timeout: Duration::from_millis(config.redis_connect_timeout_ms),
        ..Default::default()
    })
}

fn cluster_placement(
    registry: &Arc<AtomicNodeRegistry>,
    node_service_port: u16,
    warmup: &Arc<WarmupGate>,
    grpc_service: &NodeRegistryGrpcService,
) -> Arc<dyn aenv_api::node_client::NodePlacement> {
    Arc::new(NativeNodePlacement::new(
        Arc::clone(registry),
        node_service_port,
        Arc::clone(warmup),
        grpc_service.clone(),
    ))
}

struct NativeNodeRegistryBits {
    registry: Arc<AtomicNodeRegistry>,
    warmup: Arc<WarmupGate>,
    grpc_service: NodeRegistryGrpcService,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

const NODE_REGISTRY_METRICS_INTERVAL: Duration = Duration::from_secs(15);
const KUBE_DISCOVERY_INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const KUBE_DISCOVERY_MAX_BACKOFF: Duration = Duration::from_secs(30);

async fn start_native_node_registry(
    config: &aenv_api::cfg::ClusterConfig,
) -> anyhow::Result<NativeNodeRegistryBits> {
    let discovery = &config.kubernetes_discovery;

    let registry = Arc::new(AtomicNodeRegistry::with_empty_sync_guard(
        Vec::new(),
        Duration::from_secs(30),
        aenv_api::node_registry::registry::EmptySyncGuard {
            confirmations: discovery.empty_sync_confirmations,
            window: Duration::from_secs(discovery.empty_sync_window_secs),
        },
    ));
    // `assemble_api` rebases this deadline after the heartbeat listener binds.
    let warmup = Arc::new(WarmupGate::new(
        Arc::clone(&registry) as Arc<dyn aenv_api::node_registry::registry::NodeRegistry>,
        Duration::from_secs(config.native_warmup_timeout_secs),
        std::time::SystemTime::now(),
    ));
    let grpc_service = NodeRegistryGrpcService::new(Arc::clone(&registry), Arc::clone(&warmup))
        .with_placement_shadow_k(config.placement_shadow_k);

    let mut tasks = Vec::new();
    match config.node_discovery_mode {
        aenv_api::cfg::ClusterNodeDiscoveryMode::Kubernetes => {
            let namespace = discovery.namespace.trim();
            let service_name = discovery.service_name.trim();
            if namespace.is_empty() || service_name.is_empty() {
                anyhow::bail!(
                    "aenv-api needs [cluster.kubernetes_discovery].namespace and .service_name \
                     (AENV_CLUSTER_KUBERNETES_DISCOVERY_NAMESPACE / \
                     AENV_CLUSTER_KUBERNETES_DISCOVERY_SERVICE_NAME) when (the default) \
                     [cluster].node_discovery_mode = \"kubernetes\": the api half's node \
                     registry has \
                     nothing to discover nodes from otherwise. Set \
                     AENV_CLUSTER_NODE_DISCOVERY_MODE=static and \
                     [cluster].static_discovery_nodes for a non-Kubernetes deployment."
                );
            }
            let kube_config = KubernetesDiscoveryConfig {
                namespace: namespace.to_string(),
                service_name: service_name.to_string(),
                port: i32::from(discovery.port),
                scheme: discovery.scheme.clone(),
                ignore_pod_selector: discovery.ignore_pod_selector.clone(),
                no_schedule_pod_selector: discovery.no_schedule_pod_selector.clone(),
            };
            // Validate before starting the retry loop.
            validate_optional_pod_selector(
                &kube_config.ignore_pod_selector,
                "ignore_pod_selector",
            )?;
            validate_optional_pod_selector(
                &kube_config.no_schedule_pod_selector,
                "no_schedule_pod_selector",
            )?;

            let discovery_task = {
                let registry = Arc::clone(&registry);
                tokio::spawn(run_kubernetes_discovery_with_retry(kube_config, registry))
            };
            tasks.push(discovery_task);
        }
        aenv_api::cfg::ClusterNodeDiscoveryMode::Static => {
            aenv_api::node_registry::static_discovery::validate_static_discovery_nodes(
                &config.static_discovery_nodes,
            )
            .map_err(|err| {
                anyhow::anyhow!(
                    "aenv-api needs a valid [[cluster.static_discovery_nodes]] list (set via \
                     AENV_CONFIG_OVERLAY_PATH — see ClusterConfig::static_discovery_nodes's own \
                     doc comment) when [cluster].node_discovery_mode = \"static\": {err}"
                )
            })?;
            let nodes = aenv_api::node_registry::static_discovery::nodes_from_static_config(
                &config.static_discovery_nodes,
            );
            registry.set(nodes, Vec::new(), std::time::SystemTime::now());
        }
    }

    let metrics_task = {
        let metrics_service = grpc_service.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(NODE_REGISTRY_METRICS_INTERVAL);
            loop {
                interval.tick().await;
                metrics_service.refresh_observed_nodes_metric();
            }
        })
    };
    tasks.push(metrics_task);

    Ok(NativeNodeRegistryBits {
        registry,
        warmup,
        grpc_service,
        tasks,
    })
}

async fn build_binding_store(config: &BindingStoreConfig) -> anyhow::Result<Arc<dyn BindingStore>> {
    let settings = BindingStoreSettings {
        binding_ttl: Duration::from_secs(config.binding_ttl_secs),
        projection_authoritative: config.projection_authoritative,
    };
    let redis_config = RedisBindingStoreConfig {
        url: config.redis_url.clone(),
        key_prefix: config.redis_key_prefix.clone(),
        node_index_ttl: Duration::from_secs(config.redis_node_index_ttl_secs),
        response_timeout: Duration::from_millis(config.redis_response_timeout_ms),
        connect_timeout: Duration::from_millis(config.redis_connect_timeout_ms),
    };
    let store = RedisBindingStore::connect(redis_config, settings)
        .await
        .map_err(|err| anyhow::anyhow!("connecting the binding store to redis: {err}"))?;
    Ok(Arc::new(store) as Arc<dyn BindingStore>)
}

async fn wire_shared_node_observed_store(
    config: &ClusterNodeRegistryStoreConfig,
    registry: &Arc<AtomicNodeRegistry>,
) -> anyhow::Result<Option<tokio::task::JoinHandle<()>>> {
    match config.backend {
        NodeRegistryObservedBackendKind::InMemory => {
            anyhow::bail!(
                "aenv-api needs [cluster.node_registry_store].backend = \"redis\" \
                 (AENV_CLUSTER_NODE_REGISTRY_STORE_BACKEND): the in-memory node registry only \
                 sees the nodes whose heartbeat happens to be pinned to this replica, and \
                 aenv-api runs as more than one replica. Set \
                 AENV_CLUSTER_NODE_REGISTRY_STORE_BACKEND=redis"
            );
        }
        NodeRegistryObservedBackendKind::Redis => {
            let store = SharedObservedStore::connect(SharedObservedStoreConfig {
                url: config.redis_url.clone(),
                key_prefix: config.redis_key_prefix.clone(),
                response_timeout: Duration::from_millis(config.redis_response_timeout_ms),
                connect_timeout: Duration::from_millis(config.redis_connect_timeout_ms),
            })
            .await?;
            let rx = registry.enable_shared_observed_publishing();
            // The periodic pull retries an initial best-effort failure.
            match store.pull_all().await {
                Ok(remote) => registry.merge_remote_snapshot(remote),
                Err(err) => {
                    tracing::warn!(
                        target: "agentenv",
                        error = %err,
                        "node registry redis: initial pull failed; continuing, the periodic \
                         pull will retry"
                    );
                }
            }
            let task_registry = Arc::clone(registry);
            Ok(Some(tokio::spawn(run_shared_observed_sync(
                task_registry,
                rx,
                store,
                DEFAULT_PULL_INTERVAL,
            ))))
        }
    }
}

// Retry all discovery failures; `kube::Config::infer` has no permanent-failure signal.
async fn run_kubernetes_discovery_with_retry(
    config: KubernetesDiscoveryConfig,
    registry: Arc<AtomicNodeRegistry>,
) {
    let mut backoff = KUBE_DISCOVERY_INITIAL_BACKOFF;
    loop {
        match KubernetesDiscovery::connect(config.clone(), Arc::clone(&registry)).await {
            Ok(discovery) => {
                if let Err(err) = discovery.run().await {
                    warn!(target: "agentenv", error = %err, "kubernetes node discovery ended; retrying");
                }
            }
            Err(err) => {
                warn!(target: "agentenv", error = %err, "kubernetes node discovery failed to start; retrying");
            }
        }
        tokio::time::sleep(backoff).await;
        backoff = std::cmp::min(backoff.saturating_mul(2), KUBE_DISCOVERY_MAX_BACKOFF);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn the_api_binary_accepts_neither_a_role_nor_a_provisioning_mode() {
        ApiCli::command().debug_assert();

        ApiCli::parse_from(["aenv-api"]);
        for spelling in ["api", "node", "all"] {
            assert!(
                ApiCli::try_parse_from(["aenv-api", "--role", spelling]).is_err(),
                "--role {spelling} must be refused outright"
            );
        }
        assert!(
            ApiCli::try_parse_from(["aenv-api", "--setup-host"]).is_err(),
            "the api binary has no host-provisioning mode to offer"
        );
        assert!(
            ApiCli::try_parse_from(["aenv-api", "--setup-only"]).is_err(),
            "nor a dependency-provisioning one"
        );
        assert!(
            ApiCli::try_parse_from(["aenv-api", "--config", "/dev/null"]).is_ok(),
            "the control: an argument this binary does declare still parses, so the refusals \
             above are about the flags and not about parse_from being broken"
        );
    }

    #[tokio::test]
    async fn the_api_half_refuses_to_assemble_without_a_postgres_catalog() {
        let config = AppConfig::default();
        assert!(
            config.pg.is_none(),
            "the default has no [pg], which is what makes this refusal reachable without a \
             database"
        );

        let err = match build_pg_pool(&config).await {
            Ok(_) => panic!("aenv-api has no catalog at all without [pg]"),
            Err(err) => format!("{err:#}"),
        };
        assert!(err.contains("[pg]"), "{err}");
        assert!(err.contains("[pg].dsn"), "{err}");
        assert!(err.contains("aenv-api"), "{err}");
        assert!(err.contains("AENV_CONFIG_OVERLAY_PATH"), "{err}");
        assert!(
            !err.contains("AENV_PG_DSN"),
            "there is no such environment variable: {err}"
        );
    }

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
        assert!(err.contains("orchestrator.store"), "{err}");
        assert!(err.contains("AENV_ORCHESTRATOR_STORE_BACKEND"), "{err}");
        assert!(err.contains("in-memory"), "{err}");
        assert!(
            err.contains(MetadataStoreBackendKind::InMemory.as_str()),
            "{err}"
        );
        assert_ne!(
            MetadataStoreBackendKind::InMemory.as_str(),
            MetadataStoreBackendKind::Redis.as_str(),
            "the two backends must not answer to the same name"
        );

        // Use non-default values so omitted mappings cannot pass accidentally.
        config.orchestrator.store.backend = MetadataStoreBackendKind::Redis;
        config.orchestrator.store.redis_url = "redis://cluster-redis:6379".to_string();
        config.orchestrator.store.redis_key_prefix = "agentenv:probe".to_string();
        config.orchestrator.store.redis_distributed_lock_enabled = false;
        config.orchestrator.store.redis_response_timeout_ms = 1234;
        config.orchestrator.store.redis_connect_timeout_ms = 2345;

        let defaults = aenv_api::orchestrator::RedisStoreConfig::default();
        assert_ne!(defaults.url, config.orchestrator.store.redis_url);
        assert_ne!(
            defaults.key_prefix,
            config.orchestrator.store.redis_key_prefix
        );
        assert!(defaults.distributed_lock_enabled);
        assert_ne!(
            defaults.response_timeout.as_millis() as u64,
            config.orchestrator.store.redis_response_timeout_ms
        );
        assert_ne!(
            defaults.connect_timeout.as_millis() as u64,
            config.orchestrator.store.redis_connect_timeout_ms
        );

        let store = cluster_store_config(&config.orchestrator.store)
            .expect("the cluster store is what this role is for");
        assert_eq!(store.url, "redis://cluster-redis:6379");
        assert_eq!(store.key_prefix, "agentenv:probe");
        assert!(!store.distributed_lock_enabled);
        assert_eq!(store.response_timeout, Duration::from_millis(1234));
        assert_eq!(store.connect_timeout, Duration::from_millis(2345));
        store
            .validate()
            .expect("the defaults this function leans on must be a valid combination");
    }

    #[test]
    fn cluster_placement_builds_a_native_placement_with_no_scheduler_endpoint() {
        let config = AppConfig::default();
        assert_eq!(
            config.cluster.scheduler_endpoint, None,
            "the whole point of this test is that placement does not need one"
        );

        let registry = Arc::new(AtomicNodeRegistry::new(Vec::new(), Duration::from_secs(30)));
        let warmup = Arc::new(WarmupGate::new(
            Arc::clone(&registry) as Arc<dyn aenv_api::node_registry::registry::NodeRegistry>,
            Duration::from_secs(15),
            std::time::SystemTime::now(),
        ));
        let grpc_service = NodeRegistryGrpcService::new(Arc::clone(&registry), Arc::clone(&warmup));

        let _placement = cluster_placement(
            &registry,
            config.cluster.node_service_port,
            &warmup,
            &grpc_service,
        );
    }

    #[tokio::test]
    async fn the_api_half_binds_sandboxes_through_redis_with_nothing_selecting_it() {
        let mut config = AppConfig::default().binding_store;
        config.redis_url = "redis://127.0.0.1:1/0".to_string();

        let err = match build_binding_store(&config).await {
            Ok(_) => panic!(
                "nothing is listening on this port — an Ok here means this replica built some                  store other than the configured Redis one"
            ),
            Err(err) => format!("{err:#}"),
        };
        assert!(
            err.contains("connecting the binding store to redis"),
            "the failure has to be the Redis connection, which is what says Redis is what got              built: {err}"
        );
    }

    #[tokio::test]
    async fn the_api_half_refuses_a_node_registry_no_other_replica_can_see() {
        let config = AppConfig::default().cluster.node_registry_store;
        assert_eq!(
            config.backend,
            NodeRegistryObservedBackendKind::InMemory,
            "the default is the per-replica heartbeat shard, which is what makes this refusal \
             necessary"
        );

        let registry = Arc::new(AtomicNodeRegistry::new(Vec::new(), Duration::from_secs(30)));
        let err = match wire_shared_node_observed_store(&config, &registry).await {
            Ok(_) => panic!(
                "the in-memory node registry only sees the nodes whose heartbeat is pinned to \
                 this replica"
            ),
            Err(err) => err.to_string(),
        };
        assert!(err.contains("node_registry_store"), "{err}");
        assert!(
            err.contains("AENV_CLUSTER_NODE_REGISTRY_STORE_BACKEND"),
            "{err}"
        );
        assert!(err.contains("redis"), "{err}");

        let mut redis_config = AppConfig::default().cluster.node_registry_store;
        redis_config.backend = NodeRegistryObservedBackendKind::Redis;
        redis_config.redis_url = "redis://127.0.0.1:1/0".to_string();
        let connect_err = match wire_shared_node_observed_store(&redis_config, &registry).await {
            Ok(_) => panic!("nothing is listening on this port"),
            Err(err) => err.to_string(),
        };
        assert!(
            !connect_err.contains("more than one replica"),
            "a redis backend must fail on the connection, not on the multi-replica refusal: \
             {connect_err}"
        );
    }

    #[tokio::test]
    async fn native_placement_refuses_an_unconfigured_kubernetes_discovery() {
        let unconfigured_discovery = aenv_api::cfg::ClusterConfig {
            ..AppConfig::default().cluster
        };
        let err = match start_native_node_registry(&unconfigured_discovery).await {
            Ok(_) => panic!("an empty namespace/service_name must still be refused"),
            Err(err) => err.to_string(),
        };
        assert!(
            err.contains("kubernetes_discovery"),
            "an empty namespace/service_name must be refused: {err}"
        );
    }

    #[tokio::test]
    async fn native_placement_seeds_the_registry_from_static_discovery() {
        let config = aenv_api::cfg::ClusterConfig {
            node_discovery_mode: aenv_api::cfg::ClusterNodeDiscoveryMode::Static,
            static_discovery_nodes: vec![
                aenv_api::cfg::ClusterStaticDiscoveryNode {
                    id: "node-a".to_string(),
                    endpoint: "http://agentenv-a:8000".to_string(),
                },
                aenv_api::cfg::ClusterStaticDiscoveryNode {
                    id: "node-b".to_string(),
                    endpoint: "http://agentenv-b:8000".to_string(),
                },
            ],
            // Deliberately left unconfigured: static mode must never need it.
            kubernetes_discovery: Default::default(),
            ..AppConfig::default().cluster
        };

        let bits = start_native_node_registry(&config)
            .await
            .expect("a valid static node list must not be refused");

        assert_eq!(
            bits.tasks.len(),
            1,
            "static discovery is a one-shot seed; only the metrics task should be running, no \
             discovery watch task"
        );

        let mut ids: Vec<String> = bits
            .registry
            .snapshot(true)
            .into_iter()
            .map(|n| n.id)
            .collect();
        ids.sort();
        assert_eq!(ids, vec!["node-a".to_string(), "node-b".to_string()]);
        assert_eq!(
            bits.registry.resolve("node-a").map(|n| n.endpoint),
            Some("http://agentenv-a:8000".to_string())
        );

        for task in bits.tasks {
            task.abort();
        }
    }

    #[tokio::test]
    async fn native_placement_refuses_an_empty_static_discovery_node_list() {
        let config = aenv_api::cfg::ClusterConfig {
            node_discovery_mode: aenv_api::cfg::ClusterNodeDiscoveryMode::Static,
            static_discovery_nodes: Vec::new(),
            ..AppConfig::default().cluster
        };
        let err = match start_native_node_registry(&config).await {
            Ok(_) => panic!("an empty static_discovery_nodes list must still be refused"),
            Err(err) => err.to_string(),
        };
        assert!(
            err.contains("static_discovery_nodes"),
            "an empty node list must be refused: {err}"
        );
    }

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

    #[test]
    fn resume_placement_is_wired_after_every_builder() {
        let source = include_str!("aenv-api.rs");
        let raw_body = body_of(source, "async fn assemble_api(config: &AppConfig)");
        // Strip comments so prose cannot satisfy the ordering checks.
        let assemble: String = raw_body
            .lines()
            .filter(|line| !line.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n");

        let at = |needle: &str| {
            assemble
                .find(needle)
                .unwrap_or_else(|| panic!("assemble_api no longer contains {needle}"))
        };
        // Control for the strip filter: a phrase that occurs only in one of
        // assemble_api's own comment lines, so it is present before the strip
        // and absent after it.
        const COMMENT_ANCHOR: &str = "Clone only after the binding and artifact builders";
        assert!(
            raw_body.contains(COMMENT_ANCHOR),
            "the comment this check anchors on is gone; re-anchor on another \
             comment line inside assemble_api"
        );
        assert!(
            !assemble.contains(COMMENT_ANCHOR),
            "no comment line survived the strip, so this is scanning the raw body again"
        );
        let resume = at("ResumeWiring::cluster_in_process(node_registry_grpc_service.clone())");
        for builder in [".with_binding_store(", ".with_artifact_store("] {
            assert!(
                at(builder) < resume,
                "🔴 {builder} runs after the clone handed to resume placement, so the \
                 wake-up path holds a service that cannot answer a sandbox lookup the way \
                 the served one does"
            );
        }

        assert!(
            !assemble.contains("async fn assemble_api"),
            "the scan is reading more than assemble_api's body, so the ordering above \
             proves nothing about where the builders actually run"
        );
    }
}
