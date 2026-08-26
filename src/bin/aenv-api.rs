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

use agentenv::api::{server, ApiImpl, PausedSandboxWiring, ResumeWiring, StaleReleaseOutcome};
use agentenv::binding_store::{
    ArbitrationMode, BindingStore, BindingStoreSettings, RedisBindingStore, RedisBindingStoreConfig,
};
use agentenv::cfg::{
    AppConfig, BindingStoreBackendKind, BindingStoreConfig, ClusterNodeRegistryStoreConfig,
    MetadataStoreBackendKind, NodePlacementSource, NodeRegistryObservedBackendKind,
};
use agentenv::identity::NodeIdentity;
use agentenv::image::ImageResolver;
use agentenv::node_client::{
    NativeNodePlacement, RemoteSandboxBackendFactory, SchedulerNodePlacement,
};
use agentenv::node_registry::dump::NodeRegistryDumpSource;
use agentenv::node_registry::grpc_service::NodeRegistryGrpcService;
use agentenv::node_registry::kubernetes_discovery::{
    validate_optional_pod_selector, KubernetesDiscovery, KubernetesDiscoveryConfig,
};
use agentenv::node_registry::redis::{
    run_shared_observed_sync, SharedObservedStore, SharedObservedStoreConfig, DEFAULT_PULL_INTERVAL,
};
use agentenv::node_registry::registry::{AtomicNodeRegistry, NodeRegistry};
use agentenv::node_registry::warmup::WarmupGate;
use agentenv::observability::ObservabilityService;
use agentenv::orchestrator::{
    build_paused_registry, spawn_paused_registry_background_tasks, DisabledSandboxPersister,
    Orchestrator, PgPausedRegistryFactory, PostgresPausedRegistryFactory, RedisMetadataStore,
    SandboxOrchestration,
};
use agentenv::pg::{self, PgPoolSettings};
use agentenv::role::ServerRole;
use agentenv::server_main::{self, spawn_grpc_surface, Assembly};
use agentenv::snapshot::SnapshotManager;
use agentenv::template::TemplateBuilder;
use anyhow::Context as _;
use clap::Parser;
use tracing::{info, warn};

/// 🔴 `--role` is accepted and checked, not obeyed. See
/// [`ServerRole::confirm`]: this binary *is* the api half, and it is that
/// because of what it does not link — no overlaybd, no ublk, no Firecracker.
#[derive(Debug, Parser)]
#[command(name = "aenv-api")]
struct ApiCli {
    /// Which half of the split this process runs. Only `api` is accepted
    /// here; also read from AENV_ROLE.
    #[arg(long, value_enum)]
    role: Option<ServerRole>,

    /// Path to config file (same as AENV_CONFIG_PATH).
    #[arg(long)]
    config: Option<std::path::PathBuf>,
}

/// 🔴 Not `#[tokio::main]` — see `aenv-node`'s `main` for the whole argument.
/// This half opens fewer RocksDB stores than the node one does, but it opens
/// the snapshot catalog's mirror backlog, so the same bound applies.
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

    let cli = ApiCli::parse();
    let role = ServerRole::Api.confirm(cli.role)?;
    let config_manager = if let Some(config_path) = cli.config.as_deref() {
        agentenv::cfg::ConfigManager::init_global_from_path(config_path)?
    } else {
        agentenv::cfg::ConfigManager::init_global()?
    };
    let config = config_manager.config();

    info!(target: "agentenv", role = role.as_str(), "assembling server");
    let assembly = assemble_api(config).await?;
    server_main::serve(role, config, assembly).await
}

/// This replica's `[pg]` connection pool, or `None` when PostgreSQL is not
/// configured for this process.
///
/// 🔴 Never call this from `assemble_node`. `--role node` is refused at
/// startup if `[pg].dsn` is configured at all (`ServerRole::check_pg_dsn`,
/// enforced in `async_main` before any role-specific assembly runs), but that
/// guard is defense against a *configured* DSN reaching a node — it does not
/// stop this function itself from being called there. The two callers that
/// may hold PostgreSQL credentials, the connection budget and the schema are
/// `assemble_api` and `assemble_all`; see `src/pg/mod.rs`'s own module doc.
async fn build_pg_pool(config: &AppConfig) -> anyhow::Result<Option<sqlx::PgPool>> {
    let Some(settings) = PgPoolSettings::from_config(config.pg.as_ref())? else {
        return Ok(None);
    };
    let pool = pg::connect(&settings).await?;
    // 🔴 Before this pool reaches anything that queries the catalog tables —
    // the build reaper (`spawn_pg_singleton_tasks`, started right after this
    // returns) and `build_snapshot_backend`'s `PostgresSnapshotCatalog`
    // construction both assume the schema already exists. See
    // `agentenv::snapshot::repository::backends::migrate_catalog_schema`'s own
    // doc: idempotent, advisory-lock-guarded, safe on every start and across
    // a fleet of replicas racing to call it at once.
    agentenv::snapshot::repository::backends::migrate_catalog_schema(&pool).await?;
    Ok(Some(pool))
}

/// The catalog build reaper's own cadence: how often the cluster-elected
/// leader scans for stale builds, and how far behind a heartbeat may fall
/// before the leader ends the build it belongs to.
///
/// 🔴 No dedicated config knob (Stage B, per its own docs/proposals, adds no
/// new config axis beyond what `snapshot.catalog.{write,read}` already
/// give). `ttl` is derived from the existing
/// `[snapshot.catalog].build_heartbeat_interval_secs` — the node-side
/// cadence a builder renews its lease on — the same "roughly a third of the
/// TTL is the usual margin" reasoning that field's own doc comment states,
/// inverted: two renewals may be lost to a rollout or a slow network before
/// a build that is still running gets taken away from it.
fn reaper_cadence(config: &AppConfig) -> (std::time::Duration, std::time::Duration) {
    let heartbeat_interval = config.snapshot.catalog.build_heartbeat_interval_secs.max(1);
    let interval = std::time::Duration::from_secs(30);
    let ttl = std::time::Duration::from_secs(heartbeat_interval.saturating_mul(3));
    (interval, ttl)
}

/// Starts the catalog build reaper for this process when `pg_pool` is
/// `Some`. See `agentenv::snapshot::repository::backends::spawn_catalog_build_reaper`'s
/// own doc on why the handle must be shut down through its own `shutdown()`
/// path rather than folded into `paused_upkeep`.
fn spawn_pg_singleton_tasks(
    config: &AppConfig,
    pg_pool: Option<sqlx::PgPool>,
) -> Vec<agentenv::pg::SingletonTaskHandle> {
    let (interval, ttl) = reaper_cadence(config);
    let identity = agentenv::identity::NodeIdentity::from_config(&config.node_identity);
    agentenv::snapshot::repository::backends::spawn_catalog_build_reaper(
        pg_pool,
        identity.cluster_id,
        interval,
        ttl,
    )
    .into_iter()
    .collect()
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
/// - **A cold create — retired.** This bullet used to say
///   `RemoteSandboxBackendFactory::build` refuses outright — the build spec it
///   is handed has already been resolved into paths on a local disk and the
///   user's image reference is gone by then — so `POST /sandboxes-cold` was
///   refused at the door (`ServerRole::runs_sandbox_runtime`, read before
///   anything is resolved, in `crate::api::impls`) rather than failing on the
///   way there with a missing-`regctl` error. Kept here rather than deleted so
///   it is not re-derived from the same reasoning. It no longer holds:
///   `sandboxes_cold_post` now builds `SandboxLaunchSource::UnresolvedImage`
///   instead of resolving anything when `!role.runs_sandbox_runtime()`, and
///   `SandboxBackendFactory::build_from_image_ref`
///   (`RemoteSandboxBackendFactory`'s implementation, the method `build`
///   still refuses for) ships the reference — and each attached drive's own
///   reference — to a node exactly as unresolved as a snapshot id already
///   was. `NodeSandboxService::create`'s `Source::Image` arm resolves it
///   there, node-side, into the same `SandboxLaunchSource::Image` a local
///   cold create already builds, and the node's `Create` reply also carries
///   the resolved context and image configs back
///   (`SandboxRuntimeInfo::resolved_image_facts`) so this half's own record of
///   the sandbox is not left with placeholders it invented.
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
/// - **Building a template — retired.** This bullet used to say
///   `TemplateBuilder` drives a `FirecrackerSandbox` directly, outside the
///   orchestrator entirely, so a build here would reach for `/dev/kvm` in a
///   Pod that has none — and that `POST /v2/templates/{id}/builds/{id}`
///   refused at the door instead of losing the build in a background task.
///   Kept here rather than deleted so it is not re-derived from the same
///   reasoning. It no longer holds: `TemplateBuildRunner` (the piece that
///   needs `/dev/kvm`) is ordinary Rust that runs wherever it is called, so
///   the door now dispatches to a node instead of refusing —
///   `run_the_build_on_a_node` in `src/api/impls/template.rs`, using this
///   function's own `placement` to pick one and
///   `NodeSandboxService::build_template` (`src/node_server/service.rs`) to
///   run it there. The door still refuses, but only when there is truly
///   nowhere to send the build — see `ApiImpl::node_placement` and the
///   refusal's own condition in `v2_templates_template_id_builds_build_id_post`.
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
    // 🔴 Stage A's placement switch (`[cluster].node_placement_source`, task's
    // own "D7"): under `Native`, this builds api's own node registry — kube
    // discovery, the heartbeat-receiving gRPC service, and the warm-up gate —
    // *before* `cluster_placement` runs, so `cluster_placement` can hand a
    // `NativeNodePlacement` a handle on it. Under `Scheduler` (the default),
    // this constructs nothing: no registry, no kube client, no gRPC service —
    // see `start_native_node_registry`'s own doc comment.
    let native_node_registry = match config.cluster.node_placement_source {
        NodePlacementSource::Native => Some(
            start_native_node_registry(
                &config.cluster,
                &config
                    .observability
                    .scheduler_report
                    .dual_report_api_endpoint,
            )
            .await?,
        ),
        NodePlacementSource::Scheduler => None,
    };
    let (
        native_registry_handle,
        native_warmup_handle,
        node_registry_grpc_service,
        mut node_registry_upkeep,
    ) = match native_node_registry {
        Some(bits) => (
            Some(bits.registry),
            Some(bits.warmup),
            Some(bits.grpc_service),
            bits.tasks,
        ),
        None => (None, None, None, Vec::new()),
    };
    // Task's own "D3": wires the binding store into the Scheduler-compatible
    // gRPC surface this replica serves natively. Gated the same way the
    // service itself is — `node_registry_grpc_service` is only `Some` under
    // `[cluster].node_placement_source = "native"` — because without that
    // surface there is nowhere for `report_sandbox_event`/`heartbeat`/
    // `record_assignment` to run at all.
    let mut binding_store_handle: Option<Arc<dyn BindingStore>> = None;
    let node_registry_grpc_service = match node_registry_grpc_service {
        Some(service) => {
            let binding_store = build_binding_store(&config.binding_store).await?;
            binding_store_handle = Some(Arc::clone(&binding_store));
            let max_projection_ttl =
                Duration::from_secs(config.binding_store.max_projection_ttl_secs);
            let artifact_store: Arc<dyn agentenv::binding_store::artifact_index::ArtifactStore> =
                Arc::new(
                    agentenv::binding_store::artifact_index::InMemoryArtifactStore::new(
                        config.binding_store.artifact_index_capacity as usize,
                    ),
                );
            Some(
                service
                    .with_binding_store(
                        binding_store,
                        config.binding_store.projection_authoritative,
                        max_projection_ttl,
                    )
                    .with_artifact_store(artifact_store),
            )
        }
        None => None,
    };
    // The shared-roster fix: only under `[cluster].node_placement_source =
    // "native"` (`native_registry_handle` is `Some`), the same gate
    // `binding_store` above is wired under. Its background task is folded
    // into this role's own upkeep the same way the kube-discovery/metrics
    // tasks already are, via `node_registry_upkeep`.
    if let Some(registry) = native_registry_handle.as_ref() {
        if let Some(task) =
            wire_shared_node_observed_store(&config.cluster.node_registry_store, registry).await?
        {
            node_registry_upkeep.push(task);
        }
    }
    // 🔴 P1 (task's own "phase4-close"): moved up from after
    // `Orchestrator::new` (see the historical comments still attached to
    // `pg_pool`/`node_registry_for_paused`/`build_paused_registry` below).
    // `NativeNodePlacement`'s `place_existing` now answers `LookupNode`'s
    // paused-registry stage in-process through the same
    // `node_registry_grpc_service` `cluster_placement` is about to hand it
    // a clone of, so that service has to be `.with_paused_registry(...)`
    // *before* `cluster_placement` runs rather than after — which means
    // `paused_registry`, and the `pg_pool` its `postgres` backend needs,
    // now have to exist this early too. Nothing between here and their old
    // position needed either built any later than this.
    //
    // The commit side of snapshots, the resolver, and the builder's
    // scheduling half all still need this same `pg_pool` — see the
    // (unmoved) `snapshot_manager` construction below. No P2P transport is
    // passed there, and `[snapshot].p2p_enabled` is not consulted: P2P
    // moves bytes between machines that hold them, and this process holds
    // none.
    let pg_pool = build_pg_pool(config).await?;
    let mut pg_singleton_tasks = spawn_pg_singleton_tasks(config, pg_pool.clone());
    // Stage C's own use of Stage A's registry: `Arc<AtomicNodeRegistry>`
    // coerced to `Arc<dyn NodeRegistry>`, cloned rather than moved --
    // `native_registry_handle` itself is still needed below by
    // `node_registry_dump_source`.
    let node_registry_for_paused: Option<Arc<dyn NodeRegistry>> = native_registry_handle
        .clone()
        .map(|registry| registry as Arc<dyn NodeRegistry>);
    // The `postgres` arm's own constructor, when this replica has a pool at
    // all. `build_paused_registry` keeps the arm, its refusal message and its
    // `--role api` roster guard; the pool, the schema bootstrap and the
    // restart-grace entry live behind this.
    let paused_registry_factory = pg_pool.clone().map(PgPausedRegistryFactory::new);
    let paused_registry = build_paused_registry(
        &config.orchestrator.paused_registry,
        &config.cluster,
        &config.observability.scheduler_report,
        &identity_for_registry,
        role,
        paused_registry_factory
            .as_ref()
            .map(|factory| factory as &dyn PostgresPausedRegistryFactory),
        node_registry_for_paused.clone(),
    )
    .await?;
    // Task's own "Stage D remainder": `lookup_node`'s stage 3. Wired onto
    // the same service `cluster_placement` is about to hand
    // `NativeNodePlacement` a clone of -- a no-op under
    // `[cluster].node_placement_source = "scheduler"`
    // (`node_registry_grpc_service` is `None` there).
    let node_registry_grpc_service = node_registry_grpc_service
        .map(|service| service.with_paused_registry(Arc::clone(&paused_registry)));

    // 🔴 P1: under `[cluster].node_placement_source = "native"`,
    // `NativeNodePlacement` answers `place_new`/`place_existing`/
    // `record_placement` from this same in-process
    // `node_registry_grpc_service` (a clone -- `spawn_grpc_surface` below
    // still gets the original, moved in) instead of dialling a scheduler
    // this deployment no longer has to run at all. See `cluster_placement`'s
    // own doc comment for why `[cluster].scheduler_endpoint` is no longer
    // required in that mode.
    let placement = cluster_placement(
        &config.cluster,
        &config.observability.scheduler_report,
        native_registry_handle.as_ref(),
        native_warmup_handle.as_ref(),
        node_registry_grpc_service.as_ref(),
    )?;
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
        // 🔴 A clone, not the original: `ApiImpl` needs its own handle on the
        // same placement source to pick a node for a template build it
        // cannot run itself (`POST /v2/templates/{id}/builds/{id}`,
        // `run_the_build_on_a_node` in `src/api/impls/template.rs`) — the same
        // question `place_new` already answers for a fresh sandbox create,
        // asked here for a build sandbox instead of a user one.
        RemoteSandboxBackendFactory::new(Arc::clone(&placement)),
        DisabledSandboxPersister,
        agentenv::image::DisabledRuntimeImageRefs::shared(),
    )
    .await?;
    let orchestration: Arc<dyn SandboxOrchestration> =
        Arc::clone(&orchestrator) as Arc<dyn SandboxOrchestration>;

    // The central catalog's three faces and Stage B step 4's shared read-side
    // confirmation record, both over the one pool this replica built — the
    // only things `build_snapshot_backend` needs from PostgreSQL, and the
    // reason it no longer takes the pool itself.
    let pg_catalog = pg_pool
        .as_ref()
        .map(|pool| agentenv::snapshot::repository::backends::pg_catalog_parts(config, pool));
    let snapshot_backend = agentenv::snapshot::repository::backends::build_snapshot_backend(
        agentenv::snapshot::repository::backends::build_catalog_only_storage(config)?,
        pg_catalog,
        role,
    )
    .await?;
    let snapshot_manager = Arc::new(SnapshotManager::from_assembled(snapshot_backend, None));
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

    let paused_registry_tasks = spawn_paused_registry_background_tasks(
        &config.orchestrator.paused_registry,
        &identity_for_registry,
        pg_pool,
        node_registry_for_paused,
    );
    pg_singleton_tasks.extend(paused_registry_tasks.singleton);
    let mut paused_registry_upkeep = paused_registry_tasks.plain;
    let paused_wiring = PausedSandboxWiring::new(
        paused_registry,
        Arc::clone(&snapshot_manager),
        &identity_for_registry,
    );
    orchestrator.set_paused_publisher(paused_wiring.publisher());

    let api_impl = Arc::new(
        ApiImpl::new(
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
        )
        // 🔴 The role `!runs_sandbox_runtime()` names, and the one
        // `v2_templates_...`'s remote branch exists for: a template build has
        // to go somewhere, and the earlier clone into the factory is what
        // makes handing this process the same placement source free. See
        // `ApiImpl::with_node_placement`.
        .with_node_placement(placement),
    );

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
    // The node registry's own background tasks (kube discovery, its metrics
    // refresh loop) stop the same way and at the same point as the rest of
    // this role's upkeep — see `spawn_paused_record_upkeep`'s callers for why
    // that point is "before the shutdown pauses start" and not later.
    paused_upkeep.append(&mut node_registry_upkeep);
    // Task's own "D4": the heartbeat-timeout binding sweep. Only meaningful
    // alongside a wired binding store, which only exists alongside the
    // native node registry -- both `Option`s are `Some` or `None` together.
    if config.binding_store.sweep_enabled {
        if let (Some(registry), Some(store)) =
            (native_registry_handle.clone(), binding_store_handle.clone())
        {
            let cluster_id = config.node_identity.cluster_id.clone().unwrap_or_default();
            let sweeper = Arc::new(agentenv::binding_store::sweep::BindingSweeper::new(
                cluster_id,
                Duration::from_secs(config.binding_store.sweep_silence_secs),
            ));
            let registry: Arc<dyn NodeRegistry> = registry;
            let interval = Duration::from_secs(config.binding_store.sweep_interval_secs);
            paused_upkeep.push(tokio::spawn(async move {
                sweeper.run(registry, store, interval).await;
            }));
        }
    }
    // B1: the postgres backend's per-replica renewal loop, present only
    // under `[cluster].node_placement_source = "native"` (empty otherwise --
    // see `spawn_paused_registry_background_tasks`'s own doc).
    paused_upkeep.append(&mut paused_registry_upkeep);

    // 🔴 Task 4's equivalence-dump debug endpoint: built and mounted
    // regardless of `node_placement_source`, per the task's own instruction
    // that the comparison hook has to work under the default (`Scheduler`)
    // switch position too — see `node_registry::dump`'s own module doc for
    // why the two modes converge on the same output shape. Native mode reads
    // the registry `start_native_node_registry` already built above;
    // scheduler mode reuses the same, already-validated
    // `[cluster].scheduler_endpoint` `cluster_placement` required, over a
    // second lazily connected channel (never the same `Channel` as
    // `cluster_placement`'s own `SchedulerNodePlacement`, so a slow or wedged
    // debug request can never contend with real placement traffic).
    let node_registry_dump_source = match &native_registry_handle {
        Some(registry) => NodeRegistryDumpSource::Native(Arc::clone(registry)),
        None => {
            let endpoint = config
                .cluster
                .scheduler_endpoint
                .as_deref()
                .map(str::trim)
                .filter(|endpoint| !endpoint.is_empty())
                .expect("cluster_placement already required a non-empty scheduler_endpoint");
            let channel = tonic::transport::Endpoint::from_shared(
                agentenv::scheduler_endpoint::qualified(endpoint),
            )
            .context("build the node-registry dump's scheduler-proxy channel")?
            .connect_lazy();
            NodeRegistryDumpSource::SchedulerProxy(channel)
        }
    };

    let grpc = {
        let served = Arc::clone(&api_impl);
        spawn_grpc_surface(
            &config.cluster.api_grpc_addr,
            "sandbox resume service",
            move |listener, shutdown| {
                agentenv::api::grpc::serve_on(
                    listener,
                    served,
                    node_registry_grpc_service,
                    shutdown,
                )
            },
        )
        .await?
    };

    // 🔴 The warm-up clock, for real this time: `start_native_node_registry`
    // had to arm `WarmupGate` before this listener existed (it hands the
    // gate to `NodeRegistryGrpcService`, which the closure above serves) —
    // see that construction's own comment. Now that the listener has
    // actually bound and can receive a `Heartbeat` RPC, rebase the deadline
    // to start counting from here, not from wherever assembly happened to
    // be earlier. A no-op under `[cluster].node_placement_source =
    // "scheduler"` (`native_warmup_handle` is `None`) and cheap even when it
    // is not — see `WarmupGate::rebase_deadline`'s own doc comment for why
    // this is safe to call unconditionally, including on a gate that has
    // already gone warm.
    if let Some(warmup) = native_warmup_handle.as_ref() {
        warmup.rebase_deadline(
            std::time::SystemTime::now(),
            Duration::from_secs(config.cluster.native_warmup_timeout_secs),
        );
    }

    // 🔴 P5: built as its own `Router` and merged into the *generated*
    // control-plane router by `server::new_with_control_plane_routes`,
    // rather than `.route(..)`-ed onto the fully assembled `Router` `server::new`
    // hands back. axum's `Router::layer` only covers routes registered
    // before it runs, so a route added after `server::new` returns — after
    // every layer, including `require_control_plane` and the role gate —
    // was never behind either. This endpoint answers every node's internal
    // address, its resource allocation and the cluster's CPU-config
    // intersection, and is reachable through the gateway's REST fan-out the
    // same as any other unrecognized path (`services/gateway/internal/server.go`).
    // See `agentenv::api::server::new_with_control_plane_routes`'s own doc
    // comment for the full argument, including why "zero impact by default"
    // is not an accurate description of adding this endpoint at all.
    let node_registry_debug_routes = axum::Router::new().route(
        "/debug/node-registry",
        axum::routing::get(move || {
            let source = node_registry_dump_source.clone();
            async move { axum::Json(agentenv::node_registry::dump::dump(&source).await) }
        }),
    );

    Ok(Assembly {
        // 🔴 No RoleGate: this half serves the whole user-facing surface. The
        // gate exists to stop a *node* answering it.
        app: server::new_with_control_plane_routes(api_impl, role, node_registry_debug_routes),
        orchestration,
        upkeep: paused_upkeep,
        pg_singleton_tasks,
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

/// Where this half asks where sandboxes go. Under `Native`, every method
/// answers from api's own process — [`NativeNodePlacement`] wraps the local
/// node registry (`resolve_node`/`node_membership`) and a clone of the same
/// `node_registry_grpc_service` `assemble_api` serves `Schedule`/
/// `LookupNode`/`RecordAssignment` from over the wire (`place_new`/
/// `place_existing`/`record_placement`) — see that type's own module doc
/// for the P1 fix this replaced ("all five forward to the scheduler" ->
/// "all five answer locally"). Under `Scheduler` (the default), this is
/// unchanged from before P1: a `SchedulerNodePlacement` dialling
/// `[cluster].scheduler_endpoint`, byte-for-byte.
///
/// 🔴 P1 (task's own "phase4-close"): `[cluster].scheduler_endpoint` is now
/// required *only* under `Scheduler` — the doc comment this replaced said
/// "`Native` is not a way to run `--role api` without a scheduler," and
/// that was the bug: three of `NativeNodePlacement`'s five methods used to
/// forward to a `SchedulerNodePlacement` regardless, so `--role api` under
/// `Native` was simultaneously the gRPC server for `Schedule`/`LookupNode`/
/// `RecordAssignment` (Stage D) and a client of the Go scheduler for those
/// same three calls, never reaching its own answers. Scaling that scheduler
/// to zero left every create failing. `Native` now needs no scheduler
/// endpoint at all.
fn cluster_placement(
    config: &agentenv::cfg::ClusterConfig,
    scheduler_report: &agentenv::cfg::ObservabilitySchedulerReportConfig,
    native_registry: Option<&Arc<AtomicNodeRegistry>>,
    native_warmup: Option<&Arc<WarmupGate>>,
    native_grpc_service: Option<&NodeRegistryGrpcService>,
) -> anyhow::Result<Arc<dyn agentenv::node_client::NodePlacement>> {
    match (
        config.node_placement_source,
        native_registry,
        native_warmup,
        native_grpc_service,
    ) {
        (NodePlacementSource::Native, Some(registry), Some(warmup), Some(local)) => {
            Ok(Arc::new(NativeNodePlacement::new(
                Arc::clone(registry),
                config.node_service_port,
                Arc::clone(warmup),
                local.clone(),
            )))
        }
        _ => {
            let endpoint = config
                .scheduler_endpoint
                .as_deref()
                .map(str::trim)
                .filter(|endpoint| !endpoint.is_empty())
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "--role api needs [cluster].scheduler_endpoint \
                         (AENV_OBSERVABILITY_SCHEDULER_ENDPOINT): it owns sandboxes it does not \
                         run, so every create has to be placed by the scheduler and there is no \
                         machine here to fall back to"
                    )
                })?;
            let scheduler = SchedulerNodePlacement::connect_hot_reloadable(
                endpoint,
                config,
                scheduler_report,
                config.node_service_port,
            )?;
            Ok(Arc::new(scheduler))
        }
    }
}

/// Bundles what `[cluster].node_placement_source = "native"` needs running
/// before `cluster_placement` can hand a [`NativeNodePlacement`] a registry
/// to read: the registry itself, the heartbeat-receiving gRPC service that
/// feeds it (task's own "D5"), and the background tasks that keep both
/// current (kube discovery, the observed-nodes metrics gauge). Callers merge
/// `tasks` into the role's own `upkeep` and pass `grpc_service` to
/// `agentenv::api::grpc::serve_on`.
///
/// 🔴 Under `[cluster].node_placement_source = "scheduler"` (the default),
/// nothing in `assemble_api` calls this at all — no registry, no kube
/// client, no gRPC service, matching the task's own instruction that the
/// default path's dependency footprint must be unchanged.
struct NativeNodeRegistryBits {
    registry: Arc<AtomicNodeRegistry>,
    /// The same `Arc` handed to `grpc_service` below — cloned out here too
    /// so `cluster_placement` can give `NativeNodePlacement` a handle on the
    /// gate a heartbeat this process actually receives opens. See
    /// `NativeNodePlacement`'s own module doc for why an unwired gate would
    /// leave every `resolve_node`/`node_membership` call answering as
    /// confidently absent as a registry that had just started.
    warmup: Arc<WarmupGate>,
    grpc_service: NodeRegistryGrpcService,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

/// How often the observed-nodes-by-status gauge refreshes, and the interval
/// `runKubernetesDiscoveryWithRetry`'s Go counterpart's initial/maximum
/// reconnect backoff bracket (`services/scheduler/cmd/main.go`'s
/// `runKubernetesDiscoveryWithRetry`).
const NODE_REGISTRY_METRICS_INTERVAL: Duration = Duration::from_secs(15);
const KUBE_DISCOVERY_INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const KUBE_DISCOVERY_MAX_BACKOFF: Duration = Duration::from_secs(30);

async fn start_native_node_registry(
    config: &agentenv::cfg::ClusterConfig,
    dual_report_api_endpoint: &str,
) -> anyhow::Result<NativeNodeRegistryBits> {
    let discovery = &config.kubernetes_discovery;
    let namespace = discovery.namespace.trim();
    let service_name = discovery.service_name.trim();
    // 🔴 Refused here, before anything is connected — the same discipline
    // `cluster_placement`'s own doc comment names: a misconfigured replica
    // should be told about the setting it can see from its own config,
    // rather than have a retry loop fail silently in the background forever
    // for a reason a startup log line would have caught immediately.
    if namespace.is_empty() || service_name.is_empty() {
        anyhow::bail!(
            "--role api needs [cluster.kubernetes_discovery].namespace and .service_name \
             (AENV_CLUSTER_KUBERNETES_DISCOVERY_NAMESPACE / \
             AENV_CLUSTER_KUBERNETES_DISCOVERY_SERVICE_NAME) when \
             [cluster].node_placement_source = \"native\": Stage A's node registry has nothing \
             to discover nodes from otherwise"
        );
    }
    // 🔴 P1's second fix, revised: this used to be a hard `anyhow::bail!`
    // refusing to start. It was refusing on the wrong process's config.
    // `dual_report_api_endpoint` (`cfg.rs`'s own doc: "lets *a node* report
    // to both scheduler ... and api's registry") is a *sender*-side setting
    // — it belongs to whatever process calls `ObservabilityReporter::
    // send_heartbeat`, i.e. `--role node`/`--role all`, and is read there
    // from *that* process's own `[observability.scheduler_report]` section.
    // This function runs on `--role api`, where the same config struct
    // exists but nothing ever consumes this particular field — `assemble_api`
    // deliberately never starts a reporter (`reporter: None` in its
    // `Assembly`; this half receives heartbeats, it does not send them). So
    // this branch was gating api's own startup on a copy of a setting that
    // does nothing on api regardless of its value, and the deployment
    // manifest's own rollout instructions (`agentenv-api-deployment.yaml`)
    // never set it there — following those instructions exactly as written
    // hit this bail every time and CrashLoopBackOff'd.
    //
    // What is actually true and worth saying out loud: under `Native`, this
    // registry's only source of heartbeats is some node's dual report to
    // this replica's own `[cluster].api_grpc_addr` (fronted by a Service
    // reaching every replica, e.g. http://agentenv-api:8002) — never
    // `[cluster].scheduler_endpoint`, which is where the primary heartbeat
    // always goes instead. If no node is configured that way, `WarmupGate`
    // never sees a single `reported_in`, never leaves warm-up, and every
    // `resolve_node`/`node_membership` call refuses for the process's whole
    // life — indistinguishable at the call site from a scheduler that is
    // permanently down. That is real and worth a loud warning; it is just
    // not something this process's own config can confirm or deny, so it
    // cannot be a startup refusal. Whether any node dual-reports here is
    // verified operationally (`/debug/node-registry`, or the
    // `agentenv_node_registry_observed_nodes` metric), not from this value.
    if dual_report_api_endpoint.trim().is_empty() {
        tracing::warn!(
            "starting with [cluster].node_placement_source = \"native\" and this replica's own \
             [observability.scheduler_report].dual_report_api_endpoint unset — that is normal \
             here (it is a node-side setting; this process never sends heartbeats with it, see \
             this branch's own comment) and not itself evidence of a problem. What matters is \
             whether *some* node is configured with \
             AENV_OBSERVABILITY_DUAL_REPORT_API_ENDPOINT pointed at this replica's own \
             [cluster].api_grpc_addr (fronted by a Service reaching every replica, for example \
             http://agentenv-api:8002). If none is, this registry never leaves warm-up and \
             every resolve_node/node_membership call refuses for this process's whole life; \
             confirm real heartbeats are landing (/debug/node-registry, or the \
             agentenv_api_node_registry_observed_nodes metric) before relying on native \
             placement"
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
    // Same validation `KubernetesDiscovery::new` runs internally, run once
    // here so a bad selector fails startup instead of failing the same way,
    // silently, on every iteration of the retry loop below forever.
    validate_optional_pod_selector(&kube_config.ignore_pod_selector, "ignore_pod_selector")?;
    validate_optional_pod_selector(
        &kube_config.no_schedule_pod_selector,
        "no_schedule_pod_selector",
    )?;

    let registry = Arc::new(AtomicNodeRegistry::with_empty_sync_guard(
        Vec::new(),
        Duration::from_secs(30),
        agentenv::node_registry::registry::EmptySyncGuard {
            confirmations: discovery.empty_sync_confirmations,
            window: Duration::from_secs(discovery.empty_sync_window_secs),
        },
    ));
    // 🔴 This is only the gate's *initial* arming — `now` here is when
    // assembly reached this point, well before the gRPC listener this
    // registry's `Heartbeat` RPC arrives on actually binds.
    // `assemble_api` rebases this deadline (`WarmupGate::rebase_deadline`)
    // once that listener is actually up; see that call site's own comment
    // and `AENV_CLUSTER_NATIVE_WARMUP_TIMEOUT_SECS`'s doc comment (`cfg.rs`)
    // for why the two clocks must not be the same one.
    let warmup = Arc::new(WarmupGate::new(
        Arc::clone(&registry) as Arc<dyn agentenv::node_registry::registry::NodeRegistry>,
        Duration::from_secs(config.native_warmup_timeout_secs),
        std::time::SystemTime::now(),
    ));
    let grpc_service = NodeRegistryGrpcService::new(Arc::clone(&registry), Arc::clone(&warmup));

    let discovery_task = {
        let registry = Arc::clone(&registry);
        tokio::spawn(run_kubernetes_discovery_with_retry(kube_config, registry))
    };
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

    Ok(NativeNodeRegistryBits {
        registry,
        warmup,
        grpc_service,
        tasks: vec![discovery_task, metrics_task],
    })
}

/// Task's own "D3": constructs the binding store `assemble_api` wires into
/// `NodeRegistryGrpcService` (`with_binding_store`) under
/// `[cluster].node_placement_source = "native"`. Mirrors
/// `RedisMetadataStore::connect`'s own error-wrapping style — a connection
/// failure here is a startup refusal, not a background retry, the same
/// discipline `cluster_placement`'s own comment names for every other
/// config-driven refusal in this function.
///
/// 🔴 P4 (task's own "phase4-close"): also mirrors `cluster_store_config`'s
/// own refusal of the in-memory metadata store — `BindingStoreBackendKind::InMemory`
/// is refused here the same way, for the same reason (one replica's private
/// state, `--role api` runs as more than one replica), and unconditionally
/// for the same reason: nothing at this layer can distinguish "one replica,
/// alone, safe" from "one of several, silently wrong."
async fn build_binding_store(config: &BindingStoreConfig) -> anyhow::Result<Arc<dyn BindingStore>> {
    let settings = BindingStoreSettings {
        binding_ttl: Duration::from_secs(config.binding_ttl_secs),
        arbitration: ArbitrationMode::from_str_relaxed(&config.arbitration),
        projection_authoritative: config.projection_authoritative,
    };
    match config.backend {
        // 🔴 P4 (task's own "phase4-close"): refused unconditionally, the
        // same discipline `cluster_store_config` already applies to
        // `[orchestrator.store].backend` above -- `--role api` is a
        // multi-replica Deployment (`deploy/k8s/base/agentenv-api-deployment.yaml`'s
        // `replicas: 2`), and an in-memory binding store is one replica's
        // private routing table: `Schedule`/`LookupNode`/`RecordAssignment`
        // answers from it, and nothing propagates a write on one replica to
        // any other. Two replicas each holding a different, invisible
        // answer for the same sandbox is not a degraded mode this process
        // may run in — every symptom is silent (a gateway routing-projection
        // read, or `NativeNodePlacement::place_existing` on *this* replica,
        // simply missing a binding another replica wrote) — and nothing
        // here can tell whether this process is one replica of many or
        // genuinely alone, so this refuses regardless of how many are
        // actually running.
        BindingStoreBackendKind::InMemory => {
            anyhow::bail!(
                "[cluster].node_placement_source = \"native\" needs [binding_store].backend = \
                 \"redis\" (AENV_BINDING_STORE_BACKEND): the in-memory binding store is one \
                 replica's private routing table, and --role api runs as more than one \
                 replica. Set AENV_BINDING_STORE_BACKEND=redis, or keep \
                 [cluster].node_placement_source = \"scheduler\""
            );
        }
        BindingStoreBackendKind::Redis => {
            let redis_config = RedisBindingStoreConfig {
                url: config.redis_url.clone(),
                key_prefix: config.redis_key_prefix.clone(),
                node_index_ttl: Duration::from_secs(config.redis_node_index_ttl_secs),
                ..Default::default()
            };
            let store = RedisBindingStore::connect(redis_config, settings)
                .await
                .map_err(|err| anyhow::anyhow!("connecting the binding store to redis: {err}"))?;
            Ok(Arc::new(store) as Arc<dyn BindingStore>)
        }
    }
}

/// The shared-roster fix: wires `--role api`'s Stage A node registry's
/// heartbeat-derived (`observed`) state into Redis so every replica sees
/// the whole cluster's roster, not just the nodes whose heartbeat happens
/// to be pinned to it — see `agentenv::node_registry::redis`'s own module
/// doc for the full design.
///
/// Mirrors `build_binding_store`'s own multi-replica guardrail exactly, for
/// the same reason: nothing at this layer can distinguish "one replica,
/// alone, safe" from "one of several, silently split," and this is only
/// ever called from `assemble_api`, only when `native_registry_handle` is
/// `Some` — i.e. only under `[cluster].node_placement_source = "native"`,
/// the same trigger `build_binding_store` refuses
/// `BindingStoreBackendKind::InMemory` under. So
/// `NodeRegistryObservedBackendKind::InMemory` is refused here
/// unconditionally too, regardless of how many replicas are actually
/// running.
///
/// On success, spawns the background task
/// (`agentenv::node_registry::redis::run_shared_observed_sync`) that keeps
/// `registry` in sync going forward and returns its `JoinHandle` for the
/// caller to fold into its own upkeep — the same pattern
/// `start_native_node_registry` already uses for the kube-discovery and
/// metrics tasks.
async fn wire_shared_node_observed_store(
    config: &ClusterNodeRegistryStoreConfig,
    registry: &Arc<AtomicNodeRegistry>,
) -> anyhow::Result<Option<tokio::task::JoinHandle<()>>> {
    match config.backend {
        NodeRegistryObservedBackendKind::InMemory => {
            anyhow::bail!(
                "[cluster].node_placement_source = \"native\" needs \
                 [cluster.node_registry_store].backend = \"redis\" \
                 (AENV_CLUSTER_NODE_REGISTRY_STORE_BACKEND): the in-memory node registry only \
                 sees the nodes whose heartbeat happens to be pinned to this replica, and \
                 --role api runs as more than one replica. Set \
                 AENV_CLUSTER_NODE_REGISTRY_STORE_BACKEND=redis, or keep \
                 [cluster].node_placement_source = \"scheduler\""
            );
        }
        NodeRegistryObservedBackendKind::Redis => {
            let store = SharedObservedStore::connect(SharedObservedStoreConfig {
                url: config.redis_url.clone(),
                key_prefix: config.redis_key_prefix.clone(),
            })
            .await?;
            let rx = registry.enable_shared_observed_publishing();
            // 🔴 Best-effort, not a startup refusal: the connection above
            // already proved Redis is reachable, so a failure here is a
            // transient blip on an otherwise-good connection —
            // `run_shared_observed_sync`'s own periodic pull will retry
            // within `DEFAULT_PULL_INTERVAL` regardless. Doing this pull
            // now rather than waiting for the loop's first tick matters
            // for correctness, not just latency: without it, a freshly
            // (re)started replica's `Inner.all_configs_ready` gate would
            // see only the nodes it has personally heartbeated with so far
            // for up to a whole `DEFAULT_PULL_INTERVAL` — exactly the
            // partial-cluster view this fix exists to close.
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

/// Mirrors `services/scheduler/cmd/main.go`'s `runKubernetesDiscoveryWithRetry`:
/// (re)connects and runs discovery, and on any failure — connecting or the
/// watch loop itself ending — waits an exponentially growing backoff and
/// tries again. Never returns under normal operation; callers drive it as a
/// background task.
///
/// 🔴 Does not special-case an in-cluster-config failure the way Go's
/// version does (`errors.Is(err, rest.ErrNotInCluster)` stops retrying
/// outright there): `kube::Config::infer` does not expose an equivalently
/// precise "this will never succeed" signal to distinguish from "the
/// apiserver is transiently unreachable", so every failure here is treated
/// as retryable. The cost is a Pod that will never have in-cluster
/// credentials logging a warning every `KUBE_DISCOVERY_MAX_BACKOFF` forever
/// instead of failing loudly once — a known, narrower gap than the retry
/// loop's own reason for existing (a transiently unreachable apiserver at
/// startup must not be fatal).
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

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    /// 🔴 `--role` no longer selects anything; it confirms. See
    /// [`ServerRole::confirm`] and `role::confirm_tests` for the arms.
    #[test]
    fn the_api_binary_is_the_api_half() {
        ApiCli::command().debug_assert();

        let bare = ApiCli::parse_from(["aenv-api"]);
        assert_eq!(bare.role, None, "no --role means this binary's own half");
        assert_eq!(
            ServerRole::Api.confirm_with(bare.role, None).unwrap(),
            ServerRole::Api
        );
        assert!(
            ServerRole::Api
                .confirm_with(Some(ServerRole::Node), None)
                .is_err(),
            "this binary links no sandbox runtime; it cannot be the node half"
        );

        // 🔴 The provisioning flags are gone from this binary rather than
        // refused by it. `--setup-host` provisions KVM, ublk and host
        // networking; there is nothing here that could use any of it.
        assert!(
            ApiCli::try_parse_from(["aenv-api", "--setup-host"]).is_err(),
            "the api binary has no host-provisioning mode to offer"
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

    #[tokio::test]
    async fn the_api_half_refuses_to_place_sandboxes_with_nothing_to_ask() {
        let mut config = AppConfig::default();
        assert_eq!(
            config.cluster.scheduler_endpoint, None,
            "the default is no endpoint, which is what makes this refusal necessary"
        );

        let err = match cluster_placement(
            &config.cluster,
            &config.observability.scheduler_report,
            None,
            None,
            None,
        ) {
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
            cluster_placement(
                &config.cluster,
                &config.observability.scheduler_report,
                None,
                None,
                None
            )
            .is_err(),
            "a blank endpoint is not an endpoint"
        );

        // 🔴 The control, again: a real endpoint resolves, so the two refusals
        // above are about what was missing.
        config.cluster.scheduler_endpoint = Some("http://scheduler:9090".to_string());
        assert!(
            cluster_placement(
                &config.cluster,
                &config.observability.scheduler_report,
                None,
                None,
                None
            )
            .is_ok(),
            "a configured endpoint is what this role runs on"
        );
    }

    /// 🔴 P1 (task's own "phase4-close"): the other half of the same claim —
    /// under `Native`, with a real local registry/warmup/gRPC service handed
    /// in, `cluster_placement` must succeed with **no**
    /// `[cluster].scheduler_endpoint` configured at all. Before P1,
    /// `cluster_placement` read and refused on a missing endpoint
    /// unconditionally, before it ever looked at `node_placement_source` —
    /// so this exact call would have failed with "needs
    /// [cluster].scheduler_endpoint", even though nothing in this test's
    /// setup ever needs to dial one.
    #[test]
    fn native_placement_needs_no_scheduler_endpoint() {
        let mut config = AppConfig::default();
        config.cluster.node_placement_source = NodePlacementSource::Native;
        assert_eq!(
            config.cluster.scheduler_endpoint, None,
            "the whole point of this test is that native mode does not need one"
        );

        let registry = Arc::new(AtomicNodeRegistry::new(Vec::new(), Duration::from_secs(30)));
        let warmup = Arc::new(WarmupGate::new(
            Arc::clone(&registry) as Arc<dyn agentenv::node_registry::registry::NodeRegistry>,
            Duration::from_secs(15),
            std::time::SystemTime::now(),
        ));
        let grpc_service = NodeRegistryGrpcService::new(Arc::clone(&registry), Arc::clone(&warmup));

        let placement = cluster_placement(
            &config.cluster,
            &config.observability.scheduler_report,
            Some(&registry),
            Some(&warmup),
            Some(&grpc_service),
        );
        assert!(
            placement.is_ok(),
            "[cluster].node_placement_source = \"native\" must not require \
             [cluster].scheduler_endpoint: {:?}",
            placement.err()
        );
    }

    /// 🔴 P4 (task's own "phase4-close"): the binding store's own copy of
    /// `the_api_half_refuses_a_ledger_no_other_replica_can_see` above —
    /// same shape, same reasoning, a different multi-replica ledger.
    /// `[binding_store]`'s own doc comment on `backend` already named this
    /// exact risk ("every replica answers LookupNode ... out of its own,
    /// mutually invisible table") with nothing enforcing it; before this
    /// guard existed, `build_binding_store` happily built an
    /// `InMemoryBindingStore` for `--role api` regardless of how many
    /// replicas were actually running.
    #[tokio::test]
    async fn the_api_half_refuses_a_binding_ledger_no_other_replica_can_see() {
        let config = AppConfig::default().binding_store;
        assert_eq!(
            config.backend,
            BindingStoreBackendKind::InMemory,
            "the default is the per-replica table, which is what makes this refusal necessary"
        );

        let err = match build_binding_store(&config).await {
            Ok(_) => panic!("the in-memory binding store is one replica's private routing table"),
            Err(err) => err.to_string(),
        };
        assert!(err.contains("binding_store"), "{err}");
        assert!(err.contains("AENV_BINDING_STORE_BACKEND"), "{err}");
        assert!(err.contains("redis"), "{err}");

        // 🔴 The control, again: the redis backend at least attempts to
        // connect rather than being refused outright — proven by getting a
        // *different* error (a connection failure, not the multi-replica
        // refusal) against a URL nothing is listening on.
        let mut redis_config = AppConfig::default().binding_store;
        redis_config.backend = BindingStoreBackendKind::Redis;
        redis_config.redis_url = "redis://127.0.0.1:1/0".to_string();
        let connect_err = match build_binding_store(&redis_config).await {
            Ok(_) => panic!("nothing is listening on this port"),
            Err(err) => err.to_string(),
        };
        assert!(
            !connect_err.contains("more than one replica"),
            "a redis backend must fail on the connection, not on the multi-replica refusal: \
             {connect_err}"
        );
    }

    /// The shared-roster fix's own copy of
    /// `the_api_half_refuses_a_binding_ledger_no_other_replica_can_see` above
    /// — same shape, same reasoning, a third multi-replica ledger
    /// (`Inner.observed`, `src/node_registry/registry.rs`). Before this
    /// guard existed, `AtomicNodeRegistry`'s heartbeat-derived state had no
    /// cross-replica sharing at all and nothing refused starting that way.
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

        // The control, again: the redis backend at least attempts to
        // connect rather than being refused outright — a *different* error
        // (a connection failure, not the multi-replica refusal) against a
        // URL nothing is listening on.
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

    /// 🔴 F2: `[observability.scheduler_report].dual_report_api_endpoint` is
    /// a node-side setting (`cfg.rs`'s own doc: it lets *a node* dual-report
    /// to api's registry) with no runtime consumer on `--role api` at all
    /// (`assemble_api` never starts a reporter). Gating api's own startup on
    /// its own copy of this value — as this function used to, with a hard
    /// `anyhow::bail!` — meant that following the deployment manifest's own
    /// rollout instructions exactly as written (set the var on the
    /// DaemonSet, then flip `AENV_NODE_PLACEMENT_SOURCE=native` on the api
    /// Deployment, which never sets this var itself) hit that bail on every
    /// startup and CrashLoopBackOff'd. This asserts the fix: leaving it
    /// unset now only warns (see the branch's own comment) instead of
    /// refusing to start.
    ///
    /// The kubernetes_discovery check just above it in the function is
    /// unrelated to this fix and still a hard refusal; the control at the
    /// end proves that one is untouched.
    #[tokio::test]
    async fn native_placement_without_a_dual_report_endpoint_warns_but_starts() {
        let mut cluster = agentenv::cfg::ClusterConfig {
            node_placement_source: NodePlacementSource::Native,
            ..AppConfig::default().cluster
        };
        cluster.kubernetes_discovery.namespace = "agentenv-system".to_string();
        cluster.kubernetes_discovery.service_name = "agentenv-nodes".to_string();

        // `NativeNodeRegistryBits` (the `Ok` type) does not implement
        // `Debug` — it holds a live gRPC service and task handles, which is
        // not something a test wants to print — so this checks `.is_ok()`
        // rather than using `.unwrap()`/`expect_err`.
        assert!(
            start_native_node_registry(&cluster, "").await.is_ok(),
            "an empty dual_report_api_endpoint must not stop --role api from starting — this \
             process never consumes it"
        );

        // Blank is the same as absent, same convention as
        // `cluster_placement`'s own scheduler_endpoint refusal above.
        assert!(
            start_native_node_registry(&cluster, "   ").await.is_ok(),
            "a blank (whitespace-only) endpoint must be treated the same as an absent one"
        );

        // The control: a configured value obviously must not stop it either
        // — proves the two assertions above are actually about the value
        // being empty, not about this function always succeeding regardless
        // of what is passed.
        assert!(
            start_native_node_registry(&cluster, "http://agentenv-api:8002")
                .await
                .is_ok(),
            "a configured endpoint must still start cleanly"
        );

        // Control: an empty namespace/service_name is still a hard refusal,
        // unaffected by this change — proves the check ahead of this one in
        // the function was not also weakened.
        let unconfigured_discovery = agentenv::cfg::ClusterConfig {
            node_placement_source: NodePlacementSource::Native,
            ..AppConfig::default().cluster
        };
        let err = match start_native_node_registry(&unconfigured_discovery, "").await {
            Ok(_) => panic!("an empty namespace/service_name must still be refused"),
            Err(err) => err.to_string(),
        };
        assert!(
            err.contains("kubernetes_discovery"),
            "an empty namespace/service_name must be refused before the dual-report check \
             is ever reached: {err}"
        );
    }
}
