use std::collections::{BTreeSet, HashMap};
use std::sync::{
    atomic::{AtomicI64, AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, SystemTime};

use anyhow::Context;
use tokio::sync::{broadcast, oneshot, watch, Mutex, OnceCell, RwLock};
use tokio::time::MissedTickBehavior;
use tracing::{debug, error, info, trace, warn};

use crate::cfg::ConfigManager;
use crate::image::{RuntimeImageOwner, RuntimeImageRefs};
use crate::sandbox::{
    AccessTokenSeedPolicy, CustomExtensionClient, CustomExtensionParams, EnvdAccessToken,
    FreshSandboxBuildSpec, RuntimeArtifactSet, RuntimeConfirmedGone, SandboxAccessTokenGenerator,
    SandboxBackend, SandboxBackendFactory, SandboxForkSpec, SandboxLaunchConfig,
    SandboxNetworkPolicy, UnresolvedImageBuildSpec,
};
use crate::types::{ExecutionId, SandboxId, SandboxResources};

use super::grants::{GrantIssuer, NoGrants};
use super::launch_claim::{
    LaunchClaims, LaunchFailure, LaunchHeldElsewhere, LaunchSettlement, RestoredSandbox,
};
use super::launch_parts::{
    configured_runtime_versions, default_fresh_sandbox_resources, resources_with_runtime_info,
    snapshot_create_parts, SnapshotCreateInputs, SnapshotCreateParts,
};
use super::launch_plan::{LaunchPlan, LaunchSource};
use super::metrics::{
    aggregate_resource_metrics, OrchestratorCounters, OrchestratorMetrics, SandboxContribution,
};
use super::pause_publisher::PausePublisher;
use super::proxy::{ProxyLookupResult, ProxyRoute, ProxyRouteTable, ProxyTarget};
use super::runtime_routing::RuntimeRouting;
use super::store::*;
use super::types::{
    CreateSandboxRequest, ForkChildAssignment, ForkChildren, LiveSandbox, PauseOutcome,
    PublishedPause, SandboxExpiry, SandboxLaunchSource, SandboxLifecycleEvent,
    SandboxLifecycleEventType, SandboxRosterEntry, SandboxState, SnapshotCaptureResult,
};
use super::{OrchestratorError, Result, SandboxForkOutcome, SandboxOperation};

type SandboxHandle = Arc<Mutex<Box<dyn SandboxBackend>>>;

/// Maximum time to wait for a sandbox to leave a transitional state.
/// Guards against indefinite blocking when a sandbox's in-progress operation
/// never completes (e.g. the task holding the state panics without rolling back).
const WAIT_TRANSITION_TIMEOUT: Duration = Duration::from_secs(60);
/// How often a caller waiting out another replica's launch re-reads the shared
/// record. The launch it waits on writes that record once, from another
/// process, so there is nothing local to be woken by.
const LAUNCH_ELSEWHERE_POLL: Duration = Duration::from_millis(250);
const SANDBOX_EVENT_CHANNEL_CAPACITY: usize = 1024;

/// Maximum expired sandboxes processed per eviction round.
const AUTO_EVICT_BATCH_LIMIT: usize = 256;

/// A grant younger than this is left alone by the reaper: a create grants
/// before it writes the record, and a resume carries the record across
/// nodes, so a fresh grant with no record yet is not an orphan.
const GRANT_REAP_MIN_AGE: Duration = Duration::from_secs(10 * 60);
const GRANT_REAP_BATCH_LIMIT: usize = 256;

/// Commit attempts for a pause whose VM can no longer be resumed. The bytes
/// are the only copy of the sandbox from the first attempt on, so the commit
/// is retried rather than abandoned.
const PAUSE_PUBLICATION_ATTEMPTS: u32 = 5;
/// Delay before the second attempt; each further wait doubles it.
const PAUSE_PUBLICATION_RETRY_BACKOFF: Duration = Duration::from_secs(2);
/// Wall-clock budget for the retries, measured from the first failed commit.
/// An attempt whose backoff would end after it is not started, so the number
/// of attempts is a ceiling and this is the bound.
const PAUSE_PUBLICATION_RETRY_DEADLINE: Duration = Duration::from_secs(45);
/// Slack over that budget for the capture, the commit that failed first and
/// the attempt still running when the budget ends.
const PAUSE_JOIN_MARGIN: Duration = Duration::from_secs(30);
/// How long a caller waits out a pause somebody else is performing. It has to
/// cover the owner's whole retry budget, or it answers "stuck" about a pause
/// that is still working.
const CONCURRENT_PAUSE_WAIT: Duration =
    Duration::from_secs(PAUSE_PUBLICATION_RETRY_DEADLINE.as_secs() + PAUSE_JOIN_MARGIN.as_secs());

#[derive(Clone, Debug)]
enum ShutdownOutcome {
    Success,
    Failed(String),
}

impl ShutdownOutcome {
    fn from_result(result: Result<()>) -> Self {
        match result {
            Ok(()) => Self::Success,
            Err(OrchestratorError::InternalError(message)) => Self::Failed(message),
            Err(err) => Self::Failed(err.to_string()),
        }
    }

    fn as_result(&self) -> Result<()> {
        match self {
            Self::Success => Ok(()),
            Self::Failed(message) => Err(OrchestratorError::InternalError(message.clone())),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FailedLaunchStage {
    Registered,
    TransitionalPersisted,
    RunningPersisted,
}

impl FailedLaunchStage {
    fn rollback_expected_state(self, plan: &LaunchPlan) -> Option<SandboxState> {
        match self {
            Self::Registered => None,
            Self::TransitionalPersisted => Some(plan.transitional_state()),
            Self::RunningPersisted => Some(SandboxState::Running),
        }
    }

    fn should_detach_proxy_route(self) -> bool {
        matches!(self, Self::RunningPersisted)
    }
}

pub struct Orchestrator<S: MetadataStore, F: SandboxBackendFactory> {
    store: S,
    factory: F,
    sandboxes: RwLock<HashMap<SandboxId, SandboxHandle>>,
    proxy_routes: RwLock<ProxyRouteTable>,
    next_proxy_route_version: AtomicU64,
    counters: OrchestratorCounters,
    sandbox_event_tx: broadcast::Sender<SandboxLifecycleEvent>,
    default_sandbox_timeout: Duration,
    is_shutting_down: std::sync::atomic::AtomicBool,
    /// Node isolation rejects new placements while preserving existing sandboxes.
    scheduling_disabled: std::sync::atomic::AtomicBool,
    /// Unix milliseconds of the last isolation change; zero means never.
    scheduling_disabled_changed_at_ms: AtomicI64,
    shutdown_tx: watch::Sender<bool>,
    shutdown_outcome: OnceCell<ShutdownOutcome>,
    pub image_refs: Arc<dyn RuntimeImageRefs>,
    access_tokens: SandboxAccessTokenGenerator,
    /// Where a pause capture becomes durable. Without one a pause cannot
    /// succeed, because a paused sandbox exists only as what it published.
    pause_publisher: OnceCell<Arc<dyn PausePublisher>>,
    /// Who records which secret names an incarnation may read; `NoGrants`
    /// until the api half installs its store.
    grants: OnceCell<Arc<dyn GrantIssuer>>,
    /// Who says whether the cluster still routes to a sandbox's runtime. A
    /// process whose sandboxes run inside it installs none: its own handles
    /// are the answer.
    runtime_routing: OnceCell<Arc<dyn RuntimeRouting>>,
    /// The sandbox ids this process is launching. A launch takes its id here
    /// before it allocates anything, which is the only point early enough to
    /// keep two launches of one id from ever running together.
    launch_claims: Arc<LaunchClaims>,
}

/// Outcome when a process-local sandbox handle is absent.
enum AbsentHandle {
    /// Backend adopted from the authoritative record.
    Adopted(SandboxHandle),
    /// Runtime independently confirmed gone; record cleanup is allowed.
    RuntimeGone,
    /// No authoritative record exists.
    NoRecord,
}

impl<F> Orchestrator<InMemoryMetadataStore, F>
where
    F: SandboxBackendFactory,
{
    /// Creates a test orchestrator with an in-memory store whose pauses are
    /// published nowhere.
    #[cfg(any(test, feature = "test-support"))]
    pub async fn with_in_memory_store(factory: F) -> Arc<Self> {
        let orchestrator = Self::new(
            AccessTokenSeedPolicy::MayGenerate,
            InMemoryMetadataStore::new(),
            factory,
            crate::image::DisabledRuntimeImageRefs::shared(),
        )
        .await
        .expect("in-memory orchestrator should never fail to initialize");
        orchestrator
            .set_pause_publisher(super::pause_publisher::DiscardingPausePublisher::shared());
        orchestrator
    }

    /// Creates the node-local orchestrator: one process's ledger of the VMs it
    /// runs, rebuilt empty on every start.
    pub async fn with_in_memory_store_and_factory(
        factory: F,
        image_refs: Arc<dyn RuntimeImageRefs>,
    ) -> Result<Arc<Self>> {
        Self::new(
            AccessTokenSeedPolicy::MayGenerate,
            InMemoryMetadataStore::new(),
            factory,
            image_refs,
        )
        .await
    }
}

impl<S, F> Orchestrator<S, F>
where
    S: MetadataStore + 'static,
    F: SandboxBackendFactory,
{
    /// Creates an orchestrator without background tasks for tests.
    #[cfg(any(test, feature = "test-support"))]
    pub fn from_test_parts(
        store: S,
        factory: F,
        default_sandbox_timeout: std::time::Duration,
        image_refs: std::sync::Arc<dyn crate::image::RuntimeImageRefs>,
        access_token_seed: &str,
    ) -> Self {
        let (sandbox_event_tx, _sandbox_event_rx) =
            tokio::sync::broadcast::channel(SANDBOX_EVENT_CHANNEL_CAPACITY);
        Self {
            store,
            factory,
            sandboxes: RwLock::new(HashMap::new()),
            proxy_routes: RwLock::new(ProxyRouteTable::default()),
            next_proxy_route_version: AtomicU64::new(1),
            counters: Default::default(),
            sandbox_event_tx,
            default_sandbox_timeout,
            is_shutting_down: std::sync::atomic::AtomicBool::new(false),
            scheduling_disabled: std::sync::atomic::AtomicBool::new(false),
            scheduling_disabled_changed_at_ms: std::sync::atomic::AtomicI64::new(0),
            shutdown_tx: tokio::sync::watch::channel(false).0,
            shutdown_outcome: tokio::sync::OnceCell::new(),
            image_refs,
            access_tokens: SandboxAccessTokenGenerator::new(access_token_seed).unwrap(),
            pause_publisher: tokio::sync::OnceCell::new(),
            grants: tokio::sync::OnceCell::new(),
            runtime_routing: tokio::sync::OnceCell::new(),
            launch_claims: Arc::new(LaunchClaims::default()),
        }
    }

    /// Builds the orchestrator with explicit role-owned dependencies.
    pub async fn new(
        seed_policy: AccessTokenSeedPolicy,
        store: S,
        factory: F,
        image_refs: Arc<dyn RuntimeImageRefs>,
    ) -> Result<Arc<Self>> {
        let app_config = ConfigManager::global_config();
        let config = &app_config.orchestrator;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (sandbox_event_tx, _sandbox_event_rx) =
            broadcast::channel(SANDBOX_EVENT_CHANNEL_CAPACITY);

        // No record survives a restart, so no restored record can demand a
        // pre-existing seed.
        let managed_seed_must_exist = false;
        let access_tokens = tokio::task::spawn_blocking(move || {
            SandboxAccessTokenGenerator::load_or_create(
                app_config,
                seed_policy,
                managed_seed_must_exist,
            )
        })
        .await
        .context("join envd access-token seed loader")??;

        let orchestrator = Arc::new(Self {
            store,
            factory,
            sandboxes: RwLock::new(HashMap::new()),
            proxy_routes: RwLock::new(ProxyRouteTable::default()),
            next_proxy_route_version: AtomicU64::new(1),
            counters: OrchestratorCounters::default(),
            sandbox_event_tx,
            default_sandbox_timeout: Duration::from_secs(config.default_sandbox_timeout_secs),
            is_shutting_down: std::sync::atomic::AtomicBool::new(false),
            scheduling_disabled: std::sync::atomic::AtomicBool::new(false),
            scheduling_disabled_changed_at_ms: AtomicI64::new(0),
            shutdown_tx,
            shutdown_outcome: OnceCell::new(),
            image_refs,
            access_tokens,
            pause_publisher: OnceCell::new(),
            grants: OnceCell::new(),
            runtime_routing: OnceCell::new(),
            launch_claims: Arc::new(LaunchClaims::default()),
        });

        // Start the auto-evict task.
        let evict_interval = Duration::from_millis(config.auto_evict_interval_ms);
        Self::start_auto_evict_task(Arc::clone(&orchestrator), evict_interval, shutdown_rx);

        // Reconcile the layer cache's holds, then start maintenance (fail-closed).
        let gc = app_config.image.cache.gc_schedule();
        if gc.enabled {
            match orchestrator.image_refs.prepare_maintenance().await {
                Ok(()) => Self::start_local_image_maintenance_task(
                    Arc::clone(&orchestrator),
                    gc.interval,
                    orchestrator.shutdown_tx.subscribe(),
                ),
                Err(error) => warn!(
                    error = %format_args!("{error:#}"),
                    "local image cache reconcile failed at startup; not starting maintenance"
                ),
            }
        }

        Ok(orchestrator)
    }

    async fn run_cancellation_safe<T>(
        self: &Arc<Self>,
        operation: &'static str,
        sandbox_id: SandboxId,
        future: impl std::future::Future<Output = Result<T>> + Send + 'static,
    ) -> Result<T>
    where
        T: Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            let result = future.await;
            if tx.send(result).is_err() {
                debug!(
                    sandbox_id = %sandbox_id,
                    operation,
                    "operation completed after caller stopped waiting"
                );
            }
        });

        rx.await.map_err(|err| {
            OrchestratorError::InternalError(format!(
                "operation task ended before reporting result: {err}"
            ))
        })?
    }

    async fn protect_image_refs(
        &self,
        owner: RuntimeImageOwner,
        artifacts: RuntimeArtifactSet,
        context: &'static str,
    ) -> Result<()> {
        self.image_refs
            .pin(owner, artifacts)
            .await
            .map_err(|error| {
                OrchestratorError::InternalError(format!("pin {context} image refs: {error:#}"))
            })
    }

    async fn release_image_refs(&self, owner: RuntimeImageOwner) {
        self.image_refs.unpin_best_effort(owner).await;
    }

    /// Resolves a sandbox absent from the process-local handle table.
    async fn absent_handle(&self, sandbox_id: SandboxId) -> Result<AbsentHandle> {
        // Store errors propagate; they are not evidence of runtime absence.
        let Some(metadata) = self.store.get(&sandbox_id).await? else {
            return Ok(AbsentHandle::NoRecord);
        };

        let adopted = self
            .factory
            .adopt_running(sandbox_id, metadata.execution_id, metadata.resources)
            .map_err(|error| {
                OrchestratorError::InternalError(format!(
                    "could not tell where sandbox {sandbox_id} is running: {error:#}"
                ))
            })?;
        let Some(mut backend) = adopted else {
            return Ok(AbsentHandle::RuntimeGone);
        };

        if let Err(error) = backend.start().await {
            // Only a typed placement confirmation authorizes record cleanup.
            if error.downcast_ref::<RuntimeConfirmedGone>().is_some() {
                warn!(
                    %sandbox_id,
                    error = %error,
                    "the node holding this sandbox has left the cluster; treating its runtime as gone"
                );
                return Ok(AbsentHandle::RuntimeGone);
            }
            return Err(OrchestratorError::InternalError(format!(
                "sandbox {sandbox_id} is not running in this process and the machine running it \
                 could not be reached: {error:#}"
            )));
        }
        debug!(%sandbox_id, "adopted a sandbox this process did not start");
        Ok(AbsentHandle::Adopted(Arc::new(Mutex::new(backend))))
    }

    /// Snapshot the running set's local runtime artifacts for maintenance.
    pub async fn collect_running_artifacts(&self) -> Vec<(SandboxId, RuntimeArtifactSet)> {
        let handles = {
            self.sandboxes
                .read()
                .await
                .iter()
                .map(|(sandbox_id, handle)| (*sandbox_id, Arc::clone(handle)))
                .collect::<Vec<_>>()
        };
        let mut running = Vec::with_capacity(handles.len());
        for (sandbox_id, handle) in handles {
            let artifacts = {
                let sandbox = handle.lock().await;
                sandbox.runtime_info().runtime_artifacts
            };
            running.push((sandbox_id, artifacts));
        }
        running
    }

    /// Creates and starts a new sandbox from a resolved launch source.
    ///
    /// This call only returns after the sandbox is fully ready and persisted
    /// as `Running`, so callers can treat a successful return as immediately
    /// usable without additional polling.
    pub async fn create_sandbox(
        self: &Arc<Self>,
        request: CreateSandboxRequest,
    ) -> Result<SandboxMetadata> {
        let sandbox_id = SandboxId::new();
        let this = Arc::clone(self);
        self.run_cancellation_safe("create", sandbox_id, async move {
            this.create_sandbox_inner(sandbox_id, request).await
        })
        .await
    }

    /// Starts a sandbox under an id the caller already records: a resume of a
    /// paused sandbox from its snapshot row, or a node acting on the api half's
    /// create. Nothing here arbitrates; the caller's record is the licence.
    pub async fn restore_sandbox(
        self: &Arc<Self>,
        sandbox_id: SandboxId,
        request: CreateSandboxRequest,
    ) -> Result<SandboxMetadata> {
        let this = Arc::clone(self);
        self.run_cancellation_safe("restore", sandbox_id, async move {
            this.create_sandbox_inner(sandbox_id, request).await
        })
        .await
    }

    /// Starts a sandbox under an id the caller records, joining a launch of
    /// the same id already in flight rather than starting a second one.
    ///
    /// A launch this process holds is waited out in memory; one another replica
    /// holds is waited out on the shared record. Either way the answer is the
    /// sandbox that launch produced, or the error it failed with.
    pub async fn restore_or_join_launch(
        self: &Arc<Self>,
        sandbox_id: SandboxId,
        request: CreateSandboxRequest,
    ) -> Result<RestoredSandbox> {
        let joined = match self.restore_sandbox(sandbox_id, request).await {
            Err(OrchestratorError::LaunchInFlight { .. }) => self.join_launch(sandbox_id).await,
            Err(error) if LaunchHeldElsewhere::refused(&error) => {
                self.await_launch_elsewhere(sandbox_id, error).await
            }
            other => {
                return other.map(|metadata| RestoredSandbox {
                    metadata,
                    joined: false,
                })
            }
        };
        joined.map(|metadata| RestoredSandbox {
            metadata,
            joined: true,
        })
    }

    /// The incarnation of a launch this process is running under `sandbox_id`.
    ///
    /// A launch holds its id from before it allocates anything until its record
    /// is written, which is a window in which nothing else on this process
    /// names the sandbox.
    pub fn launch_in_flight(&self, sandbox_id: SandboxId) -> Option<ExecutionId> {
        self.launch_claims
            .in_flight(sandbox_id)
            .map(|held| held.execution_id())
    }

    /// Waits out the launch this process is running under `sandbox_id`.
    async fn join_launch(&self, sandbox_id: SandboxId) -> Result<SandboxMetadata> {
        // One deadline covers the whole join, including the wait on another
        // replica's record that a claim refused elsewhere ends in, so a joiner
        // never waits longer than the launch it joined.
        let deadline = tokio::time::Instant::now() + WAIT_TRANSITION_TIMEOUT;
        let Some(in_flight) = self.launch_claims.in_flight(sandbox_id) else {
            // The launch settled between the refusal and this lookup, so its
            // record is what it produced.
            return self.launched_record(sandbox_id).await;
        };
        info!(
            %sandbox_id,
            holder_execution_id = %in_flight.execution_id(),
            "joining a launch of this sandbox already in flight in this process"
        );
        let wait = deadline.saturating_duration_since(tokio::time::Instant::now());
        match in_flight.join(wait).await {
            Some(LaunchSettlement::Launched(metadata)) => Ok(*metadata),
            Some(LaunchSettlement::Failed(failure @ LaunchFailure::HeldElsewhere(_))) => {
                // The launch this caller joined stopped at another replica's
                // reservation, which decides the id instead of it. Following
                // the refusal here would answer a failure about a sandbox that
                // is starting, so this caller waits where the claimant waits.
                self.await_launch_elsewhere_until(
                    sandbox_id,
                    failure.into_error(sandbox_id),
                    deadline,
                )
                .await
            }
            Some(LaunchSettlement::Failed(failure)) => Err(failure.into_error(sandbox_id)),
            None => {
                warn!(%sandbox_id, "timed out joining a launch of this sandbox");
                Err(OrchestratorError::InvalidSandboxState {
                    sandbox_id,
                    state: SandboxState::Creating,
                })
            }
        }
    }

    /// Waits for the launch another replica holds to leave a running record.
    async fn await_launch_elsewhere(
        &self,
        sandbox_id: SandboxId,
        refusal: OrchestratorError,
    ) -> Result<SandboxMetadata> {
        let deadline = tokio::time::Instant::now() + WAIT_TRANSITION_TIMEOUT;
        self.await_launch_elsewhere_until(sandbox_id, refusal, deadline)
            .await
    }

    async fn await_launch_elsewhere_until(
        &self,
        sandbox_id: SandboxId,
        refusal: OrchestratorError,
        deadline: tokio::time::Instant,
    ) -> Result<SandboxMetadata> {
        info!(
            %sandbox_id,
            "another replica is launching this sandbox; waiting for the record it will write"
        );
        loop {
            match self.store.get(&sandbox_id).await? {
                Some(metadata) if metadata.state == SandboxState::Running => return Ok(metadata),
                Some(metadata) if metadata.state == SandboxState::Killing => {
                    return Err(OrchestratorError::SandboxNotFound(sandbox_id))
                }
                _ => {}
            }
            let now = tokio::time::Instant::now();
            if now >= deadline {
                warn!(%sandbox_id, "timed out waiting for another replica to finish this launch");
                // The refusal is what this caller actually met; a launch that
                // never produced a record has nothing better to report.
                return Err(refusal);
            }
            tokio::time::sleep(LAUNCH_ELSEWHERE_POLL.min(deadline - now)).await;
        }
    }

    /// The record a launch left behind, read as its outcome.
    async fn launched_record(&self, sandbox_id: SandboxId) -> Result<SandboxMetadata> {
        match self.store.get(&sandbox_id).await? {
            Some(metadata) if metadata.state == SandboxState::Running => Ok(metadata),
            Some(metadata) => Err(OrchestratorError::InvalidSandboxState {
                sandbox_id,
                state: metadata.state,
            }),
            None => Err(OrchestratorError::SandboxNotFound(sandbox_id)),
        }
    }

    /// Converts explicit expiry ownership into a record timeout.
    fn new_timeout_for(&self, expiry: SandboxExpiry) -> NewTimeout {
        match expiry {
            SandboxExpiry::After(timeout) => NewTimeout::Set(timeout),
            SandboxExpiry::AfterConfiguredDefault => NewTimeout::Set(self.default_sandbox_timeout),
            SandboxExpiry::NotKeptHere => NewTimeout::None,
        }
    }

    #[tracing::instrument(
        name = "create_sandbox",
        skip(self, request),
        fields(sandbox_id = %sandbox_id)
    )]
    async fn create_sandbox_inner(
        self: Arc<Self>,
        sandbox_id: SandboxId,
        request: CreateSandboxRequest,
    ) -> Result<SandboxMetadata> {
        if let Err(err) = self.ensure_accepting_new_work() {
            self.counters.record_create_fail(1);
            return Err(err);
        }

        let CreateSandboxRequest {
            source,
            expiry,
            timeout_action,
            user_metadata,
            env_vars,
            auto_resume,
            network_policy,
            custom_extension_params,
            secure,
            traffic_access_token,
            control_plane_config,
            execution_id,
            preferred_node_id,
        } = request;
        let envd_access_token = secure.then(|| self.access_tokens.generate(sandbox_id));
        // Log the three-state expiry decision without collapsing its meanings.
        info!(?expiry, "creating sandbox");
        let new_timeout = self.new_timeout_for(expiry);

        let result = match source {
            SandboxLaunchSource::Snapshot(snapshot) => {
                let SnapshotCreateParts {
                    launch_config,
                    transitional_metadata,
                } = match snapshot_create_parts(
                    snapshot.record(),
                    SnapshotCreateInputs {
                        sandbox_id,
                        envd_access_token,
                        env_vars,
                        user_metadata,
                        network_policy,
                        custom_extension_params,
                        timeout_action,
                        auto_resume,
                        secure,
                        traffic_access_token: traffic_access_token.clone(),
                        control_plane_config,
                        preferred_node_id: preferred_node_id.clone(),
                    },
                ) {
                    Ok(parts) => parts,
                    Err(err) => {
                        self.counters.record_create_fail(1);
                        return Err(err);
                    }
                };

                self.launch_sandbox(LaunchPlan::from_snapshot(
                    sandbox_id,
                    snapshot,
                    launch_config,
                    transitional_metadata,
                    new_timeout,
                    execution_id,
                ))
                .await
            }
            // The node resolves catalog snapshots into local bytes.
            SandboxLaunchSource::SnapshotRecord(record) => {
                let SnapshotCreateParts {
                    launch_config,
                    transitional_metadata,
                } = match snapshot_create_parts(
                    &record,
                    SnapshotCreateInputs {
                        sandbox_id,
                        envd_access_token,
                        env_vars,
                        user_metadata,
                        network_policy,
                        custom_extension_params,
                        timeout_action,
                        auto_resume,
                        secure,
                        traffic_access_token: traffic_access_token.clone(),
                        control_plane_config,
                        preferred_node_id: preferred_node_id.clone(),
                    },
                ) {
                    Ok(parts) => parts,
                    Err(err) => {
                        self.counters.record_create_fail(1);
                        return Err(err);
                    }
                };

                self.launch_sandbox(LaunchPlan::from_snapshot_record(
                    sandbox_id,
                    record,
                    launch_config,
                    transitional_metadata,
                    new_timeout,
                    execution_id,
                ))
                .await
            }
            SandboxLaunchSource::Image {
                image_ref,
                overlaybd_config_path,
                context,
                resources,
                extra_drives,
                extra_boot_args,
                image_configs,
            } => {
                let context = *context;
                let resources = resources.unwrap_or_else(default_fresh_sandbox_resources);
                let launch_image_configs = *image_configs;
                let mut extra_mmds = serde_json::Map::new();
                if !launch_image_configs.is_empty() {
                    extra_mmds.insert("imageConfigs".to_string(), launch_image_configs.to_value());
                };
                let launch_config = SandboxLaunchConfig {
                    sandbox_id,
                    snapshot_id: image_ref.clone(),
                    env_vars,
                    network: network_policy.runtime_policy(),
                    extra_mmds,
                    custom_extension_params: custom_extension_params.clone(),
                    envd_access_token,
                    traffic_access_token: traffic_access_token.clone(),
                    control_plane_config: None,
                    preferred_node_id: preferred_node_id.clone(),
                };
                let build_spec = FreshSandboxBuildSpec {
                    image_config_path: overlaybd_config_path,
                    context: context.clone(),
                    resources,
                    extra_drives,
                    extra_boot_args,
                };

                let transitional_metadata = SandboxMetadata {
                    id: sandbox_id,
                    snapshot_id: image_ref,
                    snapshot_alias: None,
                    virtualization_mode: ConfigManager::global_config().virtualization_mode,
                    runtime_versions: configured_runtime_versions(),
                    resources,
                    context,
                    image_configs: launch_image_configs,
                    timeout_action,
                    auto_resume,
                    user_metadata,
                    network_policy,
                    custom_extension_params,
                    secure,
                    control_plane_config,
                    max_lifetime: configured_max_sandbox_lifetime(),
                    ..Default::default()
                };

                self.launch_sandbox(LaunchPlan::fresh(
                    sandbox_id,
                    build_spec,
                    launch_config,
                    transitional_metadata,
                    new_timeout,
                    execution_id,
                ))
                .await
            }
            SandboxLaunchSource::UnresolvedImage {
                image_ref,
                resources,
                attached_drives,
                extra_boot_args,
            } => {
                let launch_config = SandboxLaunchConfig {
                    sandbox_id,
                    snapshot_id: image_ref.clone(),
                    env_vars,
                    network: network_policy.runtime_policy(),
                    // Remote factories build the node's launch config after resolution.
                    extra_mmds: serde_json::Map::new(),
                    custom_extension_params: custom_extension_params.clone(),
                    envd_access_token,
                    traffic_access_token: traffic_access_token.clone(),
                    control_plane_config: None,
                    preferred_node_id: preferred_node_id.clone(),
                };
                let build_spec = UnresolvedImageBuildSpec {
                    image_ref: image_ref.clone(),
                    resources,
                    attached_drives,
                    extra_boot_args,
                };

                let transitional_metadata = SandboxMetadata {
                    id: sandbox_id,
                    snapshot_id: image_ref,
                    snapshot_alias: None,
                    virtualization_mode: ConfigManager::global_config().virtualization_mode,
                    runtime_versions: configured_runtime_versions(),
                    resources,
                    // Resolution facts are filled from the node's start response.
                    timeout_action,
                    auto_resume,
                    user_metadata,
                    network_policy,
                    custom_extension_params,
                    secure,
                    control_plane_config,
                    max_lifetime: configured_max_sandbox_lifetime(),
                    ..Default::default()
                };

                self.launch_sandbox(LaunchPlan::from_unresolved_image(
                    sandbox_id,
                    build_spec,
                    launch_config,
                    transitional_metadata,
                    new_timeout,
                    execution_id,
                ))
                .await
            }
        };

        match result {
            Ok(metadata) => {
                self.counters.record_create_success(1);
                self.publish_sandbox_event(
                    SandboxLifecycleEventType::Create,
                    metadata.id,
                    metadata.execution_id,
                    metadata.resources,
                );
                Ok(metadata)
            }
            Err(err) => {
                self.counters.record_create_fail(1);
                Err(err)
            }
        }
    }

    /// Forks a running sandbox into multiple new sandboxes on the same node.
    pub async fn fork_sandbox(
        self: &Arc<Self>,
        source_sandbox_id: SandboxId,
        children: ForkChildren,
        new_timeout: NewTimeout,
    ) -> Result<Vec<SandboxForkOutcome>> {
        let this = Arc::clone(self);
        self.run_cancellation_safe("fork", source_sandbox_id, async move {
            this.fork_sandbox_inner(source_sandbox_id, children, new_timeout)
                .await
        })
        .await
    }

    #[tracing::instrument(
        name = "fork_sandbox",
        skip(self),
        fields(source_sandbox_id = %source_sandbox_id, count)
    )]
    async fn fork_sandbox_inner(
        self: Arc<Self>,
        source_sandbox_id: SandboxId,
        children: ForkChildren,
        new_timeout: NewTimeout,
    ) -> Result<Vec<SandboxForkOutcome>> {
        self.ensure_accepting_new_work()?;

        let count = children.count();

        info!("forking sandboxes");

        // Hold `Forking` before resolving the authoritative source execution.
        let source_metadata = self
            .store
            .update_if_state(&source_sandbox_id, &[SandboxState::Running], |metadata| {
                metadata.state = SandboxState::Forking
            })
            .await
            .map_err(|err| match err {
                StoreError::StateConflict { actual_state, .. } => match actual_state {
                    SandboxState::Killing => OrchestratorError::SandboxNotFound(source_sandbox_id),
                    _ => OrchestratorError::InvalidSandboxState {
                        sandbox_id: source_sandbox_id,
                        state: actual_state,
                    },
                },
                err => OrchestratorError::from(err),
            })?
            .previous;

        // Rebuild a missing or stale cached handle from the authoritative record.
        let source_handle = self
            .cached_handle_for_execution(source_sandbox_id, source_metadata.execution_id)
            .await;
        let source_handle = match source_handle {
            Some(handle) => handle,
            None => match self.absent_handle(source_sandbox_id).await {
                Ok(AbsentHandle::Adopted(handle)) => handle,
                Ok(AbsentHandle::RuntimeGone) | Ok(AbsentHandle::NoRecord) => {
                    let _ = self
                        .store
                        .update_state_if_state(
                            &source_sandbox_id,
                            SandboxState::Running,
                            &[SandboxState::Forking],
                        )
                        .await;
                    return Err(OrchestratorError::SandboxNotFound(source_sandbox_id));
                }
                Err(error) => {
                    warn!(error = %error, "could not reach the sandbox while forking; leaving its record alone");
                    let _ = self
                        .store
                        .update_state_if_state(
                            &source_sandbox_id,
                            SandboxState::Running,
                            &[SandboxState::Forking],
                        )
                        .await;
                    return Err(error);
                }
            },
        };

        // Caller-assigned child identity and ownership travel together.
        let children: Vec<ForkChildAssignment> = match children {
            ForkChildren::Fresh(count) => (0..count)
                .map(|_| ForkChildAssignment {
                    sandbox_id: SandboxId::new(),
                    execution_id: None,
                    control_plane_config: None,
                })
                .collect(),
            ForkChildren::Assigned(children) => children,
        };
        let children_spec = children
            .iter()
            .map(|child| SandboxForkSpec {
                sandbox_id: child.sandbox_id,
                // Fork children receive a fresh or caller-assigned incarnation.
                execution_id: child.execution_id.unwrap_or_else(ExecutionId::new),
                envd_access_token: source_metadata
                    .secure
                    .then(|| self.access_tokens.generate(child.sandbox_id)),
            })
            .collect::<Vec<_>>();

        // Children inherit the parent's policy, so each child's incarnation
        // needs its own grant before it can open a brokered connection.
        for spec in &children_spec {
            if let Err(source) = self
                .grant_secrets(
                    spec.sandbox_id,
                    spec.execution_id,
                    &source_metadata.network_policy,
                )
                .await
            {
                for granted in &children_spec {
                    self.revoke_secrets(granted.sandbox_id, granted.execution_id)
                        .await;
                }
                self.counters.record_create_fail(u64::from(count));
                let _ = self
                    .store
                    .update_state_if_state(
                        &source_sandbox_id,
                        SandboxState::Running,
                        &[SandboxState::Forking],
                    )
                    .await;
                return Err(OrchestratorError::SandboxOperationFailed {
                    sandbox_id: source_sandbox_id,
                    operation: SandboxOperation::Fork,
                    source,
                });
            }
        }

        // Start to fork the sandbox.
        // This is a single operation that will return a list of results for each child sandbox.
        let fork_result = {
            let mut sandbox = source_handle.lock().await;
            sandbox.fork(&children_spec).await
        };
        let forked_backends = match fork_result {
            Ok(forked_backends) => forked_backends,
            Err(err) => {
                warn!(error = ?err, "failed to fork sandbox");
                self.counters.record_create_fail(u64::from(count));
                // No child started, so every child grant issued above is dead.
                for granted in &children_spec {
                    self.revoke_secrets(granted.sandbox_id, granted.execution_id)
                        .await;
                }
                if err.is_terminal() {
                    self.detach_sandbox_handle_and_route(&source_sandbox_id)
                        .await;
                    let _ = {
                        let mut sandbox = source_handle.lock().await;
                        sandbox.stop().await
                    };
                    self.forget_sandbox(source_sandbox_id, source_metadata.execution_id)
                        .await?;
                } else {
                    let _ = self
                        .store
                        .update_state_if_state(
                            &source_sandbox_id,
                            SandboxState::Running,
                            &[SandboxState::Forking],
                        )
                        .await;
                }
                return Err(OrchestratorError::SandboxOperationFailed {
                    sandbox_id: source_sandbox_id,
                    operation: SandboxOperation::Fork,
                    source: err.into(),
                });
            }
        };

        // Restore the source sandbox's state to Running.
        if let Err(err) = self
            .store
            .update_state_if_state(
                &source_sandbox_id,
                SandboxState::Running,
                &[SandboxState::Forking],
            )
            .await
        {
            warn!(error = ?err, "failed to restore source sandbox metadata after fork");
        }

        // Register each forked sandbox in the store and runtime, and publish events.
        let mut outcomes = Vec::with_capacity(children_spec.len());
        let mut successes = 0u64;
        let now = SystemTime::now();
        for ((child, spec), backend) in children.into_iter().zip(children_spec).zip(forked_backends)
        {
            let sandbox_id = child.sandbox_id;
            let backend = match backend {
                Ok(backend) => backend,
                Err(err) => {
                    warn!(%sandbox_id, error = ?err, "failed to start forked sandbox");
                    self.revoke_secrets(sandbox_id, spec.execution_id).await;
                    outcomes.push(Err(Self::fork_child_error(sandbox_id, err)));
                    continue;
                }
            };

            let mut metadata = source_metadata.clone();
            metadata.id = sandbox_id;
            // Replace the cloned parent's incarnation.
            metadata.execution_id = spec.execution_id;
            // Replace the cloned parent's ownership marker.
            metadata.control_plane_config = child.control_plane_config;
            metadata.state = SandboxState::Running;
            metadata.created_at = now;
            // Fork children start a fresh lifetime budget.
            metadata.restart_lifetime_clock(now);
            metadata.update_timeout(new_timeout);

            let proxy_target =
                match Self::proxy_target_from_sandbox(backend.as_ref()).map(|target| {
                    target.with_traffic_access_token(metadata.traffic_access_token.clone())
                }) {
                    Ok(proxy_target) => proxy_target,
                    Err(err) => {
                        Self::stop_failed_fork(backend, sandbox_id).await;
                        self.revoke_secrets(sandbox_id, spec.execution_id).await;
                        outcomes.push(Err(Self::fork_child_error(
                            sandbox_id,
                            anyhow::Error::new(err),
                        )));
                        continue;
                    }
                };
            if let Err(err) = self.store.add(metadata.clone()).await {
                warn!(%sandbox_id, error = ?err, "failed to register forked sandbox");
                Self::stop_failed_fork(backend, sandbox_id).await;
                self.revoke_secrets(sandbox_id, spec.execution_id).await;
                outcomes.push(Err(Self::fork_child_error(
                    sandbox_id,
                    anyhow::Error::new(err),
                )));
                continue;
            }
            self.sandboxes
                .write()
                .await
                .insert(metadata.id, Arc::new(Mutex::new(backend)));
            self.upsert_proxy_route(metadata.id, proxy_target, metadata.execution_id)
                .await;
            self.publish_sandbox_event(
                SandboxLifecycleEventType::Fork,
                metadata.id,
                metadata.execution_id,
                metadata.resources,
            );
            successes += 1;
            outcomes.push(Ok(metadata));
        }

        self.counters.record_create_success(successes);
        self.counters
            .record_create_fail(u64::from(count) - successes);
        Ok(outcomes)
    }

    fn fork_child_error(sandbox_id: SandboxId, source: anyhow::Error) -> OrchestratorError {
        OrchestratorError::SandboxOperationFailed {
            sandbox_id,
            operation: SandboxOperation::Fork,
            source,
        }
    }

    async fn stop_failed_fork(mut backend: Box<dyn SandboxBackend>, sandbox_id: SandboxId) {
        if let Err(err) = backend.stop().await {
            warn!(%sandbox_id, error = ?err, "failed to stop unsuccessful fork");
        }
    }

    /// Retrieves the metadata for a sandbox by its ID.
    #[tracing::instrument(skip(self), fields(sandbox_id = %sandbox_id))]
    pub async fn get_sandbox(&self, sandbox_id: &SandboxId) -> Result<Option<SandboxMetadata>> {
        Ok(self.store.get(sandbox_id).await?)
    }

    /// Lists all sandboxes with their metadata.
    #[tracing::instrument(skip(self))]
    pub async fn list_sandboxes(&self) -> Result<Vec<SandboxMetadata>> {
        Ok(self.store.list().await?)
    }

    /// Lists all sandbox IDs currently tracked by the store.
    pub async fn list_sandbox_ids(&self) -> Result<Vec<SandboxId>> {
        Ok(self.store.list_ids().await?)
    }

    /// Lists the same sandbox set as ids, adding execution and projection TTL.
    pub async fn list_sandbox_roster(&self) -> Result<Vec<SandboxRosterEntry>> {
        let now = SystemTime::now();

        Ok(self
            .store
            .list()
            .await?
            .into_iter()
            .map(|metadata| SandboxRosterEntry {
                sandbox_id: metadata.id,
                execution_id: metadata.execution_id,
                // Repair writes use the sandbox's remaining projection budget.
                projection_ttl_secs: metadata.projection_ttl_secs(now),
            })
            .collect())
    }

    /// Lists sandboxes that match the provided filter criteria:
    /// - If `states` is provided, only sandboxes in those states will be included.
    /// - If `user_metadata` is provided, only sandboxes whose user metadata contains
    ///   all the specified key-value pairs will be included.
    #[tracing::instrument(skip(self, filter))]
    pub async fn list_sandboxes_filtered(
        &self,
        filter: SandboxListFilter,
    ) -> Result<Vec<SandboxMetadata>> {
        Ok(self.store.list_filtered(filter).await?)
    }

    /// Lists live handles, using records only for attributes.
    /// Busy handles remain listed with `facts_from_handle = false`.
    pub async fn list_live_sandboxes(&self) -> Result<Vec<LiveSandbox>> {
        // Release the handle-table lock before store I/O.
        let handles: Vec<(SandboxId, SandboxHandle)> = {
            let sandboxes = self.sandboxes.read().await;
            sandboxes
                .iter()
                .map(|(id, handle)| (*id, Arc::clone(handle)))
                .collect()
        };
        if handles.is_empty() {
            return Ok(Vec::new());
        }

        let ids: Vec<SandboxId> = handles.iter().map(|(id, _)| *id).collect();
        // Store failure is not an empty attribute set.
        let records = self.store.get_many(&ids).await?;
        // Partial reads cannot authorize absence.
        if !records.covers(&ids) {
            return Err(OrchestratorError::StoreOperationFailed(
                StoreError::Backend {
                    source: anyhow::anyhow!(
                        "read the records for {} live sandboxes and got {}",
                        ids.len(),
                        records.covered.len()
                    ),
                },
            ));
        }

        let mut live = Vec::with_capacity(handles.len());
        for (sandbox_id, handle) in handles {
            let record = records.entries.get(&sandbox_id);
            let mut entry = LiveSandbox {
                sandbox_id,
                execution_id: record.map(|record| record.execution_id),
                facts_from_handle: false,
                host_interaction_ip: None,
                rootfs_virtual_size: None,
                created_at: record.map(|record| record.created_at),
                expires_at: record.and_then(|record| record.expires_at),
                resources: record.map(|record| record.resources),
                control_plane_config: record.and_then(|record| record.control_plane_config.clone()),
            };

            if let Ok(sandbox) = handle.try_lock() {
                entry.facts_from_handle = true;
                // The live handle's incarnation is authoritative.
                entry.execution_id = Some(sandbox.execution_id());
                entry.host_interaction_ip = sandbox.host_interaction_ip();
                entry.rootfs_virtual_size = sandbox.runtime_info().rootfs_virtual_size;
            }

            live.push(entry);
        }

        Ok(live)
    }

    /// What this process holds under `sandbox_id`: the live handle if there is
    /// one, otherwise the record its store keeps.
    ///
    /// This is the one answer to "is this id taken here". A create refuses on
    /// it and a describe reports it, so the two can never disagree about a
    /// sandbox this process holds.
    pub async fn held_sandbox(&self, sandbox_id: SandboxId) -> Result<Option<LiveSandbox>> {
        let handle = self.sandboxes.read().await.get(&sandbox_id).cloned();
        let record = self.store.get(&sandbox_id).await?;
        if handle.is_none() && record.is_none() {
            return Ok(None);
        }

        let record = record.as_ref();
        let mut entry = LiveSandbox {
            sandbox_id,
            execution_id: record.map(|record| record.execution_id),
            facts_from_handle: false,
            host_interaction_ip: None,
            rootfs_virtual_size: None,
            created_at: record.map(|record| record.created_at),
            expires_at: record.and_then(|record| record.expires_at),
            resources: record.map(|record| record.resources),
            control_plane_config: record.and_then(|record| record.control_plane_config.clone()),
        };
        if let Some(handle) = handle.as_ref() {
            if let Ok(sandbox) = handle.try_lock() {
                entry.facts_from_handle = true;
                // The live handle's incarnation is authoritative.
                entry.execution_id = Some(sandbox.execution_id());
                entry.host_interaction_ip = sandbox.host_interaction_ip();
                entry.rootfs_virtual_size = sandbox.runtime_info().rootfs_virtual_size;
            }
        }
        Ok(Some(entry))
    }

    pub fn get_envd_access_token(&self, metadata: &SandboxMetadata) -> Option<EnvdAccessToken> {
        metadata
            .secure
            .then(|| self.access_tokens.generate(metadata.id))
    }

    pub fn validate_envd_access_token(&self, sandbox_id: SandboxId, candidate: &str) -> bool {
        self.access_tokens.matches(sandbox_id, candidate)
    }

    /// Returns the execution currently serving traffic on this node.
    pub async fn live_execution_id(&self, sandbox_id: &SandboxId) -> Option<ExecutionId> {
        self.proxy_routes
            .read()
            .await
            .route(sandbox_id)
            .map(|route| route.execution_id())
    }

    /// Resolves the current proxyability of a sandbox without touching the sandbox mutex.
    #[tracing::instrument(skip(self), fields(sandbox_id = %sandbox_id))]
    pub async fn proxy_lookup_for(&self, sandbox_id: &SandboxId) -> Result<ProxyLookupResult> {
        if let Some(route) = self.proxy_routes.read().await.route(sandbox_id).cloned() {
            trace!(
                version = route.version(),
                "resolved running proxy target from runtime table"
            );
            return Ok(ProxyLookupResult::Ready(route.target().clone()));
        }

        let metadata = self.store.get(sandbox_id).await?;
        Ok(match metadata {
            None => {
                debug!("sandbox has no runtime route or persisted metadata");
                ProxyLookupResult::NotFound
            }
            Some(metadata) if metadata.state == SandboxState::Running => {
                warn!("running sandbox is missing a runtime proxy route");
                ProxyLookupResult::RouteMissing
            }
            Some(metadata) => {
                debug!(state = ?metadata.state, "sandbox exists but is not proxyable");
                ProxyLookupResult::Unavailable(metadata.state)
            }
        })
    }

    /// Updates the keep-alive timeout for a RUNNING sandbox.
    /// If `timeout` is `None`, default timeout will be applied.
    /// If `allow_shorter` is `false`, the update will be skipped if the new TTL is not longer than the existing TTL.
    ///
    /// When the sandbox is in a transitional state that may resolve to `Running`,
    /// this method waits for the transition to complete before re-evaluating the state.
    #[tracing::instrument(skip(self), fields(sandbox_id = %sandbox_id, allow_shorter = allow_shorter))]
    pub async fn keep_alive_for(
        &self,
        sandbox_id: SandboxId,
        timeout: Option<Duration>,
        allow_shorter: bool,
    ) -> Result<Option<SandboxMetadata>> {
        self.ensure_accepting_lifecycle_operations()?;

        if timeout.is_none() {
            debug!("applying default timeout for keep-alive");
        } else {
            debug!(?timeout, "updating keep-alive timeout");
        }
        let valid_timeout = timeout.unwrap_or(self.default_sandbox_timeout);

        let mut metadata = match self.store.get(&sandbox_id).await? {
            Some(metadata) => metadata,
            None => return Err(OrchestratorError::SandboxNotFound(sandbox_id)),
        };

        // If the sandbox is in a transitional state that may lead to Running,
        // wait for the transition to complete before checking whether the
        // keep-alive is applicable.
        if matches!(
            metadata.state,
            SandboxState::Creating | SandboxState::Snapshotting | SandboxState::Forking
        ) {
            debug!(state = ?metadata.state, "sandbox in transitional state, waiting before applying keep-alive");
            metadata = self.wait_for_transition(sandbox_id, metadata.state).await?;
        }

        if metadata.state != SandboxState::Running {
            info!(state = ?metadata.state, "cannot update keep-alive timeout in non-running state");
            return Err(OrchestratorError::InvalidSandboxState {
                sandbox_id,
                state: metadata.state,
            });
        }

        // A record that says running is this replica's memory; the binding is
        // the cluster's. Extending the life of a runtime nobody can reach
        // would keep the record out of the evictor's way forever.
        if self.runtime_confirmed_gone(sandbox_id).await {
            self.forget_unrouted_runtime(sandbox_id, metadata.execution_id)
                .await;
            return Err(OrchestratorError::SandboxNotFound(sandbox_id));
        }

        // Only an already-exhausted lifetime ceiling refuses renewal.
        let now = SystemTime::now();
        if let Some(deadline) = metadata.lifetime_deadline(now) {
            if now >= deadline {
                info!(?deadline, "sandbox has exceeded its maximum lifetime");
                return Err(OrchestratorError::SandboxLifetimeExceeded {
                    sandbox_id,
                    deadline,
                });
            }
        }

        let mut timeout_updated = false;
        let update_result = self
            .store
            .update_if_state(&sandbox_id, &[SandboxState::Running], |metadata| {
                let new_expire_time = SystemTime::now().checked_add(valid_timeout);
                if !allow_shorter {
                    if let Some(current_expire) = metadata.expires_at {
                        if let Some(new_expire) = new_expire_time {
                            if new_expire <= current_expire {
                                info!(
                                    current_expire = ?current_expire,
                                    new_expire = ?new_expire,
                                    "new timeout is not longer than current timeout, skipping update",
                                );
                                return;
                            }
                        }
                    }
                }

                metadata.set_timeout(Some(valid_timeout));
                timeout_updated = true;
            })
            .await
            .map_err(|err| match err {
                StoreError::StateConflict { actual_state, .. } => {
                    info!(state = ?actual_state, "keep-alive update failed due to state conflict");
                    OrchestratorError::InvalidSandboxState {
                        sandbox_id,
                        state: actual_state,
                    }
                }
                other => OrchestratorError::from(other),
            })?;
        if timeout_updated {
            info!(?valid_timeout, "sandbox keep-alive timeout updated");
        }

        Ok(Some(update_result.current))
    }

    /// Stops and deletes the sandbox with the given ID.
    ///
    /// If the sandbox is currently in a transitional state, this method waits for
    /// the in-progress operation to finish before proceeding with deletion, preventing
    /// races where an ongoing operation might overwrite the `Killing` state.
    pub async fn delete_sandbox(self: &Arc<Self>, sandbox_id: SandboxId) -> Result<()> {
        let this = Arc::clone(self);
        self.run_cancellation_safe("delete", sandbox_id, async move {
            this.delete_sandbox_inner(sandbox_id).await
        })
        .await
    }

    #[tracing::instrument(
        name = "delete_sandbox",
        skip(self),
        fields(sandbox_id = %sandbox_id)
    )]
    async fn delete_sandbox_inner(self: &Arc<Self>, sandbox_id: SandboxId) -> Result<()> {
        info!("deleting sandbox");

        // Attempt to transition to Killing, retrying after waiting whenever we
        // find the sandbox in a transitional state.
        let previous_state = loop {
            match self
                .store
                .update_state_if_state(&sandbox_id, SandboxState::Killing, &[SandboxState::Running])
                .await
            {
                Ok(previous_state) => break previous_state,
                Err(StoreError::StateConflict { actual_state, .. }) => match actual_state {
                    SandboxState::Killing => {
                        debug!("sandbox already in killing state, waiting for delete to finish");
                        match self
                            .wait_for_transition(sandbox_id, SandboxState::Killing)
                            .await
                        {
                            Ok(_) => {
                                // The in-flight delete rolled back to a stable state.
                                // Retry the Killing CAS rather than letting multiple
                                // deleters run concurrently.
                                continue;
                            }
                            Err(OrchestratorError::SandboxNotFound(_)) => {
                                info!("sandbox was deleted by a concurrent delete");
                                return Ok(());
                            }
                            Err(e) => return Err(e),
                        }
                    }
                    SandboxState::Creating
                    | SandboxState::Snapshotting
                    | SandboxState::Forking
                    | SandboxState::Pausing => {
                        // An in-progress operation is currently holding the sandbox in this
                        // transitional state.  Wait for it to finish so our Killing transition
                        // doesn't race with the final state write from that operation.
                        debug!(
                            state = ?actual_state,
                            "sandbox in transitional state, waiting before deletion"
                        );
                        match self.wait_for_transition(sandbox_id, actual_state).await {
                            Ok(_) => {
                                // Transition finished; retry the Killing CAS.
                                continue;
                            }
                            Err(OrchestratorError::SandboxNotFound(_)) => {
                                // Sandbox was removed while we waited (e.g. by
                                // another concurrent delete).
                                info!("sandbox was deleted while waiting for transitional state");
                                return Ok(());
                            }
                            Err(e) => return Err(e),
                        }
                    }
                    // A rollback put the sandbox back to running between the
                    // read and this CAS; take it from the top.
                    SandboxState::Running => continue,
                },
                Err(err) => return Err(OrchestratorError::from(err)),
            }
        };

        self.delete_entered_sandbox(sandbox_id, previous_state)
            .await
    }

    /// Deletes a sandbox whose record this caller already moved into `Killing`.
    ///
    /// `previous_state` is where a rollback returns the record.
    #[tracing::instrument(
        name = "delete_entered_sandbox",
        skip(self),
        fields(sandbox_id = %sandbox_id)
    )]
    async fn delete_entered_sandbox(
        self: &Arc<Self>,
        sandbox_id: SandboxId,
        previous_state: SandboxState,
    ) -> Result<()> {
        // Read the authoritative execution after exclusively entering `Killing`.
        let expected_execution_id = match self.store.get(&sandbox_id).await {
            Ok(Some(metadata)) => metadata.execution_id,
            Ok(None) => {
                // We just won the Killing transition; the record vanishing
                // immediately after reads the same as "already deleted".
                warn!("sandbox record disappeared while deleting");
                return Ok(());
            }
            Err(err) => {
                warn!(error = ?err, "could not read sandbox record while deleting; leaving its record alone");
                self.store
                    .update_state_if_state(&sandbox_id, previous_state, &[SandboxState::Killing])
                    .await?;
                return Err(OrchestratorError::from(err));
            }
        };

        if self.runtime_confirmed_gone(sandbox_id).await {
            self.forget_unrouted_runtime(sandbox_id, expected_execution_id)
                .await;
            return Ok(());
        }

        let (handle, removed_route) = self
            .detach_sandbox_handle_and_route_checked(&sandbox_id, expected_execution_id)
            .await;
        let handle_was_held_here = handle.is_some();
        // Adopted backends are operation-scoped and never cached as locally running.
        let handle = match handle {
            Some(handle) => Some(handle),
            None => match self.absent_handle(sandbox_id).await {
                Ok(AbsentHandle::Adopted(handle)) => Some(handle),
                Ok(AbsentHandle::RuntimeGone) | Ok(AbsentHandle::NoRecord) => None,
                // Store or placement uncertainty must not authorize deleting the record.
                Err(error) => {
                    warn!(error = %error, "could not reach the sandbox while deleting; leaving its record alone");
                    self.restore_proxy_route(sandbox_id, removed_route).await;
                    self.store
                        .update_state_if_state(
                            &sandbox_id,
                            previous_state,
                            &[SandboxState::Killing],
                        )
                        .await?;
                    return Err(error);
                }
            },
        };

        if let Some(handle) = handle {
            let stop_result = {
                let mut sandbox = handle.lock().await;
                sandbox.stop().await
            };

            if let Err(err) = stop_result {
                warn!(error = ?err, "failed to stop sandbox during delete");
                if handle_was_held_here {
                    self.sandboxes.write().await.insert(sandbox_id, handle);
                }
                self.restore_proxy_route(sandbox_id, removed_route).await;
                self.store
                    .update_state_if_state(&sandbox_id, previous_state, &[SandboxState::Killing])
                    .await?;

                return Err(OrchestratorError::SandboxOperationFailed {
                    sandbox_id,
                    operation: SandboxOperation::Stop,
                    source: err,
                });
            }
        }

        // Now the sandbox is successfully stopped, remove its metadata.
        let metadata = self
            .forget_sandbox(sandbox_id, expected_execution_id)
            .await?;
        if let Some(metadata) = metadata {
            self.publish_sandbox_event(
                SandboxLifecycleEventType::Delete,
                metadata.id,
                metadata.execution_id,
                metadata.resources,
            );
        }
        info!("sandbox deleted");

        Ok(())
    }

    /// Wires where pause captures become durable; the first publisher wins.
    pub fn set_pause_publisher(&self, publisher: Arc<dyn PausePublisher>) {
        if self.pause_publisher.set(publisher).is_err() {
            warn!("pause publisher was already wired; ignoring");
        }
    }

    /// Installs the grant issuer once; later calls are ignored.
    pub fn set_grant_issuer(&self, issuer: Arc<dyn GrantIssuer>) {
        if self.grants.set(issuer).is_err() {
            warn!("grant issuer was already wired; ignoring");
        }
    }

    /// Installs who answers whether a sandbox's runtime is still routed to.
    pub fn set_runtime_routing(&self, routing: Arc<dyn RuntimeRouting>) {
        if self.runtime_routing.set(routing).is_err() {
            warn!("runtime routing was already wired; ignoring");
        }
    }

    /// Whether the cluster has stopped routing to this sandbox's runtime.
    ///
    /// Only a complete answer counts: a lookup that fails, and a process that
    /// installed no routing source, both leave the record alone.
    async fn runtime_confirmed_gone(&self, sandbox_id: SandboxId) -> bool {
        let Some(routing) = self.runtime_routing.get() else {
            return false;
        };
        match routing.is_routed(sandbox_id).await {
            Ok(routed) => !routed,
            Err(error) => {
                warn!(
                    %sandbox_id,
                    error = %format_args!("{error:#}"),
                    "could not tell whether anything still routes to this sandbox"
                );
                false
            }
        }
    }

    /// Drops the record and handle of a runtime nothing routes to any more.
    async fn forget_unrouted_runtime(&self, sandbox_id: SandboxId, execution_id: ExecutionId) {
        warn!(
            %sandbox_id,
            %execution_id,
            "nothing routes to this sandbox's runtime any more; dropping its record"
        );
        self.detach_sandbox_handle_and_route(&sandbox_id).await;
        if let Err(error) = self.forget_sandbox(sandbox_id, execution_id).await {
            warn!(
                %sandbox_id,
                error = ?error,
                "failed to remove the record of a sandbox nothing routes to"
            );
        }
    }

    fn grants(&self) -> Arc<dyn GrantIssuer> {
        self.grants
            .get()
            .cloned()
            .unwrap_or_else(|| NoGrants::shared())
    }

    /// Grants the names the policy references before the sandbox can open a
    /// brokered connection. An empty set is a no-op for every issuer.
    ///
    /// Nothing here asks whether this sandbox may use these names: the
    /// isolation axis is the control-plane credential, and a grant bounds
    /// what one incarnation may read, not who owns a secret. An ownership
    /// check belongs in this function once `Claims` carries an identity to
    /// check against — the broker, the store and the public surface all read
    /// the grant as it is and need no change for it.
    async fn grant_secrets(
        &self,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
        policy: &SandboxNetworkPolicy,
    ) -> anyhow::Result<()> {
        let wanted = policy.egress.referenced_secrets();
        if wanted.is_empty() {
            return Ok(());
        }
        // Only the create and network-update paths refuse an unusable name;
        // a wake carries a policy that was accepted long ago and must not be
        // refused now, so a name deleted or reshaped meanwhile is reported
        // here and the sandbox still starts. Without this the guest gets a
        // synthetic 403 and the operator gets nothing.
        if let Some(unusable) = self.grants().unusable_names(&wanted).await {
            if !unusable.is_empty() {
                warn!(
                    %sandbox_id,
                    %execution_id,
                    unusable = %unusable.join(", "),
                    "starting a sandbox whose policy names secrets the store cannot serve as used; \
                     brokered connections using them will be refused"
                );
            }
        }
        let names: BTreeSet<String> = wanted.into_keys().collect();
        self.grants()
            .grant(sandbox_id, execution_id, &names)
            .await
            .with_context(|| {
                format!(
                    "grant {} secret name(s) to sandbox {sandbox_id}",
                    names.len()
                )
            })
    }

    /// Best effort: a grant that outlives its incarnation is unusable because
    /// the execution id it names is gone; the warning is for the operator,
    /// and the reaper picks the row up on a later round.
    async fn revoke_secrets(&self, sandbox_id: SandboxId, execution_id: ExecutionId) {
        if let Err(err) = self.grants().revoke(sandbox_id, execution_id).await {
            warn!(%sandbox_id, %execution_id, error = %format_args!("{err:#}"), "failed to revoke secret grants");
        }
    }

    /// Revokes aged grants no record backs: a teardown whose revoke failed,
    /// or a record that expired out of the store while its node was
    /// unreachable, would otherwise leave the incarnation resolvable for
    /// ever. A grant whose sandbox record names the same incarnation is
    /// live and kept; a store that cannot answer ends the round, since
    /// nothing can then be compared against.
    pub async fn reap_orphaned_grants(&self) -> Result<Vec<(SandboxId, ExecutionId)>> {
        let candidates = self
            .grants()
            .stale_grant_candidates(GRANT_REAP_MIN_AGE, GRANT_REAP_BATCH_LIMIT)
            .await
            .map_err(|err| OrchestratorError::InternalError(format!("{err:#}")))?;
        let mut reaped = Vec::new();
        for (sandbox_id, execution_id) in candidates {
            let record = match self.store.get(&sandbox_id).await {
                Ok(record) => record,
                Err(err) => {
                    warn!(
                        %sandbox_id,
                        error = ?err,
                        "grant reaper stopped: the store could not say whether the sandbox exists"
                    );
                    break;
                }
            };
            if record.is_some_and(|record| record.execution_id == execution_id) {
                continue;
            }
            info!(%sandbox_id, %execution_id, "revoking a secret grant no sandbox record backs");
            self.revoke_secrets(sandbox_id, execution_id).await;
            reaped.push((sandbox_id, execution_id));
        }
        Ok(reaped)
    }

    /// One write supersedes the whole previous name set; an empty set revokes.
    async fn regrant_secrets(
        &self,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
        names: &BTreeSet<String>,
    ) -> anyhow::Result<()> {
        if names.is_empty() {
            return self
                .grants()
                .revoke(sandbox_id, execution_id)
                .await
                .with_context(|| format!("revoke the secret grants of sandbox {sandbox_id}"));
        }
        self.grants()
            .grant(sandbox_id, execution_id, names)
            .await
            .with_context(|| {
                format!(
                    "grant {} secret name(s) to sandbox {sandbox_id}",
                    names.len()
                )
            })
    }

    /// Retires the cluster's routing answer for one incarnation.
    ///
    /// A process that installed no routing source runs no cluster routing and
    /// has nothing to retire. A failure is not fatal to the teardown: the
    /// node's own event and the next heartbeat's reconcile still remove the
    /// binding, one interval later.
    async fn forget_runtime_routing(&self, sandbox_id: SandboxId, execution_id: ExecutionId) {
        let Some(routing) = self.runtime_routing.get() else {
            return;
        };
        if let Err(error) = routing.forget(sandbox_id, execution_id).await {
            warn!(
                %sandbox_id,
                %execution_id,
                error = %format_args!("{error:#}"),
                "could not retire this sandbox's routing binding; traffic may reach the node \
                 that no longer runs it until a heartbeat reconciles it"
            );
        }
    }

    /// Drops a sandbox's record, its routing binding and its incarnation's
    /// grant together. The routing binding goes first, so no request is routed
    /// at a runtime this teardown has already stopped; the grant goes even
    /// when the record removal fails, so no terminal teardown can leave one
    /// behind.
    ///
    /// The removal is fenced on `execution_id`: a record another incarnation
    /// wrote under this id belongs to that incarnation, and a caller cleaning
    /// up after its own launch must not take it. The routing retirement
    /// carries the same fence into the binding store.
    async fn forget_sandbox(
        &self,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
    ) -> Result<Option<SandboxMetadata>> {
        // Read first so the caller still learns what the removal took.
        let current = self.store.get(&sandbox_id).await;
        self.forget_runtime_routing(sandbox_id, execution_id).await;
        let removal = self
            .store
            .remove_if_execution(&sandbox_id, execution_id, &ALL_SANDBOX_STATES)
            .await;
        self.revoke_secrets(sandbox_id, execution_id).await;
        match removal? {
            FencedRemoval::Removed => Ok(current?),
            FencedRemoval::Absent => Ok(None),
            FencedRemoval::Superseded {
                state,
                execution_id: actual,
            } => {
                info!(
                    %sandbox_id,
                    %execution_id,
                    actual_execution_id = %actual,
                    actual_state = ?state,
                    "left this sandbox's record alone: a newer incarnation owns it"
                );
                Ok(None)
            }
        }
    }

    /// Returns the real machine reported by the live backend, if remote.
    pub async fn sandbox_holding_node_id(&self, sandbox_id: &SandboxId) -> Option<String> {
        let handle = self.sandboxes.read().await.get(sandbox_id).cloned()?;
        let sandbox = handle.lock().await;
        sandbox.holding_node_id().map(str::to_string)
    }

    /// Stops every known sandbox and tears down in-memory runtime state.
    ///
    /// This is single-flight: the first caller performs cleanup and subsequent
    /// callers wait for the same outcome rather than starting duplicate work.
    ///
    /// Cleanup itself is still best-effort: the executor keeps attempting
    /// remaining sandboxes even if individual deletions fail, then returns an
    /// error if any sandbox could not be cleaned up after several passes.
    #[tracing::instrument(skip(self))]
    pub async fn shutdown(self: &Arc<Self>) -> Result<()> {
        let was_already_shutting_down = self.is_shutting_down.swap(true, Ordering::AcqRel);
        let _ = self.shutdown_tx.send_replace(true);

        if !was_already_shutting_down {
            info!("orchestrator shutdown requested; stopping all sandboxes");
        }

        let this = Arc::clone(self);
        let outcome = self
            .shutdown_outcome
            .get_or_init(|| async move {
                ShutdownOutcome::from_result(this.run_shutdown_cleanup().await)
            })
            .await;

        outcome.as_result()
    }

    /// Pauses a running sandbox: captures it, publishes the capture through the
    /// wired publisher, stops the VM and forgets the record. Afterwards the
    /// sandbox exists only as the snapshot it published.
    ///
    /// A concurrent pause of the same sandbox is joined rather than repeated;
    /// the joiner learns the pause finished, and the publication belongs to the
    /// caller that made it.
    pub async fn pause_sandbox(self: &Arc<Self>, sandbox_id: SandboxId) -> Result<PauseOutcome> {
        let this = Arc::clone(self);
        self.run_cancellation_safe("pause", sandbox_id, async move {
            this.pause_sandbox_inner(sandbox_id).await
        })
        .await
    }

    #[tracing::instrument(
        name = "pause_sandbox",
        skip(self),
        fields(sandbox_id = %sandbox_id)
    )]
    async fn pause_sandbox_inner(self: &Arc<Self>, sandbox_id: SandboxId) -> Result<PauseOutcome> {
        info!("pausing sandbox");
        match self
            .store
            .update_state_if_state(&sandbox_id, SandboxState::Pausing, &[SandboxState::Running])
            .await
        {
            Ok(_) => {}
            Err(StoreError::StateConflict { actual_state, .. }) => {
                return match actual_state {
                    SandboxState::Pausing => self.join_concurrent_pause(sandbox_id).await,
                    SandboxState::Killing => {
                        info!("sandbox is being deleted while pausing");
                        Err(OrchestratorError::SandboxNotFound(sandbox_id))
                    }
                    _ => {
                        info!(state = ?actual_state, "cannot pause sandbox in current state");
                        Err(OrchestratorError::InvalidSandboxState {
                            sandbox_id,
                            state: actual_state,
                        })
                    }
                };
            }
            Err(err) => return Err(OrchestratorError::from(err)),
        }

        self.pause_entered_sandbox(sandbox_id).await
    }

    /// Pauses a sandbox whose record this caller already moved into `Pausing`.
    #[tracing::instrument(
        name = "pause_entered_sandbox",
        skip(self),
        fields(sandbox_id = %sandbox_id)
    )]
    async fn pause_entered_sandbox(
        self: &Arc<Self>,
        sandbox_id: SandboxId,
    ) -> Result<PauseOutcome> {
        let Some(publisher) = self.pause_publisher.get().cloned() else {
            self.rollback_pause_to_running(sandbox_id).await;
            return Err(OrchestratorError::InternalError(format!(
                "sandbox {sandbox_id} cannot be paused: this process has nowhere to publish a pause"
            )));
        };

        // Read the authoritative record after exclusively entering `Pausing`.
        let metadata = match self.store.get(&sandbox_id).await {
            Ok(Some(metadata)) => metadata,
            Ok(None) => {
                warn!("sandbox record disappeared while pausing");
                return Err(OrchestratorError::SandboxNotFound(sandbox_id));
            }
            Err(err) => {
                warn!(error = ?err, "failed to read sandbox record while pausing");
                self.rollback_pause_to_running(sandbox_id).await;
                return Err(OrchestratorError::from(err));
            }
        };
        let expected_execution_id = metadata.execution_id;

        if self.runtime_confirmed_gone(sandbox_id).await {
            self.forget_unrouted_runtime(sandbox_id, expected_execution_id)
                .await;
            return Err(OrchestratorError::SandboxNotFound(sandbox_id));
        }

        let (handle, removed_proxy_route) = self
            .detach_sandbox_handle_and_route_checked(&sandbox_id, expected_execution_id)
            .await;

        // Adopted backends remain operation-scoped, not locally cached.
        let handle_was_held_here = handle.is_some();
        let handle = match handle {
            Some(handle) => handle,
            None => match self.absent_handle(sandbox_id).await {
                // This replica adopted the remote runtime for the operation.
                Ok(AbsentHandle::Adopted(handle)) => handle,
                // Confirmed runtime absence permits record cleanup.
                Ok(AbsentHandle::RuntimeGone) => {
                    warn!("sandbox handle not found while pausing, removing from store");
                    self.forget_sandbox(sandbox_id, expected_execution_id)
                        .await?;
                    return Err(OrchestratorError::SandboxNotFound(sandbox_id));
                }
                Ok(AbsentHandle::NoRecord) => {
                    warn!("sandbox record disappeared while pausing");
                    return Err(OrchestratorError::SandboxNotFound(sandbox_id));
                }
                // Uncertainty rolls the record back without deletion.
                Err(error) => {
                    warn!(error = %error, "could not reach the sandbox while pausing; leaving its record alone");
                    self.rollback_pause_to_running(sandbox_id).await;
                    self.restore_proxy_route(sandbox_id, removed_proxy_route)
                        .await;
                    return Err(error);
                }
            },
        };

        // Capture with the VM paused in place; it stays paused until the
        // capture is durable somewhere, or is resumed when that fails.
        let capture = {
            let mut sandbox = handle.lock().await;
            sandbox.pause().await
        };
        let capture = match capture {
            Ok(capture) => capture,
            Err(err) => {
                warn!(error = ?err, "failed to pause sandbox");
                if err.is_terminal() {
                    // The handle was already detached from `self.sandboxes`
                    // before `pause()`. Do not reinsert it here: the live
                    // runtime may have been mutated and is no longer safe to
                    // keep serving as a running sandbox.
                    self.stop_detached(&handle, "after terminal pause failure")
                        .await;
                    self.forget_sandbox(sandbox_id, expected_execution_id)
                        .await?;
                } else if self.runtime_confirmed_gone(sandbox_id).await {
                    // A node that says it never had this sandbox, and a cluster
                    // that routes nowhere for it, agree: there is nothing to
                    // put back and nothing to retry next tick.
                    self.forget_unrouted_runtime(sandbox_id, expected_execution_id)
                        .await;
                } else {
                    if handle_was_held_here {
                        self.sandboxes.write().await.insert(sandbox_id, handle);
                    }
                    self.restore_proxy_route(sandbox_id, removed_proxy_route)
                        .await;
                    self.rollback_pause_to_running(sandbox_id).await;
                }
                return Err(OrchestratorError::SandboxOperationFailed {
                    sandbox_id,
                    operation: SandboxOperation::Pause,
                    source: err.into(),
                });
            }
        };

        // A capture the node staged can be committed again; a local one cannot,
        // because publishing consumes it.
        let restageable = match &capture {
            crate::snapshot::CapturedSandboxSnapshot::Staged(staged) => Some((**staged).clone()),
            crate::snapshot::CapturedSandboxSnapshot::Local(_) => None,
        };

        let published = match publisher.publish(&metadata, capture).await {
            Ok(published) => published,
            Err(source) => {
                warn!(
                    error = %format_args!("{source:#}"),
                    "the pause capture could not be published; putting the sandbox back"
                );
                let resumed = {
                    let mut sandbox = handle.lock().await;
                    sandbox.resume().await
                };
                match resumed {
                    Ok(()) => {
                        if handle_was_held_here {
                            self.sandboxes.write().await.insert(sandbox_id, handle);
                        }
                        self.restore_proxy_route(sandbox_id, removed_proxy_route)
                            .await;
                        self.rollback_pause_to_running(sandbox_id).await;
                        return Err(OrchestratorError::PausePublicationFailed {
                            sandbox_id,
                            source,
                        });
                    }
                    Err(resume_err) => {
                        // The VM cannot come back, so the staged capture is the
                        // only copy of this sandbox. Commit it again before
                        // giving up on it.
                        warn!(error = ?resume_err, "failed to resume sandbox after a failed publication");
                        // Only a capture the node staged can be committed
                        // again; a local one was consumed by the attempt.
                        let recovered = match restageable.as_ref() {
                            Some(staged) => {
                                self.retry_pause_publication(&publisher, &metadata, staged)
                                    .await
                            }
                            None => Err(source),
                        };
                        match recovered {
                            Ok(published) => {
                                info!(
                                    published = ?published,
                                    "a retried commit published the pause capture of a sandbox \
                                     that could not be resumed"
                                );
                                published
                            }
                            Err(retry_error) => {
                                let snapshot_id = restageable
                                    .as_ref()
                                    .map(|staged| staged.commit.id.to_string())
                                    .unwrap_or_default();
                                let staged_commit = restageable
                                    .as_ref()
                                    .and_then(|staged| serde_json::to_string(staged).ok())
                                    .unwrap_or_default();
                                // The bytes stay where the node put them; this
                                // line is what an operator re-commits from.
                                error!(
                                    %sandbox_id,
                                    %snapshot_id,
                                    %staged_commit,
                                    error = %format_args!("{retry_error:#}"),
                                    "the pause capture of this sandbox could not be committed \
                                     and its sandbox cannot be resumed; the capture bytes were \
                                     kept and only a commit of the staged snapshot recovers it"
                                );
                                self.stop_detached(&handle, "after a failed publication")
                                    .await;
                                if let Err(error) =
                                    self.forget_sandbox(sandbox_id, metadata.execution_id).await
                                {
                                    warn!(error = ?error, "failed to remove sandbox after pause failure");
                                }
                                return Err(OrchestratorError::SandboxOperationFailed {
                                    sandbox_id,
                                    operation: SandboxOperation::Pause,
                                    source: crate::sandbox::SandboxCaptureError::terminal(
                                        retry_error.context(format!(
                                            "the capture could not be committed and the sandbox \
                                             could not be resumed ({resume_err:#}); its capture \
                                             bytes were retained under snapshot {snapshot_id}"
                                        )),
                                    )
                                    .into(),
                                });
                            }
                        }
                    }
                }
            }
        };

        // The capture is durable: stop the VM and let the record go with it.
        self.stop_detached(&handle, "after pausing").await;
        if let Err(err) = self.forget_sandbox(sandbox_id, metadata.execution_id).await {
            warn!(error = ?err, "failed to remove the record of a paused sandbox");
        }
        self.publish_sandbox_event(
            SandboxLifecycleEventType::Pause,
            sandbox_id,
            metadata.execution_id,
            metadata.resources,
        );
        info!(published = ?published, "sandbox paused");

        Ok(PauseOutcome {
            metadata,
            published,
        })
    }

    /// Commits an already-staged pause capture again, with bounded backoff.
    ///
    /// Only reached when the VM is gone: the alternative to a retry is losing
    /// the sandbox, so this waits rather than failing fast. It stops at
    /// [`PAUSE_PUBLICATION_RETRY_DEADLINE`], which is what a caller waiting out
    /// this pause budgets for.
    async fn retry_pause_publication(
        &self,
        publisher: &Arc<dyn PausePublisher>,
        metadata: &SandboxMetadata,
        staged: &crate::snapshot::repository::StagedSnapshot,
    ) -> anyhow::Result<PublishedPause> {
        let deadline = tokio::time::Instant::now() + PAUSE_PUBLICATION_RETRY_DEADLINE;
        let mut delay = PAUSE_PUBLICATION_RETRY_BACKOFF;
        let mut last = anyhow::anyhow!("no commit was attempted");
        for attempt in 2..=PAUSE_PUBLICATION_ATTEMPTS {
            let wakes_at = tokio::time::Instant::now() + delay;
            if wakes_at > deadline {
                warn!(
                    sandbox_id = %metadata.id,
                    attempt,
                    "the commit of this pause capture ran out of its retry budget"
                );
                break;
            }
            tokio::time::sleep_until(wakes_at).await;
            delay = delay.saturating_mul(2);
            let capture = crate::snapshot::CapturedSandboxSnapshot::staged(staged.clone());
            match publisher.publish(metadata, capture).await {
                Ok(published) => return Ok(published),
                Err(error) => {
                    warn!(
                        sandbox_id = %metadata.id,
                        attempt,
                        error = %format_args!("{error:#}"),
                        "retrying the commit of a pause capture whose sandbox cannot be resumed"
                    );
                    last = error;
                }
            }
        }
        Err(last)
    }

    /// Settles a capture whose outcome the node did not classify.
    ///
    /// The runtime is asked directly: still running puts the record back and
    /// keeps the handle, confirmed absent forgets the record, and an
    /// unanswerable probe puts the record back so a later operation asks again.
    /// Nothing is stopped or deleted on an unknown outcome.
    async fn settle_unknown_capture(
        &self,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
        handle: &SandboxHandle,
        transitional_state: SandboxState,
    ) {
        let probe = {
            let mut sandbox = handle.lock().await;
            sandbox.is_still_running().await
        };
        match probe {
            Ok(false) => {
                warn!(
                    %sandbox_id,
                    "the node holding this sandbox says it is no longer running it; \
                     dropping its record"
                );
                self.detach_sandbox_handle_and_route(&sandbox_id).await;
                if let Err(error) = self.forget_sandbox(sandbox_id, execution_id).await {
                    warn!(%sandbox_id, error = ?error, "failed to remove the record of a sandbox its node no longer has");
                }
            }
            Ok(true) => {
                info!(
                    %sandbox_id,
                    "an unclassified capture failure left the sandbox running on its node; \
                     putting its record back"
                );
                let _ = self
                    .store
                    .update_state_if_state(
                        &sandbox_id,
                        SandboxState::Running,
                        &[transitional_state],
                    )
                    .await;
            }
            Err(error) => {
                warn!(
                    %sandbox_id,
                    error = %format_args!("{error:#}"),
                    "could not ask the node what became of this sandbox; putting its record \
                     back for a later operation to settle"
                );
                let _ = self
                    .store
                    .update_state_if_state(
                        &sandbox_id,
                        SandboxState::Running,
                        &[transitional_state],
                    )
                    .await;
            }
        }
    }

    async fn rollback_pause_to_running(&self, sandbox_id: SandboxId) {
        let _ = self
            .store
            .update_state_if_state(&sandbox_id, SandboxState::Running, &[SandboxState::Pausing])
            .await;
    }

    async fn stop_detached(&self, handle: &SandboxHandle, when: &'static str) {
        let stop_result = {
            let mut sandbox = handle.lock().await;
            sandbox.stop().await
        };
        if let Err(err) = stop_result {
            warn!(error = ?err, when, "failed to stop sandbox");
        }
    }

    /// Captures a snapshot of a running sandbox.
    pub async fn capture_snapshot(
        self: &Arc<Self>,
        sandbox_id: SandboxId,
    ) -> Result<SnapshotCaptureResult> {
        let this = Arc::clone(self);
        self.run_cancellation_safe("snapshot", sandbox_id, async move {
            this.capture_snapshot_inner(sandbox_id).await
        })
        .await
    }

    #[tracing::instrument(
        name = "capture_snapshot",
        skip(self),
        fields(sandbox_id = %sandbox_id)
    )]
    async fn capture_snapshot_inner(
        self: Arc<Self>,
        sandbox_id: SandboxId,
    ) -> Result<SnapshotCaptureResult> {
        self.ensure_accepting_lifecycle_operations()?;

        info!("capturing sandbox snapshot");
        match self
            .store
            .update_state_if_state(
                &sandbox_id,
                SandboxState::Snapshotting,
                &[SandboxState::Running],
            )
            .await
        {
            Ok(_) => {}
            Err(StoreError::StateConflict { actual_state, .. }) => {
                return match actual_state {
                    SandboxState::Killing => Err(OrchestratorError::SandboxNotFound(sandbox_id)),
                    _ => Err(OrchestratorError::InvalidSandboxState {
                        sandbox_id,
                        state: actual_state,
                    }),
                };
            }
            Err(err) => return Err(OrchestratorError::from(err)),
        }

        // Read authoritative execution after exclusively entering `Snapshotting`.
        let expected_execution_id = match self.store.get(&sandbox_id).await {
            Ok(Some(metadata)) => metadata.execution_id,
            Ok(None) => {
                warn!("sandbox record disappeared while snapshotting");
                return Err(OrchestratorError::SandboxNotFound(sandbox_id));
            }
            Err(err) => {
                warn!(error = ?err, "could not read sandbox record while snapshotting; leaving its record alone");
                let _ = self
                    .store
                    .update_state_if_state(
                        &sandbox_id,
                        SandboxState::Running,
                        &[SandboxState::Snapshotting],
                    )
                    .await;
                return Err(OrchestratorError::from(err));
            }
        };

        // Discard cached handles superseded by another execution.
        let handle = self
            .cached_handle_for_execution(sandbox_id, expected_execution_id)
            .await;
        let handle = match handle {
            Some(handle) => handle,
            None => match self.absent_handle(sandbox_id).await {
                Ok(AbsentHandle::Adopted(handle)) => handle,
                // Only confirmed runtime absence authorizes record cleanup.
                Ok(AbsentHandle::RuntimeGone) => {
                    warn!("sandbox handle not found while snapshotting, removing from store");
                    self.detach_sandbox_handle_and_route(&sandbox_id).await;
                    self.forget_sandbox(sandbox_id, expected_execution_id)
                        .await?;
                    return Err(OrchestratorError::SandboxNotFound(sandbox_id));
                }
                Ok(AbsentHandle::NoRecord) => {
                    warn!("sandbox record disappeared while snapshotting");
                    return Err(OrchestratorError::SandboxNotFound(sandbox_id));
                }
                Err(error) => {
                    warn!(error = %error, "could not reach the sandbox while snapshotting; leaving its record alone");
                    let _ = self
                        .store
                        .update_state_if_state(
                            &sandbox_id,
                            SandboxState::Running,
                            &[SandboxState::Snapshotting],
                        )
                        .await;
                    return Err(error);
                }
            },
        };

        // Call sandbox backend to capture the snapshot.
        let captured_snapshot_result = {
            let mut sandbox = handle.lock().await;
            sandbox.snapshot().await
        };

        // If snapshot capture failed, attempt to roll back to Running state and return an error.
        let captured_snapshot = match captured_snapshot_result {
            Ok(captured_snapshot) => captured_snapshot,
            Err(err) => {
                warn!(error = ?err, "failed to capture sandbox snapshot");
                if err.is_unknown() {
                    // Nobody said what the node did, so ask it. A running
                    // sandbox is put back untouched; only a node that says the
                    // sandbox is gone authorizes forgetting the record.
                    self.settle_unknown_capture(
                        sandbox_id,
                        expected_execution_id,
                        &handle,
                        SandboxState::Snapshotting,
                    )
                    .await;
                } else if err.is_terminal() {
                    self.detach_sandbox_handle_and_route(&sandbox_id).await;
                    let stop_result = {
                        let mut sandbox = handle.lock().await;
                        sandbox.stop().await
                    };
                    if let Err(stop_err) = stop_result {
                        warn!(error = ?stop_err, "failed to stop sandbox after terminal snapshot failure");
                    }
                    self.forget_sandbox(sandbox_id, expected_execution_id)
                        .await?;
                } else {
                    let _ = self
                        .store
                        .update_state_if_state(
                            &sandbox_id,
                            SandboxState::Running,
                            &[SandboxState::Snapshotting],
                        )
                        .await;
                }
                return Err(OrchestratorError::SandboxOperationFailed {
                    sandbox_id,
                    operation: SandboxOperation::Snapshot,
                    source: err.into(),
                });
            }
        };

        // Update the sandbox state back to Running and return the captured snapshot along with the latest metadata.
        self.store
            .update_state_if_state(
                &sandbox_id,
                SandboxState::Running,
                &[SandboxState::Snapshotting],
            )
            .await?;
        let metadata = match self.store.get(&sandbox_id).await? {
            Some(metadata) => metadata,
            None => {
                warn!("sandbox disappeared after snapshotting");
                return Err(OrchestratorError::SandboxNotFound(sandbox_id));
            }
        };

        info!("snapshot captured");
        Ok(SnapshotCaptureResult {
            metadata,
            captured_snapshot,
        })
    }

    pub async fn replace_sandbox_network_policy(
        self: &Arc<Self>,
        sandbox_id: SandboxId,
        network_policy: SandboxNetworkPolicy,
    ) -> Result<()> {
        let this = Arc::clone(self);
        self.run_cancellation_safe("update_network", sandbox_id, async move {
            this.replace_sandbox_network_policy_inner(sandbox_id, network_policy)
                .await
        })
        .await
    }

    #[tracing::instrument(
        name = "replace_sandbox_network_policy",
        skip(self, network_policy),
        fields(sandbox_id = %sandbox_id))
    ]
    async fn replace_sandbox_network_policy_inner(
        &self,
        sandbox_id: SandboxId,
        network_policy: SandboxNetworkPolicy,
    ) -> Result<()> {
        let metadata = self
            .store
            .get(&sandbox_id)
            .await?
            .ok_or(OrchestratorError::SandboxNotFound(sandbox_id))?;
        if metadata.state != SandboxState::Running {
            return Err(OrchestratorError::InvalidSandboxState {
                sandbox_id,
                state: metadata.state,
            });
        }

        // Rebuild missing or superseded cached handles from the record.
        let sandbox = self
            .cached_handle_for_execution(sandbox_id, metadata.execution_id)
            .await;
        let sandbox = match sandbox {
            Some(handle) => handle,
            None => match self.absent_handle(sandbox_id).await? {
                AbsentHandle::Adopted(handle) => handle,
                AbsentHandle::RuntimeGone => {
                    return Err(OrchestratorError::SandboxOperationConflict {
                        sandbox_id,
                        operation: SandboxOperation::UpdateNetwork,
                    })
                }
                AbsentHandle::NoRecord => {
                    return Err(OrchestratorError::SandboxNotFound(sandbox_id))
                }
            },
        };

        // The grant is what the broker checks, so it has to name exactly the
        // new policy's secrets before the new rules can serve a request.
        let previous_names = metadata.network_policy.egress.referenced_secret_names();
        let next_names = network_policy.egress.referenced_secret_names();
        let grant_changed = next_names != previous_names;
        if grant_changed {
            self.regrant_secrets(sandbox_id, metadata.execution_id, &next_names)
                .await
                .map_err(|source| OrchestratorError::SandboxOperationFailed {
                    sandbox_id,
                    operation: SandboxOperation::UpdateNetwork,
                    source,
                })?;
        }

        let runtime_policy = network_policy.runtime_policy();

        let update_result = {
            let mut sandbox = sandbox.lock().await;
            sandbox.update_network_policy(runtime_policy).await
        };
        if let Err(source) = update_result {
            if grant_changed {
                // The runtime kept the previous rules, so its grant goes back too.
                if let Err(err) = self
                    .regrant_secrets(sandbox_id, metadata.execution_id, &previous_names)
                    .await
                {
                    warn!(
                        error = %format_args!("{err:#}"),
                        "failed to put the previous secret grant back after a refused network update"
                    );
                }
            }
            return Err(OrchestratorError::SandboxOperationFailed {
                sandbox_id,
                operation: SandboxOperation::UpdateNetwork,
                source,
            });
        }

        self.store
            .update_if_state(&sandbox_id, &[SandboxState::Running], |metadata| {
                metadata.network_policy = network_policy;
            })
            .await?;

        Ok(())
    }

    /// Patch the custom extension params of a running sandbox.
    ///
    /// The patch document is passed through verbatim to the custom
    /// extension's patch-params hook, which returns the updated full params.
    /// On hook failure the sandbox keeps its previous params and the
    /// metadata store is left untouched. Returns the new full params (`None`
    /// means empty params).
    pub async fn patch_sandbox_custom_extension_params(
        self: &Arc<Self>,
        sandbox_id: SandboxId,
        patch: serde_json::Map<String, serde_json::Value>,
    ) -> Result<Option<CustomExtensionParams>> {
        let this = Arc::clone(self);
        self.run_cancellation_safe("patch_custom_extension_params", sandbox_id, async move {
            this.patch_sandbox_custom_extension_params_inner(sandbox_id, patch)
                .await
        })
        .await
    }

    #[tracing::instrument(
        name = "patch_sandbox_custom_extension_params",
        skip(self, patch),
        fields(sandbox_id = %sandbox_id))
    ]
    async fn patch_sandbox_custom_extension_params_inner(
        &self,
        sandbox_id: SandboxId,
        patch: serde_json::Map<String, serde_json::Value>,
    ) -> Result<Option<CustomExtensionParams>> {
        let metadata = self
            .store
            .get(&sandbox_id)
            .await?
            .ok_or(OrchestratorError::SandboxNotFound(sandbox_id))?;
        if metadata.state != SandboxState::Running {
            return Err(OrchestratorError::InvalidSandboxState {
                sandbox_id,
                state: metadata.state,
            });
        }

        // Discard cached handles superseded by the authoritative execution.
        let sandbox = self
            .cached_handle_for_execution(sandbox_id, metadata.execution_id)
            .await;
        let sandbox = match sandbox {
            Some(handle) => handle,
            None => match self.absent_handle(sandbox_id).await? {
                AbsentHandle::Adopted(handle) => handle,
                AbsentHandle::RuntimeGone => {
                    return Err(OrchestratorError::SandboxOperationConflict {
                        sandbox_id,
                        operation: SandboxOperation::PatchCustomExtensionParams,
                    })
                }
                AbsentHandle::NoRecord => {
                    return Err(OrchestratorError::SandboxNotFound(sandbox_id))
                }
            },
        };

        // Invoke the extension's patch-params hook here (the backend only
        // stores the approved value). The sandbox lock is not held during
        // the hook call so pause/stop are not blocked on extension latency.
        let client = CustomExtensionClient::global().ok_or_else(|| {
            OrchestratorError::SandboxOperationFailed {
                sandbox_id,
                operation: SandboxOperation::PatchCustomExtensionParams,
                source: anyhow::anyhow!(
                    "custom extension is not configured ([custom_extension].url is unset)"
                ),
            }
        })?;
        let new_params = client
            .hook_patch_params(sandbox_id, patch)
            .await
            .map_err(|source| OrchestratorError::SandboxOperationFailed {
                sandbox_id,
                operation: SandboxOperation::PatchCustomExtensionParams,
                source,
            })?;

        self.apply_custom_extension_params(sandbox_id, &sandbox, new_params.clone())
            .await?;

        Ok(new_params)
    }

    /// Applies already-approved custom extension parameters without invoking hooks.
    pub async fn replace_sandbox_custom_extension_params(
        self: &Arc<Self>,
        sandbox_id: SandboxId,
        params: Option<CustomExtensionParams>,
    ) -> Result<()> {
        let this = Arc::clone(self);
        self.run_cancellation_safe("replace_custom_extension_params", sandbox_id, async move {
            this.replace_sandbox_custom_extension_params_inner(sandbox_id, params)
                .await
        })
        .await
    }

    #[tracing::instrument(
        name = "replace_sandbox_custom_extension_params",
        skip(self, params),
        fields(sandbox_id = %sandbox_id))
    ]
    async fn replace_sandbox_custom_extension_params_inner(
        &self,
        sandbox_id: SandboxId,
        params: Option<CustomExtensionParams>,
    ) -> Result<()> {
        let metadata = self
            .store
            .get(&sandbox_id)
            .await?
            .ok_or(OrchestratorError::SandboxNotFound(sandbox_id))?;
        if metadata.state != SandboxState::Running {
            return Err(OrchestratorError::InvalidSandboxState {
                sandbox_id,
                state: metadata.state,
            });
        }

        // Discard cached handles superseded by the authoritative execution.
        let sandbox = self
            .cached_handle_for_execution(sandbox_id, metadata.execution_id)
            .await;
        let sandbox = match sandbox {
            Some(handle) => handle,
            None => match self.absent_handle(sandbox_id).await? {
                AbsentHandle::Adopted(handle) => handle,
                AbsentHandle::RuntimeGone => {
                    return Err(OrchestratorError::SandboxOperationConflict {
                        sandbox_id,
                        operation: SandboxOperation::PatchCustomExtensionParams,
                    })
                }
                AbsentHandle::NoRecord => {
                    return Err(OrchestratorError::SandboxNotFound(sandbox_id))
                }
            },
        };

        self.apply_custom_extension_params(sandbox_id, &sandbox, params)
            .await
    }

    /// Applies extension parameters to the backend before persisting them.
    async fn apply_custom_extension_params(
        &self,
        sandbox_id: SandboxId,
        sandbox: &SandboxHandle,
        params: Option<CustomExtensionParams>,
    ) -> Result<()> {
        let update_result = {
            let mut sandbox = sandbox.lock().await;
            sandbox.update_custom_extension_params(params.clone()).await
        };
        update_result.map_err(|source| OrchestratorError::SandboxOperationFailed {
            sandbox_id,
            operation: SandboxOperation::PatchCustomExtensionParams,
            source,
        })?;

        // NOTE: a concurrent pause may have transitioned the sandbox since the entry check,
        // so this may fail. But it's acceptable since extension state should be transient like network policy
        self.store
            .update_if_state(&sandbox_id, &[SandboxState::Running], |metadata| {
                metadata.custom_extension_params = params.clone();
            })
            .await
            .map_err(|err| match err {
                // Lost a race against a concurrent state transition (e.g.
                // pause): report it as a conflict instead of a 500.
                StoreError::StateConflict {
                    sandbox_id,
                    actual_state,
                    ..
                } => OrchestratorError::InvalidSandboxState {
                    sandbox_id,
                    state: actual_state,
                },
                other => OrchestratorError::from(other),
            })?;

        Ok(())
    }

    /// Returns the current orchestrator metrics snapshot.
    ///
    /// Counter fields are read atomically; resource fields are aggregated by
    /// scanning the metadata store, so the returned snapshot is always
    /// consistent with the orchestrator's current set of sandboxes.
    pub async fn metrics_snapshot(&self) -> Result<OrchestratorMetrics> {
        let mut metrics = OrchestratorMetrics::default();
        self.store
            .list_with_callback(|metadata| {
                aggregate_resource_metrics(
                    &mut metrics,
                    SandboxContribution::new(metadata.state, metadata.resources),
                );
            })
            .await?;
        metrics.create_successes = self.counters.create_successes();
        metrics.create_fails = self.counters.create_fails();
        Ok(metrics)
    }

    /// Number of cached handles discarded after execution supersession.
    pub fn stale_handle_discards(&self) -> u64 {
        self.counters.stale_handles_discarded()
    }

    pub fn subscribe_sandbox_events(&self) -> broadcast::Receiver<SandboxLifecycleEvent> {
        self.sandbox_event_tx.subscribe()
    }

    fn publish_sandbox_event(
        &self,
        event_type: SandboxLifecycleEventType,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
        resources: SandboxResources,
    ) {
        let event = SandboxLifecycleEvent {
            event_type,
            sandbox_id,
            execution_id,
            resources,
        };
        let _ = self.sandbox_event_tx.send(event);
    }

    /// Waits for `sandbox_id` to leave `transitional_state`, then returns the
    /// resulting metadata. Returns `SandboxNotFound` if the sandbox is removed
    /// while waiting, or `InvalidSandboxState` if the sandbox is still in the
    /// transitional state after the [`WAIT_TRANSITION_TIMEOUT`] elapses.
    async fn wait_for_transition(
        &self,
        sandbox_id: SandboxId,
        transitional_state: SandboxState,
    ) -> Result<SandboxMetadata> {
        let states = [transitional_state];
        let wait = self.store.wait_while_in_states(&sandbox_id, &states);
        match tokio::time::timeout(WAIT_TRANSITION_TIMEOUT, wait).await {
            Ok(Ok(Some(m))) => Ok(m),
            Ok(Ok(None)) => Err(OrchestratorError::SandboxNotFound(sandbox_id)),
            Ok(Err(e)) => Err(OrchestratorError::from(e)),
            Err(_elapsed) => {
                warn!(
                    sandbox_id = %sandbox_id,
                    state = ?transitional_state,
                    "timed out waiting for sandbox to leave transitional state"
                );
                Err(OrchestratorError::InvalidSandboxState {
                    sandbox_id,
                    state: transitional_state,
                })
            }
        }
    }

    /// Waits for a pause in flight on `sandbox_id` to settle.
    ///
    /// `None` is the pause having finished: the record is gone and the
    /// sandbox's newest catalog row speaks for it from here. `Some` is the
    /// record as it stands after a pause that did not finish. A pause still
    /// in flight after [`CONCURRENT_PAUSE_WAIT`] is
    /// `InvalidSandboxState { state: Pausing }`.
    pub async fn wait_for_pause_to_settle(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<SandboxMetadata>> {
        let wait = self
            .store
            .wait_while_in_states(&sandbox_id, &[SandboxState::Pausing]);
        match tokio::time::timeout(CONCURRENT_PAUSE_WAIT, wait).await {
            Ok(settled) => settled.map_err(OrchestratorError::from),
            Err(_elapsed) => {
                warn!(%sandbox_id, "timed out waiting for a pause to settle");
                Err(OrchestratorError::InvalidSandboxState {
                    sandbox_id,
                    state: SandboxState::Pausing,
                })
            }
        }
    }

    /// Joins a pause another caller is performing on the same sandbox.
    ///
    /// The record disappearing is not on its own the pause finishing: the owner
    /// removes the record on several failure paths too, so a joiner reports
    /// success only against a snapshot row a pause of this sandbox left behind.
    async fn join_concurrent_pause(&self, sandbox_id: SandboxId) -> Result<PauseOutcome> {
        debug!("concurrent pause in progress, waiting for completion");
        let joined_at = SystemTime::now();
        let before = self
            .store
            .get(&sandbox_id)
            .await?
            .ok_or(OrchestratorError::SandboxNotFound(sandbox_id))?;
        let wait = self
            .store
            .wait_while_in_states(&sandbox_id, &[SandboxState::Pausing]);
        match tokio::time::timeout(CONCURRENT_PAUSE_WAIT, wait).await {
            Ok(Ok(None)) => {
                self.joined_pause_publication(sandbox_id, before, joined_at)
                    .await
            }
            Ok(Ok(Some(after))) => match after.state {
                SandboxState::Running => {
                    info!("concurrent pause failed; sandbox returned to running state");
                    Err(OrchestratorError::InvalidSandboxState {
                        sandbox_id,
                        state: SandboxState::Running,
                    })
                }
                SandboxState::Killing => {
                    info!("sandbox is being deleted after concurrent pause attempt");
                    Err(OrchestratorError::SandboxNotFound(sandbox_id))
                }
                other => {
                    info!(state = ?other, "unexpected state after waiting for concurrent pause");
                    Err(OrchestratorError::InvalidSandboxState {
                        sandbox_id,
                        state: other,
                    })
                }
            },
            Ok(Err(err)) => Err(OrchestratorError::from(err)),
            Err(_elapsed) => {
                warn!("timed out waiting for a concurrent pause to finish");
                Err(OrchestratorError::InvalidSandboxState {
                    sandbox_id,
                    state: SandboxState::Pausing,
                })
            }
        }
    }

    /// Finds the snapshot the pause this caller joined published.
    ///
    /// The window starts one [`CONCURRENT_PAUSE_WAIT`] before the joiner began
    /// waiting, which is as far back as a pause still in flight can have
    /// started.
    async fn joined_pause_publication(
        &self,
        sandbox_id: SandboxId,
        before: SandboxMetadata,
        joined_at: SystemTime,
    ) -> Result<PauseOutcome> {
        let Some(publisher) = self.pause_publisher.get().cloned() else {
            return Err(OrchestratorError::InternalError(format!(
                "sandbox {sandbox_id} was paused elsewhere and this process has nowhere to look \
                 for what that pause published"
            )));
        };
        let not_before_unix_ms = joined_at
            .checked_sub(CONCURRENT_PAUSE_WAIT)
            .and_then(|start| start.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|since| i64::try_from(since.as_millis()).unwrap_or(i64::MAX))
            .unwrap_or(0);
        match publisher
            .published_since(sandbox_id, not_before_unix_ms)
            .await
        {
            Ok(Some(snapshot_id)) => {
                debug!(%snapshot_id, "concurrent pause succeeded");
                Ok(PauseOutcome {
                    metadata: before,
                    published: PublishedPause::Committed(snapshot_id),
                })
            }
            Ok(None) => {
                warn!(
                    "the record of a sandbox this caller was waiting on disappeared without a \
                     snapshot to show for it"
                );
                Err(OrchestratorError::PausePublicationFailed {
                    sandbox_id,
                    source: anyhow::anyhow!(
                        "the pause this call joined removed the sandbox's record without \
                         publishing a snapshot for it"
                    ),
                })
            }
            Err(error) => {
                warn!(
                    error = %format_args!("{error:#}"),
                    "could not tell whether the pause this caller joined published anything"
                );
                Err(OrchestratorError::PausePublicationFailed {
                    sandbox_id,
                    source: error.context(
                        "the pause this call joined cannot be confirmed to have published \
                         a snapshot",
                    ),
                })
            }
        }
    }

    /// Automatically pauses or stops sandboxes whose timeout has expired.
    async fn evict_expired_sandboxes(self: &Arc<Self>) -> Result<Vec<SandboxId>> {
        if self.is_shutting_down() {
            debug!("skipping auto-evict because orchestrator is shutting down");
            return Ok(Vec::new());
        }

        // Process a bounded oldest-first eviction batch.
        let expired = self
            .store
            .expired_batch(SystemTime::now(), AUTO_EVICT_BATCH_LIMIT)
            .await?;
        let mut evicted_ids = Vec::new();

        for metadata in expired {
            if metadata.state != SandboxState::Running {
                continue;
            }
            let target_state = match metadata.timeout_action {
                SandboxTimeoutAction::Pause => SandboxState::Pausing,
                SandboxTimeoutAction::Delete => SandboxState::Killing,
            };
            // The batch is a stale read; the claim re-checks expiry and the
            // incarnation against the record as it stands now.
            if !self.claim_eviction(&metadata, target_state).await {
                continue;
            }
            if let Err(err) = match metadata.timeout_action {
                SandboxTimeoutAction::Pause => {
                    self.pause_entered_sandbox(metadata.id).await.map(|_| ())
                }
                SandboxTimeoutAction::Delete => {
                    self.delete_entered_sandbox(metadata.id, SandboxState::Running)
                        .await
                }
            } {
                warn!(
                    sandbox_id = %metadata.id,
                    action = ?metadata.timeout_action,
                    error = ?err,
                    "failed to auto-evict expired sandbox"
                );
                continue;
            }
            evicted_ids.push(metadata.id);
        }

        Ok(evicted_ids)
    }

    /// Enters `target_state` for an eviction, atomically with a re-check that
    /// the sandbox is still the same run and still expired.
    ///
    /// `false` means somebody kept the sandbox alive or replaced it between
    /// the batch read and now, and this tick must leave it alone.
    async fn claim_eviction(&self, metadata: &SandboxMetadata, target_state: SandboxState) -> bool {
        let request = TransitionRequest::new(target_state, vec![SandboxState::Running])
            .with_execution(metadata.execution_id)
            .as_eviction();
        match self.store.start_transition(&metadata.id, request).await {
            Ok(TransitionOutcome::Started(guard)) => {
                // The operation settles the record itself, so the claim has
                // nothing left to do once the state is entered.
                if let Err(error) = guard.release().await {
                    warn!(
                        sandbox_id = %metadata.id,
                        error = %error,
                        "could not release an eviction claim; it expires on its own"
                    );
                }
                true
            }
            Ok(TransitionOutcome::NotExpired) => {
                debug!(
                    sandbox_id = %metadata.id,
                    "a keep-alive landed after this sandbox was picked for eviction"
                );
                false
            }
            Ok(TransitionOutcome::InFlight { transition_id }) => {
                debug!(
                    sandbox_id = %metadata.id,
                    %transition_id,
                    "another caller is already moving this sandbox out of running"
                );
                false
            }
            Err(error) => {
                debug!(
                    sandbox_id = %metadata.id,
                    error = %error,
                    "the sandbox picked for eviction is no longer the one that was picked"
                );
                false
            }
        }
    }

    /// Starts a background task that periodically evicts expired sandboxes.
    /// The eviction policy is defined by the sandbox's [`timeout_action`](SandboxMetadata::timeout_action).
    fn start_auto_evict_task(
        this: Arc<Self>,
        evict_interval: Duration,
        mut shutdown_rx: watch::Receiver<bool>,
    ) {
        let Ok(runtime_handle) = tokio::runtime::Handle::try_current() else {
            warn!("auto-evict task not started: no Tokio runtime available");
            return;
        };

        let this = Arc::downgrade(&this);
        runtime_handle.spawn(async move {
            let mut ticker = tokio::time::interval(evict_interval);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            debug!("auto-evict task started with interval {:?}", evict_interval);

            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            debug!("auto-evict task stopping because orchestrator is shutting down");
                            break;
                        }
                    }
                    _ = ticker.tick() => {
                        let Some(this) = this.upgrade() else {
                            debug!("auto-evict task stopping because orchestrator was dropped");
                            break;
                        };
                        if let Err(err) = this.evict_expired_sandboxes().await {
                            warn!("auto-evict task failed: {err}");
                        }
                        if let Err(err) = this.reap_orphaned_grants().await {
                            warn!("grant reaper failed: {err}");
                        }
                    }
                }
            }
        });
    }

    /// Starts a background task that periodically runs local image maintenance
    /// (capacity eviction + fail-closed GC) over the current running set.
    fn start_local_image_maintenance_task(
        this: Arc<Self>,
        interval: Duration,
        mut shutdown_rx: watch::Receiver<bool>,
    ) {
        let Ok(runtime_handle) = tokio::runtime::Handle::try_current() else {
            warn!("local image maintenance task not started: no Tokio runtime available");
            return;
        };

        let this = Arc::downgrade(&this);
        runtime_handle.spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            info!(interval = ?interval, "local image maintenance task started");

            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            debug!("local image maintenance task stopping because orchestrator is shutting down");
                            break;
                        }
                    }
                    _ = ticker.tick() => {
                        let Some(this) = this.upgrade() else {
                            debug!("local image maintenance task stopping because orchestrator was dropped");
                            break;
                        };

                        let running = this.collect_running_artifacts().await;
                        if let Err(err) = this.image_refs.maintain_running(running).await {
                            warn!("local image maintenance pass failed: {err:#}");
                        }
                    }
                }
            }
        });
    }

    /// Stamps complete create metadata as the node's opaque ownership marker.
    fn stamp_control_plane_ownership(&self, plan: &mut LaunchPlan) {
        if !self.factory.stamps_control_plane_ownership() {
            return;
        }
        if plan.metadata.control_plane_config.is_none() {
            plan.metadata.control_plane_config = ControlPlaneConfig::for_record(&plan.metadata);
        }
        plan.launch_config.control_plane_config = plan
            .metadata
            .control_plane_config
            .as_ref()
            .map(|marker| marker.as_bytes().to_vec());
    }

    #[tracing::instrument(skip(self, plan))]
    async fn launch_sandbox(self: &Arc<Self>, plan: LaunchPlan) -> Result<SandboxMetadata> {
        // The id is taken here, before anything is allocated, because every
        // later marker of a launch -- the handle, the record -- only appears
        // once the runtime is running, and two launches racing to that point
        // tear each other's resources down.
        let sandbox_id = plan.sandbox_id;
        let claim = match self.launch_claims.claim(sandbox_id, plan.execution_id()) {
            Ok(claim) => claim,
            Err(held) => {
                warn!(
                    %sandbox_id,
                    held_execution_id = %held.execution_id(),
                    execution_id = %plan.execution_id(),
                    "refusing a launch under a sandbox id this process is already launching"
                );
                return Err(OrchestratorError::LaunchInFlight { sandbox_id });
            }
        };
        let result = self.launch_claimed_sandbox(plan).await;
        match &result {
            Ok(metadata) => claim.settle(LaunchSettlement::Launched(Box::new(metadata.clone()))),
            Err(error) => claim.settle(LaunchSettlement::Failed(LaunchFailure::of(error))),
        }
        result
    }

    async fn launch_claimed_sandbox(self: &Arc<Self>, plan: LaunchPlan) -> Result<SandboxMetadata> {
        self.ensure_accepting_lifecycle_operations()?;

        // Stamp before both record persistence and backend construction.
        let mut plan = plan;
        self.stamp_control_plane_ownership(&mut plan);
        let plan = plan;

        let sandbox_id = plan.sandbox_id;
        let transitional_state = plan.transitional_state();

        // Refuse an id this process already holds before anything is built.
        // The handle table has one slot per sandbox, so starting a second
        // runtime under a live id would replace the entry the first one is
        // reached through and then tear that entry down when the store refuses
        // the duplicate record.
        if let Some(held) = self.held_sandbox(sandbox_id).await? {
            // A routing source with a complete view is the only thing that can
            // say the id is free here: a replica that watched a pause happen
            // elsewhere still holds a handle, and that handle must not refuse
            // this sandbox's resume.
            let residue = match held.execution_id {
                Some(execution_id) if self.runtime_confirmed_gone(sandbox_id).await => {
                    Some(execution_id)
                }
                _ => None,
            };
            match residue {
                Some(execution_id) => self.forget_unrouted_runtime(sandbox_id, execution_id).await,
                None => {
                    warn!(
                        held_execution_id = ?held.execution_id,
                        execution_id = %plan.execution_id(),
                        "refusing a launch under a sandbox id this process already holds"
                    );
                    return Err(OrchestratorError::StoreOperationFailed(
                        StoreError::SandboxAlreadyExists { sandbox_id },
                    ));
                }
            }
        }

        // Build and start the sandbox first, before making any state changes, so that we don't
        // have to roll back any persisted state if the build fails.
        // Meanwhile, the start process can be overlapped with the initial state persistence.
        let mut sandbox = match self.build_sandbox(&plan) {
            Ok(sandbox) => sandbox,
            Err(err) => {
                self.rollback_failed_launch_metadata(&plan, transitional_state)
                    .await;
                return Err(err);
            }
        };
        sandbox.set_projection_budget(plan.projection_ttl_secs(SystemTime::now()));

        // Protect artifacts before the backend opens them.
        let startup_artifacts = sandbox.startup_artifacts();
        if let Err(err) = self
            .protect_image_refs(
                RuntimeImageOwner::StartingSandbox(sandbox_id),
                startup_artifacts,
                "starting sandbox",
            )
            .await
        {
            warn!(error = %format_args!("{err:#}"), "failed to protect starting runtime artifacts");
            self.rollback_failed_launch_metadata(&plan, transitional_state)
                .await;
            return Err(err);
        }
        if let Err(source) = self
            .grant_secrets(
                sandbox_id,
                plan.execution_id(),
                &plan.metadata.network_policy,
            )
            .await
        {
            warn!(error = %format_args!("{source:#}"), "failed to grant secrets before start");
            if let Err(stop_err) = sandbox.stop().await {
                warn!(error = %format_args!("{stop_err:#}"), "failed to stop sandbox after grant failure");
            }
            self.rollback_failed_launch_metadata(&plan, transitional_state)
                .await;
            return Err(OrchestratorError::SandboxOperationFailed {
                sandbox_id,
                operation: SandboxOperation::Start,
                source,
            });
        }
        if let Err(source) = sandbox.start_nowait().await {
            warn!(error = %format_args!("{source:#}"), "failed to start sandbox");
            self.revoke_secrets(sandbox_id, plan.execution_id()).await;
            if let Err(stop_err) = sandbox.stop().await {
                warn!(error = %format_args!("{stop_err:#}"), "failed to stop sandbox after start failure");
            }
            self.rollback_failed_launch_metadata(&plan, transitional_state)
                .await;
            return Err(OrchestratorError::SandboxOperationFailed {
                sandbox_id,
                operation: SandboxOperation::Start,
                source,
            });
        }
        debug!("sandbox start requested");

        // If the orchestrator started shutting down, stop here before we persist any state.
        if self.is_shutting_down() {
            info!("orchestrator started shutting down just after starting the sandbox");
            if let Err(err) = sandbox.stop().await {
                warn!(error = %format_args!("{err:#}"), "failed to stop sandbox");
            }
            self.rollback_failed_launch_metadata(&plan, transitional_state)
                .await;
            return Err(OrchestratorError::ShuttingDown);
        }

        let runtime_info = sandbox.runtime_info();
        let runtime_resources = resources_with_runtime_info(plan.resources(), runtime_info.clone());
        let transitional_metadata = {
            let mut metadata = plan.metadata.clone();
            metadata.resources = runtime_resources;
            // Remote backends fill resolution facts after start.
            if let Some(facts) = runtime_info.resolved_image_facts.clone() {
                metadata.context = facts.context;
                metadata.image_configs = facts.image_configs;
            }
            metadata
        };

        // Store the sandbox handle in memory.
        let handle = Arc::new(Mutex::new(sandbox));
        self.sandboxes
            .write()
            .await
            .insert(sandbox_id, handle.clone());

        self.release_image_refs(RuntimeImageOwner::StartingSandbox(sandbox_id))
            .await;

        if let Err(err) = self.store.add(transitional_metadata).await {
            warn!(error = %format_args!("{err:#}"), "failed to persist sandbox metadata; cleaning up");
            self.cleanup_failed_launch(&plan, handle, FailedLaunchStage::Registered)
                .await;
            return Err(OrchestratorError::from(err));
        }

        // Check for shutdown again before we wait for the sandbox to become ready.
        if self.is_shutting_down() {
            info!("orchestrator started shutting down before sandbox became ready");
            self.cleanup_failed_launch(&plan, handle, FailedLaunchStage::TransitionalPersisted)
                .await;
            return Err(OrchestratorError::ShuttingDown);
        }

        // Wait for the sandbox to be ready
        let wait_result = {
            let sandbox = handle.lock().await;
            sandbox.wait_for_ready().await
        };
        if let Err(source) = wait_result {
            warn!(error = %format_args!("{source:#}"), "sandbox failed to become ready");
            self.cleanup_failed_launch(&plan, handle, FailedLaunchStage::TransitionalPersisted)
                .await;
            return Err(OrchestratorError::SandboxOperationFailed {
                sandbox_id,
                operation: SandboxOperation::WaitReady,
                source,
            });
        }

        // Check for shutdown again before we persist the final state and publish the proxy route.
        if self.is_shutting_down() {
            info!("orchestrator started shutting down while sandbox was becoming ready");
            self.cleanup_failed_launch(&plan, handle, FailedLaunchStage::TransitionalPersisted)
                .await;
            return Err(OrchestratorError::ShuttingDown);
        }

        let launch_timeout = plan.timeout;
        let launch_execution_id = plan.execution_id();
        let final_metadata = match self
            .store
            .update_if_state(
                &sandbox_id,
                std::slice::from_ref(&transitional_state),
                move |metadata| {
                    metadata.resources = runtime_resources;
                    metadata.execution_id = launch_execution_id;
                    metadata.state = SandboxState::Running;
                    metadata.update_timeout(launch_timeout);
                },
            )
            .await
        {
            Ok(update) => update.current,
            Err(err) => {
                warn!(error = %format_args!("{err:#}"), "failed to persist final sandbox metadata after launch");
                self.cleanup_failed_launch(&plan, handle, FailedLaunchStage::TransitionalPersisted)
                    .await;
                return Err(OrchestratorError::from(err));
            }
        };

        let proxy_target = {
            let sandbox = handle.lock().await;
            match Self::proxy_target_from_sandbox(sandbox.as_ref()).map(|target| {
                target.with_traffic_access_token(final_metadata.traffic_access_token.clone())
            }) {
                Ok(proxy_target) => proxy_target,
                Err(err) => {
                    warn!(error = %format_args!("{err:#}"), "sandbox became ready without a proxy target; rolling back launch");
                    drop(sandbox);
                    self.cleanup_failed_launch(&plan, handle, FailedLaunchStage::RunningPersisted)
                        .await;
                    return Err(err);
                }
            }
        };
        if !self
            .upsert_proxy_route_if_current_handle(
                sandbox_id,
                &handle,
                proxy_target,
                launch_execution_id,
            )
            .await
        {
            debug!("skipping runtime proxy route publication because sandbox handle is stale");
        }

        info!("sandbox launch completed");
        Ok(final_metadata)
    }

    fn build_sandbox(&self, plan: &LaunchPlan) -> Result<Box<dyn SandboxBackend>> {
        let execution_id = plan.execution_id();
        let build_result = match &plan.source {
            LaunchSource::Snapshot { snapshot } => {
                self.factory
                    .build_from_snapshot(snapshot, plan.launch_config.clone(), execution_id)
            }
            LaunchSource::SnapshotRecord { record } => self.factory.build_from_snapshot_record(
                record,
                plan.launch_config.clone(),
                execution_id,
            ),
            LaunchSource::Fresh { build_spec } => self.factory.build(
                (**build_spec).clone(),
                plan.launch_config.clone(),
                execution_id,
            ),
            LaunchSource::UnresolvedImage { build_spec } => self.factory.build_from_image_ref(
                (**build_spec).clone(),
                plan.launch_config.clone(),
                execution_id,
            ),
        };
        build_result.map_err(|source| {
            warn!(error = %format_args!("{source:#}"), "failed to build sandbox");
            OrchestratorError::SandboxOperationFailed {
                sandbox_id: plan.sandbox_id,
                operation: SandboxOperation::Build,
                source,
            }
        })
    }

    async fn cleanup_failed_launch(
        &self,
        plan: &LaunchPlan,
        handle: SandboxHandle,
        stage: FailedLaunchStage,
    ) {
        self.revoke_secrets(plan.sandbox_id, plan.execution_id())
            .await;
        let should_rollback_shared_state = self
            .detach_launch_runtime_if_current(
                &plan.sandbox_id,
                &handle,
                stage.should_detach_proxy_route(),
                stage,
            )
            .await;

        // Stop the sandbox.
        let stop_result = {
            let mut sandbox = handle.lock().await;
            sandbox.stop().await
        };
        if let Err(err) = stop_result {
            warn!(error = %format_args!("{err:#}"), "failed to stop sandbox while rolling back launch");
        }

        let Some(expected_state) = stage.rollback_expected_state(plan) else {
            return;
        };

        if should_rollback_shared_state {
            self.rollback_failed_launch_metadata(plan, expected_state)
                .await;
            return;
        }

        self.reclaim_superseded_launch_record(plan, expected_state)
            .await;
    }

    // The binding goes first under the same fence as the record, as in
    // `forget_sandbox`: a successor rebinds this id itself.
    async fn reclaim_superseded_launch_record(
        &self,
        plan: &LaunchPlan,
        expected_state: SandboxState,
    ) {
        let sandbox_id = plan.sandbox_id;
        let execution_id = plan.execution_id();
        self.forget_runtime_routing(sandbox_id, execution_id).await;
        match self
            .store
            .remove_if_execution(
                &sandbox_id,
                execution_id,
                std::slice::from_ref(&expected_state),
            )
            .await
        {
            Ok(FencedRemoval::Removed) => {
                info!(
                    %sandbox_id,
                    %execution_id,
                    state = ?expected_state,
                    "skipped shared state rollback but took this launch's own record back"
                );
            }
            Ok(FencedRemoval::Absent) => {
                debug!(
                    %sandbox_id,
                    %execution_id,
                    "skipped shared state rollback; this launch's record was already gone"
                );
            }
            Ok(FencedRemoval::Superseded {
                state,
                execution_id: actual,
            }) => {
                info!(
                    %sandbox_id,
                    %execution_id,
                    actual_execution_id = %actual,
                    actual_state = ?state,
                    "skipped shared state rollback; the record under this id is another launch's"
                );
            }
            Err(err) => {
                warn!(
                    %sandbox_id,
                    %execution_id,
                    error = %format_args!("{err:#}"),
                    "skipped shared state rollback and could not take this launch's own record back"
                );
            }
        }
    }

    async fn rollback_failed_launch_metadata(
        &self,
        plan: &LaunchPlan,
        expected_state: SandboxState,
    ) {
        self.release_image_refs(RuntimeImageOwner::StartingSandbox(plan.sandbox_id))
            .await;
        // The launch's own record is the only state to take back; a
        // superseded one is handled by `reclaim_superseded_launch_record`.
        let _ = expected_state;
        if let Err(err) = self
            .forget_sandbox(plan.sandbox_id, plan.execution_id())
            .await
        {
            warn!(error = %format_args!("{err:#}"), "failed to remove sandbox metadata during launch rollback");
        }
    }

    async fn detach_launch_runtime_if_current(
        &self,
        sandbox_id: &SandboxId,
        handle: &SandboxHandle,
        detach_proxy_route: bool,
        stage: FailedLaunchStage,
    ) -> bool {
        let mut sandboxes = self.sandboxes.write().await;
        let Some(current_handle) = sandboxes.get(sandbox_id) else {
            return true;
        };

        if !Arc::ptr_eq(current_handle, handle) {
            // Runtime cleanup stops when the handle now belongs to a replacement.
            warn!(
                stage = ?stage,
                "sandbox handle was replaced during failed launch cleanup; \
                 leaving the handle, the proxy route and the starting image pin to the replacement"
            );
            return false;
        }

        sandboxes.remove(sandbox_id);

        if detach_proxy_route {
            let removed_route = self.proxy_routes.write().await.remove(sandbox_id);
            if let Some(route) = removed_route.as_ref() {
                debug!(version = route.version(), "removed runtime proxy route");
            }
        }

        drop(sandboxes);
        true
    }

    fn proxy_target_from_sandbox(sandbox: &dyn SandboxBackend) -> Result<ProxyTarget> {
        sandbox
            .host_interaction_ip()
            .map(ProxyTarget::new)
            .ok_or_else(|| {
                warn!("sandbox started without an interaction IP");
                OrchestratorError::InternalError(
                    "sandbox missing host interaction IP after start".to_string(),
                )
            })
    }

    async fn upsert_proxy_route(
        &self,
        sandbox_id: SandboxId,
        target: ProxyTarget,
        execution_id: ExecutionId,
    ) {
        let version = self
            .next_proxy_route_version
            .fetch_add(1, Ordering::Relaxed);
        let route =
            self.proxy_routes
                .write()
                .await
                .upsert(sandbox_id, target, version, execution_id);
        debug!(
            version = route.version(),
            updated_at = ?route.updated_at(),
            host_interaction_ip = %route.target().ip,
            "updated runtime proxy route"
        );
    }

    async fn upsert_proxy_route_if_current_handle(
        &self,
        sandbox_id: SandboxId,
        handle: &SandboxHandle,
        target: ProxyTarget,
        execution_id: ExecutionId,
    ) -> bool {
        // Keep the lock order aligned with detach_sandbox_handle_and_route:
        // sandboxes first, then proxy_routes.
        let sandboxes = self.sandboxes.write().await;
        let Some(current_handle) = sandboxes.get(&sandbox_id) else {
            return false;
        };

        if !Arc::ptr_eq(current_handle, handle) {
            return false;
        }

        let version = self
            .next_proxy_route_version
            .fetch_add(1, Ordering::Relaxed);
        let route =
            self.proxy_routes
                .write()
                .await
                .upsert(sandbox_id, target, version, execution_id);
        drop(sandboxes);

        debug!(
            version = route.version(),
            updated_at = ?route.updated_at(),
            host_interaction_ip = %route.target().ip,
            "updated runtime proxy route"
        );
        true
    }

    async fn restore_proxy_route(&self, sandbox_id: SandboxId, route: Option<ProxyRoute>) {
        let Some(route) = route else {
            return;
        };
        // Restore the route under its original execution.
        self.upsert_proxy_route(sandbox_id, route.target().clone(), route.execution_id())
            .await;
    }

    async fn detach_sandbox_handle_and_route(
        &self,
        sandbox_id: &SandboxId,
    ) -> (Option<SandboxHandle>, Option<ProxyRoute>) {
        // Keep the lock order aligned with upsert_proxy_route_if_current_handle:
        // sandboxes first, then proxy_routes.
        let mut sandboxes = self.sandboxes.write().await;
        let handle = sandboxes.remove(sandbox_id);

        let removed_route = self.proxy_routes.write().await.remove(sandbox_id);
        if let Some(route) = removed_route.as_ref() {
            debug!(version = route.version(), "removed runtime proxy route");
        }

        drop(sandboxes);
        (handle, removed_route)
    }

    /// Logs and counts cached handles discarded after execution supersession.
    fn note_stale_handle_discarded(
        &self,
        sandbox_id: SandboxId,
        stale_execution_id: ExecutionId,
        current_execution_id: ExecutionId,
    ) {
        warn!(
            %sandbox_id,
            %stale_execution_id,
            %current_execution_id,
            "discarding a cached sandbox handle whose execution has been superseded; a \
             pause+resume this replica never observed must have moved the sandbox on. \
             Rebuilding the handle from the authoritative record instead of fencing \
             against the stale execution."
        );
        self.counters.record_stale_handle_discarded();
    }

    /// Returns a cached handle only when it matches the authoritative execution.
    /// Stale entries and matching routes are discarded before returning `None`.
    async fn cached_handle_for_execution(
        &self,
        sandbox_id: SandboxId,
        expected_execution_id: ExecutionId,
    ) -> Option<SandboxHandle> {
        let cached = {
            let sandboxes = self.sandboxes.read().await;
            sandboxes.get(&sandbox_id).cloned()
        };
        let handle = cached?;
        let actual_execution_id = handle.lock().await.execution_id();
        if actual_execution_id == expected_execution_id {
            return Some(handle);
        }

        self.note_stale_handle_discarded(sandbox_id, actual_execution_id, expected_execution_id);

        {
            let mut sandboxes = self.sandboxes.write().await;
            if sandboxes
                .get(&sandbox_id)
                .is_some_and(|current| Arc::ptr_eq(current, &handle))
            {
                sandboxes.remove(&sandbox_id);
            }
        }
        {
            let mut routes = self.proxy_routes.write().await;
            if routes
                .route(&sandbox_id)
                .is_some_and(|route| route.execution_id() == actual_execution_id)
            {
                routes.remove(&sandbox_id);
            }
        }

        None
    }

    /// Detaches a handle and route, discarding a handle stale against the expected execution.
    async fn detach_sandbox_handle_and_route_checked(
        &self,
        sandbox_id: &SandboxId,
        expected_execution_id: ExecutionId,
    ) -> (Option<SandboxHandle>, Option<ProxyRoute>) {
        let (handle, removed_route) = self.detach_sandbox_handle_and_route(sandbox_id).await;
        let Some(handle) = handle else {
            return (None, removed_route);
        };

        let actual_execution_id = handle.lock().await.execution_id();
        if actual_execution_id == expected_execution_id {
            return (Some(handle), removed_route);
        }

        self.note_stale_handle_discarded(*sandbox_id, actual_execution_id, expected_execution_id);

        let removed_route = match removed_route {
            Some(route) if route.execution_id() == actual_execution_id => None,
            other => other,
        };
        (None, removed_route)
    }

    async fn run_shutdown_cleanup(self: &Arc<Self>) -> Result<()> {
        const MAX_SHUTDOWN_PASSES: usize = 3;
        let mut last_failures = Vec::new();

        // Remote sandboxes outlive this process and are not paused during shutdown.
        if self.factory.sandboxes_outlive_this_process() {
            info!(
                "this process runs no sandboxes of its own; leaving the recorded sandboxes to \
                 the machines running them"
            );
            return Ok(());
        }

        // Nothing on this machine outlives the process: a VM left running would
        // be an orphan no record names, and a pause staged here could not be
        // committed by a process that is exiting. Stop every sandbox.
        for pass in 1..=MAX_SHUTDOWN_PASSES {
            let sandboxes = self.store.list().await?;
            if sandboxes.is_empty() {
                break;
            }
            last_failures.clear();

            info!(
                pass,
                remaining = sandboxes.len(),
                "stopping sandboxes during shutdown"
            );

            for metadata in sandboxes {
                let sandbox_id = metadata.id;
                match metadata.state {
                    SandboxState::Running => {
                        if let Err(err) = self.delete_sandbox_inner(sandbox_id).await {
                            last_failures.push(format!("{sandbox_id}: {err}"));
                        }
                    }
                    SandboxState::Creating
                    | SandboxState::Snapshotting
                    | SandboxState::Forking
                    | SandboxState::Pausing
                    | SandboxState::Killing => {
                        match self.wait_for_transition(sandbox_id, metadata.state).await {
                            Ok(_) | Err(OrchestratorError::SandboxNotFound(_)) => {}
                            Err(err) => {
                                warn!(
                                    sandbox_id = %sandbox_id,
                                    error = ?err,
                                    pass,
                                    "failed to wait for sandbox transition during orchestrator shutdown"
                                );
                                last_failures.push(format!("{sandbox_id}: {err}"));
                            }
                        }
                    }
                }
            }

            if last_failures.is_empty() {
                continue;
            }

            warn!(
                pass,
                failures = last_failures.len(),
                max_passes = MAX_SHUTDOWN_PASSES,
                "shutdown pass completed with failures"
            );
        }

        if !last_failures.is_empty() {
            return Err(OrchestratorError::InternalError(format!(
                "failed to stop all sandboxes during shutdown after {MAX_SHUTDOWN_PASSES} passes: {}",
                last_failures.join(", ")
            )));
        }

        // The backend factory releases its process-wide resources.
        self.factory.release_process_wide_resources();

        info!("orchestrator shutdown completed");
        Ok(())
    }

    fn is_shutting_down(&self) -> bool {
        self.is_shutting_down.load(Ordering::Acquire)
    }

    fn ensure_accepting_lifecycle_operations(&self) -> Result<()> {
        if self.is_shutting_down() {
            info!("rejecting lifecycle operation because orchestrator is shutting down");
            return Err(OrchestratorError::ShuttingDown);
        }

        Ok(())
    }

    /// Whether this node refuses new placements.
    pub fn scheduling_disabled(&self) -> bool {
        self.scheduling_disabled.load(Ordering::Acquire)
    }

    /// Changes isolation and reports whether the value changed.
    pub fn set_scheduling_disabled(&self, disabled: bool) -> bool {
        if self.scheduling_disabled.swap(disabled, Ordering::AcqRel) == disabled {
            return false;
        }

        let now_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|since| since.as_millis() as i64)
            .unwrap_or_default();
        self.scheduling_disabled_changed_at_ms
            .store(now_ms, Ordering::Release);
        info!(
            scheduling_disabled = disabled,
            "node scheduling availability changed"
        );

        true
    }

    /// Last isolation change, or `None`.
    pub fn scheduling_disabled_changed_at_ms(&self) -> Option<i64> {
        match self
            .scheduling_disabled_changed_at_ms
            .load(Ordering::Acquire)
        {
            0 => None,
            ms => Some(ms),
        }
    }

    /// Guards only paths that place new sandboxes on this node.
    fn ensure_accepting_new_work(&self) -> Result<()> {
        self.ensure_accepting_lifecycle_operations()?;

        if self.scheduling_disabled() {
            info!("rejecting new work because the node is isolated");
            return Err(OrchestratorError::NotAcceptingNewWork);
        }

        Ok(())
    }
}

#[cfg(any(test, feature = "test-support"))]
impl<S, F> Orchestrator<S, F>
where
    S: MetadataStore + 'static,
    F: SandboxBackendFactory,
{
    pub async fn set_proxy_target_for_test(
        &self,
        sandbox_id: SandboxId,
        target: ProxyTarget,
        state: SandboxState,
    ) {
        self.set_metadata_state_for_test(sandbox_id, state)
            .await
            .expect("seed proxy metadata state for test");

        if state == SandboxState::Running {
            let execution_id = self
                .store
                .get(&sandbox_id)
                .await
                .ok()
                .flatten()
                .map(|metadata| metadata.execution_id)
                .unwrap_or_else(ExecutionId::new);
            self.upsert_proxy_route(sandbox_id, target, execution_id)
                .await;
        } else {
            let _ = self.proxy_routes.write().await.remove(&sandbox_id);
        }
    }

    /// Forgets a record outright, the way a finished pause does.
    pub async fn remove_sandbox_for_test(&self, sandbox_id: &SandboxId) -> Result<()> {
        self.store.remove(sandbox_id).await?;
        Ok(())
    }

    pub async fn set_metadata_state_for_test(
        &self,
        sandbox_id: SandboxId,
        state: SandboxState,
    ) -> Result<()> {
        let existing = self.store.get(&sandbox_id).await?;
        match existing {
            Some(mut metadata) => {
                metadata.state = state;
                self.store.update(metadata).await?;
            }
            None => {
                let metadata = SandboxMetadata {
                    id: sandbox_id,
                    state,
                    ..Default::default()
                };
                self.store.add(metadata.clone()).await?;
            }
        }

        Ok(())
    }

    pub async fn set_auto_resume_for_test(
        &self,
        sandbox_id: &SandboxId,
        auto_resume_enabled: bool,
    ) -> Result<()> {
        let Some(mut metadata) = self.store.get(sandbox_id).await? else {
            return Err(OrchestratorError::SandboxNotFound(*sandbox_id));
        };

        metadata.auto_resume = auto_resume_enabled;
        self.store.update(metadata).await?;

        Ok(())
    }

    /// Test helper that drops a process-local handle without removing its record.
    pub async fn forget_sandbox_handle_for_test(&self, sandbox_id: &SandboxId) -> bool {
        self.sandboxes.write().await.remove(sandbox_id).is_some()
    }

    pub async fn remove_proxy_route_for_test(&self, sandbox_id: &SandboxId) {
        let _ = self.proxy_routes.write().await.remove(sandbox_id);
    }

    /// Returns the incarnation built into the live backend.
    pub async fn backend_execution_id_for_test(
        &self,
        sandbox_id: &SandboxId,
    ) -> Option<ExecutionId> {
        let handle = self.sandboxes.read().await.get(sandbox_id).cloned()?;
        let backend = handle.lock().await;
        Some(backend.execution_id())
    }

    /// Seeds a live backend with a chosen execution for ordered-comparison tests.
    pub async fn set_live_execution_for_test(
        &self,
        sandbox_id: SandboxId,
        target: ProxyTarget,
        execution_id: ExecutionId,
    ) {
        self.set_metadata_state_for_test(sandbox_id, SandboxState::Running)
            .await
            .expect("seed running metadata for test");
        if let Ok(Some(mut metadata)) = self.store.get(&sandbox_id).await {
            metadata.execution_id = execution_id;
            let _ = self.store.update(metadata).await;
        }
        self.upsert_proxy_route(sandbox_id, target, execution_id)
            .await;
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
