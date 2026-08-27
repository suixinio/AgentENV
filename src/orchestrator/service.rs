use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicI64, AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, SystemTime};

use anyhow::Context;
use tokio::sync::{broadcast, oneshot, watch, Mutex, OnceCell, RwLock};
use tokio::time::MissedTickBehavior;
use tracing::{debug, info, trace, warn};

use crate::cfg::ConfigManager;
use crate::image::{RuntimeImageOwner, RuntimeImageRefs};
use crate::sandbox::{
    AccessTokenSeedPolicy, CustomExtensionClient, CustomExtensionParams, EnvdAccessToken,
    FreshSandboxBuildSpec, PausedSandboxCapture, PausedSandboxState, RuntimeArtifactSet,
    RuntimeConfirmedGone, SandboxAccessTokenGenerator, SandboxBackend, SandboxBackendFactory,
    SandboxForkSpec, SandboxLaunchConfig, SandboxNetworkPolicy, SandboxRuntimeInfo,
    UnresolvedImageBuildSpec,
};
use crate::snapshot::SnapshotRuntimeVersions;
use crate::types::{bytes_to_mib_ceil, ExecutionId, SandboxId, SandboxResources};

use super::launch_plan::{ClaimedExecution, CreateLaunchSource, LaunchPlan};
use super::metrics::{
    aggregate_resource_metrics, OrchestratorCounters, OrchestratorMetrics, SandboxContribution,
};
use super::paused_registry::PausedSandboxPublisher;
use super::persistence::ClusterRegistration;
use super::persistence::{DisabledSandboxPersister, FileBackedSandboxPersister, SandboxPersister};
use super::proxy::{ProxyLookupResult, ProxyRoute, ProxyRouteTable, ProxyTarget};
use super::store::*;
use super::types::{
    CreateSandboxRequest, ForkChildAssignment, ForkChildren, LiveSandbox, PauseOutcome,
    PausePublication, SandboxExpiry, SandboxLaunchSource, SandboxLifecycleEvent,
    SandboxLifecycleEventType, SandboxRosterEntry, SandboxState, SnapshotCaptureResult,
};
use super::{OrchestratorError, Result, SandboxForkOutcome, SandboxOperation};

type SandboxHandle = Arc<Mutex<Box<dyn SandboxBackend>>>;

/// Maximum time to wait for a sandbox to leave a transitional state.
/// Guards against indefinite blocking when a sandbox's in-progress operation
/// never completes (e.g. the task holding the state panics without rolling back).
const WAIT_TRANSITION_TIMEOUT: Duration = Duration::from_secs(60);
const SANDBOX_EVENT_CHANNEL_CAPACITY: usize = 1024;

/// How many expired sandboxes [`Orchestrator::evict_expired_sandboxes`] asks
/// its store for per round.
///
/// 🔴 Before this existed, `evict_expired_sandboxes` called
/// `MetadataStore::list_expired`, which on the Redis backend
/// (`RedisMetadataStore::list_expired`) requests `expired_batch(now,
/// usize::MAX)` — and `expiry::expired_batch`'s own `count = -1` special
/// case for `usize::MAX` means that was an *unbounded* `ZRANGEBYSCORE
/// -inf <now> LIMIT 0 -1` against the whole expiry index, once per
/// `auto_evict_interval_ms` (1s default) tick, per replica. This is the
/// `expired_batch`-with-a-real-limit call the trait's own doc comment
/// already says an evictor needs ("a fixed amount of work per round
/// instead of pulling the whole table") — `evict_expired_sandboxes` just
/// was not the caller using it.
///
/// The value matches `RedisStoreConfig::expired_batch_limit`'s own default
/// (256): on the Redis backend, `expired_batch` already clamps any
/// non-`usize::MAX` limit to `min(limit, config.expired_batch_limit)`, so
/// passing a larger number here would silently be capped there anyway;
/// matching it makes this the effective bound on every backend, including
/// `InMemoryMetadataStore`, which has no such clamp of its own.
///
/// # Why a round that does not finish the backlog is still safe
///
/// `ZRANGEBYSCORE ... LIMIT 0 N` returns the `N` *lowest-scoring* (i.e.
/// oldest-overdue) members of the range, not an arbitrary `N`. Every
/// successful write that changes a record's expiry-relevant state
/// (`scripts::UPDATE`'s `rescore_flag` arm, driven by `crud.rs`) issues a
/// paired `ZREM`/`ZADD` against the same index in the same script — so a
/// sandbox this round evicts is not returned by the *next* round's query,
/// and the next-oldest `N` take its place. A round capped at
/// `AUTO_EVICT_BATCH_LIMIT` is therefore not "the rest gets dropped": it is
/// "the rest is still the oldest-first queue this exact query will ask
/// about again one second from now" (`auto_evict_interval_ms`). The one
/// case that does *not* advance is a sandbox whose eviction attempt itself
/// fails (`evict_expired_sandboxes`'s own `warn!` + `continue`) — its score
/// is untouched, so it keeps sorting first and keeps being retried every
/// round, exactly like the healer/reaper backstops elsewhere in this store
/// retry a stuck record — but it can only ever occupy its own one slot in
/// a batch, never the other `AUTO_EVICT_BATCH_LIMIT - 1`, so a
/// persistently-failing sandbox cannot starve the rest of the backlog.
const AUTO_EVICT_BATCH_LIMIT: usize = 256;

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

pub struct Orchestrator<S: MetadataStore, F: SandboxBackendFactory, P: SandboxPersister> {
    store: S,
    factory: F,
    persister: P,
    sandboxes: RwLock<HashMap<SandboxId, SandboxHandle>>,
    proxy_routes: RwLock<ProxyRouteTable>,
    next_proxy_route_version: AtomicU64,
    counters: OrchestratorCounters,
    sandbox_event_tx: broadcast::Sender<SandboxLifecycleEvent>,
    default_sandbox_timeout: Duration,
    is_shutting_down: std::sync::atomic::AtomicBool,
    /// Node-level isolation. While set, this node refuses work that would put a
    /// *new* sandbox on it and reports itself draining in its heartbeat, but
    /// keeps serving everything it already holds: isolation is about what
    /// arrives next, not about what is already here.
    ///
    /// Deliberately separate from `is_shutting_down`. A shutting-down node is
    /// on its way out and refuses lifecycle work outright; an isolated node is
    /// perfectly healthy and may be un-isolated again.
    scheduling_disabled: std::sync::atomic::AtomicBool,
    /// When isolation last changed, in unix milliseconds; zero means never.
    /// Reported by the admin API so an operator can see how long a node has
    /// been out of rotation.
    scheduling_disabled_changed_at_ms: AtomicI64,
    shutdown_tx: watch::Sender<bool>,
    shutdown_outcome: OnceCell<ShutdownOutcome>,
    pub image_refs: Arc<dyn RuntimeImageRefs>,
    access_tokens: SandboxAccessTokenGenerator,
    /// Cluster-wide bookkeeping for paused sandboxes, wired in after
    /// construction because it is built from the snapshot repository, which the
    /// orchestrator otherwise has no reason to know about. Unset means every
    /// pause stays node-local, which is the default.
    paused_publisher: OnceCell<Arc<dyn PausedSandboxPublisher>>,
}

/// What can be driven for a sandbox this process is holding no handle for.
///
/// # 🔴 Three answers, because two of them used to be told apart by nothing
///
/// [`Orchestrator::sandboxes`] is a *process-local* map of live backends. For a
/// single process, an id missing from it means the sandbox's runtime is gone,
/// and the pause and snapshot paths acted on exactly that reading: they deleted
/// the record. `aenv-api` is deployed as several replicas behind a Service
/// with no session affinity, and there the same absence usually means something
/// else entirely — *another replica started it* — because a sandbox's record is
/// shared and its handle is not.
///
/// Read as the first, the second destroys the sandbox: the record goes, the VM
/// stays up on its node, and nothing left in the cluster can name it. So the
/// two are separate values here, and the caller has to say which one it is
/// acting on.
///
/// 🔴 And a fourth answer that is not a variant: `Err`. "I could not reach the
/// store" and "I could not reach the machine" are neither of the three, and a
/// caller that folded either into [`RuntimeGone`](Self::RuntimeGone) would be
/// deleting records because a network was slow.
enum AbsentHandle {
    /// The sandbox runs on a machine this half can address, and the backend to
    /// drive it with has been rebuilt from the record. Usable exactly like the
    /// handle that was not here.
    Adopted(SandboxHandle),
    /// The runtime the record describes is gone, on either of two grounds
    /// this factory can establish on its own: this factory's sandboxes live
    /// in the process that started them, and there is none here; or its
    /// sandboxes live elsewhere and the placement source has independently
    /// confirmed — via [`RuntimeConfirmedGone`] — that the machine they
    /// depended on has left the cluster. This is the only answer that
    /// entitles a caller to clean the record up.
    RuntimeGone,
    /// The store has no record under this id: the sandbox does not exist. There
    /// is nothing to drive and nothing to clean up.
    NoRecord,
}

/// What a teardown should do with the sandbox's cluster-wide record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ClusterDisposition {
    /// The sandbox is gone for good: clear its row and the snapshot behind it.
    Forget,
    /// Only this node's copy is going: the row describes a sandbox that is
    /// alive elsewhere, and the snapshot it names is that sandbox's recovery
    /// point.
    KeepClusterRecord,
}

impl<F> Orchestrator<InMemoryMetadataStore, F, DisabledSandboxPersister>
where
    F: SandboxBackendFactory,
{
    /// A throwaway orchestrator for tests and examples.
    ///
    /// Fixed at [`AccessTokenSeedPolicy::MayGenerate`] rather than taking one,
    /// because that is what it is for: no configured envd access-token seed
    /// required of whoever calls it.
    ///
    /// 🔴 The factory is an argument. It used to be fixed at
    /// `FirecrackerSandboxFactory`, which made the one convenience constructor
    /// in this module the reason the orchestrator named a sandbox runtime at
    /// all.
    pub async fn with_in_memory_store(factory: F) -> Arc<Self> {
        Self::new(
            AccessTokenSeedPolicy::MayGenerate,
            InMemoryMetadataStore::new(),
            factory,
            DisabledSandboxPersister,
            crate::image::DisabledRuntimeImageRefs::shared(),
        )
        .await
        .expect("in-memory orchestrator should never fail to initialize")
    }
}

impl<F> Orchestrator<InMemoryMetadataStore, F, FileBackedSandboxPersister>
where
    F: SandboxBackendFactory,
{
    /// 🔴 No seed policy argument: `aenv-node` is the only caller and a node
    /// generates its own seed under `$AENV_HOME/secrets/`. A replicated
    /// process, which may not, does not build a file-backed persister at all —
    /// see `assemble_api`'s own note on `DisabledSandboxPersister`.
    pub async fn with_file_backed_store_and_factory(
        factory: F,
        image_refs: Arc<dyn RuntimeImageRefs>,
    ) -> Result<Arc<Self>> {
        let config = ConfigManager::global_config();
        let store = InMemoryMetadataStore::new();
        let persister = FileBackedSandboxPersister::new(
            config.orchestrator.persisted_sandbox_store_path.clone(),
            config.virtualization_mode,
        );
        Self::new(
            AccessTokenSeedPolicy::MayGenerate,
            store,
            factory,
            persister,
            image_refs,
        )
        .await
    }
}

impl<S, F, P> Orchestrator<S, F, P>
where
    S: MetadataStore + 'static,
    F: SandboxBackendFactory,
    P: SandboxPersister + 'static,
{
    /// An orchestrator with no background tasks, for a test.
    ///
    /// 🔴 Not [`Orchestrator::new`]: that starts the eviction and maintenance
    /// loops and restores persisted sandboxes. Tests drive the transitions
    /// themselves. Behind `feature = "test-support"` so `aenv-node`'s own
    /// suite can build one too — the struct's fields are this module's
    /// business, not something a sibling crate should be spelling out.
    #[cfg(any(test, feature = "test-support"))]
    pub fn from_test_parts(
        store: S,
        factory: F,
        persister: P,
        default_sandbox_timeout: std::time::Duration,
        image_refs: std::sync::Arc<dyn crate::image::RuntimeImageRefs>,
        access_token_seed: &str,
    ) -> Self {
        let (sandbox_event_tx, _sandbox_event_rx) =
            tokio::sync::broadcast::channel(SANDBOX_EVENT_CHANNEL_CAPACITY);
        Self {
            store,
            factory,
            persister,
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
            paused_publisher: tokio::sync::OnceCell::new(),
        }
    }

    /// Builds the orchestrator this process will run.
    ///
    /// `seed_policy` is carried no further than construction: the only thing it
    /// decides is whether this process may invent its own envd access-token
    /// seed when none is configured. A replicated half may not — see
    /// [`AccessTokenSeedPolicy`].
    ///
    /// # 🔴 `image_refs` is an argument and not a default
    ///
    /// It used to be read here from the node-local layer cache
    /// (`local_image_services_from_global_config`), which meant every process
    /// holding an orchestrator opened that cache — including one with no layers
    /// on its disk to protect. Whether this machine has a layer cache is a fact
    /// about the machine, not about the orchestrator, so the caller states it:
    /// a node passes its cache's own handle, and a process that runs no
    /// sandboxes passes [`DisabledRuntimeImageRefs`][crate::image::DisabledRuntimeImageRefs].
    pub async fn new(
        seed_policy: AccessTokenSeedPolicy,
        store: S,
        factory: F,
        persister: P,
        image_refs: Arc<dyn RuntimeImageRefs>,
    ) -> Result<Arc<Self>> {
        let app_config = ConfigManager::global_config();
        let config = &app_config.orchestrator;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (sandbox_event_tx, _sandbox_event_rx) =
            broadcast::channel(SANDBOX_EVENT_CHANNEL_CAPACITY);

        // Restore persisted sandboxes from the previous run, keeping the paused
        // ones (with their state) for the paused-protection reconcile below.
        let persisted = persister.load_all(&factory).await?;
        let managed_seed_must_exist = persisted.iter().any(|metadata| metadata.secure);
        let access_tokens = tokio::task::spawn_blocking(move || {
            SandboxAccessTokenGenerator::load_or_create(
                app_config,
                seed_policy,
                managed_seed_must_exist,
            )
        })
        .await
        .context("join envd access-token seed loader")??;
        let restored_paused: Vec<(SandboxId, Arc<dyn PausedSandboxState>)> = persisted
            .iter()
            .filter(|metadata| metadata.state == SandboxState::Paused)
            .filter_map(|metadata| {
                metadata
                    .paused_state
                    .as_ref()
                    .map(|paused_state| (metadata.id, Arc::clone(paused_state)))
            })
            .collect();
        for metadata in persisted {
            store.add(metadata).await?;
        }

        let orchestrator = Arc::new(Self {
            store,
            factory,
            persister,
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
            paused_publisher: OnceCell::new(),
        });

        // Start the auto-evict task.
        let evict_interval = Duration::from_millis(config.auto_evict_interval_ms);
        Self::start_auto_evict_task(Arc::clone(&orchestrator), evict_interval, shutdown_rx);

        // Reconcile durable paused protection, then start maintenance (fail-closed).
        let gc = app_config.image.cache.gc_schedule();
        if gc.enabled {
            match orchestrator
                .reconcile_paused_at_startup(&restored_paused)
                .await
            {
                Ok(()) => {
                    Self::start_local_image_maintenance_task(
                        Arc::clone(&orchestrator),
                        gc.interval,
                        orchestrator.shutdown_tx.subscribe(),
                    );
                }
                Err(error) => {
                    warn!(
                        error = %error,
                        "local image protection reconcile failed at startup; not starting maintenance (fail-closed)"
                    );
                }
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

    /// Works out what can be driven for a sandbox whose handle is not here.
    ///
    /// Called only after the process-local map has come up empty; the three
    /// answers and the error are described on [`AbsentHandle`].
    ///
    /// 🔴 The adopted backend is started before it is handed back, and `start`
    /// on it starts nothing: it is where a stub for an already-running sandbox
    /// finds the machine it is on and opens a channel to it. Handing back an
    /// unattached backend would move that round trip inside the caller's
    /// rollback-shaped code, where its failure is much easier to mistake for
    /// the operation's own.
    async fn absent_handle(&self, sandbox_id: SandboxId) -> Result<AbsentHandle> {
        // 🔴 The store first, and its error propagates. A caller that could not
        // read the record has learned nothing about the sandbox, and every
        // answer below is a claim about a record that was read.
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
            // 🔴 Downcast before the message below stringifies it away.
            // `start` is shared by every backend, local and remote, and only a
            // remote stub's `attach` can ever tag a failure this way — see
            // `RuntimeConfirmedGone`'s doc. It means the placement source has
            // independently confirmed the node this sandbox depended on is no
            // longer part of the cluster, which is the one case a caller here
            // may treat as more than "could not reach it this time".
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

    /// Fail-closed startup reconcile before maintenance can run: durably protect
    /// every restored paused sandbox, then drop orphaned paused protection.
    async fn reconcile_paused_at_startup(
        &self,
        restored_paused: &[(SandboxId, Arc<dyn PausedSandboxState>)],
    ) -> Result<()> {
        let mut live_paused = Vec::with_capacity(restored_paused.len());
        for (sandbox_id, paused_state) in restored_paused {
            self.protect_image_refs(
                RuntimeImageOwner::PausedSandbox(*sandbox_id),
                paused_state.runtime_artifacts(),
                "paused sandbox",
            )
            .await?;
            live_paused.push(*sandbox_id);
        }
        self.image_refs
            .reconcile_paused(&live_paused)
            .await
            .map_err(|error| {
                OrchestratorError::InternalError(format!(
                    "reconcile local image protection: {error:#}"
                ))
            })
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

    /// Rebuilds a sandbox from a published snapshot under an ID it already had.
    ///
    /// This is the cross-node half of resume. The sandbox was paused on another
    /// node and its snapshot committed to the shared repository; this node
    /// brings it back under the *same* ID, so clients keep addressing it exactly
    /// as before and the envd access token — derived from the sandbox ID —
    /// stays valid.
    ///
    /// The caller owns the decision that this node may take the sandbox over.
    /// Nothing here checks whether another node still holds it, so this must
    /// only be called after winning a claim in the paused-sandbox registry.
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

    /// What a create's [`SandboxExpiry`] means to the record this orchestrator
    /// is about to write.
    ///
    /// 🔴 The one place the configured default is reached for on a create, and
    /// the one place [`SandboxExpiry::NotKeptHere`] becomes
    /// [`NewTimeout::None`]. A record with no `expires_at` is never in the
    /// expiry index and so is never seen by
    /// [`evict_expired_sandboxes`](Self::evict_expired_sandboxes) — which is
    /// the whole of what "the caller keeps this deadline" buys, and the reason
    /// this may not fall back to the default for any reason at all.
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
            control_plane_config,
            execution_id,
        } = request;
        let envd_access_token = secure.then(|| self.access_tokens.generate(sandbox_id));
        // 🔴 The three-answer value, not a duration. What this line used to log
        // was `timeout=None` for both "the caller named none" and "the caller
        // keeps this one's deadline", which is exactly the pair that has to be
        // told apart — and a split cluster's create is logged twice, once per
        // half, so this is where the disagreement is visible or nowhere.
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
                        control_plane_config,
                    },
                ) {
                    Ok(parts) => parts,
                    Err(err) => {
                        self.counters.record_create_fail(1);
                        return Err(err);
                    }
                };

                self.launch_sandbox(LaunchPlan::for_create_from_snapshot(
                    sandbox_id,
                    snapshot,
                    launch_config,
                    transitional_metadata,
                    new_timeout,
                    execution_id,
                ))
                .await
            }
            // 🔴 The same record, the same launch config and the same
            // transitional metadata as the arm above — built by the same
            // function, from the same catalog row — with the one difference
            // that nothing here has resolved that row into local bytes. See
            // `SandboxLaunchSource::SnapshotRecord`.
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
                        control_plane_config,
                    },
                ) {
                    Ok(parts) => parts,
                    Err(err) => {
                        self.counters.record_create_fail(1);
                        return Err(err);
                    }
                };

                self.launch_sandbox(LaunchPlan::for_create_from_snapshot_record(
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
                    control_plane_config: None,
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

                self.launch_sandbox(LaunchPlan::for_create_fresh(
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
                    // 🔴 Empty, and not a placeholder to fill in: this field is
                    // read only by a factory that boots a local Firecracker VM
                    // directly from it, and the only factory that ever builds
                    // from this launch source (`RemoteSandboxBackendFactory`)
                    // does not — it ships sandbox_id/policy/etc. over the wire
                    // and the node builds its own launch config from what it
                    // resolves. See `SandboxBackendFactory::build_from_image_ref`.
                    extra_mmds: serde_json::Map::new(),
                    custom_extension_params: custom_extension_params.clone(),
                    envd_access_token,
                    control_plane_config: None,
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
                    // 🔴 `context` and `image_configs` are left at their
                    // `Default` (empty) here rather than guessed at: this
                    // process cannot resolve the image and so does not know
                    // them yet. `launch_sandbox` overwrites both with the
                    // node's answer once `start_nowait` returns
                    // (`SandboxRuntimeInfo::resolved_image_facts`); if that
                    // never arrives — an older node, or a launch that fails
                    // first — the record keeps these placeholders rather than
                    // anything invented.
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

                self.launch_sandbox(LaunchPlan::for_create_unresolved_image(
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

        // The CAS runs before the handle is resolved, not after: it is what
        // makes `source_metadata.execution_id` below authoritative rather
        // than a snapshot that a concurrent pause+resume could invalidate out
        // from under this call. Nothing can move the execution id while
        // `Forking` holds — the only op that does is a resume, and a resume
        // requires `Paused`, not `Forking` — so any handle checked against it
        // from here on is checked against the truth for the whole rest of
        // this operation.
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

        // 🔴 A replica that did not start the source sandbox is not a replica
        // the source sandbox is missing from. Answering `SandboxNotFound` here
        // made a fork fail on whichever of the api replicas the request
        // happened to reach. Nor is a replica whose cached handle a
        // pause+resume elsewhere has since superseded: `cached_handle_for_execution`
        // discards that one exactly as if it had never been here, so it also
        // falls through to `absent_handle` below.
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

        // 🔴 One entry per child, and the ownership marker is carried alongside
        // the identity rather than derived from the source: the clone of the
        // parent's metadata below would otherwise hand every child the
        // parent's own record.
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
                // A fork child is a brand-new sandbox, so it is a brand-new
                // incarnation. Minted here, next to the child's metadata below
                // being a clone of the parent's, because that clone is what
                // would otherwise carry the parent's — unless the caller has
                // already minted one and recorded it.
                execution_id: child.execution_id.unwrap_or_else(ExecutionId::new),
                envd_access_token: source_metadata
                    .secure
                    .then(|| self.access_tokens.generate(child.sandbox_id)),
            })
            .collect::<Vec<_>>();

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
                if err.is_terminal() {
                    self.detach_sandbox_handle_and_route(&source_sandbox_id)
                        .await;
                    let _ = {
                        let mut sandbox = source_handle.lock().await;
                        sandbox.stop().await
                    };
                    self.store.remove(&source_sandbox_id).await?;
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
                    outcomes.push(Err(Self::fork_child_error(sandbox_id, err)));
                    continue;
                }
            };

            let mut metadata = source_metadata.clone();
            metadata.id = sandbox_id;
            // 🔴 The clone above carries the parent's incarnation. Leaving it
            // would give two live VMs one identity, with nothing to warn about
            // it: fencing would read them as the same run and refuse neither.
            metadata.execution_id = spec.execution_id;
            // 🔴 And the ownership marker, for the same reason one line up: the
            // clone carries the marker that names the *parent*, and a child
            // reporting itself under it would be a second sandbox answering to
            // one identity. `Fresh` clears it, which is the fail-closed
            // direction — a child nobody claims is left alone.
            metadata.control_plane_config = child.control_plane_config;
            metadata.state = SandboxState::Running;
            metadata.created_at = now;
            // 🔴 And the lifetime clock with it. The clone above carries the
            // parent's spent budget; leaving it would hand a child forked from
            // a sandbox that has been running for 23 hours a one-hour life,
            // which is neither what `created_at = now` above says nor what a
            // fresh sandbox on this node would get.
            metadata.restart_lifetime_clock(now);
            metadata.paused_state = None;
            metadata.update_timeout(new_timeout);

            let proxy_target = match Self::proxy_target_from_sandbox(backend.as_ref()) {
                Ok(proxy_target) => proxy_target,
                Err(err) => {
                    Self::stop_failed_fork(backend, sandbox_id).await;
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

    /// Lists every sandbox this node tracks together with the incarnation it is
    /// running under and the TTL its routing projection should carry.
    ///
    /// The same set [`list_sandbox_ids`](Self::list_sandbox_ids) reports, which
    /// is deliberate: the heartbeat sends both, and a receiver that finds them
    /// describing different sets could not tell which one to believe.
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
                // The repair path for a projection write that was lost: it has
                // to reinstall the record with the sandbox's real remaining
                // budget, not with the receiver's default, or a single dropped
                // write silently downgrades that sandbox's routing record for
                // good.
                projection_ttl_secs: metadata.projection_ttl_secs(now),
                // 🔴 `Paused` exactly, not "anything that is not running".
                // The flag's only job is to keep a *routing projection* off a
                // sandbox that has no VM behind it, and the transitional
                // states each already own their routing: `Resuming` is on its
                // way to having a VM and its resume path writes the binding
                // it will need, `Pausing` still has one attached. Widening
                // this to the transitional states would drop and reinstall a
                // projection on every pause/resume round trip for no gain,
                // and would race the resume path's own write.
                paused: metadata.state == SandboxState::Paused,
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

    /// The sandboxes this node is actually running.
    ///
    /// # 🔴 Membership comes from the handles, attributes come from the records
    ///
    /// [`list_sandboxes`][Self::list_sandboxes] answers from the metadata
    /// store, which is this node's *account* of what it holds. This one answers
    /// from the table of live sandbox handles, which is what it holds. Anything
    /// reconciling a cluster's idea of a sandbox against the machine running it
    /// needs the second: comparing an account against an account cannot find a
    /// discrepancy between them.
    ///
    /// # 🔴 Never waits on a busy handle
    ///
    /// Reading the live facts needs the handle's lock, and that lock is held
    /// for the whole of a pause, a fork or a teardown. Waiting for it would
    /// make this call as slow as the slowest operation on the node, and the
    /// caller is deciding whether sandboxes still exist — so a busy handle
    /// contributes its membership and whatever its record knows, marked
    /// [`LiveSandbox::facts_from_handle`] `false`. What it must never do is
    /// drop the sandbox from the list: "not here" is the answer that gets a
    /// live VM torn down.
    pub async fn list_live_sandboxes(&self) -> Result<Vec<LiveSandbox>> {
        // Snapshot the handle table and let go of its lock before touching the
        // store: the store may be a network round trip away, and holding the
        // table's read lock across one blocks every create and teardown on the
        // node.
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
        // 🔴 A read that fails is an error, not an empty set of attributes. A
        // caller told "these sandboxes have no records" would conclude
        // something very different from what "I could not read the records"
        // means.
        let records = self.store.get_many(&ids).await?;
        // 🔴 And a read that only *partly* worked is the same failure wearing a
        // success. A batch that skipped some ids returns rows for the rest, and
        // every id it skipped then looks exactly like a sandbox with no record
        // — which is how a live, claimed sandbox drops out of the answer the
        // cluster reconciles against and gets treated as one that has gone.
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
                // 🔴 The handle's incarnation wins over the record's. The
                // record says which run this node filed; the handle is the run
                // that is up, and that is the one an orphan check compares.
                entry.execution_id = Some(sandbox.execution_id());
                entry.host_interaction_ip = sandbox.host_interaction_ip();
                entry.rootfs_virtual_size = sandbox.runtime_info().rootfs_virtual_size;
            }

            live.push(entry);
        }

        Ok(live)
    }

    pub fn get_envd_access_token(&self, metadata: &SandboxMetadata) -> Option<EnvdAccessToken> {
        metadata
            .secure
            .then(|| self.access_tokens.generate(metadata.id))
    }

    pub fn validate_envd_access_token(&self, sandbox_id: SandboxId, candidate: &str) -> bool {
        self.access_tokens.matches(sandbox_id, candidate)
    }

    /// The incarnation of this sandbox that is alive on this node right now, or
    /// `None` when no VM of it is up here.
    ///
    /// 🔴 Only positive evidence. `None` means "this node is not currently
    /// serving this sandbox" and never "this node is serving an old one". A
    /// sandbox that is paused here, or being resumed here, or has never been
    /// here, all answer `None` — because their records name a run that is not
    /// the one about to serve traffic. A caller that refused on the strength of
    /// one of those would refuse every request in a cross-node resume window,
    /// which is precisely the window a user is waiting through.
    ///
    /// Reads the runtime route table rather than the backend handle: taking the
    /// sandbox mutex on the data-plane path would queue every request behind a
    /// pause or a snapshot, and the route table already holds the value the
    /// backend was built with.
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
            Some(metadata) if metadata.state == SandboxState::Paused => {
                debug!(auto_resume = metadata.auto_resume, "sandbox is paused");
                ProxyLookupResult::Paused {
                    auto_resume: metadata.auto_resume,
                }
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
            SandboxState::Creating
                | SandboxState::Resuming
                | SandboxState::Snapshotting
                | SandboxState::Forking
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

        // 🔴 The ceiling refuses here and nowhere else. An over-long renewal is
        // clamped by `_set_timeout` below and succeeds; only a sandbox that has
        // already outlived its ceiling is refused, because there is no window
        // left to clamp into. Reversing this — refusing anything that asks for
        // more than the ceiling allows — would hand a new 400 to every client
        // that passes a generous timeout.
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

            // Mirror the new deadline into the cluster registry. Best-effort,
            // like every other write on `PausedSandboxPublisher`: the local
            // record above is already the authoritative one, and this only
            // keeps `ReclaimExpiredHoldings`' cluster-wide backstop from
            // judging the sandbox against a deadline as stale as its last
            // resume — see `renew_deadline`'s own doc.
            //
            // 🔴 `update_result.current.expires_at`, not `valid_timeout` and
            // not `new_expire_time` from the closure above: `set_timeout`
            // clamps to the sandbox's lifetime ceiling before it lands on the
            // record, and this has to carry the same, already-clamped value
            // out — never the caller's raw request, which the ceiling was
            // never applied to at all.
            if let Some(publisher) = self.paused_publisher() {
                publisher
                    .renew_deadline(
                        sandbox_id,
                        update_result.current.execution_id,
                        update_result.current.expires_at,
                    )
                    .await;
            }
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
            this.delete_sandbox_inner(sandbox_id, ClusterDisposition::Forget)
                .await
        })
        .await
    }

    /// Tears down a local copy of a sandbox the cluster says belongs elsewhere.
    ///
    /// Identical to [`delete_sandbox`](Self::delete_sandbox) except that it
    /// leaves the cluster record and the snapshot behind it completely alone,
    /// and that difference is the whole point. The sandbox is not being
    /// deleted — it is alive on another node, under the row this node is about
    /// to stop disagreeing with. Routing a discard through the ordinary delete
    /// would hand `forget_sandbox` a row in a parked state, which it is
    /// entitled to clear from any node, and the other node's snapshot would go
    /// with it.
    pub async fn discard_superseded_sandbox(self: &Arc<Self>, sandbox_id: SandboxId) -> Result<()> {
        let this = Arc::clone(self);
        self.run_cancellation_safe("discard_superseded", sandbox_id, async move {
            this.delete_sandbox_inner(sandbox_id, ClusterDisposition::KeepClusterRecord)
                .await
        })
        .await
    }

    #[tracing::instrument(
        name = "delete_sandbox",
        skip(self),
        fields(sandbox_id = %sandbox_id, disposition = ?disposition)
    )]
    async fn delete_sandbox_inner(
        self: &Arc<Self>,
        sandbox_id: SandboxId,
        disposition: ClusterDisposition,
    ) -> Result<()> {
        info!("deleting sandbox");

        // Attempt to transition to Killing, retrying after waiting whenever we
        // find the sandbox in a transitional state.
        let previous_state = loop {
            match self
                .store
                .update_state_if_state(
                    &sandbox_id,
                    SandboxState::Killing,
                    &[SandboxState::Running, SandboxState::Paused],
                )
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
                    | SandboxState::Pausing
                    | SandboxState::Resuming => {
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
                    _ => {
                        return Err(OrchestratorError::from(StoreError::StateConflict {
                            sandbox_id,
                            expected_states: vec![SandboxState::Running, SandboxState::Paused],
                            actual_state,
                        }));
                    }
                },
                Err(err) => return Err(OrchestratorError::from(err)),
            }
        };

        // The authoritative execution for this sandbox, read now that this
        // call exclusively holds it in `Killing`. Nothing else can move the
        // execution id while that holds — the only op that does is a resume,
        // and a resume requires `Paused`, not `Killing` — so the detached
        // handle below is checked against the truth, not a stale local copy.
        // Nothing has mutated the backend yet at this point, so a failure
        // here rolls back to `previous_state` exactly like the "could not
        // reach the sandbox" branch below it does.
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

        let (handle, removed_route) = self
            .detach_sandbox_handle_and_route_checked(&sandbox_id, expected_execution_id)
            .await;
        // See the matching note in `pause_sandbox_inner`: an adopted backend is
        // never filed in the running set.
        let handle_was_held_here = handle.is_some();

        // 🔴 A delete that finds no handle used to drop straight through to
        // "remove the record", answer 204, and never tell anybody's machine.
        // On a replicated deciding half that is how a sandbox becomes an orphan
        // VM: the user is told it is gone, the node goes on running it, and the
        // only thing that named it has just been erased.
        let handle = match handle {
            Some(handle) => Some(handle),
            None => match self.absent_handle(sandbox_id).await {
                Ok(AbsentHandle::Adopted(handle)) => Some(handle),
                // Nothing is running that this half can reach, so there is
                // nothing to stop — and the record below is the last of it.
                Ok(AbsentHandle::RuntimeGone) | Ok(AbsentHandle::NoRecord) => None,
                // 🔴 Refused rather than completed. The record is the only
                // remaining handle on a VM that may well still be up, and a
                // delete that forgot it because a lookup timed out would be the
                // orphan this branch exists to prevent.
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

        // Read before `handle` is moved into the stop below: the real machine
        // this delete is about to stop the sandbox on, when this process did
        // not run it itself. `forget` needs this to tell a sandbox this
        // operation just stopped from one the registry names as running
        // somewhere this operation never touched — see
        // `PausedSandboxPublisher::forget`'s doc, and `mark_sandbox_running`'s
        // identical read for the mirror-image case (bringing a sandbox up,
        // not tearing it down).
        let holding_node_id_for_forget = match handle.as_ref() {
            Some(handle) => {
                let sandbox = handle.lock().await;
                sandbox.holding_node_id().map(str::to_string)
            }
            None => None,
        };

        // If a runtime could be reached, attempt to stop it.
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
        let metadata = self.store.remove(&sandbox_id).await?;
        if let Some(metadata) = metadata {
            self.publish_sandbox_event(
                SandboxLifecycleEventType::Delete,
                metadata.id,
                metadata.execution_id,
                metadata.resources,
            );
        }
        if let Err(err) = self
            .persister
            .delete_record_and_artifacts(&sandbox_id)
            .await
        {
            warn!(error = ?err, "failed to delete persisted sandbox state");
        }
        self.release_image_refs(RuntimeImageOwner::PausedSandbox(sandbox_id))
            .await;
        // The sandbox is gone, so its cluster row and the snapshot behind it are
        // garbage. Done here rather than at the API so an expiry-driven delete
        // cleans up as thoroughly as a requested one.
        if disposition == ClusterDisposition::Forget {
            if let Some(publisher) = self.paused_publisher() {
                publisher
                    .forget(sandbox_id, holding_node_id_for_forget)
                    .await;
            }
        }
        info!("sandbox deleted");

        Ok(())
    }

    /// Wires in cluster-wide pause bookkeeping. Idempotent; later calls are
    /// ignored, so the first wiring wins.
    pub fn set_paused_publisher(&self, publisher: Arc<dyn PausedSandboxPublisher>) {
        if self.paused_publisher.set(publisher).is_err() {
            warn!("paused sandbox publisher was already wired; ignoring");
        }
    }

    fn paused_publisher(&self) -> Option<&Arc<dyn PausedSandboxPublisher>> {
        self.paused_publisher.get()
    }

    /// Publishes a just-paused sandbox to the cluster and records the outcome
    /// on the local record.
    ///
    /// Called from `pause_sandbox_inner` rather than from its callers, so that
    /// an API pause, an expiry auto-pause and a shutdown pause all reach the
    /// cluster identically. Doing it per call site is what left the latter two
    /// unpublished, and those are the two whose sandboxes most need to survive
    /// losing the node.
    async fn publish_paused_sandbox(&self, sandbox_id: SandboxId, outcome: PauseOutcome) {
        let Some(publisher) = self.paused_publisher() else {
            return;
        };

        let Some(registered_as) = publisher.publish_paused(outcome).await else {
            return;
        };

        // From here on the cluster knows about this sandbox, so a later
        // reconciliation is allowed to act on what the registry says about it.
        // Recording that on the local record is what keeps reconciliation off
        // records that predate the registry.
        if let Err(err) = self
            .persister
            .mark_cluster_registered(&sandbox_id, &registered_as)
            .await
        {
            warn!(error = ?err, %sandbox_id, "failed to mark the local record as registered");
        }
    }

    /// Whether a paused record was ever announced to a cluster registry, and
    /// under which node identity.
    ///
    /// Only announced records may be discarded by reconciliation, and only
    /// against the identity they were announced under — which is what makes
    /// what the registry says about them mean anything at all.
    pub async fn paused_record_cluster_registration(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<ClusterRegistration> {
        self.persister
            .cluster_registration(&sandbox_id)
            .await
            .map_err(|err| OrchestratorError::InternalError(err.to_string()))
    }

    /// Where on this machine's disk a paused sandbox's capture was written.
    ///
    /// # 🔴 Why this is a read of its own rather than a value `pause_sandbox`
    /// returns
    ///
    /// The directory is allocated inside the pause and handed to the backend,
    /// and it is the *persisted record* that keeps it afterwards. A caller that
    /// took it from the return value of one call could only ever learn it about
    /// the pause it just performed — and a pause that found the sandbox already
    /// paused does no work and has no directory to report, while the bytes are
    /// sitting in one all the same.
    ///
    /// 🔴 `Ok(None)` means this node holds no paused record for the sandbox,
    /// never "the disk could not be read": the persister keeps those apart and
    /// so does this.
    pub async fn paused_artifact_root(
        &self,
        sandbox_id: &SandboxId,
    ) -> Result<Option<std::path::PathBuf>> {
        self.persister
            .paused_artifact_root(sandbox_id)
            .await
            .map_err(OrchestratorError::from)
    }

    /// The real machine a paused sandbox will reopen on, when that machine is
    /// already knowable — before anything has tried to resume it.
    ///
    /// # 🔴 Read-only. Never a claimant, never a self-comparison
    ///
    /// It is tempting to use this as the identity a cross-node resume claims
    /// under, so that `claimed_by_node_id` names the real machine instead of
    /// this process's own pod identity while a resume is in flight. a0487f0 tried
    /// exactly that and it is wrong twice over. First, mechanically: the
    /// claim's identity is also `mark_running`'s CAS guard, and quoting the
    /// real machine there while `mark_running` still had to name it *after*
    /// placement made every claim this method could answer fail its own
    /// confirmation. Second, and the reason this stays read-only even now
    /// that the guard is fixed: this value comes from *shared* cluster state,
    /// not from this process. Two different api replicas resuming the same
    /// sandbox concurrently can both read the identical answer here, both
    /// claim under it, and the loser's "is this claim mine" self-comparison
    /// (`arbitration`'s doc) would then read the winner's claim as its own —
    /// a value has to be unique to the *deciding process* to stand in for
    /// "was this decided by me", and this one is not. `self.paused.node_id()`
    /// is; use that for any claim or self-comparison, always.
    ///
    /// So: read-only, and specifically for a caller with no better answer for
    /// *where a sandbox will end up running* — the one shape that is safe is
    /// [`PausedSandboxPublisher::mark_running`]'s `holding_node_id`, which
    /// only ever lands in `origin_node_id`, a plain write with no comparison
    /// anywhere near it.
    ///
    /// # 🔴 Why `paused_handle`, not `SandboxMetadata::paused_state`
    ///
    /// That field is `#[serde(skip)]` and comes back `None` from any store
    /// that serialises a record and reads it back in — which on the api half
    /// is every store, since replicas share state through one. Reading `None`
    /// as "not paused here" would make this always fall back, silently
    /// reintroducing the bug this method exists to close. `paused_handle` is
    /// the question with the answers this needs: see its own doc for the
    /// three-way split.
    ///
    /// `None` covers every case that is not "a remote record names a
    /// machine" — no record, a local record (this process already knows the
    /// answer, itself), a remote record with no machine attached, and a store
    /// that could not be read. Callers must fall back to their own identity
    /// for all of those, exactly as this call's own caller does.
    pub async fn paused_origin_node_id(&self, sandbox_id: &SandboxId) -> Option<String> {
        match self.store.paused_handle(sandbox_id).await {
            Ok(PausedHandle::Remote { origin_node_id, .. }) => origin_node_id,
            _ => None,
        }
    }

    /// The real machine currently running `sandbox_id`, straight from the
    /// live backend.
    ///
    /// `None` on a backend that runs the VM in this same process — which is
    /// the correct, common answer everywhere but the api half — and on a
    /// sandbox this process holds no handle for at all. See
    /// [`SandboxBackend::holding_node_id`] for the convention this reads.
    pub async fn sandbox_holding_node_id(&self, sandbox_id: &SandboxId) -> Option<String> {
        let handle = self.sandboxes.read().await.get(sandbox_id).cloned()?;
        let sandbox = handle.lock().await;
        sandbox.holding_node_id().map(str::to_string)
    }

    /// Drops this node's local copy of a paused sandbox, leaving the sandbox
    /// itself alone.
    ///
    /// Used when the cluster registry says the sandbox has moved on — another
    /// node resumed it, or it was destroyed — so the local paused record is a
    /// leftover from before. This is deliberately **not** a delete: the sandbox
    /// may well be running on another node right now, so no `Delete` lifecycle
    /// event is published and nothing is reported as killed.
    ///
    /// Returns whether a record was actually discarded. Anything other than
    /// `Paused` is left untouched, so a stale reconciliation decision can never
    /// take down a live sandbox, and a resume that started in the meantime wins
    /// the state CAS.
    #[tracing::instrument(
        name = "discard_local_paused_record",
        skip(self),
        fields(sandbox_id = %sandbox_id)
    )]
    pub async fn discard_local_paused_record(
        self: &Arc<Self>,
        sandbox_id: SandboxId,
    ) -> Result<bool> {
        match self
            .store
            .update_state_if_state(&sandbox_id, SandboxState::Killing, &[SandboxState::Paused])
            .await
        {
            Ok(_) => {}
            Err(StoreError::StateConflict { actual_state, .. }) => {
                debug!(state = ?actual_state, "not discarding: sandbox is not paused here");

                return Ok(false);
            }
            Err(StoreError::SandboxNotFound { .. }) => return Ok(false),
            Err(err) => return Err(OrchestratorError::from(err)),
        }

        self.store.remove(&sandbox_id).await?;
        if let Err(err) = self
            .persister
            .delete_record_and_artifacts(&sandbox_id)
            .await
        {
            warn!(error = ?err, "failed to delete stranded paused sandbox artifacts");
        }
        self.release_image_refs(RuntimeImageOwner::PausedSandbox(sandbox_id))
            .await;
        info!("discarded stranded local paused record");

        Ok(true)
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
                let result = this.run_shutdown_cleanup().await;
                // 🔴 Best-effort and unconditional, even when the cleanup pass
                // above failed: the persister's RocksDB store (persisted
                // sandboxes; see `FileBackedSandboxPersister::close`) is not
                // holding anything that pause failures above would make unsafe
                // to close, and skipping this on failure would leave exactly
                // the nodes whose shutdown already went wrong also the ones
                // whose store teardown stays unbounded. Runs once, inside the
                // single-flight `get_or_init`, alongside the cleanup pass
                // itself.
                this.persister
                    .close(crate::local_store::DEFAULT_CLOSE_TIMEOUT)
                    .await;
                ShutdownOutcome::from_result(result)
            })
            .await;

        outcome.as_result()
    }

    /// Pauses a running sandbox by taking a snapshot and stopping its VM.
    ///
    /// If another `pause_sandbox` call is already in progress for the same
    /// sandbox (`Pausing` state), this call waits for it to complete and then
    /// returns the outcome rather than duplicating the work.
    pub async fn pause_sandbox(self: &Arc<Self>, sandbox_id: SandboxId) -> Result<SandboxMetadata> {
        self.pause_sandbox_with(sandbox_id, PausePublication::Here)
            .await
            .map(|outcome| outcome.metadata)
    }

    /// [`Self::pause_sandbox`], handing the capture back instead of publishing
    /// it here.
    ///
    /// # 🔴 Its one caller has no sandbox of its own and no publisher either
    ///
    /// The node service serves a `Pause` for a process that decided the pause,
    /// holds the cluster's record of the sandbox, and will commit the row. This
    /// machine's own publisher is not that process — on `aenv-node` it is
    /// `DisabledPausedSandboxRegistry`, which records nothing — so letting the
    /// ordinary path run would offer the capture to a publisher that drops it,
    /// and the caller would be told the pause produced nothing publishable.
    ///
    /// 🔴 [`PauseOutcome::publishable`] is `None` here whenever the pause did
    /// not *happen* on this call: a sandbox already paused, or a concurrent
    /// pause this one joined. That is not this path failing to produce a
    /// capture. The capture belongs to the pause that made it and is long gone,
    /// and `None` says exactly that to a caller that reads it as "nothing to
    /// publish" — which is the reading the wire has always documented.
    pub async fn pause_sandbox_for_publication(
        self: &Arc<Self>,
        sandbox_id: SandboxId,
    ) -> Result<PauseOutcome> {
        self.pause_sandbox_with(sandbox_id, PausePublication::ByCaller)
            .await
    }

    async fn pause_sandbox_with(
        self: &Arc<Self>,
        sandbox_id: SandboxId,
        publication: PausePublication,
    ) -> Result<PauseOutcome> {
        let this = Arc::clone(self);
        self.run_cancellation_safe("pause", sandbox_id, async move {
            this.pause_sandbox_inner(sandbox_id, publication).await
        })
        .await
    }

    #[tracing::instrument(
        name = "pause_sandbox",
        skip(self),
        fields(sandbox_id = %sandbox_id)
    )]
    async fn pause_sandbox_inner(
        self: &Arc<Self>,
        sandbox_id: SandboxId,
        publication: PausePublication,
    ) -> Result<PauseOutcome> {
        info!("pausing sandbox");
        match self
            .store
            .update_state_if_state(&sandbox_id, SandboxState::Pausing, &[SandboxState::Running])
            .await
        {
            Ok(_) => {}
            Err(StoreError::StateConflict { actual_state, .. }) => {
                return match actual_state {
                    // Another task is already performing the pause.  Wait for
                    // it to finish and then report the final outcome.
                    SandboxState::Pausing => self
                        .join_concurrent_pause(sandbox_id)
                        .await
                        .map(PauseOutcome::nothing_to_publish),
                    // Already paused: idempotent success. The capture belongs to
                    // the pause that produced it and is long gone, so there is
                    // nothing left to publish here.
                    SandboxState::Paused => match self.store.get(&sandbox_id).await? {
                        Some(metadata) => Ok(PauseOutcome::nothing_to_publish(metadata)),
                        None => Err(OrchestratorError::SandboxNotFound(sandbox_id)),
                    },
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

        // Pin paused runtime artifacts before detaching from the running set.
        let runtime_artifacts = {
            let handle = self.sandboxes.read().await.get(&sandbox_id).cloned();
            match handle {
                Some(handle) => {
                    let sandbox = handle.lock().await;
                    sandbox.runtime_info().runtime_artifacts
                }
                None => RuntimeArtifactSet::empty(),
            }
        };
        if let Err(error) = self
            .protect_image_refs(
                RuntimeImageOwner::PausedSandbox(sandbox_id),
                runtime_artifacts,
                "paused sandbox",
            )
            .await
        {
            warn!(error = %error, "failed to protect paused runtime artifacts; keeping sandbox Running");
            let _ = self
                .store
                .update_state_if_state(&sandbox_id, SandboxState::Running, &[SandboxState::Pausing])
                .await;
            return Err(error);
        }

        // Allocate persistence space while the running handle and route are
        // still attached. Allocation does not mutate the backend, so failure
        // only needs to restore metadata and release the temporary image refs.
        let artifact_root = match self.persister.allocate_artifact_root(&sandbox_id).await {
            Ok(artifact_root) => artifact_root,
            Err(err) => {
                warn!(error = ?err, "failed to allocate paused sandbox artifact root");
                self.release_image_refs(RuntimeImageOwner::PausedSandbox(sandbox_id))
                    .await;
                let _ = self
                    .store
                    .update_state_if_state(
                        &sandbox_id,
                        SandboxState::Running,
                        &[SandboxState::Pausing],
                    )
                    .await;
                return Err(OrchestratorError::from(err));
            }
        };

        // The authoritative execution for this sandbox, read now that this
        // call exclusively holds it in `Pausing`. Nothing else can move the
        // execution id while that holds — the only op that does is a resume,
        // and a resume requires `Paused`, not `Pausing` — so the detached
        // handle below is checked against the truth, not a stale local copy.
        //
        // Nothing has touched the backend yet at this point (the pinned
        // image refs and allocated artifact root above are the only side
        // effects so far), so a failure here gets exactly the same rollback
        // as the two branches immediately above it: release the refs, put
        // the record back to `Running`, return.
        let expected_execution_id = match self.store.get(&sandbox_id).await {
            Ok(Some(metadata)) => metadata.execution_id,
            Ok(None) => {
                warn!("sandbox record disappeared while pausing");
                self.release_image_refs(RuntimeImageOwner::PausedSandbox(sandbox_id))
                    .await;
                return Err(OrchestratorError::SandboxNotFound(sandbox_id));
            }
            Err(err) => {
                warn!(error = ?err, "failed to read sandbox record while pausing");
                self.release_image_refs(RuntimeImageOwner::PausedSandbox(sandbox_id))
                    .await;
                let _ = self
                    .store
                    .update_state_if_state(
                        &sandbox_id,
                        SandboxState::Running,
                        &[SandboxState::Pausing],
                    )
                    .await;
                return Err(OrchestratorError::from(err));
            }
        };

        let (handle, removed_proxy_route) = self
            .detach_sandbox_handle_and_route_checked(&sandbox_id, expected_execution_id)
            .await;

        // 🔴 Whether the backend below is this process's own. A handle rebuilt
        // from the record is built for one operation and is never put into the
        // running set: that map is what this process is *running*, and a
        // replica that filed a stub for somebody else's sandbox in it would go
        // on reporting a sandbox it does not hold, with a backend that goes
        // stale the moment the sandbox is resumed on another machine.
        let handle_was_held_here = handle.is_some();
        let handle = match handle {
            Some(handle) => handle,
            None => match self.absent_handle(sandbox_id).await {
                // The sandbox is running on a machine this half addresses, and
                // this replica simply is not the one that started it.
                Ok(AbsentHandle::Adopted(handle)) => handle,
                // 🔴 The only branch that may still remove the record: this
                // factory's sandboxes live in this process, so a record with no
                // handle here describes a runtime that is gone.
                Ok(AbsentHandle::RuntimeGone) => {
                    warn!("sandbox handle not found while pausing, removing from store");
                    self.release_image_refs(RuntimeImageOwner::PausedSandbox(sandbox_id))
                        .await;
                    self.store.remove(&sandbox_id).await?;
                    return Err(OrchestratorError::SandboxNotFound(sandbox_id));
                }
                // Nothing to remove: something else already took the record.
                Ok(AbsentHandle::NoRecord) => {
                    warn!("sandbox record disappeared while pausing");
                    self.release_image_refs(RuntimeImageOwner::PausedSandbox(sandbox_id))
                        .await;
                    return Err(OrchestratorError::SandboxNotFound(sandbox_id));
                }
                // 🔴 Not knowing is not a licence to delete. The record stays
                // exactly as it was and the sandbox goes back to `Running`, so
                // a retry — on this replica or another — finds the same
                // sandbox it would have found had this call never happened.
                Err(error) => {
                    warn!(error = %error, "could not reach the sandbox while pausing; leaving its record alone");
                    self.release_image_refs(RuntimeImageOwner::PausedSandbox(sandbox_id))
                        .await;
                    let _ = self
                        .store
                        .update_state_if_state(
                            &sandbox_id,
                            SandboxState::Running,
                            &[SandboxState::Pausing],
                        )
                        .await;
                    self.restore_proxy_route(sandbox_id, removed_proxy_route)
                        .await;
                    return Err(error);
                }
            },
        };

        // Pause the sandbox and capture the paused state for resuming later.
        //
        // 🔴 Whether anyone is waiting to commit a publishable capture is asked
        // *now*, before the backend is told to make one. A backend that has to
        // write durable bytes to produce one — which is every backend driving a
        // sandbox on another machine — would otherwise write them for a
        // publisher that is about to drop the capture, and unannounced bytes
        // are unreachable by every read path there is. See
        // [`PausedSandboxPublisher::wants_publishable_capture`].
        let committer_waiting = match publication {
            PausePublication::ByCaller => true,
            PausePublication::Here => self
                .paused_publisher()
                .is_some_and(|publisher| publisher.wants_publishable_capture()),
        };
        let paused_state_result = {
            let mut sandbox = handle.lock().await;
            sandbox
                .pause(artifact_root.as_deref(), committer_waiting)
                .await
        };

        // If pausing failed, attempt to put the sandbox back and return an error.
        let capture = match paused_state_result {
            Ok(capture) => capture,
            Err(err) => {
                warn!(error = ?err, "failed to pause sandbox");
                if err.is_terminal() {
                    // The handle was already detached from `self.sandboxes`
                    // before `pause()`. Do not reinsert it here: the live
                    // runtime may have been mutated and is no longer safe to
                    // keep serving as a running sandbox.
                    let stop_result = {
                        let mut sandbox = handle.lock().await;
                        sandbox.stop().await
                    };
                    if let Err(stop_err) = stop_result {
                        warn!(error = ?stop_err, "failed to stop sandbox after terminal pause failure");
                    }
                    self.store.remove(&sandbox_id).await?;
                } else {
                    if handle_was_held_here {
                        self.sandboxes.write().await.insert(sandbox_id, handle);
                    }
                    self.restore_proxy_route(sandbox_id, removed_proxy_route)
                        .await;
                    let _ = self
                        .store
                        .update_state_if_state(
                            &sandbox_id,
                            SandboxState::Running,
                            &[SandboxState::Pausing],
                        )
                        .await;
                }
                self.release_image_refs(RuntimeImageOwner::PausedSandbox(sandbox_id))
                    .await;
                return Err(OrchestratorError::SandboxOperationFailed {
                    sandbox_id,
                    operation: SandboxOperation::Pause,
                    source: err.into(),
                });
            }
        };

        let PausedSandboxCapture {
            state: paused_state,
            publishable,
        } = capture;

        let persisted_metadata = {
            let mut metadata = self
                .store
                .get(&sandbox_id)
                .await?
                .ok_or(OrchestratorError::SandboxNotFound(sandbox_id))?;
            metadata.state = SandboxState::Paused;
            // 🔴 Before `persist_paused` below, not after. The persisted copy
            // is what a restarted node reads back, and `running_since` is not
            // serialised — so a run charged only into the in-memory record
            // would be given back for free by the next node restart.
            metadata.sync_running_clock(SystemTime::now());
            metadata.paused_state = Some(paused_state.clone());
            metadata
        };
        if let Err(err) = self
            .persister
            .persist_paused(
                &persisted_metadata,
                artifact_root.as_deref(),
                paused_state.as_ref(),
            )
            .await
        {
            warn!(error = ?err, "failed to persist paused sandbox state");
            let resume_result = {
                let mut sandbox = handle.lock().await;
                sandbox.resume().await
            };
            if let Err(resume_err) = resume_result {
                warn!(error = ?resume_err, "failed to resume sandbox after pause failure");
                let stop_result = {
                    let mut sandbox = handle.lock().await;
                    sandbox.stop().await
                };
                if let Err(stop_err) = stop_result {
                    warn!(error = ?stop_err, "failed to stop sandbox after pause failure");
                }
                if let Err(error) = self.store.remove(&sandbox_id).await {
                    warn!(error = ?error, "failed to remove sandbox after pause failure");
                }
            } else {
                if handle_was_held_here {
                    self.sandboxes.write().await.insert(sandbox_id, handle);
                }
                self.restore_proxy_route(sandbox_id, removed_proxy_route)
                    .await;
                let _ = self
                    .store
                    .update_state_if_state(
                        &sandbox_id,
                        SandboxState::Running,
                        &[SandboxState::Pausing],
                    )
                    .await;
            }
            self.release_image_refs(RuntimeImageOwner::PausedSandbox(sandbox_id))
                .await;
            return Err(OrchestratorError::InternalError(format!(
                "failed to persist paused sandbox state: {err:#}"
            )));
        }
        let resources = persisted_metadata.resources;
        let paused_execution_id = persisted_metadata.execution_id;
        let paused_metadata = persisted_metadata.clone();
        self.store.update(persisted_metadata).await?;

        // Stop the sandbox to free up resources.
        let stop_result = {
            let mut sandbox = handle.lock().await;
            sandbox.stop().await
        };
        if let Err(err) = stop_result {
            warn!(error = ?err, "failed to stop sandbox after pausing");
        }
        self.publish_sandbox_event(
            SandboxLifecycleEventType::Pause,
            sandbox_id,
            paused_execution_id,
            resources,
        );
        info!("sandbox paused");

        // The sandbox is already paused and locally resumable, so this runs
        // after the point of no return on purpose: it can only add cross-node
        // recovery, never take the pause away.
        let outcome = PauseOutcome {
            metadata: paused_metadata,
            publishable,
        };

        match publication {
            PausePublication::Here => {
                let metadata = outcome.metadata.clone();
                self.publish_paused_sandbox(sandbox_id, outcome).await;
                // 🔴 Emptied on the way out rather than left populated. This arm
                // has already handed the capture to the publisher; a caller
                // finding one here would be looking at a capture that has been
                // consumed, and the only thing it could do with it is publish it
                // a second time.
                Ok(PauseOutcome::nothing_to_publish(metadata))
            }
            // 🔴 No publisher call at all, not even a best-effort one. The
            // caller is the process that will commit, and this machine offering
            // the same capture to its own publisher as well is how one pause
            // comes to write two rows.
            PausePublication::ByCaller => Ok(outcome),
        }
    }

    /// Where this resume gets the runtime state it has to reopen.
    ///
    /// # 🔴 Not `SandboxMetadata::paused_state`, and the field is why
    ///
    /// That field is `#[serde(skip)]`. It survives inside the process that
    /// captured it and comes back `None` from any store that writes a record
    /// out and reads it in — which is every store the API half runs on. Read
    /// through `get`, the resulting `None` says two opposite things at once:
    /// *this sandbox was never paused*, and *this store cannot hand out
    /// handles, the bytes are on another machine*. Answering the second with
    /// the first is a 500 on every resume, describing a state the sandbox is
    /// not in.
    ///
    /// [`MetadataStore::paused_handle`] is asked instead, and it has the three
    /// answers the question has.
    ///
    /// # 🔴 Four ways this can end, and no two of them license the same move
    ///
    /// - the store could not be reached — [`OrchestratorError::StoreOperationFailed`],
    ///   and the right move is to ask again;
    /// - there is no record at all — [`OrchestratorError::SandboxNotFound`],
    ///   which the resume surface acts on by rebuilding the sandbox from the
    ///   cluster's published snapshot;
    /// - there is a record and it carries no capture — the sandbox cannot be
    ///   reopened from this record, and saying so is not the same as saying
    ///   there is no record;
    /// - there is a reference and this process cannot decode it — the bytes are
    ///   somewhere, and this build is not the one that can reach them.
    ///
    /// The last two are both internal failures and are deliberately not one
    /// message: the first is a record that lost its capture, the second is a
    /// factory that does not understand a capture that is still there.
    async fn paused_state_for_resume(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Arc<dyn PausedSandboxState>> {
        match self.store.paused_handle(&sandbox_id).await? {
            // The store kept the handle in this process; it is already the
            // thing the factory wants.
            PausedHandle::Local(state) => Ok(state),
            // 🔴 Decoded by *this* factory, which is the seam that makes the
            // reference mean something: on a node it turns back into local
            // paths, and on the API half it turns into the machine and the run
            // a `Resume` is addressed to. `artifact_root` is whatever the
            // record carried — both in-tree factories read the location out of
            // the encoded state itself and ignore the argument — so an absent
            // one is passed on as such rather than refused.
            PausedHandle::Remote {
                reference,
                origin_node_id,
            } => {
                let artifact_root = reference.artifact_root.clone().unwrap_or_default();
                self.factory
                    .decode_paused_state(artifact_root, reference.state)
                    .map_err(|err| {
                        warn!(
                            %sandbox_id,
                            origin_node_id = origin_node_id.as_deref().unwrap_or("unrecorded"),
                            error = %format_args!("{err:#}"),
                            "a paused sandbox's stored capture could not be decoded here"
                        );
                        OrchestratorError::InternalError(format!(
                            "sandbox {sandbox_id}'s stored paused state could not be decoded by \
                             this build: {err:#}"
                        ))
                    })
            }
            PausedHandle::NotPaused => {
                warn!(%sandbox_id, "a sandbox recorded as paused carries no capture");
                Err(OrchestratorError::InternalError(format!(
                    "sandbox {sandbox_id} is recorded as paused and its record carries no capture \
                     to reopen"
                )))
            }
        }
    }

    /// Brings a paused sandbox back up under the incarnation its claim
    /// allocated.
    ///
    /// If another `resume_sandbox` call is already in progress (`Resuming`
    /// state), this call waits for the ongoing resume to finish and then
    /// returns the actual outcome (either `Running` or an error) rather than
    /// duplicating the work. On success the sandbox is ready for use when this
    /// method returns.
    ///
    /// 🔴 The [`ClaimedExecution`] is the resume's licence, not a parameter of
    /// convenience: the only way to obtain one is the resume arbitration, so
    /// every path that reaches this function — the REST resume, the cross-node
    /// rebuild, and the data-plane auto-resume — has been through that one
    /// decision point. Taking it by value is what makes one claim start one
    /// sandbox.
    pub async fn resume_sandbox(
        self: &Arc<Self>,
        sandbox_id: SandboxId,
        timeout: NewTimeout,
        claimed: ClaimedExecution,
    ) -> Result<SandboxMetadata> {
        let this = Arc::clone(self);
        self.run_cancellation_safe("resume", sandbox_id, async move {
            this.resume_sandbox_inner(sandbox_id, timeout, claimed)
                .await
        })
        .await
    }

    #[tracing::instrument(
        name = "resume_sandbox",
        skip(self),
        fields(sandbox_id = %sandbox_id, timeout = ?timeout)
    )]
    async fn resume_sandbox_inner(
        self: Arc<Self>,
        sandbox_id: SandboxId,
        timeout: NewTimeout,
        claimed: ClaimedExecution,
    ) -> Result<SandboxMetadata> {
        self.ensure_accepting_lifecycle_operations()?;

        info!("resuming sandbox");
        let mut metadata = self
            .store
            .get(&sandbox_id)
            .await?
            .ok_or(OrchestratorError::SandboxNotFound(sandbox_id))?;

        // If another resume is in progress, wait for it to complete and
        // re-evaluate the resulting stable state.
        if metadata.state == SandboxState::Resuming {
            metadata = self
                .wait_for_transition(sandbox_id, SandboxState::Resuming)
                .await?;
        }

        match metadata.state {
            SandboxState::Killing => {
                return Err(OrchestratorError::SandboxNotFound(sandbox_id));
            }
            SandboxState::Running => {
                // Already running — just update the timeout if requested and return.
                return self.maybe_update_running_timeout(sandbox_id, timeout).await;
            }
            SandboxState::Paused => {}
            state => {
                return Err(OrchestratorError::InvalidSandboxState { sandbox_id, state });
            }
        }

        let node_mode = ConfigManager::global_config().virtualization_mode;
        if metadata.virtualization_mode != node_mode {
            return Err(OrchestratorError::VirtualizationModeMismatch {
                resource: format!("paused sandbox {sandbox_id}"),
                resource_mode: metadata.virtualization_mode,
                node_mode,
            });
        }

        // 🔴 Before the sandbox is moved to `Resuming`, and that ordering is the
        // whole of the fix. Read afterwards, a sandbox whose capture cannot be
        // got at is left sitting in `Resuming` for ever — a transitional state
        // with no owner, which every later resume, pause and delete waits on
        // until the wait times out. Read here, the refusal leaves it `Paused`,
        // which is what it still is.
        let paused_state = self.paused_state_for_resume(sandbox_id).await?;

        match self
            .store
            .update_state_if_state(&sandbox_id, SandboxState::Resuming, &[SandboxState::Paused])
            .await
        {
            Ok(_) => {}
            Err(StoreError::StateConflict { actual_state, .. }) => {
                return match actual_state {
                    SandboxState::Running => {
                        // Another task already completed the resume.
                        self.maybe_update_running_timeout(sandbox_id, timeout).await
                    }
                    SandboxState::Resuming => {
                        // A second concurrent resume snuck in between our state
                        // read and CAS.  Wait for it and return the outcome.
                        self.join_concurrent_resume(sandbox_id, timeout).await
                    }
                    SandboxState::Killing => {
                        info!("sandbox is being deleted while resuming");
                        Err(OrchestratorError::SandboxNotFound(sandbox_id))
                    }
                    _ => {
                        info!(state = ?actual_state, "cannot resume sandbox in current state");
                        Err(OrchestratorError::InvalidSandboxState {
                            sandbox_id,
                            state: actual_state,
                        })
                    }
                };
            }
            Err(err) => return Err(OrchestratorError::from(err)),
        }

        if let Err(err) = self.persister.mark_resuming(&sandbox_id).await {
            warn!(error = ?err, "failed to mark persisted sandbox record as resuming");
            let _ = self
                .store
                .update_state_if_state(&sandbox_id, SandboxState::Paused, &[SandboxState::Resuming])
                .await;
            return Err(OrchestratorError::InternalError(format!(
                "failed to mark persisted sandbox record as resuming: {err:#}"
            )));
        }

        let resumed_execution_id = claimed.execution_id();
        let resumed = self
            .launch_sandbox(LaunchPlan::for_resume(
                sandbox_id,
                claimed,
                paused_state,
                timeout,
                metadata.resources,
                metadata
                    .secure
                    .then(|| self.access_tokens.generate(metadata.id)),
            ))
            .await;
        if let Ok(metadata) = resumed.as_ref() {
            self.release_image_refs(RuntimeImageOwner::PausedSandbox(sandbox_id))
                .await;
            self.publish_sandbox_event(
                SandboxLifecycleEventType::Resume,
                metadata.id,
                metadata.execution_id,
                metadata.resources,
            );
            // Tell the cluster the sandbox is live again. Its snapshot stays
            // behind as the sandbox's durable fallback until the next pause
            // replaces it.
            if let Some(publisher) = self.paused_publisher() {
                // The deadline goes with the write. Reclamation needs one, and
                // until the first lease renewal the row would otherwise carry
                // none — a window in which losing this node strands the row
                // permanently.
                // 🔴 The incarnation the claim allocated, not a fresh one.
                // The registry's cross-node branch matches on exactly this
                // value, so minting here would fail every cross-node resume.
                //
                // 🔴 The real machine, read off the backend `launch_sandbox`
                // just started, not this process's own identity. `None` on
                // every backend that runs the VM in this same process — the
                // publisher falls back to its own identity for exactly that
                // case, mirroring `publish_paused`'s identical fallback for
                // the paused half of the same question.
                let holding_node_id = self.sandbox_holding_node_id(&sandbox_id).await;
                publisher
                    .mark_running(
                        sandbox_id,
                        resumed_execution_id,
                        metadata.expires_at,
                        holding_node_id,
                    )
                    .await;
            }
        }
        resumed
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

        // The authoritative execution for this sandbox, read now that this
        // call exclusively holds it in `Snapshotting`. Nothing else can move
        // the execution id while that holds, so a cached handle is checked
        // against the truth, not a stale local copy of it. Nothing has
        // touched the backend yet at this point, so a failure here rolls
        // back to `Running` exactly like the "could not reach the sandbox"
        // branch below it does.
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

        // Get the sandbox handle, discarding it first if a pause+resume this
        // replica never observed has already superseded it.
        let handle = self
            .cached_handle_for_execution(sandbox_id, expected_execution_id)
            .await;
        let handle = match handle {
            Some(handle) => handle,
            None => match self.absent_handle(sandbox_id).await {
                Ok(AbsentHandle::Adopted(handle)) => handle,
                // 🔴 See the matching branch in `pause_sandbox_inner`: this is
                // the only reading of a missing handle that means the sandbox
                // is gone, and so the only one that may take the record.
                Ok(AbsentHandle::RuntimeGone) => {
                    warn!("sandbox handle not found while snapshotting, removing from store");
                    self.detach_sandbox_handle_and_route(&sandbox_id).await;
                    self.store.remove(&sandbox_id).await?;
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
                if err.is_terminal() {
                    self.detach_sandbox_handle_and_route(&sandbox_id).await;
                    let stop_result = {
                        let mut sandbox = handle.lock().await;
                        sandbox.stop().await
                    };
                    if let Err(stop_err) = stop_result {
                        warn!(error = ?stop_err, "failed to stop sandbox after terminal snapshot failure");
                    }
                    self.store.remove(&sandbox_id).await?;
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

        // As in `fork_sandbox_inner`: on a replicated deciding half the handle
        // usually lives on another replica, and that is not a conflict — nor
        // is a handle this replica does hold but whose execution
        // `metadata.execution_id` above has already superseded;
        // `cached_handle_for_execution` discards that one exactly as if it
        // had never been here.
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

        let runtime_policy = network_policy.runtime_policy();

        let update_result = {
            let mut sandbox = sandbox.lock().await;
            sandbox.update_network_policy(runtime_policy).await
        };
        update_result.map_err(|source| OrchestratorError::SandboxOperationFailed {
            sandbox_id,
            operation: SandboxOperation::UpdateNetwork,
            source,
        })?;

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

        // A handle this replica holds but whose execution `metadata.execution_id`
        // above has already superseded is discarded exactly as if it had
        // never been cached — see `cached_handle_for_execution`.
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

    /// Assigns an already-approved custom extension params value to a
    /// running sandbox, with no hook involved.
    ///
    /// The node-reachable half of
    /// [`patch_sandbox_custom_extension_params`](Self::patch_sandbox_custom_extension_params):
    /// the deciding half runs the patch-params hook and then calls this to
    /// apply what the hook approved; a node's `update_params` RPC handler
    /// calls this directly, because by the time a request reaches it the hook
    /// has already run once, on the caller's side, and running it again would
    /// be a second chance for the extension to change its mind about a value
    /// the caller has already recorded.
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

        // A handle this replica holds but whose execution `metadata.execution_id`
        // above has already superseded is discarded exactly as if it had
        // never been cached — see `cached_handle_for_execution`.
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

    /// The assign-then-persist tail shared by
    /// [`patch_sandbox_custom_extension_params_inner`](Self::patch_sandbox_custom_extension_params_inner)
    /// and
    /// [`replace_sandbox_custom_extension_params_inner`](Self::replace_sandbox_custom_extension_params_inner).
    ///
    /// The backend assignment is tried first and its failure is returned
    /// without touching the metadata store: a caller that gets `Err` here
    /// must not also see `GET` report a value the running sandbox never
    /// received — the store staying stale on failure is the point, not a
    /// side effect.
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

    /// How many times this orchestrator has thrown away a process-local
    /// sandbox handle because its execution no longer matched the
    /// authoritative metadata record — see
    /// [`OrchestratorCounters::record_stale_handle_discarded`] for what that
    /// means and why every store read used to investigate one shows nothing
    /// wrong.
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

    /// Applies `timeout` to `metadata` and persists the change if the sandbox
    /// is still `Running`. Returns the updated metadata. If `timeout` is `None`,
    /// the timeout will be cleared, which indicates no expiration.
    async fn maybe_update_running_timeout(
        &self,
        sandbox_id: SandboxId,
        timeout: NewTimeout,
    ) -> Result<SandboxMetadata> {
        let update_result = self
            .store
            .update_if_state(&sandbox_id, &[SandboxState::Running], |metadata| {
                metadata.update_timeout(timeout);
            })
            .await
            .map_err(|err| match err {
                StoreError::StateConflict { actual_state, .. } => {
                    info!(state = ?actual_state, "cannot update timeout for sandbox in current state");
                    OrchestratorError::InvalidSandboxState {
                        sandbox_id,
                        state: actual_state,
                    }
                }
                other => OrchestratorError::from(other),
            })?;
        Ok(update_result.current)
    }

    /// Joins a concurrent pause already in progress for the same sandbox.
    /// Waits for the `Pausing` state to resolve and maps the final state to
    /// the appropriate `Ok(...)` / `Err(...)` result.
    ///
    /// The joined outcome never carries a publishable capture: the capture
    /// belongs to the pause that produced it, and only that caller can publish
    /// it. A joiner learns the pause succeeded, nothing more.
    async fn join_concurrent_pause(&self, sandbox_id: SandboxId) -> Result<SandboxMetadata> {
        debug!("concurrent pause in progress, waiting for completion");
        let m = self
            .wait_for_transition(sandbox_id, SandboxState::Pausing)
            .await?;
        match m.state {
            SandboxState::Paused => {
                debug!("concurrent pause succeeded");

                // Publishing belongs to the pause that produced the capture;
                // the winner has already done it.
                Ok(m)
            }
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
        }
    }

    /// Joins a concurrent resume already in progress for the same sandbox.
    /// Waits for the `Resuming` state to resolve, then applies `timeout` if
    /// the sandbox reached `Running`, and returns the final metadata.
    async fn join_concurrent_resume(
        &self,
        sandbox_id: SandboxId,
        timeout: NewTimeout,
    ) -> Result<SandboxMetadata> {
        debug!("concurrent resume in progress, waiting for completion");
        let m = self
            .wait_for_transition(sandbox_id, SandboxState::Resuming)
            .await?;
        match m.state {
            SandboxState::Running => self.maybe_update_running_timeout(sandbox_id, timeout).await,
            SandboxState::Paused => {
                info!("concurrent resume failed; sandbox returned to paused state");
                Err(OrchestratorError::InvalidSandboxState {
                    sandbox_id,
                    state: SandboxState::Paused,
                })
            }
            SandboxState::Killing => {
                info!("sandbox is being deleted while resuming");
                Err(OrchestratorError::SandboxNotFound(sandbox_id))
            }
            state => {
                info!(state = ?state, "unexpected state after waiting for concurrent resume");
                Err(OrchestratorError::InvalidSandboxState { sandbox_id, state })
            }
        }
    }

    /// Automatically pauses or stops sandboxes whose timeout has expired.
    async fn evict_expired_sandboxes(self: &Arc<Self>) -> Result<Vec<SandboxId>> {
        if self.is_shutting_down() {
            debug!("skipping auto-evict because orchestrator is shutting down");
            return Ok(Vec::new());
        }

        // Bounded, not `list_expired` (unbounded) — see
        // `AUTO_EVICT_BATCH_LIMIT`'s own doc for why a capped round is
        // still guaranteed to make progress on the whole backlog.
        let expired = self
            .store
            .expired_batch(SystemTime::now(), AUTO_EVICT_BATCH_LIMIT)
            .await?;
        let mut evicted_ids = Vec::new();

        for metadata in expired {
            if metadata.state != SandboxState::Running {
                continue;
            }
            if let Err(err) = match metadata.timeout_action {
                // Publishing happens inside the pause, so an expired sandbox is
                // just as recoverable from another node as an explicitly paused
                // one — which matters more here, not less: nobody is watching.
                SandboxTimeoutAction::Pause => self
                    .pause_sandbox_inner(metadata.id, PausePublication::Here)
                    .await
                    .map(|_| ()),
                SandboxTimeoutAction::Delete => {
                    self.delete_sandbox_inner(metadata.id, ClusterDisposition::Forget)
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

    /// Puts this orchestrator's own record of a sandbox onto the create that
    /// is about to be sent to the machine that will run it.
    ///
    /// # 🔴 Why here and not at the API surface
    ///
    /// The marker is the control plane's record of *this* sandbox, and a
    /// record is only complete once the incarnation is minted — which happens
    /// in [`LaunchPlan::for_create_from_snapshot`], below the surface that
    /// decided to create anything. A marker written earlier would name a run
    /// that had not been chosen yet, and fencing compares exactly that value.
    ///
    /// # 🔴 Why it is a no-op almost everywhere
    ///
    /// Only a factory whose sandboxes run on other machines asks for one
    /// ([`SandboxBackendFactory::stamps_control_plane_ownership`]), so on
    /// `aenv-node` this returns on its first line. That is
    /// the property that keeps `None` meaning what it has always meant on the
    /// user-facing REST surface: not that a marker went missing, but that no
    /// control plane owns this sandbox.
    ///
    /// A caller that supplied its own marker keeps it: that is the node
    /// service's create, where the marker arrived from the control plane and
    /// this process is not it.
    fn stamp_control_plane_ownership(&self, plan: &mut LaunchPlan) {
        if !self.factory.stamps_control_plane_ownership() {
            return;
        }
        let LaunchPlan::Create(plan) = plan else {
            // A resume drives a sandbox that already exists, and its marker was
            // written when it was created.
            return;
        };
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
        self.ensure_accepting_lifecycle_operations()?;

        // Before the record is written and before the backend is built: both
        // of those consume the marker, and they must consume the same one.
        let mut plan = plan;
        self.stamp_control_plane_ownership(&mut plan);
        let plan = plan;

        let sandbox_id = plan.sandbox_id();
        let transitional_state = plan.transitional_state();

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
        if let Err(source) = sandbox.start_nowait().await {
            warn!(error = %format_args!("{source:#}"), "failed to start sandbox");
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
        let transitional_metadata = plan.transitional_metadata().map(|metadata| {
            let mut metadata = metadata.clone();
            metadata.resources = runtime_resources;
            // 🔴 Only ever `Some` for a backend that did not know its own
            // sandbox's context and image configs until it started — see
            // `SandboxRuntimeInfo::resolved_image_facts`. Every other backend
            // already wrote the right values into `transitional_metadata`
            // before this point, and leaves this `None`.
            if let Some(facts) = runtime_info.resolved_image_facts.clone() {
                metadata.context = facts.context;
                metadata.image_configs = facts.image_configs;
            }
            metadata
        });

        // Store the sandbox handle in memory.
        let handle = Arc::new(Mutex::new(sandbox));
        self.sandboxes
            .write()
            .await
            .insert(sandbox_id, handle.clone());

        self.release_image_refs(RuntimeImageOwner::StartingSandbox(sandbox_id))
            .await;

        // Persist the sandbox metadata if needed (during creation).
        if let Some(metadata) = transitional_metadata.as_ref() {
            if let Err(err) = self.store.add(metadata.clone()).await {
                warn!(error = %format_args!("{err:#}"), "failed to persist sandbox metadata; cleaning up");
                self.cleanup_failed_launch(&plan, handle, FailedLaunchStage::Registered)
                    .await;
                return Err(OrchestratorError::from(err));
            }
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

        let launch_timeout = plan.timeout();
        let launch_execution_id = plan.execution_id();
        let final_metadata = match self
            .store
            .update_if_state(
                &sandbox_id,
                std::slice::from_ref(&transitional_state),
                move |metadata| {
                    metadata.resources = runtime_resources;
                    // A resume starts from the record the pause left behind,
                    // which names the run that produced it. This is where the
                    // record starts naming the run that is about to serve it.
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
            match Self::proxy_target_from_sandbox(sandbox.as_ref()) {
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

        if matches!(plan, LaunchPlan::Resume(_)) {
            if let Err(err) = self.persister.delete_record(&sandbox_id).await {
                warn!(error = %format_args!("{err:#}"), "failed to delete persisted sandbox record after resume");
            }
        }

        info!("sandbox launch completed");
        Ok(final_metadata)
    }

    fn build_sandbox(&self, plan: &LaunchPlan) -> Result<Box<dyn SandboxBackend>> {
        let execution_id = plan.execution_id();
        let build_result = match plan {
            LaunchPlan::Create(plan) => match &plan.source {
                CreateLaunchSource::Snapshot { snapshot } => self.factory.build_from_snapshot(
                    snapshot,
                    plan.launch_config.clone(),
                    execution_id,
                ),
                CreateLaunchSource::SnapshotRecord { record } => self
                    .factory
                    .build_from_snapshot_record(record, plan.launch_config.clone(), execution_id),
                CreateLaunchSource::Fresh { build_spec } => self.factory.build(
                    (**build_spec).clone(),
                    plan.launch_config.clone(),
                    execution_id,
                ),
                CreateLaunchSource::UnresolvedImage { build_spec } => {
                    self.factory.build_from_image_ref(
                        (**build_spec).clone(),
                        plan.launch_config.clone(),
                        execution_id,
                    )
                }
            },
            LaunchPlan::Resume(plan) => self.factory.build_from_paused_state(
                plan.sandbox_id,
                execution_id,
                plan.paused_state.as_ref(),
                plan.envd_access_token.clone(),
            ),
        };
        build_result.map_err(|source| {
            warn!(error = %format_args!("{source:#}"), "failed to build sandbox");
            OrchestratorError::SandboxOperationFailed {
                sandbox_id: plan.sandbox_id(),
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
        let should_rollback_shared_state = self
            .detach_launch_runtime_if_current(
                &plan.sandbox_id(),
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

    /// The half of a launch rollback that survives losing the id.
    ///
    /// # 🔴 Why this is not simply the rollback above
    ///
    /// When [`detach_launch_runtime_if_current`](Self::detach_launch_runtime_if_current)
    /// refuses, everything this node keys by sandbox id — the handle, the proxy
    /// route, the `StartingSandbox` image pin — describes the *replacement*
    /// launch, and touching any of it would tear down a sandbox somebody else
    /// is still building. That refusal is correct and stays.
    ///
    /// The record is the one thing that is not merely keyed by the id: it is
    /// stamped with the incarnation that wrote it. So this launch can ask for
    /// its own record back without being able to touch a replacement's, and
    /// [`MetadataStore::remove_if_execution`] is where that question is decided
    /// atomically.
    ///
    /// # 🔴 Why it may not be skipped
    ///
    /// A create's record is written in `Creating`, and the state machine has no
    /// edge from `Creating` to `Killing` (`creating_has_no_direct_edge_to_killing`).
    /// A delete therefore waits for `Creating` to end and gives up with
    /// `invalid state Creating` — so a create that stops the VM and leaves its
    /// own record behind leaves one no API call can ever remove. That is the
    /// record an operator had to delete out of Redis by hand.
    ///
    /// A resume is deliberately *not* rescued here. Its record predates the
    /// launch and belongs to the sandbox rather than to this attempt, its
    /// rollback is a state change back to `Paused` rather than a removal, and
    /// the rest of that rollback (the persister's `rollback_resuming`, the
    /// image pin) is keyed by sandbox id and so is exactly what the refusal
    /// above is protecting. Answering that needs a fenced state write, not a
    /// fenced removal.
    async fn reclaim_superseded_launch_record(
        &self,
        plan: &LaunchPlan,
        expected_state: SandboxState,
    ) {
        let LaunchPlan::Create(_) = plan else {
            return;
        };
        let sandbox_id = plan.sandbox_id();
        let execution_id = plan.execution_id();
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
        self.release_image_refs(RuntimeImageOwner::StartingSandbox(plan.sandbox_id()))
            .await;
        match plan {
            LaunchPlan::Create(_) => {
                if let Err(err) = self.store.remove(&plan.sandbox_id()).await {
                    warn!(error = %format_args!("{err:#}"), "failed to remove sandbox metadata during launch rollback");
                }
            }
            LaunchPlan::Resume(_) => {
                if let Err(err) = self
                    .store
                    .update_state_if_state(
                        &plan.sandbox_id(),
                        SandboxState::Paused,
                        std::slice::from_ref(&expected_state),
                    )
                    .await
                {
                    warn!(error = %format_args!("{err:#}"), "failed to restore sandbox metadata during launch rollback");
                }
                if let Err(err) = self.persister.rollback_resuming(&plan.sandbox_id()).await {
                    warn!(error = %format_args!("{err:#}"), "failed to restore persisted sandbox record lifecycle during launch rollback");
                }
            }
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
            // 🔴 Names what is being given up, not only that something was.
            // Everything below is keyed by sandbox id and now describes the
            // replacement, so none of it may be undone from here; the record
            // is handled separately, by
            // [`reclaim_superseded_launch_record`](Self::reclaim_superseded_launch_record),
            // because it names the incarnation that wrote it.
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
        // Restoring a route puts back the incarnation it was published under:
        // this path exists for operations that never started a new run.
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

    /// Logs and counts the one event this half of the fix exists to make
    /// visible: a handle this process cached is being thrown away because it
    /// no longer names the run the metadata store says is authoritative.
    ///
    /// # 🔴 Why this can happen with every stored record agreeing
    ///
    /// A replicated deciding half's handle table is not part of the record
    /// it caches a stub for. Pause a sandbox on replica B, resume it (also on
    /// B, or on any replica) — PG, Redis and the node all move to the new
    /// execution together — and replica A, which never fielded either call,
    /// still holds the [`RemoteSandboxStub`](crate::node_client::stub) it
    /// built when *it* created the sandbox, fenced to the run that no longer
    /// exists. Every store read anyone runs to investigate this will show
    /// three consistent records and nothing wrong at all; the only place the
    /// stale value lives is this table, in this process's memory, which is
    /// exactly why this warning (and the counter behind it) has to exist —
    /// nothing else will ever say so.
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

    /// Looks up the process-local handle for `sandbox_id` without removing
    /// it, discarding it first if it is stale.
    ///
    /// A handle is trusted only when [`SandboxBackend::execution_id`] equals
    /// `expected_execution_id` — a value the caller must already have read
    /// from (or established atomically via) the metadata store, since that
    /// store, not this table, is what "authoritative" means here. A match
    /// returns the handle untouched, still filed in the table, for callers
    /// that go on serving the sandbox from it afterwards (a fork's source, an
    /// in-place network-policy or custom-extension-params update, a running
    /// snapshot capture).
    ///
    /// A mismatch means this table is holding a fencing token for a run the
    /// authoritative record has already moved past — seconds or days out of
    /// date, there is no way to tell from here — and handing it to the node
    /// would only earn a `superseded` refusal no retry on this replica could
    /// ever clear. The stale entry is removed (only if it is still exactly
    /// the entry just read — a concurrent operation may have already
    /// replaced it with something newer, which must not be clobbered) along
    /// with its paired proxy route when that route was published under the
    /// same stale execution, logged and counted via
    /// [`note_stale_handle_discarded`](Self::note_stale_handle_discarded),
    /// and `None` is returned — exactly what a caller sees when nothing was
    /// ever cached, so every call site already knows how to fall through to
    /// [`absent_handle`](Self::absent_handle) from here.
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

    /// [`detach_sandbox_handle_and_route`](Self::detach_sandbox_handle_and_route),
    /// with the same staleness check as
    /// [`cached_handle_for_execution`](Self::cached_handle_for_execution)
    /// applied to whatever it detached.
    ///
    /// For callers — pause, delete — that always take the handle out of the
    /// table up front and decide afterwards whether they end up driving it or
    /// an adopted replacement. A detached handle that turns out to be stale
    /// is reported exactly like [`cached_handle_for_execution`] and then
    /// dropped from the return value entirely: the caller sees `(None, _)`,
    /// identical to what it would have seen had the table never held an
    /// entry for this sandbox, and its existing "fall through to
    /// `absent_handle`" branch takes care of the rest. The paired route comes
    /// back untouched unless it was published under the same stale
    /// execution — routes and handles are always written together, so this
    /// should be the only case in practice, but a route under some other
    /// execution is left for the caller to decide about rather than guessed
    /// away here.
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

        // 🔴 Nothing to preserve when the VMs are not this process's.
        //
        // The loop below reads every non-paused record in the store and pauses
        // it, which is what a machine about to stop running VMs owes the
        // sandboxes on it. A replicated deciding half's store is the *cluster's*
        // ledger, so the same loop there pauses — or, before the handle-absence
        // reading was fixed, deleted the records of — every running sandbox in
        // the cluster, once per replica rolled.
        if self.factory.sandboxes_outlive_this_process() {
            info!(
                "this process runs no sandboxes of its own; leaving the recorded sandboxes to \
                 the machines running them"
            );
            return Ok(());
        }

        // Preserve recoverable sandboxes by pausing running VMs before process exit.
        for pass in 1..=MAX_SHUTDOWN_PASSES {
            let sandboxes = self
                .store
                .list_filtered(SandboxListFilter {
                    states: None,
                    excluded_states: Some(vec![SandboxState::Paused]),
                    user_metadata: None,
                })
                .await?;
            if sandboxes.is_empty() {
                break;
            }
            last_failures.clear();

            info!(
                pass,
                remaining = sandboxes.len(),
                "preserving sandboxes during shutdown"
            );

            for metadata in sandboxes {
                let sandbox_id = metadata.id;
                match metadata.state {
                    SandboxState::Paused => {
                        unreachable!("paused sandboxes should have been filtered out")
                    }
                    SandboxState::Running => {
                        if let Err(err) = self
                            .pause_sandbox_inner(sandbox_id, PausePublication::Here)
                            .await
                        {
                            last_failures.push(format!("{sandbox_id}: {err}"));
                        }
                    }
                    SandboxState::Creating
                    | SandboxState::Snapshotting
                    | SandboxState::Forking
                    | SandboxState::Pausing
                    | SandboxState::Resuming
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
                "shutdown preservation pass completed with failures"
            );
        }

        if !last_failures.is_empty() {
            return Err(OrchestratorError::InternalError(format!(
                "failed to preserve all sandboxes during shutdown after {MAX_SHUTDOWN_PASSES} passes: {}",
                last_failures.join(", ")
            )));
        }

        // Whatever the factory set up process-wide for the sandboxes it
        // builds — on the Firecracker backend, the host network slots — is the
        // factory's to take down. See
        // [`SandboxBackendFactory::release_process_wide_resources`].
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

    /// Whether this node currently refuses to take on new sandboxes.
    pub fn scheduling_disabled(&self) -> bool {
        self.scheduling_disabled.load(Ordering::Acquire)
    }

    /// Isolates this node, or puts it back in rotation. Reports whether the
    /// call changed anything, so callers can stay idempotent without having to
    /// read first.
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

    /// When isolation last changed, or `None` if it never has.
    pub fn scheduling_disabled_changed_at_ms(&self) -> Option<i64> {
        match self
            .scheduling_disabled_changed_at_ms
            .load(Ordering::Acquire)
        {
            0 => None,
            ms => Some(ms),
        }
    }

    /// Guards the paths that would put a new sandbox on this node.
    ///
    /// Deliberately *not* used by paths that act on sandboxes already here —
    /// keep-alive, snapshot, pause, delete — nor by `launch_sandbox`, which
    /// resume shares: a node that is merely isolated must still be able to
    /// bring back a sandbox it alone can recover (see the resume gate in the
    /// API layer, which decides that question where the answer is known).
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
impl<S, F, P> Orchestrator<S, F, P>
where
    S: MetadataStore + 'static,
    F: SandboxBackendFactory,
    P: SandboxPersister + 'static,
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

    pub async fn set_secure_for_test(&self, sandbox_id: &SandboxId, secure: bool) -> Result<()> {
        let Some(mut metadata) = self.store.get(sandbox_id).await? else {
            return Err(OrchestratorError::SandboxNotFound(*sandbox_id));
        };
        metadata.secure = secure;
        self.store.update(metadata).await?;
        Ok(())
    }

    /// Drops this process's handle for a sandbox, leaving its record alone.
    ///
    /// What a replica that never started the sandbox looks like from the
    /// inside, and the only way to produce that shape without standing up a
    /// second replica.
    pub async fn forget_sandbox_handle_for_test(&self, sandbox_id: &SandboxId) -> bool {
        self.sandboxes.write().await.remove(sandbox_id).is_some()
    }

    pub async fn remove_proxy_route_for_test(&self, sandbox_id: &SandboxId) {
        let _ = self.proxy_routes.write().await.remove(sandbox_id);
    }

    /// The incarnation the live backend was built with.
    ///
    /// 🔴 Read off the backend, not the store. Asserting against the store
    /// would only prove that the value written there is the value written
    /// there; this proves the VM was actually started under it.
    pub async fn backend_execution_id_for_test(
        &self,
        sandbox_id: &SandboxId,
    ) -> Option<ExecutionId> {
        let handle = self.sandboxes.read().await.get(sandbox_id).cloned()?;
        let backend = handle.lock().await;
        Some(backend.execution_id())
    }

    /// Seeds a running sandbox whose live incarnation is a chosen value, so the
    /// data plane's ordered comparison can be driven from both sides.
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

/// Everything a create from a committed snapshot needs beyond the catalog row.
///
/// 🔴 A struct rather than ten arguments because it has exactly two callers
/// and they must not drift: `SandboxLaunchSource::Snapshot` and
/// `SandboxLaunchSource::SnapshotRecord` differ only in whether this process
/// resolved the row into local bytes, and everything the sandbox's record says
/// about itself has to come out the same either way. Adding a field here is
/// a compile error in both arms; adding one to a per-arm struct literal would
/// have been a silent divergence in one.
struct SnapshotCreateInputs {
    sandbox_id: SandboxId,
    envd_access_token: Option<EnvdAccessToken>,
    env_vars: Option<HashMap<String, String>>,
    user_metadata: Option<HashMap<String, String>>,
    network_policy: SandboxNetworkPolicy,
    custom_extension_params: Option<CustomExtensionParams>,
    timeout_action: super::SandboxTimeoutAction,
    auto_resume: bool,
    secure: bool,
    control_plane_config: Option<crate::orchestrator::ControlPlaneConfig>,
}

struct SnapshotCreateParts {
    launch_config: SandboxLaunchConfig,
    transitional_metadata: SandboxMetadata,
}

/// Turns one committed snapshot's catalog row into the launch config and
/// transitional record a create from it starts with.
///
/// Reads nothing but the row: every value here comes from `record` or from the
/// caller's request, which is what lets the unresolved arm produce the same
/// record as the resolved one.
fn snapshot_create_parts(
    record: &crate::snapshot::SnapshotRecord,
    inputs: SnapshotCreateInputs,
) -> Result<SnapshotCreateParts> {
    let SnapshotCreateInputs {
        sandbox_id,
        envd_access_token,
        env_vars,
        user_metadata,
        network_policy,
        custom_extension_params,
        timeout_action,
        auto_resume,
        secure,
        control_plane_config,
    } = inputs;

    // 🔴 Refused rather than unwrapped. `RunnableSnapshot::committed` may
    // `expect` here because resolving a row without a committed payload fails
    // before a `RunnableSnapshot` exists; this function is also reached with a
    // row nothing has resolved, so the same absence has to be an answer rather
    // than a panic — and a 400, because a caller naming a template that is
    // still building is a caller asking for something that cannot be built.
    let Some(committed) = record.committed.as_ref() else {
        return Err(OrchestratorError::InvalidRequest(format!(
            "snapshot {} is not ready to launch from: it has no committed artifacts",
            record.id
        )));
    };

    let configured_mode = ConfigManager::global_config().virtualization_mode;
    if committed.virtualization_mode != configured_mode {
        return Err(OrchestratorError::VirtualizationModeMismatch {
            resource: format!("snapshot {}", record.id),
            resource_mode: committed.virtualization_mode,
            node_mode: configured_mode,
        });
    }
    let launch_image_configs = committed.image_configs.clone();
    let mut extra_mmds = serde_json::Map::new();
    if !launch_image_configs.is_empty() {
        extra_mmds.insert("imageConfigs".to_string(), launch_image_configs.to_value());
    };
    // Effective custom config: a launch-provided value overrides the
    // one persisted in the source snapshot; otherwise inherit it.
    // Store the effective value so publishing a snapshot from this
    // sandbox keeps the inherited config instead of dropping it.
    let effective_custom_extension_params =
        custom_extension_params.or_else(|| committed.custom_extension_params.clone());
    let launch_config = SandboxLaunchConfig {
        sandbox_id,
        snapshot_id: record.id.to_string(),
        env_vars,
        network: network_policy.runtime_policy(),
        extra_mmds,
        custom_extension_params: effective_custom_extension_params.clone(),
        envd_access_token,
        // Filled in by `stamp_control_plane_ownership` once the
        // record this marker encodes is complete; see there.
        control_plane_config: None,
    };

    let transitional_metadata = SandboxMetadata {
        id: sandbox_id,
        snapshot_id: record.id.to_string(),
        snapshot_alias: record.alias.as_ref().map(ToString::to_string),
        virtualization_mode: committed.virtualization_mode,
        runtime_versions: committed.runtime_versions.clone(),
        resources: record.resources,
        context: committed.context.clone(),
        startup: committed.startup.clone(),
        image_configs: launch_image_configs,
        timeout_action,
        auto_resume,
        user_metadata,
        network_policy,
        custom_extension_params: effective_custom_extension_params,
        secure,
        control_plane_config,
        max_lifetime: configured_max_sandbox_lifetime(),
        ..Default::default()
    };

    Ok(SnapshotCreateParts {
        launch_config,
        transitional_metadata,
    })
}

fn default_fresh_sandbox_resources() -> SandboxResources {
    let config = ConfigManager::global_config();
    SandboxResources {
        cpu_count: config.machine.vcpu_count,
        memory_mib: config.machine.mem_size_mib,
        // Filled from backend runtime info after the rootfs device is created.
        disk_size_mib: 0,
    }
}

fn resources_with_runtime_info(
    mut resources: SandboxResources,
    runtime_info: SandboxRuntimeInfo,
) -> SandboxResources {
    // This API resource field tracks the rootfs block device size. Attached
    // drives are separately configured storage and are not folded into it.
    if let Some(size) = runtime_info.rootfs_virtual_size {
        resources.disk_size_mib = bytes_to_mib_ceil(size);
    }
    resources
}

fn configured_runtime_versions() -> SnapshotRuntimeVersions {
    let config = ConfigManager::global_config();
    SnapshotRuntimeVersions::new(
        config
            .kernel
            .version
            .clone()
            .unwrap_or_else(|| "unknown".to_string()),
        config
            .firecracker
            .version
            .clone()
            .unwrap_or_else(|| "unknown".to_string()),
        config.envd.version.clone(),
        config.resolved_tools_version().to_string(),
    )
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
