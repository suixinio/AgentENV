use std::collections::{HashMap, VecDeque};
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::result::Result as StdResult;
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::time::SystemTime;

use async_trait::async_trait;
use serde_json::json;
use tempfile::TempDir;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};
use uuid::Uuid;

use super::super::launch_plan::LaunchPlan;
use super::super::persistence::{
    DisabledSandboxPersister, RecordingCall, RecordingPersister, SandboxPersister,
};
use super::super::types::SandboxLaunchSource;
use super::*;
use crate::cfg::ResolvedImageCacheConfig;
use crate::image::cache::test_support::{
    test_local_image_services_from_service, ImageCacheService, RecordingRuntimeImageRefs,
};
use crate::image::cache::{
    local_image_services_from_global_config, RuntimeImageOwner, RuntimeImageRefs,
};
use crate::sandbox::mock::{
    MockAction, MockBackendFactory, MockBehavior, MockOperation, MockSandboxBackend, MockSnapshot,
};
use crate::sandbox::{
    BaseSandboxNetworkPolicy, PausedSandboxState, RuntimeArtifactSet, SandboxLaunchConfig,
    SandboxNetworkEgressPolicy, SandboxNetworkPolicy, SandboxRuntimeInfo,
};
use crate::snapshot::RunnableSnapshot;
use crate::types::{ImageConfigs, SandboxId, SandboxResources};

const STATE_POLL_INTERVAL: Duration = Duration::from_millis(50);

type TestOrchestrator<
    S = InMemoryMetadataStore,
    F = MockBackendFactory,
    P = DisabledSandboxPersister,
> = Orchestrator<S, F, P>;

fn setup() {
    crate::logging::init_for_tests();
}

fn test_runtime_image_refs() -> Arc<dyn RuntimeImageRefs> {
    local_image_services_from_global_config().runtime_refs
}

async fn make_orchestrator() -> Arc<TestOrchestrator> {
    Orchestrator::new_inner(
        ServerRole::All,
        InMemoryMetadataStore::new(),
        MockBackendFactory::new(),
        DisabledSandboxPersister,
        test_runtime_image_refs(),
    )
    .await
    .expect("in-memory orchestrator should not fail to construct")
}

async fn make_orchestrator_with_factory(factory: MockBackendFactory) -> Arc<TestOrchestrator> {
    Orchestrator::new_inner(
        ServerRole::All,
        InMemoryMetadataStore::new(),
        factory,
        DisabledSandboxPersister,
        test_runtime_image_refs(),
    )
    .await
    .expect("in-memory orchestrator should not fail to construct")
}

fn make_orchestrator_without_background<S: MetadataStore + 'static>(
    store: S,
) -> Arc<TestOrchestrator<S>> {
    make_orchestrator_without_background_with_factory(store, MockBackendFactory::new())
}

fn make_orchestrator_without_background_with_factory<
    S: MetadataStore + 'static,
    F: SandboxBackendFactory,
>(
    store: S,
    factory: F,
) -> Arc<TestOrchestrator<S, F>> {
    make_orchestrator_without_background_with_factory_and_persister(
        store,
        factory,
        DisabledSandboxPersister,
    )
}

fn make_orchestrator_without_background_with_factory_and_persister<
    S: MetadataStore + 'static,
    F: SandboxBackendFactory,
    P: SandboxPersister + 'static,
>(
    store: S,
    factory: F,
    persister: P,
) -> Arc<TestOrchestrator<S, F, P>> {
    let (sandbox_event_tx, _sandbox_event_rx) =
        tokio::sync::broadcast::channel(SANDBOX_EVENT_CHANNEL_CAPACITY);
    Arc::new(Orchestrator {
        store,
        factory,
        persister,
        sandboxes: RwLock::new(HashMap::new()),
        proxy_routes: RwLock::new(ProxyRouteTable::default()),
        next_proxy_route_version: AtomicU64::new(1),
        counters: Default::default(),
        sandbox_event_tx,
        default_sandbox_timeout: Duration::from_secs(15),
        is_shutting_down: std::sync::atomic::AtomicBool::new(false),
        scheduling_disabled: std::sync::atomic::AtomicBool::new(false),
        scheduling_disabled_changed_at_ms: std::sync::atomic::AtomicI64::new(0),
        shutdown_tx: tokio::sync::watch::channel(false).0,
        shutdown_outcome: tokio::sync::OnceCell::new(),
        image_refs: test_runtime_image_refs(),
        access_tokens: SandboxAccessTokenGenerator::new("orchestrator-test-seed").unwrap(),
        paused_publisher: tokio::sync::OnceCell::new(),
    })
}

enum StoreAction {
    Delegate,
    Fail(StoreError),
}

type StoreHook = Arc<dyn Fn(&SandboxMetadata) + Send + Sync>;
type StoreHookSlot = StdMutex<Option<StoreHook>>;

#[derive(Default)]
struct ScriptedStoreControl {
    add_actions: StdMutex<VecDeque<StoreAction>>,
    update_if_state_actions: StdMutex<VecDeque<StoreAction>>,
    on_add: StoreHookSlot,
}

impl ScriptedStoreControl {
    fn push_add_action(&self, action: StoreAction) {
        self.add_actions
            .lock()
            .expect("add actions mutex poisoned")
            .push_back(action);
    }

    fn push_update_if_state_action(&self, action: StoreAction) {
        self.update_if_state_actions
            .lock()
            .expect("update_if_state actions mutex poisoned")
            .push_back(action);
    }

    fn set_on_add(&self, hook: StoreHook) {
        *self.on_add.lock().expect("on_add mutex poisoned") = Some(hook);
    }

    fn take_action(queue: &StdMutex<VecDeque<StoreAction>>) -> StoreAction {
        queue
            .lock()
            .expect("store action mutex poisoned")
            .pop_front()
            .unwrap_or(StoreAction::Delegate)
    }

    fn run_hook(slot: &StoreHookSlot, metadata: &SandboxMetadata) {
        if let Some(hook) = slot.lock().expect("store hook mutex poisoned").clone() {
            hook(metadata);
        }
    }
}

struct ScriptedStore {
    inner: InMemoryMetadataStore,
    control: Arc<ScriptedStoreControl>,
}

impl ScriptedStore {
    fn new(control: Arc<ScriptedStoreControl>) -> Self {
        Self {
            inner: InMemoryMetadataStore::new(),
            control,
        }
    }
}

#[async_trait]
impl MetadataStore for ScriptedStore {
    async fn add(&self, metadata: SandboxMetadata) -> StdResult<(), StoreError> {
        ScriptedStoreControl::run_hook(&self.control.on_add, &metadata);
        match ScriptedStoreControl::take_action(&self.control.add_actions) {
            StoreAction::Delegate => self.inner.add(metadata).await,
            StoreAction::Fail(err) => Err(err),
        }
    }

    async fn update(&self, metadata: SandboxMetadata) -> StdResult<(), StoreError> {
        self.inner.update(metadata).await
    }

    async fn update_state_if_state(
        &self,
        sandbox_id: &SandboxId,
        new_state: SandboxState,
        expected_states: &[SandboxState],
    ) -> StdResult<SandboxState, StoreError> {
        match ScriptedStoreControl::take_action(&self.control.update_if_state_actions) {
            StoreAction::Delegate => {
                self.inner
                    .update_state_if_state(sandbox_id, new_state, expected_states)
                    .await
            }
            StoreAction::Fail(err) => Err(err),
        }
    }

    async fn update_if_state<F>(
        &self,
        sandbox_id: &SandboxId,
        expected_states: &[SandboxState],
        update: F,
    ) -> StdResult<MetadataUpdateResult, StoreError>
    where
        F: FnOnce(&mut SandboxMetadata) + Send,
    {
        match ScriptedStoreControl::take_action(&self.control.update_if_state_actions) {
            StoreAction::Delegate => {
                self.inner
                    .update_if_state(sandbox_id, expected_states, update)
                    .await
            }
            StoreAction::Fail(err) => Err(err),
        }
    }

    async fn get(&self, sandbox_id: &SandboxId) -> StdResult<Option<SandboxMetadata>, StoreError> {
        self.inner.get(sandbox_id).await
    }

    async fn remove(
        &self,
        sandbox_id: &SandboxId,
    ) -> StdResult<Option<SandboxMetadata>, StoreError> {
        self.inner.remove(sandbox_id).await
    }

    async fn list(&self) -> StdResult<Vec<SandboxMetadata>, StoreError> {
        self.inner.list().await
    }

    async fn list_with_callback<F>(&self, callback: F) -> StdResult<(), StoreError>
    where
        F: FnMut(&SandboxMetadata) + Send,
    {
        self.inner.list_with_callback(callback).await
    }

    async fn list_filtered(
        &self,
        filter: SandboxListFilter,
    ) -> StdResult<Vec<SandboxMetadata>, StoreError> {
        self.inner.list_filtered(filter).await
    }

    async fn list_expired(
        &self,
        now: std::time::SystemTime,
    ) -> StdResult<Vec<SandboxMetadata>, StoreError> {
        self.inner.list_expired(now).await
    }

    async fn list_ids(&self) -> StdResult<Vec<SandboxId>, StoreError> {
        self.inner.list_ids().await
    }

    async fn wait_while_in_states(
        &self,
        sandbox_id: &SandboxId,
        transitional_states: &[SandboxState],
    ) -> StdResult<Option<SandboxMetadata>, StoreError> {
        self.inner
            .wait_while_in_states(sandbox_id, transitional_states)
            .await
    }
}

enum WaitScript {
    Return(StdResult<Option<SandboxMetadata>, StoreError>),
}

struct ScriptedWaitStore {
    next_wait: Mutex<Option<WaitScript>>,
}

impl ScriptedWaitStore {
    fn with_wait(script: WaitScript) -> Self {
        Self {
            next_wait: Mutex::new(Some(script)),
        }
    }
}

struct ConflictOnUpdateStore {
    metadata: Mutex<Option<SandboxMetadata>>,
}

impl ConflictOnUpdateStore {
    fn new(metadata: SandboxMetadata) -> Self {
        Self {
            metadata: Mutex::new(Some(metadata)),
        }
    }
}

struct RaceBeforeUpdateStore {
    inner: InMemoryMetadataStore,
    injected: Mutex<bool>,
    injected_timeout: Duration,
}

impl RaceBeforeUpdateStore {
    fn new(injected_timeout: Duration) -> Self {
        Self {
            inner: InMemoryMetadataStore::new(),
            injected: Mutex::new(false),
            injected_timeout,
        }
    }
}

#[async_trait::async_trait]
impl MetadataStore for RaceBeforeUpdateStore {
    async fn add(&self, metadata: SandboxMetadata) -> StdResult<(), StoreError> {
        self.inner.add(metadata).await
    }

    async fn update(&self, metadata: SandboxMetadata) -> StdResult<(), StoreError> {
        self.inner.update(metadata).await
    }

    async fn update_state_if_state(
        &self,
        sandbox_id: &SandboxId,
        new_state: SandboxState,
        expected_states: &[SandboxState],
    ) -> StdResult<SandboxState, StoreError> {
        self.inner
            .update_state_if_state(sandbox_id, new_state, expected_states)
            .await
    }

    async fn update_if_state<F>(
        &self,
        sandbox_id: &SandboxId,
        expected_states: &[SandboxState],
        update: F,
    ) -> StdResult<MetadataUpdateResult, StoreError>
    where
        F: FnOnce(&mut SandboxMetadata) + Send,
    {
        let should_inject = {
            let mut injected = self.injected.lock().await;
            if *injected {
                false
            } else {
                *injected = true;
                true
            }
        };

        if should_inject {
            let injected_timeout = self.injected_timeout;
            self.inner
                .update_if_state(sandbox_id, expected_states, |metadata| {
                    metadata.set_timeout(Some(injected_timeout));
                })
                .await?;
        }

        self.inner
            .update_if_state(sandbox_id, expected_states, update)
            .await
    }

    async fn get(&self, sandbox_id: &SandboxId) -> StdResult<Option<SandboxMetadata>, StoreError> {
        self.inner.get(sandbox_id).await
    }

    async fn remove(
        &self,
        sandbox_id: &SandboxId,
    ) -> StdResult<Option<SandboxMetadata>, StoreError> {
        self.inner.remove(sandbox_id).await
    }

    async fn list_ids(&self) -> StdResult<Vec<SandboxId>, StoreError> {
        self.inner.list_ids().await
    }

    async fn list(&self) -> StdResult<Vec<SandboxMetadata>, StoreError> {
        self.inner.list().await
    }

    async fn list_with_callback<F>(&self, callback: F) -> StdResult<(), StoreError>
    where
        F: FnMut(&SandboxMetadata) + Send,
    {
        self.inner.list_with_callback(callback).await
    }

    async fn list_filtered(
        &self,
        filter: SandboxListFilter,
    ) -> StdResult<Vec<SandboxMetadata>, StoreError> {
        self.inner.list_filtered(filter).await
    }

    async fn list_expired(
        &self,
        now: std::time::SystemTime,
    ) -> StdResult<Vec<SandboxMetadata>, StoreError> {
        self.inner.list_expired(now).await
    }

    async fn wait_while_in_states(
        &self,
        sandbox_id: &SandboxId,
        transitional_states: &[SandboxState],
    ) -> StdResult<Option<SandboxMetadata>, StoreError> {
        self.inner
            .wait_while_in_states(sandbox_id, transitional_states)
            .await
    }
}

#[async_trait::async_trait]
impl MetadataStore for ConflictOnUpdateStore {
    async fn add(&self, metadata: SandboxMetadata) -> StdResult<(), StoreError> {
        *self.metadata.lock().await = Some(metadata);
        Ok(())
    }

    async fn update(&self, metadata: SandboxMetadata) -> StdResult<(), StoreError> {
        *self.metadata.lock().await = Some(metadata);
        Ok(())
    }

    async fn update_state_if_state(
        &self,
        _sandbox_id: &SandboxId,
        _new_state: SandboxState,
        _expected_states: &[SandboxState],
    ) -> StdResult<SandboxState, StoreError> {
        Err(StoreError::Backend {
            source: anyhow::anyhow!("unused in conflict test store"),
        })
    }

    async fn update_if_state<F>(
        &self,
        sandbox_id: &SandboxId,
        expected_states: &[SandboxState],
        _update: F,
    ) -> StdResult<MetadataUpdateResult, StoreError>
    where
        F: FnOnce(&mut SandboxMetadata) + Send,
    {
        let current =
            self.metadata
                .lock()
                .await
                .clone()
                .ok_or_else(|| StoreError::SandboxNotFound {
                    sandbox_id: *sandbox_id,
                })?;

        if !expected_states.contains(&current.state) {
            return Err(StoreError::StateConflict {
                sandbox_id: *sandbox_id,
                expected_states: expected_states.to_vec(),
                actual_state: current.state,
            });
        }

        Err(StoreError::StateConflict {
            sandbox_id: *sandbox_id,
            expected_states: expected_states.to_vec(),
            actual_state: SandboxState::Paused,
        })
    }

    async fn get(&self, _sandbox_id: &SandboxId) -> StdResult<Option<SandboxMetadata>, StoreError> {
        Ok(self.metadata.lock().await.clone())
    }

    async fn remove(
        &self,
        _sandbox_id: &SandboxId,
    ) -> StdResult<Option<SandboxMetadata>, StoreError> {
        Ok(self.metadata.lock().await.take())
    }

    async fn list_ids(&self) -> StdResult<Vec<SandboxId>, StoreError> {
        Ok(self
            .metadata
            .lock()
            .await
            .clone()
            .into_iter()
            .map(|metadata| metadata.id)
            .collect())
    }

    async fn list(&self) -> StdResult<Vec<SandboxMetadata>, StoreError> {
        Ok(self.metadata.lock().await.clone().into_iter().collect())
    }

    async fn list_with_callback<F>(&self, mut callback: F) -> StdResult<(), StoreError>
    where
        F: FnMut(&SandboxMetadata) + Send,
    {
        if let Some(metadata) = self.metadata.lock().await.as_ref() {
            callback(metadata);
        }
        Ok(())
    }

    async fn list_filtered(
        &self,
        _filter: SandboxListFilter,
    ) -> StdResult<Vec<SandboxMetadata>, StoreError> {
        Ok(self.metadata.lock().await.clone().into_iter().collect())
    }

    async fn list_expired(
        &self,
        _now: std::time::SystemTime,
    ) -> StdResult<Vec<SandboxMetadata>, StoreError> {
        Ok(vec![])
    }

    async fn wait_while_in_states(
        &self,
        _sandbox_id: &SandboxId,
        _transitional_states: &[SandboxState],
    ) -> StdResult<Option<SandboxMetadata>, StoreError> {
        Ok(self.metadata.lock().await.clone())
    }
}

#[async_trait::async_trait]
impl MetadataStore for ScriptedWaitStore {
    async fn add(&self, _metadata: SandboxMetadata) -> StdResult<(), StoreError> {
        Ok(())
    }

    async fn update(&self, _metadata: SandboxMetadata) -> StdResult<(), StoreError> {
        Ok(())
    }

    async fn update_state_if_state(
        &self,
        _sandbox_id: &SandboxId,
        _new_state: SandboxState,
        _expected_states: &[SandboxState],
    ) -> StdResult<SandboxState, StoreError> {
        Err(StoreError::Backend {
            source: anyhow::anyhow!("unused in this test store"),
        })
    }

    async fn update_if_state<F>(
        &self,
        _sandbox_id: &SandboxId,
        _expected_states: &[SandboxState],
        _update: F,
    ) -> StdResult<MetadataUpdateResult, StoreError>
    where
        F: FnOnce(&mut SandboxMetadata) + Send,
    {
        Err(StoreError::Backend {
            source: anyhow::anyhow!("unused in this test store"),
        })
    }

    async fn get(&self, _sandbox_id: &SandboxId) -> StdResult<Option<SandboxMetadata>, StoreError> {
        Ok(None)
    }

    async fn remove(
        &self,
        _sandbox_id: &SandboxId,
    ) -> StdResult<Option<SandboxMetadata>, StoreError> {
        Ok(None)
    }

    async fn list_ids(&self) -> StdResult<Vec<SandboxId>, StoreError> {
        Ok(vec![])
    }

    async fn list(&self) -> StdResult<Vec<SandboxMetadata>, StoreError> {
        Ok(vec![])
    }

    async fn list_with_callback<F>(&self, _callback: F) -> StdResult<(), StoreError>
    where
        F: FnMut(&SandboxMetadata) + Send,
    {
        Ok(())
    }

    async fn list_filtered(
        &self,
        _filter: SandboxListFilter,
    ) -> StdResult<Vec<SandboxMetadata>, StoreError> {
        Ok(vec![])
    }

    async fn list_expired(
        &self,
        _now: std::time::SystemTime,
    ) -> StdResult<Vec<SandboxMetadata>, StoreError> {
        Ok(vec![])
    }

    async fn wait_while_in_states(
        &self,
        _sandbox_id: &SandboxId,
        _transitional_states: &[SandboxState],
    ) -> StdResult<Option<SandboxMetadata>, StoreError> {
        let script = self
            .next_wait
            .lock()
            .await
            .take()
            .unwrap_or(WaitScript::Return(Ok(None)));
        match script {
            WaitScript::Return(result) => result,
        }
    }
}

#[tokio::test]
async fn with_in_memory_store_constructs_without_panic() {
    setup();
    let orchestrator = Orchestrator::with_in_memory_store().await;
    drop(orchestrator);
}

#[tokio::test]
async fn new_loads_persisted_sandboxes_into_store() -> Result<()> {
    setup();
    let sandbox_id = SandboxId::new();
    let mut paused = paused_resume_metadata(sandbox_id);
    paused.auto_resume = true;
    let persister = RecordingPersister::with_loaded(vec![paused.clone()]);

    let orchestrator = Orchestrator::new(
        ServerRole::All,
        InMemoryMetadataStore::new(),
        MockBackendFactory::new(),
        persister.clone(),
    )
    .await?;

    assert_eq!(persister.calls(), vec![RecordingCall::LoadAll]);
    let restored = orchestrator
        .get_sandbox(&sandbox_id)
        .await?
        .expect("persisted paused sandbox should be restored into metadata store");
    assert_eq!(restored.state, SandboxState::Paused);
    assert!(restored.paused_state.is_some());
    assert_eq!(
        orchestrator.proxy_lookup_for(&sandbox_id).await?,
        ProxyLookupResult::Paused { auto_resume: true }
    );
    assert_metrics_values(&orchestrator, 0, 0, 0, 0, 0, 0).await;
    Ok(())
}

/// 🔴 The startup ordering the routing projection's TTL now rests on.
///
/// Heartbeat reconciliation deletes every binding a node owns when that node
/// reports an empty roster — that is what makes a node's disappearance clear
/// its records instead of leaving them pointing at nothing. It also means a
/// node that heartbeats *before* it has finished restoring its persisted
/// sandboxes would wipe its own routing records on every restart, and would do
/// it quietly: the records come back on the following heartbeat, so all anyone
/// sees is a few seconds of 404s that look like a cold cache.
///
/// The ordering that prevents it is that `Orchestrator::new` finishes the
/// restore before it returns, and `src/bin/server.rs` starts the reporter after
/// that await. Nothing else pins it, so this does.
#[tokio::test]
async fn the_roster_is_complete_the_moment_new_returns() -> Result<()> {
    setup();
    let sandbox_id = SandboxId::new();
    let paused = paused_resume_metadata(sandbox_id);
    let persister = RecordingPersister::with_loaded(vec![paused.clone()]);

    let orchestrator = Orchestrator::new(
        ServerRole::All,
        InMemoryMetadataStore::new(),
        MockBackendFactory::new(),
        persister,
    )
    .await?;

    // No await in between, and no background task to wait for: whatever the
    // first heartbeat would carry is already here.
    let roster = orchestrator.list_sandbox_roster().await?;

    assert_eq!(
        roster.len(),
        1,
        "a restored sandbox must be in the roster before anything can report an empty one"
    );
    assert_eq!(roster[0].sandbox_id, sandbox_id);
    assert_eq!(roster[0].execution_id, paused.execution_id);
    Ok(())
}

#[tokio::test]
async fn new_returns_error_when_loading_persisted_sandboxes_fails() {
    setup();
    let persister = RecordingPersister::default();
    persister.fail_next(RecordingCall::LoadAll);

    let result = Orchestrator::new(
        ServerRole::All,
        InMemoryMetadataStore::new(),
        MockBackendFactory::new(),
        persister.clone(),
    )
    .await;

    let err = match result {
        Ok(_) => panic!("orchestrator construction should fail when persister load fails"),
        Err(err) => err,
    };
    assert!(matches!(
        err,
        OrchestratorError::SandboxPersistenceFailed(_)
    ));
    assert_eq!(persister.calls(), vec![RecordingCall::LoadAll]);
}

fn test_paused_state() -> &'static Arc<dyn PausedSandboxState> {
    static STATE: OnceLock<Arc<dyn PausedSandboxState>> = OnceLock::new();
    STATE.get_or_init(|| Arc::new(MockSnapshot))
}

fn test_runnable_snapshot() -> &'static RunnableSnapshot {
    static SNAPSHOT: OnceLock<RunnableSnapshot> = OnceLock::new();
    SNAPSHOT.get_or_init(RunnableSnapshot::mock)
}

fn create_launch_plan_with_resources(sandbox_id: SandboxId) -> LaunchPlan {
    let transitional_metadata = SandboxMetadata {
        id: sandbox_id,
        state: SandboxState::Creating,
        ..Default::default()
    };
    LaunchPlan::for_create_from_snapshot(
        sandbox_id,
        Box::new(test_runnable_snapshot().clone()),
        SandboxLaunchConfig::default(),
        transitional_metadata,
        NewTimeout::Set(Duration::from_secs(15)),
        None,
    )
}

/// A claim token for tests that drive the orchestrator directly.
///
/// Goes through the same constructor the arbitration uses, so these tests
/// exercise the real shape rather than a test-only door.
fn test_claim() -> ClaimedExecution {
    ClaimedExecution::from_claim(ExecutionId::new())
}

fn resume_launch_plan(sandbox_id: SandboxId) -> LaunchPlan {
    LaunchPlan::for_resume(
        sandbox_id,
        test_claim(),
        Arc::clone(test_paused_state()),
        NewTimeout::None,
        SandboxResources::default(),
        None,
    )
}

fn paused_resume_metadata(sandbox_id: SandboxId) -> SandboxMetadata {
    SandboxMetadata {
        id: sandbox_id,
        state: SandboxState::Paused,
        paused_state: Some(test_paused_state().clone()),
        ..Default::default()
    }
}

/// The expected `OrchestratorMetrics` snapshot for a state in which a single
/// `SandboxMetadata::default()`-shaped paused sandbox is the only entry in the
/// store: no active running / starting contributions, and one Paused sandbox
/// contributing its CPU / memory to the `paused_*` fields.
fn expected_metrics_with_one_paused_default() -> OrchestratorMetrics {
    let resources = SandboxResources::default();
    OrchestratorMetrics {
        paused_sandbox_count: 1,
        paused_allocated_cpu: resources.cpu_count,
        paused_allocated_memory_bytes: u64::from(resources.memory_mib) * 1024 * 1024,
        ..OrchestratorMetrics::default()
    }
}

async fn proxy_target_for<S, F, P>(
    orchestrator: &TestOrchestrator<S, F, P>,
    sandbox_id: &SandboxId,
) -> Result<Option<ProxyTarget>>
where
    S: MetadataStore + 'static,
    F: SandboxBackendFactory,
    P: SandboxPersister + 'static,
{
    Ok(match orchestrator.proxy_lookup_for(sandbox_id).await? {
        ProxyLookupResult::Ready(target) => Some(target),
        ProxyLookupResult::NotFound
        | ProxyLookupResult::Paused { .. }
        | ProxyLookupResult::Unavailable(_)
        | ProxyLookupResult::RouteMissing => None,
    })
}

async fn current_metrics<S, F, P>(orchestrator: &TestOrchestrator<S, F, P>) -> OrchestratorMetrics
where
    S: MetadataStore + 'static,
    F: SandboxBackendFactory,
    P: SandboxPersister + 'static,
{
    orchestrator
        .metrics_snapshot()
        .await
        .expect("metrics snapshot succeeds")
}

async fn assert_metrics_values<S, F, P>(
    orchestrator: &TestOrchestrator<S, F, P>,
    create_successes: u64,
    create_fails: u64,
    running_sandbox_count: u32,
    starting_sandbox_count: u32,
    allocated_cpu: u32,
    allocated_memory_mib: u32,
) where
    S: MetadataStore + 'static,
    F: SandboxBackendFactory,
    P: SandboxPersister + 'static,
{
    let metrics = current_metrics(orchestrator).await;
    assert_eq!(metrics.create_successes, create_successes);
    assert_eq!(metrics.create_fails, create_fails);
    assert_eq!(metrics.running_sandbox_count, running_sandbox_count);
    assert_eq!(metrics.starting_sandbox_count, starting_sandbox_count);
    assert_eq!(metrics.allocated_cpu, allocated_cpu);
    assert_eq!(
        metrics.allocated_memory_bytes,
        u64::from(allocated_memory_mib) * 1024 * 1024
    );
}

async fn assert_metrics_snapshot<S, F, P>(
    orchestrator: &TestOrchestrator<S, F, P>,
    expected: &OrchestratorMetrics,
) where
    S: MetadataStore + 'static,
    F: SandboxBackendFactory,
    P: SandboxPersister + 'static,
{
    assert_eq!(&current_metrics(orchestrator).await, expected);
}

#[tokio::test]
async fn proxy_target_for_only_returns_running_routes() {
    let orchestrator = make_orchestrator().await;
    let sandbox_id = SandboxId::new();
    let target = ProxyTarget::new(Ipv4Addr::new(10, 11, 0, 42));

    orchestrator
        .upsert_proxy_route(sandbox_id, target.clone(), ExecutionId::new())
        .await;
    assert_eq!(
        proxy_target_for(&orchestrator, &sandbox_id).await.unwrap(),
        Some(target)
    );
}

#[tokio::test]
async fn restore_proxy_route_republishes_running_target() {
    let orchestrator = make_orchestrator().await;
    let sandbox_id = SandboxId::new();
    let target = ProxyTarget::new(Ipv4Addr::new(10, 11, 0, 77));

    orchestrator
        .upsert_proxy_route(sandbox_id, target.clone(), ExecutionId::new())
        .await;

    let (_, removed) = orchestrator
        .detach_sandbox_handle_and_route(&sandbox_id)
        .await;
    assert!(proxy_target_for(&orchestrator, &sandbox_id)
        .await
        .unwrap()
        .is_none());

    orchestrator.restore_proxy_route(sandbox_id, removed).await;
    assert_eq!(
        proxy_target_for(&orchestrator, &sandbox_id).await.unwrap(),
        Some(target)
    );
}

#[tokio::test]
async fn proxy_lookup_reports_route_missing_for_running_metadata_without_route() {
    let orchestrator = make_orchestrator().await;
    let sandbox_id = SandboxId::new();

    orchestrator
        .set_metadata_state_for_test(sandbox_id, SandboxState::Running)
        .await
        .unwrap();

    assert_eq!(
        orchestrator.proxy_lookup_for(&sandbox_id).await.unwrap(),
        ProxyLookupResult::RouteMissing
    );
}

#[tokio::test]
async fn proxy_lookup_reports_paused_for_paused_sandbox() {
    let orchestrator = make_orchestrator().await;
    let sandbox_id = SandboxId::new();

    orchestrator
        .set_metadata_state_for_test(sandbox_id, SandboxState::Paused)
        .await
        .unwrap();
    assert_eq!(
        orchestrator.proxy_lookup_for(&sandbox_id).await.unwrap(),
        ProxyLookupResult::Paused { auto_resume: false }
    );

    orchestrator
        .set_auto_resume_for_test(&sandbox_id, true)
        .await
        .unwrap();
    assert_eq!(
        orchestrator.proxy_lookup_for(&sandbox_id).await.unwrap(),
        ProxyLookupResult::Paused { auto_resume: true }
    );
}

#[tokio::test]
async fn cleanup_failed_launch_removes_created_running_metadata() {
    let orchestrator = make_orchestrator().await;
    let sandbox_id = SandboxId::new();
    let plan = create_launch_plan_with_resources(sandbox_id);
    let handle: SandboxHandle = Arc::new(Mutex::new(Box::new(MockSandboxBackend::new(
        Arc::new(MockBehavior::new()),
        ExecutionId::new(),
    ))));

    orchestrator
        .store
        .add(SandboxMetadata {
            id: sandbox_id,
            state: SandboxState::Running,
            ..Default::default()
        })
        .await
        .unwrap();

    orchestrator
        .cleanup_failed_launch(&plan, handle, FailedLaunchStage::RunningPersisted)
        .await;

    assert!(orchestrator.store.get(&sandbox_id).await.unwrap().is_none());
}

#[tokio::test]
async fn cleanup_failed_launch_restores_resume_metadata() {
    let orchestrator = make_orchestrator().await;
    let sandbox_id = SandboxId::new();
    let rollback_metadata = paused_resume_metadata(sandbox_id);
    let mut running_metadata = rollback_metadata.clone();
    running_metadata.state = SandboxState::Running;
    let plan = resume_launch_plan(sandbox_id);
    let handle: SandboxHandle = Arc::new(Mutex::new(Box::new(MockSandboxBackend::new(
        Arc::new(MockBehavior::new()),
        ExecutionId::new(),
    ))));

    orchestrator.store.add(running_metadata).await.unwrap();

    orchestrator
        .cleanup_failed_launch(&plan, handle, FailedLaunchStage::RunningPersisted)
        .await;

    assert_eq!(
        orchestrator
            .store
            .get(&sandbox_id)
            .await
            .unwrap()
            .unwrap()
            .state,
        SandboxState::Paused
    );
}

#[tokio::test]
async fn stale_handle_cannot_republish_running_proxy_route() {
    let orchestrator = make_orchestrator().await;
    let sandbox_id = SandboxId::new();
    let behavior = Arc::new(MockBehavior::new());
    let current_handle: SandboxHandle = Arc::new(Mutex::new(Box::new(MockSandboxBackend::new(
        behavior.clone(),
        ExecutionId::new(),
    ))));
    let stale_handle: SandboxHandle = Arc::new(Mutex::new(Box::new(MockSandboxBackend::new(
        behavior.clone(),
        ExecutionId::new(),
    ))));

    orchestrator
        .sandboxes
        .write()
        .await
        .insert(sandbox_id, current_handle);

    let published = orchestrator
        .upsert_proxy_route_if_current_handle(
            sandbox_id,
            &stale_handle,
            ProxyTarget::new(Ipv4Addr::new(10, 11, 0, 99)),
            ExecutionId::new(),
        )
        .await;

    assert!(!published);
    assert!(proxy_target_for(&orchestrator, &sandbox_id)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn cleanup_failed_launch_does_not_remove_replacement_runtime_state() {
    let orchestrator = make_orchestrator().await;
    let sandbox_id = SandboxId::new();
    let plan = create_launch_plan_with_resources(sandbox_id);
    let stale_handle: SandboxHandle = Arc::new(Mutex::new(Box::new(MockSandboxBackend::new(
        Arc::new(MockBehavior::new()),
        ExecutionId::new(),
    ))));
    let replacement_handle: SandboxHandle = Arc::new(Mutex::new(Box::new(
        MockSandboxBackend::new(Arc::new(MockBehavior::new()), ExecutionId::new()),
    )));
    let replacement_target = ProxyTarget::new(Ipv4Addr::new(10, 11, 0, 42));

    orchestrator
        .store
        .add(SandboxMetadata {
            id: sandbox_id,
            state: SandboxState::Running,
            ..Default::default()
        })
        .await
        .unwrap();
    orchestrator
        .sandboxes
        .write()
        .await
        .insert(sandbox_id, replacement_handle.clone());
    orchestrator
        .upsert_proxy_route(sandbox_id, replacement_target.clone(), ExecutionId::new())
        .await;

    orchestrator
        .cleanup_failed_launch(&plan, stale_handle, FailedLaunchStage::RunningPersisted)
        .await;

    let current_handle = orchestrator
        .sandboxes
        .read()
        .await
        .get(&sandbox_id)
        .cloned()
        .expect("replacement handle should remain registered");
    assert!(Arc::ptr_eq(&current_handle, &replacement_handle));
    assert_eq!(
        proxy_target_for(&orchestrator, &sandbox_id).await.unwrap(),
        Some(replacement_target)
    );
    assert_eq!(
        orchestrator
            .store
            .get(&sandbox_id)
            .await
            .unwrap()
            .expect("running metadata should remain untouched")
            .state,
        SandboxState::Running
    );
}

fn create_request(
    timeout_secs: Option<u64>,
    user_metadata: &[(&str, &str)],
) -> CreateSandboxRequest {
    let user_metadata = if user_metadata.is_empty() {
        None
    } else {
        Some(
            user_metadata
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect::<HashMap<_, _>>(),
        )
    };

    CreateSandboxRequest {
        source: SandboxLaunchSource::Snapshot(Box::new(RunnableSnapshot::mock())),
        timeout: timeout_secs.map(Duration::from_secs),
        timeout_action: SandboxTimeoutAction::Pause,
        user_metadata,
        env_vars: None,
        network_policy: SandboxNetworkPolicy::default(),
        custom_extension_params: None,
        control_plane_config: None,
        execution_id: None,
        auto_resume: false,
        secure: false,
    }
}

fn write_local_commit_image_config(path: &Path, file: &Path, digest: &str, size: u64) {
    std::fs::create_dir_all(path.parent().expect("image config parent"))
        .expect("create image config dir");
    std::fs::write(
        path,
        serde_json::to_vec_pretty(&json!({
            "repoBlobUrl": "",
            "lowers": [{
                "file": file.display().to_string(),
                "digest": digest,
                "size": size
            }],
            "upper": {},
            "resultFile": ""
        }))
        .expect("serialize image config"),
    )
    .expect("write image config");
}

#[tokio::test]
async fn create_sandbox_from_image_uses_fresh_launch_metadata() -> Result<()> {
    setup();
    let behavior = Arc::new(MockBehavior::new());
    behavior.set_runtime_info(SandboxRuntimeInfo {
        rootfs_virtual_size: Some((3 * 1024 * 1024) + 1),
        ..Default::default()
    });
    let orchestrator =
        make_orchestrator_with_factory(MockBackendFactory::with_behavior(Arc::clone(&behavior)))
            .await;
    let resources = SandboxResources {
        cpu_count: 2,
        memory_mib: 512,
        disk_size_mib: 1024,
    };
    let expected_resources = SandboxResources {
        disk_size_mib: 4,
        ..resources
    };
    let created = orchestrator
        .create_sandbox(CreateSandboxRequest {
            source: SandboxLaunchSource::Image {
                image_ref: "ubuntu:24.04".to_string(),
                overlaybd_config_path: PathBuf::from("/tmp/ubuntu-image.json"),
                context: Default::default(),
                resources: Some(resources),
                extra_drives: Vec::new(),
                extra_boot_args: None,
                image_configs: Box::new(ImageConfigs::new()),
            },
            timeout: Some(Duration::from_secs(60)),
            timeout_action: SandboxTimeoutAction::Pause,
            user_metadata: None,
            env_vars: None,
            network_policy: SandboxNetworkPolicy::default(),
            custom_extension_params: None,
            control_plane_config: None,
            execution_id: None,
            auto_resume: false,
            secure: false,
        })
        .await?;

    assert_eq!(created.state, SandboxState::Running);
    assert_eq!(created.snapshot_id, "ubuntu:24.04");
    assert_eq!(created.snapshot_alias, None);
    assert_eq!(created.resources, expected_resources);
    assert_proxy_ready(&orchestrator, &created.id).await?;
    Ok(())
}

#[tokio::test]
async fn pause_uses_runtime_config_when_source_config_was_evicted() -> Result<()> {
    setup();
    let temp = TempDir::new().expect("tempdir");
    let root_dir = temp.path().join("image-cache");
    let image_cache = Arc::new(ImageCacheService::from_resolved_config(
        ResolvedImageCacheConfig {
            commit_store: root_dir.join("commits"),
            remote_blocks_dir: root_dir.join("remote-blocks"),
            root_dir: root_dir.clone(),
            remote_blocks_size_gb: 10,
            capacity_bytes: None,
        },
    ));

    let source = temp.path().join("source.commit");
    std::fs::write(&source, b"paused").expect("write source commit");
    let commit_file = image_cache
        .import_hard_commit_trusted_descriptor(&source, "sha256:paused", 6)
        .await
        .expect("import paused commit");

    let source_config = root_dir.join("configs/source-image.json");
    let runtime_config = temp.path().join("runtime/image.json");
    write_local_commit_image_config(&source_config, &commit_file, "sha256:paused", 6);
    write_local_commit_image_config(&runtime_config, &commit_file, "sha256:paused", 6);

    let behavior = Arc::new(MockBehavior::new());
    behavior.set_source_config_paths(vec![source_config.clone()]);
    behavior.set_runtime_info(SandboxRuntimeInfo {
        runtime_artifacts: RuntimeArtifactSet::from_overlaybd_image_configs(vec![runtime_config]),
        ..Default::default()
    });
    let mut orchestrator = make_orchestrator_without_background_with_factory(
        InMemoryMetadataStore::new(),
        MockBackendFactory::with_behavior(behavior),
    );
    Arc::get_mut(&mut orchestrator)
        .expect("orchestrator should be uniquely owned")
        .image_refs = test_local_image_services_from_service(
        Arc::clone(&image_cache),
        None,
        Duration::from_secs(0),
    )
    .runtime_refs;

    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[]))
        .await?;
    std::fs::remove_file(&source_config).expect("evict source config");

    orchestrator.pause_sandbox(created.id).await?;
    let running: Vec<(String, Vec<PathBuf>)> = orchestrator
        .collect_running_artifacts()
        .await
        .into_iter()
        .map(|(id, artifacts)| {
            (
                id.to_string(),
                artifacts.into_overlaybd_image_config_paths(),
            )
        })
        .collect();
    let summary = image_cache
        .run_maintenance(running, None, Duration::from_secs(0))
        .await
        .expect("run gc");

    assert_eq!(summary.collected, 0);
    assert!(commit_file.exists());
    assert!(
        summary.retained >= 1,
        "paused commit must be retained by its durable pin"
    );
    Ok(())
}

#[tokio::test]
async fn sandbox_network_policy_is_applied_and_persisted() -> Result<()> {
    setup();
    let behavior = Arc::new(MockBehavior::new());
    let orchestrator =
        make_orchestrator_with_factory(MockBackendFactory::with_behavior(Arc::clone(&behavior)))
            .await;
    let initial_policy = SandboxNetworkPolicy::new(
        BaseSandboxNetworkPolicy::Deny,
        SandboxNetworkEgressPolicy::new(
            Some(vec!["8.8.8.8".to_string()]),
            Some(vec!["203.0.113.0/24".to_string()]),
        )?,
    );
    let mut request = create_request(Some(60), &[]);
    request.network_policy = initial_policy.clone();

    let created = orchestrator.create_sandbox(request).await?;
    assert_eq!(created.network_policy, initial_policy);

    let updated_policy = SandboxNetworkPolicy::new(
        BaseSandboxNetworkPolicy::Allow,
        SandboxNetworkEgressPolicy::new(None, Some(vec!["198.51.100.0/24".to_string()]))?,
    );
    orchestrator
        .replace_sandbox_network_policy(created.id, updated_policy.clone())
        .await?;
    assert_eq!(behavior.update_network_calls(), 1);

    let updated = orchestrator
        .get_sandbox(&created.id)
        .await?
        .expect("sandbox metadata should exist");
    assert_eq!(updated.network_policy, updated_policy);

    Ok(())
}

async fn wait_for_state(
    orchestrator: &Arc<TestOrchestrator>,
    sandbox_id: &SandboxId,
    expected: SandboxState,
) -> Result<SandboxMetadata> {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let metadata = orchestrator
            .get_sandbox(sandbox_id)
            .await?
            .expect("sandbox metadata should exist while waiting for state");
        if metadata.state == expected {
            return Ok(metadata);
        }
        if std::time::Instant::now() >= deadline {
            return Err(anyhow::anyhow!(
                "sandbox {} did not reach {:?}, final state: {:?}",
                sandbox_id,
                expected,
                metadata.state
            )
            .into());
        }
        sleep(STATE_POLL_INTERVAL).await;
    }
}

async fn assert_proxy_ready<S, F, P>(
    orchestrator: &Arc<TestOrchestrator<S, F, P>>,
    sandbox_id: &SandboxId,
) -> Result<()>
where
    S: MetadataStore + 'static,
    F: SandboxBackendFactory,
    P: SandboxPersister + 'static,
{
    let lookup = orchestrator.proxy_lookup_for(sandbox_id).await?;
    assert!(
        matches!(lookup, ProxyLookupResult::Ready(_)),
        "expected proxy lookup Ready for sandbox {sandbox_id}, got {lookup:?}"
    );
    Ok(())
}

async fn assert_proxy_paused<S, F, P>(
    orchestrator: &Arc<TestOrchestrator<S, F, P>>,
    sandbox_id: &SandboxId,
) -> Result<()>
where
    S: MetadataStore + 'static,
    F: SandboxBackendFactory,
    P: SandboxPersister + 'static,
{
    let expected = ProxyLookupResult::Paused { auto_resume: false };
    let actual = orchestrator.proxy_lookup_for(sandbox_id).await?;
    assert_eq!(actual, expected);
    Ok(())
}

async fn assert_proxy_not_found<S, F, P>(
    orchestrator: &Arc<TestOrchestrator<S, F, P>>,
    sandbox_id: &SandboxId,
) -> Result<()>
where
    S: MetadataStore + 'static,
    F: SandboxBackendFactory,
    P: SandboxPersister + 'static,
{
    let actual = orchestrator.proxy_lookup_for(sandbox_id).await?;
    assert_eq!(actual, ProxyLookupResult::NotFound);
    Ok(())
}

#[tokio::test]
async fn wait_for_transition_maps_none_to_not_found() {
    setup();
    let orchestrator = make_orchestrator_without_background(ScriptedWaitStore::with_wait(
        WaitScript::Return(Ok(None)),
    ));
    let sandbox_id = SandboxId::new();

    let err = orchestrator
        .wait_for_transition(sandbox_id, SandboxState::Resuming)
        .await
        .expect_err("none from store wait should map to not found");
    assert!(matches!(err, OrchestratorError::SandboxNotFound(_)));
}

#[tokio::test]
async fn wait_for_transition_maps_store_error() {
    setup();
    let orchestrator = make_orchestrator_without_background(ScriptedWaitStore::with_wait(
        WaitScript::Return(Err(StoreError::Backend {
            source: anyhow::anyhow!("forced wait error"),
        })),
    ));
    let sandbox_id = SandboxId::new();

    let err = orchestrator
        .wait_for_transition(sandbox_id, SandboxState::Pausing)
        .await
        .expect_err("store wait error should bubble as store operation failure");
    assert!(matches!(err, OrchestratorError::StoreOperationFailed(_)));
}

#[tokio::test]
async fn join_concurrent_pause_maps_killing_to_not_found() {
    setup();
    let sandbox_id = SandboxId::new();
    let orchestrator = make_orchestrator_without_background(ScriptedWaitStore::with_wait(
        WaitScript::Return(Ok(Some(SandboxMetadata {
            id: sandbox_id,
            state: SandboxState::Killing,
            ..Default::default()
        }))),
    ));

    let err = orchestrator
        .join_concurrent_pause(sandbox_id)
        .await
        .expect_err("killing after joined pause should map to not found");
    assert!(matches!(err, OrchestratorError::SandboxNotFound(_)));
}

#[tokio::test]
async fn join_concurrent_resume_maps_unexpected_state() {
    setup();
    let sandbox_id = SandboxId::new();
    let orchestrator = make_orchestrator_without_background(ScriptedWaitStore::with_wait(
        WaitScript::Return(Ok(Some(SandboxMetadata {
            id: sandbox_id,
            state: SandboxState::Creating,
            ..Default::default()
        }))),
    ));

    let err = orchestrator
        .join_concurrent_resume(sandbox_id, NewTimeout::None)
        .await
        .expect_err("unexpected joined resume state should map to invalid state");
    assert!(matches!(
        err,
        OrchestratorError::InvalidSandboxState {
            state: SandboxState::Creating,
            ..
        }
    ));
}

#[tokio::test]
async fn pause_resume_transitions_and_is_idempotent() -> Result<()> {
    setup();
    let orchestrator = make_orchestrator().await;
    let case_id = Uuid::now_v7().to_string();
    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[("case_id", case_id.as_str())]))
        .await?;
    let sandbox_id = created.id;
    assert_proxy_ready(&orchestrator, &sandbox_id).await?;
    assert_metrics_values(
        &orchestrator,
        1,
        0,
        1,
        0,
        created.resources.cpu_count,
        created.resources.memory_mib,
    )
    .await;

    orchestrator.pause_sandbox(sandbox_id).await?;
    assert_proxy_paused(&orchestrator, &sandbox_id).await?;

    let paused = orchestrator
        .get_sandbox(&sandbox_id)
        .await?
        .expect("sandbox metadata should exist after pause");
    assert_eq!(paused.state, SandboxState::Paused);
    assert_eq!(
        paused
            .user_metadata
            .as_ref()
            .and_then(|metadata| metadata.get("case_id"))
            .map(String::as_str),
        Some(case_id.as_str())
    );
    assert!(
        paused.paused_state.is_some(),
        "paused sandbox should persist paused state"
    );
    assert_metrics_values(&orchestrator, 1, 0, 0, 0, 0, 0).await;

    // Calling pause again while already paused should be idempotent.
    orchestrator.pause_sandbox(sandbox_id).await?;

    // keep_alive_for only supports RUNNING sandboxes.
    let keep_alive_error = orchestrator
        .keep_alive_for(sandbox_id, Some(Duration::from_secs(10)), true)
        .await
        .expect_err("keep_alive_for on paused sandbox should fail");
    assert!(matches!(
        keep_alive_error,
        OrchestratorError::InvalidSandboxState {
            state: SandboxState::Paused,
            ..
        }
    ));

    let resumed_metadata = orchestrator
        .resume_sandbox(
            sandbox_id,
            NewTimeout::Set(Duration::from_secs(90)),
            test_claim(),
        )
        .await?;
    assert_eq!(resumed_metadata.state, SandboxState::Running);
    assert_eq!(resumed_metadata.timeout, Some(Duration::from_secs(90)));

    let resumed = orchestrator
        .get_sandbox(&sandbox_id)
        .await?
        .expect("sandbox metadata should exist after resume");
    assert_eq!(resumed.state, SandboxState::Running);
    assert_proxy_ready(&orchestrator, &sandbox_id).await?;
    assert_metrics_values(
        &orchestrator,
        1,
        0,
        1,
        0,
        created.resources.cpu_count,
        created.resources.memory_mib,
    )
    .await;

    // Calling resume again while already running should be idempotent.
    let resumed_metadata = orchestrator
        .resume_sandbox(
            sandbox_id,
            NewTimeout::Set(Duration::from_secs(120)),
            test_claim(),
        )
        .await?;
    assert_eq!(resumed_metadata.state, SandboxState::Running);
    assert_eq!(resumed_metadata.timeout, Some(Duration::from_secs(120)));

    orchestrator.delete_sandbox(sandbox_id).await?;
    assert!(orchestrator.get_sandbox(&sandbox_id).await?.is_none());
    assert_proxy_not_found(&orchestrator, &sandbox_id).await?;
    assert_metrics_values(&orchestrator, 1, 0, 0, 0, 0, 0).await;

    Ok(())
}

#[tokio::test]
async fn pause_succeeds_and_releases_metrics_even_when_stop_fails_after_snapshot() -> Result<()> {
    setup();
    let behavior = Arc::new(MockBehavior::new());
    behavior.push_action(
        MockOperation::Stop,
        MockAction::Fail {
            message: "stop failed".to_string(),
        },
    );
    let orchestrator =
        make_orchestrator_with_factory(MockBackendFactory::with_behavior(behavior)).await;
    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[]))
        .await?;

    orchestrator.pause_sandbox(created.id).await?;

    let paused = orchestrator
        .get_sandbox(&created.id)
        .await?
        .expect("sandbox should still exist after pause");
    assert_eq!(paused.state, SandboxState::Paused);

    // Resource metrics are derived from the current metadata state, so a
    // sandbox that is logically Paused no longer counts toward allocated
    // CPU/memory regardless of whether the backing VM `stop()` succeeded.
    assert_metrics_values(&orchestrator, 1, 0, 0, 0, 0, 0).await;
    Ok(())
}

#[tokio::test]
async fn pause_terminal_failure_removes_sandbox_and_metrics() -> Result<()> {
    setup();
    let behavior = Arc::new(MockBehavior::new());
    behavior.push_action(
        MockOperation::Pause,
        MockAction::FailTerminal {
            message: "restack succeeded but snapshot staging failed".to_string(),
        },
    );
    let orchestrator =
        make_orchestrator_with_factory(MockBackendFactory::with_behavior(behavior)).await;
    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[]))
        .await?;
    let sandbox_id = created.id;

    let err = orchestrator
        .pause_sandbox(sandbox_id)
        .await
        .expect_err("pause should fail terminally");
    assert!(matches!(
        err,
        OrchestratorError::SandboxOperationFailed {
            operation: SandboxOperation::Pause,
            ..
        }
    ));

    assert!(
        orchestrator.get_sandbox(&sandbox_id).await?.is_none(),
        "terminal pause failure should remove sandbox metadata"
    );
    assert_proxy_not_found(&orchestrator, &sandbox_id).await?;
    assert_metrics_values(&orchestrator, 1, 0, 0, 0, 0, 0).await;
    Ok(())
}

#[tokio::test]
async fn pause_removes_handle_less_running_sandbox_and_releases_metrics() -> Result<()> {
    setup();
    let orchestrator = make_orchestrator().await;
    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[("team", "missing-handle")]))
        .await?;
    let sandbox_id = created.id;

    assert_proxy_ready(&orchestrator, &sandbox_id).await?;
    assert_metrics_values(
        &orchestrator,
        1,
        0,
        1,
        0,
        created.resources.cpu_count,
        created.resources.memory_mib,
    )
    .await;

    let removed = orchestrator.sandboxes.write().await.remove(&sandbox_id);
    assert!(
        removed.is_some(),
        "created sandbox should have a registered runtime handle"
    );

    let err = orchestrator
        .pause_sandbox(sandbox_id)
        .await
        .expect_err("pause should fail when persisted running sandbox has no handle");
    assert!(matches!(err, OrchestratorError::SandboxNotFound(_)));

    assert!(
        orchestrator.get_sandbox(&sandbox_id).await?.is_none(),
        "handle-less running sandbox should be removed from the store"
    );
    assert_proxy_not_found(&orchestrator, &sandbox_id).await?;
    assert_metrics_values(&orchestrator, 1, 0, 0, 0, 0, 0).await;

    Ok(())
}

#[tokio::test]
async fn pause_persists_before_publishing_paused_metadata() -> Result<()> {
    setup();
    let persister = RecordingPersister::default();
    let orchestrator = make_orchestrator_without_background_with_factory_and_persister(
        InMemoryMetadataStore::new(),
        MockBackendFactory::new(),
        persister.clone(),
    );
    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[("team", "pause-persist")]))
        .await?;

    orchestrator.pause_sandbox(created.id).await?;

    assert_eq!(
        persister.calls(),
        vec![
            RecordingCall::AllocateArtifactRoot,
            RecordingCall::PersistPaused
        ]
    );
    let metadata = orchestrator
        .get_sandbox(&created.id)
        .await?
        .expect("metadata should remain after pause");
    assert_eq!(metadata.state, SandboxState::Paused);
    Ok(())
}

/// 🔴 The run has to be charged *before* the record is written, not merely
/// before the store sees it.
///
/// `running_since` is `#[serde(skip)]`, so the only thing a restarted node can
/// read back is `running_elapsed`. A pause that left the charging to the store
/// would persist a record with the last run missing from it, and every node
/// restart would hand that run back for free.
#[tokio::test]
async fn pause_charges_the_run_into_the_record_it_persists() -> Result<()> {
    setup();
    let persister = RecordingPersister::default();
    let orchestrator = make_orchestrator_without_background_with_factory_and_persister(
        InMemoryMetadataStore::new(),
        MockBackendFactory::new(),
        persister.clone(),
    );
    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[]))
        .await?;
    sleep(Duration::from_millis(30)).await;

    orchestrator.pause_sandbox(created.id).await?;

    let persisted = persister.persisted();
    let record = persisted.first().expect("one persisted record");
    assert_eq!(record.state, SandboxState::Paused);
    assert_eq!(record.running_since, None);
    assert!(
        record.running_elapsed >= Duration::from_millis(25),
        "the persisted record must carry the run that just ended, got {:?}",
        record.running_elapsed
    );
    Ok(())
}

#[tokio::test]
async fn pause_persistence_failure_rolls_back_to_running() -> Result<()> {
    setup();
    let persister = RecordingPersister::default();
    persister.fail_next(RecordingCall::PersistPaused);
    let orchestrator = make_orchestrator_without_background_with_factory_and_persister(
        InMemoryMetadataStore::new(),
        MockBackendFactory::new(),
        persister.clone(),
    );
    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[("team", "pause-persist-fail")]))
        .await?;

    let err = orchestrator
        .pause_sandbox(created.id)
        .await
        .expect_err("pause should fail when persisted paused state cannot be written");

    assert!(matches!(err, OrchestratorError::InternalError(_)));
    assert_eq!(
        persister.calls(),
        vec![
            RecordingCall::AllocateArtifactRoot,
            RecordingCall::PersistPaused
        ]
    );
    let metadata = orchestrator
        .get_sandbox(&created.id)
        .await?
        .expect("metadata should remain after pause persistence failure");
    assert_eq!(metadata.state, SandboxState::Running);
    assert!(metadata.paused_state.is_none());
    assert_proxy_ready(&orchestrator, &created.id).await?;
    assert_metrics_values(
        &orchestrator,
        1,
        0,
        1,
        0,
        created.resources.cpu_count,
        created.resources.memory_mib,
    )
    .await;
    Ok(())
}

#[tokio::test]
async fn pause_artifact_root_allocation_failure_restores_running_for_retry() -> Result<()> {
    setup();
    let persister = RecordingPersister::default();
    persister.fail_next(RecordingCall::AllocateArtifactRoot);
    let runtime_artifacts =
        RuntimeArtifactSet::from_overlaybd_image_configs(vec![PathBuf::from("runtime/image.json")]);
    let behavior = Arc::new(MockBehavior::new());
    behavior.set_runtime_info(SandboxRuntimeInfo {
        runtime_artifacts: runtime_artifacts.clone(),
        ..Default::default()
    });
    let mut orchestrator = make_orchestrator_without_background_with_factory_and_persister(
        InMemoryMetadataStore::new(),
        MockBackendFactory::with_behavior(behavior),
        persister.clone(),
    );
    let image_refs = Arc::new(RecordingRuntimeImageRefs::default());
    Arc::get_mut(&mut orchestrator)
        .expect("orchestrator should be uniquely owned")
        .image_refs = image_refs.clone();

    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[("team", "pause-allocate-fail")]))
        .await?;
    let sandbox_id = created.id;

    let err = orchestrator
        .pause_sandbox(sandbox_id)
        .await
        .expect_err("pause should fail when artifact-root allocation fails");
    assert!(matches!(
        err,
        OrchestratorError::SandboxPersistenceFailed(_)
    ));

    let running = orchestrator
        .get_sandbox(&sandbox_id)
        .await?
        .expect("metadata should remain after allocation failure");
    assert_eq!(running.state, SandboxState::Running);
    assert!(running.paused_state.is_none());
    assert!(
        orchestrator
            .sandboxes
            .read()
            .await
            .contains_key(&sandbox_id),
        "allocation failure must not detach the runtime handle"
    );
    assert_proxy_ready(&orchestrator, &sandbox_id).await?;
    assert_metrics_values(
        &orchestrator,
        1,
        0,
        1,
        0,
        created.resources.cpu_count,
        created.resources.memory_mib,
    )
    .await;
    assert_eq!(
        image_refs.pinned(),
        vec![
            (
                RuntimeImageOwner::StartingSandbox(sandbox_id),
                RuntimeArtifactSet::empty(),
            ),
            (
                RuntimeImageOwner::PausedSandbox(sandbox_id),
                runtime_artifacts.clone(),
            ),
        ]
    );
    assert_eq!(
        image_refs.unpinned(),
        vec![
            RuntimeImageOwner::StartingSandbox(sandbox_id),
            RuntimeImageOwner::PausedSandbox(sandbox_id),
        ]
    );
    assert_eq!(persister.calls(), vec![RecordingCall::AllocateArtifactRoot]);

    orchestrator.pause_sandbox(sandbox_id).await?;
    let paused = orchestrator
        .get_sandbox(&sandbox_id)
        .await?
        .expect("metadata should remain after retry");
    assert_eq!(paused.state, SandboxState::Paused);
    assert!(paused.paused_state.is_some());
    assert_proxy_paused(&orchestrator, &sandbox_id).await?;
    assert_eq!(
        persister.calls(),
        vec![
            RecordingCall::AllocateArtifactRoot,
            RecordingCall::AllocateArtifactRoot,
            RecordingCall::PersistPaused,
        ]
    );

    orchestrator.delete_sandbox(sandbox_id).await?;
    Ok(())
}

#[tokio::test]
async fn capture_snapshot_returns_snapshot_and_preserves_running_sandbox() -> Result<()> {
    setup();
    let orchestrator = make_orchestrator().await;
    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[("team", "snapshot-success")]))
        .await?;
    let sandbox_id = created.id;

    let result = orchestrator.capture_snapshot(sandbox_id).await?;
    assert_eq!(result.metadata.id, sandbox_id);
    assert_eq!(result.metadata.state, SandboxState::Running);
    assert!(
        result
            .captured_snapshot
            .downcast_ref::<crate::sandbox::mock::MockCapturedSnapshot>()
            .is_some(),
        "capture_snapshot should return the backend-provided snapshot payload"
    );

    let persisted = orchestrator
        .get_sandbox(&sandbox_id)
        .await?
        .expect("sandbox metadata should still exist after snapshot capture");
    assert_eq!(persisted.state, SandboxState::Running);
    assert_proxy_ready(&orchestrator, &sandbox_id).await?;
    assert_metrics_values(
        &orchestrator,
        1,
        0,
        1,
        0,
        created.resources.cpu_count,
        created.resources.memory_mib,
    )
    .await;

    orchestrator.delete_sandbox(sandbox_id).await?;
    Ok(())
}

#[tokio::test]
async fn capture_snapshot_recoverable_failure_rolls_back_to_running_and_allows_retry() -> Result<()>
{
    setup();
    let behavior = Arc::new(MockBehavior::new());
    behavior.push_action(
        MockOperation::Snapshot,
        MockAction::Fail {
            message: "snapshot staging failed".to_string(),
        },
    );
    let orchestrator =
        make_orchestrator_with_factory(MockBackendFactory::with_behavior(behavior)).await;
    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[("team", "snapshot-retry")]))
        .await?;
    let sandbox_id = created.id;
    let baseline_metrics = current_metrics(&orchestrator).await;

    let err = orchestrator
        .capture_snapshot(sandbox_id)
        .await
        .expect_err("recoverable snapshot failure should be returned to the caller");
    assert!(matches!(
        err,
        OrchestratorError::SandboxOperationFailed {
            operation: SandboxOperation::Snapshot,
            ..
        }
    ));

    let persisted = orchestrator
        .get_sandbox(&sandbox_id)
        .await?
        .expect("sandbox metadata should remain after recoverable snapshot failure");
    assert_eq!(persisted.state, SandboxState::Running);
    assert_proxy_ready(&orchestrator, &sandbox_id).await?;
    assert_metrics_snapshot(&orchestrator, &baseline_metrics).await;

    let retry = orchestrator.capture_snapshot(sandbox_id).await?;
    assert_eq!(retry.metadata.state, SandboxState::Running);
    assert!(
        retry
            .captured_snapshot
            .downcast_ref::<crate::sandbox::mock::MockCapturedSnapshot>()
            .is_some(),
        "retry should produce a captured snapshot"
    );

    orchestrator.delete_sandbox(sandbox_id).await?;
    Ok(())
}

#[tokio::test]
async fn capture_snapshot_terminal_failure_removes_sandbox_and_releases_metrics() -> Result<()> {
    setup();
    let behavior = Arc::new(MockBehavior::new());
    behavior.push_action(
        MockOperation::Snapshot,
        MockAction::FailTerminal {
            message: "snapshot capture became unrecoverable".to_string(),
        },
    );
    behavior.push_action(
        MockOperation::Stop,
        MockAction::Fail {
            message: "stop failed after terminal snapshot error".to_string(),
        },
    );
    let orchestrator =
        make_orchestrator_with_factory(MockBackendFactory::with_behavior(behavior)).await;
    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[("team", "snapshot-terminal")]))
        .await?;
    let sandbox_id = created.id;

    let err = orchestrator
        .capture_snapshot(sandbox_id)
        .await
        .expect_err("terminal snapshot failure should fail the operation");
    assert!(matches!(
        err,
        OrchestratorError::SandboxOperationFailed {
            operation: SandboxOperation::Snapshot,
            ..
        }
    ));

    assert!(
        orchestrator.get_sandbox(&sandbox_id).await?.is_none(),
        "terminal snapshot failure should remove sandbox metadata"
    );
    assert_proxy_not_found(&orchestrator, &sandbox_id).await?;
    assert_metrics_values(&orchestrator, 1, 0, 0, 0, 0, 0).await;
    Ok(())
}

#[tokio::test]
async fn capture_snapshot_without_runtime_handle_removes_sandbox_and_releases_metrics() -> Result<()>
{
    setup();
    let orchestrator = make_orchestrator().await;
    let created = orchestrator
        .create_sandbox(create_request(
            Some(60),
            &[("team", "snapshot-missing-handle")],
        ))
        .await?;
    let sandbox_id = created.id;
    assert_proxy_ready(&orchestrator, &sandbox_id).await?;
    assert_metrics_values(
        &orchestrator,
        1,
        0,
        1,
        0,
        created.resources.cpu_count,
        created.resources.memory_mib,
    )
    .await;

    let removed = orchestrator.sandboxes.write().await.remove(&sandbox_id);
    assert!(
        removed.is_some(),
        "created sandbox should have a registered runtime handle"
    );

    let err = orchestrator
        .capture_snapshot(sandbox_id)
        .await
        .expect_err("capture_snapshot should fail when persisted running sandbox has no handle");
    assert!(matches!(err, OrchestratorError::SandboxNotFound(_)));

    assert!(
        orchestrator.get_sandbox(&sandbox_id).await?.is_none(),
        "handle-less running sandbox should be removed from the store"
    );
    assert_proxy_not_found(&orchestrator, &sandbox_id).await?;
    assert_metrics_values(&orchestrator, 1, 0, 0, 0, 0, 0).await;
    Ok(())
}

#[tokio::test]
async fn capture_snapshot_rejects_non_running_states() -> Result<()> {
    setup();
    let orchestrator = make_orchestrator().await;
    let created = orchestrator
        .create_sandbox(create_request(
            Some(60),
            &[("team", "snapshot-invalid-state")],
        ))
        .await?;
    let sandbox_id = created.id;
    orchestrator.pause_sandbox(sandbox_id).await?;

    let err = orchestrator
        .capture_snapshot(sandbox_id)
        .await
        .expect_err("capture_snapshot should reject paused sandboxes");
    assert!(matches!(
        err,
        OrchestratorError::InvalidSandboxState {
            sandbox_id: id,
            state: SandboxState::Paused,
        } if id == sandbox_id
    ));

    Ok(())
}

#[tokio::test]
async fn capture_snapshot_maps_killing_state_to_not_found() -> Result<()> {
    setup();
    let orchestrator = make_orchestrator().await;
    let sandbox_id = SandboxId::new();
    orchestrator
        .set_metadata_state_for_test(sandbox_id, SandboxState::Killing)
        .await?;

    let err = orchestrator
        .capture_snapshot(sandbox_id)
        .await
        .expect_err("capture_snapshot should treat killing sandboxes as gone");
    assert!(matches!(err, OrchestratorError::SandboxNotFound(id) if id == sandbox_id));

    Ok(())
}

#[tokio::test]
async fn resume_sandbox_build_failure_does_not_subtract_metrics_that_were_never_added() -> Result<()>
{
    setup();
    let behavior = Arc::new(MockBehavior::new());
    behavior.push_action(
        MockOperation::BuildFromSnapshot,
        MockAction::Fail {
            message: "resume build failed".to_string(),
        },
    );
    let orchestrator =
        make_orchestrator_with_factory(MockBackendFactory::with_behavior(behavior)).await;

    let running = orchestrator
        .create_sandbox(create_request(Some(60), &[]))
        .await?;

    let paused_id = SandboxId::new();
    orchestrator
        .store
        .add(SandboxMetadata {
            id: paused_id,
            state: SandboxState::Paused,
            paused_state: Some(test_paused_state().clone()),
            ..Default::default()
        })
        .await?;

    // Capture the baseline *after* introducing the paused sandbox so the
    // expected snapshot reflects its paused_* contribution; the test then
    // asserts that a failed resume leaves both running and paused
    // contributions untouched.
    let baseline_metrics = current_metrics(&orchestrator).await;

    let err = orchestrator
        .resume_sandbox(paused_id, NewTimeout::None, test_claim())
        .await
        .expect_err("resume should fail when building from paused state fails");
    assert!(matches!(
        err,
        OrchestratorError::SandboxOperationFailed {
            operation: SandboxOperation::Build,
            ..
        }
    ));

    assert_metrics_snapshot(&orchestrator, &baseline_metrics).await;

    let still_running = orchestrator
        .get_sandbox(&running.id)
        .await?
        .expect("baseline running sandbox should remain present");
    assert_eq!(still_running.state, SandboxState::Running);
    Ok(())
}

#[tokio::test]
async fn orchestrator_proxy_lookup_tracks_create_pause_resume_delete_lifecycle() -> Result<()> {
    setup();
    let orchestrator = make_orchestrator().await;
    let case_id = Uuid::now_v7().to_string();
    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[("case_id", case_id.as_str())]))
        .await?;
    let sandbox_id = created.id;
    assert_metrics_values(
        &orchestrator,
        1,
        0,
        1,
        0,
        created.resources.cpu_count,
        created.resources.memory_mib,
    )
    .await;
    assert_proxy_ready(&orchestrator, &sandbox_id).await?;

    orchestrator.pause_sandbox(sandbox_id).await?;
    assert_metrics_values(&orchestrator, 1, 0, 0, 0, 0, 0).await;
    assert_proxy_paused(&orchestrator, &sandbox_id).await?;

    orchestrator
        .resume_sandbox(sandbox_id, NewTimeout::UseExisting, test_claim())
        .await?;
    assert_metrics_values(
        &orchestrator,
        1,
        0,
        1,
        0,
        created.resources.cpu_count,
        created.resources.memory_mib,
    )
    .await;
    assert_proxy_ready(&orchestrator, &sandbox_id).await?;

    orchestrator.delete_sandbox(sandbox_id).await?;
    assert_metrics_values(&orchestrator, 1, 0, 0, 0, 0, 0).await;
    assert_proxy_not_found(&orchestrator, &sandbox_id).await?;

    Ok(())
}

// ── Concurrent state-modification tests ──────────────────────────────────────
//
// These tests exercise the wait-for-transitional-state logic in
// `pause_sandbox`, `resume_sandbox`, `delete_sandbox`, and `keep_alive_for`.
// Multiple tasks race to modify the same sandbox concurrently; the orchestrator
// must serialise the operations correctly and return consistent results to every
// caller rather than returning stale/incorrect states or errors.

/// Spawning N concurrent `pause_sandbox` calls on the same running sandbox
/// must result in every caller receiving `Ok(())` and the sandbox ending up in
/// the `Paused` state exactly once.
#[tokio::test]
async fn orchestrator_concurrent_pause_calls_all_succeed() -> Result<()> {
    setup();
    let behavior = Arc::new(MockBehavior::new());
    behavior.push_action(
        MockOperation::Pause,
        MockAction::SucceedAfter(Duration::from_millis(200)),
    );
    let orchestrator =
        make_orchestrator_with_factory(MockBackendFactory::with_behavior(behavior)).await;
    let case_id = Uuid::now_v7().to_string();
    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[("case_id", case_id.as_str())]))
        .await?;
    let sandbox_id = created.id;
    assert_metrics_values(
        &orchestrator,
        1,
        0,
        1,
        0,
        created.resources.cpu_count,
        created.resources.memory_mib,
    )
    .await;

    // Spawn 3 concurrent pause calls.  At most one will win the
    // Running → Pausing CAS; the others will detect the Pausing state and
    // wait until the winner finishes, then observe Paused and return Ok(()).
    const N: usize = 3;
    let handles: Vec<_> = (0..N)
        .map(|_| {
            let orch = Arc::clone(&orchestrator);
            let id = sandbox_id;
            tokio::spawn(async move { orch.pause_sandbox(id).await })
        })
        .collect();

    for (i, handle) in handles.into_iter().enumerate() {
        handle
            .await
            .unwrap_or_else(|e| panic!("pause task {i} panicked: {e}"))
            .unwrap_or_else(|e| panic!("concurrent pause call {i} failed: {e}"));
    }

    // All callers returned Ok(()); the sandbox must now be Paused.
    let metadata = orchestrator
        .get_sandbox(&sandbox_id)
        .await?
        .expect("sandbox should still exist after concurrent pauses");
    assert_eq!(
        metadata.state,
        SandboxState::Paused,
        "sandbox should be Paused after all concurrent pause calls complete"
    );
    assert_metrics_values(&orchestrator, 1, 0, 0, 0, 0, 0).await;
    assert_proxy_paused(&orchestrator, &sandbox_id).await?;

    orchestrator.delete_sandbox(sandbox_id).await?;
    assert!(orchestrator.get_sandbox(&sandbox_id).await?.is_none());
    assert_metrics_values(&orchestrator, 1, 0, 0, 0, 0, 0).await;
    assert_proxy_not_found(&orchestrator, &sandbox_id).await?;

    Ok(())
}

/// Spawning N concurrent `resume_sandbox` calls on the same paused sandbox
/// must result in every caller receiving metadata with state `Running` and the
/// sandbox ending up running.
#[tokio::test]
async fn orchestrator_concurrent_resume_calls_all_return_running() -> Result<()> {
    setup();
    let behavior = Arc::new(MockBehavior::new());
    behavior.push_action(
        MockOperation::BuildFromSnapshot,
        MockAction::SucceedAfter(Duration::from_millis(200)),
    );
    let orchestrator =
        make_orchestrator_with_factory(MockBackendFactory::with_behavior(behavior)).await;
    let case_id = Uuid::now_v7().to_string();
    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[("case_id", case_id.as_str())]))
        .await?;
    let sandbox_id = created.id;
    assert_metrics_values(
        &orchestrator,
        1,
        0,
        1,
        0,
        created.resources.cpu_count,
        created.resources.memory_mib,
    )
    .await;

    // Put the sandbox into Paused state first.
    orchestrator.pause_sandbox(sandbox_id).await?;
    assert_metrics_values(&orchestrator, 1, 0, 0, 0, 0, 0).await;
    assert_eq!(
        orchestrator.get_sandbox(&sandbox_id).await?.unwrap().state,
        SandboxState::Paused
    );

    // Spawn 3 concurrent resume calls, each requesting the same timeout.
    const N: usize = 3;
    let handles: Vec<_> = (0..N)
        .map(|_| {
            let orch = Arc::clone(&orchestrator);
            let id = sandbox_id;
            tokio::spawn(async move {
                orch.resume_sandbox(id, NewTimeout::UseExisting, test_claim())
                    .await
            })
        })
        .collect();

    for (i, handle) in handles.into_iter().enumerate() {
        let metadata = handle
            .await
            .unwrap_or_else(|e| panic!("resume task {i} panicked: {e}"))
            .unwrap_or_else(|e| panic!("concurrent resume call {i} failed: {e}"));

        assert!(
            matches!(metadata.state, SandboxState::Running),
            "resume call {i} should return running metadata"
        );
    }

    // The sandbox must be Running after all callers complete.
    let final_state = wait_for_state(&orchestrator, &sandbox_id, SandboxState::Running)
        .await?
        .state;
    assert_eq!(
        final_state,
        SandboxState::Running,
        "sandbox should be Running after all concurrent resume calls complete"
    );
    assert_metrics_values(
        &orchestrator,
        1,
        0,
        1,
        0,
        created.resources.cpu_count,
        created.resources.memory_mib,
    )
    .await;
    assert_proxy_ready(&orchestrator, &sandbox_id).await?;

    orchestrator.delete_sandbox(sandbox_id).await?;
    assert!(orchestrator.get_sandbox(&sandbox_id).await?.is_none());
    assert_metrics_values(&orchestrator, 1, 0, 0, 0, 0, 0).await;
    assert_proxy_not_found(&orchestrator, &sandbox_id).await?;

    Ok(())
}

/// Spawning N concurrent `delete_sandbox` calls on the same sandbox must
/// result in every caller returning `Ok(())` or `SandboxNotFound`, and
/// the sandbox being absent afterwards.
#[tokio::test]
async fn orchestrator_concurrent_delete_calls_are_idempotent() -> Result<()> {
    setup();
    let behavior = Arc::new(MockBehavior::new());
    behavior.push_action(
        MockOperation::Stop,
        MockAction::SucceedAfter(Duration::from_millis(200)),
    );
    let orchestrator =
        make_orchestrator_with_factory(MockBackendFactory::with_behavior(behavior)).await;
    let case_id = Uuid::now_v7().to_string();
    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[("case_id", case_id.as_str())]))
        .await?;
    let sandbox_id = created.id;
    assert_metrics_values(
        &orchestrator,
        1,
        0,
        1,
        0,
        created.resources.cpu_count,
        created.resources.memory_mib,
    )
    .await;

    // Spawn 3 concurrent delete calls.
    const N: usize = 3;
    let handles: Vec<_> = (0..N)
        .map(|_| {
            let orch = Arc::clone(&orchestrator);
            let id = sandbox_id;
            tokio::spawn(async move { orch.delete_sandbox(id).await })
        })
        .collect();

    for (i, handle) in handles.into_iter().enumerate() {
        let outcome = handle
            .await
            .unwrap_or_else(|e| panic!("delete task {i} panicked: {e}"));
        // The first caller performs the real teardown and returns Ok(()).
        // Later callers may find the sandbox already gone and receive
        // SandboxNotFound — that is also an acceptable outcome.
        assert!(
            outcome.is_ok() || matches!(outcome, Err(OrchestratorError::SandboxNotFound(_))),
            "concurrent delete {i} should succeed or report SandboxNotFound, got: {outcome:?}"
        );
    }

    // Sandbox must be gone after all callers complete.
    assert!(
        orchestrator.get_sandbox(&sandbox_id).await?.is_none(),
        "sandbox should be removed after concurrent deletes complete"
    );
    assert_metrics_values(&orchestrator, 1, 0, 0, 0, 0, 0).await;
    assert_proxy_not_found(&orchestrator, &sandbox_id).await?;

    Ok(())
}

/// `keep_alive_for` called concurrently with `resume_sandbox` must never
/// surface transitional states to the caller.
///
/// Accepted outcomes:
/// - `Ok(_)` — `keep_alive_for` saw `Running` (or waited and observed
///   `Running` after the resume completed).
/// - `Err(InvalidSandboxState { state: Paused })` — `keep_alive_for` ran
///   before `resume_sandbox` had a chance to set `Resuming`; the sandbox was
///   still paused.
///
/// Forbidden outcome:
/// - `Err(InvalidSandboxState { state: Resuming })` — means `keep_alive_for`
///   returned without waiting.
#[tokio::test]
async fn orchestrator_keep_alive_during_resume_never_reports_resuming_state() -> Result<()> {
    setup();
    let behavior = Arc::new(MockBehavior::new());
    behavior.push_action(
        MockOperation::BuildFromSnapshot,
        MockAction::SucceedAfter(Duration::from_millis(200)),
    );
    let orchestrator =
        make_orchestrator_with_factory(MockBackendFactory::with_behavior(behavior)).await;
    let case_id = Uuid::now_v7().to_string();
    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[("case_id", case_id.as_str())]))
        .await?;
    let sandbox_id = created.id;

    orchestrator.pause_sandbox(sandbox_id).await?;
    assert_metrics_values(&orchestrator, 1, 0, 0, 0, 0, 0).await;

    // Race resume_sandbox against keep_alive_for.  The exact winner of the
    // race is non-deterministic, but the forbidden outcome is clear.
    let orch_resume = Arc::clone(&orchestrator);
    let orch_ka = Arc::clone(&orchestrator);

    let resume_handle = tokio::spawn(async move {
        orch_resume
            .resume_sandbox(sandbox_id, NewTimeout::UseExisting, test_claim())
            .await
    });
    let keep_alive_handle = tokio::spawn(async move {
        orch_ka
            .keep_alive_for(sandbox_id, Some(Duration::from_secs(120)), true)
            .await
    });

    let (resume_result, ka_result) = tokio::join!(resume_handle, keep_alive_handle);

    resume_result
        .expect("resume task should not panic")
        .expect("resume_sandbox should always succeed");

    let ka_outcome = ka_result.expect("keep_alive task should not panic");

    // The one outcome that must NEVER happen: returning Resuming as the
    // conflicting state.  Without the fix this would be returned because
    // keep_alive_for did not wait for the Resuming transition.
    if let Err(ref e) = ka_outcome {
        assert!(
            !matches!(
                e,
                OrchestratorError::InvalidSandboxState {
                    state: SandboxState::Resuming,
                    ..
                }
            ),
            "keep_alive_for must not return InvalidSandboxState{{Resuming}}; \
             it should wait for the transition to complete instead"
        );
    }

    // After both futures settle the sandbox must be Running.
    let final_state = orchestrator
        .get_sandbox(&sandbox_id)
        .await?
        .expect("sandbox should still exist")
        .state;
    assert_eq!(
        final_state,
        SandboxState::Running,
        "sandbox must be Running after resume completes"
    );
    assert_metrics_values(
        &orchestrator,
        1,
        0,
        1,
        0,
        created.resources.cpu_count,
        created.resources.memory_mib,
    )
    .await;
    assert_proxy_ready(&orchestrator, &sandbox_id).await?;

    orchestrator.delete_sandbox(sandbox_id).await?;
    assert!(orchestrator.get_sandbox(&sandbox_id).await?.is_none());
    assert_metrics_values(&orchestrator, 1, 0, 0, 0, 0, 0).await;
    assert_proxy_not_found(&orchestrator, &sandbox_id).await?;

    Ok(())
}

/// Calling `delete_sandbox` concurrently with an in-progress `pause_sandbox`
/// must be safe: the delete waits for the pause to leave its `Pausing`
/// transitional state before acquiring `Killing`, so the two operations
/// never corrupt each other's state writes.
///
/// The invariant under test: regardless of which operation wins the race, the
/// sandbox is cleanly removed by the time both futures complete.
#[tokio::test]
async fn orchestrator_delete_during_pause_completes_cleanly() -> Result<()> {
    setup();
    let behavior = Arc::new(MockBehavior::new());
    behavior.push_action(
        MockOperation::Pause,
        MockAction::SucceedAfter(Duration::from_millis(200)),
    );
    let orchestrator =
        make_orchestrator_with_factory(MockBackendFactory::with_behavior(behavior)).await;
    let case_id = Uuid::now_v7().to_string();
    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[("case_id", case_id.as_str())]))
        .await?;
    let sandbox_id = created.id;
    assert_metrics_values(
        &orchestrator,
        1,
        0,
        1,
        0,
        created.resources.cpu_count,
        created.resources.memory_mib,
    )
    .await;

    // Race pause and delete against each other.
    let orch_pause = Arc::clone(&orchestrator);
    let orch_delete = Arc::clone(&orchestrator);

    let pause_handle = tokio::spawn(async move { orch_pause.pause_sandbox(sandbox_id).await });
    let delete_handle = tokio::spawn(async move { orch_delete.delete_sandbox(sandbox_id).await });

    let (pause_result, delete_result) = tokio::join!(pause_handle, delete_handle);

    // The pause may succeed (if it finished before delete took over) or fail
    // with SandboxNotFound (if delete transitioned to Killing first).  Both
    // outcomes are acceptable — what matters is that neither panics and that
    // delete always succeeds.
    let pause_outcome = pause_result.expect("pause task should not panic");
    assert!(
        pause_outcome.is_ok()
            || matches!(
                pause_outcome,
                Err(OrchestratorError::SandboxNotFound(_))
            ),
        "pause during delete should either succeed or report SandboxNotFound, got: {pause_outcome:?}"
    );

    delete_result
        .expect("delete task should not panic")
        .expect("delete_sandbox must always succeed in this race");

    // The sandbox must be absent regardless of which operation won.
    assert!(
        orchestrator.get_sandbox(&sandbox_id).await?.is_none(),
        "sandbox should be removed after concurrent pause+delete complete"
    );
    assert_metrics_values(&orchestrator, 1, 0, 0, 0, 0, 0).await;
    assert_proxy_not_found(&orchestrator, &sandbox_id).await?;

    Ok(())
}

#[tokio::test]
async fn orchestrator_unknown_sandbox_behaviors() -> Result<()> {
    setup();
    let orchestrator = make_orchestrator().await;

    let missing_id = SandboxId::new();

    let got = orchestrator.get_sandbox(&missing_id).await?;
    assert!(
        got.is_none(),
        "get_sandbox should return None for unknown id"
    );

    let keep_alive_err = orchestrator
        .keep_alive_for(missing_id, Some(Duration::from_secs(10)), true)
        .await
        .expect_err("keep_alive_for should fail for unknown sandbox");
    assert!(matches!(
        keep_alive_err,
        OrchestratorError::SandboxNotFound(_)
    ));

    let pause_err = orchestrator
        .pause_sandbox(missing_id)
        .await
        .expect_err("pause_sandbox should fail for unknown sandbox");
    assert!(matches!(pause_err, OrchestratorError::SandboxNotFound(_)));

    let resume_err = orchestrator
        .resume_sandbox(missing_id, NewTimeout::None, test_claim())
        .await
        .expect_err("resume_sandbox should fail for unknown sandbox");
    assert!(matches!(resume_err, OrchestratorError::SandboxNotFound(_)));

    let err = orchestrator
        .delete_sandbox(missing_id)
        .await
        .expect_err("deleting unknown sandbox should fail");

    assert!(matches!(err, OrchestratorError::SandboxNotFound(_)));

    Ok(())
}

#[tokio::test]
async fn orchestrator_list_empty_returns_empty() -> Result<()> {
    setup();
    let orchestrator = make_orchestrator().await;

    let all = orchestrator.list_sandboxes().await?;
    assert!(
        all.is_empty(),
        "empty store should return empty sandbox list"
    );

    let filtered = orchestrator
        .list_sandboxes_filtered(SandboxListFilter {
            states: Some(vec![SandboxState::Running]),
            excluded_states: None,
            user_metadata: None,
        })
        .await?;
    assert!(
        filtered.is_empty(),
        "empty store should return empty filtered sandbox list"
    );

    Ok(())
}

#[tokio::test]
async fn orchestrator_keep_alive_none_sets_default_timeout() -> Result<()> {
    setup();
    let orchestrator = make_orchestrator().await;
    let case_id = Uuid::now_v7().to_string();
    let created = orchestrator
        .create_sandbox(create_request(Some(45), &[("case_id", case_id.as_str())]))
        .await?;
    let sandbox_id = created.id;

    let updated = orchestrator
        .keep_alive_for(sandbox_id, None, true)
        .await?
        .expect("keep_alive_for should return updated metadata");
    assert_eq!(updated.timeout, Some(Duration::from_secs(15)));
    assert_ne!(
        updated.expires_at, created.expires_at,
        "expires_at should be updated when timeout is reset"
    );

    let fetched = orchestrator
        .get_sandbox(&sandbox_id)
        .await?
        .expect("sandbox metadata should exist after keep_alive_for");
    assert_eq!(fetched.timeout, Some(Duration::from_secs(15)));
    assert_eq!(
        fetched.expires_at, updated.expires_at,
        "get_sandbox should reflect updated expires_at"
    );

    orchestrator.delete_sandbox(sandbox_id).await?;
    Ok(())
}

#[tokio::test]
async fn keep_alive_skips_shorter_timeout_when_disallowed() -> Result<()> {
    setup();
    let orchestrator = make_orchestrator().await;
    let case_id = Uuid::now_v7().to_string();
    let created = orchestrator
        .create_sandbox(create_request(Some(90), &[("case_id", case_id.as_str())]))
        .await?;
    let sandbox_id = created.id;

    let skipped = orchestrator
        .keep_alive_for(sandbox_id, Some(Duration::from_secs(10)), false)
        .await
        .expect("shorter timeout should be skipped when allow_shorter=false")
        .expect("sandbox metadata should be returned when update is skipped");
    assert_eq!(skipped.timeout, created.timeout);
    assert_eq!(skipped.expires_at, created.expires_at);

    let fetched = orchestrator
        .get_sandbox(&sandbox_id)
        .await?
        .expect("sandbox metadata should still exist after skipped keep_alive_for");
    assert_eq!(fetched.timeout, created.timeout);
    assert_eq!(fetched.expires_at, created.expires_at);

    let updated = orchestrator
        .keep_alive_for(sandbox_id, Some(Duration::from_secs(10)), true)
        .await?
        .expect("allow_shorter=true should allow timeout decrease");
    assert_eq!(updated.timeout, Some(Duration::from_secs(10)));

    orchestrator.delete_sandbox(sandbox_id).await?;
    Ok(())
}

#[tokio::test]
async fn keep_alive_uses_latest_metadata_when_deciding_whether_to_shorten() -> Result<()> {
    setup();
    let sandbox_id = SandboxId::new();
    let mut metadata = SandboxMetadata {
        id: sandbox_id,
        state: SandboxState::Running,
        ..Default::default()
    };
    metadata.set_timeout(Some(Duration::from_secs(30)));

    let store = RaceBeforeUpdateStore::new(Duration::from_secs(300));
    store.add(metadata).await?;
    let orchestrator = make_orchestrator_without_background(store);

    let updated = orchestrator
        .keep_alive_for(sandbox_id, Some(Duration::from_secs(5)), false)
        .await?
        .expect("keep_alive_for should return sandbox metadata");
    assert_eq!(updated.timeout, Some(Duration::from_secs(300)));

    let fetched = orchestrator
        .get_sandbox(&sandbox_id)
        .await?
        .expect("sandbox metadata should still exist after keep-alive");
    assert_eq!(fetched.timeout, Some(Duration::from_secs(300)));
    assert_eq!(fetched.expires_at, updated.expires_at);
    Ok(())
}

/// The ceiling clamps a renewal; it does not refuse one.
///
/// 🔴 The direction matters. Refusing an over-long renewal would hand a new 400
/// to every client that passes a generous timeout — and clients pass generous
/// timeouts because that is what the API has always accepted.
#[tokio::test]
async fn keep_alive_clamps_an_over_long_renewal_to_the_ceiling() -> Result<()> {
    setup();
    let sandbox_id = SandboxId::new();
    let created_at = SystemTime::now() - Duration::from_secs(60);
    let mut metadata = SandboxMetadata {
        id: sandbox_id,
        state: SandboxState::Running,
        created_at,
        max_lifetime: Some(Duration::from_secs(300)),
        // Running since it was created, so its window is pinned there and this
        // test can name the instant the ceiling falls on.
        running_since: Some(created_at),
        ..Default::default()
    };
    metadata.set_timeout(Some(Duration::from_secs(30)));

    let store = InMemoryMetadataStore::new();
    store.add(metadata).await?;
    let orchestrator = make_orchestrator_without_background(store);

    let updated = orchestrator
        .keep_alive_for(sandbox_id, Some(Duration::from_secs(3_600)), true)
        .await?
        .expect("keep_alive_for should return updated metadata");

    assert_eq!(
        updated.expires_at,
        Some(created_at + Duration::from_secs(300)),
        "the deadline may not be pushed past the ceiling"
    );
    // The request itself succeeded and recorded what was asked for.
    assert_eq!(updated.timeout, Some(Duration::from_secs(3_600)));
    Ok(())
}

/// The control face for the test above: the same call on a node with no
/// ceiling. Without this, "clamped to 300" could just as well be "the ceiling
/// was never consulted and 300 came from somewhere else".
#[tokio::test]
async fn keep_alive_without_a_ceiling_is_not_clamped() -> Result<()> {
    setup();
    let sandbox_id = SandboxId::new();
    let mut metadata = SandboxMetadata {
        id: sandbox_id,
        state: SandboxState::Running,
        created_at: SystemTime::now() - Duration::from_secs(60),
        max_lifetime: None,
        ..Default::default()
    };
    metadata.set_timeout(Some(Duration::from_secs(30)));

    let store = InMemoryMetadataStore::new();
    store.add(metadata).await?;
    let orchestrator = make_orchestrator_without_background(store);

    let updated = orchestrator
        .keep_alive_for(sandbox_id, Some(Duration::from_secs(3_600)), true)
        .await?
        .expect("keep_alive_for should return updated metadata");

    let deadline = updated.expires_at.expect("a deadline");
    assert!(
        deadline > SystemTime::now() + Duration::from_secs(3_000),
        "an uncapped sandbox keeps the full hour it asked for"
    );
    Ok(())
}

/// The one refusal the ceiling produces, and the reason `/timeout` and
/// `/refreshes` grew a 400 they never returned before.
///
/// 🔴 Only reachable as a unit test. End to end the window is under a second:
/// the eviction loop runs every `auto_evict_interval_ms` and tears down a
/// sandbox the moment it passes `expires_at`, which a clamped sandbox reaches
/// at the same instant it passes its ceiling.
#[tokio::test]
async fn keep_alive_refuses_a_sandbox_that_is_already_past_its_ceiling() {
    setup();
    let sandbox_id = SandboxId::new();
    let created_at = SystemTime::now() - Duration::from_secs(3_600);
    let metadata = SandboxMetadata {
        id: sandbox_id,
        state: SandboxState::Running,
        created_at,
        max_lifetime: Some(Duration::from_secs(300)),
        // 🔴 Running for the whole hour, not merely created an hour ago. That
        // is what puts it past a five-minute ceiling: paused time would not.
        running_since: Some(created_at),
        ..Default::default()
    };

    let store = InMemoryMetadataStore::new();
    store.add(metadata).await.expect("seed the store");
    let orchestrator = make_orchestrator_without_background(store);

    let err = orchestrator
        .keep_alive_for(sandbox_id, Some(Duration::from_secs(60)), true)
        .await
        .expect_err("a sandbox past its ceiling has no window left to extend into");

    match err {
        OrchestratorError::SandboxLifetimeExceeded { deadline, .. } => {
            assert_eq!(deadline, created_at + Duration::from_secs(300));
        }
        other => panic!("expected SandboxLifetimeExceeded, got {other:?}"),
    }
}

/// 🔴 QA F3. A sandbox paused for longer than the ceiling has to resume — and
/// stay resumed.
///
/// The first cut of the ceiling derived the deadline from `created_at` alone,
/// so this sequence returned a successful 201 and then handed the eviction loop
/// a Running sandbox whose `expires_at` was already in the past. Within one
/// `auto_evict_interval_ms` the sandbox was paused again (or deleted, for a
/// sandbox carrying that timeout action), and the client saw a resume that
/// worked followed by a sandbox that was dead.
///
/// Twenty-five hours of wall clock cannot be waited out in a unit test, and
/// they do not have to be: `created_at` is the whole of what the old model read
/// and it is set directly here. The paired control face below shows the ceiling
/// still bites when the budget is genuinely spent.
#[tokio::test]
async fn a_sandbox_paused_past_the_ceiling_resumes_and_survives_the_evictor() -> Result<()> {
    setup();
    let orchestrator = make_orchestrator().await;
    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[]))
        .await?;
    let sandbox_id = created.id;
    assert_proxy_ready(&orchestrator, &sandbox_id).await?;

    // Put the node's default ceiling on it, then pause.
    let mut running = orchestrator
        .get_sandbox(&sandbox_id)
        .await?
        .expect("a running sandbox");
    running.max_lifetime = Some(Duration::from_secs(86_400));
    orchestrator.store.update(running).await?;
    orchestrator.pause_sandbox(sandbox_id).await?;

    let mut paused = orchestrator
        .get_sandbox(&sandbox_id)
        .await?
        .expect("a paused sandbox");
    assert_eq!(paused.state, SandboxState::Paused);
    assert!(
        paused.running_elapsed < Duration::from_secs(60),
        "the pause charged the run it actually had, not the ceiling"
    );
    assert_eq!(
        paused.running_since, None,
        "a paused sandbox has no run in progress to charge"
    );

    // Twenty-five hours later.
    paused.created_at = SystemTime::now() - Duration::from_secs(90_000);
    orchestrator.store.update(paused).await?;

    let resumed = orchestrator
        .resume_sandbox(
            sandbox_id,
            NewTimeout::Set(Duration::from_secs(60)),
            test_claim(),
        )
        .await?;
    assert_eq!(resumed.state, SandboxState::Running);
    let deadline = resumed
        .expires_at
        .expect("a resumed sandbox has a deadline");
    assert!(
        deadline > SystemTime::now(),
        "resume handed back a deadline that had already passed"
    );

    // The half that actually kills the sandbox: the eviction loop reads
    // `expires_at` and state, and would tear this one down within the second.
    let evicted = orchestrator.evict_expired_sandboxes().await?;
    assert!(
        evicted.is_empty(),
        "a sandbox resumed inside its running budget must not be evicted: {evicted:?}"
    );
    assert_eq!(
        orchestrator
            .get_sandbox(&sandbox_id)
            .await?
            .expect("the sandbox is still there")
            .state,
        SandboxState::Running
    );

    orchestrator.delete_sandbox(sandbox_id).await?;
    Ok(())
}

/// 🔴 The control face for the test above, and the reason it proves anything.
///
/// Same sequence, same evictor, one difference: this sandbox really has spent
/// its running budget. It still resumes — §6.3 clamps rather than refuses — and
/// the evictor still takes it, which is what says the ceiling was not simply
/// switched off.
#[tokio::test]
async fn a_sandbox_that_has_spent_its_running_budget_is_still_evicted_after_a_resume() -> Result<()>
{
    setup();
    let orchestrator = make_orchestrator().await;
    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[]))
        .await?;
    let sandbox_id = created.id;
    assert_proxy_ready(&orchestrator, &sandbox_id).await?;

    let mut running = orchestrator
        .get_sandbox(&sandbox_id)
        .await?
        .expect("a running sandbox");
    running.max_lifetime = Some(Duration::from_secs(300));
    // Five minutes of budget, six minutes already burned by earlier runs.
    running.running_elapsed = Duration::from_secs(360);
    orchestrator.store.update(running).await?;
    orchestrator.pause_sandbox(sandbox_id).await?;

    let resumed = orchestrator
        .resume_sandbox(
            sandbox_id,
            NewTimeout::Set(Duration::from_secs(60)),
            test_claim(),
        )
        .await?;
    assert_eq!(resumed.state, SandboxState::Running);
    assert!(
        resumed.expires_at.expect("a deadline") <= SystemTime::now(),
        "an exhausted budget clamps the deadline into the past"
    );

    let evicted = orchestrator.evict_expired_sandboxes().await?;
    assert_eq!(
        evicted,
        vec![sandbox_id],
        "a sandbox with no running budget left is still evicted"
    );
    assert_eq!(
        orchestrator
            .get_sandbox(&sandbox_id)
            .await?
            .expect("the sandbox is still tracked")
            .state,
        SandboxState::Paused,
        "the configured timeout action is Pause"
    );

    orchestrator.delete_sandbox(sandbox_id).await?;
    Ok(())
}

/// The budget is spent by running and only by running, across as many
/// pause/resume cycles as it takes.
#[tokio::test]
async fn running_time_accumulates_across_pause_and_resume_cycles() -> Result<()> {
    setup();
    let orchestrator = make_orchestrator().await;
    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[]))
        .await?;
    let sandbox_id = created.id;
    assert_proxy_ready(&orchestrator, &sandbox_id).await?;

    orchestrator.pause_sandbox(sandbox_id).await?;
    let after_first = orchestrator
        .get_sandbox(&sandbox_id)
        .await?
        .expect("a paused sandbox")
        .running_elapsed;

    // Sitting paused costs nothing, however many times it is read.
    sleep(Duration::from_millis(30)).await;
    assert_eq!(
        orchestrator
            .get_sandbox(&sandbox_id)
            .await?
            .expect("a paused sandbox")
            .running_elapsed,
        after_first,
        "paused wall-clock time must not be charged"
    );

    orchestrator
        .resume_sandbox(sandbox_id, NewTimeout::UseExisting, test_claim())
        .await?;
    sleep(Duration::from_millis(30)).await;
    orchestrator.pause_sandbox(sandbox_id).await?;

    let after_second = orchestrator
        .get_sandbox(&sandbox_id)
        .await?
        .expect("a paused sandbox")
        .running_elapsed;
    assert!(
        after_second >= after_first + Duration::from_millis(25),
        "the second run has to be charged too: {after_first:?} then {after_second:?}"
    );

    orchestrator.delete_sandbox(sandbox_id).await?;
    Ok(())
}

/// A fork's child starts its budget over, the way it starts `created_at` over.
#[tokio::test]
async fn a_forked_child_does_not_inherit_the_parents_spent_budget() -> Result<()> {
    setup();
    let orchestrator = make_orchestrator().await;
    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[]))
        .await?;
    let sandbox_id = created.id;
    assert_proxy_ready(&orchestrator, &sandbox_id).await?;

    let mut parent = orchestrator
        .get_sandbox(&sandbox_id)
        .await?
        .expect("a running sandbox");
    parent.max_lifetime = Some(Duration::from_secs(300));
    // Nearly all of it spent: a child that cloned this would be evicted almost
    // at once.
    parent.running_elapsed = Duration::from_secs(290);
    orchestrator.store.update(parent).await?;

    let children = orchestrator
        .fork_sandbox(
            sandbox_id,
            ForkChildren::Fresh(1),
            NewTimeout::Set(Duration::from_secs(120)),
        )
        .await?;
    let child = children
        .into_iter()
        .next()
        .expect("one child")
        .expect("the child started");

    assert_eq!(child.running_elapsed, Duration::ZERO);
    assert_eq!(
        child.max_lifetime,
        Some(Duration::from_secs(300)),
        "the ceiling itself is inherited; only the spend is reset"
    );
    let deadline = child.expires_at.expect("a deadline");
    assert!(
        deadline > SystemTime::now() + Duration::from_secs(110),
        "the child keeps the two minutes it asked for, not the parent's last ten seconds"
    );

    orchestrator.delete_sandbox(child.id).await?;
    orchestrator.delete_sandbox(sandbox_id).await?;
    Ok(())
}

#[tokio::test]
async fn keep_alive_maps_update_conflict_to_invalid_state() {
    setup();
    let sandbox_id = SandboxId::new();
    let mut metadata = SandboxMetadata {
        id: sandbox_id,
        state: SandboxState::Running,
        ..Default::default()
    };
    metadata.set_timeout(Some(Duration::from_secs(60)));

    let orchestrator = make_orchestrator_without_background(ConflictOnUpdateStore::new(metadata));

    let err = orchestrator
        .keep_alive_for(sandbox_id, Some(Duration::from_secs(120)), true)
        .await
        .expect_err("state conflict from store should map to invalid sandbox state");

    assert!(matches!(
        err,
        OrchestratorError::InvalidSandboxState {
            state: SandboxState::Paused,
            ..
        }
    ));
}

#[tokio::test]
async fn maybe_update_running_timeout_maps_conflict_to_invalid_state() {
    setup();
    let sandbox_id = SandboxId::new();
    let metadata = SandboxMetadata {
        id: sandbox_id,
        state: SandboxState::Running,
        ..Default::default()
    };
    let orchestrator = make_orchestrator_without_background(ConflictOnUpdateStore::new(metadata));

    let err = orchestrator
        .maybe_update_running_timeout(sandbox_id, NewTimeout::Set(Duration::from_secs(30)))
        .await
        .expect_err("state conflict should map to invalid sandbox state");

    assert!(matches!(
        err,
        OrchestratorError::InvalidSandboxState {
            state: SandboxState::Paused,
            ..
        }
    ));
}

#[tokio::test]
async fn auto_evict_sandboxes_skips_non_running_and_non_expired_and_continues_on_error(
) -> Result<()> {
    setup();
    let store = InMemoryMetadataStore::new();
    let now = std::time::SystemTime::now();

    let running_id = SandboxId::new();
    let paused_id = SandboxId::new();
    let non_expired_id = SandboxId::new();

    let running_expired = SandboxMetadata {
        id: running_id,
        state: SandboxState::Running,
        created_at: now,
        timeout: Some(Duration::from_secs(1)),
        expires_at: now.checked_sub(Duration::from_secs(1)),
        ..Default::default()
    };
    let paused_expired = SandboxMetadata {
        id: paused_id,
        state: SandboxState::Paused,
        created_at: now,
        timeout: Some(Duration::from_secs(1)),
        expires_at: now.checked_sub(Duration::from_secs(1)),
        ..Default::default()
    };
    let non_expired = SandboxMetadata {
        id: non_expired_id,
        state: SandboxState::Running,
        created_at: now,
        timeout: None,
        expires_at: None,
        ..Default::default()
    };

    store.add(running_expired).await?;
    store.add(paused_expired).await?;
    store.add(non_expired).await?;

    let orchestrator = make_orchestrator_without_background(store);
    let paused_ids = orchestrator.evict_expired_sandboxes().await?;
    assert!(
        paused_ids.is_empty(),
        "running sandbox pause should fail without handle and be skipped"
    );

    let running = orchestrator.get_sandbox(&running_id).await?;
    assert!(
        running.is_none(),
        "expired, handle-less running sandbox should be removed"
    );

    let paused = orchestrator
        .get_sandbox(&paused_id)
        .await?
        .expect("paused metadata should remain");
    assert_eq!(paused.state, SandboxState::Paused);

    let non_expired = orchestrator
        .get_sandbox(&non_expired_id)
        .await?
        .expect("non-expired metadata should remain");
    assert_eq!(non_expired.state, SandboxState::Running);

    Ok(())
}

#[tokio::test]
async fn orchestrator_list_filtered_returns_empty_on_non_matching_metadata() -> Result<()> {
    setup();
    let orchestrator = make_orchestrator().await;
    let case_id = Uuid::now_v7().to_string();
    let created = orchestrator
        .create_sandbox(create_request(
            Some(30),
            &[
                ("team", "alpha"),
                ("owner", "alice"),
                ("case_id", case_id.as_str()),
            ],
        ))
        .await?;

    let mut required_metadata = HashMap::new();
    required_metadata.insert("team".to_string(), "beta".to_string());

    let filtered = orchestrator
        .list_sandboxes_filtered(SandboxListFilter {
            states: Some(vec![SandboxState::Running]),
            excluded_states: None,
            user_metadata: Some(required_metadata),
        })
        .await?;
    assert!(
        filtered.is_empty(),
        "non-matching metadata filter should return empty list"
    );

    orchestrator.delete_sandbox(created.id).await?;
    Ok(())
}

#[tokio::test]
async fn orchestrator_delete_paused_sandbox_removes_metadata() -> Result<()> {
    setup();
    let persister = RecordingPersister::default();
    let orchestrator = make_orchestrator_without_background_with_factory_and_persister(
        InMemoryMetadataStore::new(),
        MockBackendFactory::new(),
        persister.clone(),
    );
    let case_id = Uuid::now_v7().to_string();
    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[("case_id", case_id.as_str())]))
        .await?;
    let sandbox_id = created.id;

    orchestrator.pause_sandbox(sandbox_id).await?;
    assert_metrics_values(&orchestrator, 1, 0, 0, 0, 0, 0).await;

    let paused = orchestrator
        .get_sandbox(&sandbox_id)
        .await?
        .expect("sandbox metadata should exist in paused state");
    assert_eq!(paused.state, SandboxState::Paused);
    assert_eq!(
        paused
            .user_metadata
            .as_ref()
            .and_then(|metadata| metadata.get("case_id"))
            .map(String::as_str),
        Some(case_id.as_str())
    );
    assert_proxy_paused(&orchestrator, &sandbox_id).await?;

    orchestrator.delete_sandbox(sandbox_id).await?;
    assert_eq!(
        persister.calls(),
        vec![
            RecordingCall::AllocateArtifactRoot,
            RecordingCall::PersistPaused,
            RecordingCall::DeleteRecordAndArtifacts
        ]
    );
    assert!(orchestrator.get_sandbox(&sandbox_id).await?.is_none());
    assert_metrics_values(&orchestrator, 1, 0, 0, 0, 0, 0).await;
    assert_proxy_not_found(&orchestrator, &sandbox_id).await?;

    Ok(())
}

#[tokio::test]
async fn pause_failure_rolls_back_to_running_and_preserves_handle() -> Result<()> {
    setup();
    let behavior = Arc::new(MockBehavior::new());
    let resume_calls = Arc::new(StdMutex::new(0usize));
    let resume_calls_for_hook = Arc::clone(&resume_calls);
    behavior.set_on_operation(
        MockOperation::Resume,
        Arc::new(move || {
            *resume_calls_for_hook
                .lock()
                .expect("resume call counter mutex poisoned") += 1;
        }),
    );
    behavior.push_action(
        MockOperation::Pause,
        MockAction::Fail {
            message: "forced pause failure".to_string(),
        },
    );
    let orchestrator =
        make_orchestrator_with_factory(MockBackendFactory::with_behavior(Arc::clone(&behavior)))
            .await;

    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[("team", "pause-failure")]))
        .await?;
    let sandbox_id = created.id;
    assert_metrics_values(
        &orchestrator,
        1,
        0,
        1,
        0,
        created.resources.cpu_count,
        created.resources.memory_mib,
    )
    .await;

    let err = orchestrator
        .pause_sandbox(sandbox_id)
        .await
        .expect_err("pause should fail when backend pause fails");
    assert!(matches!(
        err,
        OrchestratorError::SandboxOperationFailed {
            operation: SandboxOperation::Pause,
            ..
        }
    ));

    let metadata = orchestrator
        .get_sandbox(&sandbox_id)
        .await?
        .expect("sandbox should still exist after failed pause");
    assert_eq!(metadata.state, SandboxState::Running);
    assert_metrics_values(
        &orchestrator,
        1,
        0,
        1,
        0,
        created.resources.cpu_count,
        created.resources.memory_mib,
    )
    .await;
    assert_eq!(
        *resume_calls
            .lock()
            .expect("resume call counter mutex poisoned"),
        1,
        "recoverable pause failure should resume the backend before returning"
    );

    orchestrator.delete_sandbox(sandbox_id).await?;
    Ok(())
}

#[tokio::test]
async fn pause_failure_with_failed_recovery_removes_sandbox() -> Result<()> {
    setup();
    let behavior = Arc::new(MockBehavior::new());
    behavior.push_action(
        MockOperation::Pause,
        MockAction::Fail {
            message: "forced pause failure".to_string(),
        },
    );
    behavior.push_action(
        MockOperation::Resume,
        MockAction::Fail {
            message: "forced resume failure".to_string(),
        },
    );
    let orchestrator =
        make_orchestrator_with_factory(MockBackendFactory::with_behavior(Arc::clone(&behavior)))
            .await;

    let created = orchestrator
        .create_sandbox(create_request(
            Some(60),
            &[("team", "pause-recovery-failure")],
        ))
        .await?;
    let sandbox_id = created.id;

    let err = orchestrator
        .pause_sandbox(sandbox_id)
        .await
        .expect_err("pause should fail terminally when the backend cannot recover");
    let message = format!("{err:#}");
    assert!(matches!(
        err,
        OrchestratorError::SandboxOperationFailed {
            operation: SandboxOperation::Pause,
            ..
        }
    ));
    assert!(message.contains("forced pause failure"), "{message}");
    assert!(message.contains("forced resume failure"), "{message}");

    assert!(orchestrator.get_sandbox(&sandbox_id).await?.is_none());
    assert_proxy_not_found(&orchestrator, &sandbox_id).await?;
    assert_eq!(behavior.stop_calls(), 1);
    assert_metrics_values(&orchestrator, 1, 0, 0, 0, 0, 0).await;
    Ok(())
}

#[tokio::test]
async fn concurrent_pause_when_leader_fails_maps_waiter_to_running_state() -> anyhow::Result<()> {
    setup();
    let behavior = Arc::new(MockBehavior::new());
    behavior.push_action(
        MockOperation::Pause,
        MockAction::FailAfter {
            delay: Duration::from_millis(150),
            message: "forced concurrent pause failure".to_string(),
        },
    );
    let orchestrator =
        make_orchestrator_with_factory(MockBackendFactory::with_behavior(behavior)).await;

    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[("team", "pause-join-failure")]))
        .await?;
    let sandbox_id = created.id;
    assert_metrics_values(
        &orchestrator,
        1,
        0,
        1,
        0,
        created.resources.cpu_count,
        created.resources.memory_mib,
    )
    .await;

    let orch1 = Arc::clone(&orchestrator);
    let orch2 = Arc::clone(&orchestrator);

    let h1 = tokio::spawn(async move { orch1.pause_sandbox(sandbox_id).await });
    sleep(Duration::from_millis(20)).await;
    let h2 = tokio::spawn(async move { orch2.pause_sandbox(sandbox_id).await });

    let r1 = h1.await.expect("pause leader should not panic");
    let r2 = h2.await.expect("pause waiter should not panic");

    let mut saw_pause_failed = false;
    let mut saw_waiter_running = false;
    for result in [r1, r2] {
        match result {
            Err(OrchestratorError::SandboxOperationFailed {
                operation: SandboxOperation::Pause,
                ..
            }) => saw_pause_failed = true,
            Err(OrchestratorError::InvalidSandboxState {
                state: SandboxState::Running,
                ..
            }) => saw_waiter_running = true,
            other => anyhow::bail!("unexpected concurrent pause result: {other:?}"),
        }
    }

    assert!(
        saw_pause_failed,
        "one caller should observe backend pause failure"
    );
    assert!(
        saw_waiter_running,
        "one caller should map joined pause outcome to InvalidSandboxState::Running"
    );
    assert_metrics_values(
        &orchestrator,
        1,
        0,
        1,
        0,
        created.resources.cpu_count,
        created.resources.memory_mib,
    )
    .await;

    orchestrator.delete_sandbox(sandbox_id).await?;
    Ok(())
}

#[tokio::test]
async fn concurrent_resume_when_leader_build_fails_is_consistent() -> anyhow::Result<()> {
    setup();
    let behavior = Arc::new(MockBehavior::new());
    behavior.push_action(
        MockOperation::BuildFromSnapshot,
        MockAction::FailAfter {
            delay: Duration::from_millis(250),
            message: "forced concurrent resume failure".to_string(),
        },
    );
    let orchestrator =
        make_orchestrator_with_factory(MockBackendFactory::with_behavior(behavior)).await;

    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[("team", "resume-join-failure")]))
        .await?;
    let id = created.id;
    orchestrator.pause_sandbox(id).await?;
    assert_metrics_values(&orchestrator, 1, 0, 0, 0, 0, 0).await;

    let orch1 = Arc::clone(&orchestrator);
    let orch2 = Arc::clone(&orchestrator);

    let h1 = tokio::spawn(async move {
        orch1
            .resume_sandbox(id, NewTimeout::UseExisting, test_claim())
            .await
    });
    sleep(Duration::from_millis(100)).await;
    let h2 = tokio::spawn(async move {
        orch2
            .resume_sandbox(id, NewTimeout::UseExisting, test_claim())
            .await
    });

    let r1 = h1.await.expect("resume leader should not panic");
    let r2 = h2.await.expect("resume waiter should not panic");

    let mut saw_launch_build_failed = false;
    for result in [r1, r2] {
        match result {
            Err(OrchestratorError::SandboxOperationFailed {
                operation: SandboxOperation::Build,
                ..
            }) => saw_launch_build_failed = true,
            Err(OrchestratorError::InvalidSandboxState {
                state: SandboxState::Paused,
                ..
            }) => {}
            Ok(metadata) if metadata.state == SandboxState::Running => {}
            other => anyhow::bail!("unexpected concurrent resume result: {other:?}"),
        }
    }
    assert!(
        saw_launch_build_failed,
        "at least one caller should observe launch build failure"
    );

    let final_state = orchestrator
        .get_sandbox(&id)
        .await?
        .expect("sandbox should still exist")
        .state;
    assert!(
        matches!(final_state, SandboxState::Paused | SandboxState::Running),
        "final state after concurrent resume failure race should be Paused or Running"
    );
    match final_state {
        SandboxState::Paused => assert_metrics_values(&orchestrator, 1, 0, 0, 0, 0, 0).await,
        SandboxState::Running => {
            assert_metrics_values(
                &orchestrator,
                1,
                0,
                1,
                0,
                created.resources.cpu_count,
                created.resources.memory_mib,
            )
            .await
        }
        _ => unreachable!(),
    }

    orchestrator.delete_sandbox(id).await?;
    Ok(())
}

#[tokio::test]
async fn resume_sandbox_start_failure_from_launch_rolls_back_to_paused_and_allows_retry(
) -> Result<()> {
    setup();
    let behavior = Arc::new(MockBehavior::new());
    let orchestrator =
        make_orchestrator_with_factory(MockBackendFactory::with_behavior(behavior.clone())).await;

    let created = orchestrator
        .create_sandbox(create_request(
            Some(60),
            &[("team", "resume-start-failure")],
        ))
        .await?;
    let sandbox_id = created.id;
    orchestrator.pause_sandbox(sandbox_id).await?;
    let paused_metrics = current_metrics(&orchestrator).await;
    assert_metrics_values(&orchestrator, 1, 0, 0, 0, 0, 0).await;
    behavior.push_action(
        MockOperation::StartNowait,
        MockAction::Fail {
            message: "forced resume start_nowait failure".to_string(),
        },
    );

    let err = orchestrator
        .resume_sandbox(sandbox_id, NewTimeout::UseExisting, test_claim())
        .await
        .expect_err("resume should fail when start_nowait fails after snapshot restore");
    assert!(matches!(
        err,
        OrchestratorError::SandboxOperationFailed {
            operation: SandboxOperation::Start,
            ..
        }
    ));

    let paused = orchestrator
        .get_sandbox(&sandbox_id)
        .await?
        .expect("sandbox metadata should remain after failed resume start");
    assert_eq!(paused.state, SandboxState::Paused);
    assert!(
        paused.paused_state.is_some(),
        "failed resume start should keep paused state for retry"
    );
    assert_metrics_snapshot(&orchestrator, &paused_metrics).await;

    let resumed = orchestrator
        .resume_sandbox(sandbox_id, NewTimeout::UseExisting, test_claim())
        .await?;
    assert_eq!(resumed.state, SandboxState::Running);
    assert_metrics_values(
        &orchestrator,
        1,
        0,
        1,
        0,
        created.resources.cpu_count,
        created.resources.memory_mib,
    )
    .await;

    orchestrator.delete_sandbox(sandbox_id).await?;
    Ok(())
}

#[tokio::test]
async fn resume_sandbox_wait_ready_failure_from_launch_rolls_back_to_paused_and_allows_retry(
) -> Result<()> {
    setup();
    let behavior = Arc::new(MockBehavior::new());
    let orchestrator =
        make_orchestrator_with_factory(MockBackendFactory::with_behavior(behavior.clone())).await;

    let created = orchestrator
        .create_sandbox(create_request(
            Some(60),
            &[("team", "resume-ready-failure")],
        ))
        .await?;
    let sandbox_id = created.id;
    orchestrator.pause_sandbox(sandbox_id).await?;
    let paused_metrics = current_metrics(&orchestrator).await;
    assert_metrics_values(&orchestrator, 1, 0, 0, 0, 0, 0).await;
    behavior.push_action(
        MockOperation::WaitForReady,
        MockAction::Fail {
            message: "forced resume wait_for_ready failure".to_string(),
        },
    );

    let err = orchestrator
        .resume_sandbox(sandbox_id, NewTimeout::UseExisting, test_claim())
        .await
        .expect_err("resume should fail when wait_for_ready fails after snapshot restore");
    assert!(matches!(
        err,
        OrchestratorError::SandboxOperationFailed {
            operation: SandboxOperation::WaitReady,
            ..
        }
    ));

    let paused = orchestrator
        .get_sandbox(&sandbox_id)
        .await?
        .expect("sandbox metadata should remain after failed resume readiness wait");
    assert_eq!(paused.state, SandboxState::Paused);
    assert!(
        paused.paused_state.is_some(),
        "failed resume readiness wait should keep paused state for retry"
    );
    assert_metrics_snapshot(&orchestrator, &paused_metrics).await;

    let resumed = orchestrator
        .resume_sandbox(sandbox_id, NewTimeout::UseExisting, test_claim())
        .await?;
    assert_eq!(resumed.state, SandboxState::Running);
    assert_metrics_values(
        &orchestrator,
        1,
        0,
        1,
        0,
        created.resources.cpu_count,
        created.resources.memory_mib,
    )
    .await;

    orchestrator.delete_sandbox(sandbox_id).await?;
    Ok(())
}

#[tokio::test]
async fn resume_marks_resuming_and_deletes_record_after_success() -> Result<()> {
    setup();
    let persister = RecordingPersister::default();
    let orchestrator = make_orchestrator_without_background_with_factory_and_persister(
        InMemoryMetadataStore::new(),
        MockBackendFactory::new(),
        persister.clone(),
    );
    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[("team", "resume-persist")]))
        .await?;
    orchestrator.pause_sandbox(created.id).await?;
    persister.clear_calls();

    orchestrator
        .resume_sandbox(created.id, NewTimeout::UseExisting, test_claim())
        .await?;

    assert_eq!(
        persister.calls(),
        vec![RecordingCall::MarkResuming, RecordingCall::DeleteRecord]
    );
    let metadata = orchestrator
        .get_sandbox(&created.id)
        .await?
        .expect("metadata should remain after resume");
    assert_eq!(metadata.state, SandboxState::Running);
    persister.clear_calls();

    orchestrator.delete_sandbox(created.id).await?;
    assert_eq!(
        persister.calls(),
        vec![RecordingCall::DeleteRecordAndArtifacts]
    );
    Ok(())
}

#[tokio::test]
async fn resume_mark_resuming_failure_restores_paused_metadata() -> Result<()> {
    setup();
    let persister = RecordingPersister::default();
    let orchestrator = make_orchestrator_without_background_with_factory_and_persister(
        InMemoryMetadataStore::new(),
        MockBackendFactory::new(),
        persister.clone(),
    );
    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[("team", "resume-mark-fail")]))
        .await?;
    orchestrator.pause_sandbox(created.id).await?;
    persister.clear_calls();
    persister.fail_next(RecordingCall::MarkResuming);

    let err = orchestrator
        .resume_sandbox(created.id, NewTimeout::UseExisting, test_claim())
        .await
        .expect_err("resume should fail when persister cannot mark record resuming");

    assert!(matches!(err, OrchestratorError::InternalError(_)));
    assert_eq!(persister.calls(), vec![RecordingCall::MarkResuming]);
    let metadata = orchestrator
        .get_sandbox(&created.id)
        .await?
        .expect("metadata should remain after resume mark failure");
    assert_eq!(metadata.state, SandboxState::Paused);
    assert!(metadata.paused_state.is_some());
    assert_proxy_paused(&orchestrator, &created.id).await?;
    assert_metrics_values(&orchestrator, 1, 0, 0, 0, 0, 0).await;
    Ok(())
}

#[tokio::test]
async fn resume_launch_failure_rolls_back_resuming_record() -> Result<()> {
    setup();
    let persister = RecordingPersister::default();
    let behavior = Arc::new(MockBehavior::new());
    let orchestrator = make_orchestrator_without_background_with_factory_and_persister(
        InMemoryMetadataStore::new(),
        MockBackendFactory::with_behavior(behavior.clone()),
        persister.clone(),
    );
    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[("team", "resume-launch-fail")]))
        .await?;
    orchestrator.pause_sandbox(created.id).await?;
    persister.clear_calls();
    behavior.push_action(
        MockOperation::WaitForReady,
        MockAction::Fail {
            message: "forced resume wait failure".to_string(),
        },
    );

    let err = orchestrator
        .resume_sandbox(created.id, NewTimeout::UseExisting, test_claim())
        .await
        .expect_err("resume should fail when restored sandbox does not become ready");

    assert!(matches!(
        err,
        OrchestratorError::SandboxOperationFailed {
            operation: SandboxOperation::WaitReady,
            ..
        }
    ));
    assert_eq!(
        persister.calls(),
        vec![RecordingCall::MarkResuming, RecordingCall::RollbackResuming]
    );
    let metadata = orchestrator
        .get_sandbox(&created.id)
        .await?
        .expect("metadata should remain after resume launch failure");
    assert_eq!(metadata.state, SandboxState::Paused);
    assert_proxy_paused(&orchestrator, &created.id).await?;
    assert_metrics_values(&orchestrator, 1, 0, 0, 0, 0, 0).await;
    Ok(())
}

#[tokio::test]
async fn delete_when_stop_fails_returns_error_and_allows_retry() -> Result<()> {
    setup();
    let behavior = Arc::new(MockBehavior::new());
    behavior.push_action(
        MockOperation::Stop,
        MockAction::Fail {
            message: "forced stop failure".to_string(),
        },
    );
    let orchestrator =
        make_orchestrator_with_factory(MockBackendFactory::with_behavior(behavior)).await;

    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[("team", "delete-stop-failure")]))
        .await?;
    let sandbox_id = created.id;
    let running_metrics = current_metrics(&orchestrator).await;
    assert_metrics_values(
        &orchestrator,
        1,
        0,
        1,
        0,
        created.resources.cpu_count,
        created.resources.memory_mib,
    )
    .await;

    let err = orchestrator
        .delete_sandbox(sandbox_id)
        .await
        .expect_err("delete should fail when backend stop fails");
    assert!(matches!(
        err,
        OrchestratorError::SandboxOperationFailed {
            operation: SandboxOperation::Stop,
            ..
        }
    ));

    assert!(
        orchestrator.get_sandbox(&sandbox_id).await?.is_some(),
        "metadata should still exist after failed delete"
    );
    assert_metrics_snapshot(&orchestrator, &running_metrics).await;

    orchestrator.delete_sandbox(sandbox_id).await?;
    assert!(orchestrator.get_sandbox(&sandbox_id).await?.is_none());
    assert_metrics_values(&orchestrator, 1, 0, 0, 0, 0, 0).await;
    Ok(())
}

#[tokio::test]
async fn resume_running_with_none_timeout_clears_timeout() -> Result<()> {
    setup();
    let orchestrator = make_orchestrator().await;
    let created = orchestrator
        .create_sandbox(create_request(
            Some(77),
            &[("team", "resume-running-none-timeout")],
        ))
        .await?;
    let sandbox_id = created.id;
    assert_metrics_values(
        &orchestrator,
        1,
        0,
        1,
        0,
        created.resources.cpu_count,
        created.resources.memory_mib,
    )
    .await;

    let resumed = orchestrator
        .resume_sandbox(sandbox_id, NewTimeout::None, test_claim())
        .await?;
    assert_eq!(resumed.state, SandboxState::Running);
    assert_eq!(resumed.timeout, None);
    assert_metrics_values(
        &orchestrator,
        1,
        0,
        1,
        0,
        created.resources.cpu_count,
        created.resources.memory_mib,
    )
    .await;

    orchestrator.delete_sandbox(sandbox_id).await?;
    Ok(())
}

#[tokio::test]
async fn resume_rejects_paused_sandbox_from_other_virtualization_mode_without_mutation() {
    use crate::virtualization::VirtualizationMode;

    setup();
    let sandbox_id = SandboxId::new();
    let node_mode = ConfigManager::global_config().virtualization_mode;
    let sandbox_mode = match node_mode {
        VirtualizationMode::Kvm => VirtualizationMode::Pvm,
        VirtualizationMode::Pvm => VirtualizationMode::Kvm,
    };
    let persister = RecordingPersister::with_loaded(vec![SandboxMetadata {
        id: sandbox_id,
        state: SandboxState::Paused,
        virtualization_mode: sandbox_mode,
        paused_state: None,
        ..Default::default()
    }]);
    let orchestrator = Orchestrator::new_inner(
        ServerRole::All,
        InMemoryMetadataStore::new(),
        MockBackendFactory::new(),
        persister.clone(),
        test_runtime_image_refs(),
    )
    .await
    .expect("orchestrator should retain incompatible paused metadata");

    let listed = orchestrator.list_sandboxes().await.expect("list metadata");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, sandbox_id);
    assert_eq!(listed[0].virtualization_mode, sandbox_mode);

    let error = orchestrator
        .resume_sandbox(sandbox_id, NewTimeout::UseExisting, test_claim())
        .await
        .expect_err("cross-mode paused sandbox must not resume");

    assert!(matches!(
        error,
        OrchestratorError::VirtualizationModeMismatch {
            resource,
            resource_mode,
            node_mode: actual_node_mode,
        } if resource == format!("paused sandbox {sandbox_id}")
            && resource_mode == sandbox_mode
            && actual_node_mode == node_mode
    ));
    let metadata = orchestrator
        .get_sandbox(&sandbox_id)
        .await
        .expect("get metadata")
        .expect("metadata remains visible");
    assert_eq!(metadata.state, SandboxState::Paused);
    assert_eq!(metadata.virtualization_mode, sandbox_mode);
    assert!(metadata.paused_state.is_none());
    assert_eq!(persister.calls(), vec![RecordingCall::LoadAll]);
}

#[tokio::test]
async fn auto_evict_expired_sandbox() -> anyhow::Result<()> {
    setup();
    let orchestrator = make_orchestrator().await;
    let case_id = Uuid::now_v7().to_string();
    let pause_request = create_request(Some(1), &[("case_id", case_id.as_str())]);
    let mut delete_request = pause_request.clone();
    delete_request.timeout_action = SandboxTimeoutAction::Delete;

    let to_pause = orchestrator.create_sandbox(pause_request).await?;
    let to_delete = orchestrator.create_sandbox(delete_request).await?;

    assert!(orchestrator.get_sandbox(&to_pause.id).await?.is_some());
    assert!(orchestrator.get_sandbox(&to_delete.id).await?.is_some());
    assert_metrics_values(
        &orchestrator,
        2,
        0,
        2,
        0,
        to_pause.resources.cpu_count + to_delete.resources.cpu_count,
        to_pause.resources.memory_mib + to_delete.resources.memory_mib,
    )
    .await;

    let auto_evict_timeout = Duration::from_secs(20);
    let poll_interval = Duration::from_millis(100);
    let deadline = std::time::Instant::now() + auto_evict_timeout;

    let mut paused = false;
    let mut deleted = false;
    while !(paused && deleted) {
        if !paused {
            let metadata = orchestrator
                .get_sandbox(&to_pause.id)
                .await?
                .expect("sandbox metadata should exist while waiting for auto-evict");

            if metadata.state == SandboxState::Paused {
                paused = true;
            }

            if std::time::Instant::now() >= deadline {
                orchestrator.delete_sandbox(to_pause.id).await?;
                return Err(anyhow::anyhow!(
                    "sandbox was not auto-paused before timeout, final state: {:?}",
                    metadata.state
                ));
            }
        }
        if !deleted {
            if orchestrator.get_sandbox(&to_delete.id).await?.is_none() {
                deleted = true;
            } else if std::time::Instant::now() >= deadline {
                orchestrator.delete_sandbox(to_delete.id).await?;
                return Err(anyhow::anyhow!(
                    "sandbox was not auto-deleted before timeout"
                ));
            }
        }

        sleep(poll_interval).await;
    }

    let paused_sandbox = orchestrator
        .get_sandbox(&to_pause.id)
        .await?
        .expect("sandbox should still exist after auto-pause");
    assert_eq!(paused_sandbox.state, SandboxState::Paused);
    assert_metrics_values(&orchestrator, 2, 0, 0, 0, 0, 0).await;
    assert_proxy_paused(&orchestrator, &to_pause.id).await?;

    orchestrator.delete_sandbox(to_pause.id).await?;
    assert!(orchestrator.get_sandbox(&to_pause.id).await?.is_none());
    assert_metrics_values(&orchestrator, 2, 0, 0, 0, 0, 0).await;
    assert_proxy_not_found(&orchestrator, &to_pause.id).await?;

    Ok(())
}

#[tokio::test]
async fn auto_evict_task_does_not_keep_orchestrator_alive() -> anyhow::Result<()> {
    setup();
    let orchestrator = make_orchestrator().await;
    let weak = Arc::downgrade(&orchestrator);

    drop(orchestrator);
    tokio::task::yield_now().await;

    assert!(weak.upgrade().is_none());
    Ok(())
}

#[tokio::test]
async fn shutdown_pauses_running_sandboxes_and_rejects_new_lifecycle_operations() -> Result<()> {
    setup();
    let persister = RecordingPersister::default();
    let orchestrator = make_orchestrator_without_background_with_factory_and_persister(
        InMemoryMetadataStore::new(),
        MockBackendFactory::new(),
        persister.clone(),
    );
    let first = orchestrator
        .create_sandbox(create_request(Some(60), &[("team", "shutdown-all-1")]))
        .await?;
    let second = orchestrator
        .create_sandbox(create_request(Some(60), &[("team", "shutdown-all-2")]))
        .await?;
    assert_metrics_values(
        &orchestrator,
        2,
        0,
        2,
        0,
        first.resources.cpu_count + second.resources.cpu_count,
        first.resources.memory_mib + second.resources.memory_mib,
    )
    .await;

    orchestrator.shutdown().await?;

    let sandboxes = orchestrator.list_sandboxes().await?;
    assert_eq!(sandboxes.len(), 2);
    assert!(sandboxes
        .iter()
        .all(|metadata| metadata.state == SandboxState::Paused));
    assert_metrics_values(&orchestrator, 2, 0, 0, 0, 0, 0).await;
    assert_proxy_paused(&orchestrator, &first.id).await?;
    assert_proxy_paused(&orchestrator, &second.id).await?;
    assert_eq!(
        persister.calls(),
        vec![
            RecordingCall::AllocateArtifactRoot,
            RecordingCall::PersistPaused,
            RecordingCall::AllocateArtifactRoot,
            RecordingCall::PersistPaused
        ]
    );

    let err = orchestrator
        .create_sandbox(create_request(Some(60), &[("team", "shutdown-reject")]))
        .await
        .expect_err("shutdown orchestrator should reject new sandbox creation");
    assert!(matches!(err, OrchestratorError::ShuttingDown));
    assert_metrics_values(&orchestrator, 2, 1, 0, 0, 0, 0).await;

    Ok(())
}

#[tokio::test]
async fn shutdown_succeeds_when_stop_after_pause_fails() -> Result<()> {
    setup();
    let behavior = Arc::new(MockBehavior::new());
    behavior.push_action(
        MockOperation::Stop,
        MockAction::Fail {
            message: "shutdown stop failure 1".to_string(),
        },
    );
    behavior.push_action(
        MockOperation::Stop,
        MockAction::Fail {
            message: "shutdown stop failure 2".to_string(),
        },
    );
    behavior.push_action(
        MockOperation::Stop,
        MockAction::Fail {
            message: "shutdown stop failure 3".to_string(),
        },
    );
    let orchestrator =
        make_orchestrator_with_factory(MockBackendFactory::with_behavior(behavior)).await;

    let created = orchestrator
        .create_sandbox(create_request(
            Some(60),
            &[("team", "shutdown-stop-failure")],
        ))
        .await?;
    let sandbox_id = created.id;

    orchestrator.shutdown().await?;

    let metadata = orchestrator
        .get_sandbox(&sandbox_id)
        .await?
        .expect("paused sandbox metadata should remain after shutdown");
    assert_eq!(metadata.state, SandboxState::Paused);

    orchestrator.delete_sandbox(sandbox_id).await?;
    Ok(())
}

#[tokio::test]
async fn shutdown_retries_pause_failures_and_preserves_sandbox_on_success() -> Result<()> {
    setup();
    let behavior = Arc::new(MockBehavior::new());
    behavior.push_action(
        MockOperation::Pause,
        MockAction::Fail {
            message: "shutdown pause failure".to_string(),
        },
    );
    let orchestrator =
        make_orchestrator_with_factory(MockBackendFactory::with_behavior(behavior)).await;

    let created = orchestrator
        .create_sandbox(create_request(
            Some(60),
            &[("team", "shutdown-pause-retry")],
        ))
        .await?;
    let sandbox_id = created.id;

    orchestrator.shutdown().await?;

    let metadata = orchestrator
        .get_sandbox(&sandbox_id)
        .await?
        .expect("sandbox metadata should remain after shutdown retry succeeds");
    assert_eq!(metadata.state, SandboxState::Paused);

    Ok(())
}

#[tokio::test]
async fn shutdown_returns_error_after_exhausting_pause_retries() -> Result<()> {
    setup();
    let behavior = Arc::new(MockBehavior::new());
    for pass in 1..=3 {
        behavior.push_action(
            MockOperation::Pause,
            MockAction::Fail {
                message: format!("shutdown pause failure pass {pass}"),
            },
        );
    }
    let orchestrator =
        make_orchestrator_with_factory(MockBackendFactory::with_behavior(behavior)).await;

    let created = orchestrator
        .create_sandbox(create_request(
            Some(60),
            &[("team", "shutdown-pause-failure")],
        ))
        .await?;

    let err = orchestrator
        .shutdown()
        .await
        .expect_err("shutdown should fail after exhausting pause retries");
    assert!(matches!(err, OrchestratorError::InternalError(_)));

    let metadata = orchestrator
        .get_sandbox(&created.id)
        .await?
        .expect("running sandbox metadata should remain after failed shutdown pause");
    assert_eq!(metadata.state, SandboxState::Running);

    Ok(())
}

#[tokio::test]
async fn shutdown_reuses_recorded_success_instead_of_running_cleanup_again() -> Result<()> {
    setup();
    let behavior = Arc::new(MockBehavior::new());
    behavior.push_action(
        MockOperation::Stop,
        MockAction::Fail {
            message: "shutdown memoized failure 1".to_string(),
        },
    );
    behavior.push_action(
        MockOperation::Stop,
        MockAction::Fail {
            message: "shutdown memoized failure 2".to_string(),
        },
    );
    behavior.push_action(
        MockOperation::Stop,
        MockAction::Fail {
            message: "shutdown memoized failure 3".to_string(),
        },
    );
    let orchestrator =
        make_orchestrator_with_factory(MockBackendFactory::with_behavior(behavior)).await;

    let created = orchestrator
        .create_sandbox(create_request(
            Some(60),
            &[("team", "shutdown-memoized-failure")],
        ))
        .await?;
    let sandbox_id = created.id;

    orchestrator.shutdown().await?;
    orchestrator.shutdown().await?;

    let metadata = orchestrator
        .get_sandbox(&sandbox_id)
        .await?
        .expect("paused sandbox metadata should remain after shutdown");
    assert_eq!(metadata.state, SandboxState::Paused);

    Ok(())
}

#[tokio::test]
async fn launch_sandbox_rejects_when_orchestrator_is_already_shutting_down() -> Result<()> {
    setup();
    let orchestrator = make_orchestrator_without_background_with_factory(
        InMemoryMetadataStore::new(),
        MockBackendFactory::new(),
    );
    orchestrator.is_shutting_down.store(true, Ordering::Release);

    let err = Arc::clone(&orchestrator)
        .launch_sandbox(create_launch_plan_with_resources(SandboxId::new()))
        .await
        .expect_err("launch_sandbox should reject when shutdown has already begun");

    assert!(matches!(err, OrchestratorError::ShuttingDown));
    assert!(orchestrator.store.list().await?.is_empty());
    assert_eq!(
        current_metrics(orchestrator.as_ref()).await,
        OrchestratorMetrics::default()
    );
    Ok(())
}

#[tokio::test]
async fn launch_sandbox_create_add_failure_stops_backend_and_reverts_metrics() -> Result<()> {
    setup();
    let store_control = Arc::new(ScriptedStoreControl::default());
    store_control.push_add_action(StoreAction::Fail(StoreError::Backend {
        source: anyhow::anyhow!("forced add failure"),
    }));
    let backend_control = Arc::new(MockBehavior::new());
    let orchestrator = make_orchestrator_without_background_with_factory(
        ScriptedStore::new(store_control),
        MockBackendFactory::with_behavior(backend_control.clone()),
    );

    let err = Arc::clone(&orchestrator)
        .launch_sandbox(create_launch_plan_with_resources(SandboxId::new()))
        .await
        .expect_err("launch_sandbox should fail when persisting creating metadata fails");

    assert!(matches!(err, OrchestratorError::StoreOperationFailed(_)));
    assert_eq!(backend_control.stop_calls(), 1);
    assert!(orchestrator.sandboxes.read().await.is_empty());
    assert!(orchestrator.store.list().await?.is_empty());
    assert_eq!(
        current_metrics(orchestrator.as_ref()).await,
        OrchestratorMetrics::default()
    );
    Ok(())
}

#[tokio::test]
async fn launch_sandbox_create_starts_timeout_after_ready() -> Result<()> {
    setup();
    let store_control = Arc::new(ScriptedStoreControl::default());
    let saw_creating_expiry = Arc::new(StdMutex::new(None));
    let saw_creating_expiry_for_hook = Arc::clone(&saw_creating_expiry);
    store_control.set_on_add(Arc::new(move |metadata| {
        *saw_creating_expiry_for_hook
            .lock()
            .expect("creating expiry mutex poisoned") = Some(metadata.expires_at);
    }));
    let orchestrator = make_orchestrator_without_background(ScriptedStore::new(store_control));

    let created = Arc::clone(&orchestrator)
        .launch_sandbox(create_launch_plan_with_resources(SandboxId::new()))
        .await?;

    assert_eq!(
        *saw_creating_expiry
            .lock()
            .expect("creating expiry mutex poisoned"),
        Some(None),
        "creating metadata should not start the timeout clock"
    );
    assert_eq!(created.timeout, Some(Duration::from_secs(15)));
    assert!(
        created.expires_at.is_some(),
        "running metadata should start the timeout clock"
    );

    orchestrator.delete_sandbox(created.id).await?;
    Ok(())
}

#[tokio::test]
async fn launch_sandbox_shutdown_just_after_start_stops_without_persisting_state() -> Result<()> {
    setup();
    let backend_control = Arc::new(MockBehavior::new());
    backend_control.push_action(
        MockOperation::StartNowait,
        MockAction::SucceedAfter(Duration::from_millis(10)),
    );
    let orchestrator = make_orchestrator_without_background_with_factory(
        InMemoryMetadataStore::new(),
        MockBackendFactory::with_behavior(backend_control.clone()),
    );
    let orchestrator_weak = Arc::downgrade(&orchestrator);
    backend_control.set_on_operation(
        MockOperation::StartNowait,
        Arc::new(move || {
            if let Some(orchestrator) = orchestrator_weak.upgrade() {
                orchestrator.is_shutting_down.store(true, Ordering::Release);
            }
        }),
    );

    let err = Arc::clone(&orchestrator)
        .launch_sandbox(create_launch_plan_with_resources(SandboxId::new()))
        .await
        .expect_err("launch_sandbox should stop after start if shutdown begins immediately");

    assert!(matches!(err, OrchestratorError::ShuttingDown));
    assert_eq!(backend_control.stop_calls(), 1);
    assert!(orchestrator.sandboxes.read().await.is_empty());
    assert!(orchestrator.store.list().await?.is_empty());
    assert_eq!(
        current_metrics(orchestrator.as_ref()).await,
        OrchestratorMetrics::default()
    );
    Ok(())
}

#[tokio::test]
async fn launch_sandbox_shutdown_before_wait_for_ready_rolls_back_create_state() -> Result<()> {
    setup();
    let store_control = Arc::new(ScriptedStoreControl::default());
    let backend_control = Arc::new(MockBehavior::new());
    let orchestrator = make_orchestrator_without_background_with_factory(
        ScriptedStore::new(store_control.clone()),
        MockBackendFactory::with_behavior(backend_control.clone()),
    );
    let orchestrator_weak = Arc::downgrade(&orchestrator);
    store_control.set_on_add(Arc::new(move |_| {
        if let Some(orchestrator) = orchestrator_weak.upgrade() {
            orchestrator.is_shutting_down.store(true, Ordering::Release);
        }
    }));

    let err = Arc::clone(&orchestrator)
        .launch_sandbox(create_launch_plan_with_resources(SandboxId::new()))
        .await
        .expect_err("launch_sandbox should roll back if shutdown starts before readiness wait");

    assert!(matches!(err, OrchestratorError::ShuttingDown));
    assert_eq!(backend_control.stop_calls(), 1);
    assert!(orchestrator.sandboxes.read().await.is_empty());
    assert!(orchestrator.store.list().await?.is_empty());
    assert_eq!(
        current_metrics(orchestrator.as_ref()).await,
        OrchestratorMetrics::default()
    );
    Ok(())
}

#[tokio::test]
async fn launch_sandbox_shutdown_after_ready_rolls_back_create_state() -> Result<()> {
    setup();
    let backend_control = Arc::new(MockBehavior::new());
    backend_control.push_action(
        MockOperation::WaitForReady,
        MockAction::SucceedAfter(Duration::from_millis(10)),
    );
    let orchestrator = make_orchestrator_without_background_with_factory(
        InMemoryMetadataStore::new(),
        MockBackendFactory::with_behavior(backend_control.clone()),
    );
    let orchestrator_weak = Arc::downgrade(&orchestrator);
    backend_control.set_on_operation(
        MockOperation::WaitForReady,
        Arc::new(move || {
            if let Some(orchestrator) = orchestrator_weak.upgrade() {
                orchestrator.is_shutting_down.store(true, Ordering::Release);
            }
        }),
    );

    let err = Arc::clone(&orchestrator)
        .launch_sandbox(create_launch_plan_with_resources(SandboxId::new()))
        .await
        .expect_err("launch_sandbox should roll back if shutdown starts after readiness");

    assert!(matches!(err, OrchestratorError::ShuttingDown));
    assert_eq!(backend_control.stop_calls(), 1);
    assert!(orchestrator.sandboxes.read().await.is_empty());
    assert!(orchestrator.store.list().await?.is_empty());
    assert_eq!(
        current_metrics(orchestrator.as_ref()).await,
        OrchestratorMetrics::default()
    );
    Ok(())
}

#[tokio::test]
async fn launch_sandbox_create_running_metadata_persist_failure_rolls_back() -> Result<()> {
    setup();
    let store_control = Arc::new(ScriptedStoreControl::default());
    store_control.push_update_if_state_action(StoreAction::Fail(StoreError::Backend {
        source: anyhow::anyhow!("forced running update failure"),
    }));
    let backend_control = Arc::new(MockBehavior::new());
    let orchestrator = make_orchestrator_without_background_with_factory(
        ScriptedStore::new(store_control),
        MockBackendFactory::with_behavior(backend_control.clone()),
    );

    let err = Arc::clone(&orchestrator)
        .launch_sandbox(create_launch_plan_with_resources(SandboxId::new()))
        .await
        .expect_err("launch_sandbox should fail when persisting running metadata fails");

    assert!(matches!(err, OrchestratorError::StoreOperationFailed(_)));
    assert_eq!(backend_control.stop_calls(), 1);
    assert!(orchestrator.sandboxes.read().await.is_empty());
    assert!(orchestrator.store.list().await?.is_empty());
    assert_eq!(
        current_metrics(orchestrator.as_ref()).await,
        OrchestratorMetrics::default()
    );
    Ok(())
}

#[tokio::test]
async fn launch_sandbox_create_missing_proxy_target_rolls_back_running_state() -> Result<()> {
    setup();
    let backend_control = Arc::new(MockBehavior::new());
    let orchestrator = make_orchestrator_without_background_with_factory(
        InMemoryMetadataStore::new(),
        MockBackendFactory::with_behavior_and_host_ip(backend_control.clone(), None),
    );

    let err = Arc::clone(&orchestrator)
        .launch_sandbox(create_launch_plan_with_resources(SandboxId::new()))
        .await
        .expect_err("launch_sandbox should roll back if ready sandbox has no proxy target");

    match err {
        OrchestratorError::InternalError(message) => {
            assert!(message.contains("missing host interaction IP"));
        }
        other => panic!("expected internal error for missing proxy target, got {other:?}"),
    }
    assert_eq!(backend_control.stop_calls(), 1);
    assert!(orchestrator.sandboxes.read().await.is_empty());
    assert!(orchestrator.store.list().await?.is_empty());
    assert_eq!(
        current_metrics(orchestrator.as_ref()).await,
        OrchestratorMetrics::default()
    );
    Ok(())
}

#[tokio::test]
async fn launch_sandbox_create_with_stale_handle_skips_proxy_route_publication() -> Result<()> {
    setup();
    let sandbox_id = SandboxId::new();
    let backend_control = Arc::new(MockBehavior::new());
    backend_control.push_action(
        MockOperation::WaitForReady,
        MockAction::SucceedAfter(Duration::from_millis(10)),
    );
    let orchestrator = make_orchestrator_without_background_with_factory(
        InMemoryMetadataStore::new(),
        MockBackendFactory::with_behavior(backend_control.clone()),
    );
    let replacement_control = Arc::new(MockBehavior::new());
    let replacement_handle: SandboxHandle =
        Arc::new(Mutex::new(Box::new(MockSandboxBackend::new_with_host_ip(
            replacement_control,
            Some(Ipv4Addr::new(127, 0, 0, 2)),
            ExecutionId::new(),
        ))));
    let sandbox_id_for_hook = sandbox_id;
    let replacement_for_hook = replacement_handle.clone();
    let orchestrator_weak = Arc::downgrade(&orchestrator);
    backend_control.set_on_operation(
        MockOperation::WaitForReady,
        Arc::new(move || {
            if let Some(orchestrator) = orchestrator_weak.upgrade() {
                let sandbox_id = sandbox_id_for_hook;
                let replacement = replacement_for_hook.clone();
                tokio::spawn(async move {
                    orchestrator
                        .sandboxes
                        .write()
                        .await
                        .insert(sandbox_id, replacement);
                });
            }
        }),
    );

    let metadata = Arc::clone(&orchestrator)
        .launch_sandbox(create_launch_plan_with_resources(sandbox_id))
        .await?;

    assert_eq!(metadata.state, SandboxState::Running);
    assert_eq!(
        orchestrator.proxy_lookup_for(&sandbox_id).await?,
        ProxyLookupResult::RouteMissing
    );
    let persisted = orchestrator
        .store
        .get(&sandbox_id)
        .await?
        .expect("running metadata should remain persisted");
    assert_eq!(persisted.state, SandboxState::Running);
    assert_eq!(
        current_metrics(orchestrator.as_ref())
            .await
            .running_sandbox_count,
        1
    );
    Ok(())
}

#[tokio::test]
async fn launch_sandbox_resume_running_metadata_persist_failure_restores_paused_state() -> Result<()>
{
    setup();
    let sandbox_id = SandboxId::new();
    let paused_metadata = paused_resume_metadata(sandbox_id);
    let mut resuming_metadata = paused_metadata.clone();
    resuming_metadata.state = SandboxState::Resuming;

    let store_control = Arc::new(ScriptedStoreControl::default());
    store_control.push_update_if_state_action(StoreAction::Fail(StoreError::Backend {
        source: anyhow::anyhow!("forced running update failure on resume"),
    }));
    let backend_control = Arc::new(MockBehavior::new());
    let orchestrator = make_orchestrator_without_background_with_factory(
        ScriptedStore::new(store_control),
        MockBackendFactory::with_behavior(backend_control.clone()),
    );
    orchestrator.store.add(resuming_metadata).await?;

    let err = Arc::clone(&orchestrator)
        .launch_sandbox(resume_launch_plan(sandbox_id))
        .await
        .expect_err("resume launch should fail when running metadata update fails");

    assert!(matches!(err, OrchestratorError::StoreOperationFailed(_)));
    assert_eq!(backend_control.stop_calls(), 1);
    assert!(orchestrator.sandboxes.read().await.is_empty());
    let restored = orchestrator
        .store
        .get(&sandbox_id)
        .await?
        .expect("resume failure should restore paused metadata");
    assert_eq!(restored.state, SandboxState::Paused);
    assert!(restored.paused_state.is_some());
    assert_proxy_paused(&orchestrator, &sandbox_id).await?;
    // The rolled-back paused sandbox still exists in the store, so derived
    // metrics include its paused_* contribution; only the runtime fields are
    // zero.
    assert_eq!(
        current_metrics(orchestrator.as_ref()).await,
        expected_metrics_with_one_paused_default()
    );
    Ok(())
}

#[tokio::test]
async fn launch_sandbox_resume_missing_proxy_target_restores_paused_state() -> Result<()> {
    setup();
    let sandbox_id = SandboxId::new();
    let paused_metadata = paused_resume_metadata(sandbox_id);
    let mut resuming_metadata = paused_metadata.clone();
    resuming_metadata.state = SandboxState::Resuming;

    let backend_control = Arc::new(MockBehavior::new());
    let orchestrator = make_orchestrator_without_background_with_factory(
        InMemoryMetadataStore::new(),
        MockBackendFactory::with_behavior_and_host_ip(backend_control.clone(), None),
    );
    orchestrator.store.add(resuming_metadata).await?;

    let err = Arc::clone(&orchestrator)
        .launch_sandbox(resume_launch_plan(sandbox_id))
        .await
        .expect_err("resume launch should fail when restored sandbox has no proxy target");

    match err {
        OrchestratorError::InternalError(message) => {
            assert!(message.contains("missing host interaction IP"));
        }
        other => panic!("expected internal error for missing proxy target, got {other:?}"),
    }
    assert_eq!(backend_control.stop_calls(), 1);
    assert!(orchestrator.sandboxes.read().await.is_empty());
    let restored = orchestrator
        .store
        .get(&sandbox_id)
        .await?
        .expect("resume failure should restore paused metadata");
    assert_eq!(restored.state, SandboxState::Paused);
    assert!(restored.paused_state.is_some());
    assert_proxy_paused(&orchestrator, &sandbox_id).await?;
    // See the analogous assertion in
    // launch_sandbox_resume_running_metadata_persist_failure_restores_paused_state.
    assert_eq!(
        current_metrics(orchestrator.as_ref()).await,
        expected_metrics_with_one_paused_default()
    );
    Ok(())
}

#[tokio::test]
async fn create_sandbox_records_failed_create_when_launch_start_fails() -> Result<()> {
    setup();
    let behavior = Arc::new(MockBehavior::new());
    behavior.push_action(
        MockOperation::StartNowait,
        MockAction::FailAfter {
            delay: Duration::from_millis(10),
            message: "forced start_nowait failure".to_string(),
        },
    );
    let orchestrator =
        make_orchestrator_with_factory(MockBackendFactory::with_behavior(behavior)).await;

    let err = orchestrator
        .create_sandbox(create_request(
            Some(30),
            &[("team", "create-start-failure")],
        ))
        .await
        .expect_err("create_sandbox should fail when start_nowait fails");

    assert!(matches!(
        err,
        OrchestratorError::SandboxOperationFailed {
            operation: SandboxOperation::Start,
            ..
        }
    ));

    assert!(
        orchestrator.list_sandboxes().await?.is_empty(),
        "failed create must not leave persisted sandbox metadata"
    );

    Ok(())
}

#[tokio::test]
async fn create_sandbox_records_failed_create_when_launch_wait_ready_fails() -> Result<()> {
    setup();
    let behavior = Arc::new(MockBehavior::new());
    behavior.push_action(
        MockOperation::WaitForReady,
        MockAction::Fail {
            message: "forced wait_for_ready failure".to_string(),
        },
    );
    let orchestrator =
        make_orchestrator_with_factory(MockBackendFactory::with_behavior(behavior)).await;

    let err = orchestrator
        .create_sandbox(create_request(
            Some(30),
            &[("team", "create-ready-failure")],
        ))
        .await
        .expect_err("create_sandbox should fail when wait_for_ready fails");

    assert!(matches!(
        err,
        OrchestratorError::SandboxOperationFailed {
            operation: SandboxOperation::WaitReady,
            ..
        }
    ));

    assert!(
        orchestrator.list_sandboxes().await?.is_empty(),
        "failed readiness must clean up in-memory handle and metadata"
    );

    Ok(())
}

#[tokio::test]
async fn create_sandbox_reports_build_failure_and_leaves_store_empty() -> Result<()> {
    setup();
    let behavior = Arc::new(MockBehavior::new());
    behavior.push_action(
        MockOperation::Build,
        MockAction::Fail {
            message: "forced build failure".to_string(),
        },
    );
    let orchestrator =
        make_orchestrator_with_factory(MockBackendFactory::with_behavior(behavior)).await;

    let err = orchestrator
        .create_sandbox(create_request(
            Some(30),
            &[("team", "create-build-failure")],
        ))
        .await
        .expect_err("create_sandbox should fail when build fails");

    assert!(matches!(
        err,
        OrchestratorError::SandboxOperationFailed {
            operation: SandboxOperation::Build,
            ..
        }
    ));
    assert!(orchestrator.list_sandboxes().await?.is_empty());

    Ok(())
}

/// A fork never lets a child inherit its parent's ownership marker.
///
/// 🔴 The marker is the control plane's record *of the parent*, and it names
/// the parent. A child that carried it would report itself to the control
/// plane under its parent's identity, so the reconcile that reads those
/// listings would be told the same sandbox is running twice — and the record
/// it rebuilt from the child would describe the wrong machine.
///
/// The control probe is the `Fresh` half: the parent is deliberately given a
/// marker, so a forwarding that copied the parent's metadata wholesale would
/// show up here as a child with one.
#[tokio::test]
async fn a_forked_child_never_inherits_its_parents_owner() -> Result<()> {
    setup();
    let behavior = Arc::new(MockBehavior::new());
    behavior.push_action(MockOperation::Build, MockAction::Succeed);
    let orchestrator =
        make_orchestrator_with_factory(MockBackendFactory::with_behavior(Arc::clone(&behavior)))
            .await;

    let mut request = create_request(Some(60), &[("team", "fork-ownership")]);
    request.control_plane_config = ControlPlaneConfig::from_bytes(b"the-parents-record".to_vec());
    let source = orchestrator.create_sandbox(request).await?;
    assert!(
        source.control_plane_config.is_some(),
        "the source must be owned, or this test cannot fail"
    );

    let outcomes = orchestrator
        .fork_sandbox(source.id, ForkChildren::Fresh(2), NewTimeout::UseExisting)
        .await?;
    for child in outcomes.into_iter().collect::<StdResult<Vec<_>, _>>()? {
        assert_eq!(
            child.control_plane_config, None,
            "a fork nobody claimed produced an owned child"
        );
    }
    Ok(())
}

/// An assigned fork gives each child the marker that was assigned to *it*.
///
/// 🔴 Position is the whole contract: `SandboxBackend::fork` returns one result
/// per spec in the same order, and this rides on that. A rotation by one would
/// still produce the right number of markers and the right set of them, so the
/// assertion has to pair each child's marker with that child's id.
#[tokio::test]
async fn an_assigned_fork_pairs_each_child_with_its_own_owner() -> Result<()> {
    setup();
    let behavior = Arc::new(MockBehavior::new());
    behavior.push_action(MockOperation::Build, MockAction::Succeed);
    let orchestrator =
        make_orchestrator_with_factory(MockBackendFactory::with_behavior(Arc::clone(&behavior)))
            .await;
    let source = orchestrator
        .create_sandbox(create_request(Some(60), &[("team", "fork-assigned")]))
        .await?;

    // The marker names the child it belongs to, which is the reason the caller
    // has to decide the child's id: it cannot write this before it knows one.
    let assigned = (0..3)
        .map(|_| {
            let sandbox_id = SandboxId::new();
            ForkChildAssignment {
                sandbox_id,
                execution_id: None,
                control_plane_config: ControlPlaneConfig::from_bytes(
                    format!("record-for-{sandbox_id}").into_bytes(),
                ),
            }
        })
        .collect::<Vec<_>>();

    let outcomes = orchestrator
        .fork_sandbox(
            source.id,
            ForkChildren::Assigned(assigned.clone()),
            NewTimeout::UseExisting,
        )
        .await?;
    let children = outcomes.into_iter().collect::<StdResult<Vec<_>, _>>()?;

    assert_eq!(children.len(), assigned.len());
    for (child, wanted) in children.iter().zip(&assigned) {
        assert_eq!(
            child.id, wanted.sandbox_id,
            "children came back out of order"
        );
        // 🔴 The incarnation is the node's to mint, and each child's must be
        // its own: the record it is cloned from carries the parent's.
        assert_ne!(child.execution_id, source.execution_id);
        assert_eq!(
            child.control_plane_config.as_ref().map(|c| c.as_bytes()),
            Some(format!("record-for-{}", child.id).as_bytes()),
            "child {} got another child's record",
            child.id
        );
    }
    Ok(())
}

#[tokio::test]
async fn fork_sandbox_creates_running_children_from_one_source() -> Result<()> {
    setup();
    let behavior = Arc::new(MockBehavior::new());
    behavior.push_action(MockOperation::Build, MockAction::Succeed);
    let orchestrator =
        make_orchestrator_with_factory(MockBackendFactory::with_behavior(Arc::clone(&behavior)))
            .await;
    let mut request = create_request(Some(60), &[("team", "batch-fork-source")]);
    request.secure = true;
    let source = orchestrator.create_sandbox(request).await?;
    assert!(source.secure);
    let source_token = orchestrator
        .get_envd_access_token(&source)
        .expect("secure source has a token");
    behavior.push_action(
        MockOperation::Build,
        MockAction::Fail {
            message: "fork should not use the fresh build path".to_string(),
        },
    );

    let outcomes = orchestrator
        .fork_sandbox(source.id, ForkChildren::Fresh(3), NewTimeout::UseExisting)
        .await?;
    let children = outcomes.into_iter().collect::<StdResult<Vec<_>, _>>()?;

    assert_eq!(children.len(), 3);
    let mut child_tokens = Vec::with_capacity(children.len());
    for child in &children {
        assert_ne!(child.id, source.id);
        assert_eq!(child.state, SandboxState::Running);
        assert_eq!(child.snapshot_id, source.snapshot_id);
        assert_eq!(child.user_metadata, source.user_metadata);
        assert_eq!(child.timeout, source.timeout);
        assert!(child.secure);
        let child_token = orchestrator
            .get_envd_access_token(child)
            .expect("secure child has a token");
        assert_ne!(child_token, source_token);
        assert!(!child_tokens.contains(&child_token));
        child_tokens.push(child_token);
        assert_proxy_ready(&orchestrator, &child.id).await?;
    }
    let source_after = orchestrator
        .get_sandbox(&source.id)
        .await?
        .expect("source sandbox should remain after batch fork");
    assert_eq!(source_after.state, SandboxState::Running);
    assert_eq!(source_after.timeout, source.timeout);
    assert_proxy_ready(&orchestrator, &source.id).await?;
    assert_metrics_values(
        &orchestrator,
        4,
        0,
        4,
        0,
        source_after.resources.cpu_count * 4,
        source_after.resources.memory_mib * 4,
    )
    .await;

    for child in children {
        orchestrator.delete_sandbox(child.id).await?;
    }
    orchestrator.delete_sandbox(source.id).await?;
    Ok(())
}

#[tokio::test]
async fn fork_sandbox_keeps_successful_siblings_when_one_start_fails() -> Result<()> {
    setup();
    let behavior = Arc::new(MockBehavior::new());
    let orchestrator =
        make_orchestrator_with_factory(MockBackendFactory::with_behavior(Arc::clone(&behavior)))
            .await;
    let source = orchestrator
        .create_sandbox(create_request(Some(60), &[("team", "partial-fork")]))
        .await?;

    behavior.push_action(MockOperation::ForkChild, MockAction::Succeed);
    behavior.push_action(
        MockOperation::ForkChild,
        MockAction::Fail {
            message: "injected child start failure".to_string(),
        },
    );
    behavior.push_action(MockOperation::ForkChild, MockAction::Succeed);

    let outcomes = orchestrator
        .fork_sandbox(
            source.id,
            ForkChildren::Fresh(3),
            NewTimeout::Set(Duration::from_secs(30)),
        )
        .await?;

    assert_eq!(outcomes.len(), 3);
    assert_eq!(outcomes.iter().filter(|outcome| outcome.is_ok()).count(), 2);
    let failed_child_id = match outcomes
        .iter()
        .find_map(|outcome| outcome.as_ref().err())
        .expect("one fork should fail")
    {
        OrchestratorError::SandboxOperationFailed {
            sandbox_id,
            operation: SandboxOperation::Fork,
            source,
        } => {
            assert!(source
                .chain()
                .any(|cause| cause.to_string().contains("injected child start failure")));
            *sandbox_id
        }
        err => panic!("unexpected fork outcome error: {err:?}"),
    };

    let successful_children = outcomes
        .into_iter()
        .filter_map(std::result::Result::ok)
        .collect::<Vec<_>>();
    assert!(successful_children
        .iter()
        .all(|child| child.id != failed_child_id));
    for child in &successful_children {
        assert_eq!(child.state, SandboxState::Running);
        assert_eq!(child.timeout, Some(Duration::from_secs(30)));
        assert_proxy_ready(&orchestrator, &child.id).await?;
    }
    let source_after = orchestrator
        .get_sandbox(&source.id)
        .await?
        .expect("source should remain after a child start failure");
    assert_eq!(source_after.state, SandboxState::Running);
    assert_eq!(source_after.timeout, source.timeout);
    assert_proxy_ready(&orchestrator, &source.id).await?;
    assert_metrics_values(
        &orchestrator,
        3,
        1,
        3,
        0,
        source_after.resources.cpu_count * 3,
        source_after.resources.memory_mib * 3,
    )
    .await;

    for child in successful_children {
        orchestrator.delete_sandbox(child.id).await?;
    }
    orchestrator.delete_sandbox(source.id).await?;
    Ok(())
}

// fork failure metrics tests
//
// These tests cover creation counters and live resource metrics when fork
// fails before child startup or when an individual child cannot be registered.

/// A recoverable pre-start failure records each requested child as failed while
/// leaving live resource metrics with only the source sandbox.
#[tokio::test]
async fn fork_sandbox_recoverable_failure_cleans_up_metrics() -> Result<()> {
    setup();
    let behavior = Arc::new(MockBehavior::new());
    let orchestrator =
        make_orchestrator_with_factory(MockBackendFactory::with_behavior(Arc::clone(&behavior)))
            .await;
    let source = orchestrator
        .create_sandbox(create_request(Some(60), &[("team", "fork-recoverable")]))
        .await?;

    // Make the fork fail recoverably.
    behavior.push_action(
        MockOperation::Fork,
        MockAction::Fail {
            message: "recoverable fork failure".to_string(),
        },
    );

    let err = orchestrator
        .fork_sandbox(
            source.id,
            ForkChildren::Fresh(3),
            NewTimeout::Set(Duration::from_secs(15)),
        )
        .await
        .expect_err("fork should fail when backend fork fails");
    assert!(matches!(
        err,
        OrchestratorError::SandboxOperationFailed {
            operation: SandboxOperation::Fork,
            ..
        }
    ));

    // Source sandbox must still be Running and metrics must be back to baseline.
    let source_after = orchestrator
        .get_sandbox(&source.id)
        .await?
        .expect("source sandbox should survive a recoverable fork failure");
    assert_eq!(source_after.state, SandboxState::Running);
    assert_proxy_ready(&orchestrator, &source.id).await?;
    assert_metrics_values(
        &orchestrator,
        1,
        3,
        1,
        0,
        source_after.resources.cpu_count,
        source_after.resources.memory_mib,
    )
    .await;

    orchestrator.delete_sandbox(source.id).await?;
    Ok(())
}

/// A terminal pre-start failure records each requested child as failed and
/// releases the source sandbox's resource contribution.
#[tokio::test]
async fn fork_sandbox_terminal_failure_removes_source_and_cleans_up_metrics() -> Result<()> {
    setup();
    let behavior = Arc::new(MockBehavior::new());
    let orchestrator =
        make_orchestrator_with_factory(MockBackendFactory::with_behavior(Arc::clone(&behavior)))
            .await;
    let source = orchestrator
        .create_sandbox(create_request(Some(60), &[("team", "fork-terminal")]))
        .await?;

    // Make the fork fail terminally.
    behavior.push_action(
        MockOperation::Fork,
        MockAction::FailTerminal {
            message: "terminal fork failure".to_string(),
        },
    );

    let err = orchestrator
        .fork_sandbox(
            source.id,
            ForkChildren::Fresh(3),
            NewTimeout::Set(Duration::from_secs(15)),
        )
        .await
        .expect_err("fork should fail terminally when backend fork is terminal");
    assert!(matches!(
        err,
        OrchestratorError::SandboxOperationFailed {
            operation: SandboxOperation::Fork,
            ..
        }
    ));

    // Source sandbox must be gone and all metrics zeroed.
    assert!(
        orchestrator.get_sandbox(&source.id).await?.is_none(),
        "source sandbox must be removed after a terminal fork failure"
    );
    assert_proxy_not_found(&orchestrator, &source.id).await?;
    assert_metrics_values(&orchestrator, 1, 3, 0, 0, 0, 0).await;

    Ok(())
}

/// A failure during one child registration is reported for that child without
/// rolling back a sibling that was registered successfully.
#[tokio::test]
async fn fork_sandbox_register_failure_cleans_up_metrics() -> Result<()> {
    setup();
    let control = Arc::new(ScriptedStoreControl::default());
    let store = ScriptedStore::new(Arc::clone(&control));
    let orchestrator =
        make_orchestrator_without_background_with_factory(store, MockBackendFactory::new());

    let source = orchestrator
        .create_sandbox(create_request(Some(60), &[("team", "fork-register-fail")]))
        .await?;
    let baseline = current_metrics(&orchestrator).await;

    // Inject a store failure for the first child registration add.
    control.push_add_action(StoreAction::Fail(StoreError::Backend {
        source: anyhow::anyhow!("injected store failure during child registration"),
    }));

    let outcomes = orchestrator
        .fork_sandbox(
            source.id,
            ForkChildren::Fresh(2),
            NewTimeout::Set(Duration::from_secs(15)),
        )
        .await?;
    assert_eq!(outcomes.len(), 2);
    assert!(matches!(
        &outcomes[0],
        Err(OrchestratorError::SandboxOperationFailed {
            operation: SandboxOperation::Fork,
            source,
            ..
        }) if source.chain().any(|cause| cause.to_string().contains(
            "injected store failure during child registration"
        ))
    ));
    let child = outcomes[1]
        .as_ref()
        .expect("the sibling should remain successful after registration failure");

    // Source sandbox must still be Running.
    let source_after = orchestrator
        .get_sandbox(&source.id)
        .await?
        .expect("source sandbox should survive a register failure");
    assert_eq!(source_after.state, SandboxState::Running);

    // The failed registration is counted without rolling back the sibling.
    assert_metrics_values(
        &orchestrator,
        baseline.create_successes + 1,
        baseline.create_fails + 1,
        2,
        0,
        source_after.resources.cpu_count + child.resources.cpu_count,
        source_after.resources.memory_mib + child.resources.memory_mib,
    )
    .await;

    orchestrator.delete_sandbox(child.id).await?;
    orchestrator.delete_sandbox(source.id).await?;
    Ok(())
}

#[tokio::test]
async fn discard_local_paused_record_drops_a_paused_sandbox() -> Result<()> {
    let orchestrator = make_orchestrator().await;
    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[]))
        .await?;
    orchestrator.pause_sandbox(created.id).await?;

    let discarded = orchestrator.discard_local_paused_record(created.id).await?;

    assert!(discarded, "a paused record should be discardable");
    assert!(
        orchestrator.store.get(&created.id).await?.is_none(),
        "the local record should be gone"
    );

    Ok(())
}

/// The dangerous direction: reconciliation runs against whatever the registry
/// reports, so a wrong answer must never be able to take down a live sandbox.
#[tokio::test]
async fn discard_local_paused_record_refuses_a_running_sandbox() -> Result<()> {
    let orchestrator = make_orchestrator().await;
    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[]))
        .await?;

    let discarded = orchestrator.discard_local_paused_record(created.id).await?;

    assert!(!discarded, "a running sandbox must never be discarded");
    let metadata = orchestrator
        .store
        .get(&created.id)
        .await?
        .expect("running sandbox should still be tracked");
    assert_eq!(metadata.state, SandboxState::Running);

    Ok(())
}

#[tokio::test]
async fn discard_local_paused_record_is_a_noop_for_unknown_sandboxes() -> Result<()> {
    let orchestrator = make_orchestrator().await;

    assert!(
        !orchestrator
            .discard_local_paused_record(SandboxId::new())
            .await?
    );

    Ok(())
}

/// Reconciliation reads "no row" as "this sandbox moved on". A registry that
/// tracks nothing answers that for every sandbox, so it must never be treated
/// as cluster-backed — otherwise the first reconciliation pass would discard
/// every paused sandbox on the node.
#[test]
fn disabled_registry_is_not_cluster_backed() {
    use crate::orchestrator::{DisabledPausedSandboxRegistry, PausedSandboxRegistry};

    assert!(!DisabledPausedSandboxRegistry.is_cluster_backed());
}

/// Records what the orchestrator asks of the cluster, so tests can assert that
/// a pause reached it without standing up a registry.
#[derive(Default)]
struct RecordingPublisher {
    published: StdMutex<Vec<(SandboxId, ExecutionId)>>,
    /// Both halves of the write, because "which sandbox" and "which run of it"
    /// are separate facts and only the second one can be got wrong.
    marked_running: StdMutex<Vec<(SandboxId, ExecutionId)>>,
    forgotten: StdMutex<Vec<SandboxId>>,
}

impl RecordingPublisher {
    fn published(&self) -> Vec<(SandboxId, ExecutionId)> {
        self.published.lock().unwrap().clone()
    }

    fn published_ids(&self) -> Vec<SandboxId> {
        self.published
            .lock()
            .unwrap()
            .iter()
            .map(|(sandbox_id, _)| *sandbox_id)
            .collect()
    }

    fn marked_running(&self) -> Vec<(SandboxId, ExecutionId)> {
        self.marked_running.lock().unwrap().clone()
    }

    fn marked_running_ids(&self) -> Vec<SandboxId> {
        self.marked_running
            .lock()
            .unwrap()
            .iter()
            .map(|(sandbox_id, _)| *sandbox_id)
            .collect()
    }

    fn forgotten(&self) -> Vec<SandboxId> {
        self.forgotten.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl crate::orchestrator::PausedSandboxPublisher for RecordingPublisher {
    async fn publish_paused(&self, outcome: crate::orchestrator::PauseOutcome) -> Option<String> {
        self.published
            .lock()
            .unwrap()
            .push((outcome.metadata.id, outcome.metadata.execution_id));

        Some("test-node".to_string())
    }

    async fn mark_running(
        &self,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
        _expires_at: Option<std::time::SystemTime>,
    ) {
        self.marked_running
            .lock()
            .unwrap()
            .push((sandbox_id, execution_id));
    }

    async fn forget(&self, sandbox_id: SandboxId) {
        self.forgotten.lock().unwrap().push(sandbox_id);
    }
}

async fn orchestrator_with_recording_publisher() -> (Arc<TestOrchestrator>, Arc<RecordingPublisher>)
{
    let orchestrator = make_orchestrator().await;
    let publisher = Arc::new(RecordingPublisher::default());
    orchestrator.set_paused_publisher(
        Arc::clone(&publisher) as Arc<dyn crate::orchestrator::PausedSandboxPublisher>
    );

    (orchestrator, publisher)
}

#[tokio::test]
async fn an_api_pause_is_published_to_the_cluster() -> Result<()> {
    let (orchestrator, publisher) = orchestrator_with_recording_publisher().await;
    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[]))
        .await?;

    orchestrator.pause_sandbox(created.id).await?;

    assert_eq!(publisher.published_ids(), vec![created.id]);

    Ok(())
}

/// The path that actually pauses most sandboxes. It used to drop the capture on
/// the floor, so an expired sandbox was resumable only on the node it happened
/// to expire on — and nobody is watching when that node is later lost.
#[tokio::test]
async fn an_expiry_auto_pause_is_published_to_the_cluster() -> Result<()> {
    let (orchestrator, publisher) = orchestrator_with_recording_publisher().await;
    let created = orchestrator
        .create_sandbox(create_request(Some(0), &[]))
        .await?;

    let evicted = orchestrator.evict_expired_sandboxes().await?;

    assert_eq!(evicted, vec![created.id], "the sandbox should have expired");
    assert_eq!(publisher.published_ids(), vec![created.id]);

    Ok(())
}

/// Shutdown pauses everything still running, and is the one case where the node
/// may genuinely never come back. Publishing here is the difference between a
/// decommissioned node's sandboxes surviving and evaporating.
#[tokio::test]
async fn a_shutdown_pause_is_published_to_the_cluster() -> Result<()> {
    let (orchestrator, publisher) = orchestrator_with_recording_publisher().await;
    let created = orchestrator
        .create_sandbox(create_request(Some(600), &[]))
        .await?;

    orchestrator.shutdown().await?;

    assert_eq!(publisher.published_ids(), vec![created.id]);

    Ok(())
}

/// A resume repoints the cluster record at this node. Without it the node that
/// paused the sandbox keeps advertising a copy it no longer owns.
#[tokio::test]
async fn a_resume_marks_the_sandbox_running_in_the_cluster() -> Result<()> {
    let (orchestrator, publisher) = orchestrator_with_recording_publisher().await;
    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[]))
        .await?;
    orchestrator.pause_sandbox(created.id).await?;

    orchestrator
        .resume_sandbox(created.id, NewTimeout::UseExisting, test_claim())
        .await?;

    assert_eq!(publisher.marked_running_ids(), vec![created.id]);

    Ok(())
}

/// The row and its snapshot outlive the paused period on purpose, so the delete
/// is the only thing that collects them. An expiry-driven delete has no API
/// call behind it and must clean up just as thoroughly.
#[tokio::test]
async fn a_delete_forgets_the_sandbox_in_the_cluster() -> Result<()> {
    let (orchestrator, publisher) = orchestrator_with_recording_publisher().await;
    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[]))
        .await?;

    orchestrator.delete_sandbox(created.id).await?;

    assert_eq!(publisher.forgotten(), vec![created.id]);

    Ok(())
}

/// 🔴 The mirror of the test above, and the reason the two teardown paths are
/// distinct at all. Discarding a superseded copy means the sandbox is alive on
/// another node; `forget_sandbox` would happily clear a row in any parked state
/// from any node, taking that node's snapshot — the sandbox's only recovery
/// point — with it.
#[tokio::test]
async fn discarding_a_superseded_copy_leaves_the_cluster_record_alone() -> Result<()> {
    let (orchestrator, publisher) = orchestrator_with_recording_publisher().await;
    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[]))
        .await?;

    orchestrator.discard_superseded_sandbox(created.id).await?;

    assert!(
        publisher.forgotten().is_empty(),
        "a superseded copy must not clear the cluster record"
    );
    assert!(
        orchestrator.get_sandbox(&created.id).await?.is_none(),
        "the local copy must be gone"
    );

    Ok(())
}

/// Isolation is about what arrives next. A node that has been taken out of
/// rotation must refuse work that would put another sandbox on it, and must
/// carry on serving everything it already holds — otherwise isolating a node
/// would be indistinguishable from breaking it.
#[tokio::test]
async fn an_isolated_node_refuses_new_sandboxes_and_keeps_serving_its_own() -> Result<()> {
    setup();
    let orchestrator = make_orchestrator().await;
    let case_id = Uuid::now_v7().to_string();
    let created = orchestrator
        .create_sandbox(create_request(Some(90), &[("case_id", case_id.as_str())]))
        .await?;

    assert!(
        orchestrator.set_scheduling_disabled(true),
        "isolating a node in rotation is a change"
    );
    assert!(
        !orchestrator.set_scheduling_disabled(true),
        "isolating an already isolated node changes nothing"
    );
    assert!(orchestrator.scheduling_disabled_changed_at_ms().is_some());

    let create_err = orchestrator
        .create_sandbox(create_request(Some(60), &[]))
        .await
        .expect_err("an isolated node must not take a new sandbox");
    assert!(matches!(create_err, OrchestratorError::NotAcceptingNewWork));

    let fork_err = Arc::clone(&orchestrator)
        .fork_sandbox(created.id, ForkChildren::Fresh(1), NewTimeout::UseExisting)
        .await
        .expect_err("a fork puts another sandbox on this node, so it is new work");
    assert!(matches!(fork_err, OrchestratorError::NotAcceptingNewWork));

    // Existing sandboxes are untouched: keeping one alive and putting it to
    // sleep are both things this node still owns.
    orchestrator
        .keep_alive_for(created.id, Some(Duration::from_secs(300)), false)
        .await?
        .expect("keep-alive must survive isolation");
    orchestrator.pause_sandbox(created.id).await?;

    assert!(
        orchestrator.set_scheduling_disabled(false),
        "clearing isolation is a change"
    );
    orchestrator
        .create_sandbox(create_request(Some(60), &[]))
        .await
        .expect("clearing isolation puts the node back in rotation");

    Ok(())
}

/// The orchestrator itself never refuses a resume: a paused sandbox that only
/// this node can rebuild has nowhere else to go, and refusing it here would
/// turn "this node is busy leaving" into "your sandbox is gone". Declining a
/// resume somebody else can serve is a routing decision, and lives in the API
/// layer where the registry can be consulted.
#[tokio::test]
async fn an_isolated_node_still_resumes_a_sandbox_it_alone_holds() -> Result<()> {
    setup();
    let orchestrator = make_orchestrator().await;
    let case_id = Uuid::now_v7().to_string();
    let created = orchestrator
        .create_sandbox(create_request(Some(90), &[("case_id", case_id.as_str())]))
        .await?;
    orchestrator.pause_sandbox(created.id).await?;

    orchestrator.set_scheduling_disabled(true);

    let resumed = orchestrator
        .resume_sandbox(created.id, NewTimeout::UseExisting, test_claim())
        .await
        .expect("an isolated node must still resume what only it can recover");
    assert_eq!(resumed.state, SandboxState::Running);

    Ok(())
}

// ── A1: incarnations ─────────────────────────────────────────────────────────
//
// Every assertion below reads the incarnation off the mock backend wherever it
// can, not off the metadata store. Reading the store would only show that the
// value written there is the value written there; reading the backend shows
// which incarnation the VM was actually started under.

/// T-A1-1. A pause and resume is a new run of the same machine, so it gets a
/// new incarnation — and the record says so too.
#[tokio::test]
async fn a_resume_runs_under_a_new_execution() -> Result<()> {
    setup();
    let orchestrator = make_orchestrator().await;
    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[]))
        .await?;

    let first = orchestrator
        .backend_execution_id_for_test(&created.id)
        .await
        .expect("a running sandbox has a backend");
    assert_eq!(created.execution_id, first);

    orchestrator.pause_sandbox(created.id).await?;
    let resumed = orchestrator
        .resume_sandbox(created.id, NewTimeout::UseExisting, test_claim())
        .await?;

    let second = orchestrator
        .backend_execution_id_for_test(&created.id)
        .await
        .expect("a resumed sandbox has a backend");
    assert_ne!(
        first, second,
        "a resume is a new run and must not reuse the paused run's incarnation"
    );
    assert_eq!(
        resumed.execution_id, second,
        "the record must name the incarnation the VM was started under"
    );
    assert_eq!(
        orchestrator
            .get_sandbox(&created.id)
            .await?
            .expect("resumed sandbox is tracked")
            .execution_id,
        second
    );

    orchestrator.delete_sandbox(created.id).await?;
    Ok(())
}

/// T-A1-2. Creating from a snapshot is a create. The backend below it boots
/// through `LaunchMode::Resume`, so anything that decided on the launch mode or
/// on the hook kind would call this a resume and hand out one incarnation for
/// every sandbox ever launched from that snapshot.
#[tokio::test]
async fn a_create_from_a_snapshot_is_a_new_execution_not_a_resume() -> Result<()> {
    setup();
    let orchestrator = make_orchestrator().await;

    let first = orchestrator
        .create_sandbox(create_request(Some(60), &[]))
        .await?;
    let second = orchestrator
        .create_sandbox(create_request(Some(60), &[]))
        .await?;

    assert_ne!(first.id, second.id);
    assert_ne!(
        first.execution_id, second.execution_id,
        "two sandboxes launched from one snapshot are two runs"
    );
    assert_eq!(
        orchestrator
            .backend_execution_id_for_test(&first.id)
            .await
            .expect("first sandbox has a backend"),
        first.execution_id
    );
    assert_eq!(
        orchestrator
            .backend_execution_id_for_test(&second.id)
            .await
            .expect("second sandbox has a backend"),
        second.execution_id
    );

    // The other half of the same statement: the plan itself says Create, which
    // is what the incarnation decision is taken on.
    let plan = create_launch_plan_with_resources(SandboxId::new());
    assert_eq!(plan.transitional_state(), SandboxState::Creating);

    orchestrator.delete_sandbox(first.id).await?;
    orchestrator.delete_sandbox(second.id).await?;
    Ok(())
}

/// T-A1-3. A snapshot pauses and resumes the VM in place. Same run, same
/// incarnation.
#[tokio::test]
async fn a_snapshot_does_not_change_the_execution() -> Result<()> {
    setup();
    let orchestrator = make_orchestrator().await;
    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[]))
        .await?;
    let before = orchestrator
        .backend_execution_id_for_test(&created.id)
        .await
        .expect("a running sandbox has a backend");

    orchestrator.capture_snapshot(created.id).await?;

    assert_eq!(
        orchestrator
            .backend_execution_id_for_test(&created.id)
            .await
            .expect("the sandbox is still running after a snapshot"),
        before
    );
    assert_eq!(
        orchestrator
            .get_sandbox(&created.id)
            .await?
            .expect("the sandbox is still tracked")
            .execution_id,
        before
    );

    orchestrator.delete_sandbox(created.id).await?;
    Ok(())
}

/// T-A1-4. Forking pauses and resumes the *parent* in place, so the parent is
/// still the same run.
#[tokio::test]
async fn forking_leaves_the_parent_execution_alone() -> Result<()> {
    setup();
    let orchestrator = make_orchestrator().await;
    let source = orchestrator
        .create_sandbox(create_request(Some(60), &[]))
        .await?;
    let before = orchestrator
        .backend_execution_id_for_test(&source.id)
        .await
        .expect("a running sandbox has a backend");

    let outcomes = orchestrator
        .fork_sandbox(source.id, ForkChildren::Fresh(2), NewTimeout::UseExisting)
        .await?;
    let children = outcomes.into_iter().collect::<StdResult<Vec<_>, _>>()?;

    assert_eq!(
        orchestrator
            .backend_execution_id_for_test(&source.id)
            .await
            .expect("the parent survives a fork"),
        before
    );
    assert_eq!(
        orchestrator
            .get_sandbox(&source.id)
            .await?
            .expect("the parent is still tracked")
            .execution_id,
        before
    );

    for child in children {
        orchestrator.delete_sandbox(child.id).await?;
    }
    orchestrator.delete_sandbox(source.id).await?;
    Ok(())
}

/// T-A1-5. 🔴 Each fork child is a brand-new sandbox and a brand-new run.
///
/// The child's record is built by cloning the parent's, so an incarnation that
/// is merely a field would be inherited — two live VMs under one identity, with
/// no warning of any kind.
#[tokio::test]
async fn every_fork_child_gets_its_own_execution() -> Result<()> {
    setup();
    let orchestrator = make_orchestrator().await;
    let source = orchestrator
        .create_sandbox(create_request(Some(60), &[]))
        .await?;
    let parent = orchestrator
        .backend_execution_id_for_test(&source.id)
        .await
        .expect("a running sandbox has a backend");

    let outcomes = orchestrator
        .fork_sandbox(source.id, ForkChildren::Fresh(3), NewTimeout::UseExisting)
        .await?;
    let children = outcomes.into_iter().collect::<StdResult<Vec<_>, _>>()?;
    assert_eq!(children.len(), 3);

    let mut seen = Vec::new();
    for child in &children {
        let recorded = child.execution_id;
        let on_backend = orchestrator
            .backend_execution_id_for_test(&child.id)
            .await
            .expect("a forked child has a backend");

        assert_eq!(
            recorded, on_backend,
            "the child's record must name the incarnation its VM was started under"
        );
        assert_ne!(
            recorded, parent,
            "a fork child must not inherit the parent's incarnation"
        );
        assert!(
            !seen.contains(&recorded),
            "fork children must not share an incarnation with each other"
        );
        seen.push(recorded);
    }

    for child in children {
        orchestrator.delete_sandbox(child.id).await?;
    }
    orchestrator.delete_sandbox(source.id).await?;
    Ok(())
}

/// T-A1-6. 🔴 What `mark_running` reports must be the incarnation the claim
/// allocated and the VM actually started under.
///
/// The controller's cross-node branch matches on exactly this value. Minting a
/// fresh one at report time, or reporting the one the record held before the
/// resume, makes every cross-node resume fail — and it fails silently, because
/// the write simply matches no row.
#[tokio::test]
async fn a_resume_reports_the_execution_it_actually_started() -> Result<()> {
    setup();
    let (orchestrator, publisher) = orchestrator_with_recording_publisher().await;
    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[]))
        .await?;
    let before = created.execution_id;

    orchestrator.pause_sandbox(created.id).await?;
    let claimed = test_claim();
    let allocated = claimed.execution_id();
    let resumed = orchestrator
        .resume_sandbox(created.id, NewTimeout::UseExisting, claimed)
        .await?;

    let started = orchestrator
        .backend_execution_id_for_test(&created.id)
        .await
        .expect("a resumed sandbox has a backend");
    assert_eq!(
        started, allocated,
        "the VM must run under the incarnation the claim allocated"
    );
    assert_eq!(resumed.execution_id, allocated);
    assert_ne!(started, before);

    let reported = publisher.marked_running();
    assert_eq!(reported, vec![(created.id, allocated)]);

    orchestrator.delete_sandbox(created.id).await?;
    Ok(())
}

/// T-A1-7. A paused record names the run that produced it — which is what
/// `begin_pause` quotes, and therefore what the registry fences the row against.
#[tokio::test]
async fn a_paused_record_carries_the_execution_that_produced_it() -> Result<()> {
    setup();
    let (orchestrator, publisher) = orchestrator_with_recording_publisher().await;
    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[]))
        .await?;
    let ran_as = orchestrator
        .backend_execution_id_for_test(&created.id)
        .await
        .expect("a running sandbox has a backend");

    let paused = orchestrator.pause_sandbox(created.id).await?;

    assert_eq!(paused.state, SandboxState::Paused);
    assert_eq!(
        paused.execution_id, ran_as,
        "pausing does not start a new run, so the record still names the old one"
    );
    assert_eq!(
        publisher.published(),
        vec![(created.id, ran_as)],
        "the pause published to the cluster must quote the run that produced it"
    );

    orchestrator.delete_sandbox(created.id).await?;
    Ok(())
}

/// A factory that answers the ownership-marker question the way a remote one
/// does, and keeps the launch configs it was handed.
///
/// 🔴 A wrapper rather than a change to `MockBackendFactory`: the question this
/// factory answers differently is the whole subject of the tests below, and a
/// shared mock that answered it `true` would make every other test in this file
/// exercise the stamping path without saying so.
struct StampingFactory {
    inner: MockBackendFactory,
    stamps: bool,
    seen: Arc<StdMutex<Vec<SandboxLaunchConfig>>>,
}

impl StampingFactory {
    fn new(stamps: bool) -> Self {
        Self {
            inner: MockBackendFactory::new(),
            stamps,
            seen: Arc::new(StdMutex::new(Vec::new())),
        }
    }

    fn seen(&self) -> Arc<StdMutex<Vec<SandboxLaunchConfig>>> {
        Arc::clone(&self.seen)
    }
}

impl SandboxBackendFactory for StampingFactory {
    fn stamps_control_plane_ownership(&self) -> bool {
        self.stamps
    }

    fn build(
        &self,
        build_spec: crate::sandbox::FreshSandboxBuildSpec,
        launch_config: SandboxLaunchConfig,
        execution_id: crate::types::ExecutionId,
    ) -> anyhow::Result<Box<dyn crate::sandbox::SandboxBackend>> {
        self.seen.lock().unwrap().push(launch_config.clone());
        self.inner.build(build_spec, launch_config, execution_id)
    }

    fn build_from_snapshot(
        &self,
        snapshot: &RunnableSnapshot,
        launch_config: SandboxLaunchConfig,
        execution_id: crate::types::ExecutionId,
    ) -> anyhow::Result<Box<dyn crate::sandbox::SandboxBackend>> {
        self.seen.lock().unwrap().push(launch_config.clone());
        self.inner
            .build_from_snapshot(snapshot, launch_config, execution_id)
    }

    fn decode_paused_state(
        &self,
        artifact_root: PathBuf,
        state: serde_json::Value,
    ) -> anyhow::Result<Arc<dyn PausedSandboxState>> {
        self.inner.decode_paused_state(artifact_root, state)
    }

    fn build_from_paused_state(
        &self,
        sandbox_id: SandboxId,
        execution_id: crate::types::ExecutionId,
        state: &dyn PausedSandboxState,
        envd_access_token: Option<crate::sandbox::EnvdAccessToken>,
    ) -> anyhow::Result<Box<dyn crate::sandbox::SandboxBackend>> {
        self.inner
            .build_from_paused_state(sandbox_id, execution_id, state, envd_access_token)
    }
}

/// The marker the record carries and the marker the backend is handed are the
/// same bytes, and they decode back to the record they describe.
///
/// 🔴 Three separate assertions and not one, because each failure is different
/// and only one of them is visible from the outside. A record with no marker is
/// a sandbox the control plane will not recognise as its own; a launch config
/// with no marker is a *node* that will not report it; and a marker that
/// decodes to a different sandbox is a rebuild that produces a plausible record
/// of something else.
#[tokio::test]
async fn a_control_plane_orchestrator_stamps_its_own_record_onto_the_create() {
    setup();
    let factory = StampingFactory::new(true);
    let seen = factory.seen();
    let orchestrator = Orchestrator::new_inner(
        ServerRole::All,
        InMemoryMetadataStore::new(),
        factory,
        DisabledSandboxPersister,
        test_runtime_image_refs(),
    )
    .await
    .expect("orchestrator");

    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[("case", "stamped")]))
        .await
        .expect("create");

    let marker = created
        .control_plane_config
        .as_ref()
        .expect("the record carries the control plane's marker");

    let on_the_wire = seen.lock().unwrap();
    let on_the_wire = on_the_wire
        .first()
        .expect("the factory was handed a launch config")
        .control_plane_config
        .clone()
        .expect("the launch config carries the same marker");
    assert_eq!(
        on_the_wire,
        marker.as_bytes(),
        "the node would store a different marker from the one the record names"
    );

    let decoded = marker.decode_record().expect("the marker decodes");
    assert_eq!(decoded.id, created.id);
    // 🔴 The incarnation in particular. It is minted below the surface that
    // decided to create anything, so a marker built any earlier would name a
    // run that had not been chosen — and fencing compares exactly this value.
    assert_eq!(decoded.execution_id, created.execution_id);
    assert_eq!(
        decoded.user_metadata, created.user_metadata,
        "the marker is the record, not a summary of it"
    );
    assert!(
        decoded.control_plane_config.is_none(),
        "the marker must not contain a copy of itself"
    );
}

/// 🔴 The control probe for the test above, and the one that keeps `--role all`
/// honest. Everything is identical except the factory's answer to one question;
/// if the stamping were unconditional, the test above would still pass and the
/// user-facing REST surface would start producing sandboxes that claim to
/// belong to a control plane.
#[tokio::test]
async fn a_machine_local_orchestrator_stamps_nothing() {
    setup();
    let factory = StampingFactory::new(false);
    let seen = factory.seen();
    let orchestrator = Orchestrator::new_inner(
        ServerRole::All,
        InMemoryMetadataStore::new(),
        factory,
        DisabledSandboxPersister,
        test_runtime_image_refs(),
    )
    .await
    .expect("orchestrator");

    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[("case", "unstamped")]))
        .await
        .expect("create");

    assert!(
        created.control_plane_config.is_none(),
        "a user-facing create must not look like the control plane's"
    );
    let on_the_wire = seen.lock().unwrap();
    let launch_config = on_the_wire
        .first()
        .expect("the factory was handed a launch config");
    assert!(
        launch_config.control_plane_config.is_none(),
        "nothing may reach a backend that would make a node call this sandbox the control \
         plane's"
    );
    // 🔴 And the evidence that this create happened at all, so the two
    // emptiness assertions above are not both satisfied by a create that never
    // ran (§15.4: an assertion that something is empty is only evidence if
    // something else in the same test is not).
    assert_eq!(launch_config.sandbox_id, created.id);
    assert_eq!(created.state, SandboxState::Running);
}

/// A marker supplied by a caller is kept, not overwritten.
///
/// 🔴 This is the node service's create: the marker arrived from the control
/// plane and this process is not it. An orchestrator that re-stamped would hand
/// the control plane back a record it never wrote, under its own sandbox's id.
#[tokio::test]
async fn a_marker_the_caller_supplied_survives_the_stamp() {
    setup();
    let factory = StampingFactory::new(true);
    let seen = factory.seen();
    let orchestrator = Orchestrator::new_inner(
        ServerRole::All,
        InMemoryMetadataStore::new(),
        factory,
        DisabledSandboxPersister,
        test_runtime_image_refs(),
    )
    .await
    .expect("orchestrator");

    let supplied = ControlPlaneConfig::from_bytes(b"somebody else's record".to_vec())
        .expect("a non-empty marker");
    let created = orchestrator
        .create_sandbox(CreateSandboxRequest {
            control_plane_config: Some(supplied.clone()),
            ..create_request(Some(60), &[])
        })
        .await
        .expect("create");

    assert_eq!(
        created
            .control_plane_config
            .as_ref()
            .expect("the supplied marker")
            .as_bytes(),
        supplied.as_bytes()
    );
    assert_eq!(
        seen.lock().unwrap()[0]
            .control_plane_config
            .as_deref()
            .expect("the supplied marker on the wire"),
        supplied.as_bytes()
    );
}
