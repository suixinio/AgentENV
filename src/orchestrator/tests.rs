use std::collections::{HashMap, VecDeque};
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::result::Result as StdResult;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::time::SystemTime;

use async_trait::async_trait;
use serde_json::json;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};
use uuid::Uuid;

use super::super::launch_plan::LaunchPlan;
use super::super::pause_publisher::{DiscardingPausePublisher, PausePublisher};
use super::super::types::{PublishedPause, SandboxLaunchSource};
use super::*;
use crate::image::{RecordingRuntimeImageRefs, RuntimeImageRefs};
use crate::runtime_snapshot::RunnableSnapshot;
use crate::sandbox::mock::{
    MockAction, MockBackendFactory, MockBehavior, MockOperation, MockSandboxBackend,
};
use crate::sandbox::{
    BaseSandboxNetworkPolicy, CapturedSandboxSnapshot, SandboxLaunchConfig,
    SandboxNetworkEgressPolicy, SandboxNetworkPolicy, SandboxRuntimeInfo,
};
use crate::types::{ImageConfigs, SandboxId, SandboxResources};

const STATE_POLL_INTERVAL: Duration = Duration::from_millis(50);

type TestOrchestrator<S = InMemoryMetadataStore, F = MockBackendFactory> = Orchestrator<S, F>;

fn setup() {
    crate::logging::init_for_tests();
}

fn test_runtime_image_refs() -> Arc<dyn RuntimeImageRefs> {
    Arc::new(RecordingRuntimeImageRefs::default())
}

async fn make_orchestrator() -> Arc<TestOrchestrator> {
    make_orchestrator_with_factory(MockBackendFactory::new()).await
}

async fn make_orchestrator_with_factory(factory: MockBackendFactory) -> Arc<TestOrchestrator> {
    make_orchestrator_with_publisher(factory, DiscardingPausePublisher::shared()).await
}

async fn make_orchestrator_with_publisher<F: SandboxBackendFactory>(
    factory: F,
    publisher: Arc<dyn PausePublisher>,
) -> Arc<TestOrchestrator<InMemoryMetadataStore, F>> {
    let orchestrator = Orchestrator::new(
        crate::sandbox::AccessTokenSeedPolicy::MayGenerate,
        InMemoryMetadataStore::new(),
        factory,
        test_runtime_image_refs(),
    )
    .await
    .expect("in-memory orchestrator should not fail to construct");
    orchestrator.set_pause_publisher(publisher);
    orchestrator
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
    make_orchestrator_without_background_parts(store, factory, TEST_DEFAULT_SANDBOX_TIMEOUT)
}

const TEST_DEFAULT_SANDBOX_TIMEOUT: Duration = Duration::from_secs(15);

fn make_orchestrator_without_background_with_default_timeout(
    default_sandbox_timeout: Duration,
) -> Arc<TestOrchestrator> {
    make_orchestrator_without_background_parts(
        InMemoryMetadataStore::new(),
        MockBackendFactory::new(),
        default_sandbox_timeout,
    )
}

fn make_orchestrator_without_background_parts<
    S: MetadataStore + 'static,
    F: SandboxBackendFactory,
>(
    store: S,
    factory: F,
    default_sandbox_timeout: Duration,
) -> Arc<TestOrchestrator<S, F>> {
    let orchestrator = make_orchestrator_without_publisher(store, factory, default_sandbox_timeout);
    orchestrator.set_pause_publisher(DiscardingPausePublisher::shared());
    orchestrator
}

/// An orchestrator with nowhere to publish a pause.
fn make_orchestrator_without_publisher<S: MetadataStore + 'static, F: SandboxBackendFactory>(
    store: S,
    factory: F,
    default_sandbox_timeout: Duration,
) -> Arc<TestOrchestrator<S, F>> {
    Arc::new(Orchestrator::from_test_parts(
        store,
        factory,
        default_sandbox_timeout,
        test_runtime_image_refs(),
        "orchestrator-test-seed",
    ))
}

struct FailingPausePublisher;

/// Fails its first `failures` publications, then succeeds.
struct FlakyPausePublisher {
    failures: usize,
    attempts: AtomicUsize,
}

impl FlakyPausePublisher {
    fn new(failures: usize) -> Arc<Self> {
        Arc::new(Self {
            failures,
            attempts: AtomicUsize::new(0),
        })
    }

    fn attempts(&self) -> usize {
        self.attempts.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl PausePublisher for FlakyPausePublisher {
    async fn publish(
        &self,
        _metadata: &SandboxMetadata,
        _capture: CapturedSandboxSnapshot,
    ) -> anyhow::Result<PublishedPause> {
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
        if attempt < self.failures {
            return Err(anyhow::anyhow!("the catalog is unreachable"));
        }
        Ok(PublishedPause::Committed(
            crate::snapshot::SnapshotId::generate(),
        ))
    }
}

#[async_trait]
impl PausePublisher for FailingPausePublisher {
    async fn publish(
        &self,
        _metadata: &SandboxMetadata,
        _capture: CapturedSandboxSnapshot,
    ) -> anyhow::Result<PublishedPause> {
        Err(anyhow::anyhow!("the catalog is unreachable"))
    }
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
    remove_actions: StdMutex<VecDeque<StoreAction>>,
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

    fn push_remove_action(&self, action: StoreAction) {
        self.remove_actions
            .lock()
            .expect("remove actions mutex poisoned")
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
        match ScriptedStoreControl::take_action(&self.control.remove_actions) {
            StoreAction::Delegate => self.inner.remove(sandbox_id).await,
            StoreAction::Fail(err) => Err(err),
        }
    }

    async fn remove_if_execution(
        &self,
        sandbox_id: &SandboxId,
        expected_execution_id: crate::types::ExecutionId,
        expected_states: &[SandboxState],
    ) -> StdResult<FencedRemoval, StoreError> {
        self.inner
            .remove_if_execution(sandbox_id, expected_execution_id, expected_states)
            .await
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
    current: Option<SandboxMetadata>,
}

impl ScriptedWaitStore {
    fn with_wait(script: WaitScript) -> Self {
        Self {
            next_wait: Mutex::new(Some(script)),
            current: None,
        }
    }

    fn holding(mut self, metadata: SandboxMetadata) -> Self {
        self.current = Some(metadata);
        self
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

    async fn remove_if_execution(
        &self,
        sandbox_id: &SandboxId,
        expected_execution_id: crate::types::ExecutionId,
        expected_states: &[SandboxState],
    ) -> StdResult<FencedRemoval, StoreError> {
        self.inner
            .remove_if_execution(sandbox_id, expected_execution_id, expected_states)
            .await
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
            actual_state: SandboxState::Pausing,
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

    async fn remove_if_execution(
        &self,
        _sandbox_id: &SandboxId,
        expected_execution_id: crate::types::ExecutionId,
        expected_states: &[SandboxState],
    ) -> StdResult<FencedRemoval, StoreError> {
        let mut slot = self.metadata.lock().await;
        let Some(metadata) = slot.as_ref() else {
            return Ok(FencedRemoval::Absent);
        };
        if metadata.execution_id != expected_execution_id
            || !expected_states.contains(&metadata.state)
        {
            return Ok(FencedRemoval::Superseded {
                state: metadata.state,
                execution_id: metadata.execution_id,
            });
        }
        slot.take();
        Ok(FencedRemoval::Removed)
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
        Ok(self.current.clone())
    }

    async fn remove(
        &self,
        _sandbox_id: &SandboxId,
    ) -> StdResult<Option<SandboxMetadata>, StoreError> {
        Ok(None)
    }

    async fn remove_if_execution(
        &self,
        _sandbox_id: &SandboxId,
        _expected_execution_id: crate::types::ExecutionId,
        _expected_states: &[SandboxState],
    ) -> StdResult<FencedRemoval, StoreError> {
        Ok(FencedRemoval::Absent)
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
    let orchestrator = Orchestrator::with_in_memory_store(MockBackendFactory::new()).await;
    drop(orchestrator);
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
    LaunchPlan::from_snapshot(
        sandbox_id,
        Box::new(test_runnable_snapshot().clone()),
        SandboxLaunchConfig::default(),
        transitional_metadata,
        NewTimeout::Set(Duration::from_secs(15)),
        None,
    )
}

fn mock_sandbox_handle() -> SandboxHandle {
    Arc::new(Mutex::new(Box::new(MockSandboxBackend::new(
        Arc::new(MockBehavior::new()),
        ExecutionId::new(),
    ))))
}

async fn proxy_target_for<S, F>(
    orchestrator: &TestOrchestrator<S, F>,
    sandbox_id: &SandboxId,
) -> Result<Option<ProxyTarget>>
where
    S: MetadataStore + 'static,
    F: SandboxBackendFactory,
{
    Ok(match orchestrator.proxy_lookup_for(sandbox_id).await? {
        ProxyLookupResult::Ready(target) => Some(target),
        ProxyLookupResult::NotFound
        | ProxyLookupResult::Unavailable(_)
        | ProxyLookupResult::RouteMissing => None,
    })
}

async fn current_metrics<S, F>(orchestrator: &TestOrchestrator<S, F>) -> OrchestratorMetrics
where
    S: MetadataStore + 'static,
    F: SandboxBackendFactory,
{
    orchestrator
        .metrics_snapshot()
        .await
        .expect("metrics snapshot succeeds")
}

async fn assert_metrics_values<S, F>(
    orchestrator: &TestOrchestrator<S, F>,
    create_successes: u64,
    create_fails: u64,
    running_sandbox_count: u32,
    starting_sandbox_count: u32,
    allocated_cpu: u32,
    allocated_memory_mib: u32,
) where
    S: MetadataStore + 'static,
    F: SandboxBackendFactory,
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

async fn assert_metrics_snapshot<S, F>(
    orchestrator: &TestOrchestrator<S, F>,
    expected: &OrchestratorMetrics,
) where
    S: MetadataStore + 'static,
    F: SandboxBackendFactory,
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

    let replacement_execution_id = ExecutionId::new();
    assert_ne!(replacement_execution_id, plan.execution_id());
    orchestrator
        .store
        .add(SandboxMetadata {
            id: sandbox_id,
            execution_id: replacement_execution_id,
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
    let untouched = orchestrator
        .store
        .get(&sandbox_id)
        .await
        .unwrap()
        .expect("running metadata should remain untouched");
    assert_eq!(untouched.state, SandboxState::Running);
    assert_eq!(untouched.execution_id, replacement_execution_id);
}

#[tokio::test]
async fn a_failed_create_clears_its_own_creating_record_even_when_its_handle_was_replaced() {
    let orchestrator = make_orchestrator().await;

    let handle_kept = SandboxId::new();
    let handle_replaced = SandboxId::new();
    let record_is_another_launchs = SandboxId::new();

    let plan_kept = create_launch_plan_with_resources(handle_kept);
    let plan_replaced = create_launch_plan_with_resources(handle_replaced);
    let plan_other = create_launch_plan_with_resources(record_is_another_launchs);

    for plan in [&plan_kept, &plan_replaced] {
        orchestrator.store.add(plan.metadata.clone()).await.unwrap();
    }
    let other_launch = SandboxMetadata {
        id: record_is_another_launchs,
        execution_id: ExecutionId::new(),
        state: SandboxState::Creating,
        ..Default::default()
    };
    assert_ne!(other_launch.execution_id, plan_other.execution_id());
    orchestrator.store.add(other_launch.clone()).await.unwrap();

    let own_handle = mock_sandbox_handle();
    let replacement_for_mine = mock_sandbox_handle();
    let replacement_for_other = mock_sandbox_handle();
    {
        let mut sandboxes = orchestrator.sandboxes.write().await;
        sandboxes.insert(handle_kept, Arc::clone(&own_handle));
        sandboxes.insert(handle_replaced, Arc::clone(&replacement_for_mine));
        sandboxes.insert(
            record_is_another_launchs,
            Arc::clone(&replacement_for_other),
        );
    }

    orchestrator
        .cleanup_failed_launch(
            &plan_kept,
            own_handle,
            FailedLaunchStage::TransitionalPersisted,
        )
        .await;
    orchestrator
        .cleanup_failed_launch(
            &plan_replaced,
            mock_sandbox_handle(),
            FailedLaunchStage::TransitionalPersisted,
        )
        .await;
    orchestrator
        .cleanup_failed_launch(
            &plan_other,
            mock_sandbox_handle(),
            FailedLaunchStage::TransitionalPersisted,
        )
        .await;

    assert!(
        orchestrator
            .store
            .get(&handle_kept)
            .await
            .unwrap()
            .is_none(),
        "a create that kept its handle must not leave a Creating record"
    );
    assert!(!orchestrator
        .sandboxes
        .read()
        .await
        .contains_key(&handle_kept));

    assert!(
        orchestrator
            .store
            .get(&handle_replaced)
            .await
            .unwrap()
            .is_none(),
        "a create whose handle was replaced must still take back its own record"
    );
    let still_registered = orchestrator
        .sandboxes
        .read()
        .await
        .get(&handle_replaced)
        .cloned()
        .expect("the replacement handle must stay registered");
    assert!(Arc::ptr_eq(&still_registered, &replacement_for_mine));

    let survivor = orchestrator
        .store
        .get(&record_is_another_launchs)
        .await
        .unwrap()
        .expect("a record written by another launch must survive this cleanup");
    assert_eq!(survivor.state, SandboxState::Creating);
    assert_eq!(survivor.execution_id, other_launch.execution_id);
    let other_registered = orchestrator
        .sandboxes
        .read()
        .await
        .get(&record_is_another_launchs)
        .cloned()
        .expect("the replacement handle must stay registered");
    assert!(Arc::ptr_eq(&other_registered, &replacement_for_other));
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
        traffic_access_token: None,
        source: SandboxLaunchSource::Snapshot(Box::new(RunnableSnapshot::mock())),
        expiry: match timeout_secs {
            Some(secs) => SandboxExpiry::After(Duration::from_secs(secs)),
            None => SandboxExpiry::AfterConfiguredDefault,
        },
        timeout_action: SandboxTimeoutAction::Pause,
        user_metadata,
        env_vars: None,
        network_policy: SandboxNetworkPolicy::default(),
        custom_extension_params: None,
        control_plane_config: None,
        execution_id: None,
        preferred_node_id: None,
        auto_resume: false,
        secure: false,
    }
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
            traffic_access_token: None,
            source: SandboxLaunchSource::Image {
                image_ref: "ubuntu:24.04".to_string(),
                overlaybd_config_path: PathBuf::from("/tmp/ubuntu-image.json"),
                context: Default::default(),
                resources: Some(resources),
                extra_drives: Vec::new(),
                extra_boot_args: None,
                image_configs: Box::new(ImageConfigs::new()),
            },
            expiry: SandboxExpiry::After(Duration::from_secs(60)),
            timeout_action: SandboxTimeoutAction::Pause,
            user_metadata: None,
            env_vars: None,
            network_policy: SandboxNetworkPolicy::default(),
            custom_extension_params: None,
            control_plane_config: None,
            execution_id: None,
            preferred_node_id: None,
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

#[tokio::test]
async fn sandbox_custom_extension_params_are_applied_and_persisted() -> Result<()> {
    setup();
    let behavior = Arc::new(MockBehavior::new());
    let orchestrator =
        make_orchestrator_with_factory(MockBackendFactory::with_behavior(Arc::clone(&behavior)))
            .await;
    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[]))
        .await?;
    assert_eq!(created.custom_extension_params, None);
    assert_eq!(behavior.last_custom_extension_params(), None);

    let mut params = serde_json::Map::new();
    params.insert("mode".to_string(), json!("fast"));
    orchestrator
        .replace_sandbox_custom_extension_params(created.id, Some(params.clone()))
        .await?;
    assert_eq!(
        behavior.last_custom_extension_params(),
        Some(Some(params.clone())),
        "the runtime must hold exactly the value that was applied"
    );

    let updated = orchestrator
        .get_sandbox(&created.id)
        .await?
        .expect("sandbox metadata should exist");
    assert_eq!(updated.custom_extension_params, Some(params));

    Ok(())
}

#[tokio::test]
async fn a_failed_custom_extension_params_assignment_leaves_the_store_untouched() -> Result<()> {
    setup();
    let behavior = Arc::new(MockBehavior::new());
    let orchestrator =
        make_orchestrator_with_factory(MockBackendFactory::with_behavior(Arc::clone(&behavior)))
            .await;
    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[]))
        .await?;

    let mut first = serde_json::Map::new();
    first.insert("mode".to_string(), json!("fast"));
    orchestrator
        .replace_sandbox_custom_extension_params(created.id, Some(first.clone()))
        .await?;

    behavior.push_action(
        MockOperation::UpdateCustomExtensionParams,
        MockAction::Fail {
            message: "extension runtime unreachable".to_string(),
        },
    );
    let mut second = serde_json::Map::new();
    second.insert("mode".to_string(), json!("slow"));
    let err = orchestrator
        .replace_sandbox_custom_extension_params(created.id, Some(second.clone()))
        .await
        .expect_err("the injected backend failure must surface");
    assert!(
        matches!(
            err,
            OrchestratorError::SandboxOperationFailed {
                operation: SandboxOperation::PatchCustomExtensionParams,
                ..
            }
        ),
        "unexpected error variant: {err:?}"
    );

    assert_eq!(
        behavior.last_custom_extension_params(),
        Some(Some(first.clone())),
        "the runtime must not adopt a value it never accepted"
    );
    let after = orchestrator
        .get_sandbox(&created.id)
        .await?
        .expect("sandbox metadata should exist");
    assert_eq!(
        after.custom_extension_params,
        Some(first),
        "the metadata store must not adopt a value the runtime never received"
    );

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

async fn assert_proxy_ready<S, F>(
    orchestrator: &Arc<TestOrchestrator<S, F>>,
    sandbox_id: &SandboxId,
) -> Result<()>
where
    S: MetadataStore + 'static,
    F: SandboxBackendFactory,
{
    let lookup = orchestrator.proxy_lookup_for(sandbox_id).await?;
    assert!(
        matches!(lookup, ProxyLookupResult::Ready(_)),
        "expected proxy lookup Ready for sandbox {sandbox_id}, got {lookup:?}"
    );
    Ok(())
}

async fn assert_proxy_not_found<S, F>(
    orchestrator: &Arc<TestOrchestrator<S, F>>,
    sandbox_id: &SandboxId,
) -> Result<()>
where
    S: MetadataStore + 'static,
    F: SandboxBackendFactory,
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
        .wait_for_transition(sandbox_id, SandboxState::Pausing)
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
    let orchestrator = make_orchestrator_without_background(
        ScriptedWaitStore::with_wait(WaitScript::Return(Ok(Some(SandboxMetadata {
            id: sandbox_id,
            state: SandboxState::Killing,
            ..Default::default()
        }))))
        .holding(SandboxMetadata {
            id: sandbox_id,
            state: SandboxState::Pausing,
            ..Default::default()
        }),
    );

    let err = orchestrator
        .join_concurrent_pause(sandbox_id)
        .await
        .expect_err("killing after joined pause should map to not found");
    assert!(matches!(err, OrchestratorError::SandboxNotFound(_)));
}

#[tokio::test]
async fn pause_publishes_the_capture_stops_the_vm_and_forgets_the_record() -> Result<()> {
    setup();
    let behavior = Arc::new(MockBehavior::new());
    let publisher = DiscardingPausePublisher::shared();
    let orchestrator = make_orchestrator_with_publisher(
        MockBackendFactory::with_behavior(Arc::clone(&behavior)),
        publisher.clone(),
    )
    .await;
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

    let outcome = orchestrator.pause_sandbox(sandbox_id).await?;

    assert_eq!(outcome.metadata.id, sandbox_id);
    assert_eq!(outcome.metadata.execution_id, created.execution_id);
    assert_eq!(
        outcome
            .metadata
            .user_metadata
            .as_ref()
            .and_then(|metadata| metadata.get("case_id"))
            .map(String::as_str),
        Some(case_id.as_str())
    );
    assert!(
        matches!(outcome.published, Some(PublishedPause::Committed(_))),
        "the caller that performed the pause owns its publication"
    );
    assert_eq!(publisher.published(), vec![sandbox_id]);
    assert_eq!(
        behavior.stop_calls(),
        1,
        "the VM is stopped once the capture is durable"
    );
    assert!(
        orchestrator.get_sandbox(&sandbox_id).await?.is_none(),
        "a paused sandbox exists only as the snapshot it published"
    );
    assert_proxy_not_found(&orchestrator, &sandbox_id).await?;
    assert_metrics_values(&orchestrator, 1, 0, 0, 0, 0, 0).await;

    for (what, result) in [
        (
            "pause",
            orchestrator.pause_sandbox(sandbox_id).await.map(|_| ()),
        ),
        (
            "keep-alive",
            orchestrator
                .keep_alive_for(sandbox_id, Some(Duration::from_secs(10)), true)
                .await
                .map(|_| ()),
        ),
        ("delete", orchestrator.delete_sandbox(sandbox_id).await),
    ] {
        assert!(
            matches!(result, Err(OrchestratorError::SandboxNotFound(id)) if id == sandbox_id),
            "a {what} after the pause found something: {result:?}"
        );
    }
    assert_eq!(behavior.stop_calls(), 1);

    Ok(())
}

#[tokio::test]
async fn pause_succeeds_and_releases_metrics_even_when_stop_fails_after_publication() -> Result<()>
{
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

    let outcome = orchestrator.pause_sandbox(created.id).await?;

    assert!(outcome.published.is_some());
    assert!(
        orchestrator.get_sandbox(&created.id).await?.is_none(),
        "a stop failure after publication does not bring the record back"
    );
    assert_proxy_not_found(&orchestrator, &created.id).await?;
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
async fn the_roster_drops_a_sandbox_once_it_is_paused() -> Result<()> {
    setup();
    let orchestrator = make_orchestrator().await;
    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[("team", "roster-paused")]))
        .await?;

    let running = orchestrator.list_sandbox_roster().await?;
    assert_eq!(running.len(), 1);
    assert_eq!(running[0].sandbox_id, created.id);
    assert_eq!(running[0].execution_id, created.execution_id);

    orchestrator.pause_sandbox(created.id).await?;

    assert!(
        orchestrator.list_sandbox_roster().await?.is_empty(),
        "a paused sandbox has no VM for a routing projection to reach"
    );
    Ok(())
}

#[tokio::test]
async fn pause_publication_failure_puts_the_sandbox_back_to_running() -> Result<()> {
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
    let orchestrator = make_orchestrator_with_publisher(
        MockBackendFactory::with_behavior(Arc::clone(&behavior)),
        Arc::new(FailingPausePublisher),
    )
    .await;
    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[("team", "pause-publish-fail")]))
        .await?;
    let sandbox_id = created.id;

    let err = orchestrator
        .pause_sandbox(sandbox_id)
        .await
        .expect_err("a pause whose capture cannot be published must fail");
    assert!(
        matches!(
            err,
            OrchestratorError::PausePublicationFailed { sandbox_id: id, .. } if id == sandbox_id
        ),
        "{err:?}"
    );

    let metadata = orchestrator
        .get_sandbox(&sandbox_id)
        .await?
        .expect("the sandbox is still recorded");
    assert_eq!(metadata.state, SandboxState::Running);
    assert_eq!(
        *resume_calls
            .lock()
            .expect("resume call counter mutex poisoned"),
        1,
        "the VM was resumed in place"
    );
    assert_eq!(behavior.stop_calls(), 0);
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
    assert_eq!(behavior.stop_calls(), 1);
    Ok(())
}

#[tokio::test]
async fn pause_publication_failure_with_a_vm_that_cannot_resume_removes_the_sandbox() -> Result<()>
{
    setup();
    let behavior = Arc::new(MockBehavior::new());
    behavior.push_action(
        MockOperation::Resume,
        MockAction::Fail {
            message: "the vm is gone".to_string(),
        },
    );
    let orchestrator = make_orchestrator_with_publisher(
        MockBackendFactory::with_behavior(Arc::clone(&behavior)),
        Arc::new(FailingPausePublisher),
    )
    .await;
    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[("team", "pause-publish-dead")]))
        .await?;
    let sandbox_id = created.id;

    let err = orchestrator
        .pause_sandbox(sandbox_id)
        .await
        .expect_err("nothing runnable is left, so the pause fails terminally");
    let OrchestratorError::SandboxOperationFailed {
        operation: SandboxOperation::Pause,
        source,
        ..
    } = err
    else {
        panic!("expected a terminal pause failure, got {err:?}");
    };
    let causes = source
        .chain()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(" <- ");
    assert!(causes.contains("the catalog is unreachable"), "{causes}");
    assert!(causes.contains("the vm is gone"), "{causes}");

    assert!(orchestrator.get_sandbox(&sandbox_id).await?.is_none());
    assert_proxy_not_found(&orchestrator, &sandbox_id).await?;
    assert_eq!(behavior.stop_calls(), 1);
    assert_metrics_values(&orchestrator, 1, 0, 0, 0, 0, 0).await;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_pause_whose_vm_is_gone_retries_the_commit_of_the_bytes_the_node_staged() -> Result<()> {
    setup();
    let behavior = Arc::new(MockBehavior::new());
    behavior.make_captures_staged();
    behavior.push_action(
        MockOperation::Resume,
        MockAction::Fail {
            message: "the vm is gone".to_string(),
        },
    );
    let publisher = FlakyPausePublisher::new(2);
    let orchestrator = make_orchestrator_without_publisher(
        InMemoryMetadataStore::new(),
        MockBackendFactory::with_behavior(Arc::clone(&behavior)),
        TEST_DEFAULT_SANDBOX_TIMEOUT,
    );
    orchestrator.set_pause_publisher(Arc::clone(&publisher) as Arc<dyn PausePublisher>);
    let created = orchestrator
        .create_sandbox(create_request(Some(600), &[("team", "pause-retry")]))
        .await?;

    let outcome = orchestrator
        .pause_sandbox(created.id)
        .await
        .expect("a retried commit must make the pause succeed");

    assert!(outcome.published.is_some());
    assert_eq!(
        publisher.attempts(),
        3,
        "the commit that finally landed was the third attempt"
    );
    assert!(orchestrator.get_sandbox(&created.id).await?.is_none());
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_pause_that_never_commits_says_where_its_bytes_were_kept() -> Result<()> {
    setup();
    let behavior = Arc::new(MockBehavior::new());
    behavior.make_captures_staged();
    behavior.push_action(
        MockOperation::Resume,
        MockAction::Fail {
            message: "the vm is gone".to_string(),
        },
    );
    let publisher = FlakyPausePublisher::new(usize::MAX);
    let orchestrator = make_orchestrator_without_publisher(
        InMemoryMetadataStore::new(),
        MockBackendFactory::with_behavior(Arc::clone(&behavior)),
        TEST_DEFAULT_SANDBOX_TIMEOUT,
    );
    orchestrator.set_pause_publisher(Arc::clone(&publisher) as Arc<dyn PausePublisher>);
    let created = orchestrator
        .create_sandbox(create_request(
            Some(600),
            &[("team", "pause-retry-exhausted")],
        ))
        .await?;

    let err = orchestrator
        .pause_sandbox(created.id)
        .await
        .expect_err("every commit was refused");

    assert_eq!(
        publisher.attempts(),
        5,
        "the first commit plus four retries"
    );
    let OrchestratorError::SandboxOperationFailed { source, .. } = err else {
        panic!("expected a pause failure, got {err:?}");
    };
    let causes = source
        .chain()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(" <- ");
    assert!(
        causes.contains("capture bytes were retained under snapshot"),
        "the error must name where an operator can find the bytes: {causes}"
    );
    Ok(())
}

#[tokio::test]
async fn pause_without_a_wired_publisher_is_an_internal_error() -> Result<()> {
    setup();
    let behavior = Arc::new(MockBehavior::new());
    let orchestrator = make_orchestrator_without_publisher(
        InMemoryMetadataStore::new(),
        MockBackendFactory::with_behavior(Arc::clone(&behavior)),
        TEST_DEFAULT_SANDBOX_TIMEOUT,
    );
    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[("team", "pause-no-publisher")]))
        .await?;
    let sandbox_id = created.id;

    let err = orchestrator
        .pause_sandbox(sandbox_id)
        .await
        .expect_err("a process with nowhere to publish a pause cannot pause");
    assert!(
        matches!(err, OrchestratorError::InternalError(_)),
        "{err:?}"
    );

    let metadata = orchestrator
        .get_sandbox(&sandbox_id)
        .await?
        .expect("the sandbox is still recorded");
    assert_eq!(metadata.state, SandboxState::Running);
    assert_eq!(behavior.stop_calls(), 0);
    assert_proxy_ready(&orchestrator, &sandbox_id).await?;
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
        matches!(
            result.captured_snapshot,
            crate::snapshot::CapturedSandboxSnapshot::Local(_)
        ),
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
        matches!(
            retry.captured_snapshot,
            crate::snapshot::CapturedSandboxSnapshot::Local(_)
        ),
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
    let sandbox_id = SandboxId::new();
    orchestrator
        .set_metadata_state_for_test(sandbox_id, SandboxState::Pausing)
        .await?;

    let err = orchestrator
        .capture_snapshot(sandbox_id)
        .await
        .expect_err("capture_snapshot should reject a sandbox that is pausing");
    assert!(matches!(
        err,
        OrchestratorError::InvalidSandboxState {
            sandbox_id: id,
            state: SandboxState::Pausing,
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
async fn orchestrator_proxy_lookup_tracks_create_pause_delete_lifecycle() -> Result<()> {
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
    assert_proxy_not_found(&orchestrator, &sandbox_id).await?;

    let err = orchestrator
        .delete_sandbox(sandbox_id)
        .await
        .expect_err("a paused sandbox has no record left to delete");
    assert!(matches!(err, OrchestratorError::SandboxNotFound(id) if id == sandbox_id));
    assert_metrics_values(&orchestrator, 1, 0, 0, 0, 0, 0).await;
    assert_proxy_not_found(&orchestrator, &sandbox_id).await?;

    Ok(())
}

#[tokio::test]
async fn orchestrator_concurrent_pause_calls_all_succeed_and_exactly_one_publishes() -> Result<()> {
    setup();
    let behavior = Arc::new(MockBehavior::new());
    behavior.push_action(
        MockOperation::Pause,
        MockAction::SucceedAfter(Duration::from_millis(200)),
    );
    let publisher = DiscardingPausePublisher::shared();
    let orchestrator = make_orchestrator_with_publisher(
        MockBackendFactory::with_behavior(Arc::clone(&behavior)),
        publisher.clone(),
    )
    .await;
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

    const N: usize = 3;
    let handles: Vec<_> = (0..N)
        .map(|_| {
            let orch = Arc::clone(&orchestrator);
            let id = sandbox_id;
            tokio::spawn(async move { orch.pause_sandbox(id).await })
        })
        .collect();

    let mut performed = 0usize;
    let mut joined = 0usize;
    for (i, handle) in handles.into_iter().enumerate() {
        let outcome = handle
            .await
            .unwrap_or_else(|e| panic!("pause task {i} panicked: {e}"))
            .unwrap_or_else(|e| panic!("concurrent pause call {i} failed: {e}"));
        assert_eq!(outcome.metadata.id, sandbox_id);
        match outcome.published {
            Some(_) => performed += 1,
            None => joined += 1,
        }
    }
    assert_eq!(performed, 1, "exactly one caller performed the pause");
    assert_eq!(joined, N - 1, "every other caller joined it");
    assert_eq!(
        publisher.published(),
        vec![sandbox_id],
        "the capture was published once"
    );
    assert_eq!(behavior.stop_calls(), 1);

    assert!(orchestrator.get_sandbox(&sandbox_id).await?.is_none());
    assert_metrics_values(&orchestrator, 1, 0, 0, 0, 0, 0).await;
    assert_proxy_not_found(&orchestrator, &sandbox_id).await?;

    Ok(())
}

#[tokio::test]
async fn delete_waits_through_a_pause_and_finds_the_sandbox_gone() -> Result<()> {
    setup();
    let behavior = Arc::new(MockBehavior::new());
    behavior.push_action(
        MockOperation::Pause,
        MockAction::SucceedAfter(Duration::from_millis(200)),
    );
    let orchestrator =
        make_orchestrator_with_factory(MockBackendFactory::with_behavior(Arc::clone(&behavior)))
            .await;
    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[("team", "delete-during-pause")]))
        .await?;
    let sandbox_id = created.id;

    let pausing = Arc::clone(&orchestrator);
    let pause = tokio::spawn(async move { pausing.pause_sandbox(sandbox_id).await });
    wait_for_state(&orchestrator, &sandbox_id, SandboxState::Pausing).await?;

    orchestrator.delete_sandbox(sandbox_id).await?;

    let paused = pause.await.expect("the pause task should not panic")?;
    assert!(paused.published.is_some(), "the pause ran to completion");
    assert_eq!(
        behavior.stop_calls(),
        1,
        "the pause stopped the VM; the delete had nothing left to stop"
    );
    assert!(orchestrator.get_sandbox(&sandbox_id).await?.is_none());
    assert_proxy_not_found(&orchestrator, &sandbox_id).await?;
    assert_metrics_values(&orchestrator, 1, 0, 0, 0, 0, 0).await;
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
    assert_eq!(updated.timeout, Some(Duration::from_secs(3_600)));
    Ok(())
}

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

#[tokio::test]
async fn the_evictor_takes_the_deadlines_this_orchestrator_keeps_and_no_others() -> Result<()> {
    setup();
    const CONFIGURED_DEFAULT: Duration = Duration::from_millis(20);
    const LONGER_THAN_THE_WHOLE_TEST: Duration = Duration::from_secs(600);

    let orchestrator =
        make_orchestrator_without_background_with_default_timeout(CONFIGURED_DEFAULT);
    let with = |expiry| CreateSandboxRequest {
        traffic_access_token: None,
        expiry,
        ..create_request(None, &[])
    };

    let caller_kept = orchestrator
        .create_sandbox(with(SandboxExpiry::NotKeptHere))
        .await?;
    let node_default = orchestrator
        .create_sandbox(with(SandboxExpiry::AfterConfiguredDefault))
        .await?;
    let node_named = orchestrator
        .create_sandbox(with(SandboxExpiry::After(LONGER_THAN_THE_WHOLE_TEST)))
        .await?;

    assert_eq!(caller_kept.timeout, None);
    assert_eq!(
        caller_kept.expires_at, None,
        "a sandbox whose deadline the caller keeps must not be in this orchestrator's expiry index"
    );
    assert_eq!(node_default.timeout, Some(CONFIGURED_DEFAULT));
    assert_eq!(node_named.timeout, Some(LONGER_THAN_THE_WHOLE_TEST));

    sleep(CONFIGURED_DEFAULT * 10).await;

    let evicted = orchestrator.evict_expired_sandboxes().await?;
    assert_eq!(
        evicted,
        vec![node_default.id],
        "exactly the sandbox whose deadline this orchestrator keeps, and it did pass"
    );

    let state = |id| {
        let orchestrator = Arc::clone(&orchestrator);
        async move {
            orchestrator
                .get_sandbox(&id)
                .await
                .expect("read")
                .expect("the sandbox is still tracked")
                .state
        }
    };
    assert!(
        orchestrator.get_sandbox(&node_default.id).await?.is_none(),
        "the timeout action is Pause, and a paused sandbox exists only as its snapshot"
    );
    assert_eq!(
        state(caller_kept.id).await,
        SandboxState::Running,
        "the node paused a sandbox whose deadline it was told it does not keep"
    );
    assert_eq!(
        state(node_named.id).await,
        SandboxState::Running,
        "a deadline ten minutes out has not passed"
    );

    orchestrator.delete_sandbox(caller_kept.id).await?;
    orchestrator.delete_sandbox(node_named.id).await?;
    Ok(())
}

#[tokio::test]
async fn a_fork_of_a_sandbox_with_no_deadline_gives_its_children_none() -> Result<()> {
    setup();
    const SHORT_ENOUGH_TO_OUTLAST: Duration = Duration::from_millis(20);

    let orchestrator =
        make_orchestrator_without_background_with_default_timeout(Duration::from_secs(15));
    let source = orchestrator
        .create_sandbox(CreateSandboxRequest {
            traffic_access_token: None,
            expiry: SandboxExpiry::NotKeptHere,
            ..create_request(None, &[])
        })
        .await?;
    assert_eq!(source.expires_at, None);

    let fork = |timeout| {
        let orchestrator = Arc::clone(&orchestrator);
        async move {
            orchestrator
                .fork_sandbox(source.id, ForkChildren::Fresh(1), timeout)
                .await
                .expect("the fork ran")
                .into_iter()
                .next()
                .expect("one child")
                .expect("the child started")
        }
    };

    let inherited = fork(NewTimeout::UseExisting).await;
    let asked_for = fork(NewTimeout::Set(SHORT_ENOUGH_TO_OUTLAST)).await;

    assert_eq!(
        inherited.expires_at, None,
        "a child of a sandbox with no deadline acquired one from somewhere"
    );
    assert_eq!(asked_for.timeout, Some(SHORT_ENOUGH_TO_OUTLAST));

    sleep(SHORT_ENOUGH_TO_OUTLAST * 10).await;
    let evicted = orchestrator.evict_expired_sandboxes().await?;
    assert_eq!(
        evicted,
        vec![asked_for.id],
        "the child that asked for a deadline is the one the same pass takes"
    );

    for id in [source.id, inherited.id] {
        assert_eq!(
            orchestrator
                .get_sandbox(&id)
                .await?
                .expect("still tracked")
                .state,
            SandboxState::Running
        );
    }
    assert!(
        orchestrator.get_sandbox(&asked_for.id).await?.is_none(),
        "the evicted child was paused, and a paused sandbox has no record"
    );

    orchestrator.delete_sandbox(inherited.id).await?;
    orchestrator.delete_sandbox(source.id).await?;
    Ok(())
}

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
            state: SandboxState::Pausing,
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
    let pausing_id = SandboxId::new();
    let non_expired_id = SandboxId::new();

    let running_expired = SandboxMetadata {
        id: running_id,
        state: SandboxState::Running,
        created_at: now,
        timeout: Some(Duration::from_secs(1)),
        expires_at: now.checked_sub(Duration::from_secs(1)),
        ..Default::default()
    };
    let pausing_expired = SandboxMetadata {
        id: pausing_id,
        state: SandboxState::Pausing,
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
    store.add(pausing_expired).await?;
    store.add(non_expired).await?;

    let orchestrator = make_orchestrator_without_background(store);
    let evicted_ids = orchestrator.evict_expired_sandboxes().await?;
    assert!(
        evicted_ids.is_empty(),
        "running sandbox pause should fail without handle and be skipped"
    );

    let running = orchestrator.get_sandbox(&running_id).await?;
    assert!(
        running.is_none(),
        "expired, handle-less running sandbox should be removed"
    );

    let pausing = orchestrator
        .get_sandbox(&pausing_id)
        .await?
        .expect("a sandbox mid-transition is left to the operation holding it");
    assert_eq!(pausing.state, SandboxState::Pausing);

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
async fn auto_evict_expired_sandbox() -> anyhow::Result<()> {
    setup();
    let publisher = DiscardingPausePublisher::shared();
    let orchestrator =
        make_orchestrator_with_publisher(MockBackendFactory::new(), publisher.clone()).await;
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

    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let paused = orchestrator.get_sandbox(&to_pause.id).await?.is_none();
        let deleted = orchestrator.get_sandbox(&to_delete.id).await?.is_none();
        if paused && deleted {
            break;
        }
        if std::time::Instant::now() >= deadline {
            anyhow::bail!("auto-evict did not finish: paused={paused}, deleted={deleted}");
        }
        sleep(Duration::from_millis(100)).await;
    }

    assert_eq!(
        publisher.published(),
        vec![to_pause.id],
        "the expiry paused one sandbox and deleted the other"
    );
    assert_metrics_values(&orchestrator, 2, 0, 0, 0, 0, 0).await;
    assert_proxy_not_found(&orchestrator, &to_pause.id).await?;
    assert_proxy_not_found(&orchestrator, &to_delete.id).await?;

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
async fn shutdown_stops_every_running_sandbox_and_rejects_new_lifecycle_operations() -> Result<()> {
    setup();
    let behavior = Arc::new(MockBehavior::new());
    let publisher = DiscardingPausePublisher::shared();
    let orchestrator = make_orchestrator_with_publisher(
        MockBackendFactory::with_behavior(Arc::clone(&behavior)),
        publisher.clone(),
    )
    .await;
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

    assert!(
        orchestrator.list_sandboxes().await?.is_empty(),
        "nothing survives a shutdown"
    );
    assert_eq!(behavior.stop_calls(), 2);
    assert!(
        publisher.published().is_empty(),
        "a shutdown stops sandboxes; it does not pause them"
    );
    assert_metrics_values(&orchestrator, 2, 0, 0, 0, 0, 0).await;
    assert_proxy_not_found(&orchestrator, &first.id).await?;
    assert_proxy_not_found(&orchestrator, &second.id).await?;

    let err = orchestrator
        .create_sandbox(create_request(Some(60), &[("team", "shutdown-reject")]))
        .await
        .expect_err("shutdown orchestrator should reject new sandbox creation");
    assert!(matches!(err, OrchestratorError::ShuttingDown));
    assert_metrics_values(&orchestrator, 2, 1, 0, 0, 0, 0).await;

    Ok(())
}

#[tokio::test]
async fn shutdown_retries_a_failed_stop_and_succeeds_once_the_vm_stops() -> Result<()> {
    setup();
    let behavior = Arc::new(MockBehavior::new());
    behavior.push_action(
        MockOperation::Stop,
        MockAction::Fail {
            message: "shutdown stop failure".to_string(),
        },
    );
    let orchestrator =
        make_orchestrator_with_factory(MockBackendFactory::with_behavior(Arc::clone(&behavior)))
            .await;

    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[("team", "shutdown-stop-retry")]))
        .await?;
    let sandbox_id = created.id;

    orchestrator.shutdown().await?;

    assert_eq!(behavior.stop_calls(), 2, "the second pass stopped it");
    assert!(orchestrator.get_sandbox(&sandbox_id).await?.is_none());
    Ok(())
}

#[tokio::test]
async fn shutdown_returns_error_after_exhausting_stop_retries() -> Result<()> {
    setup();
    let behavior = Arc::new(MockBehavior::new());
    for pass in 1..=3 {
        behavior.push_action(
            MockOperation::Stop,
            MockAction::Fail {
                message: format!("shutdown stop failure pass {pass}"),
            },
        );
    }
    let orchestrator =
        make_orchestrator_with_factory(MockBackendFactory::with_behavior(Arc::clone(&behavior)))
            .await;

    let created = orchestrator
        .create_sandbox(create_request(
            Some(60),
            &[("team", "shutdown-stop-failure")],
        ))
        .await?;

    let err = orchestrator
        .shutdown()
        .await
        .expect_err("shutdown should fail after exhausting stop retries");
    assert!(matches!(err, OrchestratorError::InternalError(_)));
    assert_eq!(behavior.stop_calls(), 3);

    let metadata = orchestrator
        .get_sandbox(&created.id)
        .await?
        .expect("a sandbox that would not stop is still recorded");
    assert_eq!(metadata.state, SandboxState::Running);

    Ok(())
}

#[tokio::test]
async fn shutdown_reuses_recorded_success_instead_of_running_cleanup_again() -> Result<()> {
    setup();
    let behavior = Arc::new(MockBehavior::new());
    let orchestrator =
        make_orchestrator_with_factory(MockBackendFactory::with_behavior(Arc::clone(&behavior)))
            .await;

    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[("team", "shutdown-memoized")]))
        .await?;

    orchestrator.shutdown().await?;
    assert_eq!(behavior.stop_calls(), 1);
    assert!(orchestrator.get_sandbox(&created.id).await?.is_none());

    let late = SandboxId::new();
    orchestrator
        .set_metadata_state_for_test(late, SandboxState::Running)
        .await?;
    orchestrator.shutdown().await?;

    assert_eq!(
        behavior.stop_calls(),
        1,
        "the second call reported the first outcome instead of cleaning up again"
    );
    assert!(
        orchestrator.get_sandbox(&late).await?.is_some(),
        "a record that appeared after the cleanup was not swept by the memoized call"
    );

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
async fn a_fork_child_with_no_address_is_refused_rather_than_registered() -> Result<()> {
    setup();
    let behavior = Arc::new(MockBehavior::new());
    let orchestrator =
        make_orchestrator_with_factory(MockBackendFactory::with_behavior(Arc::clone(&behavior)))
            .await;
    let source = orchestrator
        .create_sandbox(create_request(Some(60), &[("team", "fork-addressing")]))
        .await?;

    let routable = orchestrator
        .fork_sandbox(source.id, ForkChildren::Fresh(2), NewTimeout::UseExisting)
        .await?
        .into_iter()
        .collect::<StdResult<Vec<_>, _>>()?;
    assert_eq!(routable.len(), 2);
    let mut routes = Vec::with_capacity(routable.len());
    for child in &routable {
        match orchestrator.proxy_lookup_for(&child.id).await? {
            ProxyLookupResult::Ready(target) => routes.push(target.ip),
            other => panic!("a started fork child is not routable: {other:?}"),
        }
    }
    assert_ne!(
        routes[0], routes[1],
        "two fork children were routed to one address"
    );
    for route in &routes {
        assert_ne!(
            Some(*route),
            orchestrator
                .proxy_lookup_for(&source.id)
                .await
                .ok()
                .and_then(|lookup| match lookup {
                    ProxyLookupResult::Ready(target) => Some(target.ip),
                    _ => None,
                }),
            "a fork child was routed to the sandbox it was forked from"
        );
    }

    behavior.set_fork_children_without_address(true);
    let refused = orchestrator
        .fork_sandbox(source.id, ForkChildren::Fresh(2), NewTimeout::UseExisting)
        .await?;
    assert_eq!(refused.len(), 2);
    for outcome in refused {
        let err = outcome.expect_err("a child with no address must not be registered");
        assert!(
            err.to_string().contains("missing host interaction IP"),
            "a child with no address was refused for some other reason: {err:?}"
        );
    }

    let mut recorded = orchestrator
        .list_sandboxes()
        .await?
        .into_iter()
        .map(|metadata| metadata.id)
        .collect::<Vec<_>>();
    recorded.sort();
    let mut expected = vec![source.id, routable[0].id, routable[1].id];
    expected.sort();
    assert_eq!(
        recorded, expected,
        "a fork child that could not be routed was registered anyway"
    );
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

#[tokio::test]
async fn a_create_from_a_snapshot_is_a_new_execution() -> Result<()> {
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

    let plan = create_launch_plan_with_resources(SandboxId::new());
    assert_eq!(plan.transitional_state(), SandboxState::Creating);

    orchestrator.delete_sandbox(first.id).await?;
    orchestrator.delete_sandbox(second.id).await?;
    Ok(())
}

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

#[tokio::test]
async fn a_pause_reports_the_execution_that_produced_it() -> Result<()> {
    setup();
    let publisher = DiscardingPausePublisher::shared();
    let orchestrator =
        make_orchestrator_with_publisher(MockBackendFactory::new(), publisher.clone()).await;
    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[]))
        .await?;
    let ran_as = orchestrator
        .backend_execution_id_for_test(&created.id)
        .await
        .expect("a running sandbox has a backend");

    let paused = orchestrator.pause_sandbox(created.id).await?;

    assert_eq!(
        paused.metadata.execution_id, ran_as,
        "pausing does not start a new run, so the outcome names the run it captured"
    );
    assert_eq!(publisher.published(), vec![created.id]);
    assert!(orchestrator.get_sandbox(&created.id).await?.is_none());
    Ok(())
}

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
}

#[tokio::test]
async fn a_control_plane_orchestrator_stamps_its_own_record_onto_the_create() {
    setup();
    let factory = StampingFactory::new(true);
    let seen = factory.seen();
    let orchestrator = Orchestrator::new(
        crate::sandbox::AccessTokenSeedPolicy::MayGenerate,
        InMemoryMetadataStore::new(),
        factory,
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

#[tokio::test]
async fn a_machine_local_orchestrator_stamps_nothing() {
    setup();
    let factory = StampingFactory::new(false);
    let seen = factory.seen();
    let orchestrator = Orchestrator::new(
        crate::sandbox::AccessTokenSeedPolicy::MayGenerate,
        InMemoryMetadataStore::new(),
        factory,
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
    assert_eq!(launch_config.sandbox_id, created.id);
    assert_eq!(created.state, SandboxState::Running);
}

#[tokio::test]
async fn a_marker_the_caller_supplied_survives_the_stamp() {
    setup();
    let factory = StampingFactory::new(true);
    let seen = factory.seen();
    let orchestrator = Orchestrator::new(
        crate::sandbox::AccessTokenSeedPolicy::MayGenerate,
        InMemoryMetadataStore::new(),
        factory,
        test_runtime_image_refs(),
    )
    .await
    .expect("orchestrator");

    let supplied = ControlPlaneConfig::from_bytes(b"somebody else's record".to_vec())
        .expect("a non-empty marker");
    let created = orchestrator
        .create_sandbox(CreateSandboxRequest {
            traffic_access_token: None,
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

struct AdoptingFactory {
    inner: MockBackendFactory,
    behavior: Arc<MockBehavior>,
    adopt_calls: Arc<std::sync::atomic::AtomicUsize>,
}

impl AdoptingFactory {
    fn new(behavior: Arc<MockBehavior>) -> Self {
        Self {
            inner: MockBackendFactory::with_behavior(Arc::clone(&behavior)),
            behavior,
            adopt_calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }

    fn adopt_calls_handle(&self) -> Arc<std::sync::atomic::AtomicUsize> {
        Arc::clone(&self.adopt_calls)
    }
}

impl SandboxBackendFactory for AdoptingFactory {
    fn build(
        &self,
        build_spec: crate::sandbox::FreshSandboxBuildSpec,
        launch_config: SandboxLaunchConfig,
        execution_id: ExecutionId,
    ) -> anyhow::Result<Box<dyn SandboxBackend>> {
        self.inner.build(build_spec, launch_config, execution_id)
    }

    fn build_from_snapshot(
        &self,
        snapshot: &RunnableSnapshot,
        launch_config: SandboxLaunchConfig,
        execution_id: ExecutionId,
    ) -> anyhow::Result<Box<dyn SandboxBackend>> {
        self.inner
            .build_from_snapshot(snapshot, launch_config, execution_id)
    }

    fn adopt_running(
        &self,
        _sandbox_id: SandboxId,
        execution_id: ExecutionId,
        _resources: SandboxResources,
    ) -> anyhow::Result<Option<Box<dyn SandboxBackend>>> {
        self.adopt_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(Some(Box::new(MockSandboxBackend::new(
            Arc::clone(&self.behavior),
            execution_id,
        ))))
    }
}

async fn make_adopting_orchestrator() -> (
    Arc<TestOrchestrator<InMemoryMetadataStore, AdoptingFactory>>,
    Arc<std::sync::atomic::AtomicUsize>,
) {
    let behavior = Arc::new(MockBehavior::new());
    let factory = AdoptingFactory::new(behavior);
    let adopt_calls = factory.adopt_calls_handle();
    let orchestrator =
        make_orchestrator_with_publisher(factory, DiscardingPausePublisher::shared()).await;
    (orchestrator, adopt_calls)
}

async fn supersede_execution_without_touching_the_handle(
    orchestrator: &Arc<TestOrchestrator<InMemoryMetadataStore, AdoptingFactory>>,
    sandbox_id: SandboxId,
) -> ExecutionId {
    let current_execution_id = ExecutionId::new();
    orchestrator
        .set_live_execution_for_test(
            sandbox_id,
            ProxyTarget::new(Ipv4Addr::LOCALHOST),
            current_execution_id,
        )
        .await;
    current_execution_id
}

#[tokio::test]
async fn delete_discards_a_stale_handle_and_rebuilds_from_the_record() -> anyhow::Result<()> {
    setup();
    let (orchestrator, adopt_calls) = make_adopting_orchestrator().await;

    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[]))
        .await?;
    let sandbox_id = created.id;
    let stale_execution_id = created.execution_id;
    let current_execution_id =
        supersede_execution_without_touching_the_handle(&orchestrator, sandbox_id).await;
    assert_ne!(stale_execution_id, current_execution_id);

    let discards_before = orchestrator.stale_handle_discards();

    orchestrator.delete_sandbox(sandbox_id).await?;

    assert_eq!(
        orchestrator.stale_handle_discards(),
        discards_before + 1,
        "the stale handle should have been logged and counted as discarded"
    );
    assert_eq!(
        adopt_calls.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "delete should have rebuilt the handle from the record rather than \
         driving the stale one straight at the node"
    );
    assert!(
        orchestrator.get_sandbox(&sandbox_id).await?.is_none(),
        "the sandbox should be gone after a successful delete"
    );
    Ok(())
}

#[tokio::test]
async fn delete_reuses_a_matching_handle_without_rebuilding_it() -> anyhow::Result<()> {
    setup();
    let (orchestrator, adopt_calls) = make_adopting_orchestrator().await;

    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[]))
        .await?;
    let sandbox_id = created.id;

    let discards_before = orchestrator.stale_handle_discards();

    orchestrator.delete_sandbox(sandbox_id).await?;

    assert_eq!(
        orchestrator.stale_handle_discards(),
        discards_before,
        "a handle whose execution still matches the record must not be \
         reported as discarded"
    );
    assert_eq!(
        adopt_calls.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "delete must drive the cached handle directly rather than rebuild one \
         that would carry the same execution id anyway — an \"always \
         rebuild\" implementation also leaves the sandbox deleted, but it \
         would call adopt_running here, which this asserts it did not"
    );
    assert!(orchestrator.get_sandbox(&sandbox_id).await?.is_none());
    Ok(())
}

#[tokio::test]
async fn pause_discards_a_stale_handle_and_rebuilds_from_the_record() -> anyhow::Result<()> {
    setup();
    let (orchestrator, adopt_calls) = make_adopting_orchestrator().await;

    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[]))
        .await?;
    let sandbox_id = created.id;
    let stale_execution_id = created.execution_id;
    let current_execution_id =
        supersede_execution_without_touching_the_handle(&orchestrator, sandbox_id).await;
    assert_ne!(stale_execution_id, current_execution_id);

    let discards_before = orchestrator.stale_handle_discards();

    let paused = orchestrator.pause_sandbox(sandbox_id).await?;

    assert!(paused.published.is_some());
    assert!(orchestrator.get_sandbox(&sandbox_id).await?.is_none());
    assert_eq!(
        orchestrator.stale_handle_discards(),
        discards_before + 1,
        "the stale handle should have been logged and counted as discarded"
    );
    assert_eq!(
        adopt_calls.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "pause should have rebuilt the handle from the record rather than \
         driving the stale one straight at the node"
    );
    Ok(())
}

#[tokio::test]
async fn pause_reuses_a_matching_handle_without_rebuilding_it() -> anyhow::Result<()> {
    setup();
    let (orchestrator, adopt_calls) = make_adopting_orchestrator().await;

    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[]))
        .await?;
    let sandbox_id = created.id;

    let discards_before = orchestrator.stale_handle_discards();

    let paused = orchestrator.pause_sandbox(sandbox_id).await?;

    assert!(paused.published.is_some());
    assert!(orchestrator.get_sandbox(&sandbox_id).await?.is_none());
    assert_eq!(
        orchestrator.stale_handle_discards(),
        discards_before,
        "a handle whose execution still matches the record must not be \
         reported as discarded"
    );
    assert_eq!(
        adopt_calls.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "pause must drive the cached handle directly rather than rebuild one \
         that would carry the same execution id anyway"
    );
    Ok(())
}

#[tokio::test]
async fn capture_snapshot_discards_a_stale_handle_and_rebuilds_from_the_record(
) -> anyhow::Result<()> {
    setup();
    let (orchestrator, adopt_calls) = make_adopting_orchestrator().await;

    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[]))
        .await?;
    let sandbox_id = created.id;
    let stale_execution_id = created.execution_id;
    let current_execution_id =
        supersede_execution_without_touching_the_handle(&orchestrator, sandbox_id).await;
    assert_ne!(stale_execution_id, current_execution_id);

    let discards_before = orchestrator.stale_handle_discards();

    orchestrator.capture_snapshot(sandbox_id).await?;

    assert_eq!(
        orchestrator.stale_handle_discards(),
        discards_before + 1,
        "the stale handle should have been logged and counted as discarded"
    );
    assert_eq!(
        adopt_calls.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "capture_snapshot should have rebuilt the handle from the record \
         rather than driving the stale one straight at the node"
    );
    Ok(())
}

#[tokio::test]
async fn capture_snapshot_reuses_a_matching_handle_without_rebuilding_it() -> anyhow::Result<()> {
    setup();
    let (orchestrator, adopt_calls) = make_adopting_orchestrator().await;

    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[]))
        .await?;
    let sandbox_id = created.id;

    let discards_before = orchestrator.stale_handle_discards();

    orchestrator.capture_snapshot(sandbox_id).await?;

    assert_eq!(
        orchestrator.stale_handle_discards(),
        discards_before,
        "a handle whose execution still matches the record must not be \
         reported as discarded"
    );
    assert_eq!(
        adopt_calls.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "capture_snapshot must drive the cached handle directly rather than \
         rebuild one that would carry the same execution id anyway"
    );
    Ok(())
}

fn policy_with_rules(name: &str) -> SandboxNetworkPolicy {
    let mut rules = std::collections::BTreeMap::new();
    rules.insert(
        "api.example.com".to_string(),
        vec![crate::sandbox::network::policy::DomainRule {
            transform: crate::sandbox::network::policy::HeaderTransform {
                headers: [(
                    "Authorization".to_string(),
                    format!("Bearer ${{aenv.secrets.{name}}}"),
                )]
                .into_iter()
                .collect(),
            },
        }],
    );
    SandboxNetworkPolicy::new(
        BaseSandboxNetworkPolicy::Default,
        SandboxNetworkEgressPolicy::with_rules(None, None, Some(rules)).unwrap(),
    )
}

#[tokio::test]
async fn a_create_with_rules_grants_before_start_and_a_delete_revokes() -> Result<()> {
    use crate::orchestrator::grants::recording::{GrantEvent, RecordingGrantIssuer};

    setup();
    let orchestrator = make_orchestrator_with_factory(MockBackendFactory::new()).await;
    let grants = RecordingGrantIssuer::shared();
    orchestrator.set_grant_issuer(Arc::clone(&grants) as Arc<dyn crate::orchestrator::GrantIssuer>);

    let mut request = create_request(Some(60), &[]);
    request.network_policy = policy_with_rules("openai");
    let created = orchestrator.create_sandbox(request).await?;

    let expected_names: std::collections::BTreeSet<String> =
        ["openai".to_string()].into_iter().collect();
    assert_eq!(
        grants.events(),
        vec![GrantEvent::Grant {
            sandbox_id: created.id,
            execution_id: created.execution_id,
            names: expected_names,
        }]
    );

    orchestrator.delete_sandbox(created.id).await?;
    assert_eq!(
        grants.events().last(),
        Some(&GrantEvent::Revoke {
            sandbox_id: created.id,
            execution_id: created.execution_id,
        })
    );
    Ok(())
}

#[tokio::test]
async fn a_name_the_store_does_not_hold_is_reported_and_still_starts() -> Result<()> {
    use crate::orchestrator::grants::recording::{GrantEvent, RecordingGrantIssuer};

    setup();
    let orchestrator = make_orchestrator_with_factory(MockBackendFactory::new()).await;
    let grants = RecordingGrantIssuer::shared();
    grants.answer_unusable_names(vec!["openai".to_string()]);
    orchestrator.set_grant_issuer(Arc::clone(&grants) as Arc<dyn crate::orchestrator::GrantIssuer>);

    let mut request = create_request(Some(60), &[]);
    request.network_policy = policy_with_rules("openai");

    // A wake carries a policy accepted long ago; a secret deleted since then
    // degrades the sandbox and must not keep it from coming back.
    let created = orchestrator.create_sandbox(request).await?;
    assert!(matches!(
        grants.events().first(),
        Some(GrantEvent::Grant { names, .. }) if names.contains("openai")
    ));
    orchestrator.delete_sandbox(created.id).await?;
    Ok(())
}

#[tokio::test]
async fn the_reaper_revokes_aged_grants_no_record_backs_and_keeps_live_ones() -> Result<()> {
    use crate::orchestrator::grants::recording::{GrantEvent, RecordingGrantIssuer};

    setup();
    let orchestrator = make_orchestrator_with_factory(MockBackendFactory::new()).await;
    let grants = RecordingGrantIssuer::shared();
    orchestrator.set_grant_issuer(Arc::clone(&grants) as Arc<dyn crate::orchestrator::GrantIssuer>);

    let mut request = create_request(Some(60), &[]);
    request.network_policy = policy_with_rules("openai");
    let live = orchestrator.create_sandbox(request).await?;

    // A grant whose record is gone, and one whose record now names another
    // incarnation, are both orphans; the live sandbox's own grant is not.
    let gone = (SandboxId::new(), ExecutionId::new());
    let superseded = (live.id, ExecutionId::new());
    grants.answer_stale_grants(vec![(live.id, live.execution_id), gone, superseded]);

    let reaped = orchestrator.reap_orphaned_grants().await?;
    assert_eq!(reaped, vec![gone, superseded]);
    let revoked: Vec<_> = grants
        .events()
        .into_iter()
        .filter(|event| matches!(event, GrantEvent::Revoke { .. }))
        .collect();
    assert_eq!(
        revoked,
        vec![
            GrantEvent::Revoke {
                sandbox_id: gone.0,
                execution_id: gone.1,
            },
            GrantEvent::Revoke {
                sandbox_id: superseded.0,
                execution_id: superseded.1,
            },
        ]
    );

    orchestrator.delete_sandbox(live.id).await?;
    Ok(())
}

#[tokio::test]
async fn a_create_without_rules_asks_for_no_grant() -> Result<()> {
    use crate::orchestrator::grants::recording::RecordingGrantIssuer;

    setup();
    let orchestrator = make_orchestrator_with_factory(MockBackendFactory::new()).await;
    let grants = RecordingGrantIssuer::shared();
    orchestrator.set_grant_issuer(Arc::clone(&grants) as Arc<dyn crate::orchestrator::GrantIssuer>);

    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[]))
        .await?;
    assert!(grants.events().is_empty());
    orchestrator.delete_sandbox(created.id).await?;
    assert_eq!(grants.events().len(), 1, "a delete always revokes, cheaply");
    Ok(())
}

#[tokio::test]
async fn the_node_half_issuer_starts_a_create_with_rules_without_a_store() -> Result<()> {
    setup();
    let orchestrator = make_orchestrator_with_factory(MockBackendFactory::new()).await;
    orchestrator.set_grant_issuer(crate::orchestrator::GrantsIssuedUpstream::shared());
    let mut request = create_request(Some(60), &[]);
    request.network_policy = policy_with_rules("openai");

    let created = orchestrator.create_sandbox(request).await?;
    assert_eq!(orchestrator.list_sandboxes().await?.len(), 1);
    orchestrator.delete_sandbox(created.id).await?;
    Ok(())
}

#[tokio::test]
async fn without_a_grant_issuer_a_create_with_rules_fails_and_leaves_no_record() -> Result<()> {
    setup();
    let orchestrator = make_orchestrator_with_factory(MockBackendFactory::new()).await;
    let mut request = create_request(Some(60), &[]);
    request.network_policy = policy_with_rules("openai");

    let err = orchestrator.create_sandbox(request).await.err().unwrap();
    let OrchestratorError::SandboxOperationFailed { source, .. } = &err else {
        panic!("expected a start failure, got {err:#}");
    };
    assert!(
        source
            .chain()
            .any(|cause| cause.to_string().contains("no secrets store")),
        "the failure names the missing store: {source:#}"
    );
    assert!(orchestrator.list_sandboxes().await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn an_update_that_adds_a_rule_grants_its_secret_before_the_runtime_takes_it() -> Result<()> {
    use crate::orchestrator::grants::recording::{GrantEvent, RecordingGrantIssuer};

    setup();
    let orchestrator = make_orchestrator_with_factory(MockBackendFactory::new()).await;
    let grants = RecordingGrantIssuer::shared();
    orchestrator.set_grant_issuer(Arc::clone(&grants) as Arc<dyn crate::orchestrator::GrantIssuer>);

    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[]))
        .await?;
    assert!(grants.events().is_empty());

    orchestrator
        .replace_sandbox_network_policy(created.id, policy_with_rules("openai"))
        .await?;

    let expected: std::collections::BTreeSet<String> = ["openai".to_string()].into_iter().collect();
    assert_eq!(
        grants.events(),
        vec![GrantEvent::Grant {
            sandbox_id: created.id,
            execution_id: created.execution_id,
            names: expected,
        }],
        "a 204 with no grant is a policy the broker refuses on every request"
    );
    Ok(())
}

#[tokio::test]
async fn an_update_replaces_the_grant_with_exactly_the_new_names() -> Result<()> {
    use crate::orchestrator::grants::recording::{GrantEvent, RecordingGrantIssuer};

    setup();
    let orchestrator = make_orchestrator_with_factory(MockBackendFactory::new()).await;
    let grants = RecordingGrantIssuer::shared();
    orchestrator.set_grant_issuer(Arc::clone(&grants) as Arc<dyn crate::orchestrator::GrantIssuer>);

    let mut request = create_request(Some(60), &[]);
    request.network_policy = policy_with_rules("openai");
    let created = orchestrator.create_sandbox(request).await?;

    orchestrator
        .replace_sandbox_network_policy(created.id, policy_with_rules("anthropic"))
        .await?;

    let expected: std::collections::BTreeSet<String> =
        ["anthropic".to_string()].into_iter().collect();
    assert_eq!(
        grants.events().last(),
        Some(&GrantEvent::Grant {
            sandbox_id: created.id,
            execution_id: created.execution_id,
            names: expected,
        }),
        "the replaced name must not stay granted alongside the new one"
    );
    Ok(())
}

#[tokio::test]
async fn an_update_that_drops_every_rule_revokes_the_grant() -> Result<()> {
    use crate::orchestrator::grants::recording::{GrantEvent, RecordingGrantIssuer};

    setup();
    let orchestrator = make_orchestrator_with_factory(MockBackendFactory::new()).await;
    let grants = RecordingGrantIssuer::shared();
    orchestrator.set_grant_issuer(Arc::clone(&grants) as Arc<dyn crate::orchestrator::GrantIssuer>);

    let mut request = create_request(Some(60), &[]);
    request.network_policy = policy_with_rules("openai");
    let created = orchestrator.create_sandbox(request).await?;

    orchestrator
        .replace_sandbox_network_policy(created.id, SandboxNetworkPolicy::default())
        .await?;

    assert_eq!(
        grants.events().last(),
        Some(&GrantEvent::Revoke {
            sandbox_id: created.id,
            execution_id: created.execution_id,
        }),
        "a rule the caller removed must not leave its secret readable"
    );
    Ok(())
}

#[tokio::test]
async fn an_update_whose_grant_is_refused_fails_and_leaves_the_runtime_alone() -> Result<()> {
    setup();
    let behavior = Arc::new(MockBehavior::new());
    let orchestrator =
        make_orchestrator_with_factory(MockBackendFactory::with_behavior(Arc::clone(&behavior)))
            .await;

    // The default issuer has no store, so any non-empty name set is refused.
    let created = orchestrator
        .create_sandbox(create_request(Some(60), &[]))
        .await?;

    let err = orchestrator
        .replace_sandbox_network_policy(created.id, policy_with_rules("openai"))
        .await
        .expect_err("a policy whose secrets cannot be granted must not answer 204");
    assert!(
        matches!(err, OrchestratorError::SandboxOperationFailed { .. }),
        "got {err:#}"
    );
    assert_eq!(
        behavior.update_network_calls(),
        0,
        "the runtime must not take rules whose credentials were refused"
    );

    let stored = orchestrator
        .get_sandbox(&created.id)
        .await?
        .expect("the sandbox is still there");
    assert!(stored.network_policy.egress.rules.is_empty());
    Ok(())
}

#[tokio::test]
async fn an_update_the_runtime_refuses_puts_the_previous_grant_back() -> Result<()> {
    use crate::orchestrator::grants::recording::{GrantEvent, RecordingGrantIssuer};
    use crate::sandbox::mock::{MockAction, MockOperation};

    setup();
    let behavior = Arc::new(MockBehavior::new());
    let orchestrator =
        make_orchestrator_with_factory(MockBackendFactory::with_behavior(Arc::clone(&behavior)))
            .await;
    let grants = RecordingGrantIssuer::shared();
    orchestrator.set_grant_issuer(Arc::clone(&grants) as Arc<dyn crate::orchestrator::GrantIssuer>);

    let mut request = create_request(Some(60), &[]);
    request.network_policy = policy_with_rules("openai");
    let created = orchestrator.create_sandbox(request).await?;

    behavior.push_action(
        MockOperation::UpdateNetwork,
        MockAction::Fail {
            message: "the node cannot broker".to_string(),
        },
    );
    assert!(orchestrator
        .replace_sandbox_network_policy(created.id, policy_with_rules("anthropic"))
        .await
        .is_err());

    let expected: std::collections::BTreeSet<String> = ["openai".to_string()].into_iter().collect();
    assert_eq!(
        grants.events().last(),
        Some(&GrantEvent::Grant {
            sandbox_id: created.id,
            execution_id: created.execution_id,
            names: expected,
        }),
        "the sandbox still runs the previous rules, so it keeps their grant"
    );
    Ok(())
}

#[tokio::test]
async fn a_fork_child_that_never_started_does_not_keep_its_grant() -> Result<()> {
    use crate::orchestrator::grants::recording::{GrantEvent, RecordingGrantIssuer};
    use crate::sandbox::mock::{MockAction, MockOperation};

    setup();
    let behavior = Arc::new(MockBehavior::new());
    let orchestrator =
        make_orchestrator_with_factory(MockBackendFactory::with_behavior(Arc::clone(&behavior)))
            .await;
    let grants = RecordingGrantIssuer::shared();
    orchestrator.set_grant_issuer(Arc::clone(&grants) as Arc<dyn crate::orchestrator::GrantIssuer>);

    let mut request = create_request(Some(60), &[]);
    request.network_policy = policy_with_rules("openai");
    let created = orchestrator.create_sandbox(request).await?;

    behavior.push_action(
        MockOperation::Fork,
        MockAction::Fail {
            message: "no memory for a child".to_string(),
        },
    );
    assert!(orchestrator
        .fork_sandbox(created.id, ForkChildren::Fresh(1), NewTimeout::UseExisting)
        .await
        .is_err());

    let child_revokes: Vec<_> = grants
        .events()
        .into_iter()
        .filter(|event| match event {
            GrantEvent::Revoke { sandbox_id, .. } => *sandbox_id != created.id,
            _ => false,
        })
        .collect();
    assert_eq!(
        child_revokes.len(),
        1,
        "the child's grant outlived a child that never existed: {:?}",
        grants.events()
    );
    Ok(())
}

#[tokio::test]
async fn a_delete_whose_record_removal_fails_still_revokes() -> Result<()> {
    use crate::orchestrator::grants::recording::{GrantEvent, RecordingGrantIssuer};

    setup();
    let control = Arc::new(ScriptedStoreControl::default());
    let orchestrator =
        make_orchestrator_without_background(ScriptedStore::new(Arc::clone(&control)));
    let grants = RecordingGrantIssuer::shared();
    orchestrator.set_grant_issuer(Arc::clone(&grants) as Arc<dyn crate::orchestrator::GrantIssuer>);

    let mut request = create_request(Some(60), &[]);
    request.network_policy = policy_with_rules("openai");
    let created = orchestrator.create_sandbox(request).await?;

    control.push_remove_action(StoreAction::Fail(StoreError::Backend {
        source: anyhow::anyhow!("the store is unreachable"),
    }));
    assert!(orchestrator.delete_sandbox(created.id).await.is_err());

    assert_eq!(
        grants.events().last(),
        Some(&GrantEvent::Revoke {
            sandbox_id: created.id,
            execution_id: created.execution_id,
        }),
        "a record this process could not remove must not keep its grant alive"
    );
    Ok(())
}
