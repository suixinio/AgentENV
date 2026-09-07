//! In-process mock sandbox backend for unit testing.
//!
//! [`MockSandboxBackend`] immediately completes all lifecycle operations
//! without starting any real process. It is used by
//! [`MockBackendFactory`] to power Orchestrator unit tests that do not
//! need a real VM.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::{sync::Mutex, thread};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use tokio::time::sleep;

use super::backend::{
    CapturedSandboxSnapshot, RuntimeArtifactSet, SandboxBackend, SandboxBackendFactory,
    SandboxCaptureResult, SandboxForkResult, SandboxForkSpec, SandboxRuntimeInfo,
};
use super::{FreshSandboxBuildSpec, SandboxCaptureError, SandboxLaunchConfig};
use crate::runtime_snapshot::RunnableSnapshot;
use crate::sandbox::CustomExtensionParams;
use crate::types::ExecutionId;

/// What a liveness probe of a mock backend answers.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum MockLiveness {
    #[default]
    Running,
    Gone,
    Unanswerable,
}

#[derive(Debug)]
pub struct MockCapturedSnapshot;

impl crate::snapshot::LocalCapturedArtifacts for MockCapturedSnapshot {
    fn publishable_manifest(&self) -> Option<&crate::types::FirecrackerSnapshotManifest> {
        None
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum MockOperation {
    Build,
    BuildFromSnapshot,
    Start,
    StartNowait,
    WaitForReady,
    Pause,
    Resume,
    Snapshot,
    Fork,
    ForkChild,
    Stop,
    UpdateNetwork,
    UpdateCustomExtensionParams,
}

#[derive(Clone, Debug)]
pub enum MockAction {
    Succeed,
    SucceedAfter(Duration),
    Fail {
        message: String,
    },
    FailTerminal {
        message: String,
    },
    /// A capture failure the node did not classify.
    FailUnknown {
        message: String,
    },
    FailAfter {
        delay: Duration,
        message: String,
    },
}

#[derive(Default)]
pub struct MockBehavior {
    actions: Mutex<HashMap<MockOperation, VecDeque<MockAction>>>,
    on_operation: Mutex<HashMap<MockOperation, Arc<dyn Fn() + Send + Sync>>>,
    runtime_info: Mutex<SandboxRuntimeInfo>,
    source_config_paths: Mutex<Vec<std::path::PathBuf>>,
    stop_calls: AtomicUsize,
    update_network_calls: AtomicUsize,
    forked_children: AtomicUsize,
    fork_children_without_address: AtomicBool,
    last_custom_extension_params: Mutex<Option<Option<CustomExtensionParams>>>,
    captures_are_stageable: AtomicBool,
    captures_are_staged: AtomicBool,
    liveness: Mutex<MockLiveness>,
    holding_node_id: Mutex<Option<&'static str>>,
}

impl MockBehavior {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push_action(&self, operation: MockOperation, action: MockAction) {
        let mut actions = self.actions.lock().expect("mock behavior mutex poisoned");
        actions.entry(operation).or_default().push_back(action);
    }

    /// Makes every capture from here on something a repository can stage.
    pub fn make_captures_stageable(&self) {
        self.captures_are_stageable.store(true, Ordering::SeqCst);
    }

    /// What a liveness probe of this backend answers.
    pub fn set_liveness(&self, liveness: MockLiveness) {
        *self.liveness.lock().expect("mock behavior mutex poisoned") = liveness;
    }

    fn probe_liveness(&self) -> Result<bool> {
        match *self.liveness.lock().expect("mock behavior mutex poisoned") {
            MockLiveness::Running => Ok(true),
            MockLiveness::Gone => Ok(false),
            MockLiveness::Unanswerable => Err(anyhow!("the node could not be reached")),
        }
    }

    /// Makes every capture from here on one another machine already staged,
    /// which is what a pause on a remote node answers with.
    pub fn make_captures_staged(&self) {
        self.captures_are_staged.store(true, Ordering::SeqCst);
    }

    fn capture(&self) -> CapturedSandboxSnapshot {
        if self.captures_are_staged.load(Ordering::SeqCst) {
            return CapturedSandboxSnapshot::staged(
                crate::snapshot::repository::interfaces::StagedSnapshot {
                    commit: crate::snapshot::repository::interfaces::SnapshotCommit::new(
                        &crate::snapshot::SnapshotPublishMetadata::mock(),
                        Default::default(),
                        0,
                        Some("mock-node".to_string()),
                    ),
                    staged_at_unix_ms: 0,
                    origin_node_id: "mock-node".to_string(),
                },
            );
        }
        if self.captures_are_stageable.load(Ordering::SeqCst) {
            CapturedSandboxSnapshot::local(crate::snapshot::CallerOwnedArtifacts::new(
                crate::types::FirecrackerSnapshotManifest::for_test(32768, &[]),
            ))
        } else {
            CapturedSandboxSnapshot::local(MockCapturedSnapshot)
        }
    }

    /// Returns the last successfully applied custom-extension parameters.
    pub fn last_custom_extension_params(&self) -> Option<Option<CustomExtensionParams>> {
        self.last_custom_extension_params
            .lock()
            .expect("last_custom_extension_params mutex poisoned")
            .clone()
    }

    pub fn set_on_operation(&self, operation: MockOperation, hook: Arc<dyn Fn() + Send + Sync>) {
        self.on_operation
            .lock()
            .expect("on_operation mutex poisoned")
            .insert(operation, hook);
    }

    pub fn set_runtime_info(&self, runtime_info: SandboxRuntimeInfo) {
        *self
            .runtime_info
            .lock()
            .expect("runtime_info mutex poisoned") = runtime_info;
    }

    /// Makes every backend built from here on answer `holding_node_id` with
    /// `node_id`, the way `RemoteSandboxStub` does once placement has resolved
    /// it. `None` restores the default — this process is the machine.
    pub fn set_holding_node_id(&self, node_id: Option<&'static str>) {
        *self
            .holding_node_id
            .lock()
            .expect("holding_node_id mutex poisoned") = node_id;
    }

    fn runtime_info(&self) -> SandboxRuntimeInfo {
        self.runtime_info
            .lock()
            .expect("runtime_info mutex poisoned")
            .clone()
    }

    /// Makes every fork child from here on come back without an address.
    pub fn set_fork_children_without_address(&self, without: bool) {
        self.fork_children_without_address
            .store(without, Ordering::SeqCst);
    }

    fn next_fork_child_host_ip(
        &self,
        parent: Option<std::net::Ipv4Addr>,
    ) -> Option<std::net::Ipv4Addr> {
        if self.fork_children_without_address.load(Ordering::SeqCst) {
            return None;
        }
        let offset = self.forked_children.fetch_add(1, Ordering::SeqCst);
        let offset = u32::try_from(offset).unwrap_or(u32::MAX);
        parent.map(|parent| {
            std::net::Ipv4Addr::from(u32::from(parent).wrapping_add(offset).wrapping_add(1))
        })
    }

    pub fn set_source_config_paths(&self, paths: Vec<std::path::PathBuf>) {
        *self
            .source_config_paths
            .lock()
            .expect("source_config_paths mutex poisoned") = paths;
    }

    fn source_config_paths(&self) -> Vec<std::path::PathBuf> {
        self.source_config_paths
            .lock()
            .expect("source_config_paths mutex poisoned")
            .clone()
    }

    pub fn stop_calls(&self) -> usize {
        self.stop_calls.load(Ordering::Relaxed)
    }

    pub fn update_network_calls(&self) -> usize {
        self.update_network_calls.load(Ordering::Relaxed)
    }

    fn pop_action(&self, operation: MockOperation) -> MockAction {
        let mut actions = self.actions.lock().expect("mock behavior mutex poisoned");
        actions
            .get_mut(&operation)
            .and_then(VecDeque::pop_front)
            .unwrap_or(MockAction::Succeed)
    }

    fn run_operation_hook(&self, operation: MockOperation) {
        if let Some(hook) = self
            .on_operation
            .lock()
            .expect("on_operation mutex poisoned")
            .get(&operation)
            .cloned()
        {
            hook();
        }
    }

    async fn run_async_action<E, F, G>(
        action: MockAction,
        fail: F,
        fail_terminal: G,
    ) -> std::result::Result<(), E>
    where
        F: FnOnce(String) -> E,
        G: FnOnce(String) -> E,
    {
        match action {
            MockAction::Succeed => Ok(()),
            MockAction::SucceedAfter(delay) => {
                sleep(delay).await;
                Ok(())
            }
            MockAction::Fail { message } | MockAction::FailUnknown { message } => {
                Err(fail(message))
            }
            MockAction::FailTerminal { message } => Err(fail_terminal(message)),
            MockAction::FailAfter { delay, message } => {
                sleep(delay).await;
                Err(fail(message))
            }
        }
    }

    fn run_sync_action<E, F, G>(
        action: MockAction,
        fail: F,
        fail_terminal: G,
    ) -> std::result::Result<(), E>
    where
        F: FnOnce(String) -> E,
        G: FnOnce(String) -> E,
    {
        match action {
            MockAction::Succeed => Ok(()),
            MockAction::SucceedAfter(delay) => {
                thread::sleep(delay);
                Ok(())
            }
            MockAction::Fail { message } | MockAction::FailUnknown { message } => {
                Err(fail(message))
            }
            MockAction::FailTerminal { message } => Err(fail_terminal(message)),
            MockAction::FailAfter { delay, message } => {
                thread::sleep(delay);
                Err(fail(message))
            }
        }
    }

    async fn apply_capture_result(&self, operation: MockOperation) -> SandboxCaptureResult<()> {
        self.run_operation_hook(operation);
        let action = self.pop_action(operation);
        if let MockAction::FailUnknown { message } = action {
            return Err(SandboxCaptureError::unknown(anyhow!(message)));
        }
        Self::run_async_action(
            action,
            |message| SandboxCaptureError::recoverable(anyhow!(message)),
            |message| SandboxCaptureError::terminal(anyhow!(message)),
        )
        .await
    }

    async fn apply_async(&self, operation: MockOperation) -> Result<()> {
        self.run_operation_hook(operation);
        match operation {
            MockOperation::Stop => {
                self.stop_calls.fetch_add(1, Ordering::Relaxed);
            }
            MockOperation::UpdateNetwork => {
                self.update_network_calls.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }

        Self::run_async_action(
            self.pop_action(operation),
            |message| anyhow!(message),
            |message| anyhow!(message),
        )
        .await
    }

    fn apply_sync(&self, operation: MockOperation) -> Result<()> {
        Self::run_sync_action(
            self.pop_action(operation),
            |message| anyhow!(message),
            |message| anyhow!(message),
        )
    }
}

// ── MockSandboxBackend ────────────────────────────────────────────────────────

/// A no-op sandbox backend for unit tests.
///
/// All lifecycle operations succeed immediately; no real processes are spawned.
pub struct MockSandboxBackend {
    behavior: Arc<MockBehavior>,
    host_ip: Option<std::net::Ipv4Addr>,
    execution_id: ExecutionId,
}

impl MockSandboxBackend {
    pub fn new(behavior: Arc<MockBehavior>, execution_id: ExecutionId) -> Self {
        Self::new_with_host_ip(
            behavior,
            Some(std::net::Ipv4Addr::new(127, 0, 0, 1)),
            execution_id,
        )
    }

    pub fn new_with_host_ip(
        behavior: Arc<MockBehavior>,
        host_ip: Option<std::net::Ipv4Addr>,
        execution_id: ExecutionId,
    ) -> Self {
        Self {
            behavior,
            host_ip,
            execution_id,
        }
    }
}

#[async_trait]
impl SandboxBackend for MockSandboxBackend {
    fn execution_id(&self) -> ExecutionId {
        self.execution_id
    }

    async fn start(&mut self) -> Result<()> {
        self.behavior.apply_async(MockOperation::Start).await
    }

    async fn start_nowait(&mut self) -> Result<()> {
        self.behavior.apply_async(MockOperation::StartNowait).await
    }

    async fn wait_for_ready(&self) -> Result<()> {
        self.behavior.apply_async(MockOperation::WaitForReady).await
    }

    async fn pause(&mut self) -> SandboxCaptureResult<CapturedSandboxSnapshot> {
        let pause_result = self
            .behavior
            .apply_capture_result(MockOperation::Pause)
            .await;
        if let Err(pause_err) = pause_result {
            if pause_err.is_terminal() {
                return Err(pause_err);
            }
            if let Err(resume_err) = self.behavior.apply_async(MockOperation::Resume).await {
                return Err(SandboxCaptureError::terminal(anyhow!(
                    "pause failed and sandbox could not be resumed: pause error: {pause_err}; resume error: {resume_err:#}"
                )));
            }
            return Err(pause_err);
        }
        Ok(self.behavior.capture())
    }

    async fn resume(&mut self) -> Result<()> {
        self.behavior.apply_async(MockOperation::Resume).await
    }

    async fn snapshot(&mut self) -> SandboxCaptureResult<CapturedSandboxSnapshot> {
        self.behavior
            .apply_capture_result(MockOperation::Snapshot)
            .await?;
        Ok(self.behavior.capture())
    }

    async fn fork(
        &mut self,
        spec: &[SandboxForkSpec],
    ) -> SandboxCaptureResult<Vec<SandboxForkResult>> {
        self.behavior
            .apply_capture_result(MockOperation::Fork)
            .await?;
        Ok(spec
            .iter()
            .map(|child| {
                self.behavior
                    .apply_sync(MockOperation::ForkChild)
                    .map(|()| {
                        // Each child uses its specified incarnation and a distinct address.
                        Box::new(Self::new_with_host_ip(
                            Arc::clone(&self.behavior),
                            self.behavior.next_fork_child_host_ip(self.host_ip),
                            child.execution_id,
                        )) as Box<dyn SandboxBackend>
                    })
            })
            .collect())
    }

    async fn stop(&mut self) -> Result<()> {
        self.behavior.apply_async(MockOperation::Stop).await
    }

    async fn is_still_running(&mut self) -> Result<bool> {
        self.behavior.probe_liveness()
    }

    fn host_interaction_ip(&self) -> Option<std::net::Ipv4Addr> {
        self.host_ip
    }

    fn runtime_info(&self) -> SandboxRuntimeInfo {
        self.behavior.runtime_info()
    }

    fn holding_node_id(&self) -> Option<&str> {
        *self
            .behavior
            .holding_node_id
            .lock()
            .expect("holding_node_id mutex poisoned")
    }

    fn startup_artifacts(&self) -> RuntimeArtifactSet {
        RuntimeArtifactSet::from_overlaybd_image_configs(self.behavior.source_config_paths())
    }

    async fn update_network_policy(
        &mut self,
        _policy: Option<super::SandboxNetworkPolicy>,
    ) -> Result<()> {
        self.behavior
            .apply_async(MockOperation::UpdateNetwork)
            .await
    }

    async fn update_custom_extension_params(
        &mut self,
        params: Option<CustomExtensionParams>,
    ) -> Result<()> {
        self.behavior
            .apply_async(MockOperation::UpdateCustomExtensionParams)
            .await?;
        *self
            .behavior
            .last_custom_extension_params
            .lock()
            .expect("last_custom_extension_params mutex poisoned") = Some(params);
        Ok(())
    }
}

// ── MockBackendFactory ────────────────────────────────────────────────────────

/// Factory that produces [`MockSandboxBackend`] instances.
///
/// Produced backends use deterministic placeholder values suitable for
/// asserting against in tests.
pub struct MockBackendFactory {
    behavior: Arc<MockBehavior>,
    host_ip: Option<std::net::Ipv4Addr>,
}

impl MockBackendFactory {
    pub fn new() -> Self {
        Self::with_behavior(Arc::new(MockBehavior::new()))
    }

    pub fn with_behavior(behavior: Arc<MockBehavior>) -> Self {
        Self::with_behavior_and_host_ip(behavior, Some(std::net::Ipv4Addr::new(127, 0, 0, 1)))
    }

    pub fn with_behavior_and_host_ip(
        behavior: Arc<MockBehavior>,
        host_ip: Option<std::net::Ipv4Addr>,
    ) -> Self {
        Self { behavior, host_ip }
    }
}

impl Default for MockBackendFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl SandboxBackendFactory for MockBackendFactory {
    fn build(
        &self,
        _build_spec: FreshSandboxBuildSpec,
        _launch_config: SandboxLaunchConfig,
        execution_id: ExecutionId,
    ) -> Result<Box<dyn SandboxBackend>> {
        self.behavior.apply_sync(MockOperation::Build)?;
        Ok(Box::new(MockSandboxBackend::new_with_host_ip(
            Arc::clone(&self.behavior),
            self.host_ip,
            execution_id,
        )))
    }

    fn build_from_snapshot(
        &self,
        _snapshot: &RunnableSnapshot,
        _launch_config: SandboxLaunchConfig,
        execution_id: ExecutionId,
    ) -> Result<Box<dyn SandboxBackend>> {
        self.behavior.apply_sync(MockOperation::Build)?;
        Ok(Box::new(MockSandboxBackend::new_with_host_ip(
            Arc::clone(&self.behavior),
            self.host_ip,
            execution_id,
        )))
    }

    fn build_from_snapshot_record(
        &self,
        _record: &crate::snapshot::SnapshotRecord,
        _launch_config: SandboxLaunchConfig,
        execution_id: ExecutionId,
    ) -> Result<Box<dyn SandboxBackend>> {
        self.behavior.apply_sync(MockOperation::Build)?;
        Ok(Box::new(MockSandboxBackend::new_with_host_ip(
            Arc::clone(&self.behavior),
            self.host_ip,
            execution_id,
        )))
    }
}
