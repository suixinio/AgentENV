//! Abstractions for sandbox backends.
//!
//! [`SandboxBackend`] represents the lifecycle of a single sandbox instance.
//! [`SandboxBackendFactory`] is responsible for constructing new sandbox
//! instances (from scratch, from a committed snapshot, or from paused state).

use std::any::Any;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Result};
use async_trait::async_trait;
use serde_json::Value;

use super::{
    EnvdAccessToken, Executor, FreshSandboxBuildSpec, ProcessHandle, ProcessOpts, ProcessOutput,
    SandboxLaunchConfig, SandboxNetworkPolicy, UnresolvedImageBuildSpec,
};
use crate::runtime_snapshot::RunnableSnapshot;
use crate::sandbox::CustomExtensionParams;
pub use crate::snapshot::CapturedSandboxSnapshot;
use crate::types::{ExecutionId, SandboxId};

/// A concrete sandbox backend's paused state.
///
/// The Orchestrator treats this value as completely opaque: it stores it in
/// [`SandboxMetadata`][crate::orchestrator::SandboxMetadata] after a
/// `pause` call and passes it back to
/// [`SandboxBackendFactory::build_from_paused_state`] when a resume is requested.
/// Concrete implementations own their serialized form.
pub trait PausedSandboxState: Any + fmt::Debug + Send + Sync + 'static {
    fn encode(&self) -> Result<Value>;

    /// Local artifacts this paused sandbox will reopen on resume.
    /// The orchestrator only carries this value to the image-liveness layer; it
    /// does not interpret the backend-specific artifact identities inside it.
    fn runtime_artifacts(&self) -> RuntimeArtifactSet;

    /// Returns the remote machine holding this capture, or `None` when it is
    /// held on this machine.
    fn holding_node_id(&self) -> Option<&str> {
        None
    }
}

impl dyn PausedSandboxState {
    pub fn downcast_ref<T>(&self) -> Option<&T>
    where
        T: PausedSandboxState,
    {
        (self as &dyn Any).downcast_ref::<T>()
    }
}

#[derive(thiserror::Error, Debug)]
pub enum SandboxCaptureError {
    #[error("{0}")]
    Recoverable(#[source] anyhow::Error),
    #[error("{0}")]
    Terminal(#[source] anyhow::Error),
}

impl SandboxCaptureError {
    pub fn recoverable(err: anyhow::Error) -> Self {
        Self::Recoverable(err)
    }

    pub fn terminal(err: anyhow::Error) -> Self {
        Self::Terminal(err)
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Terminal(_))
    }
}

impl From<anyhow::Error> for SandboxCaptureError {
    fn from(err: anyhow::Error) -> Self {
        match err.downcast::<Self>() {
            Ok(snapshot_err) => snapshot_err,
            Err(err) => Self::Recoverable(err),
        }
    }
}

/// Marks a runtime as permanently gone after its machine was confirmed absent
/// from the cluster.
#[derive(thiserror::Error, Debug)]
#[error("{0}")]
pub struct RuntimeConfirmedGone(#[source] pub anyhow::Error);

/// Marks a sandbox build failure as invalid caller input.
#[derive(thiserror::Error, Debug)]
#[error("{0}")]
pub struct InvalidSandboxRequest(pub String);

pub type SandboxCaptureResult<T> = std::result::Result<T, SandboxCaptureError>;
pub type SandboxForkResult = anyhow::Result<Box<dyn SandboxBackend>>;

#[derive(Clone, Debug)]
pub struct SandboxForkSpec {
    pub sandbox_id: SandboxId,
    /// The child's required, independently minted incarnation.
    pub execution_id: ExecutionId,
    pub envd_access_token: Option<EnvdAccessToken>,
}

/// Opaque set of local runtime artifacts a sandbox needs while it is alive.
///
/// Sandbox backends construct this from their runtime config, the orchestrator
/// carries it across lifecycle boundaries, and the image-liveness layer decides
/// how to protect the concrete local artifacts.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RuntimeArtifactSet {
    overlaybd_image_config_paths: Vec<PathBuf>,
}

impl RuntimeArtifactSet {
    /// No local runtime artifacts.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Build from overlaybd image configs whose local-only layers must stay
    /// available while the sandbox may reopen them.
    pub fn from_overlaybd_image_configs(overlaybd_image_config_paths: Vec<PathBuf>) -> Self {
        Self {
            overlaybd_image_config_paths,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.overlaybd_image_config_paths.is_empty()
    }

    pub fn into_overlaybd_image_config_paths(self) -> Vec<PathBuf> {
        self.overlaybd_image_config_paths
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SandboxRuntimeInfo {
    pub rootfs_virtual_size: Option<u64>,
    pub runtime_artifacts: RuntimeArtifactSet,
    /// Image facts learned only after a remote backend starts.
    pub resolved_image_facts: Option<ResolvedImageFacts>,
}

/// A sandbox's context and image configs, learned by a backend that did not
/// know them until it started. See [`SandboxRuntimeInfo::resolved_image_facts`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedImageFacts {
    pub context: crate::snapshot::CommandContext,
    pub image_configs: crate::types::ImageConfigs,
}

/// Everything a single pause produced.
///
/// A pause captures the sandbox once and can express that capture two ways: the
/// backend-specific state the *same* node reopens on resume, and — when the
/// capture landed in a caller-managed artifact directory that outlives the
/// runtime — the publishable form a snapshot repository can commit so *any*
/// node can rebuild the sandbox. Both come out of the same capture, so offering
/// the publishable form costs no second snapshot.
pub struct PausedSandboxCapture {
    /// Reopened by the origin node on a local resume.
    pub state: Arc<dyn PausedSandboxState>,
    /// `None` when the backend cannot hand out a publishable capture — for
    /// example a pause into backend-managed temporary artifacts, which are
    /// reclaimed as soon as the paused state is dropped.
    pub publishable: Option<CapturedSandboxSnapshot>,
}

impl PausedSandboxCapture {
    /// A capture that only the pausing node can reopen.
    pub fn local_only(state: Arc<dyn PausedSandboxState>) -> Self {
        Self {
            state,
            publishable: None,
        }
    }
}

/// Lifecycle interface for a single sandbox instance.
///
/// Implementors must be `Send + 'static` so that they can be stored inside
/// `Arc<Mutex<Box<dyn SandboxBackend>>>` handles managed by the Orchestrator.
#[async_trait]
pub trait SandboxBackend: Send + 'static {
    /// The incarnation this backend was built for.
    ///
    /// Fixed for the lifetime of the backend: there is no setter, and neither
    /// `snapshot` nor the parent side of `fork` changes it, because both pause
    /// and resume the same VM in place rather than starting a new one.
    fn execution_id(&self) -> ExecutionId;

    /// Start the sandbox and block until readiness.
    async fn start(&mut self) -> Result<()>;

    /// Start the sandbox without waiting for the sandbox to become ready.
    async fn start_nowait(&mut self) -> Result<()>;

    /// Block until the sandbox signals readiness.
    ///
    /// Should be called after [`start_nowait`][Self::start_nowait] before any
    /// workload is submitted.
    async fn wait_for_ready(&self) -> Result<()>;

    /// Pause the sandbox and capture its state for later resume.
    ///
    /// After this call the caller is expected to invoke [`stop`][Self::stop]
    /// to release system resources; the paused state encapsulates everything
    /// needed to resume the sandbox later via
    /// [`SandboxBackendFactory::build_from_paused_state`].
    ///
    /// [`SandboxCaptureError::Terminal`] indicates snapshot capture mutated the live
    /// runtime before failing, so callers must not keep treating the sandbox
    /// as safely runnable.
    ///
    /// For simplicity, [`SandboxCaptureError::Recoverable`] must guarantee the sandbox
    /// has already been restored to a running state before the error is returned.
    ///
    /// `committer_waiting` is false when durable bytes produced solely for
    /// publication would be orphaned; borrowed existing artifacts may still be offered.
    async fn pause(
        &mut self,
        artifact_root: Option<&Path>,
        committer_waiting: bool,
    ) -> SandboxCaptureResult<PausedSandboxCapture>;

    /// Resume a paused but not-yet-stopped sandbox from its snapshot.
    ///
    /// Idempotent: calling `resume` more than once must not return an error.
    async fn resume(&mut self) -> Result<()>;

    /// Capture a persistent snapshot from a running sandbox.
    ///
    /// After this call the sandbox is expected to continue running.
    ///
    /// [`SandboxCaptureError::Terminal`] indicates snapshot capture mutated the live
    /// runtime before failing, so callers must not keep treating the sandbox
    /// as safely runnable.
    async fn snapshot(&mut self) -> SandboxCaptureResult<CapturedSandboxSnapshot>;

    /// Fork this running sandbox into ready child backends.
    ///
    /// The outer error is reserved for failures before child startup begins.
    /// After the source has been restored, implementations must attempt every
    /// child concurrently and return one result per `spec` entry in the
    /// same order. Successful children stay running when a sibling fails.
    ///
    /// [`SandboxCaptureError::Terminal`] indicates the fork attempt mutated the
    /// source runtime past safe resume, so callers must stop treating the
    /// source as runnable. Child construction/start failures after source
    /// recovery belong in the corresponding [`SandboxForkResult`].
    async fn fork(
        &mut self,
        spec: &[SandboxForkSpec],
    ) -> SandboxCaptureResult<Vec<SandboxForkResult>>;

    /// Stop the sandbox and release all associated system resources.
    ///
    /// Idempotent: calling `stop` more than once must not return an error.
    async fn stop(&mut self) -> Result<()>;

    /// Obtain the IP address that the sandbox can use to interact with the host.
    fn host_interaction_ip(&self) -> Option<std::net::Ipv4Addr>;

    /// Return runtime facts that are only known after the backend has started.
    fn runtime_info(&self) -> SandboxRuntimeInfo;

    /// Returns the remote machine running this sandbox, or `None` when it runs
    /// on this machine.
    fn holding_node_id(&self) -> Option<&str> {
        None
    }

    /// Local runtime artifacts this sandbox opens on start.
    fn startup_artifacts(&self) -> RuntimeArtifactSet;

    /// Update the sandbox network policy at runtime.
    async fn update_network_policy(&mut self, policy: Option<SandboxNetworkPolicy>) -> Result<()>;

    /// Update the custom extension params held by the sandbox runtime.
    ///
    /// Assignment of an already-approved value: the custom extension
    /// patch-params hook is invoked by the caller (orchestrator layer), not
    /// by the backend. Fallible like [`update_network_policy`], and for the
    /// same reason: a local backend's assignment is a plain field write and
    /// cannot fail, but a remote one is a round trip to the node actually
    /// running the sandbox, and that can time out or the node can refuse it.
    /// Callers must not update any durable record of the new value until this
    /// returns `Ok` — an error here means the running sandbox never saw the
    /// value, and a store that disagreed would be lying to the next `GET`.
    ///
    /// [`update_network_policy`]: SandboxBackend::update_network_policy
    async fn update_custom_extension_params(
        &mut self,
        params: Option<CustomExtensionParams>,
    ) -> Result<()>;
}

/// Factory interface for creating and restoring sandbox backend instances.
///
/// A single factory instance is stored inside the
/// [`Orchestrator`][crate::orchestrator::Orchestrator] and is used for every
/// `create_sandbox` and `resume_sandbox` request.
pub trait SandboxBackendFactory: Send + Sync + 'static {
    /// Build a brand-new sandbox backend from a high-level launch request.
    ///
    /// `execution_id` is required rather than derived: a factory that could
    /// default it would have a branch in which a sandbox starts under an
    /// incarnation nobody minted, and that branch is exactly what fencing has
    /// no defence against.
    fn build(
        &self,
        build_spec: FreshSandboxBuildSpec,
        launch_config: SandboxLaunchConfig,
        execution_id: ExecutionId,
    ) -> Result<Box<dyn SandboxBackend>>;

    /// Builds from an unresolved OCI image reference.
    ///
    /// The default rejects this path; only remote factories resolve on another node.
    fn build_from_image_ref(
        &self,
        _spec: UnresolvedImageBuildSpec,
        _launch_config: SandboxLaunchConfig,
        _execution_id: ExecutionId,
    ) -> Result<Box<dyn SandboxBackend>> {
        bail!(
            "this factory builds sandboxes on this machine and resolves OCI images itself, so \
             an already-unresolved image reference should never have reached it: see \
             SandboxBackendFactory::build for the path a local cold create actually takes"
        )
    }

    /// Build a sandbox backend from a runnable committed snapshot plus launch request.
    fn build_from_snapshot(
        &self,
        snapshot: &RunnableSnapshot,
        launch_config: SandboxLaunchConfig,
        execution_id: ExecutionId,
    ) -> Result<Box<dyn SandboxBackend>>;

    /// Builds from an unresolved committed snapshot record.
    ///
    /// The default rejects this path; local factories require a resolved snapshot.
    fn build_from_snapshot_record(
        &self,
        _record: &crate::snapshot::SnapshotRecord,
        _launch_config: SandboxLaunchConfig,
        _execution_id: ExecutionId,
    ) -> Result<Box<dyn SandboxBackend>> {
        bail!(
            "this factory builds sandboxes on this machine and resolves committed snapshots \
             itself, so an unresolved snapshot record should never have reached it: see \
             SandboxBackendFactory::build_from_snapshot for the path a local create from a \
             snapshot actually takes"
        )
    }

    /// Best-effort release of process-wide resources owned by this factory.
    fn release_process_wide_resources(&self) {}

    /// Whether remote sandboxes need a control-plane ownership marker.
    fn stamps_control_plane_ownership(&self) -> bool {
        false
    }

    /// Decode backend-specific paused state loaded from persistence.
    fn decode_paused_state(
        &self,
        artifact_root: PathBuf,
        state: Value,
    ) -> Result<Arc<dyn PausedSandboxState>>;

    /// Whether sandboxes built by this factory survive this process.
    fn sandboxes_outlive_this_process(&self) -> bool {
        false
    }

    /// Adopts a sandbox already running according to its record.
    ///
    /// `Ok(None)` means this factory cannot adopt an external runtime; `Err`
    /// means it could not determine the answer.
    fn adopt_running(
        &self,
        _sandbox_id: SandboxId,
        _execution_id: ExecutionId,
        _resources: crate::types::SandboxResources,
    ) -> Result<Option<Box<dyn SandboxBackend>>> {
        Ok(None)
    }

    /// Build a sandbox backend from backend-specific paused state captured by `pause`.
    ///
    /// 🔴 `decode_paused_state` above deliberately takes no incarnation: it
    /// reads the state a previous run left on disk, which says nothing about
    /// which run is about to happen. This one does, because it builds the run.
    fn build_from_paused_state(
        &self,
        sandbox_id: crate::types::SandboxId,
        execution_id: ExecutionId,
        state: &dyn PausedSandboxState,
        envd_access_token: Option<EnvdAccessToken>,
    ) -> Result<Box<dyn SandboxBackend>>;
}

/// Process execution capability of a running sandbox.
///
/// Implement [`executor`][Self::executor] to provide a [`ProcessClient`][envd::process::ProcessClient]-backed
/// [`Executor`]. The three convenience methods (`run_command`,
/// `run_command_with_opts`, `start_process`) have default implementations that
/// simply call `self.executor()?` and delegate, so callers can continue using
/// the familiar `sandbox.run_command(...)` pattern without boilerplate.
///
/// # Note on `Send`
/// `&Self` may be `!Send` (e.g. `FirecrackerSandbox` holds tonic clients that
/// are `!Sync`), so the generated futures are not required to be `Send`.
#[async_trait(?Send)]
pub trait SandboxExecutor: Send {
    /// Obtain a process executor backed by this sandbox's envd connection.
    ///
    /// Returns an error if the sandbox is not running.
    fn executor(&self) -> Result<Executor<'_>>;

    /// Run a command inside the sandbox and wait for it to complete.
    ///
    /// Returns the captured stdout, stderr, and exit code.
    ///
    /// # Example
    /// ```no_run
    /// use aenv_core::sandbox::SandboxExecutor;
    /// # async fn example(sandbox: &impl SandboxExecutor) -> anyhow::Result<()> {
    /// let output = sandbox.run_command("echo", &["hello", "world"]).await?;
    /// assert_eq!(output.exit_code, 0);
    /// println!("{}", output.stdout);
    /// # Ok(())
    /// # }
    /// ```
    async fn run_command(&self, cmd: &str, args: &[&str]) -> Result<ProcessOutput> {
        self.executor()?.run_command(cmd, args).await
    }

    /// Run a command with custom options and wait for it to complete.
    ///
    /// # Example
    /// ```no_run
    /// use aenv_core::sandbox::{ProcessOpts, SandboxExecutor};
    /// use std::collections::HashMap;
    /// # async fn example(sandbox: &impl SandboxExecutor) -> anyhow::Result<()> {
    /// let opts = ProcessOpts::new().with_cwd("/tmp");
    /// let output = sandbox.run_command_with_opts("ls", &["-la"], &opts).await?;
    /// # Ok(())
    /// # }
    /// ```
    async fn run_command_with_opts(
        &self,
        cmd: &str,
        args: &[&str],
        opts: &ProcessOpts,
    ) -> Result<ProcessOutput> {
        self.executor()?
            .run_command_with_opts(cmd, args, opts)
            .await
    }

    /// Create a directory (and any missing parents) inside the sandbox.
    ///
    /// Goes through envd's filesystem service rather than exec'ing a binary,
    /// so it works in images that ship no userland (scratch, distroless).
    /// An already-existing directory is not an error.
    ///
    /// # Example
    /// ```no_run
    /// use aenv_core::sandbox::SandboxExecutor;
    /// # async fn example(sandbox: &impl SandboxExecutor) -> anyhow::Result<()> {
    /// sandbox.create_dir_all("/home/user/work").await?;
    /// # Ok(())
    /// # }
    /// ```
    async fn create_dir_all(&self, path: &str) -> Result<()> {
        self.executor()?.create_dir_all(path).await
    }

    /// Start a long-running process and return a handle.
    ///
    /// # Example
    /// ```no_run
    /// use aenv_core::sandbox::{ProcessOpts, SandboxExecutor};
    /// # async fn example(sandbox: &impl SandboxExecutor) -> anyhow::Result<()> {
    /// let mut handle = sandbox.start_process("cat", &[], &ProcessOpts::default()).await?;
    /// handle.send_stdin(b"hello\n").await?;
    /// handle.kill().await?;
    /// # Ok(())
    /// # }
    /// ```
    async fn start_process(
        &self,
        cmd: &str,
        args: &[&str],
        opts: &ProcessOpts,
    ) -> Result<ProcessHandle> {
        self.executor()?.start_process(cmd, args, opts).await
    }
}
