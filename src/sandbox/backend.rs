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
use crate::sandbox::CustomExtensionParams;
use crate::snapshot::RunnableSnapshot;
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

    /// The machine whose disk holds this capture, when that is not the machine
    /// this process runs on.
    ///
    /// 🔴 `None` — the default, and the answer for every backend that captures
    /// locally — means *this machine*, never *nowhere*. Whether a local capture
    /// went anywhere that outlives the runtime is
    /// [`PausedSandboxCapture::publishable`]'s question, and the two are not
    /// interchangeable: a pause driven from the deciding half offers nothing
    /// publishable *here* and is still parked, durably, on the node that wrote
    /// the bytes. Reading the second question's answer as the first's is how a
    /// pause that is perfectly recoverable comes to be treated as one that left
    /// nothing behind.
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

/// Downcast marker on an `anyhow::Error` out of [`SandboxBackend::start`]:
/// more than "could not reach this runtime right now" — the caller has
/// independently confirmed that the machine the runtime depends on is no
/// longer part of the cluster, so nothing about it is coming back on its own.
///
/// # 🔴 A marker on the error, not a new branch on `start`'s signature
///
/// `start` is shared by every backend — a fresh boot, a resume, a fork child
/// readying, and a stub *attaching* to a sandbox this process never started —
/// and only the last of those, driven against a machine that can be asked
/// about its own node's cluster membership, can ever produce this. Widening
/// `start`'s return type for one caller's one outcome would be a branch every
/// other implementer has to answer for and never can. Downcast out of the
/// plain error instead, the same way [`SandboxCaptureError`] is recovered
/// from an error `pause` never typed as one.
///
/// # Where this is raised and where it is read
///
/// The remote node-client stub raises it out of `attach` once the placement
/// source has answered that the node it could not dial is gone from the
/// cluster's own registry — not merely unreachable this instant. The
/// orchestrator's `absent_handle` is the one place that downcasts for it: it
/// is what turns "could not reach the sandbox, leave the record alone" into
/// "the runtime is gone, the record may be forgotten" for a sandbox this
/// process never started itself.
#[derive(thiserror::Error, Debug)]
#[error("{0}")]
pub struct RuntimeConfirmedGone(#[source] pub anyhow::Error);

pub type SandboxCaptureResult<T> = std::result::Result<T, SandboxCaptureError>;
pub type SandboxForkResult = anyhow::Result<Box<dyn SandboxBackend>>;

#[derive(Clone, Debug)]
pub struct SandboxForkSpec {
    pub sandbox_id: SandboxId,
    /// The child's own incarnation.
    ///
    /// 🔴 Required, and deliberately not defaulted. A fork child's metadata is
    /// built by cloning the parent's and overwriting the fields that differ, so
    /// an incarnation that could be left out would be inherited from the parent
    /// — two live VMs sharing one identity, with nothing to warn about it.
    /// Making it a field of this struct puts the mint next to
    /// `SandboxId::new()` at every construction site and makes forgetting it a
    /// compile error.
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
    pub(crate) fn from_overlaybd_image_configs(overlaybd_image_config_paths: Vec<PathBuf>) -> Self {
        Self {
            overlaybd_image_config_paths,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.overlaybd_image_config_paths.is_empty()
    }

    pub(crate) fn into_overlaybd_image_config_paths(self) -> Vec<PathBuf> {
        self.overlaybd_image_config_paths
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SandboxRuntimeInfo {
    pub rootfs_virtual_size: Option<u64>,
    pub runtime_artifacts: RuntimeArtifactSet,
    /// The sandbox's context and image configs, when this backend only
    /// learned them once it had started.
    ///
    /// # 🔴 `None` for every backend that already knew before it was built
    ///
    /// A local factory resolves an image before it ever builds a backend, so
    /// the orchestrator's transitional record is already right and this stays
    /// `None`. `RemoteSandboxStub` is the one exception: built from
    /// [`UnresolvedImageBuildSpec`], it does not learn the resolved context
    /// and image configs until the node's `Create` reply comes back inside
    /// `start()` — the node resolved the reference, this process never did.
    /// The orchestrator reads this after `start_nowait` succeeds and, when it
    /// is `Some`, overwrites the placeholder it wrote into the transitional
    /// record before the backend existed. See
    /// `Orchestrator::launch_sandbox`.
    pub resolved_image_facts: Option<ResolvedImageFacts>,
}

/// A sandbox's context and image configs, learned by a backend that did not
/// know them until it started. See [`SandboxRuntimeInfo::resolved_image_facts`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedImageFacts {
    pub context: crate::snapshot::CommandContext,
    pub image_configs: crate::types::ImageConfigs,
}

/// Opaque captured snapshot artifacts produced from a running sandbox.
///
/// Unlike [`PausedSandboxState`], this value is intended for one-shot
/// consumption by snapshot publication code. Concrete backends may use it to
/// keep temporary artifact directories alive until publication finishes.
pub struct CapturedSandboxSnapshot {
    inner: Box<dyn Any + Send>,
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

impl CapturedSandboxSnapshot {
    pub fn new<T>(snapshot: T) -> Self
    where
        T: Send + 'static,
    {
        Self {
            inner: Box::new(snapshot),
        }
    }

    pub fn downcast_ref<T>(&self) -> Option<&T>
    where
        T: Send + 'static,
    {
        self.inner.downcast_ref::<T>()
    }

    pub fn downcast<T>(self) -> std::result::Result<T, Self>
    where
        T: Send + 'static,
    {
        match self.inner.downcast::<T>() {
            Ok(inner) => Ok(*inner),
            Err(inner) => Err(Self { inner }),
        }
    }
}

impl fmt::Debug for CapturedSandboxSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CapturedSandboxSnapshot")
            .field("opaque", &true)
            .finish()
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
    /// # 🔴 `committer_waiting` binds only the backends that have to spend to
    /// answer it
    ///
    /// It says whether the caller will commit a
    /// [`PausedSandboxCapture::publishable`] if one is offered. An
    /// implementation that produces one by *writing durable bytes* must not
    /// write them when this is `false`: nothing announces bytes nobody
    /// commits, no read path resolves an unannounced snapshot, and so nothing
    /// ever finds them again.
    ///
    /// An implementation whose publishable capture is a borrow of artifacts
    /// something else already wrote spends nothing to offer one and may offer
    /// it either way. Suppressing it there would not save storage; it would
    /// only change *which* "not recorded" reason the caller reports, turning
    /// "nobody was going to commit this" into "there was nothing to commit".
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

    /// The real machine this sandbox is running on, when that is not the
    /// machine this process is running on.
    ///
    /// 🔴 `None` — the default, and the answer for every backend that runs the
    /// VM in this same process — means *this machine*, mirroring the exact
    /// convention [`PausedSandboxState::holding_node_id`] already uses for the
    /// paused half of the same question. `Some` only from a backend that
    /// drives the sandbox over the wire, once it has learned which machine
    /// accepted it. A caller that needs a cluster-visible node identity for a
    /// running sandbox — the paused-sandbox registry's `origin_node_id` is the
    /// one this exists for — must ask here rather than assume its own
    /// identity is the answer; see the doc on
    /// [`PausedSandboxState::holding_node_id`] for why the assumption is wrong
    /// on exactly the role this backend is for.
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

    /// Build a brand-new sandbox backend from an OCI image reference this
    /// factory has not resolved.
    ///
    /// # 🔴 Default refuses, and that is the answer for every factory that
    /// resolves images itself
    ///
    /// [`build`](Self::build) already covers "build fresh, from an image": a
    /// factory that runs sandboxes locally resolves the reference into a
    /// [`FreshSandboxBuildSpec`] before it ever reaches a factory, because
    /// resolving needs `regctl` and the factory has it. This method exists
    /// for the one factory that does not — `RemoteSandboxBackendFactory`,
    /// whose sandboxes run on a machine that has `regctl` and this process
    /// does not — and it is the only implementation that should ever override
    /// the refusal below. Every local factory, and every test mock that
    /// builds locally, is correct to inherit it unchanged.
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

    /// Whether the sandboxes this factory builds need the control plane's
    /// ownership marker sent with them.
    ///
    /// 🔴 `false` for a factory that builds sandboxes on this machine, and the
    /// default is that answer rather than the other one. A machine-local
    /// orchestrator's records and its sandboxes are the same process's; the
    /// marker exists for the case where they are not, and stamping one on a
    /// local sandbox would mean the user-facing REST surface producing
    /// sandboxes that claim to belong to a control plane — the exact inference
    /// `ControlPlaneConfig` was made explicit to end.
    ///
    /// A factory that answers `true` is one whose sandboxes run somewhere
    /// else, and the marker is how the machine they run on can hand the record
    /// back to whoever owns it.
    fn stamps_control_plane_ownership(&self) -> bool {
        false
    }

    /// Decode backend-specific paused state loaded from persistence.
    fn decode_paused_state(
        &self,
        artifact_root: PathBuf,
        state: Value,
    ) -> Result<Arc<dyn PausedSandboxState>>;

    /// Whether the sandboxes this factory builds keep running after this
    /// process exits.
    ///
    /// 🔴 `false` — the default — is what makes a shutdown pause every sandbox
    /// in the record store before the process goes away: the VMs are this
    /// process's, so nobody else will preserve them.
    ///
    /// A factory that answers `true` runs its sandboxes on other machines, and
    /// a caller that preserved them on the way out would be pausing the whole
    /// cluster's running sandboxes every time one replica of a replicated half
    /// was rolled. Two answers rather than a role check because the fact
    /// belongs to the factory: it is the thing that knows where its sandboxes
    /// are.
    fn sandboxes_outlive_this_process(&self) -> bool {
        false
    }

    /// A backend for a sandbox that is **already running**, rebuilt from what
    /// the record says about it.
    ///
    /// # 🔴 `Ok(None)` is a fact about this factory, not about the sandbox
    ///
    /// It means *the sandboxes this factory builds live in the process that
    /// started them*, so a caller holding no handle for one is holding no
    /// handle for a runtime that is gone. It does **not** mean the sandbox does
    /// not exist — the caller has a record in front of it saying otherwise —
    /// and the difference is the whole reason this returns an `Option` instead
    /// of an error.
    ///
    /// A factory whose sandboxes run on other machines answers `Some`. Nothing
    /// about the machine is needed here: the identity, the incarnation and the
    /// record are enough to address a sandbox that is already up, and finding
    /// which machine it is on is the backend's own asynchronous work.
    ///
    /// 🔴 An `Err` is "I could not tell", and a caller may not read it as
    /// either of the two answers above. The default is `Ok(None)` because a
    /// machine-local factory can answer that without asking anyone.
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
    /// use agentenv::sandbox::SandboxExecutor;
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
    /// use agentenv::sandbox::{ProcessOpts, SandboxExecutor};
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
    /// use agentenv::sandbox::SandboxExecutor;
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
    /// use agentenv::sandbox::{ProcessOpts, SandboxExecutor};
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
