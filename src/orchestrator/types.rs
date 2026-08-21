use std::collections::HashMap;
use std::fmt::Display;
use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::sandbox::CustomExtensionParams;
use crate::snapshot::CommandContext;
use crate::types::{ExecutionId, ImageConfigs, SandboxId, SandboxResources};

#[derive(Clone)]
pub enum SandboxLaunchSource {
    Snapshot(Box<crate::snapshot::RunnableSnapshot>),
    Image {
        image_ref: String,
        overlaybd_config_path: PathBuf,
        context: Box<CommandContext>,
        resources: Option<crate::types::SandboxResources>,
        extra_drives: Vec<crate::sandbox::ExtraDrive>,
        extra_boot_args: Option<String>,
        /// Raw source image config metadata for the sandbox's resolved images.
        image_configs: Box<ImageConfigs>,
    },
}

#[derive(Clone)]
pub struct CreateSandboxRequest {
    pub source: SandboxLaunchSource,
    pub timeout: Option<Duration>,
    pub timeout_action: super::SandboxTimeoutAction,
    pub auto_resume: bool,
    pub user_metadata: Option<HashMap<String, String>>,
    pub env_vars: Option<HashMap<String, String>>,
    pub network_policy: crate::sandbox::SandboxNetworkPolicy,
    pub secure: bool,
    /// Opaque user-provided JSON passed through to the custom extension hooks.
    pub custom_extension_params: Option<CustomExtensionParams>,
    /// The control plane's record of this sandbox, to be stored verbatim.
    ///
    /// 🔴 `None` for every user-facing create. Only the node gRPC surface — the
    /// one the API half drives — supplies one, and its presence is the *only*
    /// thing that marks a sandbox as the control plane's. See
    /// [`ControlPlaneConfig`][crate::orchestrator::ControlPlaneConfig].
    pub control_plane_config: Option<crate::orchestrator::ControlPlaneConfig>,
}

/// One sandbox this node is *running*, as against one it has a record of.
///
/// # 🔴 Why the two are not the same list
///
/// The record store is what a node claims to be holding; the table of live
/// sandbox handles is what it holds. They come apart exactly when something has
/// gone wrong — a create that half-failed, a teardown that did not finish — and
/// those are the cases anything reconciling a cluster against its nodes exists
/// to find. Answering from the store would be reconciling one ledger against
/// another.
///
/// So membership here comes from the handle table and nothing else. The record
/// store contributes attributes, and the fields it contributes are `Option`
/// because a live sandbox with no record is a real thing that must still be
/// reported.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LiveSandbox {
    pub sandbox_id: SandboxId,
    /// The run that is live, taken from the handle when the handle could be
    /// read and from the record otherwise.
    ///
    /// 🔴 `None` only when neither had one, which means the sandbox has no
    /// record and its handle was busy. Such a sandbox necessarily has no
    /// ownership marker either — the marker lives in the record — so it is not
    /// one any control plane is looking for.
    pub execution_id: Option<ExecutionId>,
    /// Whether the facts below were read from the live handle.
    ///
    /// 🔴 `false` means the handle was mid-operation and was not waited on. A
    /// listing that blocked behind a pause would take as long as the slowest
    /// operation on the node, and the thing waiting for it is deciding whether
    /// sandboxes still exist — so it reports what the record knows and says so,
    /// rather than either stalling or leaving the sandbox out.
    pub facts_from_handle: bool,
    pub host_interaction_ip: Option<std::net::Ipv4Addr>,
    pub rootfs_virtual_size: Option<u64>,
    pub created_at: Option<std::time::SystemTime>,
    pub expires_at: Option<std::time::SystemTime>,
    pub resources: Option<SandboxResources>,
    /// The control plane's record of this sandbox, when it has one.
    ///
    /// 🔴 Reported, not filtered on. Whether a caller wants only the sandboxes
    /// some control plane owns is a property of the surface being served, not
    /// of this list: the same list also answers "what is running on this
    /// machine at all", and a filter baked in here would quietly give that
    /// question the other one's answer.
    pub control_plane_config: Option<crate::orchestrator::ControlPlaneConfig>,
}

/// The children a fork is being asked to produce.
///
/// 🔴 Identity and ownership travel together, and both are the *caller's* to
/// decide when the caller is the control plane. Two reasons they cannot be
/// split:
///
/// - A child's record is built by cloning its parent's, so anything the child
///   must not inherit has to be overwritten from here. The incarnation was
///   already such a field; the ownership marker is the second.
/// - The marker is the control plane's whole record of the sandbox, and a
///   record names the sandbox it is about. A node that minted the child's id
///   would therefore be handing back a marker written before anyone knew which
///   sandbox it described.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ForkChildren {
    /// Mint `count` fresh identities here. No child carries an ownership
    /// marker: this is the user-facing fork, and the control plane did not ask
    /// for these sandboxes.
    Fresh(u32),
    /// One entry per child, in request order.
    ///
    /// 🔴 The order is the contract [`SandboxBackend::fork`] already states for
    /// its results — one result per spec, same order — and the outcomes this
    /// produces line up with it entry for entry.
    ///
    /// [`SandboxBackend::fork`]: crate::sandbox::SandboxBackend::fork
    Assigned(Vec<ForkChildAssignment>),
}

/// One fork child whose identity the caller has already decided.
///
/// 🔴 The identity here is the sandbox id and not the incarnation. Minting an
/// incarnation stays with the node, as it does for a create: a caller-supplied
/// one would be a second place a run can be authorised from, and the caller
/// learns the child's incarnation from the outcome anyway. What the caller must
/// decide is the sandbox id, because its own record of the child names it and
/// that record is written before the child exists.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ForkChildAssignment {
    pub sandbox_id: SandboxId,
    /// The control plane's record of this child, or `None` when it has none.
    pub control_plane_config: Option<crate::orchestrator::ControlPlaneConfig>,
}

impl ForkChildren {
    /// How many children this asks for.
    pub fn count(&self) -> u32 {
        match self {
            Self::Fresh(count) => *count,
            // A fork request cannot carry more children than a u32 counts, and
            // the API surface it arrives through is bounded far below that.
            Self::Assigned(children) => children.len().try_into().unwrap_or(u32::MAX),
        }
    }
}

/// One sandbox as the heartbeat reports it: which sandbox, which incarnation,
/// and how long its routing projection should live.
///
/// The TTL travels with the roster and not only on the create response because
/// the roster is the *repair* path — the one that reinstalls a projection write
/// that was lost. A repair that installs the receiver's default TTL instead of
/// the sandbox's real budget turns one dropped write into a permanently
/// short-lived record, which is exactly the failure the budget exists to avoid.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SandboxRosterEntry {
    pub sandbox_id: SandboxId,
    pub execution_id: ExecutionId,
    /// 🔴 `0` means "use the receiver's default", never "never expires".
    pub projection_ttl_secs: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SandboxLifecycleEventType {
    Create,
    Delete,
    Pause,
    Resume,
    Fork,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SandboxLifecycleEvent {
    pub event_type: SandboxLifecycleEventType,
    pub sandbox_id: SandboxId,
    /// The incarnation this event belongs to.
    ///
    /// 🔴 Not an `Option`. Every one of the five publish points holds the
    /// incarnation on the line that sends the event, so an absent value here
    /// could only ever mean "somebody forgot", and a delete guarded by a value
    /// that means that is not guarded at all. `ExecutionId` is a `Uuid`
    /// newtype, so this stays `Copy`.
    pub execution_id: ExecutionId,
    pub resources: SandboxResources,
}

#[derive(Clone, Debug, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SandboxState {
    Creating,
    Resuming,
    Running,
    Snapshotting,
    Forking,
    Pausing,
    Paused,
    Killing,
}

impl SandboxState {
    /// Whether a sandbox in this state is spending its lifetime budget.
    ///
    /// 🔴 Everything but `Paused`, and the asymmetry is the point. A paused
    /// sandbox is a row and a set of layers on disk: no VM, no vCPU, no memory,
    /// no network slot. Every other state — the transitional ones included —
    /// has a machine attached to it. So the ceiling bounds how long a sandbox
    /// may *run*, and wall-clock time spent paused does not count against it.
    ///
    /// The alternative, charging paused time, is what the first cut of the
    /// ceiling did by deriving the deadline from `created_at` alone, and it
    /// made a sandbox resumed a day after creation resume successfully and then
    /// be evicted within the second — because its deadline was already in the
    /// past. e2b, which this whole stage follows, cannot even ask the question:
    /// it has no paused state (`sandboxtypes/states.go` is running / pausing /
    /// killing / snapshotting), and a paused sandbox there leaves the active
    /// store entirely to become a catalog row, so its `MaxInstanceLength` can
    /// only ever bound running time. Charging paused time here would be a
    /// product change smuggled into an infrastructure stage.
    pub fn spends_lifetime(self) -> bool {
        !matches!(self, SandboxState::Paused)
    }
}

impl Display for SandboxState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            SandboxState::Creating => "creating",
            SandboxState::Resuming => "resuming",
            SandboxState::Running => "running",
            SandboxState::Snapshotting => "snapshotting",
            SandboxState::Forking => "forking",
            SandboxState::Pausing => "pausing",
            SandboxState::Paused => "paused",
            SandboxState::Killing => "killing",
        };
        write!(f, "{s}")
    }
}

#[derive(Debug)]
pub struct SnapshotCaptureResult {
    pub metadata: super::store::SandboxMetadata,
    pub captured_snapshot: crate::sandbox::CapturedSandboxSnapshot,
}

/// What a completed pause leaves for the caller to act on.
///
/// The sandbox is already paused, persisted and stopped by the time this is
/// returned; `publishable` is the same capture in a form a snapshot repository
/// can commit, offered so the caller can make the paused sandbox resumable
/// beyond this node. It is `None` when the backend captured into managed
/// temporaries, and callers that only care about the pause itself may drop it.
#[derive(Debug)]
pub struct PauseOutcome {
    pub metadata: super::store::SandboxMetadata,
    pub publishable: Option<crate::sandbox::CapturedSandboxSnapshot>,
}
