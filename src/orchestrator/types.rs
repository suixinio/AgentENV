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
