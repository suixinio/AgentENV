use std::collections::HashMap;
use std::fmt::Display;
use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::sandbox::CustomExtensionParams;
use crate::snapshot::repository::interfaces::StagedSnapshot;
use crate::snapshot::{CommandContext, PausedSandboxConfig, SnapshotId};
use crate::types::{ExecutionId, ImageConfigs, SandboxId, SandboxResources};

#[derive(Clone)]
pub enum SandboxLaunchSource {
    Snapshot(Box<crate::runtime_snapshot::RunnableSnapshot>),
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
    /// OCI image reference resolved by the node-side backend factory.
    UnresolvedImage {
        image_ref: String,
        resources: crate::types::SandboxResources,
        attached_drives: Vec<crate::sandbox::UnresolvedAttachedDrive>,
        extra_boot_args: Option<String>,
    },
    /// Committed snapshot record resolved into local bytes by the node-side factory.
    SnapshotRecord(Box<crate::snapshot::SnapshotRecord>),
}

/// Identifies who owns sandbox expiry and which timeout applies.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SandboxExpiry {
    /// Explicit timeout owned by this orchestrator.
    After(Duration),
    /// Client omitted timeout, so use the configured default.
    AfterConfiguredDefault,
    /// Deadline is owned elsewhere; this orchestrator must not evict it.
    NotKeptHere,
}

#[derive(Clone)]
pub struct CreateSandboxRequest {
    pub source: SandboxLaunchSource,
    /// Explicit three-state expiry ownership.
    pub expiry: SandboxExpiry,
    pub timeout_action: super::SandboxTimeoutAction,
    pub auto_resume: bool,
    pub user_metadata: Option<HashMap<String, String>>,
    pub env_vars: Option<HashMap<String, String>>,
    pub network_policy: crate::sandbox::SandboxNetworkPolicy,
    pub secure: bool,
    /// Opaque user-provided JSON passed through to the custom extension hooks.
    pub custom_extension_params: Option<CustomExtensionParams>,
    /// Opaque control-plane ownership marker, supplied only by the control plane.
    pub control_plane_config: Option<crate::orchestrator::ControlPlaneConfig>,
    /// Pre-minted incarnation for delegated creates; user creates leave it absent.
    pub execution_id: Option<ExecutionId>,
    /// Node the placement should favour, typically the one that paused the
    /// sandbox this request restores. A preference only: an unschedulable
    /// node yields to ordinary placement.
    pub preferred_node_id: Option<String>,
}

/// One sandbox present in the node's live handle table.
/// Record-derived fields remain optional so orphaned handles are still reported.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LiveSandbox {
    pub sandbox_id: SandboxId,
    /// Live incarnation from the handle, falling back to the record.
    pub execution_id: Option<ExecutionId>,
    /// Whether runtime facts came from the handle rather than its record.
    pub facts_from_handle: bool,
    pub host_interaction_ip: Option<std::net::Ipv4Addr>,
    pub rootfs_virtual_size: Option<u64>,
    pub created_at: Option<std::time::SystemTime>,
    pub expires_at: Option<std::time::SystemTime>,
    pub resources: Option<SandboxResources>,
    /// Ownership marker reported without filtering the node-wide listing.
    pub control_plane_config: Option<crate::orchestrator::ControlPlaneConfig>,
}

/// Fork children either minted locally or assigned by the caller.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ForkChildren {
    /// Mints `count` unowned child identities locally.
    Fresh(u32),
    /// Caller-assigned children in result order.
    Assigned(Vec<ForkChildAssignment>),
}

/// Caller-assigned fork child.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ForkChildAssignment {
    /// Child identity chosen by the caller.
    pub sandbox_id: SandboxId,
    /// Pre-minted incarnation, or `None` to mint locally.
    pub execution_id: Option<ExecutionId>,
    /// The control plane's ownership marker for this child, or `None`.
    pub control_plane_config: Option<crate::orchestrator::ControlPlaneConfig>,
}

impl ForkChildren {
    /// Number of requested children.
    pub fn count(&self) -> u32 {
        match self {
            Self::Fresh(count) => *count,
            // API limits keep this conversion below `u32::MAX`.
            Self::Assigned(children) => children.len().try_into().unwrap_or(u32::MAX),
        }
    }
}

/// Heartbeat roster entry carrying incarnation and routing-projection TTL.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SandboxRosterEntry {
    pub sandbox_id: SandboxId,
    pub execution_id: ExecutionId,
    /// `0` selects the receiver's default TTL.
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
    /// Required incarnation fence for this lifecycle event.
    pub execution_id: ExecutionId,
    pub resources: SandboxResources,
}

/// States of a sandbox record. A record exists only while a VM is running or
/// being brought up or down; a paused sandbox is a snapshot catalog row and
/// has no record here.
#[derive(Clone, Debug, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SandboxState {
    Creating,
    Running,
    Snapshotting,
    Forking,
    Pausing,
    Killing,
}

impl Display for SandboxState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            SandboxState::Creating => "creating",
            SandboxState::Running => "running",
            SandboxState::Snapshotting => "snapshotting",
            SandboxState::Forking => "forking",
            SandboxState::Pausing => "pausing",
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

/// Builds snapshot publication metadata from authoritative runtime metadata.
///
/// A pause capture carries the sandbox's own configuration so a later resume
/// can rebuild the sandbox from the row alone; a checkpoint carries none.
pub fn capture_publish_metadata(
    metadata: &super::store::SandboxMetadata,
    alias: Option<crate::snapshot::SnapshotAlias>,
    paused_sandbox: Option<PausedSandboxConfig>,
) -> crate::snapshot::SnapshotPublishMetadata {
    crate::snapshot::SnapshotPublishMetadata {
        id: crate::snapshot::SnapshotId::generate(),
        alias,
        source: crate::snapshot::SnapshotPublishSource::Sandbox {
            source_sandbox_id: metadata.id.to_string(),
        },
        context: metadata.context.clone(),
        startup: metadata.startup.clone(),
        resources: metadata.resources,
        runtime_versions: metadata.runtime_versions.clone(),
        virtualization_mode: metadata.virtualization_mode,
        image_configs: metadata.image_configs.clone(),
        custom_extension_params: metadata.custom_extension_params.clone(),
        paused_sandbox,
    }
}

/// What a pause left behind once the sandbox's record was gone.
#[derive(Debug)]
pub enum PublishedPause {
    /// Bytes durable on this node, row not yet announced. The caller commits it.
    Staged(Box<StagedSnapshot>),
    /// Catalog row committed; the sandbox is resumable anywhere.
    Committed(SnapshotId),
}

/// A completed pause.
#[derive(Debug)]
pub struct PauseOutcome {
    /// The record as it stood when the VM stopped.
    pub metadata: super::store::SandboxMetadata,
    /// `None` when this call joined a pause another caller was already
    /// performing: the publication belongs to that caller.
    pub published: Option<PublishedPause>,
}

impl PauseOutcome {
    /// A pause completed by somebody else.
    pub fn joined(metadata: super::store::SandboxMetadata) -> Self {
        Self {
            metadata,
            published: None,
        }
    }
}
