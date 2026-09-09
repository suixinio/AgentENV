mod facade;
pub mod grants;
mod launch_claim;
pub mod launch_parts;
mod launch_plan;

mod metrics;
mod pause_publisher;
mod proxy;
mod runtime_routing;
mod service;
pub mod store;
mod types;

use std::time::SystemTime;

use crate::types::SandboxId;
use crate::virtualization::VirtualizationMode;

pub use facade::{NodeOrchestration, SandboxOrchestration};
pub use grants::{GrantIssuer, GrantsIssuedUpstream, NoGrants};
pub use launch_claim::{LaunchFailure, LaunchHeldElsewhere, LaunchSettlement, RestoredSandbox};
pub use launch_parts::{
    configured_runtime_versions, default_fresh_sandbox_resources, resources_with_runtime_info,
    snapshot_create_parts, SnapshotCreateInputs, SnapshotCreateParts,
};
pub use metrics::OrchestratorMetrics;
#[cfg(any(test, feature = "test-support"))]
pub use pause_publisher::DiscardingPausePublisher;
pub use pause_publisher::{CommittingPausePublisher, PausePublisher, StagingPausePublisher};
pub use proxy::{ProxyLookupResult, ProxyTarget};
pub use runtime_routing::RuntimeRouting;
pub use service::Orchestrator;
pub use store::{
    configured_max_sandbox_lifetime, is_allowed_transition, ActiveStateRecord, ControlPlaneConfig,
    FencedRemoval, InMemoryMetadataStore, MetadataRows, MetadataStore, MetadataUpdateResult,
    NewTimeout, RedisMetadataStore, RedisStoreConfig, RedisStoreConfigError, SandboxListFilter,
    SandboxMetadata, SandboxTimeoutAction, StoreError, StoredSandboxRecord, TransitionEffect,
    TransitionGuard, TransitionOutcome, TransitionRequest, TransitionSettlement,
    DEFAULT_STORE_KEY_PREFIX, STORE_RECORD_VERSION,
};
pub use types::{
    capture_publish_metadata, CreateSandboxRequest, ForkChildAssignment, ForkChildren, LiveSandbox,
    PauseOutcome, PublishedPause, SandboxExpiry, SandboxLaunchSource, SandboxLifecycleEvent,
    SandboxLifecycleEventType, SandboxRosterEntry, SandboxState, SnapshotCaptureResult,
};

pub type Result<T> = std::result::Result<T, OrchestratorError>;
pub type SandboxForkOutcome = std::result::Result<SandboxMetadata, OrchestratorError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxOperation {
    Build,
    Start,
    WaitReady,
    Pause,
    Resume,
    Snapshot,
    Fork,
    UpdateNetwork,
    PatchCustomExtensionParams,
    Stop,
}

#[derive(thiserror::Error, Debug)]
pub enum OrchestratorError {
    #[error("failed to load sandbox config")]
    ConfigLoadFailed(#[from] anyhow::Error),

    #[error(
        "{resource} uses virtualization mode '{resource_mode}', but this node runs in mode '{node_mode}'"
    )]
    VirtualizationModeMismatch {
        resource: String,
        resource_mode: VirtualizationMode,
        node_mode: VirtualizationMode,
    },

    #[error("orchestrator is shutting down")]
    ShuttingDown,

    #[error("node is isolated and is not taking new sandboxes")]
    NotAcceptingNewWork,

    #[error("sandbox {0} not found")]
    SandboxNotFound(SandboxId),

    /// Another launch under the same sandbox id is still running in this
    /// process. Nothing was built for the refused one.
    #[error("sandbox {sandbox_id} is already being launched by this process")]
    LaunchInFlight { sandbox_id: SandboxId },

    #[error("sandbox {sandbox_id} is in invalid state {state:?}")]
    InvalidSandboxState {
        sandbox_id: SandboxId,
        state: SandboxState,
    },

    #[error("sandbox {sandbox_id} operation {operation:?} failed: {source}")]
    SandboxOperationFailed {
        sandbox_id: SandboxId,
        operation: SandboxOperation,
        #[source]
        source: anyhow::Error,
    },

    #[error("sandbox {sandbox_id} operation {operation:?} conflicted with another operation")]
    SandboxOperationConflict {
        sandbox_id: SandboxId,
        operation: SandboxOperation,
    },

    #[error("store operation failed: {0}")]
    StoreOperationFailed(#[source] store::StoreError),

    /// The pause captured the sandbox but nothing durable came of it; the
    /// sandbox was put back to running.
    #[error("sandbox {sandbox_id} could not be published after pausing: {source}")]
    PausePublicationFailed {
        sandbox_id: SandboxId,
        #[source]
        source: anyhow::Error,
    },

    #[error("invalid timeout for {sandbox_id}: {timeout}")]
    InvalidTimeout {
        sandbox_id: SandboxId,
        timeout: String,
    },

    /// The sandbox has no remaining lifetime budget.
    #[error("sandbox {sandbox_id} has exceeded its maximum lifetime")]
    SandboxLifetimeExceeded {
        sandbox_id: SandboxId,
        deadline: SystemTime,
    },

    /// The caller requested an operation that cannot be constructed.
    #[error("invalid request: {0}")]
    InvalidRequest(String),

    #[error("internal error: {0}")]
    InternalError(String),
}

impl From<store::StoreError> for OrchestratorError {
    fn from(value: store::StoreError) -> Self {
        match value {
            store::StoreError::SandboxNotFound { sandbox_id } => {
                OrchestratorError::SandboxNotFound(sandbox_id)
            }
            other => OrchestratorError::StoreOperationFailed(other),
        }
    }
}

impl From<OrchestratorError> for String {
    fn from(err: OrchestratorError) -> Self {
        err.to_string()
    }
}
