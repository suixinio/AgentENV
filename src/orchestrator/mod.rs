mod facade;
mod launch_plan;

mod metrics;
pub mod paused_registry;
mod persistence;
mod proxy;
mod service;
pub mod store;
mod types;

use std::time::SystemTime;

use crate::types::SandboxId;
use crate::virtualization::VirtualizationMode;

pub use facade::SandboxOrchestration;
pub use launch_plan::ClaimedExecution;
pub use metrics::OrchestratorMetrics;
pub use paused_registry::{
    build_paused_registry, BeganPause, ConflictReason, DeadlineRenewalOutcome,
    DisabledPausedSandboxRegistry, HeldSandbox, MarkRunningOutcome, PausedRegistryError,
    PausedRegistryListEntry, PausedRegistryListing, PausedRegistryRows, PausedRegistryState,
    PausedSandboxEntry, PausedSandboxPublisher, PausedSandboxRegistry,
    PostgresPausedRegistryFactory, ReclaimedHoldings, RegistryResult, ReleasedHoldings,
    ResumeClaim,
};
pub use persistence::{
    ClusterRegistration, DisabledSandboxPersister, FileBackedSandboxPersister, PersistenceResult,
    SandboxPersistenceError, SandboxPersister,
};
#[cfg(any(test, feature = "test-support"))]
pub use persistence::{RecordingCall, RecordingPersister};
pub use proxy::{ProxyLookupResult, ProxyTarget};
pub use service::Orchestrator;
pub use store::{
    configured_max_sandbox_lifetime, is_allowed_transition, ActiveStateRecord, ControlPlaneConfig,
    FencedRemoval, InMemoryMetadataStore, MetadataRows, MetadataStore, MetadataUpdateResult,
    NewTimeout, PausedHandle, PausedStateRef, RedisMetadataStore, RedisStoreConfig,
    RedisStoreConfigError, Reservation, ReservationGuard, SandboxListFilter, SandboxMetadata,
    SandboxTimeoutAction, StartWaiter, StoreError, StoredSandboxRecord, TransitionEffect,
    TransitionGuard, TransitionOutcome, TransitionRequest, TransitionSettlement, WaitForStart,
    DEFAULT_STORE_KEY_PREFIX, STORE_RECORD_VERSION,
};
pub use types::{
    capture_publish_metadata, CreateSandboxRequest, ForkChildAssignment, ForkChildren, LiveSandbox,
    PauseOutcome, PausePublication, SandboxExpiry, SandboxLaunchSource, SandboxLifecycleEvent,
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

    #[error("sandbox persistence failed: {0}")]
    SandboxPersistenceFailed(#[from] SandboxPersistenceError),

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

impl OrchestratorError {
    /// Whether the capture a resume needed is not on the node that was supposed
    /// to hold it, found either locally or in a node's answer.
    ///
    /// Callers holding a claim on a published row may rebuild from the
    /// repository instead of surfacing this.
    pub fn is_paused_capture_absent(&self) -> bool {
        let source = match self {
            Self::SandboxPersistenceFailed(SandboxPersistenceError::RecordAbsent { .. }) => {
                return true
            }
            Self::SandboxOperationFailed { source, .. } | Self::ConfigLoadFailed(source) => source,
            _ => return false,
        };

        crate::node_client::wire::capture_absent(source)
    }
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
