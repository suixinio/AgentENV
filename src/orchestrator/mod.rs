mod facade;
mod launch_plan;

mod metrics;
mod paused_registry;
mod persistence;
mod proxy;
mod service;
mod store;
mod types;

use std::time::SystemTime;

use crate::types::SandboxId;
use crate::virtualization::VirtualizationMode;

pub use facade::SandboxOrchestration;
pub use launch_plan::ClaimedExecution;
pub use metrics::OrchestratorMetrics;
pub use paused_registry::{
    build_paused_registry, BeganPause, ConflictReason, DisabledPausedSandboxRegistry, HeldSandbox,
    MarkRunningOutcome, PausedRegistryError, PausedRegistryState, PausedSandboxEntry,
    PausedSandboxPublisher, PausedSandboxRegistry, ReclaimedHoldings, RegistryResult,
    ReleasedHoldings, ResumeClaim,
};
pub use persistence::{
    ClusterRegistration, DisabledSandboxPersister, FileBackedSandboxPersister, PersistenceResult,
    SandboxPersistenceError, SandboxPersister,
};
#[cfg(test)]
pub(crate) use persistence::{RecordingCall, RecordingPersister};
pub use proxy::{ProxyLookupResult, ProxyTarget};
pub use service::Orchestrator;
pub use store::{
    configured_max_sandbox_lifetime, is_allowed_transition, ActiveStateRecord, ControlPlaneConfig,
    InMemoryMetadataStore, MetadataRows, MetadataStore, MetadataUpdateResult, NewTimeout,
    PausedHandle, PausedStateRef, RedisMetadataStore, RedisStoreConfig, RedisStoreConfigError,
    Reservation, ReservationGuard, SandboxListFilter, SandboxMetadata, SandboxTimeoutAction,
    StartWaiter, StoreError, StoredSandboxRecord, TransitionEffect, TransitionGuard,
    TransitionOutcome, TransitionRequest, TransitionSettlement, WaitForStart,
    DEFAULT_STORE_KEY_PREFIX, STORE_RECORD_VERSION,
};
pub use types::{
    CreateSandboxRequest, ForkChildAssignment, ForkChildren, LiveSandbox, PauseOutcome,
    SandboxLaunchSource, SandboxLifecycleEvent, SandboxLifecycleEventType, SandboxRosterEntry,
    SandboxState, SnapshotCaptureResult,
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

    /// The sandbox is already past the lifetime ceiling it was created under,
    /// so there is no window left to extend into.
    ///
    /// 🔴 This is the *only* thing about the ceiling that refuses a request. A
    /// keep-alive asking for more time than the ceiling leaves is clamped, not
    /// rejected — see `SandboxMetadata::_set_timeout`.
    #[error("sandbox {sandbox_id} has exceeded its maximum lifetime")]
    SandboxLifetimeExceeded {
        sandbox_id: SandboxId,
        deadline: SystemTime,
    },

    /// The caller asked for something that cannot be built, decided before any
    /// of it is attempted.
    ///
    /// 🔴 Distinct from [`OrchestratorError::InternalError`] on purpose: this
    /// one is the caller's fault and is answered with a 400, so a request that
    /// pairs the wrong number of things together is refused rather than
    /// reported as a fault in the node.
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
