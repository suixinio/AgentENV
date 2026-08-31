mod in_memory;
mod metadata;
pub mod redis;
mod transitions;

#[cfg(test)]
pub mod contract;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;

use crate::orchestrator::SandboxState;
use crate::types::{ExecutionId, SandboxId};

pub use in_memory::InMemoryMetadataStore;
pub use metadata::{
    configured_max_sandbox_lifetime, ControlPlaneConfig, NewTimeout, SandboxMetadata,
    SandboxTimeoutAction,
};
pub use redis::{
    ActiveStateRecord, PausedStateRef, RedisMetadataStore, RedisStoreConfig, RedisStoreConfigError,
    StoredSandboxRecord, DEFAULT_KEY_PREFIX as DEFAULT_STORE_KEY_PREFIX,
    RECORD_VERSION as STORE_RECORD_VERSION,
};
pub use transitions::{is_allowed_transition, state_from_token, state_token, TransitionEffect};

pub type Result<T> = std::result::Result<T, StoreError>;

#[derive(Debug)]
pub struct MetadataUpdateResult {
    pub previous: SandboxMetadata,
    pub current: SandboxMetadata,
}

/// Result of removing a record under an execution and state fence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FencedRemoval {
    /// The record was still the one this incarnation wrote, and is gone.
    Removed,
    /// There was no record under this id.
    Absent,
    /// A different incarnation or later state owns the record.
    Superseded {
        state: SandboxState,
        execution_id: ExecutionId,
    },
}

#[derive(thiserror::Error, Debug)]
pub enum StoreError {
    #[error("store backend error: {source}")]
    Backend {
        #[source]
        source: anyhow::Error,
    },
    #[error("sandbox {sandbox_id} not found")]
    SandboxNotFound { sandbox_id: SandboxId },
    #[error("sandbox {sandbox_id} already exists")]
    SandboxAlreadyExists { sandbox_id: SandboxId },
    #[error(
        "sandbox {sandbox_id} state conflict: expected one of {expected_states:?}, got {actual_state:?}"
    )]
    StateConflict {
        sandbox_id: SandboxId,
        expected_states: Vec<SandboxState>,
        actual_state: SandboxState,
    },
    /// The stored record belongs to a superseding execution; never retry this write.
    #[error(
        "sandbox {sandbox_id} incarnation superseded: wrote for {expected}, store holds {actual:?}"
    )]
    ExecutionSuperseded {
        sandbox_id: SandboxId,
        expected: ExecutionId,
        actual: Option<ExecutionId>,
    },
    /// The compare-and-set at the end of a read-modify-write lost.
    #[error("sandbox {sandbox_id} was modified concurrently")]
    ConcurrentUpdate { sandbox_id: SandboxId },
    /// A synchronous callback exceeded its budget and was not written.
    #[error("sandbox {sandbox_id} update callback exceeded its budget after {elapsed:?}")]
    ClosureBudgetExceeded {
        sandbox_id: SandboxId,
        elapsed: Duration,
    },
    /// The lock lacked enough remaining lifetime to attempt the write.
    #[error("sandbox {sandbox_id} lock lapsed after being held for {held_for:?}")]
    LockLapsed {
        sandbox_id: SandboxId,
        held_for: Duration,
    },
    /// Another replica owns an incompatible transition.
    #[error("sandbox {sandbox_id} already has a transition in flight towards {target}")]
    TransitionInProgress {
        sandbox_id: SandboxId,
        target: SandboxState,
    },
    /// The requested state-machine edge does not exist.
    #[error("sandbox {sandbox_id} cannot transition from {from} to {to}")]
    InvalidTransition {
        sandbox_id: SandboxId,
        from: SandboxState,
        to: SandboxState,
    },
    /// This backend cannot provide the requested cluster primitive.
    #[error("{method} is not supported by this metadata store")]
    UnsupportedByBackend { method: &'static str },
}

/// The filter criteria for listing sandboxes in the metadata store.
///
/// The returned sandboxes must match `states` (if specified) and include `user_metadata` (if specified),
/// but must not match `excluded_states` (if specified).
#[derive(Clone, Debug, Default)]
pub struct SandboxListFilter {
    pub states: Option<Vec<SandboxState>>,
    pub excluded_states: Option<Vec<SandboxState>>,
    pub user_metadata: Option<HashMap<String, String>>,
}

/// Batched records plus the ids authoritatively covered by the read.
/// Destructive callers may act on absence only after verifying full coverage.
#[derive(Debug, Default)]
pub struct MetadataRows {
    pub entries: HashMap<SandboxId, SandboxMetadata>,
    pub covered: Vec<SandboxId>,
}

impl MetadataRows {
    /// Whether this batch covered every id it was asked about.
    pub fn covers(&self, ids: &[SandboxId]) -> bool {
        self.covered.len() == ids.len()
    }
}

/// Request to start a fenced state transition.
#[derive(Clone, Debug)]
pub struct TransitionRequest {
    /// State entered while the transition runs.
    pub target_state: SandboxState,
    /// States accepted as its source.
    pub expected_states: Vec<SandboxState>,
    /// Optional incarnation predicate evaluated atomically by the store.
    pub expected_execution_id: Option<ExecutionId>,
    /// Settlement applied when the operation completes.
    pub effect: TransitionEffect,
    /// Whether expiry must be revalidated atomically with the transition.
    pub eviction: bool,
}

impl TransitionRequest {
    pub fn new(target_state: SandboxState, expected_states: Vec<SandboxState>) -> Self {
        Self {
            target_state,
            expected_states,
            expected_execution_id: None,
            effect: TransitionEffect::Transient,
            eviction: false,
        }
    }

    pub fn with_execution(mut self, execution_id: ExecutionId) -> Self {
        self.expected_execution_id = Some(execution_id);
        self
    }

    pub fn with_effect(mut self, effect: TransitionEffect) -> Self {
        self.effect = effect;
        self
    }

    pub fn as_eviction(mut self) -> Self {
        self.eviction = true;
        self
    }
}

/// Store-side transition completion contract.
#[async_trait]
pub trait TransitionCompleter: Send + Sync {
    async fn complete(
        &self,
        transition_id: &str,
        outcome: std::result::Result<(), String>,
    ) -> Result<()>;
}

/// Owned transition that must be completed.
pub struct TransitionGuard {
    sandbox_id: SandboxId,
    transition_id: String,
    target_state: SandboxState,
    completer: Option<Arc<dyn TransitionCompleter>>,
}

impl std::fmt::Debug for TransitionGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransitionGuard")
            .field("sandbox_id", &self.sandbox_id)
            .field("transition_id", &self.transition_id)
            .field("target_state", &self.target_state)
            .finish_non_exhaustive()
    }
}

impl TransitionGuard {
    pub fn new(
        sandbox_id: SandboxId,
        transition_id: String,
        target_state: SandboxState,
        completer: Arc<dyn TransitionCompleter>,
    ) -> Self {
        Self {
            sandbox_id,
            transition_id,
            target_state,
            completer: Some(completer),
        }
    }

    pub fn transition_id(&self) -> &str {
        &self.transition_id
    }

    pub fn target_state(&self) -> SandboxState {
        self.target_state
    }

    /// Settles the transition and releases its key.
    pub async fn complete(mut self, outcome: std::result::Result<(), String>) -> Result<()> {
        let Some(completer) = self.completer.take() else {
            return Ok(());
        };
        completer.complete(&self.transition_id, outcome).await
    }
}

impl Drop for TransitionGuard {
    fn drop(&mut self) {
        let Some(completer) = self.completer.take() else {
            return;
        };
        tracing::warn!(
            sandbox_id = %self.sandbox_id,
            transition_id = %self.transition_id,
            target_state = %self.target_state,
            "transition guard dropped without completing; marking it failed best-effort"
        );
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            // The transition TTL is the fallback when no runtime is available.
            return;
        };
        let transition_id = self.transition_id.clone();
        handle.spawn(async move {
            let _ = completer
                .complete(&transition_id, Err("transition guard dropped".to_string()))
                .await;
        });
    }
}

/// Source of paused runtime state: local handle, remote reference, or confirmed absence.
pub enum PausedHandle {
    /// Process-local paused-state handle.
    Local(Arc<dyn crate::sandbox::PausedSandboxState>),
    /// Remote paused-state reference and the node whose path it names.
    Remote {
        reference: PausedStateRef,
        /// Node whose local path `reference` names.
        origin_node_id: Option<String>,
    },
    /// Confirmed absence of paused state.
    NotPaused,
}

impl std::fmt::Debug for PausedHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PausedHandle::Local(_) => f.write_str("PausedHandle::Local(..)"),
            PausedHandle::Remote { origin_node_id, .. } => f
                .debug_struct("PausedHandle::Remote")
                .field("origin_node_id", origin_node_id)
                .finish_non_exhaustive(),
            PausedHandle::NotPaused => f.write_str("PausedHandle::NotPaused"),
        }
    }
}

/// Settlement observed by a transition joiner.
#[derive(Debug, PartialEq, Eq)]
pub enum TransitionSettlement {
    /// Still running.
    Running,
    /// Finished, carrying whatever the owner left. `Ok(())` means success.
    Settled(std::result::Result<(), String>),
    /// The transition key expired with no result behind it.
    OwnerVanished,
}

/// Outcome of trying to start a transition.
#[derive(Debug)]
pub enum TransitionOutcome {
    /// The caller owns the transition and must finish the guard.
    Started(TransitionGuard),
    /// Another replica owns the named in-flight transition.
    InFlight { transition_id: String },
    /// An eviction found the sandbox no longer expired.
    NotExpired,
}

/// Outcome of reserving a sandbox id for creation.
#[derive(Debug)]
pub enum Reservation {
    /// This caller owns the creation window and must finish the guard.
    Reserved(ReservationGuard),
    /// The sandbox already exists.
    AlreadyInStorage,
    /// Another creator owns the pending window.
    AlreadyPending(WaitForStart),
    /// Reserved for a future tenant quota model and unreachable today.
    LimitExceeded { subject: String, limit: u64 },
}

/// Store-side reservation settlement contract.
#[async_trait]
pub trait ReservationFinisher: Send + Sync {
    async fn finish(
        &self,
        sandbox_id: &SandboxId,
        outcome: std::result::Result<(), String>,
    ) -> Result<()>;
}

/// Owned creation reservation that must be finished.
pub struct ReservationGuard {
    sandbox_id: SandboxId,
    finisher: Option<Arc<dyn ReservationFinisher>>,
}

impl std::fmt::Debug for ReservationGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReservationGuard")
            .field("sandbox_id", &self.sandbox_id)
            .finish_non_exhaustive()
    }
}

impl ReservationGuard {
    pub fn new(sandbox_id: SandboxId, finisher: Arc<dyn ReservationFinisher>) -> Self {
        Self {
            sandbox_id,
            finisher: Some(finisher),
        }
    }

    pub async fn finish(mut self, outcome: std::result::Result<(), String>) -> Result<()> {
        let Some(finisher) = self.finisher.take() else {
            return Ok(());
        };
        finisher.finish(&self.sandbox_id, outcome).await
    }
}

impl Drop for ReservationGuard {
    fn drop(&mut self) {
        let Some(finisher) = self.finisher.take() else {
            return;
        };
        tracing::warn!(
            sandbox_id = %self.sandbox_id,
            "reservation guard dropped without finishing; releasing it best-effort"
        );
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let sandbox_id = self.sandbox_id;
        handle.spawn(async move {
            let _ = finisher
                .finish(&sandbox_id, Err("reservation guard dropped".to_string()))
                .await;
        });
    }
}

/// Waits for the owner of a creation window to publish its result.
#[async_trait]
pub trait StartWaiter: Send + Sync {
    async fn wait(&self, sandbox_id: &SandboxId) -> Result<SandboxMetadata>;
}

pub struct WaitForStart {
    sandbox_id: SandboxId,
    waiter: Arc<dyn StartWaiter>,
}

impl std::fmt::Debug for WaitForStart {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WaitForStart")
            .field("sandbox_id", &self.sandbox_id)
            .finish_non_exhaustive()
    }
}

impl WaitForStart {
    pub fn new(sandbox_id: SandboxId, waiter: Arc<dyn StartWaiter>) -> Self {
        Self { sandbox_id, waiter }
    }

    /// Waits without imposing a caller deadline.
    pub async fn wait(self) -> Result<SandboxMetadata> {
        self.waiter.wait(&self.sandbox_id).await
    }
}

#[async_trait]
pub trait MetadataStore: Send + Sync {
    async fn add(&self, metadata: SandboxMetadata) -> Result<()>;
    async fn update(&self, metadata: SandboxMetadata) -> Result<()>;
    /// Updates the sandbox state to `new_state` if the current state is one of `expected_states`.
    /// Returns the PREVIOUS sandbox state on success.
    async fn update_state_if_state(
        &self,
        sandbox_id: &SandboxId,
        new_state: SandboxState,
        expected_states: &[SandboxState],
    ) -> Result<SandboxState>;
    /// Atomically updates sandbox metadata using the latest stored value if the
    /// current state is one of `expected_states`.
    ///
    /// The update callback is synchronous by design: store implementations may
    /// run it while holding their metadata lock, so callers must not perform
    /// async work inside the callback.
    /// Runs the synchronous callback exactly once; implementations may fail but never retry it.
    async fn update_if_state<F>(
        &self,
        sandbox_id: &SandboxId,
        expected_states: &[SandboxState],
        update: F,
    ) -> Result<MetadataUpdateResult>
    where
        F: FnOnce(&mut SandboxMetadata) + Send;
    async fn get(&self, sandbox_id: &SandboxId) -> Result<Option<SandboxMetadata>>;
    async fn remove(&self, sandbox_id: &SandboxId) -> Result<Option<SandboxMetadata>>;
    /// Removes only the expected execution in one of the expected states.
    /// Absence is idempotent and returns [`FencedRemoval::Absent`].
    async fn remove_if_execution(
        &self,
        sandbox_id: &SandboxId,
        expected_execution_id: ExecutionId,
        expected_states: &[SandboxState],
    ) -> Result<FencedRemoval>;
    async fn list(&self) -> Result<Vec<SandboxMetadata>>;
    async fn list_with_callback<F>(&self, callback: F) -> Result<()>
    where
        F: FnMut(&SandboxMetadata) + Send;
    async fn list_filtered(&self, filter: SandboxListFilter) -> Result<Vec<SandboxMetadata>>;
    async fn list_expired(&self, now: SystemTime) -> Result<Vec<SandboxMetadata>>;
    async fn list_ids(&self) -> Result<Vec<SandboxId>>;

    /// Waits until the sandbox state is no longer any of the given `transitional_states`,
    /// then returns the latest metadata.
    ///
    /// Returns `Ok(None)` if the sandbox was removed while waiting.
    /// Returns immediately (without blocking) if the sandbox is already in a non-transitional state.
    async fn wait_while_in_states(
        &self,
        sandbox_id: &SandboxId,
        transitional_states: &[SandboxState],
    ) -> Result<Option<SandboxMetadata>>;

    /// Returns at most `limit` expired sandboxes.
    async fn expired_batch(&self, now: SystemTime, limit: usize) -> Result<Vec<SandboxMetadata>> {
        let mut expired = self.list_expired(now).await?;
        expired.truncate(limit);
        Ok(expired)
    }

    /// Reads records and reports every id authoritatively covered.
    async fn get_many(&self, ids: &[SandboxId]) -> Result<MetadataRows> {
        // An empty request covers nothing.
        if ids.is_empty() {
            return Ok(MetadataRows::default());
        }
        let mut entries = HashMap::with_capacity(ids.len());
        for id in ids {
            if let Some(metadata) = self.get(id).await? {
                entries.insert(*id, metadata);
            }
        }
        Ok(MetadataRows {
            entries,
            covered: ids.to_vec(),
        })
    }

    /// Claims a state transition.
    async fn start_transition(
        &self,
        _sandbox_id: &SandboxId,
        _request: TransitionRequest,
    ) -> Result<TransitionOutcome> {
        Err(StoreError::UnsupportedByBackend {
            method: "start_transition",
        })
    }

    /// Returns local, remote, or absent paused runtime state.
    async fn paused_handle(&self, sandbox_id: &SandboxId) -> Result<PausedHandle> {
        let metadata = self
            .get(sandbox_id)
            .await?
            .ok_or(StoreError::SandboxNotFound {
                sandbox_id: *sandbox_id,
            })?;
        Ok(match metadata.paused_state {
            Some(handle) => PausedHandle::Local(handle),
            None => PausedHandle::NotPaused,
        })
    }

    /// Reads the outcome of an in-flight transition.
    async fn transition_settlement(
        &self,
        _sandbox_id: &SandboxId,
        _transition_id: &str,
    ) -> Result<TransitionSettlement> {
        Err(StoreError::UnsupportedByBackend {
            method: "transition_settlement",
        })
    }

    /// Claims a sandbox id for creation.
    async fn reserve(&self, _sandbox_id: &SandboxId) -> Result<Reservation> {
        Err(StoreError::UnsupportedByBackend { method: "reserve" })
    }

    /// Repairs missing expiry-index entries; in-lock stores return zero.
    async fn heal_expiry_index(&self) -> Result<usize> {
        Ok(0)
    }

    /// Makes ownerless transitions recoverable; process-local stores return none.
    async fn reap_stuck_transitions(&self, _now: SystemTime) -> Result<Vec<SandboxId>> {
        Ok(Vec::new())
    }
}
