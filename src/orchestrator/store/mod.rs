mod in_memory;
mod metadata;
pub mod redis;
mod transitions;

#[cfg(test)]
pub(crate) mod contract;

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
pub use transitions::{is_allowed_transition, state_token, TransitionEffect};

pub type Result<T> = std::result::Result<T, StoreError>;

#[derive(Debug)]
pub struct MetadataUpdateResult {
    pub previous: SandboxMetadata,
    pub current: SandboxMetadata,
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
    /// The record in the store belongs to a different run of the sandbox than
    /// the caller was writing for.
    ///
    /// 🔴 Distinct from [`StoreError::ConcurrentUpdate`] on purpose. That one
    /// means "somebody else changed this record while you were thinking"; this
    /// one means "the machine you were writing about has been replaced", and
    /// the operation must not be retried against the new incarnation.
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
    /// The synchronous update callback ran for longer than its budget, so the
    /// result was thrown away rather than written.
    ///
    /// 🔴 Terminal, never retried: `update_if_state` takes `FnOnce`, and at
    /// least one caller's callback writes a variable outside itself
    /// (`keep_alive`'s `timeout_updated`), so running it twice would corrupt
    /// the caller's own bookkeeping even if the type system allowed it.
    #[error("sandbox {sandbox_id} update callback exceeded its budget after {elapsed:?}")]
    ClosureBudgetExceeded {
        sandbox_id: SandboxId,
        elapsed: Duration,
    },
    /// The distributed lock's remaining lifetime was too short to cover the
    /// write, so the write was abandoned before it was attempted.
    #[error("sandbox {sandbox_id} lock lapsed after being held for {held_for:?}")]
    LockLapsed {
        sandbox_id: SandboxId,
        held_for: Duration,
    },
    /// Another replica is already running a transition on this sandbox, and it
    /// is not one this caller can wait for.
    #[error("sandbox {sandbox_id} already has a transition in flight towards {target}")]
    TransitionInProgress {
        sandbox_id: SandboxId,
        target: SandboxState,
    },
    /// The requested transition is not one the state machine has an edge for.
    ///
    /// 🔴 Not a [`StoreError::StateConflict`]. That means "you were a step too
    /// late"; this means "the thing you asked for does not exist", and a
    /// caller that retries on it will retry forever.
    #[error("sandbox {sandbox_id} cannot transition from {from} to {to}")]
    InvalidTransition {
        sandbox_id: SandboxId,
        from: SandboxState,
        to: SandboxState,
    },
    /// This store does not implement the requested primitive.
    ///
    /// 🔴 An explicit refusal rather than a silent weaker behaviour. The four
    /// cluster primitives only mean anything on a store that several replicas
    /// share; a test double that quietly pretended to run one would be
    /// answering a question it cannot answer.
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

/// The result of a batched read, carrying what the batch actually looked at.
///
/// 🔴 A sandbox missing from `entries` has no record, and the caller acts on
/// that absence by deleting local artifacts and tearing down running VMs. A
/// map that came back short for any other reason — a failed chunk, a truncated
/// response, a partial answer from a store that could not be reached — looks
/// exactly like that answer. `covered` is the guarantee made checkable: it
/// lists every id this call actually asked about, present and absent alike, so
/// a caller can assert `covered.len() == ids.len()` before treating absence as
/// authorisation to destroy anything.
///
/// This mirrors `registry.Rows.Covered` on the Go side
/// (`services/scheduler/internal/registry/store.go`), which
/// `paused_registry::central::require_full_coverage` already asserts against.
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

/// What a caller wants to start doing to a sandbox.
#[derive(Clone, Debug)]
pub struct TransitionRequest {
    /// The transitional (or terminal) state to move into now.
    pub target_state: SandboxState,
    /// States the record may currently be in.
    pub expected_states: Vec<SandboxState>,
    /// The incarnation the caller believes it is operating on, if it has one.
    ///
    /// 🔴 Compared inside the script, never before it. A lockless `add` — which
    /// is what `restore_sandbox` performs — can install a new incarnation
    /// between a check made here and the write that follows it.
    pub expected_execution_id: Option<ExecutionId>,
    /// What completion should settle the record on.
    pub effect: TransitionEffect,
    /// Whether this transition is an eviction, in which case expiry is
    /// re-validated atomically with the state write.
    ///
    /// 🔴 Today's evictor checks expiry, then compare-and-sets on *state*. A
    /// `keep_alive` that lands in between pushes `expires_at` out and the
    /// eviction pauses the sandbox anyway. Setting this makes the expiry part
    /// of the same atomic step.
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

/// How a started transition is finished.
///
/// Implemented by the store; callers only ever see it through
/// [`TransitionGuard`].
#[async_trait]
pub trait TransitionCompleter: Send + Sync {
    async fn complete(
        &self,
        transition_id: &str,
        outcome: std::result::Result<(), String>,
    ) -> Result<()>;
}

/// A transition this caller owns and must finish.
///
/// 🔴 Not `Clone`, and it warns on drop. e2b's equivalent is a bare closure the
/// caller is trusted to invoke; forgetting it there wedges the sandbox until
/// the transition key's TTL expires. This cannot make forgetting harmless —
/// the TTL is still the backstop — but it makes forgetting *visible*, which is
/// the difference between a bug that is found and one that is not.
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

    /// Settles the transition, applying its effect and releasing the key.
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
            // Nothing to spawn onto. The transition key's TTL is the backstop.
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

/// Where a paused sandbox's runtime state can be got from.
///
/// # 🔴 Three answers, because two of them look identical as `None`
///
/// `SandboxMetadata::paused_state` is `#[serde(skip)]`, so any store that
/// serialises a record hands it back empty. Read through `get`, that empty
/// value means two different things — *this sandbox is not paused* and *this
/// store cannot give you handles, the bytes are on another machine* — and a
/// `resume_sandbox` that read it answered the first by failing with "missing
/// paused state" for every sandbox on such a store, which is a 500 describing a
/// state the sandbox is not in.
///
/// 🔴 `Orchestrator::paused_state_for_resume` is the one caller, and it is the
/// only path a resume takes to its capture.
///
/// So the question is asked separately, and the answer has the three states the
/// question has. A store may not answer [`PausedHandle::NotPaused`] for a
/// record that carries a reference.
pub enum PausedHandle {
    /// This store is holding the handle in this process. Pass it to the
    /// backend factory directly.
    Local(Arc<dyn crate::sandbox::PausedSandboxState>),
    /// The bytes are on `origin_node_id`'s disk and the reference decodes
    /// there. An `api` replica forwards this; a node decodes it with its own
    /// factory.
    Remote {
        reference: PausedStateRef,
        /// 🔴 `PausedStateRef::artifact_root` is a path on one particular
        /// machine, so without this a caller holds a path and no idea whose.
        origin_node_id: Option<String>,
    },
    /// The sandbox has no paused state, and that is a fact rather than a
    /// limitation of the store that was asked.
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

/// What a joiner learns about a transition it did not start.
///
/// 🔴 Three answers, not two. "The transition key is gone and no result was
/// left behind" is **not** success — it is the owner having died — and a joiner
/// that reads it as success reports an operation complete that never happened.
/// Absence is not an outcome, and a store that cannot be reached produces an
/// error rather than any of these.
#[derive(Debug, PartialEq, Eq)]
pub enum TransitionSettlement {
    /// Still running.
    Running,
    /// Finished, carrying whatever the owner left. `Ok(())` means success.
    Settled(std::result::Result<(), String>),
    /// The transition key expired with no result behind it.
    OwnerVanished,
}

/// What happened when a caller asked to start a transition.
#[derive(Debug)]
pub enum TransitionOutcome {
    /// The caller owns the transition and must finish the guard.
    Started(TransitionGuard),
    /// Another replica is already running a transition on this sandbox.
    ///
    /// The caller decides whether to wait for it (its target is the same as
    /// theirs), retry after it (a legal edge follows it) or refuse.
    InFlight { transition_id: String },
    /// An eviction found the sandbox no longer expired.
    ///
    /// 🔴 Not an error. A `keep_alive` that lands between the expiry index scan
    /// and the transition is the system working, not failing.
    NotExpired,
}

/// The three answers a reservation can give.
///
/// 🔴 Three, not four. e2b's fourth — `limitExceeded` — counts a tenant's
/// sandboxes against that tenant's quota, and AgentENV has no tenant model:
/// `team_id` appears only in the generated E2B-compatible schema, and
/// `api/impls/auth.rs` describes itself as checking "presence, not validity".
/// There is no subject to count against, so the variant is a documented stub
/// (see [`Reservation::LimitExceeded`]) rather than a branch that can be
/// reached.
#[derive(Debug)]
pub enum Reservation {
    /// This caller owns the creation window and must finish the guard.
    Reserved(ReservationGuard),
    /// The sandbox already exists.
    AlreadyInStorage,
    /// Somebody else is creating it. Wait for their result rather than
    /// returning a conflict — from the caller's point of view the sandbox is
    /// coming up either way.
    AlreadyPending(WaitForStart),
    /// 🔴 Intentionally unreachable in this phase. Reserved so that adding a
    /// tenant model later is a change to the middle of one Lua script, rather
    /// than a change to this enum and to every `match` that names it. Matches
    /// on this variant must say `unreachable!`, never `_ => {}`: when the
    /// tenant model arrives the compiler has to be able to point at every site
    /// that forgot it.
    LimitExceeded { subject: String, limit: u64 },
}

/// How a reservation is settled.
#[async_trait]
pub trait ReservationFinisher: Send + Sync {
    async fn finish(
        &self,
        sandbox_id: &SandboxId,
        outcome: std::result::Result<(), String>,
    ) -> Result<()>;
}

/// A creation window this caller owns and must close.
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

/// Waits for whoever holds the creation window to publish its result.
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

    /// 🔴 Sets no deadline of its own. The store does not know how patient the
    /// caller is; an HTTP handler must give this a budget shorter than the
    /// reverse proxy's timeout, or a stuck creation becomes a hung connection.
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
    ///
    /// 🔴 The callback runs exactly once, and the signature says so. A
    /// distributed implementation may not turn this into an optimistic retry
    /// loop: `FnOnce` forbids it, and `keep_alive`'s callback sets a variable
    /// declared outside itself, which a second run would set a second time.
    /// Implementations that cannot write may only fail, never re-run.
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

    // ---------------------------------------------------------------------
    // Cluster primitives.
    //
    // 🔴 Every one of these has a default so that the four scripted test
    // doubles in `orchestrator/tests.rs` — which assert nothing about any of
    // them — do not each grow six forwarding methods with no assertion value.
    // The defaults are honest: they either compute the answer from the
    // required methods, or they refuse.
    // ---------------------------------------------------------------------

    /// Expired sandboxes, at most `limit` of them.
    ///
    /// The bound is what lets an evictor that runs on N replicas do a bounded
    /// amount of work per round instead of pulling the whole table.
    async fn expired_batch(&self, now: SystemTime, limit: usize) -> Result<Vec<SandboxMetadata>> {
        let mut expired = self.list_expired(now).await?;
        expired.truncate(limit);
        Ok(expired)
    }

    /// Reads several records, reporting which ids the read actually covered.
    ///
    /// The default answers from `get`, one id at a time: correct for any store,
    /// and any error at all propagates rather than shortening the map.
    async fn get_many(&self, ids: &[SandboxId]) -> Result<MetadataRows> {
        // 🔴 An empty batch issues no request. "I looked at nothing" is a fact
        // the caller compares against a request for nothing.
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

    /// Claims the right to run a state transition on this sandbox.
    async fn start_transition(
        &self,
        _sandbox_id: &SandboxId,
        _request: TransitionRequest,
    ) -> Result<TransitionOutcome> {
        Err(StoreError::UnsupportedByBackend {
            method: "start_transition",
        })
    }

    /// Where this sandbox's paused runtime state can be got from.
    ///
    /// The default reads it out of the record, which is the true answer for a
    /// store that keeps handles in process. A store that serialises records
    /// must override it — returning `NotPaused` for a record that has a
    /// reference would be the ambiguity this method exists to remove.
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

    /// Asks how a transition somebody else started has ended, if it has.
    ///
    /// The counterpart to [`TransitionOutcome::InFlight`]: without it a caller
    /// that finds a transition already in flight has been told to wait and
    /// given nothing to wait on.
    async fn transition_settlement(
        &self,
        _sandbox_id: &SandboxId,
        _transition_id: &str,
    ) -> Result<TransitionSettlement> {
        Err(StoreError::UnsupportedByBackend {
            method: "transition_settlement",
        })
    }

    /// Claims the right to create this sandbox id.
    async fn reserve(&self, _sandbox_id: &SandboxId) -> Result<Reservation> {
        Err(StoreError::UnsupportedByBackend { method: "reserve" })
    }

    /// Repairs expiry-index entries that exist as records but not as index
    /// members. Returns how many were repaired.
    ///
    /// The default is `0`, which is the true answer for any store whose index
    /// and records live under one lock: they cannot drift apart.
    async fn heal_expiry_index(&self) -> Result<usize> {
        Ok(0)
    }

    /// Finds transitions whose owner died and pushes their sandboxes towards a
    /// path that can settle them. Returns the sandboxes it acted on.
    ///
    /// The default is empty, which is the true answer for a store whose
    /// transitions cannot outlive the process that started them.
    async fn reap_stuck_transitions(&self, _now: SystemTime) -> Result<Vec<SandboxId>> {
        Ok(Vec::new())
    }
}
