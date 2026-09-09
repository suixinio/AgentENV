//! Routing projection from sandbox id to serving node, and the only answer to
//! which node runs a sandbox: a heartbeat roster reaches routing by being
//! reconciled into this store, never by being read beside it.
//! Its key space and failure model remain separate from the orchestrator metadata store.
//! Deletes are incarnation-fenced, and both backends run the shared contract suite.

pub mod arbitration;
pub mod artifact_index;
/// Test-only in-memory backend; production routing uses Redis.
#[cfg(any(test, feature = "test-support"))]
pub mod in_memory;
pub mod lookup;
pub mod record;
pub mod redis;
pub mod reservation;
pub mod sweep;

#[cfg(test)]
pub mod contract;

use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use thiserror::Error;

use crate::node_registry::types::{Node, RosterEntry};

pub use arbitration::BindingDecision;
#[cfg(any(test, feature = "test-support"))]
pub use in_memory::InMemoryBindingStore;
pub use record::BindingState;
pub use redis::{RedisBindingStore, RedisBindingStoreConfig};
pub use reservation::{LaunchReservationOutcome, ReservationRecord};

/// A sandbox's serving node and, when known, execution incarnation.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Binding {
    pub node: Node,
    /// Canonical UUIDv7, or empty when unknown.
    pub execution_id: String,
    /// `ZERO` selects the store's configured binding TTL.
    pub projection_ttl: Duration,
    /// Defaults to [`BindingState::Confirmed`]; only a create-time reservation
    /// is [`BindingState::Starting`].
    pub state: BindingState,
}

/// Guarded delete outcome and its metrics label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindingDeleteOutcome {
    /// Nothing held the sandbox (already absent, or expired).
    Absent,
    /// The record named the incarnation the caller supplied; removed.
    Deleted,
    /// An event with a known incarnation removed a record with none.
    DeletedUnknownIncumbent,
    /// Refused because the record names another incarnation.
    RejectedStale,
    /// Refused because the record is a confirmation, not the reservation the
    /// caller asked to withdraw.
    RejectedConfirmed,
}

impl BindingDeleteOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            BindingDeleteOutcome::Absent => "noop_absent",
            BindingDeleteOutcome::Deleted => "deleted",
            BindingDeleteOutcome::DeletedUnknownIncumbent => "deleted_unknown_incumbent",
            BindingDeleteOutcome::RejectedStale => "rejected_stale",
            BindingDeleteOutcome::RejectedConfirmed => "rejected_confirmed",
        }
    }
}

/// How long a `Starting` reservation excludes a newer incarnation.
///
/// Inside it a launch is assumed to still be running, and a newer one is
/// refused rather than allowed to supersede it; past it the reservation is read
/// as the residue of a replica that died mid-launch, and a newer launch takes
/// the sandbox over. It must therefore exceed the slowest launch this process
/// will wait out. The Lua `accepts` prelude in `redis/scripts.rs` is handed
/// this same value as `inflight_ttl_ms`; the two must not drift.
pub const LAUNCH_RESERVATION_EXCLUSIVE_TTL: Duration = Duration::from_secs(120);

/// Store failures are never interpreted as absence.
#[derive(Debug, Error)]
#[error("binding store unavailable: {0}")]
pub struct BindingStoreError(pub String);

impl BindingStoreError {
    pub fn new(msg: impl Into<String>) -> Self {
        Self(msg.into())
    }
}

/// Contract implemented by every binding-store backend.
#[async_trait]
pub trait BindingStore: Send + Sync {
    /// The sandbox's current binding, or `None` when absent or expired.
    async fn get(
        &self,
        sandbox_id: &str,
        now: SystemTime,
    ) -> Result<Option<Binding>, BindingStoreError>;

    /// Records a binding; rejected challengers leave the incumbent untouched.
    async fn record(
        &self,
        sandbox_id: &str,
        binding: Binding,
        now: SystemTime,
    ) -> Result<BindingDecision, BindingStoreError>;

    /// Reconciles all bindings owned by `node` against its latest roster.
    async fn reconcile_node(
        &self,
        node: Node,
        roster: Vec<RosterEntry>,
        now: SystemTime,
    ) -> Result<Vec<(String, BindingDecision)>, BindingStoreError>;

    /// Removes a binding only when it still names `execution_id`.
    async fn delete(
        &self,
        sandbox_id: &str,
        execution_id: &str,
        now: SystemTime,
    ) -> Result<BindingDeleteOutcome, BindingStoreError>;

    /// Withdraws a reservation, leaving a confirmed binding of the same
    /// incarnation in place.
    ///
    /// A create whose node acknowledged it between the failure and this call is
    /// already confirmed, and the runtime it names is real.
    async fn release_reservation(
        &self,
        sandbox_id: &str,
        execution_id: &str,
        now: SystemTime,
    ) -> Result<BindingDeleteOutcome, BindingStoreError>;

    /// Takes a sandbox id for a launch before any node is chosen, or names the
    /// launch already holding it.
    ///
    /// This is the cluster's single-activation truth source: the routing record
    /// cannot be, because it must name a node and a launch has none yet.
    async fn reserve_launch(
        &self,
        sandbox_id: &str,
        execution_id: &str,
        now: SystemTime,
    ) -> Result<LaunchReservationOutcome, BindingStoreError>;

    /// Gives a sandbox id back once the launch has settled, either way.
    ///
    /// Fenced on `execution_id`: a reservation another launch took over after
    /// this one's window ran out is left where it is.
    async fn release_launch(
        &self,
        sandbox_id: &str,
        execution_id: &str,
        now: SystemTime,
    ) -> Result<BindingDeleteOutcome, BindingStoreError>;

    /// Removes reservations past their exclusivity window, returning how many.
    ///
    /// A replica that dies mid-launch leaves one behind, and nothing else ever
    /// releases it.
    async fn reap_expired_launches(&self, now: SystemTime) -> Result<u64, BindingStoreError>;
}

/// Tuning shared by all binding-store backends.
#[derive(Debug, Clone)]
pub struct BindingStoreSettings {
    /// Default TTL when a write supplies none.
    pub binding_ttl: Duration,
    /// Whether same-incarnation heartbeat refreshes preserve the current deadline.
    pub projection_authoritative: bool,
}

impl Default for BindingStoreSettings {
    fn default() -> Self {
        Self {
            binding_ttl: Duration::from_secs(30),
            projection_authoritative: false,
        }
    }
}
