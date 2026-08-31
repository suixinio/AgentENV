//! Routing projection from sandbox id to serving node.
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
pub use redis::{RedisBindingStore, RedisBindingStoreConfig};

/// A sandbox's serving node and, when known, execution incarnation.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Binding {
    pub node: Node,
    /// Canonical UUIDv7, or empty when unknown.
    pub execution_id: String,
    /// `ZERO` selects the store's configured binding TTL.
    pub projection_ttl: Duration,
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
}

impl BindingDeleteOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            BindingDeleteOutcome::Absent => "noop_absent",
            BindingDeleteOutcome::Deleted => "deleted",
            BindingDeleteOutcome::DeletedUnknownIncumbent => "deleted_unknown_incumbent",
            BindingDeleteOutcome::RejectedStale => "rejected_stale",
        }
    }
}

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
