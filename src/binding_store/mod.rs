//! Task's own "D1"/"D3": the routing/binding store — ports
//! `services/scheduler/internal/store.go` (`BindingStore`,
//! `InMemoryBindingStore`) and `services/scheduler/internal/redis_store.go`
//! (`RedisBindingStore`).
//!
//! # What this is, and what it deliberately is not
//!
//! A binding maps a sandbox id to the node currently answering for it, so
//! gateway can proxy data-plane traffic without asking `api`/scheduler on
//! every request (`GATEWAY_ROUTING_PROJECTION_READ=on`, reading the same
//! Redis key space this store's [`redis`] backend writes). It is
//! deliberately **not** folded into `src/orchestrator/store/redis`
//! (`RedisMetadataStore`, api's own sandbox-metadata CAS store) even though
//! both may end up pointed at the same Redis: that store's failure model is
//! "control plane down blocks sandbox operations"; this one's whole reason
//! to exist is "control plane down does not block a gateway proxying an
//! already-running sandbox" (阶段 1's core deliverable). Mixing the two
//! key spaces would couple their lifetimes for no benefit — see
//! `src/orchestrator/store/redis/config.rs`'s own collision guard, which
//! refuses a `key_prefix` that overlaps [`record::DEFAULT_KEY_PREFIX`].
//!
//! # The four operations, and the guard that makes `Delete` safe
//!
//! [`BindingStore::get`]/[`BindingStore::record`]/
//! [`BindingStore::reconcile_node`] mirror Go one to one. [`BindingStore::delete`]
//! is the one the interface comment (`store.go:14-30`) calls "the guard is
//! the whole point": a late PAUSE/DELETE event for a sandbox that has since
//! been resumed elsewhere under a new incarnation must not tear down the
//! live record — it is refused ([`BindingDeleteOutcome::RejectedStale`])
//! rather than applied. Both backends implement this guard; `contract`
//! (test-only) runs the same assertions against both so a fix made to one
//! and forgotten for the other turns red rather than invisible.

pub mod arbitration;
pub mod artifact_index;
pub mod in_memory;
pub mod lookup;
pub mod record;
pub mod redis;
pub mod sweep;

#[cfg(test)]
pub(crate) mod contract;

use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use thiserror::Error;

use crate::node_registry::types::{Node, RosterEntry};

pub use arbitration::{ArbitrationMode, BindingDecision};
pub use in_memory::InMemoryBindingStore;
pub use redis::{RedisBindingStore, RedisBindingStoreConfig};

/// A sandbox's current binding: the node answering for it, and (when known)
/// the execution incarnation that installed it. Mirrors Go's `Binding`
/// (`store.go:60-84`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Binding {
    pub node: Node,
    /// Lowercase canonical UUIDv7, or empty when not known — see
    /// [`record::normalize_execution_id_reason`].
    pub execution_id: String,
    /// `Duration::ZERO` means "use the store's own `binding_ttl`", never
    /// "forever". Only meaningful on a write ([`BindingStore::record`]);
    /// [`BindingStore::get`] does not return it back — the record carries
    /// its own expiry, not a re-exposed TTL (matches Go's `Get`, which
    /// returns only `Node`/`ExecutionID`).
    pub projection_ttl: Duration,
}

/// Ports Go's `BindingDeleteOutcome` (`store.go:32-58`) — the closed set of
/// answers [`BindingStore::delete`] may give, and the value both
/// `agentenv_api_sandbox_event_total{outcome}` (task's own "D1") and
/// `agentenv_api_binding_sweep_total{outcome}` (task's own "D4") report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindingDeleteOutcome {
    /// Nothing held the sandbox (already absent, or expired).
    Absent,
    /// The record named the incarnation the caller supplied; removed.
    Deleted,
    /// The record named no incarnation (an old writer, or a heartbeat-only
    /// arbitration-off record); removed anyway — deliberately asymmetric
    /// with the write path's "unknown never displaces known" rule, because
    /// an event that supplies a *known* incarnation is stronger evidence
    /// than a record that never named one.
    DeletedUnknownIncumbent,
    /// The record named a *different* incarnation than the caller supplied
    /// — refused. This is the guard: a late event for a superseded
    /// incarnation must not tear down the live record.
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

/// A store-level failure — Redis unreachable, a malformed record, and the
/// like. Every [`BindingStore`] caller maps this to `Unavailable`, never to
/// "not found": a caller that cannot tell "this sandbox has no binding"
/// apart from "the store could not be asked" must always assume the latter
/// — see `lookup.rs`'s own module doc for why this distinction is the
/// entire point of the three-stage lookup ladder.
#[derive(Debug, Error)]
#[error("binding store unavailable: {0}")]
pub struct BindingStoreError(pub String);

impl BindingStoreError {
    pub fn new(msg: impl Into<String>) -> Self {
        Self(msg.into())
    }
}

/// The behavior every binding store backend must provide. Mirrors Go's
/// `BindingStore` interface (`store.go:14-30`).
#[async_trait]
pub trait BindingStore: Send + Sync {
    /// The sandbox's current binding, or `None` when absent or expired.
    async fn get(
        &self,
        sandbox_id: &str,
        now: SystemTime,
    ) -> Result<Option<Binding>, BindingStoreError>;

    /// Installs (or refreshes) a binding under this store's configured
    /// arbitration rule. The returned [`BindingDecision`] is for metrics —
    /// a rejected challenger leaves the existing record untouched, which is
    /// not itself an error.
    async fn record(
        &self,
        sandbox_id: &str,
        binding: Binding,
        now: SystemTime,
    ) -> Result<BindingDecision, BindingStoreError>;

    /// Reconciles every binding this store has recorded for `node` against
    /// its freshly reported `roster`: entries not in the new roster are
    /// removed (an empty roster removes everything the node owns), entries
    /// in it are recorded through the same arbitration `record` uses. One
    /// decision per roster entry, in the same order.
    async fn reconcile_node(
        &self,
        node: Node,
        roster: Vec<RosterEntry>,
        now: SystemTime,
    ) -> Result<Vec<(String, BindingDecision)>, BindingStoreError>;

    /// Removes a sandbox's binding, but only if the record still names the
    /// incarnation `execution_id` supplies — see the module doc's "the
    /// guard is the whole point". `execution_id` must already be
    /// normalized and non-empty; an empty one is the caller's mistake to
    /// refuse before calling this (mirrors Go: `applyProjectionDelete`
    /// refuses an empty execution id before ever reaching `store.Delete`).
    async fn delete(
        &self,
        sandbox_id: &str,
        execution_id: &str,
        now: SystemTime,
    ) -> Result<BindingDeleteOutcome, BindingStoreError>;
}

/// Shared tuning both backends read the same way. Mirrors the constructor
/// arguments Go threads through `NewInMemoryBindingStoreWithModes`/
/// `NewRedisBindingStoreWithModes`.
#[derive(Debug, Clone)]
pub struct BindingStoreSettings {
    /// The TTL a binding gets when the caller does not supply its own
    /// (`Binding::projection_ttl <= Duration::ZERO`).
    pub binding_ttl: Duration,
    pub arbitration: ArbitrationMode,
    /// Mirrors Go's `projectionAuthoritative`: whether a heartbeat refresh
    /// of the *same* incarnation may keep the record's existing deadline
    /// (`KEEPTTL`) instead of always re-arming it. `false` means every
    /// write always sets a fresh deadline — the safe default before a
    /// deployment's `SCHEDULER_ROUTING_PROJECTION_AUTHORITATIVE` switch is
    /// turned on.
    pub projection_authoritative: bool,
}

impl Default for BindingStoreSettings {
    fn default() -> Self {
        Self {
            binding_ttl: Duration::from_secs(30),
            arbitration: ArbitrationMode::Fenced,
            projection_authoritative: false,
        }
    }
}
