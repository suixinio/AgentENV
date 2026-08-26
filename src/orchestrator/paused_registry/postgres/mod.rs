//! `PostgresPausedSandboxRegistry`: the paused registry backend that
//! connects directly to PostgreSQL from `--role api`, folding
//! `services/scheduler/internal/registry/` (4,191 lines) plus the
//! heartbeat-lease-renewal half of `internal/reconcile.go` into this
//! process. Stage C of the phase-4 scheduler fold
//! (`docs/proposals/_sd-phase4-stageC-paused-registry.md`).
//!
//! # Module map
//!
//! - [`schema`]: `paused_sandboxes` DDL bootstrap (`migrate.go`).
//! - [`sql`]: every SQL statement, ported verbatim from `store_postgres.go`.
//! - [`row`]: row decoding shared by every reader.
//! - [`reads`]: `get`/`get_many` (trait) + the internal full-column reads
//!   [`reconcile`]/[`reclaim`] use.
//! - [`writes`]: the fencing write path (`begin_pause` through `remove`).
//! - [`lease`]: `renew_lease` (trait) + the two heartbeat-driven,
//!   internal-only siblings Fix A/Fix B need.
//! - [`reclaim`]: `reclaim_expired_holdings`/`release_node_holdings` (trait)
//!   + the `DiscardBreaker`.
//! - [`grace`]: the restart-grace redesign for N replicas -- **read this
//!   module's doc before touching [`reconcile`] or [`reclaim_task`]**.
//! - [`reconcile`]: the reconcile leader loop (D2 Fix A's home) + D4's
//!   monitoring fix.
//! - [`reclaim_task`]: the reclaim leader loop.
//!
//! # D1: which parts of this backend need leader election, and why only
//! these two
//!
//! Every method in [`writes`]/[`reads`]/[`lease`] (the whole
//! [`PausedSandboxRegistry`] trait impl below) is safe to call from every
//! `--role api` replica concurrently, unelected -- each is a single
//! generation/execution_id-CAS'd statement, and PostgreSQL's own row locking
//! serialises the rest. See the Stage C report's D1 section for the
//! per-statement review this claim rests on.
//!
//! [`spawn_background_tasks`] is the only place this module starts anything
//! that needs leadership: the reconcile loop
//! (`AdvisoryLockKey::PausedRegistryReconcile`, [`reconcile`]) and the
//! reclaim loop (`AdvisoryLockKey::PausedRegistryReclaim`,
//! [`reclaim_task`]). Both require a heartbeat roster source
//! ([`crate::node_registry::registry::NodeRegistry`]) to do anything useful
//! with Fix A -- see [`spawn_background_tasks`]'s own doc on why this
//! backend refuses to start without one.

mod grace;
mod lease;
mod reads;
mod reclaim;
mod reclaim_task;
mod reconcile;
mod row;
pub mod schema;
mod sql;
mod writes;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use sqlx::PgPool;
use uuid::Uuid;

use crate::node_registry::registry::NodeRegistry;
use crate::pg::SingletonTaskHandle;
use crate::snapshot::SnapshotId;
use crate::types::{ExecutionId, SandboxId};

use super::{
    BeganPause, DeadlineRenewalOutcome, HeldSandbox, MarkRunningOutcome, PausedSandboxEntry,
    PausedSandboxRegistry, ReclaimedHoldings, RegistryResult, ReleasedHoldings, ResumeClaim,
};

/// A direct PostgreSQL-backed [`PausedSandboxRegistry`]. One shared `[pg]`
/// pool (built once per `--role api`/`--role all` process by
/// `src/bin/server.rs::build_pg_pool`, the same pool Stage B's catalog
/// backend uses) covers both the per-request CRUD paths in this struct's
/// trait impl and the two background leader tasks
/// [`spawn_background_tasks`] starts.
pub struct PostgresPausedSandboxRegistry {
    pool: PgPool,
    cluster_id: Uuid,
    lease_ttl: Duration,
}

impl PostgresPausedSandboxRegistry {
    pub fn new(pool: PgPool, cluster_id: Uuid, lease_ttl: Duration) -> Self {
        Self {
            pool,
            cluster_id,
            lease_ttl,
        }
    }

    fn lease_ttl_secs(&self) -> f64 {
        self.lease_ttl.as_secs_f64()
    }
}

#[async_trait]
impl PausedSandboxRegistry for PostgresPausedSandboxRegistry {
    async fn begin_pause(&self, entry: &PausedSandboxEntry) -> RegistryResult<BeganPause> {
        writes::begin_pause(self, entry).await
    }

    async fn complete_pause(
        &self,
        sandbox_id: &SandboxId,
        generation: i64,
        snapshot_id: &SnapshotId,
    ) -> RegistryResult<()> {
        writes::complete_pause(self, sandbox_id, generation, snapshot_id).await
    }

    async fn mark_local_only(&self, sandbox_id: &SandboxId, generation: i64) -> RegistryResult<()> {
        writes::mark_local_only(self, sandbox_id, generation).await
    }

    async fn get(&self, sandbox_id: &SandboxId) -> RegistryResult<Option<PausedSandboxEntry>> {
        reads::get(self, sandbox_id).await
    }

    async fn get_many(
        &self,
        sandbox_ids: &[SandboxId],
    ) -> RegistryResult<HashMap<SandboxId, PausedSandboxEntry>> {
        reads::get_many(self, sandbox_ids).await
    }

    async fn claim_for_resume(
        &self,
        sandbox_id: &SandboxId,
        node_id: &str,
        execution_id: ExecutionId,
    ) -> RegistryResult<ResumeClaim> {
        let durable_only = !grace::is_serving(&self.pool, self.cluster_id)
            .await
            .map_err(|e| super::PausedRegistryError::backend("claim_for_resume", e))?;
        writes::claim_for_resume(self, durable_only, sandbox_id, node_id, execution_id).await
    }

    async fn release_claim(&self, sandbox_id: &SandboxId, generation: i64) -> RegistryResult<bool> {
        writes::release_claim(self, sandbox_id, generation).await
    }

    async fn renew_lease(&self, node_id: &str, held: &[HeldSandbox]) -> RegistryResult<u64> {
        lease::renew_lease(self, node_id, held).await
    }

    async fn reclaim_expired_holdings(&self) -> RegistryResult<ReclaimedHoldings> {
        reclaim::reclaim_expired_holdings(
            &self.pool,
            self.cluster_id,
            reclaim::DiscardBreaker::default(),
        )
        .await
    }

    async fn mark_running(
        &self,
        sandbox_id: &SandboxId,
        node_id: &str,
        holder_node_id: &str,
        execution_id: ExecutionId,
        expires_at: Option<SystemTime>,
    ) -> RegistryResult<MarkRunningOutcome> {
        writes::mark_running(
            self,
            sandbox_id,
            node_id,
            holder_node_id,
            execution_id,
            expires_at.map(chrono::DateTime::<chrono::Utc>::from),
        )
        .await
    }

    async fn renew_sandbox_deadline(
        &self,
        sandbox_id: &SandboxId,
        execution_id: ExecutionId,
        expires_at: Option<SystemTime>,
    ) -> RegistryResult<DeadlineRenewalOutcome> {
        writes::renew_sandbox_deadline(
            self,
            sandbox_id,
            execution_id,
            expires_at.map(chrono::DateTime::<chrono::Utc>::from),
        )
        .await
    }

    async fn release_node_holdings(&self, node_id: &str) -> RegistryResult<ReleasedHoldings> {
        reclaim::release_node_holdings(&self.pool, self.cluster_id, node_id).await
    }

    async fn remove(&self, sandbox_id: &SandboxId, generation: i64) -> RegistryResult<bool> {
        writes::remove(self, sandbox_id, generation).await
    }

    fn is_cluster_backed(&self) -> bool {
        true
    }
}

/// Starts the reconcile and reclaim leader loops.
///
/// # 🔴 Requires a heartbeat roster source -- refuses to be called without one
///
/// D2 Fix A only does anything under a real
/// [`NodeRegistry::rosters_in_cluster`] answer: without it, `running` rows
/// have no path back to a renewed lease at all under the split node/api
/// identity model (`renew_lease`'s own `Running` branch never matches an api
/// replica's identity -- see `lease.rs`'s doc), and this backend would
/// silently reintroduce the exact bug Fix A fixed
/// (`151d00b`, and this repository's own memory note on the "running 行租约
/// 冻结" failure mode). `--role api` only builds a real
/// [`crate::node_registry::registry::AtomicNodeRegistry`] under
/// `[cluster].node_placement_source = "native"`
/// (`src/bin/server.rs::start_native_node_registry`); under the default
/// `"scheduler"`, and always under `--role all` (which never builds one at
/// all), `node_registry` here is `None`.
///
/// This is a **caller** contract, not enforced inside this function: the one
/// call site (`crate::orchestrator::paused_registry::build_paused_registry`)
/// refuses to select the `postgres` backend at all when it cannot supply a
/// roster source, so `spawn_background_tasks` is simply never reached in
/// that configuration. See that function's own doc for the refusal message.
pub(super) fn spawn_background_tasks(
    pool: PgPool,
    cluster_id: Uuid,
    lease_ttl: Duration,
    reconcile_interval: Duration,
    reclaim_interval: Duration,
    node_registry: Arc<dyn NodeRegistry>,
) -> Vec<SingletonTaskHandle> {
    let mut handles = Vec::with_capacity(2);

    let registry_for_reconcile = Arc::new(PostgresPausedSandboxRegistry::new(
        pool.clone(),
        cluster_id,
        lease_ttl,
    ));
    handles.push(reconcile::spawn(
        pool.clone(),
        registry_for_reconcile,
        cluster_id,
        lease_ttl,
        reconcile_interval,
        node_registry,
    ));

    handles.push(reclaim_task::spawn(pool, cluster_id, reclaim_interval));

    handles
}
