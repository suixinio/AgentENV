//! `PostgresPausedSandboxRegistry`: the paused registry backend that
//! connects directly to PostgreSQL from `--role api`, folding
//! `services/scheduler/internal/registry/` (4,191 lines) plus the
//! heartbeat-lease-renewal half of `internal/reconcile.go` into this
//! process. Stage C of the phase-4 scheduler fold
//! (`docs/proposals/_sd-phase4-stageC-paused-registry.md`).
//!
//! # Module map
//!
//! - [`schema`][]: `paused_sandboxes` DDL bootstrap (`migrate.go`).
//! - [`sql`][]: every SQL statement, ported verbatim from `store_postgres.go`.
//! - [`row`][]: row decoding shared by every reader.
//! - [`reads`][]: `get`/`get_many` (trait) + the internal full-column reads
//!   [`reconcile`]/[`reclaim`] use.
//! - [`writes`][]: the fencing write path (`begin_pause` through `remove`).
//! - [`lease`][]: `renew_lease` (trait) + the two heartbeat-driven,
//!   internal-only siblings Fix A/Fix B need.
//! - [`reclaim`][]: `reclaim_expired_holdings`/`release_node_holdings` (trait)
//!   + the `DiscardBreaker`.
//! - [`grace`][]: the restart-grace redesign for N replicas -- **read this
//!   module's doc before touching [`reconcile`] or [`reclaim_task`]**.
//! - [`reconcile`][]: the reconcile leader loop (D4's monitoring fix + grace
//!   entry). D2 Fix A no longer lives here -- see [`replica_renewal`] and
//!   [`reconcile`]'s own module doc's "B1" section for why.
//! - [`replica_renewal`][]: **B1**'s per-replica, unelected Fix A -- read
//!   this module's doc for why Fix A cannot be leader-elected under N
//!   `--role api` replicas.
//! - [`reclaim_task`][]: the reclaim leader loop.
//!
//! # D1: which parts of this backend need leader election, and why only
//! these two
//!
//! Every method in [`writes`]/[`reads`]/[`lease`] (the whole
//! [`PausedSandboxRegistry`] trait impl below) is safe to call from every
//! `--role api` replica concurrently, unelected -- each is a single
//! generation/execution_id-CAS'd statement, and PostgreSQL's own row locking
//! serialises the rest. So is [`replica_renewal`] (B1) -- see its own module
//! doc. See the Stage C report's D1 section for the per-statement review
//! this claim rests on.
//!
//! [`spawn_background_tasks`] starts the two things that genuinely do need
//! cluster-wide leadership: the reconcile loop
//! (`AdvisoryLockKey::PausedRegistryReconcile`, [`reconcile`]) and the
//! reclaim loop (`AdvisoryLockKey::PausedRegistryReclaim`,
//! [`reclaim_task`]). Neither requires a heartbeat roster source any more
//! (M1) -- [`replica_renewal`]'s per-replica loop is spawned alongside them
//! only when [`crate::node_registry::registry::NodeRegistry`] is available,
//! and is simply skipped (not required) when it is not, which is exactly
//! `--role all`'s own shape: see [`spawn_background_tasks`]'s own doc.

#[cfg(test)]
mod contract;
mod grace;
mod lease;
mod reads;
mod reclaim;
mod reclaim_task;
mod reconcile;
mod replica_renewal;
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

    async fn list_all(&self) -> RegistryResult<super::PausedRegistryListing> {
        reads::list_all(self).await
    }

    fn is_cluster_backed(&self) -> bool {
        true
    }
}

/// This backend's background tasks, split by whether shutdown has to
/// release an advisory lock ([`SingletonTaskHandle::shutdown`], `async`) or
/// can simply be aborted (a plain [`tokio::task::JoinHandle`]) -- see
/// [`spawn_background_tasks`]'s own doc for which is which and why.
pub(super) struct BackgroundTasks {
    /// The reconcile and reclaim leader loops. Belongs in the same
    /// `pg_singleton_tasks` bucket every other PostgreSQL-elected background
    /// task in this process shuts down through
    /// (`src/bin/server.rs::Assembly::pg_singleton_tasks`).
    pub(super) singleton: Vec<SingletonTaskHandle>,
    /// B1's per-replica renewal loop, present only when a
    /// [`NodeRegistry`] was supplied. Belongs in `Assembly::upkeep`
    /// alongside `spawn_paused_record_upkeep`'s own tasks -- see
    /// [`replica_renewal::spawn`]'s own doc for why a plain abort is safe
    /// here.
    pub(super) plain: Vec<tokio::task::JoinHandle<()>>,
}

/// Starts this backend's background tasks: the reconcile leader loop, the
/// reclaim leader loop, and (M1) B1's per-replica renewal loop when a
/// heartbeat roster source is available.
///
/// # M1: `node_registry` is optional
///
/// D2 Fix A (now [`replica_renewal`]) only does anything under a real
/// [`NodeRegistry::rosters_in_cluster`] answer -- without one, there is
/// nothing for it to renew from, so [`replica_renewal::spawn`] is simply not
/// started. This is **not** the same gap Fix A originally closed: under the
/// split node/api identity model (`--role api`, `[cluster]
/// .node_placement_source = "scheduler"`, the default), a missing roster
/// really would leave `running` rows with no renewal path at all, and
/// `crate::orchestrator::paused_registry::build_paused_registry` still
/// refuses to select this backend in that configuration for exactly that
/// reason (see that function's own doc). The case this function *does* have
/// to accept a missing roster for is `--role all`, which never builds a
/// [`crate::node_registry::registry::AtomicNodeRegistry`] at all: there,
/// this process's own identity coincides with `origin_node_id` for
/// everything it runs, so the ordinary `renew_lease` trait method (driven by
/// `spawn_paused_record_upkeep` in `src/bin/server.rs`) already renews those
/// rows under matching identity -- `--role all` never needed Fix A in the
/// first place. See [`replica_renewal`]'s own module doc for the full
/// argument.
///
/// The reconcile and reclaim leader loops are started unconditionally
/// either way: grace entry ([`grace::enter`]) and D4's metrics
/// ([`reconcile::compute_reconcile`]) are both roster-independent, and a
/// cluster running `--role all` still needs restart-grace protection against
/// the same fleet-wide-coverage-gap scenario a `--role api` deployment does
/// (see [`grace`]'s own module doc).
pub(super) fn spawn_background_tasks(
    pool: PgPool,
    cluster_id: Uuid,
    lease_ttl: Duration,
    reconcile_interval: Duration,
    reclaim_interval: Duration,
    node_registry: Option<Arc<dyn NodeRegistry>>,
) -> BackgroundTasks {
    let mut singleton = Vec::with_capacity(2);

    let registry_for_reconcile = Arc::new(PostgresPausedSandboxRegistry::new(
        pool.clone(),
        cluster_id,
        lease_ttl,
    ));
    singleton.push(reconcile::spawn(
        pool.clone(),
        registry_for_reconcile,
        cluster_id,
        lease_ttl,
        reconcile_interval,
    ));

    singleton.push(reclaim_task::spawn(
        pool.clone(),
        cluster_id,
        reclaim_interval,
    ));

    let plain = match node_registry {
        Some(node_registry) => {
            let registry_for_renewal = Arc::new(PostgresPausedSandboxRegistry::new(
                pool, cluster_id, lease_ttl,
            ));
            vec![replica_renewal::spawn(registry_for_renewal, node_registry)]
        }
        None => Vec::new(),
    };

    BackgroundTasks { singleton, plain }
}

/// B2(1): delegates to [`grace::attempt_initial_entry`] -- see that
/// function's own doc. Exposed at this module's boundary so
/// `crate::orchestrator::paused_registry::build_paused_registry` (the
/// parent module) can call it without reaching into [`grace`] directly.
pub(super) async fn attempt_initial_grace_entry(pool: &PgPool, cluster_id: Uuid, ttl_secs: f64) {
    grace::attempt_initial_entry(pool, cluster_id, ttl_secs).await
}
