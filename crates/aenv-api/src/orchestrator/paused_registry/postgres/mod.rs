//! PostgreSQL paused-sandbox registry.
//! CRUD and lease operations are safe on every replica through row locks and
//! generation/execution fencing. Reconcile and reclaim alone are singleton
//! tasks; heartbeat-derived renewal runs independently on each replica.
//! Restart-grace state is persisted so both singleton leaders observe it.

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

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use sqlx::PgPool;
use uuid::Uuid;

use crate::cfg::{PausedRegistryBackendKind, PausedRegistryConfig};
use crate::identity::NodeIdentity;
use crate::node_registry::registry::NodeRegistry;
use crate::pg::SingletonTaskHandle;
use crate::snapshot::SnapshotId;
use crate::types::{ExecutionId, SandboxId};

use super::{
    BeganPause, DeadlineRenewalOutcome, HeldSandbox, MarkRunningOutcome, PausedRegistryRows,
    PausedSandboxEntry, PausedSandboxRegistry, PostgresPausedRegistryFactory, ReclaimedHoldings,
    RegistryResult, ReleasedHoldings, ResumeClaim,
};

/// PostgreSQL paused-registry factory over a shared pool.
pub struct PgPausedRegistryFactory {
    pool: PgPool,
}

impl PgPausedRegistryFactory {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait(?Send)]
impl PostgresPausedRegistryFactory for PgPausedRegistryFactory {
    async fn build(
        &self,
        identity: &NodeIdentity,
        lease_ttl: Duration,
    ) -> anyhow::Result<Arc<dyn PausedSandboxRegistry>> {
        build_registry(self.pool.clone(), identity.cluster_id, lease_ttl).await
    }
}

async fn build_registry(
    pool: PgPool,
    cluster_id: Uuid,
    lease_ttl: Duration,
) -> anyhow::Result<Arc<dyn PausedSandboxRegistry>> {
    use anyhow::Context as _;

    schema::migrate(&pool)
        .await
        .context("bootstrap the paused_sandboxes schema")?;

    // Best-effort grace entry narrows the window before the reconcile loop starts.
    attempt_initial_grace_entry(&pool, cluster_id, lease_ttl.as_secs_f64()).await;

    Ok(Arc::new(PostgresPausedSandboxRegistry::new(
        pool, cluster_id, lease_ttl,
    )))
}

/// PostgreSQL-backed [`PausedSandboxRegistry`] sharing the process pool.
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

    async fn get_many(&self, sandbox_ids: &[SandboxId]) -> RegistryResult<PausedRegistryRows> {
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

/// Background tasks grouped by shutdown mechanism.
pub struct BackgroundTasks {
    /// Advisory-lock singleton tasks requiring async shutdown.
    pub singleton: Vec<SingletonTaskHandle>,
    /// Per-replica tasks safe to abort.
    pub plain: Vec<tokio::task::JoinHandle<()>>,
}

/// Starts reconcile and reclaim singletons plus optional per-replica renewal.
///
/// Renewal is omitted when no heartbeat roster source is available.
pub fn spawn_background_tasks(
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

/// Attempts the synchronous best-effort restart-grace entry.
pub async fn attempt_initial_grace_entry(pool: &PgPool, cluster_id: Uuid, ttl_secs: f64) {
    grace::attempt_initial_entry(pool, cluster_id, ttl_secs).await
}

/// Background tasks grouped by shutdown mechanism.
#[derive(Default)]
pub struct PausedRegistryBackgroundTasks {
    pub singleton: Vec<SingletonTaskHandle>,
    pub plain: Vec<tokio::task::JoinHandle<()>>,
}

/// Starts PostgreSQL paused-registry tasks only when that backend is selected.
///
/// The matching registry must already have been built successfully.
pub fn spawn_paused_registry_background_tasks(
    config: &PausedRegistryConfig,
    identity: &NodeIdentity,
    pg_pool: Option<sqlx::PgPool>,
    node_registry: Option<Arc<dyn NodeRegistry>>,
) -> PausedRegistryBackgroundTasks {
    if config.backend != PausedRegistryBackendKind::Postgres {
        return PausedRegistryBackgroundTasks::default();
    }
    let Some(pool) = pg_pool else {
        return PausedRegistryBackgroundTasks::default();
    };

    let lease_ttl = std::time::Duration::from_secs(config.lease_ttl_secs());
    let tasks = spawn_background_tasks(
        pool,
        identity.cluster_id,
        lease_ttl,
        config.reconcile_interval(),
        config.reclaim_interval(),
        node_registry,
    );
    PausedRegistryBackgroundTasks {
        singleton: tasks.singleton,
        plain: tasks.plain,
    }
}
