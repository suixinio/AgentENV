//! The reclaim leader loop -- a Rust port of `PausedRegistryService.RunReclaim`
//! (`services/scheduler/internal/registry_service.go:919-958`).
//!
//! Runs as its own cluster-wide singleton
//! ([`crate::pg::election::spawn_singleton_task`],
//! `AdvisoryLockKey::PausedRegistryReclaim`) -- **independent** of the
//! reconcile loop's election. See [`super::grace`]'s module doc for why that
//! independence is safe: this loop gates every pass on
//! [`super::grace::is_serving`], a persisted fact any replica can read,
//! rather than on in-memory state only the reconcile leader could see.

use std::time::Duration;

use sqlx::PgPool;
use tracing::{debug, warn};
use uuid::Uuid;

use super::reclaim::{reclaim_expired_holdings, DiscardBreaker};

/// This loop's own tick budget -- see [`super::reconcile::TICK_BUDGET`]'s
/// doc for why `tokio::time::timeout` is safe here specifically because
/// every query in [`reclaim_expired_holdings`] runs against the shared pool
/// (ephemeral per-query connections), never a pinned leader session.
const TICK_BUDGET: Duration = Duration::from_secs(25);

/// Starts the reclaim leader loop. `interval <= Duration::ZERO` disables it
/// outright -- matching Go's own `RunReclaim`, which warns and returns
/// rather than substituting a default (unlike `RunRegistryReconcile`'s
/// 30s fallback): "nothing about it is urgent... every row it collects has
/// been stranded for at least a sandbox lifetime already".
///
/// 🔴 Unlike Go's `RunReclaim`, this loop does **not** fire immediately on
/// start -- it only acts on the first tick after `interval`, exactly
/// mirroring Go's own behaviour (`RunRegistryReconcile` fires-then-ticks;
/// `RunReclaim` only ticks). `spawn_singleton_task`'s own `FIRST_ATTEMPT_DELAY`
/// (100ms) governs when this replica first *contests* leadership, which is
/// a different question from when a newly-elected leader's body first runs
/// -- both loops share that leader-acquisition timing; only the
/// reconcile/reclaim distinction above is Go's own.
pub(super) fn spawn(
    pool: PgPool,
    cluster_id: Uuid,
    interval: Duration,
) -> crate::pg::SingletonTaskHandle {
    crate::pg::spawn_singleton_task(
        pool.clone(),
        crate::pg::AdvisoryLockKey::PausedRegistryReclaim,
        interval,
        move |_ctx: crate::pg::LeaderContext<'_>| {
            let pool = pool.clone();
            Box::pin(async move {
                match tokio::time::timeout(TICK_BUDGET, reclaim_once(&pool, cluster_id)).await {
                    Ok(Ok(())) => {}
                    Ok(Err(err)) => {
                        warn!(target: "agentenv", error = %err, "paused registry reclamation pass failed");
                    }
                    Err(_) => {
                        warn!(
                            target: "agentenv",
                            budget_secs = TICK_BUDGET.as_secs(),
                            "paused registry reclamation pass exceeded its time budget"
                        );
                    }
                }
            })
        },
    )
}

async fn reclaim_once(pool: &PgPool, cluster_id: Uuid) -> anyhow::Result<()> {
    if !super::grace::is_serving(pool, cluster_id).await? {
        debug!(
            target: "agentenv",
            cluster_id = %cluster_id,
            "skipping paused registry reclamation: still inside the restart grace window"
        );
        return Ok(());
    }

    let outcome = reclaim_expired_holdings(pool, cluster_id, DiscardBreaker::default())
        .await
        .map_err(anyhow::Error::from)?;

    if outcome.released > 0 || outcome.discarded > 0 {
        tracing::info!(
            target: "agentenv",
            cluster_id = %cluster_id,
            released = outcome.released,
            discarded = outcome.discarded,
            "paused registry reclamation pass"
        );
    }
    Ok(())
}
