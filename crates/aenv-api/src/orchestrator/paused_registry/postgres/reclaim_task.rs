//! Reclaim leader loop, elected independently from reconciliation.
//! Every pass reads persisted restart-grace state before reclaiming.

use std::time::Duration;

use sqlx::PgPool;
use tracing::{debug, warn};
use uuid::Uuid;

use super::reclaim::{reclaim_expired_holdings, DiscardBreaker};

const TICK_BUDGET: Duration = Duration::from_secs(25);

/// Starts delayed reclaim ticks; a zero interval disables the loop.
pub fn spawn(pool: PgPool, cluster_id: Uuid, interval: Duration) -> crate::pg::SingletonTaskHandle {
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
