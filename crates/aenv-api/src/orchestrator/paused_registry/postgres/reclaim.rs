//! Reclaims expired holdings and releases a dead node's rows.
//! Running and resuming releases stay separate statements in one transaction;
//! the discard breaker rolls back the entire transaction when it trips.

use anyhow::anyhow;
use sqlx::PgPool;
use tracing::{error, warn};
use uuid::Uuid;

use super::sql;
use crate::orchestrator::paused_registry::{PausedRegistryError, RegistryResult, ReleasedHoldings};

fn backend_err(operation: &'static str, err: sqlx::Error) -> PausedRegistryError {
    PausedRegistryError::backend(operation, err)
}

/// Limits destructive reclaim by both row count and ratio.
#[derive(Debug, Clone, Copy)]
pub struct DiscardBreaker {
    pub max_rows: i64,
    pub max_ratio: f64,
    pub min_ratio_rows: i64,
}

impl Default for DiscardBreaker {
    fn default() -> Self {
        Self {
            max_rows: 10,
            max_ratio: 0.10,
            min_ratio_rows: 3,
        }
    }
}

impl DiscardBreaker {
    /// Allows a discard pass only when neither breaker threshold is exceeded.
    pub fn allow(&self, candidates: i64, total: i64) -> Result<(), String> {
        if candidates == 0 {
            return Ok(());
        }

        let max_rows = if self.max_rows > 0 {
            self.max_rows
        } else {
            Self::default().max_rows
        };
        let max_ratio = if self.max_ratio > 0.0 {
            self.max_ratio
        } else {
            Self::default().max_ratio
        };
        let min_ratio_rows = if self.min_ratio_rows > 0 {
            self.min_ratio_rows
        } else {
            Self::default().min_ratio_rows
        };

        let over_count = candidates > max_rows;
        let over_ratio = total > 0
            && candidates > min_ratio_rows
            && (candidates as f64) / (total as f64) > max_ratio;

        if !over_count && !over_ratio {
            return Ok(());
        }

        Err(format!(
            "refusing to reclaim: this pass would discard more rows than anything here can \
             explain, and a discarded row has no snapshot to come back from ({candidates} of \
             {total} rows exceeds the limit of {max_rows}, or {:.0}% above {min_ratio_rows} rows)",
            max_ratio * 100.0
        ))
    }
}

/// Releases expired rows, then discards unrecoverable rows if the breaker allows.
///
/// A tripped breaker rolls back releases and discards together.
pub async fn reclaim_expired_holdings(
    pool: &PgPool,
    cluster_id: Uuid,
    breaker: DiscardBreaker,
) -> RegistryResult<ReleasedHoldings> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| backend_err("reclaim_expired_holdings", e))?;

    let released_running = sqlx::query(sql::RECLAIM_RELEASED_RUNNING_SQL)
        .bind(cluster_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| backend_err("reclaim_expired_holdings", e))?
        .rows_affected();

    let released_resuming = sqlx::query(sql::RECLAIM_RELEASED_RESUMING_SQL)
        .bind(cluster_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| backend_err("reclaim_expired_holdings", e))?
        .rows_affected();

    let released = released_running + released_resuming;

    let candidates: i64 = sqlx::query_scalar(sql::COUNT_RECLAIM_DISCARDABLE_SQL)
        .bind(cluster_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| backend_err("reclaim_expired_holdings", e))?;
    let total: i64 = sqlx::query_scalar(sql::COUNT_CLUSTER_ROWS_SQL)
        .bind(cluster_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| backend_err("reclaim_expired_holdings", e))?;

    if let Err(reason) = breaker.allow(candidates, total) {
        // Dropping the uncommitted transaction rolls back both release updates.
        error!(
            candidates,
            total,
            max_rows = breaker.max_rows,
            max_ratio = breaker.max_ratio,
            min_ratio_rows = breaker.min_ratio_rows,
            "{reason}"
        );
        return Err(PausedRegistryError::backend(
            "reclaim_expired_holdings",
            anyhow!("{reason}"),
        ));
    }

    let discarded = sqlx::query(sql::RECLAIM_DISCARDED_SQL)
        .bind(cluster_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| backend_err("reclaim_expired_holdings", e))?
        .rows_affected();

    tx.commit()
        .await
        .map_err(|e| backend_err("reclaim_expired_holdings", e))?;

    let outcome = ReleasedHoldings {
        released,
        discarded,
    };
    if !outcome.is_empty() {
        warn!(
            cluster_id = %cluster_id,
            released = outcome.released,
            discarded = outcome.discarded,
            "reclaimed sandboxes that outlived their deadline on a node that stopped \
             reporting; released ones resume from their last published snapshot"
        );
    }
    Ok(outcome)
}

/// Releases or discards rows still held by `node_id`.
pub async fn release_node_holdings(
    pool: &PgPool,
    cluster_id: Uuid,
    node_id: &str,
) -> RegistryResult<ReleasedHoldings> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| backend_err("release_node_holdings", e))?;

    let released = sqlx::query(&sql::release_holdings_released_sql())
        .bind(cluster_id)
        .bind(node_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| backend_err("release_node_holdings", e))?
        .rows_affected();

    let discarded = sqlx::query(&sql::release_holdings_discarded_sql())
        .bind(cluster_id)
        .bind(node_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| backend_err("release_node_holdings", e))?
        .rows_affected();

    tx.commit()
        .await
        .map_err(|e| backend_err("release_node_holdings", e))?;

    Ok(ReleasedHoldings {
        released,
        discarded,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_candidates_always_allows() {
        assert!(DiscardBreaker::default().allow(0, 1000).is_ok());
    }

    #[test]
    fn under_both_thresholds_allows() {
        assert!(DiscardBreaker::default().allow(2, 100).is_ok());
    }

    #[test]
    fn over_max_rows_trips_even_with_a_tiny_ratio() {
        let breaker = DiscardBreaker {
            max_rows: 10,
            max_ratio: 0.10,
            min_ratio_rows: 3,
        };
        assert!(breaker.allow(11, 100_000).is_err());
    }

    #[test]
    fn over_ratio_trips_even_under_max_rows() {
        let breaker = DiscardBreaker {
            max_rows: 1000,
            max_ratio: 0.10,
            min_ratio_rows: 3,
        };
        assert!(breaker.allow(20, 100).is_err());
    }

    #[test]
    fn min_ratio_rows_protects_a_small_cluster() {
        let breaker = DiscardBreaker {
            max_rows: 1000,
            max_ratio: 0.10,
            min_ratio_rows: 3,
        };
        assert!(breaker.allow(2, 8).is_ok());
    }
}
