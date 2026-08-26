//! `ReclaimExpiredHoldings` (`store_postgres.go`'s
//! `reclaimExpiredHoldings` + `DiscardBreaker`, `grace.go:411-513`) and
//! `ReleaseNodeHoldings` (`releaseHoldingsReleasedSQL`/
//! `releaseHoldingsDiscardedSQL`, `store_postgres.go:1798-1814`).
//!
//! 🔴 **D2 Fix B lives here**: [`super::sql::RECLAIM_RELEASED_RUNNING_SQL`]
//! and [`super::sql::RECLAIM_RELEASED_RESUMING_SQL`] are two independent
//! statements, run as two independent `UPDATE`s in the same transaction --
//! never folded into one `state IN ('running', 'resuming')` statement. See
//! their own doc comments in `sql.rs` for why re-merging them reopens the
//! stuck-forever `resuming` deadlock, worse under N `--role api` replicas
//! than under Go's single instance.

use anyhow::anyhow;
use sqlx::PgPool;
use tracing::{error, warn};
use uuid::Uuid;

use super::sql;
use crate::orchestrator::paused_registry::{PausedRegistryError, RegistryResult, ReleasedHoldings};

fn backend_err(operation: &'static str, err: sqlx::Error) -> PausedRegistryError {
    PausedRegistryError::backend(operation, err)
}

/// `DiscardBreaker` (`grace.go:411-513`), ported. Both arms are ORed --
/// the stricter wins.
#[derive(Debug, Clone, Copy)]
pub(super) struct DiscardBreaker {
    pub max_rows: i64,
    pub max_ratio: f64,
    pub min_ratio_rows: i64,
}

/// `defaultDiscardMaxRows`/`defaultDiscardMaxRatio`/`defaultDiscardMinRatioRows`
/// (`grace.go:430-434`), verbatim. Not exposed as a config knob here, same as
/// Go: `MinRatioRows` was never configurable on the Go side either
/// (`services/shared/config/config.go` exposes `DiscardMaxRows`/
/// `DiscardMaxRatio` only).
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
    /// `DiscardBreaker.Allow` (`grace.go:465-513`), ported verbatim.
    /// `candidates == 0` always allows -- nothing to trip a breaker over.
    pub(super) fn allow(&self, candidates: i64, total: i64) -> Result<(), String> {
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

/// `ReclaimExpiredHoldings` (`store_postgres.go`'s `reclaimExpiredHoldings`),
/// ported: releases both live states unconditionally, then gates the
/// discard DELETE on the breaker -- **the whole transaction, releases
/// included, is abandoned if the breaker trips**, exactly mirroring Go's own
/// comment on that point.
pub(super) async fn reclaim_expired_holdings(
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
        // 🔴 The whole transaction is abandoned, releases included -- `tx`
        // drops here without `commit()`, which rolls back both UPDATEs
        // above along with the DELETE that never ran.
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

/// `ReleaseNodeHoldings` (`store_postgres.go:1798-1814`), ported: the third
/// seizure path, releasing whatever `node_id`'s previous process on this
/// same machine was holding when it died.
///
/// 🔴 For a `Running` row this only ever matches when `node_id` equals the
/// row's `origin_node_id` -- which, under the split node/api identity model,
/// `--role api`'s own identity never is (`origin_node_id` names the real
/// machine; an api replica's identity is a Pod name). It remains fully
/// effective for `Resuming` rows this same api replica claimed and never
/// finished resuming -- see the Stage C report's D1 section for why this is
/// not a gap this port needs to close: Fix B's lease-based reclaim is the
/// primary safety net for that state, this is a faster, identity-based
/// shortcut on top of it.
pub(super) async fn release_node_holdings(
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

    /// `MinRatioRows` exists so a small cluster's ordinary reclamation (two
    /// sandboxes out of eight, 25%) does not trip a 10% ratio limit.
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
