//! Cluster-singleton reconcile loop.
//! Grace entry and roster-free metrics stay leader-elected; heartbeat-derived
//! lease renewal runs per replica in [`super::replica_renewal`].
//! Metrics include parked and live rows in one at-risk total.

use std::sync::atomic::AtomicI32;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use tracing::{info, warn};
use uuid::Uuid;

use super::PostgresPausedSandboxRegistry;
use crate::orchestrator::paused_registry::PausedRegistryState;

use super::row::RegistryRow;

const LEASE_WARN_WINDOW: Duration = Duration::from_secs(30);

/// Reconcile counters derived from one registry snapshot.
#[derive(Debug, Default, Clone, Copy)]
pub struct ReconcileOutcome {
    /// Parked rows without a durable snapshot.
    pub stranded_rows: u64,
    /// Parked rows close to lease expiry.
    pub parked_lease_expiring: u64,
    /// Live rows with lapsed leases.
    pub live_lease_lapsed: u64,
    /// Running rows past their user deadline while the lease remains live.
    pub live_deadline_passed: u64,
    /// Rows the next reclaim pass can act on.
    pub reclaimable_now: u64,
}

impl ReconcileOutcome {
    /// Returns the total parked and live rows showing neglect.
    pub fn at_risk_rows(&self) -> u64 {
        self.stranded_rows
            + self.parked_lease_expiring
            + self.live_lease_lapsed
            + self.live_deadline_passed
    }
}

/// Computes reconcile counters from already-read rows.
pub fn compute_reconcile(rows: &[RegistryRow], now: DateTime<Utc>) -> ReconcileOutcome {
    let mut outcome = ReconcileOutcome::default();

    for row in rows {
        match row.state {
            PausedRegistryState::Publishing | PausedRegistryState::LocalOnly => {
                if row.snapshot_id.is_none() {
                    outcome.stranded_rows += 1;
                    continue;
                }
                let deadline = row.lease_expires_at.unwrap_or(row.updated_at);
                if deadline < now + LEASE_WARN_WINDOW {
                    outcome.parked_lease_expiring += 1;
                }
            }
            PausedRegistryState::Running | PausedRegistryState::Resuming => {
                let lease_expired = row.lease_expired(now);
                let deadline_passed = row.sandbox_expires_at.is_some_and(|d| d < now);
                if lease_expired {
                    outcome.live_lease_lapsed += 1;
                    if row.state == PausedRegistryState::Resuming || deadline_passed {
                        outcome.reclaimable_now += 1;
                    }
                } else if row.state == PausedRegistryState::Running && deadline_passed {
                    outcome.live_deadline_passed += 1;
                }
            }
            PausedRegistryState::Paused => {}
        }
    }

    outcome
}

/// Reads rows, records metrics, and returns one reconcile outcome.
pub async fn reconcile_once(
    registry: &PostgresPausedSandboxRegistry,
) -> anyhow::Result<ReconcileOutcome> {
    let start = std::time::Instant::now();
    let rows = match super::reads::list_registry_rows(registry).await {
        Ok(rows) => rows,
        Err(err) => {
            record_reconcile_read_failure(registry.cluster_id);
            return Err(anyhow::Error::from(err));
        }
    };

    let outcome = compute_reconcile(&rows, Utc::now());

    if outcome.at_risk_rows() > 0 {
        warn!(
            target: "agentenv",
            cluster_id = %registry.cluster_id,
            stranded_rows = outcome.stranded_rows,
            parked_lease_expiring = outcome.parked_lease_expiring,
            live_lease_lapsed = outcome.live_lease_lapsed,
            live_deadline_passed = outcome.live_deadline_passed,
            at_risk_rows = outcome.at_risk_rows(),
            reclaimable_now = outcome.reclaimable_now,
            "paused registry reconcile pass"
        );
    } else {
        info!(
            target: "agentenv",
            cluster_id = %registry.cluster_id,
            "paused registry reconcile pass"
        );
    }

    record_reconcile_metrics(registry.cluster_id, &rows, outcome, start.elapsed());

    Ok(outcome)
}

fn record_reconcile_metrics(
    cluster_id: Uuid,
    rows: &[RegistryRow],
    outcome: ReconcileOutcome,
    elapsed: Duration,
) {
    let cluster_label = cluster_id.to_string();

    // Seed every state so zero and missing remain distinct metrics.
    let mut by_state: std::collections::HashMap<&'static str, u64> = PausedRegistryState::ALL
        .iter()
        .map(|s| (s.as_str(), 0))
        .collect();
    for row in rows {
        *by_state.entry(row.state.as_str()).or_insert(0) += 1;
    }
    for (state, count) in by_state {
        metrics::gauge!(
            REGISTRY_ROWS_METRIC,
            "cluster_id" => cluster_label.clone(),
            "state" => state,
        )
        .set(count as f64);
    }

    metrics::gauge!(STRANDED_ROWS_METRIC, "cluster_id" => cluster_label.clone())
        .set(outcome.stranded_rows as f64);
    metrics::gauge!(PARKED_LEASE_EXPIRING_METRIC, "cluster_id" => cluster_label.clone())
        .set(outcome.parked_lease_expiring as f64);
    metrics::gauge!(LIVE_LEASE_LAPSED_METRIC, "cluster_id" => cluster_label.clone())
        .set(outcome.live_lease_lapsed as f64);
    metrics::gauge!(LIVE_DEADLINE_PASSED_METRIC, "cluster_id" => cluster_label.clone())
        .set(outcome.live_deadline_passed as f64);
    metrics::gauge!(RECLAIMABLE_NOW_METRIC, "cluster_id" => cluster_label.clone())
        .set(outcome.reclaimable_now as f64);
    metrics::gauge!(AT_RISK_ROWS_METRIC, "cluster_id" => cluster_label.clone())
        .set(outcome.at_risk_rows() as f64);
    metrics::histogram!(RECONCILE_DURATION_METRIC, "cluster_id" => cluster_label.clone())
        .record(elapsed.as_secs_f64());
    metrics::gauge!(LAST_SUCCESS_METRIC, "cluster_id" => cluster_label).set(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0),
    );
}

fn record_reconcile_read_failure(cluster_id: Uuid) {
    metrics::counter!(READ_FAILURES_METRIC, "cluster_id" => cluster_id.to_string()).increment(1);
}
const REGISTRY_ROWS_METRIC: &str = "agentenv_api_paused_registry_rows";
const STRANDED_ROWS_METRIC: &str = "agentenv_api_paused_registry_stranded_rows";
const PARKED_LEASE_EXPIRING_METRIC: &str = "agentenv_api_paused_registry_parked_lease_expiring";
const LIVE_LEASE_LAPSED_METRIC: &str = "agentenv_api_paused_registry_live_lease_lapsed";
const LIVE_DEADLINE_PASSED_METRIC: &str = "agentenv_api_paused_registry_live_deadline_passed";
const RECLAIMABLE_NOW_METRIC: &str = "agentenv_api_paused_registry_reclaimable_now";
const AT_RISK_ROWS_METRIC: &str = "agentenv_api_paused_registry_at_risk_rows";
const RECONCILE_DURATION_METRIC: &str = "agentenv_api_paused_registry_reconcile_duration_seconds";
const LAST_SUCCESS_METRIC: &str = "agentenv_api_paused_registry_last_success_timestamp_seconds";
const READ_FAILURES_METRIC: &str = "agentenv_api_paused_registry_read_failures_total";


// Bound leader-session statements server-side. Cancelling a client future on
// the pinned advisory-lock connection can leave its protocol desynchronized.
const TICK_BUDGET: Duration = Duration::from_secs(25);

// Applies the server-side budget once per leadership session.
async fn bound_statement_timeout(conn: &mut sqlx::PgConnection) {
    if let Err(err) = sqlx::query(&format!(
        "SET statement_timeout = '{}s'",
        TICK_BUDGET.as_secs()
    ))
    .execute(&mut *conn)
    .await
    {
        warn!(target: "agentenv", error = %err, "could not set the reconcile leader connection's statement_timeout");
    }
}

/// Starts the reconcile singleton, entering grace once per successful session
/// epoch before running reconcile passes. A zero interval disables the loop.
pub fn spawn(
    pool: PgPool,
    registry: Arc<PostgresPausedSandboxRegistry>,
    cluster_id: Uuid,
    lease_ttl: Duration,
    interval: Duration,
) -> crate::pg::SingletonTaskHandle {
    let last_pid = std::sync::Arc::new(AtomicI32::new(0));

    crate::pg::spawn_singleton_task(
        pool,
        crate::pg::AdvisoryLockKey::PausedRegistryReconcile,
        interval,
        move |mut ctx: crate::pg::LeaderContext<'_>| {
            let registry = Arc::clone(&registry);
            let last_pid = Arc::clone(&last_pid);
            Box::pin(async move {
                match super::grace::current_backend_pid(&mut ctx).await {
                    Ok(pid) if super::grace::is_new_epoch(&last_pid, pid) => {
                        bound_statement_timeout(ctx.conn).await;
                        match super::grace::enter(ctx.conn, cluster_id, lease_ttl.as_secs_f64())
                            .await
                        {
                            Ok(_) => {
                                // Record the epoch only after grace entry succeeds.
                                super::grace::record_epoch_entered(&last_pid, pid);
                            }
                            Err(err) => {
                                warn!(target: "agentenv", error = %err, "paused registry restart grace failed; will retry next tick");
                            }
                        }
                    }
                    Ok(_) => {}
                    Err(err) => {
                        warn!(target: "agentenv", error = %err, "could not tell whether this is a new reconcile leadership epoch");
                    }
                }

                // This timeout is safe because reconcile uses ephemeral pool connections,
                // never the pinned advisory-lock connection.
                match tokio::time::timeout(TICK_BUDGET, reconcile_once(&registry)).await {
                    Ok(Ok(_outcome)) => {}
                    Ok(Err(err)) => {
                        warn!(target: "agentenv", error = %err, "paused registry reconcile pass failed");
                    }
                    Err(_) => {
                        warn!(
                            target: "agentenv",
                            budget_secs = TICK_BUDGET.as_secs(),
                            "paused registry reconcile pass exceeded its time budget"
                        );
                    }
                }
            })
        },
    )
}

#[cfg(test)]
mod tests {
    use chrono::Duration as ChronoDuration;
    use uuid::Uuid;

    use super::*;
    use crate::types::SandboxId;

    fn row(state: PausedRegistryState, origin: &str) -> RegistryRow {
        let now = Utc::now();
        RegistryRow {
            sandbox_id: SandboxId::new(),
            cluster_id: Uuid::new_v4(),
            state,
            generation: 1,
            origin_node_id: origin.to_string(),
            claimed_by_node_id: None,
            snapshot_id: None,
            paused_at: now,
            updated_at: now,
            lease_expires_at: Some(now + ChronoDuration::seconds(3600)),
            sandbox_expires_at: None,
            execution_id: None,
            execution_started_at: None,
        }
    }

    #[test]
    fn at_risk_rows_folds_running_and_resuming_into_the_same_total_as_parked() {
        let mut stranded = row(PausedRegistryState::Publishing, "node-a");
        stranded.snapshot_id = None;

        let mut lapsed_running = row(PausedRegistryState::Running, "node-b");
        lapsed_running.lease_expires_at = Some(Utc::now() - ChronoDuration::seconds(10));

        let outcome = compute_reconcile(&[stranded, lapsed_running], Utc::now());
        assert_eq!(outcome.stranded_rows, 1);
        assert_eq!(outcome.live_lease_lapsed, 1);
        assert_eq!(
            outcome.at_risk_rows(),
            2,
            "both a stranded parked row and a lapsed running row must count toward the same total"
        );
    }

    #[test]
    fn reclaimable_now_matches_fix_bs_split_conditions() {
        let mut resuming_lapsed_only = row(PausedRegistryState::Resuming, "node-a");
        resuming_lapsed_only.lease_expires_at = Some(Utc::now() - ChronoDuration::seconds(1));
        resuming_lapsed_only.sandbox_expires_at = None;

        let mut running_lapsed_only = row(PausedRegistryState::Running, "node-b");
        running_lapsed_only.lease_expires_at = Some(Utc::now() - ChronoDuration::seconds(1));
        running_lapsed_only.sandbox_expires_at = None;

        let outcome = compute_reconcile(&[resuming_lapsed_only, running_lapsed_only], Utc::now());
        assert_eq!(
            outcome.reclaimable_now, 1,
            "only the resuming row (lease alone) should be reclaimable; the running row still needs a passed deadline"
        );
    }

    #[test]
    fn a_healthy_parked_row_is_not_at_risk() {
        let r = row(PausedRegistryState::Publishing, "node-a");
        let mut r = r;
        r.snapshot_id = Some(crate::snapshot::SnapshotId::generate());

        let outcome = compute_reconcile(&[r], Utc::now());
        assert_eq!(outcome.at_risk_rows(), 0);
    }
}

#[cfg(test)]
mod pg {
    use uuid::Uuid;

    use super::super::schema::migrate;
    use super::*;
    use crate::pg::harness::isolated_schema_pool_or_skip;

    async fn seed_row(
        pool: &PgPool,
        cluster_id: Uuid,
        state: &str,
        origin_node_id: &str,
        has_snapshot: bool,
    ) {
        let sandbox_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO paused_sandboxes (
                sandbox_id, cluster_id, state, generation, origin_node_id, snapshot_id,
                metadata, paused_at, updated_at, lease_expires_at, execution_id, execution_started_at
             ) VALUES ($1, $2, $3, 1, $4, $5, '{}'::jsonb, now(), now(),
                       now() + interval '1 hour', $6, now())",
        )
        .bind(sandbox_id)
        .bind(cluster_id)
        .bind(state)
        .bind(origin_node_id)
        .bind(if has_snapshot {
            Some(Uuid::new_v4())
        } else {
            None::<Uuid>
        })
        .bind(Uuid::new_v4())
        .execute(pool)
        .await
        .expect("seeding a row should succeed");
    }

    #[tokio::test]
    async fn reconcile_once_reports_row_and_at_risk_gauges() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};

        let pool = isolated_schema_pool_or_skip!("reconcile_once_reports_row_and_at_risk_gauges");
        migrate(&pool).await.expect("migration should succeed");

        let cluster_id = Uuid::new_v4();
        seed_row(&pool, cluster_id, "running", "node-a", true).await;
        seed_row(&pool, cluster_id, "publishing", "node-b", false).await;

        let registry =
            PostgresPausedSandboxRegistry::new(pool, cluster_id, Duration::from_secs(90));

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let guard = metrics::set_default_local_recorder(&recorder);
        let outcome = reconcile_once(&registry)
            .await
            .expect("reconcile pass should succeed");
        drop(guard);

        assert_eq!(outcome.stranded_rows, 1, "the snapshot-less publishing row");

        let cluster_label = cluster_id.to_string();
        let mut rows_by_state: std::collections::HashMap<String, f64> =
            std::collections::HashMap::new();
        let mut stranded = None;
        for (composite, _unit, _description, value) in snapshotter.snapshot().into_vec() {
            let key = composite.key();
            let Some(cid) = key.labels().find(|l| l.key() == "cluster_id") else {
                continue;
            };
            if cid.value() != cluster_label {
                continue;
            }
            match key.name() {
                "agentenv_api_paused_registry_rows" => {
                    if let DebugValue::Gauge(v) = value {
                        if let Some(state) = key.labels().find(|l| l.key() == "state") {
                            rows_by_state.insert(state.value().to_string(), v.into_inner());
                        }
                    }
                }
                "agentenv_api_paused_registry_stranded_rows" => {
                    if let DebugValue::Gauge(v) = value {
                        stranded = Some(v.into_inner());
                    }
                }
                _ => {}
            }
        }

        assert_eq!(
            rows_by_state.get("running").copied(),
            Some(1.0),
            "{rows_by_state:?}"
        );
        assert_eq!(
            rows_by_state.get("publishing").copied(),
            Some(1.0),
            "{rows_by_state:?}"
        );
        assert_eq!(
            rows_by_state.get("paused").copied(),
            Some(0.0),
            "every known state must be published, zeroed if absent -- not just the states this \
             cluster happens to have a row in: {rows_by_state:?}"
        );
        assert_eq!(stranded, Some(1.0));
    }
}
