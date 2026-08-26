//! The reconcile leader loop -- a Rust port of `computeRegistryReconcile` +
//! `RunRegistryReconcile` (`services/scheduler/internal/reconcile.go`).
//!
//! Runs as a cluster-wide singleton
//! ([`crate::pg::election::spawn_singleton_task`],
//! `AdvisoryLockKey::PausedRegistryReconcile`) -- see [`super::grace`]'s
//! module doc for why this loop, not the reclaim loop, is the one that owns
//! entering the restart grace window.
//!
//! # B1: D2 Fix A no longer lives here
//!
//! An earlier version of this module also renewed `Running`/parked-state
//! leases from a fresh heartbeat roster (D2 Fix A, `151d00b`) as part of
//! this same leader-elected pass, using
//! [`crate::node_registry::registry::NodeRegistry::rosters_in_cluster`].
//! That was wrong under N `--role api` replicas for a structural reason, not
//! a bug in the renewal logic itself: `AtomicNodeRegistry` is a per-process,
//! in-memory roster with **no synchronisation between replicas** -- each
//! node's gRPC heartbeat connection is a long-lived HTTP/2 stream pinned to
//! whichever one `agentenv-api` Pod it happened to dial, so any single
//! replica's own `rosters_in_cluster` answer only ever covers the subset of
//! nodes whose heartbeats landed on *that* replica. Running Fix A only on
//! the reconcile *leader* -- one specific replica -- meant every node whose
//! heartbeat was not pinned to that one replica had no renewal path for its
//! `running` rows at all, silently reintroducing the exact "running row
//! lease freeze" failure Fix A was written to close (see this crate's own
//! memory note by that name), just steady-state instead of after a failover.
//!
//! Fix A now runs on [`super::replica_renewal`] instead: every replica, on
//! its own timer, unelected, renews from *its own* roster alone.
//! `renew_parked_leases`/`renew_live_leases` are idempotent, caller-asserted
//! `UPDATE`s (their own WHERE re-checks `origin_node_id` against the
//! asserted identity -- see `lease.rs`'s doc), so nothing about them ever
//! needed leadership; the leader-election here was serialising something
//! that did not require serialising. Running the same renewal
//! independently, unelected, on every replica means the *union* of what
//! every replica's own roster covers is what ends up renewed -- which, since
//! every node's heartbeat is pinned to exactly one replica, is the entire
//! cluster. See `postgres::contract`'s pg-gated
//! `two_replicas_each_holding_part_of_the_roster_together_renew_every_running_row`
//! for the union claim proved directly against two independent rosters.
//!
//! What stays leader-elected here, and why: [`super::grace::enter`] (a
//! per-cluster write that really must run exactly once per coverage-gap
//! epoch, not once per replica) and D4's metrics below (a per-tick read that
//! is cheap to run once and pointless to run N times over).
//!
//! # D4: the monitoring gap this port closes
//!
//! Go's `strandedRows`/`parkedLeaseExpiring` metrics only ever counted
//! `publishing`/`local_only` rows -- `running`/`resuming` rows have their own
//! `liveLeaseLapsed`/`liveDeadlinePassed` counters computed right alongside
//! them, but the two families were never folded into one exported gauge, so
//! an operator watching only the "stranded" family never saw a `running`
//! row's lease going unrenewed. [`ReconcileOutcome::at_risk_rows`] is the
//! unified figure this port exposes from day one -- see its own doc.

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

/// `defaultRegistryLeaseWarnWindow` (`reconcile.go:16`), verbatim: how far
/// ahead of a lease's own deadline this loop starts warning.
const LEASE_WARN_WINDOW: Duration = Duration::from_secs(30);

/// `computeRegistryReconcile`'s result (`registryReconcileResult`,
/// `reconcile.go`), ported -- minus the renewal-candidate lists B1 moved to
/// [`super::replica_renewal`].
#[derive(Debug, Default, Clone, Copy)]
pub(super) struct ReconcileOutcome {
    /// `publishing`/`local_only` rows with no snapshot at all -- a pause that
    /// never finished publishing and never will unless its origin node comes
    /// back.
    pub stranded_rows: u64,
    /// `publishing`/`local_only` rows whose lease is within
    /// [`LEASE_WARN_WINDOW`] of lapsing.
    pub parked_lease_expiring: u64,
    /// `running`/`resuming` rows whose lease has already lapsed.
    pub live_lease_lapsed: u64,
    /// `running` rows (only -- `resuming` has no user deadline of its own)
    /// whose `sandbox_expires_at` has passed while the lease has not.
    pub live_deadline_passed: u64,
    /// Rows [`super::reclaim`]'s next pass would actually act on: `running`
    /// rows with both conditions, or `resuming` rows with the lease alone
    /// (Fix B -- see `sql.rs::RECLAIM_RELEASED_RESUMING_SQL`'s doc).
    pub reclaimable_now: u64,
}

impl ReconcileOutcome {
    /// D4's unified figure: every row this pass found evidence of neglect
    /// for, live or parked, folded into the one number an operator watching
    /// a single gauge would actually see. Go's own metrics never exported
    /// this sum -- see this module's own doc.
    pub fn at_risk_rows(&self) -> u64 {
        self.stranded_rows
            + self.parked_lease_expiring
            + self.live_lease_lapsed
            + self.live_deadline_passed
    }
}

/// `computeRegistryReconcile` (`reconcile.go`), ported: a pure function over
/// the rows already read, so it is testable without a database (see the
/// `#[cfg(test)] mod tests` below) the same way Go's own version is tested
/// without one in `reconcile_test.go`.
pub(super) fn compute_reconcile(rows: &[RegistryRow], now: DateTime<Utc>) -> ReconcileOutcome {
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

/// One reconcile pass against the live database: list rows, compute D4's
/// metrics, log. Grace's own entry (`super::grace::enter`) happens in the
/// caller, on the same connection, before this runs -- see [`spawn`]'s doc.
pub(super) async fn reconcile_once(
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

/// D4-adjacent (task's own "Stage D remainder"): ports the subset of Go's
/// `recordRegistryReconcile` (`metrics.go`) that this port can compute
/// honestly from rows alone. **Deliberately excludes** `registry_ghost`,
/// `registry_untracked`, `registry_stale_copy`, `registry_holder_conflict`,
/// `registry_rows_without_roster`, `registry_roster_stale`, and the
/// heartbeat-lease-renewal candidate/renewed/failure gauges: every one of
/// those cross-references registry rows against a node's *heartbeat
/// roster*, and this module's own B1 doc (above) explains why that
/// cross-reference cannot run here -- `AtomicNodeRegistry` is a per-replica,
/// in-memory view covering only the nodes whose heartbeat happens to be
/// pinned to *this* `--role api` Pod, not the cluster's. Computing those six
/// metrics from a partial roster would not degrade gracefully, it would
/// actively lie: a node whose heartbeat landed on a different replica reads
/// as `ghost`/`untracked` here even though it is perfectly healthy. Go's
/// single-process scheduler has no such gap, which is exactly why this
/// reconcile port is the leader-elected, roster-free half (see the module
/// doc's "B1" section) and [`super::replica_renewal`] is the per-replica,
/// roster-scoped half -- the split this metrics function respects too.
/// `registry_enabled` also has no counterpart: this whole module is only
/// ever spawned under the Postgres backend (see [`spawn`]), so the series'
/// own presence in a scrape already says what the gauge would have.
fn record_reconcile_metrics(
    cluster_id: Uuid,
    rows: &[RegistryRow],
    outcome: ReconcileOutcome,
    elapsed: Duration,
) {
    let cluster_label = cluster_id.to_string();

    let mut by_state: std::collections::HashMap<&'static str, u64> = [
        ("publishing", 0),
        ("paused", 0),
        ("resuming", 0),
        ("local_only", 0),
        ("running", 0),
    ]
    .into_iter()
    .collect();
    for row in rows {
        *by_state.entry(row_state_label(row.state)).or_insert(0) += 1;
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

fn row_state_label(state: PausedRegistryState) -> &'static str {
    match state {
        PausedRegistryState::Publishing => "publishing",
        PausedRegistryState::Paused => "paused",
        PausedRegistryState::Resuming => "resuming",
        PausedRegistryState::LocalOnly => "local_only",
        PausedRegistryState::Running => "running",
    }
}

/// Ports `agentenv_scheduler_registry_rows{state}`.
const REGISTRY_ROWS_METRIC: &str = "agentenv_api_paused_registry_rows";
/// Ports `agentenv_scheduler_registry_stranded_rows`.
const STRANDED_ROWS_METRIC: &str = "agentenv_api_paused_registry_stranded_rows";
/// Ports `agentenv_scheduler_registry_parked_lease_expiring`.
const PARKED_LEASE_EXPIRING_METRIC: &str = "agentenv_api_paused_registry_parked_lease_expiring";
/// Ports `agentenv_scheduler_registry_live_lease_lapsed`.
const LIVE_LEASE_LAPSED_METRIC: &str = "agentenv_api_paused_registry_live_lease_lapsed";
/// Ports `agentenv_scheduler_registry_live_deadline_passed`.
const LIVE_DEADLINE_PASSED_METRIC: &str = "agentenv_api_paused_registry_live_deadline_passed";
/// Ports `agentenv_scheduler_registry_reclaimable_now`.
const RECLAIMABLE_NOW_METRIC: &str = "agentenv_api_paused_registry_reclaimable_now";
/// No Go counterpart -- this port's own D4 unified figure (see
/// [`ReconcileOutcome::at_risk_rows`]'s own doc).
const AT_RISK_ROWS_METRIC: &str = "agentenv_api_paused_registry_at_risk_rows";
/// Ports `agentenv_scheduler_registry_reconcile_duration_seconds`.
const RECONCILE_DURATION_METRIC: &str = "agentenv_api_paused_registry_reconcile_duration_seconds";
/// Ports `agentenv_scheduler_registry_last_success_timestamp_seconds`.
const LAST_SUCCESS_METRIC: &str = "agentenv_api_paused_registry_last_success_timestamp_seconds";
/// Ports `agentenv_scheduler_registry_read_failures_total`.
const READ_FAILURES_METRIC: &str = "agentenv_api_paused_registry_read_failures_total";

/// A single reconcile-task tick's own time budget, independent of `interval`.
///
/// `SingletonTaskHandle::shutdown` does not preempt a `body` call already in
/// flight (`src/pg/election.rs`'s own 🔴 doc) -- graceful shutdown waits out
/// whatever this tick is doing. This task's half of honouring that contract
/// is a server-side `SET statement_timeout`, applied once per newly-acquired
/// leader connection (see [`bound_statement_timeout`]) -- **not**
/// `tokio::time::timeout` wrapping the query future: this task reuses one
/// `PgConnection` across every tick for as long as it stays leader (the
/// connection [`crate::pg::election::spawn_singleton_task`] is holding the
/// advisory lock on), and dropping a query future client-side mid-flight
/// leaves that shared connection's wire protocol desynced for whatever tick
/// runs next on it -- a self-inflicted outage worse than the slow query it
/// was meant to bound. A server-side statement timeout aborts the statement
/// on PostgreSQL's own side and hands the same connection back usable,
/// surfacing as an ordinary `sqlx::Error`. [`reclaim_task::TICK_BUDGET`] is
/// the reclaim loop's identical counterpart, for the identical reason.
const TICK_BUDGET: Duration = Duration::from_secs(25);

/// Applies [`TICK_BUDGET`] as this session's `statement_timeout`, once per
/// newly-detected leadership epoch (cheap, but no reason to repeat it every
/// tick on an unchanged session).
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

/// Starts the reconcile leader loop
/// (`AdvisoryLockKey::PausedRegistryReconcile`). On every tick this
/// replica holds leadership: detects a new epoch on its own connection
/// (see [`super::grace::is_new_epoch`]) and, if so, runs
/// [`super::grace::enter`] first -- recording the epoch as seen only once
/// `enter` actually succeeds (B2(b) -- see [`super::grace::record_epoch_entered`]'s
/// own doc for why) -- then always runs one [`reconcile_once`] pass.
///
/// `interval <= Duration::ZERO` disables the loop, matching
/// [`super::reclaim_task::spawn`]'s identical convention (and Go's own
/// `RunRegistryReconcile`, which substitutes a 30s default instead --
/// this port refuses instead, since `PausedRegistryConfig::reconcile_interval`
/// already floors at one second and a zero here can only mean a caller
/// bypassed that floor).
pub(super) fn spawn(
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
                                // 🔴 B2(b): recorded only now, after success --
                                // see `grace::record_epoch_entered`'s own doc.
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

                // 🔴 `tokio::time::timeout` here, unlike around `grace::enter`
                // above: `reconcile_once` runs every one of its queries against
                // `registry.pool` (a fresh, ephemeral connection per query),
                // never against `ctx.conn` -- dropping this future on timeout
                // drops at most one such ephemeral connection, which the pool
                // reclaims cleanly. `ctx.conn` is the one connection this
                // safety valve must never be used on (see [`TICK_BUDGET`]'s
                // own doc).
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

    /// D4's own regression pin: a `running` row with a lapsed lease counts
    /// in [`ReconcileOutcome::at_risk_rows`], the same as a stranded
    /// `publishing` row -- not just in a `running`-only counter nobody
    /// watching "stranded" would see.
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

    /// D2 Fix B's own metric split, mirrored: a `resuming` row is
    /// reclaimable on a lapsed lease alone, a `running` row needs the
    /// deadline too.
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

    /// A `publishing` row with a snapshot and a lease well inside the warn
    /// window contributes to neither `stranded_rows` nor
    /// `parked_lease_expiring` -- the healthy, uninteresting case, pinned so
    /// a change that makes every row "at risk" is caught.
    #[test]
    fn a_healthy_parked_row_is_not_at_risk() {
        let r = row(PausedRegistryState::Publishing, "node-a");
        let mut r = r;
        r.snapshot_id = Some(crate::snapshot::SnapshotId::generate());

        let outcome = compute_reconcile(&[r], Utc::now());
        assert_eq!(outcome.at_risk_rows(), 0);
    }
}

/// Task's own "Stage D remainder": proves `reconcile_once` actually reports
/// [`record_reconcile_metrics`] against a real database end to end -- the
/// pure-function tests above cover `compute_reconcile`'s arithmetic, but
/// nothing until this exercised the metric emission wired onto its result
/// (the same "was the Ok(_outcome) actually read from" gap
/// `record_assignment`/`heartbeat`'s binding-execution metric test closes
/// for the RPC layer).
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
        // One healthy `running` row (fresh lease, has a snapshot) and one
        // `publishing` row with no snapshot at all -- stranded, per
        // `compute_reconcile`.
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
