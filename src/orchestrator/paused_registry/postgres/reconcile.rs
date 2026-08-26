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
    let rows = super::reads::list_registry_rows(registry)
        .await
        .map_err(anyhow::Error::from)?;

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

    Ok(outcome)
}

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
