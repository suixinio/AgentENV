//! The reconcile leader loop -- a Rust port of `computeRegistryReconcile` +
//! `RunRegistryReconcile` (`services/scheduler/internal/reconcile.go`), the
//! home of **D2 Fix A** (`151d00b`): proactively renewing `Running` row
//! leases from a fresh heartbeat roster, the path that never existed before
//! Fix A and that the split node/api identity model makes mandatory rather
//! than optional (`renew_lease`'s own `Running` branch never matches an api
//! replica's pod identity -- see `lease.rs`'s doc).
//!
//! Runs as a cluster-wide singleton
//! ([`crate::pg::election::spawn_singleton_task`],
//! `AdvisoryLockKey::PausedRegistryReconcile`) -- see [`super::grace`]'s
//! module doc for why this loop, not the reclaim loop, is the one that owns
//! entering the restart grace window.
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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use tracing::{info, warn};
use uuid::Uuid;

use super::lease::{renew_live_leases, renew_parked_leases, LeaseHolder};
use super::PostgresPausedSandboxRegistry;
use crate::node_registry::types::Roster;
use crate::orchestrator::paused_registry::PausedRegistryState;

use super::row::RegistryRow;

/// `defaultObservedReportTTL` (Stage A, `src/node_registry/registry.rs`) --
/// reused rather than re-declared: a heartbeat roster older than this is
/// exactly as stale for this loop's purposes as it is for Stage A's own
/// `NodeStatus` derivation.
const ROSTER_FRESH_TTL: Duration = crate::node_registry::registry::DEFAULT_OBSERVED_REPORT_TTL;

/// `defaultRegistryLeaseWarnWindow` (`reconcile.go:16`), verbatim: how far
/// ahead of a lease's own deadline this loop starts warning.
const LEASE_WARN_WINDOW: Duration = Duration::from_secs(30);

/// `computeRegistryReconcile`'s result (`registryReconcileResult`,
/// `reconcile.go`), ported.
#[derive(Debug, Default, Clone)]
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
    /// `publishing`/`local_only` rows whose origin node has a fresh
    /// heartbeat roster that still lists them -- Fix A's parked-side sibling.
    pub parked_lease_renewals: Vec<LeaseHolder>,
    /// `running` rows whose origin node has a fresh heartbeat roster that
    /// still lists them -- **Fix A itself**.
    pub live_lease_renewals: Vec<LeaseHolder>,
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
/// the rows already read and the roster already fetched, so it is testable
/// without a database (see the `#[cfg(test)] mod tests` below) the same way
/// Go's own version is tested without one in `reconcile_test.go`.
pub(super) fn compute_reconcile(
    rows: &[RegistryRow],
    rosters: &[Roster],
    now: DateTime<Utc>,
    roster_now: SystemTime,
) -> ReconcileOutcome {
    let roster_by_node: HashMap<&str, &Roster> =
        rosters.iter().map(|r| (r.node_id.as_str(), r)).collect();
    let is_fresh_and_listed = |node_id: &str, sandbox_id: &str| -> bool {
        let Some(roster) = roster_by_node.get(node_id) else {
            return false;
        };
        let Some(last_seen) = roster.last_seen else {
            return false;
        };
        let fresh = roster_now
            .duration_since(last_seen)
            .map(|age| age <= ROSTER_FRESH_TTL)
            .unwrap_or(true); // last_seen in the future (clock skew): treat as fresh.
        fresh
            && roster
                .entries
                .iter()
                .any(|entry| entry.sandbox_id == sandbox_id)
    };

    let mut outcome = ReconcileOutcome::default();

    for row in rows {
        let sandbox_id = row.sandbox_id.to_string();
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
                if is_fresh_and_listed(&row.origin_node_id, &sandbox_id) {
                    outcome.parked_lease_renewals.push(LeaseHolder {
                        sandbox_id: row.sandbox_id,
                        node_id: row.origin_node_id.clone(),
                    });
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
                if row.state == PausedRegistryState::Running
                    && is_fresh_and_listed(&row.origin_node_id, &sandbox_id)
                {
                    outcome.live_lease_renewals.push(LeaseHolder {
                        sandbox_id: row.sandbox_id,
                        node_id: row.origin_node_id.clone(),
                    });
                }
            }
            PausedRegistryState::Paused => {}
        }
    }

    outcome
}

/// One reconcile pass against the live database: list rows, fetch the
/// current roster, compute, then act on the two renewal candidate lists.
/// Grace's own entry (`super::grace::enter`) happens in the caller, on the
/// same connection, before this runs -- see [`spawn`]'s doc.
pub(super) async fn reconcile_once(
    registry: &PostgresPausedSandboxRegistry,
    node_registry: &dyn crate::node_registry::registry::NodeRegistry,
) -> anyhow::Result<ReconcileOutcome> {
    let rows = super::reads::list_registry_rows(registry)
        .await
        .map_err(anyhow::Error::from)?;
    let rosters = node_registry.rosters_in_cluster(&registry.cluster_id.to_string());

    let outcome = compute_reconcile(&rows, &rosters, Utc::now(), SystemTime::now());

    if !outcome.parked_lease_renewals.is_empty() {
        renew_parked_leases(registry, &outcome.parked_lease_renewals)
            .await
            .map_err(anyhow::Error::from)?;
    }
    if !outcome.live_lease_renewals.is_empty() {
        renew_live_leases(registry, &outcome.live_lease_renewals)
            .await
            .map_err(anyhow::Error::from)?;
    }

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
            parked_leases_renewed = outcome.parked_lease_renewals.len(),
            live_leases_renewed = outcome.live_lease_renewals.len(),
            "paused registry reconcile pass"
        );
    } else {
        info!(
            target: "agentenv",
            cluster_id = %registry.cluster_id,
            parked_leases_renewed = outcome.parked_lease_renewals.len(),
            live_leases_renewed = outcome.live_lease_renewals.len(),
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
/// (see [`super::grace::new_epoch_since`]) and, if so, runs
/// [`super::grace::enter`] first -- then always runs one
/// [`reconcile_once`] pass.
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
    node_registry: Arc<dyn crate::node_registry::registry::NodeRegistry>,
) -> crate::pg::SingletonTaskHandle {
    let last_pid = std::sync::Arc::new(std::sync::atomic::AtomicI32::new(0));

    crate::pg::spawn_singleton_task(
        pool,
        crate::pg::AdvisoryLockKey::PausedRegistryReconcile,
        interval,
        move |mut ctx: crate::pg::LeaderContext<'_>| {
            let registry = Arc::clone(&registry);
            let node_registry = Arc::clone(&node_registry);
            let last_pid = Arc::clone(&last_pid);
            Box::pin(async move {
                match super::grace::new_epoch_since(&mut ctx, &last_pid).await {
                    Ok(true) => {
                        bound_statement_timeout(ctx.conn).await;
                        if let Err(err) =
                            super::grace::enter(ctx.conn, cluster_id, lease_ttl.as_secs_f64()).await
                        {
                            warn!(target: "agentenv", error = %err, "paused registry restart grace failed");
                        }
                    }
                    Ok(false) => {}
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
                match tokio::time::timeout(
                    TICK_BUDGET,
                    reconcile_once(&registry, node_registry.as_ref()),
                )
                .await
                {
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
    use crate::node_registry::types::RosterEntry;
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

    fn fresh_roster(node_id: &str, sandbox_ids: &[SandboxId]) -> Roster {
        Roster {
            node_id: node_id.to_string(),
            entries: sandbox_ids
                .iter()
                .map(|id| RosterEntry {
                    sandbox_id: id.to_string(),
                    execution_id: Uuid::new_v4().to_string(),
                    projection_ttl: Duration::from_secs(30),
                })
                .collect(),
            last_seen: Some(SystemTime::now()),
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

        let outcome = compute_reconcile(
            &[stranded, lapsed_running],
            &[],
            Utc::now(),
            SystemTime::now(),
        );
        assert_eq!(outcome.stranded_rows, 1);
        assert_eq!(outcome.live_lease_lapsed, 1);
        assert_eq!(
            outcome.at_risk_rows(),
            2,
            "both a stranded parked row and a lapsed running row must count toward the same total"
        );
    }

    /// Fix A itself: a `running` row whose origin node has a fresh roster
    /// still listing it is a renewal candidate.
    #[test]
    fn a_running_row_with_a_fresh_roster_entry_is_a_live_renewal_candidate() {
        let r = row(PausedRegistryState::Running, "node-a");
        let roster = fresh_roster("node-a", &[r.sandbox_id]);

        let outcome = compute_reconcile(&[r.clone()], &[roster], Utc::now(), SystemTime::now());
        assert_eq!(outcome.live_lease_renewals.len(), 1);
        assert_eq!(outcome.live_lease_renewals[0].sandbox_id, r.sandbox_id);
        assert_eq!(outcome.parked_lease_renewals.len(), 0);
    }

    /// The sibling for `publishing`/`local_only` -- Fix A's original half.
    #[test]
    fn a_parked_row_with_a_fresh_roster_entry_is_a_parked_renewal_candidate() {
        let r = row(PausedRegistryState::LocalOnly, "node-a");
        let mut r = r;
        r.snapshot_id = Some(crate::snapshot::SnapshotId::generate());
        let roster = fresh_roster("node-a", &[r.sandbox_id]);

        let outcome = compute_reconcile(&[r.clone()], &[roster], Utc::now(), SystemTime::now());
        assert_eq!(outcome.parked_lease_renewals.len(), 1);
        assert_eq!(outcome.live_lease_renewals.len(), 0);
    }

    /// A `resuming` row is never a renewal candidate through this path --
    /// only `running` rows are (`renew_live_leases`' own WHERE is
    /// `state = 'running'` alone).
    #[test]
    fn a_resuming_row_is_never_a_live_renewal_candidate_even_with_a_fresh_roster() {
        let r = row(PausedRegistryState::Resuming, "node-a");
        let roster = fresh_roster("node-a", &[r.sandbox_id]);

        let outcome = compute_reconcile(&[r], &[roster], Utc::now(), SystemTime::now());
        assert_eq!(outcome.live_lease_renewals.len(), 0);
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

        let outcome = compute_reconcile(
            &[resuming_lapsed_only, running_lapsed_only],
            &[],
            Utc::now(),
            SystemTime::now(),
        );
        assert_eq!(
            outcome.reclaimable_now, 1,
            "only the resuming row (lease alone) should be reclaimable; the running row still needs a passed deadline"
        );
    }

    /// A roster entry older than [`ROSTER_FRESH_TTL`] does not count as a
    /// renewal source -- a stale heartbeat is not evidence the node is
    /// still alive.
    #[test]
    fn a_stale_roster_entry_is_not_a_renewal_source() {
        let r = row(PausedRegistryState::Running, "node-a");
        let mut roster = fresh_roster("node-a", &[r.sandbox_id]);
        roster.last_seen = Some(SystemTime::now() - Duration::from_secs(120));

        let outcome = compute_reconcile(&[r], &[roster], Utc::now(), SystemTime::now());
        assert_eq!(outcome.live_lease_renewals.len(), 0);
    }
}
