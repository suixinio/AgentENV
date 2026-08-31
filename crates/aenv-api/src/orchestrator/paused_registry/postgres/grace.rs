//! PostgreSQL-backed restart grace shared by independently elected reconcile
//! and reclaim leaders. [`enter`] extends leases and persists the grace phase;
//! [`is_serving`] gates takeover and reclaim from every replica.
//! A new PostgreSQL backend PID starts a new epoch. Record it only after
//! [`enter`] succeeds, so transient failures retry on the next tick.
//! Extra entries only extend leases and are therefore conservative.

use std::sync::atomic::{AtomicI32, Ordering};
use std::time::Duration;

use anyhow::Context;

use sqlx::{PgConnection, PgPool};
use tracing::{info, warn};
use uuid::Uuid;

use crate::pg::{AdvisoryLockKey, LeaderContext};

use super::sql::EXTEND_LEASES_SQL;
const WRITE_PHASE_METRIC: &str = "agentenv_api_paused_registry_write_phase";
const GRACE_DOWNTIME_METRIC: &str = "agentenv_api_paused_registry_grace_downtime_seconds";

/// Restart-grace measurements produced by [`enter`].
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
pub struct GraceObservation {
    pub downtime_secs: f64,
    pub extended: i64,
}

/// Extends leases and persists the cluster's restart-grace phase.
///
/// The caller must hold the reconcile leader lock on `conn`.
pub async fn enter(
    conn: &mut PgConnection,
    cluster_id: Uuid,
    ttl_secs: f64,
) -> anyhow::Result<GraceObservation> {
    let (downtime_secs, extended): (f64, i64) = sqlx::query_as(EXTEND_LEASES_SQL)
        .bind(cluster_id)
        .bind(ttl_secs)
        .fetch_one(&mut *conn)
        .await
        .context("extend paused registry leases")?;

    sqlx::query(
        "INSERT INTO paused_registry_grace (cluster_id, grace_until, downtime_secs, leases_extended, entered_at)
         VALUES ($1, now() + make_interval(secs => $2::double precision), $3, $4, now())
         ON CONFLICT (cluster_id) DO UPDATE SET
             grace_until     = EXCLUDED.grace_until,
             downtime_secs   = EXCLUDED.downtime_secs,
             leases_extended = EXCLUDED.leases_extended,
             entered_at      = EXCLUDED.entered_at",
    )
    .bind(cluster_id)
    .bind(ttl_secs)
    .bind(downtime_secs)
    .bind(extended)
    .execute(&mut *conn)
    .await
    .context("record the restart-grace phase")?;

    info!(
        target: "agentenv",
        cluster_id = %cluster_id,
        downtime_secs,
        leases_extended = extended,
        grace_ttl_secs = ttl_secs,
        "paused registry write surface entering its restart grace period"
    );

    let cluster_label = cluster_id.to_string();
    metrics::gauge!(WRITE_PHASE_METRIC, "cluster_id" => cluster_label.clone()).set(1.0);
    metrics::gauge!(GRACE_DOWNTIME_METRIC, "cluster_id" => cluster_label).set(downtime_secs);

    Ok(GraceObservation {
        downtime_secs,
        extended,
    })
}

/// Returns whether the cluster is past restart grace.
///
/// Missing rows conservatively return `false`.
pub async fn is_serving(pool: &PgPool, cluster_id: Uuid) -> anyhow::Result<bool> {
    let serving: Option<bool> = sqlx::query_scalar(
        "SELECT (now() >= grace_until) FROM paused_registry_grace WHERE cluster_id = $1",
    )
    .bind(cluster_id)
    .fetch_optional(pool)
    .await
    .context("read the restart-grace phase")?;

    let phase = match serving {
        None => 0.0,        // cold: no grace row for this cluster yet
        Some(false) => 1.0, // grace: row exists, still short of grace_until
        Some(true) => 2.0,  // serving: past grace_until
    };
    metrics::gauge!(WRITE_PHASE_METRIC, "cluster_id" => cluster_id.to_string()).set(phase);

    Ok(serving.unwrap_or(false))
}

/// Returns the reconcile leader connection's PostgreSQL backend PID.
pub async fn current_backend_pid(ctx: &mut LeaderContext<'_>) -> anyhow::Result<i32> {
    sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *ctx.conn)
        .await
        .context("read this session's backend pid")
}

/// Returns whether `pid` has not completed [`enter`] on this replica.
pub fn is_new_epoch(last_pid: &AtomicI32, pid: i32) -> bool {
    // PostgreSQL backend PIDs start at 1, so 0 is the unseen sentinel.
    last_pid.load(Ordering::SeqCst) != pid
}

/// Records `pid` only after its [`enter`] call succeeds.
pub fn record_epoch_entered(last_pid: &AtomicI32, pid: i32) {
    last_pid.store(pid, Ordering::SeqCst);
}

/// Startup budget for the best-effort initial grace entry.
const INITIAL_ENTRY_BUDGET: Duration = Duration::from_secs(5);

/// Attempts one non-blocking initial grace entry before startup completes.
///
/// It uses the reconcile election lock and leaves an existing leader untouched.
pub async fn attempt_initial_entry(pool: &PgPool, cluster_id: Uuid, ttl_secs: f64) {
    let attempt = async {
        let mut conn = pool
            .acquire()
            .await
            .context("acquire a connection for the initial restart-grace attempt")?;

        let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(AdvisoryLockKey::PausedRegistryReconcile.as_i64())
            .fetch_one(&mut *conn)
            .await
            .context("attempt the initial restart-grace lock")?;

        if acquired {
            let result = enter(&mut conn, cluster_id, ttl_secs).await;
            // Release even when `enter` fails so the background election is never delayed.
            let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
                .bind(AdvisoryLockKey::PausedRegistryReconcile.as_i64())
                .execute(&mut *conn)
                .await;
            result?;
        }
        anyhow::Ok(())
    };

    match tokio::time::timeout(INITIAL_ENTRY_BUDGET, attempt).await {
        Ok(Ok(())) => {}
        Ok(Err(err)) => {
            warn!(target: "agentenv", error = %err, "initial paused registry restart-grace attempt failed; the background reconcile loop will retry");
        }
        Err(_) => {
            warn!(
                target: "agentenv",
                budget_secs = INITIAL_ENTRY_BUDGET.as_secs(),
                "initial paused registry restart-grace attempt exceeded its time budget; the \
                 background reconcile loop will retry"
            );
        }
    }
}

#[cfg(test)]
mod pg {
    use std::time::Duration;

    use uuid::Uuid;

    use super::super::schema::migrate;
    use super::*;
    use crate::pg::harness::isolated_schema_pool_or_skip;

    async fn seed_row(pool: &PgPool, cluster_id: Uuid, updated_at_age_secs: i64) {
        let sandbox_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO paused_sandboxes (
                sandbox_id, cluster_id, state, generation, origin_node_id, snapshot_id,
                metadata, paused_at, updated_at, lease_expires_at, execution_id, execution_started_at
             ) VALUES ($1, $2, 'running', 1, 'node-a', NULL, '{}'::jsonb,
                       now() - make_interval(secs => $3::double precision),
                       now() - make_interval(secs => $3::double precision),
                       now() - make_interval(secs => $3::double precision),
                       $4, now() - make_interval(secs => $3::double precision))",
        )
        .bind(sandbox_id)
        .bind(cluster_id)
        .bind(updated_at_age_secs as f64)
        .bind(Uuid::new_v4())
        .execute(pool)
        .await
        .expect("seeding a running row should succeed");
    }

    #[tokio::test]
    async fn a_cluster_that_never_entered_grace_is_not_serving() {
        let pool =
            isolated_schema_pool_or_skip!("a_cluster_that_never_entered_grace_is_not_serving");
        migrate(&pool).await.expect("migration should succeed");

        let cluster_id = Uuid::new_v4();
        assert!(!is_serving(&pool, cluster_id)
            .await
            .expect("query should succeed"));
    }

    #[tokio::test]
    async fn entering_grace_extends_every_lease_by_the_inferred_downtime_plus_ttl() {
        let pool = isolated_schema_pool_or_skip!(
            "entering_grace_extends_every_lease_by_the_inferred_downtime_plus_ttl"
        );
        migrate(&pool).await.expect("migration should succeed");

        let cluster_id = Uuid::new_v4();
        seed_row(&pool, cluster_id, 3600).await;

        let mut conn = pool.acquire().await.expect("acquire should succeed");
        let observation = enter(&mut conn, cluster_id, 90.0)
            .await
            .expect("entering grace should succeed");
        assert!(observation.extended >= 1);
        assert!(
            observation.downtime_secs > 3000.0,
            "downtime should reflect the seeded row's staleness: {}",
            observation.downtime_secs
        );

        assert!(!is_serving(&pool, cluster_id)
            .await
            .expect("query should succeed"));
    }

    #[tokio::test]
    async fn is_serving_flips_true_once_the_ttl_elapses() {
        let pool = isolated_schema_pool_or_skip!("is_serving_flips_true_once_the_ttl_elapses");
        migrate(&pool).await.expect("migration should succeed");

        let cluster_id = Uuid::new_v4();
        seed_row(&pool, cluster_id, 10).await;

        let mut conn = pool.acquire().await.expect("acquire should succeed");
        // Short TTL keeps the test fast.
        enter(&mut conn, cluster_id, 0.2)
            .await
            .expect("entering grace should succeed");
        assert!(!is_serving(&pool, cluster_id)
            .await
            .expect("query should succeed"));

        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(is_serving(&pool, cluster_id)
            .await
            .expect("query should succeed"));
    }

    #[tokio::test]
    async fn is_new_epoch_fires_once_per_session_change_once_recorded() {
        let pool = isolated_schema_pool_or_skip!(
            "is_new_epoch_fires_once_per_session_change_once_recorded"
        );
        let last_pid = AtomicI32::new(0);

        let mut conn = pool.acquire().await.expect("acquire should succeed");
        let mut ctx = LeaderContext { conn: &mut conn };
        let pid = current_backend_pid(&mut ctx)
            .await
            .expect("query should succeed");
        assert!(
            is_new_epoch(&last_pid, pid),
            "the first observation on a fresh sentinel must read as a new epoch"
        );
        record_epoch_entered(&last_pid, pid);
        assert!(
            !is_new_epoch(&last_pid, pid),
            "the same session, observed again after being recorded, must not read as a new epoch"
        );

        let mut conn_b = pool.acquire().await.expect("acquire should succeed");
        let mut ctx_b = LeaderContext { conn: &mut conn_b };
        let pid_b = current_backend_pid(&mut ctx_b)
            .await
            .expect("query should succeed");
        assert!(
            is_new_epoch(&last_pid, pid_b),
            "a different session must read as a new epoch"
        );
    }

    #[test]
    fn checking_is_new_epoch_repeatedly_without_recording_never_marks_it_seen() {
        let last_pid = AtomicI32::new(0);
        let pid = 4242;

        assert!(is_new_epoch(&last_pid, pid), "unseen pid must read as new");
        assert!(
            is_new_epoch(&last_pid, pid),
            "a pid that was only checked, never recorded, must still read as new on the next tick \
             -- this is what lets a failed enter() retry instead of being skipped forever"
        );
        assert!(
            is_new_epoch(&last_pid, pid),
            "and again -- checking alone must never have a side effect"
        );

        record_epoch_entered(&last_pid, pid);
        assert!(
            !is_new_epoch(&last_pid, pid),
            "once actually recorded, the same pid must read as already-seen"
        );
    }

    #[tokio::test]
    async fn attempt_initial_entry_enters_grace_when_no_leader_holds_the_lock_yet() {
        let pool = isolated_schema_pool_or_skip!(
            "attempt_initial_entry_enters_grace_when_no_leader_holds_the_lock_yet"
        );
        migrate(&pool).await.expect("migration should succeed");

        let cluster_id = Uuid::new_v4();
        seed_row(&pool, cluster_id, 3600).await;

        assert!(!is_serving(&pool, cluster_id)
            .await
            .expect("query should succeed"));

        attempt_initial_entry(&pool, cluster_id, 90.0).await;

        let entered_at: chrono::DateTime<chrono::Utc> = sqlx::query_scalar(
            "SELECT entered_at FROM paused_registry_grace WHERE cluster_id = $1",
        )
        .bind(cluster_id)
        .fetch_one(&pool)
        .await
        .expect("a grace row should now exist");
        let age = chrono::Utc::now() - entered_at;
        assert!(
            age < chrono::Duration::seconds(30),
            "the row must have just been written by this attempt, not be some other stale row: \
             age = {age}"
        );
    }

    #[tokio::test]
    async fn attempt_initial_entry_backs_off_when_another_session_already_holds_the_lock() {
        let pool = isolated_schema_pool_or_skip!(
            "attempt_initial_entry_backs_off_when_another_session_already_holds_the_lock"
        );
        migrate(&pool).await.expect("migration should succeed");
        let cluster_id = Uuid::new_v4();

        let mut leader_conn = pool.acquire().await.expect("acquire should succeed");
        let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(crate::pg::AdvisoryLockKey::PausedRegistryReconcile.as_i64())
            .fetch_one(&mut *leader_conn)
            .await
            .expect("the leader's own acquire should succeed");
        assert!(acquired, "the simulated leader must win the lock first");

        attempt_initial_entry(&pool, cluster_id, 90.0).await;

        let row_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM paused_registry_grace WHERE cluster_id = $1)",
        )
        .bind(cluster_id)
        .fetch_one(&pool)
        .await
        .expect("query should succeed");
        assert!(
            !row_exists,
            "a losing attempt must not have written a grace row of its own"
        );
    }
}
