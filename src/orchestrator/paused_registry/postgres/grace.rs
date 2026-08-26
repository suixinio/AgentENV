//! The restart-grace lease extension -- a Rust port of `Grace`/`ExtendLeases`
//! (`grace.go`), redesigned for N `--role api` replicas instead of Go's
//! single scheduler process. Read this module's doc in full before touching
//! [`super::reconcile`] or [`super::reclaim_task`]: it is the piece the plan
//! doc (`docs/proposals/_sd-phase4-stageC-paused-registry.md` §6.3-6.4)
//! left as "leader-elected" without resolving how a *second*, independently
//! elected leader (the reclaim loop) learns the outcome.
//!
//! # Why Go's design does not translate directly
//!
//! Go's `Grace` is one `*Grace` struct shared in-process between
//! `RunRegistryReconcile` (which calls `Grace.Enter` once at startup, before
//! its own loop begins) and `RunReclaim` (which checks
//! `s.grace.RequireServing()` before every pass) -- a plain, cheap,
//! in-memory read, correct because there is exactly one process holding both
//! ends of it.
//!
//! `--role api` is N replicas, and this port gives the reconcile loop and
//! the reclaim loop **independent** elections
//! (`AdvisoryLockKey::PausedRegistryReconcile` /
//! `AdvisoryLockKey::PausedRegistryReclaim`) -- deliberately, because they
//! have no other reason to serialise on one replica (see this crate's own
//! Stage C report, D1, for the full argument). That means the reclaim
//! leader and the reconcile leader can be two different replicas, and
//! "is this cluster still inside its restart grace window" has to be a fact
//! both of them can observe -- not a flag one of them holds in memory the
//! other cannot see.
//!
//! # The design: persist the phase, gate every reader on the same query
//!
//! [`enter`] (called only by the reconcile task, on its own newly-detected
//! leadership epoch -- see [`new_epoch_since`]) runs `ExtendLeases` and
//! upserts one row per cluster into `paused_registry_grace`
//! (`grace_until`/`downtime_secs`/`leases_extended`). [`is_serving`] is a
//! single read of that row, safe to call from any replica, any number of
//! times, with no coordination of its own required: it answers "has *this*
//! cluster's write surface been extended past its last known coverage gap",
//! which is exactly what Go's `RequireServing()` and `allowsLeaseTakeover()`
//! both ask (this port treats them as the one question Go's own comments
//! suggest they are -- see this function's doc for the one place that
//! distinction would matter if it turned out not to be).
//!
//! [`enter`] itself additionally takes
//! `AdvisoryLockKey::PausedRegistryRestartGrace` as an xact-scoped mutex
//! around the extend-then-upsert pair. This is **not** required for
//! correctness under the design above -- only the reconcile leader ever
//! calls `enter` (never the reclaim leader), and leader election already
//! serialises that against every other reconcile-task contender. It is
//! defense in depth, spending the reserved key
//! (`src/pg::lock_keys.rs::AdvisoryLockKey::PausedRegistryRestartGrace`)
//! for the exact purpose its own doc comment names ("N replicas starting at
//! once must not each add their own downtime estimate"), in case a future
//! change ever lets more than one caller reach `enter` concurrently.
//!
//! # Why this can safely diverge from Go's exact "once per process" semantics
//!
//! [`new_epoch_since`] fires `enter` on every genuinely new PostgreSQL
//! session the reconcile leader connection represents -- both a fresh
//! election (this replica, or a different one, just won leadership) and a
//! same-replica reconnect after a ping failure. That is a strictly finer
//! grain than Go's "once per process lifetime": a friendly, near-instant
//! leadership handoff between two healthy replicas re-runs `ExtendLeases`
//! even though the real coverage gap was near zero.
//!
//! That is deliberately accepted rather than engineered away. `ExtendLeases`
//! is additive and monotonic-in-the-safe-direction only: every extra call
//! pushes affected leases *further* into the future, never earlier, so an
//! extra invocation cannot cause a healthy sandbox to be reclaimed early --
//! it can only make `lease_expires_at` a weaker signal over many redeploys
//! (`docs/proposals/_sd-phase4-stageC-paused-registry.md` §6.3 names this
//! same tradeoff for the N-simultaneous-cold-start case and accepts it for
//! the same reason). The one thing this design must never do -- and does
//! not -- is the opposite: leave a real coverage gap ungraced because some
//! *other* epoch already consumed a "run once" flag. See the Stage C
//! report's D1 section for the full argument this module's design rests on.

use std::sync::atomic::{AtomicI32, Ordering};

use anyhow::Context;

use sqlx::{PgConnection, PgPool};
use tracing::info;
use uuid::Uuid;

use crate::pg::{AdvisoryLockKey, LeaderContext};

use super::sql::EXTEND_LEASES_SQL;

/// `Grace.Enter`'s outcome (`GraceObservation`, `grace.go`), for logging and
/// for the `pg::` test suite's own assertions on what one `enter` call
/// actually computed -- not read by production code past the `info!` call
/// inside [`enter`] itself, which is why `cargo check --lib` alone (as
/// opposed to a build that also compiles tests) flags these fields.
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
pub(super) struct GraceObservation {
    pub downtime_secs: f64,
    pub extended: i64,
}

/// `ExtendLeases` (`grace.go:376-397`) + this port's own grace-phase upsert,
/// run together on `conn` -- the single session
/// [`crate::pg::election::spawn_singleton_task`] is holding the reconcile
/// leader lock on, so this and the reconcile pass that follows it in the
/// same tick are already serialised against every other contender for that
/// lock. The `AdvisoryLockKey::PausedRegistryRestartGrace` xact lock this
/// function also takes is belt-and-suspenders -- see the module doc.
pub(super) async fn enter(
    conn: &mut PgConnection,
    cluster_id: Uuid,
    ttl_secs: f64,
) -> anyhow::Result<GraceObservation> {
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(AdvisoryLockKey::PausedRegistryRestartGrace.as_i64())
        .execute(&mut *conn)
        .await
        .context("take the restart-grace xact lock")?;

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

    // The xact lock releases automatically at the implicit transaction each
    // of the two statements above ran in (autocommit, one statement per
    // transaction) -- there is no explicit `COMMIT`/advisory-unlock pair to
    // write here the way `schema.rs::migrate` needs one for its
    // session-scoped lock. `pg_advisory_xact_lock` is intentionally
    // transaction-scoped for exactly this reason: nothing here can leak it.

    info!(
        target: "agentenv",
        cluster_id = %cluster_id,
        downtime_secs,
        leases_extended = extended,
        grace_ttl_secs = ttl_secs,
        "paused registry write surface entering its restart grace period"
    );

    Ok(GraceObservation {
        downtime_secs,
        extended,
    })
}

/// Whether `cluster_id`'s write surface has been extended past its last
/// known coverage gap -- Go's `RequireServing()`/`allowsLeaseTakeover()`,
/// treated as one question (see the module doc). `false` includes "no row
/// yet" (this cluster's reconcile leader has never entered grace under this
/// backend, i.e. `PhaseCold`), which is the conservative answer: nothing may
/// reclaim or take over a lapsed lease before the first grace pass has run.
pub(super) async fn is_serving(pool: &PgPool, cluster_id: Uuid) -> anyhow::Result<bool> {
    let serving: Option<bool> = sqlx::query_scalar(
        "SELECT (now() >= grace_until) FROM paused_registry_grace WHERE cluster_id = $1",
    )
    .bind(cluster_id)
    .fetch_optional(pool)
    .await
    .context("read the restart-grace phase")?;

    Ok(serving.unwrap_or(false))
}

/// Detects a new PostgreSQL session on the reconcile leader's own connection
/// since the last tick this was called for -- both a fresh election (this
/// replica, or a different one, just won leadership) and a same-replica
/// reconnect after a ping failure produce a new backend pid. See the module
/// doc for why this is the right unit of "coverage gap" to run
/// [`enter`] on, and why it is intentionally finer-grained than Go's "once
/// per process".
pub(super) async fn new_epoch_since(
    ctx: &mut LeaderContext<'_>,
    last_pid: &AtomicI32,
) -> anyhow::Result<bool> {
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *ctx.conn)
        .await
        .context("read this session's backend pid")?;

    // 0 is not a real backend pid (PostgreSQL's lowest is 1), so it is safe
    // as the "never observed" sentinel -- `AtomicI32` has no native
    // `Option`, and this avoids a `Mutex<Option<i32>>` for a value this
    // module only ever compares and swaps.
    let previous = last_pid.swap(pid, Ordering::SeqCst);
    Ok(previous != pid)
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
        // The downtime inferred is roughly the row's own staleness (3600s),
        // not the ttl fallback (which only applies to an empty cluster).
        assert!(
            observation.downtime_secs > 3000.0,
            "downtime should reflect the seeded row's staleness: {}",
            observation.downtime_secs
        );

        // 🔴 Freshly extended, this cluster must read as serving: `grace_until`
        // is `now() + ttl`, strictly in the future the instant `enter` returns.
        // Give the clock a moment before asserting the opposite case below.
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
        // A ttl short enough for the test to outlive without a long sleep.
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
    async fn new_epoch_since_fires_once_per_session_change() {
        let pool = isolated_schema_pool_or_skip!("new_epoch_since_fires_once_per_session_change");
        let last_pid = AtomicI32::new(0);

        let mut conn = pool.acquire().await.expect("acquire should succeed");
        let mut ctx = LeaderContext { conn: &mut conn };
        assert!(
            new_epoch_since(&mut ctx, &last_pid)
                .await
                .expect("query should succeed"),
            "the first observation on a fresh sentinel must read as a new epoch"
        );
        assert!(
            !new_epoch_since(&mut ctx, &last_pid)
                .await
                .expect("query should succeed"),
            "the same session, observed again, must not read as a new epoch"
        );

        // A different connection (a different PostgreSQL backend pid) must
        // read as a new epoch again -- simulating this replica losing and
        // regaining leadership, or a different replica taking over.
        let mut conn_b = pool.acquire().await.expect("acquire should succeed");
        let mut ctx_b = LeaderContext { conn: &mut conn_b };
        assert!(
            new_epoch_since(&mut ctx_b, &last_pid)
                .await
                .expect("query should succeed"),
            "a different session must read as a new epoch"
        );
    }
}
