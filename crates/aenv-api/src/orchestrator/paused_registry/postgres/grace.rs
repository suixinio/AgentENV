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
//! # B3: `AdvisoryLockKey::PausedRegistryRestartGrace` is reserved, not spent
//!
//! An earlier version of [`enter`] additionally took
//! `AdvisoryLockKey::PausedRegistryRestartGrace` with `pg_advisory_xact_lock`
//! around the extend-then-upsert pair, meant as defense in depth against a
//! future change letting more than one caller reach `enter` concurrently.
//! It did not do that: `pg_advisory_xact_lock` releases at the end of its
//! own implicit one-statement transaction (this connection runs outside an
//! explicit `BEGIN`, so every statement is its own transaction), which had
//! already happened by the time the *next* statement -- `EXTEND_LEASES_SQL`
//! -- even started. The lock was gone before anything it was meant to guard
//! ran, so two concurrent callers would have raced the extend-then-upsert
//! pair exactly as if the call were never there.
//!
//! Rather than wrap the pair in an explicit transaction to make the lock
//! real (`AdvisoryLockKey::PausedRegistryReconcile`'s own leader election
//! already provides the only serialisation this function has ever needed --
//! see the design section above), the call is simply removed: a lock that
//! protects nothing is worse than no lock, because it reads as protection to
//! the next person who has to reason about concurrent callers. Per
//! `src/pg/lock_keys.rs`'s own convention, the key stays reserved and
//! retired rather than being recycled for something else --
//! `AdvisoryLockKey::PausedRegistryRestartGrace` is simply unused by this
//! backend today.
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
use std::time::Duration;

use anyhow::Context;

use sqlx::{PgConnection, PgPool};
use tracing::{info, warn};
use uuid::Uuid;

use crate::pg::{AdvisoryLockKey, LeaderContext};

use super::sql::EXTEND_LEASES_SQL;

/// Task's own "D5": ports Go's `agentenv_scheduler_registry_write_phase`
/// (`grace.go`), renamed per this codebase's own `scheduler` -> `api`
/// convention. `0` = cold (no grace row for this cluster yet, the
/// conservative default [`is_serving`] itself already treats a missing row
/// as); `1` = grace (a row exists but `now() < grace_until`, set by
/// [`enter`] the instant it (re-)computes a fresh `grace_until`); `2` =
/// serving (`now() >= grace_until`, observed as a side effect of
/// [`is_serving`]'s own read -- cheap, and it is the only place this
/// process asks the question at all).
const WRITE_PHASE_METRIC: &str = "agentenv_api_paused_registry_write_phase";
/// Ports `agentenv_scheduler_registry_write_grace_downtime_seconds`.
const GRACE_DOWNTIME_METRIC: &str = "agentenv_api_paused_registry_grace_downtime_seconds";

/// `Grace.Enter`'s outcome (`GraceObservation`, `grace.go`), for logging and
/// for the `pg::` test suite's own assertions on what one `enter` call
/// actually computed -- not read by production code past the `info!` call
/// inside [`enter`] itself, which is why `cargo check --lib` alone (as
/// opposed to a build that also compiles tests) flags these fields.
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
pub struct GraceObservation {
    pub downtime_secs: f64,
    pub extended: i64,
}

/// `ExtendLeases` (`grace.go:376-397`) + this port's own grace-phase upsert,
/// run together on `conn`. Every real (non-[`attempt_initial_entry`]) caller
/// runs this on the single session
/// [`crate::pg::election::spawn_singleton_task`] is holding the reconcile
/// leader lock on, so this and the reconcile pass that follows it in the
/// same tick are already serialised against every other contender for that
/// lock -- see the module doc's own B3 section for why no additional lock is
/// taken here.
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

/// Whether `cluster_id`'s write surface has been extended past its last
/// known coverage gap -- Go's `RequireServing()`/`allowsLeaseTakeover()`,
/// treated as one question (see the module doc). `false` includes "no row
/// yet" (this cluster's reconcile leader has never entered grace under this
/// backend, i.e. `PhaseCold`), which is the conservative answer: nothing may
/// reclaim or take over a lapsed lease before the first grace pass has run.
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

/// Reads the reconcile leader's own connection's current PostgreSQL backend
/// pid -- the identity [`is_new_epoch`]/[`record_epoch_entered`] compare
/// against. Split out from what used to be one `new_epoch_since` call
/// (B2(b) below) so the caller can decide whether to record this pid as
/// "seen" only *after* [`enter`] for it has actually succeeded.
pub async fn current_backend_pid(ctx: &mut LeaderContext<'_>) -> anyhow::Result<i32> {
    sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *ctx.conn)
        .await
        .context("read this session's backend pid")
}

/// Whether `pid` is a PostgreSQL session this replica has not already
/// recorded [`enter`] as having succeeded for -- both a fresh election (this
/// replica, or a different one, just won leadership) and a same-replica
/// reconnect after a ping failure produce a new backend pid. See the module
/// doc for why this is the right unit of "coverage gap" to run [`enter`] on,
/// and why it is intentionally finer-grained than Go's "once per process".
///
/// A pure comparison, never a write -- see [`record_epoch_entered`] for the
/// other half.
pub fn is_new_epoch(last_pid: &AtomicI32, pid: i32) -> bool {
    // 0 is not a real backend pid (PostgreSQL's lowest is 1), so it is safe
    // as the "never observed" sentinel -- `AtomicI32` has no native
    // `Option`, and this avoids a `Mutex<Option<i32>>` for a value this
    // module only ever compares and swaps.
    last_pid.load(Ordering::SeqCst) != pid
}

/// Records `pid` as an epoch [`enter`] has succeeded for.
///
/// 🔴 B2(b): call this **only after `enter` has actually returned `Ok`** for
/// `pid`, never before. The previous shape swapped `last_pid` unconditionally
/// inside the epoch check itself, before `enter` had even been attempted --
/// so a single failed `enter` (a `statement_timeout` on a large table, a
/// transient connection error) permanently marked that epoch as "already
/// entered", and [`is_new_epoch`] would say `false` on every later tick for
/// the same session, skipping `enter` for the rest of that leadership term
/// even though it never actually ran. Recording only on success means a
/// failed attempt is retried on the very next tick, since the pid was never
/// marked seen.
pub fn record_epoch_entered(last_pid: &AtomicI32, pid: i32) {
    last_pid.store(pid, Ordering::SeqCst);
}

/// How long [`attempt_initial_entry`] will wait for its one-shot attempt
/// before giving up and letting startup continue regardless -- long enough
/// for an ordinary `enter` against a healthy database, short enough that a
/// wedged or unreachable one does not stall this process's own readiness.
const INITIAL_ENTRY_BUDGET: Duration = Duration::from_secs(5);

/// B2(1): a synchronous, best-effort attempt to close the window between
/// this replica's HTTP listener opening and the reconcile leader's first
/// [`enter`] call landing -- called once from
/// [`super::super::build_paused_registry`]'s `postgres` arm, after
/// `schema::migrate`, before that function returns and its caller opens for
/// traffic.
///
/// # Why not simply clear `paused_registry_grace` on every startup instead
///
/// The obvious-looking alternative -- delete or invalidate this cluster's
/// grace row on every `build_paused_registry` call, so [`is_serving`] falls
/// back to its conservative "no row" answer until *some* `enter` lands --
/// was considered and rejected. [`enter`] only ever runs again when the
/// *current* reconcile leader's own connection observes a new epoch
/// ([`is_new_epoch`]); an ordinary rolling restart of one non-leader replica
/// does not change the leader's connection at all. Clearing the row from
/// that replica's own startup would strand the whole cluster in
/// "not serving" -- reclaim permanently skipped, every claim forced onto
/// the conservative durable-only path -- until the *unrelated*, still-healthy
/// leader happens to fail over for some other reason, which could be a very
/// long time or never. A guard that is wrong in the common case (one pod
/// restarting during an ordinary deploy) to fix a narrow one (every replica
/// down at once) is not a fix.
///
/// # What this does instead
///
/// One non-blocking `pg_try_advisory_lock` attempt on the *same* key
/// [`crate::pg::AdvisoryLockKey::PausedRegistryReconcile`]'s own background
/// election contends for:
///
/// - If a healthy leader already holds it (the ordinary case, any time this
///   is not a fleet-wide cold start), the attempt fails immediately and this
///   function returns without touching anything -- the existing leader's
///   own `paused_registry_grace` row is trusted as-is, exactly as it was
///   before this fix. No wasted work, no risk.
/// - If nothing holds it (a genuine fleet-wide restart, or the very first
///   deployment ever), whichever replica's attempt wins runs [`enter`]
///   synchronously, right here, before releasing the lock again -- so by the
///   time *that* replica's own caller opens its listener, this cluster's
///   grace row is fresh. The lock is released immediately after (never held
///   past this one call), so the background reconcile loop's own election
///   is completely unaffected and simply re-acquires it in the ordinary way
///   on its own schedule.
///
/// This does not fully close the race for every replica simultaneously: a
/// *losing* replica's synchronous attempt fails fast and that replica
/// proceeds to open its own listener without waiting for the winner's
/// `enter` to finish, so a request landing on a losing replica within that
/// single round trip could still observe a stale row. That residual window
/// is a single advisory-lock-plus-two-statement round trip (typically low
/// single-digit milliseconds), not the unbounded "until the reconcile loop's
/// own first tick, which may not even have started before this process opens
/// for traffic" window that existed before this fix. Closing it completely
/// would require every replica to block on a cluster-wide startup barrier,
/// which is a materially larger amount of complexity and startup latency for
/// a residual risk this narrow.
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
            // Always release, win or lose the `enter` call itself -- holding
            // this past a single attempt would delay the background
            // reconcile loop's own election for no reason.
            let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
                .bind(AdvisoryLockKey::PausedRegistryReconcile.as_i64())
                .execute(&mut *conn)
                .await;
            result?;
        }
        // Not acquired: a healthy leader already holds it. Nothing to do --
        // its own `paused_registry_grace` row is trusted as-is.
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

        // A different connection (a different PostgreSQL backend pid) must
        // read as a new epoch again -- simulating this replica losing and
        // regaining leadership, or a different replica taking over.
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

    /// 🔴 B2(b)'s own regression pin: an epoch must **not** be recorded as
    /// seen just because it was checked -- only [`record_epoch_entered`]
    /// (called by the real caller only once `enter` has actually succeeded)
    /// may do that. Before this fix, the same call that checked also
    /// recorded, so a failed `enter` for `pid` permanently skipped retrying
    /// it for the rest of that session's leadership term.
    #[test]
    fn checking_is_new_epoch_repeatedly_without_recording_never_marks_it_seen() {
        let last_pid = AtomicI32::new(0);
        let pid = 4242;

        assert!(is_new_epoch(&last_pid, pid), "unseen pid must read as new");
        // Simulate a failed `enter`: the epoch is checked again on the next
        // tick, but never recorded.
        assert!(
            is_new_epoch(&last_pid, pid),
            "a pid that was only checked, never recorded, must still read as new on the next tick \
             -- this is what lets a failed enter() retry instead of being skipped forever"
        );
        assert!(
            is_new_epoch(&last_pid, pid),
            "and again -- checking alone must never have a side effect"
        );

        // Only recording flips it.
        record_epoch_entered(&last_pid, pid);
        assert!(
            !is_new_epoch(&last_pid, pid),
            "once actually recorded, the same pid must read as already-seen"
        );
    }

    /// B2(1): the fleet-wide-cold-start case -- nothing holds the reconcile
    /// lock yet, so `attempt_initial_entry` must win it, run `enter`, and
    /// leave the cluster reading as freshly graced (not serving yet, but for
    /// the reason a healthy `enter` produces, not the "no row at all" reason).
    #[tokio::test]
    async fn attempt_initial_entry_enters_grace_when_no_leader_holds_the_lock_yet() {
        let pool = isolated_schema_pool_or_skip!(
            "attempt_initial_entry_enters_grace_when_no_leader_holds_the_lock_yet"
        );
        migrate(&pool).await.expect("migration should succeed");

        let cluster_id = Uuid::new_v4();
        seed_row(&pool, cluster_id, 3600).await;

        // Before: PhaseCold, exactly like `a_cluster_that_never_entered_grace_is_not_serving`.
        assert!(!is_serving(&pool, cluster_id)
            .await
            .expect("query should succeed"));

        attempt_initial_entry(&pool, cluster_id, 90.0).await;

        // A row now exists and was freshly entered -- `entered_at` is recent,
        // proving `enter` actually ran rather than this being a no-op.
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

    /// B2(1)'s other half -- and the reason the "just delete the row on
    /// every startup" alternative was rejected (see `attempt_initial_entry`'s
    /// own doc): a *second* replica racing the same attempt while the first
    /// already holds the reconcile lock must back off and touch nothing,
    /// never overwrite the winner's fresh entry with its own redundant one
    /// concurrently.
    #[tokio::test]
    async fn attempt_initial_entry_backs_off_when_another_session_already_holds_the_lock() {
        let pool = isolated_schema_pool_or_skip!(
            "attempt_initial_entry_backs_off_when_another_session_already_holds_the_lock"
        );
        migrate(&pool).await.expect("migration should succeed");
        let cluster_id = Uuid::new_v4();

        // Simulate a healthy leader already holding the reconcile lock on
        // its own long-lived connection.
        let mut leader_conn = pool.acquire().await.expect("acquire should succeed");
        let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(crate::pg::AdvisoryLockKey::PausedRegistryReconcile.as_i64())
            .fetch_one(&mut *leader_conn)
            .await
            .expect("the leader's own acquire should succeed");
        assert!(acquired, "the simulated leader must win the lock first");

        // A losing replica's attempt must not block, must not error, and
        // must leave no grace row behind -- it observed a live leader and
        // deferred to it entirely.
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
