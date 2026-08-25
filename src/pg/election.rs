//! A cluster-wide singleton background task, built on PostgreSQL's
//! session-scoped advisory locks.
//!
//! Every `--role api` replica runs the same loop with the same
//! [`AdvisoryLockKey`]; PostgreSQL decides which one is "it" purely by which
//! replica's session first calls `pg_try_advisory_lock` successfully. There
//! is no lease, no TTL and nothing to renew: the lock lives exactly as long
//! as the one Postgres session that holds it, so a crash or a network
//! partition on the leader releases it automatically the moment that
//! session's connection drops — which is the whole reason this is built on
//! `pg_try_advisory_lock` rather than a Redis-style TTL lock needing its own
//! renewal loop and its own split-brain window while a renewal is late.
//!
//! Stage B and Stage C consumers: see [`AdvisoryLockKey`] for the reserved
//! key each of you owns. Nothing in this module runs anything on its own —
//! call [`spawn_singleton_task`] from wherever `--role api` assembly already
//! starts its other background loops.

use std::panic::AssertUnwindSafe;
use std::time::Duration;

use futures::future::BoxFuture;
use futures::FutureExt as _;
use sqlx::pool::PoolConnection;
use sqlx::postgres::{PgConnection, PgPool, Postgres};
use sqlx::Connection as _;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::{debug, info, warn};

use super::lock_keys::AdvisoryLockKey;

/// How long the very first leadership attempt waits after
/// [`spawn_singleton_task`] is called, before falling back to the caller's
/// `interval` for every attempt after. Short on purpose — mirrors
/// `src/observability/reporter.rs`'s heartbeat loop, so a freshly started
/// replica does not sit idle for a whole `interval` before making its first
/// bid for leadership.
const FIRST_ATTEMPT_DELAY: Duration = Duration::from_millis(100);

/// What `body` is handed each time it runs as leader.
///
/// `conn` is the single PostgreSQL session currently holding the advisory
/// lock. Every statement `body` issues through it runs on that same session
/// — so a caller that also needs the lock and a write to be atomic (the
/// Stage C `Grace.ExtendLeases` case: extend every lease in one statement,
/// under the same session that is proving only one replica is doing it) gets
/// that for free, without asking the pool for a second connection.
pub struct LeaderContext<'a> {
    pub conn: &'a mut PgConnection,
}

/// A `body` callback for [`spawn_singleton_task`].
///
/// Written as a plain function pointer/closure bound rather than a trait so
/// callers do not need `async-trait`: `|ctx| Box::pin(async move { .. })`.
pub trait SingletonTaskBody:
    for<'a> Fn(LeaderContext<'a>) -> BoxFuture<'a, ()> + Send + Sync + 'static
{
}

impl<F> SingletonTaskBody for F where
    F: for<'a> Fn(LeaderContext<'a>) -> BoxFuture<'a, ()> + Send + Sync + 'static
{
}

/// Handle to a running [`spawn_singleton_task`] loop.
///
/// Dropping this without calling [`shutdown`](Self::shutdown) leaves the
/// background task running detached — it will keep polling for leadership
/// (and release the lock only when the process exits or the connection
/// otherwise drops) until the process itself ends. Always call `shutdown`
/// during graceful shutdown so a currently-leading replica releases the lock
/// promptly instead of making the next election wait out this process's own
/// connection eventually timing out on the server side.
pub struct SingletonTaskHandle {
    shutdown_tx: watch::Sender<bool>,
    join: JoinHandle<()>,
}

impl SingletonTaskHandle {
    /// Requests the loop stop, releases the advisory lock if this replica is
    /// currently leader, and waits for the background task to exit.
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(true);
        if let Err(err) = self.join.await {
            warn!(error = %err, "pg singleton task join failed");
        }
    }
}

/// Runs `body` on a fixed `interval`, but only on whichever `--role api`
/// replica currently holds `key`'s session-scoped advisory lock.
///
/// Every replica is expected to call this with the same `key` and the same
/// `interval`; which one actually runs `body` on any given tick is decided
/// entirely by PostgreSQL. A replica that is not leader spends each tick on
/// one cheap `pg_try_advisory_lock` call and nothing else.
///
/// `body` runs on the pool connection this function is holding the lock on
/// (see [`LeaderContext`]), which stays checked out of `pool` for as long as
/// this replica remains leader — potentially the rest of the process's
/// lifetime. Size `pool`'s `max_connections` with at least one spare
/// connection beyond however many concurrent `spawn_singleton_task` loops
/// share it: a follower's `pg_try_advisory_lock` attempt needs a connection
/// of its own for the moment it takes to run, and a pool with none free
/// cannot compete for leadership at all.
pub fn spawn_singleton_task<B>(
    pool: PgPool,
    key: AdvisoryLockKey,
    interval: Duration,
    body: B,
) -> SingletonTaskHandle
where
    B: SingletonTaskBody,
{
    spawn_singleton_task_raw(pool, key.as_i64(), interval, body)
}

/// [`spawn_singleton_task`]'s implementation, over a raw key.
///
/// `pub(crate)` rather than folded into the public function so this crate's
/// own tests can exercise the election mechanism — concurrent acquisition,
/// failover, shutdown release — with fresh, collision-proof keys per test
/// case, without touching [`AdvisoryLockKey`]'s closed, centrally-managed set
/// of real production variants. External callers only ever reach this
/// through [`spawn_singleton_task`], so a real key can never be bypassed.
pub(crate) fn spawn_singleton_task_raw<B>(
    pool: PgPool,
    key: i64,
    interval: Duration,
    body: B,
) -> SingletonTaskHandle
where
    B: SingletonTaskBody,
{
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

    let join = tokio::spawn(async move {
        let mut leader: Option<PoolConnection<Postgres>> = None;
        let mut wait = FIRST_ATTEMPT_DELAY;

        loop {
            tokio::select! {
                _ = sleep(wait) => {}
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        release(leader.take(), key).await;
                        debug!(key, "pg singleton task loop stopping");
                        return;
                    }
                }
            }
            wait = interval;

            if let Some(conn) = leader.as_mut() {
                // Reentrant would be wrong here: re-issuing
                // `pg_try_advisory_lock` on the same session that already
                // holds it would succeed and increment PostgreSQL's
                // per-session hold count for that key, requiring a matching
                // extra unlock to fully release. A cheap ping instead
                // confirms the session — and with it, the lock — is still
                // alive without touching that count.
                if let Err(err) = conn.ping().await {
                    warn!(
                        key,
                        error = %err,
                        "pg singleton task's leader connection died; rejoining the election"
                    );
                    leader = None;
                    continue;
                }
            } else {
                match try_acquire(&pool, key).await {
                    Ok(Some(conn)) => {
                        info!(key, "acquired pg singleton task leadership");
                        leader = Some(conn);
                    }
                    Ok(None) => continue,
                    Err(err) => {
                        warn!(
                            key,
                            error = %err,
                            "pg singleton task could not attempt to acquire its advisory lock \
                             this round"
                        );
                        continue;
                    }
                }
            }

            let Some(conn) = leader.as_mut() else {
                continue;
            };
            // 🔴 Caught, not left to unwind through this task. An unwind would
            // drop `leader` on the way out, and `PoolConnection::drop` returns
            // the connection to `pool` — silently, still holding the advisory
            // lock, for some other borrower to inherit still locked. `release`
            // below documents the same hazard for the ordinary unlock-failure
            // path; a panic just reaches it a different way.
            let outcome = AssertUnwindSafe(body(LeaderContext { conn }))
                .catch_unwind()
                .await;
            if let Err(payload) = outcome {
                warn!(
                    key,
                    panic = %panic_message(&payload),
                    "pg singleton task body panicked; closing the leader connection instead of \
                     returning it to the pool with the advisory lock still held"
                );
                // Close outright rather than routing through `release`'s
                // ordinary `pg_advisory_unlock` attempt: a panic mid-`body`
                // gives no guarantee the session (or this key's hold count,
                // if `body` itself took other locks before panicking) is in a
                // state where one unlock call is enough to fully release it.
                // Closing the connection releases everything it held,
                // unconditionally.
                if let Some(conn) = leader.take() {
                    let _ = conn.close().await;
                }
            }
        }
    });

    SingletonTaskHandle { shutdown_tx, join }
}

/// Best-effort text for a caught `body` panic payload, for the log line only
/// — never propagated, since the payload itself is not `Send`-safe to hold
/// past this point in every case `std::panic::catch_unwind` allows.
fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "<non-string panic payload>".to_string()
    }
}

async fn try_acquire(
    pool: &PgPool,
    key: i64,
) -> Result<Option<PoolConnection<Postgres>>, sqlx::Error> {
    let mut conn = pool.acquire().await?;
    let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
        .bind(key)
        .fetch_one(&mut *conn)
        .await?;
    Ok(if acquired { Some(conn) } else { None })
}

/// Best-effort release: unlock explicitly when the session is still healthy,
/// and force-close rather than return to `pool` when it is not — a
/// connection returned to the pool while this call cannot confirm the lock
/// was released is a connection some other borrower could inherit still
/// holding it.
async fn release(conn: Option<PoolConnection<Postgres>>, key: i64) {
    let Some(mut conn) = conn else {
        return;
    };
    match sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(key)
        .execute(&mut *conn)
        .await
    {
        Ok(_) => debug!(key, "released pg singleton task advisory lock"),
        Err(err) => {
            warn!(
                key,
                error = %err,
                "failed to release pg singleton task advisory lock; closing the connection \
                 instead of returning it to the pool"
            );
            let _ = conn.close().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::sync::Notify;
    use tokio::time::timeout;

    use super::*;
    use crate::pg::harness::{next_test_lock_key, pool_or_skip};

    const TICK: Duration = Duration::from_millis(50);

    /// Two loops competing for the same key on two independent pools — the
    /// same shape two `--role api` replicas would be, each with its own
    /// connections to one database. Only one may ever see itself run `body`
    /// while the other is still up.
    #[tokio::test]
    async fn only_one_competitor_leads_at_once() {
        let pool_a = pool_or_skip!("only_one_competitor_leads_at_once");
        let pool_b = pool_or_skip!("only_one_competitor_leads_at_once");
        let key = next_test_lock_key();

        let leader_a = Arc::new(AtomicUsize::new(0));
        let leader_b = Arc::new(AtomicUsize::new(0));
        let a = Arc::clone(&leader_a);
        let b = Arc::clone(&leader_b);

        let handle_a = spawn_singleton_task_raw(pool_a, key, TICK, move |_ctx| {
            let a = Arc::clone(&a);
            Box::pin(async move {
                a.fetch_add(1, Ordering::SeqCst);
            })
        });
        let handle_b = spawn_singleton_task_raw(pool_b, key, TICK, move |_ctx| {
            let b = Arc::clone(&b);
            Box::pin(async move {
                b.fetch_add(1, Ordering::SeqCst);
            })
        });

        tokio::time::sleep(TICK * 10).await;

        let ran_a = leader_a.load(Ordering::SeqCst);
        let ran_b = leader_b.load(Ordering::SeqCst);
        assert!(
            (ran_a > 0) ^ (ran_b > 0),
            "exactly one competitor should have led; a={ran_a} b={ran_b}"
        );
        assert!(
            ran_a > 1 || ran_b > 1,
            "the leader should run body more than once; a={ran_a} b={ran_b}"
        );

        handle_a.shutdown().await;
        handle_b.shutdown().await;
    }

    /// Shutting the leader down has to free the lock promptly — not leave the
    /// follower waiting out a server-side TCP timeout — because
    /// `SingletonTaskHandle::shutdown` explicitly unlocks before its future
    /// resolves.
    #[tokio::test]
    async fn shutdown_releases_the_lock_for_the_next_leader() {
        let pool_a = pool_or_skip!("shutdown_releases_the_lock_for_the_next_leader");
        let pool_b = pool_or_skip!("shutdown_releases_the_lock_for_the_next_leader");
        let key = next_test_lock_key();

        let became_leader = Arc::new(Notify::new());
        let notify = Arc::clone(&became_leader);
        let handle_a = spawn_singleton_task_raw(pool_a, key, TICK, move |_ctx| {
            let notify = Arc::clone(&notify);
            Box::pin(async move {
                notify.notify_one();
            })
        });
        timeout(Duration::from_secs(5), became_leader.notified())
            .await
            .expect("the first competitor should acquire leadership");

        let became_leader_b = Arc::new(Notify::new());
        let notify_b = Arc::clone(&became_leader_b);
        let handle_b = spawn_singleton_task_raw(pool_b, key, TICK, move |_ctx| {
            let notify_b = Arc::clone(&notify_b);
            Box::pin(async move {
                notify_b.notify_one();
            })
        });

        // b cannot have led yet: a is still up and holding the lock.
        tokio::time::sleep(TICK * 3).await;

        handle_a.shutdown().await;

        timeout(Duration::from_secs(5), became_leader_b.notified())
            .await
            .expect(
                "the second competitor should take over promptly after shutdown releases the lock",
            );

        handle_b.shutdown().await;
    }

    /// A follower that never becomes leader must never run `body` at all —
    /// the other half of "only run body once you hold the lock".
    #[tokio::test]
    async fn a_follower_never_runs_body() {
        let pool_a = pool_or_skip!("a_follower_never_runs_body");
        let pool_b = pool_or_skip!("a_follower_never_runs_body");
        let key = next_test_lock_key();

        // a takes the lock and holds it for the whole test by never handing
        // control back to the runtime inside body — simplest way to pin
        // leadership on a for the test's duration is just to keep it alive
        // and let its own frequent ticks keep winning the ping check.
        let handle_a = spawn_singleton_task_raw(pool_a, key, TICK, |_ctx| Box::pin(async {}));
        tokio::time::sleep(TICK * 3).await;

        let ran_b = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&ran_b);
        let handle_b = spawn_singleton_task_raw(pool_b, key, TICK, move |_ctx| {
            let counter = Arc::clone(&counter);
            Box::pin(async move {
                counter.fetch_add(1, Ordering::SeqCst);
            })
        });

        tokio::time::sleep(TICK * 10).await;
        assert_eq!(
            ran_b.load(Ordering::SeqCst),
            0,
            "b never held the lock and must never have run body"
        );

        handle_a.shutdown().await;
        handle_b.shutdown().await;
    }

    /// `body` panicking must not leave the advisory lock stranded on a
    /// connection that goes back to the pool still holding it — the T1-3
    /// hazard: an uncaught unwind would drop `leader`, and
    /// `PoolConnection::drop` returns the connection to the pool as-is,
    /// lock and all, for the next borrower to inherit still locked.
    ///
    /// `a`'s body panics on its very first run; the test then shuts `a` down
    /// (without waiting out any further ticks, so `a` gets no chance to
    /// re-contest) and confirms `b` can still take over — proving the panic
    /// path itself released the lock rather than shutdown's own release.
    #[tokio::test]
    async fn a_panicking_body_releases_the_lock_for_another_competitor() {
        let pool_a = pool_or_skip!("a_panicking_body_releases_the_lock_for_another_competitor");
        let pool_b = pool_or_skip!("a_panicking_body_releases_the_lock_for_another_competitor");
        let key = next_test_lock_key();

        let led_then_panicked = Arc::new(Notify::new());
        let notify = Arc::clone(&led_then_panicked);
        let handle_a = spawn_singleton_task_raw(pool_a, key, TICK, move |_ctx| {
            let notify = Arc::clone(&notify);
            Box::pin(async move {
                notify.notify_one();
                panic!("intentional panic exercising the singleton task's panic-recovery path");
            })
        });
        timeout(Duration::from_secs(5), led_then_panicked.notified())
            .await
            .expect("a should acquire leadership and run body before panicking");

        // Shut a down immediately so it cannot win a re-acquisition race
        // against b before b even starts — isolating that the panic path
        // itself, not shutdown's own release, freed the lock.
        handle_a.shutdown().await;

        let became_leader_b = Arc::new(Notify::new());
        let notify_b = Arc::clone(&became_leader_b);
        let handle_b = spawn_singleton_task_raw(pool_b, key, TICK, move |_ctx| {
            let notify_b = Arc::clone(&notify_b);
            Box::pin(async move {
                notify_b.notify_one();
            })
        });

        timeout(Duration::from_secs(5), became_leader_b.notified())
            .await
            .expect("a competitor should take over after the previous leader's body panicked");

        handle_b.shutdown().await;
    }

    /// The other half of the design's advantage over a Redis-style TTL lock:
    /// the lock is tied to the leader's actual PostgreSQL session, so killing
    /// that session out from under it — not asking it to shut down — must
    /// still free the lock for another competitor, with no lease to wait out.
    #[tokio::test]
    async fn killing_the_leaders_backend_lets_another_competitor_take_over() {
        let pool_a = pool_or_skip!("killing_the_leaders_backend_lets_another_competitor_take_over");
        let pool_b = pool_or_skip!("killing_the_leaders_backend_lets_another_competitor_take_over");
        // A third, throwaway pool used only to issue `pg_terminate_backend`
        // and then probe the lock directly, from a session that isn't the
        // one being killed.
        let pool_probe =
            pool_or_skip!("killing_the_leaders_backend_lets_another_competitor_take_over");
        let key = next_test_lock_key();

        let leader_pid = Arc::new(tokio::sync::Mutex::new(None::<i32>));
        let pid_slot = Arc::clone(&leader_pid);
        let handle_a = spawn_singleton_task_raw(pool_a, key, TICK, move |ctx| {
            let pid_slot = Arc::clone(&pid_slot);
            Box::pin(async move {
                let mut slot = pid_slot.lock().await;
                if slot.is_none() {
                    if let Ok(pid) = sqlx::query_scalar::<_, i32>("SELECT pg_backend_pid()")
                        .fetch_one(&mut *ctx.conn)
                        .await
                    {
                        *slot = Some(pid);
                    }
                }
            })
        });

        let pid = timeout(Duration::from_secs(5), async {
            loop {
                if let Some(pid) = *leader_pid.lock().await {
                    return pid;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("a should acquire leadership and report its backend pid");

        let mut probe = pool_probe
            .acquire()
            .await
            .expect("acquiring a probe connection should succeed");
        sqlx::query("SELECT pg_terminate_backend($1)")
            .bind(pid)
            .execute(&mut *probe)
            .await
            .expect("terminating the leader's backend should succeed");

        // The core claim under test: PostgreSQL released the session-scoped
        // advisory lock the instant the leader's backend died — no lease, no
        // TTL, nothing for anyone to wait out. Proved directly here, on a
        // probe session distinct from both a and b, and independent of
        // either of their own polling loops' timing — `pg_terminate_backend`
        // returning does not guarantee the backend has fully exited yet, so
        // this polls briefly rather than asserting on the very first try.
        let lock_and_unlock = timeout(Duration::from_secs(5), async {
            loop {
                let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
                    .bind(key)
                    .fetch_one(&mut *probe)
                    .await
                    .expect("pg_try_advisory_lock should succeed");
                if acquired {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        lock_and_unlock
            .expect("the lock should become acquirable promptly once the leader's backend is dead");
        sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(key)
            .execute(&mut *probe)
            .await
            .expect("releasing the probe's advisory lock should succeed");
        drop(probe);

        // Stop a's own loop so it cannot race b for re-acquisition below —
        // the mechanism under test was already proved directly above; this
        // just isolates b's takeover from a fresh, unrelated race between a
        // and b's independent polling intervals.
        handle_a.shutdown().await;

        let became_leader_b = Arc::new(Notify::new());
        let notify_b = Arc::clone(&became_leader_b);
        let handle_b = spawn_singleton_task_raw(pool_b, key, TICK, move |_ctx| {
            let notify_b = Arc::clone(&notify_b);
            Box::pin(async move {
                notify_b.notify_one();
            })
        });

        timeout(Duration::from_secs(5), became_leader_b.notified())
            .await
            .expect("b should take over the lock once a is no longer contesting for it");

        handle_b.shutdown().await;
    }
}
