//! Cluster-singleton tasks using session-scoped PostgreSQL advisory locks.
//! Leadership lasts exactly as long as the holding session, so crashes and
//! network loss release it without a lease-renewal window.

use std::panic::AssertUnwindSafe;
use std::time::Duration;

use futures::future::BoxFuture;
use futures::FutureExt as _;
use sqlx::pool::PoolConnection;
use sqlx::postgres::{PgConnection, PgPool, Postgres};
use sqlx::Connection as _;
use tokio::sync::watch;
use tokio::time::sleep;
use tracing::{debug, info, warn};

use super::lock_keys::AdvisoryLockKey;

const FIRST_ATTEMPT_DELAY: Duration = Duration::from_millis(100);

/// Leader callback context carrying the lock-holding session.
pub struct LeaderContext<'a> {
    pub conn: &'a mut PgConnection,
}

/// Callback invoked by [`spawn_singleton_task`] while leadership is held.
pub trait SingletonTaskBody:
    for<'a> Fn(LeaderContext<'a>) -> BoxFuture<'a, ()> + Send + Sync + 'static
{
}

impl<F> SingletonTaskBody for F where
    F: for<'a> Fn(LeaderContext<'a>) -> BoxFuture<'a, ()> + Send + Sync + 'static
{
}

/// Handle for a singleton loop; callers must shut it down to release promptly.
///
/// Shutdown waits for an in-flight body, so bodies must bound their own runtime.
pub use crate::leader_task::LeaderTaskHandle as SingletonTaskHandle;

/// Runs `body` at `interval` on the replica holding `key`.
///
/// The pool needs one connection per leader plus capacity for contenders.
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

/// Raw-key implementation used by collision-free election tests.
pub fn spawn_singleton_task_raw<B>(
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
                // Reacquiring on the same session increments a reentrant hold count;
                // ping instead to verify that the lock-holding session is alive.
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
            // Catch panics so the lock-holding connection is never returned to the pool.
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
                // A panicked body leaves lock state uncertain; closing releases every lock.
                if let Some(conn) = leader.take() {
                    let _ = conn.close().await;
                }
            }
        }
    });

    SingletonTaskHandle::new(shutdown_tx, join)
}

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

// Unlock explicitly, or close rather than pool a session with uncertain lock state.
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

        tokio::time::sleep(TICK * 3).await;

        handle_a.shutdown().await;

        timeout(Duration::from_secs(5), became_leader_b.notified())
            .await
            .expect(
                "the second competitor should take over promptly after shutdown releases the lock",
            );

        handle_b.shutdown().await;
    }

    #[tokio::test]
    async fn a_follower_never_runs_body() {
        let pool_a = pool_or_skip!("a_follower_never_runs_body");
        let pool_b = pool_or_skip!("a_follower_never_runs_body");
        let key = next_test_lock_key();

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

    #[tokio::test]
    async fn a_panicking_body_releases_the_lock_for_another_competitor() {
        let pool_a = pool_or_skip!("a_panicking_body_releases_the_lock_for_another_competitor");
        let pool_b = pool_or_skip!("a_panicking_body_releases_the_lock_for_another_competitor");
        let key = next_test_lock_key();

        let led_then_panicked = Arc::new(Notify::new());
        let notify = Arc::clone(&led_then_panicked);
        let handle_a = spawn_singleton_task_raw(pool_a.clone(), key, TICK, move |_ctx| {
            let notify = Arc::clone(&notify);
            Box::pin(async move {
                notify.notify_one();
                panic!("intentional panic exercising the singleton task's panic-recovery path");
            })
        });
        timeout(Duration::from_secs(5), led_then_panicked.notified())
            .await
            .expect("a should acquire leadership and run body before panicking");

        // Keep `pool_a` alive so pool teardown cannot hide a leaked lock.
        handle_a.shutdown().await;

        // Probe through `pool_b` to avoid reentrancy on the leaked session.
        let mut probe = pool_b
            .acquire()
            .await
            .expect("acquiring a probe connection should succeed");
        let still_held: bool = sqlx::query_scalar("SELECT NOT pg_try_advisory_lock($1)")
            .bind(key)
            .fetch_one(&mut *probe)
            .await
            .expect("pg_try_advisory_lock should succeed");
        assert!(
            !still_held,
            "the advisory lock is still held after the leader's body panicked and its task shut \
             down — the panic path leaked the lock instead of releasing it"
        );
        sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(key)
            .execute(&mut *probe)
            .await
            .expect("releasing the probe's advisory lock should succeed");
        drop(probe);

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

    #[tokio::test]
    async fn killing_the_leaders_backend_lets_another_competitor_take_over() {
        let pool_a = pool_or_skip!("killing_the_leaders_backend_lets_another_competitor_take_over");
        let pool_b = pool_or_skip!("killing_the_leaders_backend_lets_another_competitor_take_over");
        // Use a distinct session to terminate and probe the leader.
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

        // Backend termination is asynchronous, so poll briefly for lock release.
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

        // Stop `a` before testing `b`'s takeover.
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
