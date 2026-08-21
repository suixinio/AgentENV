//! The per-sandbox distributed lock.
//!
//! # 🔴 The lock does not carry correctness
//!
//! It buys two things: throughput under contention, and the guarantee that a
//! caller's synchronous callback runs exactly once. Correctness is carried by
//! the `rev` and `execution_id` predicates inside the write script.
//!
//! Turn this lock off entirely and the store still cannot write bad data — it
//! will simply return `ConcurrentUpdate` a great deal more often under
//! contention. That is not a hedge, it is the property that makes the two
//! failure modes distinguishable: removing the lock changes the *rate* of lost
//! races, and removing the predicates changes whether racing writes are lost at
//! all.
//!
//! Consequently there is no Redlock here. Multi-instance Redlock's correctness
//! is contested, and buying a contested guarantee for a component that does not
//! rely on the guarantee would be paying for nothing.

use std::time::{Duration, Instant};

use rand::RngExt;
use redis::AsyncCommands;
use tracing::debug;
use uuid::Uuid;

use super::super::{Result, StoreError};
use super::config::RedisStoreConfig;
use super::scripts;
use crate::types::SandboxId;

/// A held lock. Releasing is explicit; dropping one without releasing leaves
/// it to expire, which is correct but slow.
#[derive(Debug)]
pub struct SandboxLock {
    key: String,
    token: String,
    acquired_at: Instant,
    ttl: Duration,
}

impl SandboxLock {
    pub fn held_for(&self) -> Duration {
        self.acquired_at.elapsed()
    }

    /// 🔴 How long the lock is still good for, used to decide whether to
    /// attempt a write at all.
    ///
    /// There is deliberately no watchdog renewing this in the background. A
    /// renewal would convert "this operation took suspiciously long" from an
    /// error somebody sees into a long silent hold nobody does.
    pub fn remaining(&self) -> Duration {
        self.ttl.saturating_sub(self.acquired_at.elapsed())
    }
}

pub struct LockManager {
    connection: redis::aio::ConnectionManager,
    config: RedisStoreConfig,
}

impl LockManager {
    pub fn new(connection: redis::aio::ConnectionManager, config: RedisStoreConfig) -> Self {
        Self { connection, config }
    }

    /// Takes the lock, waiting up to `lock_wait` for it.
    ///
    /// `wake` is awaited between attempts so that a release publishes its way
    /// to the next holder instead of everybody polling.
    pub async fn acquire(
        &self,
        sandbox_id: &SandboxId,
        key: String,
        wake: &mut tokio::sync::broadcast::Receiver<()>,
    ) -> Result<SandboxLock> {
        let token = Uuid::now_v7().to_string();
        let deadline = Instant::now() + self.config.lock_wait;
        let mut backoff = self.config.lock_retry_min;

        loop {
            let mut connection = self.connection.clone();
            let acquired: Option<String> = connection
                .set_options(
                    &key,
                    &token,
                    redis::SetOptions::default()
                        .conditional_set(redis::ExistenceCheck::NX)
                        .with_expiration(redis::SetExpiry::PX(
                            self.config.lock_ttl.as_millis() as u64
                        )),
                )
                .await
                .map_err(backend)?;

            if acquired.is_some() {
                return Ok(SandboxLock {
                    key,
                    token,
                    acquired_at: Instant::now(),
                    ttl: self.config.lock_ttl,
                });
            }

            let now = Instant::now();
            if now >= deadline {
                debug!(
                    sandbox_id = %sandbox_id,
                    waited = ?self.config.lock_wait,
                    "gave up waiting for the sandbox lock"
                );
                return Err(StoreError::ConcurrentUpdate {
                    sandbox_id: *sandbox_id,
                });
            }

            let sleep_for = jitter(backoff, self.config.lock_retry_jitter).min(deadline - now);
            tokio::select! {
                _ = wake.recv() => {}
                _ = tokio::time::sleep(sleep_for) => {}
            }
            backoff = (backoff * 2).min(self.config.lock_retry_max);
        }
    }

    /// Releases the lock if this holder still owns it.
    ///
    /// 🔴 Token-compared inside a script. A bare `DEL` would release whatever
    /// lock is present, which after a TTL lapse is the *next* holder's.
    pub async fn release(&self, lock: SandboxLock) -> Result<bool> {
        let mut connection = self.connection.clone();
        let removed: i64 = scripts::release_lock()
            .key(&lock.key)
            .arg(&lock.token)
            .invoke_async(&mut connection)
            .await
            .map_err(backend)?;
        Ok(removed == 1)
    }
}

fn jitter(base: Duration, fraction: f64) -> Duration {
    if fraction <= 0.0 {
        return base;
    }
    let base_secs = base.as_secs_f64();
    let spread = base_secs * fraction;
    let sampled = rand::rng().random_range((base_secs - spread)..=(base_secs + spread));
    Duration::from_secs_f64(sampled.max(0.0))
}

fn backend(source: redis::RedisError) -> StoreError {
    StoreError::Backend {
        source: anyhow::Error::from(source),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jitter_stays_within_its_band() {
        let base = Duration::from_millis(200);
        for _ in 0..1_000 {
            let sampled = jitter(base, 0.25);
            assert!(sampled >= Duration::from_millis(150), "{sampled:?}");
            assert!(sampled <= Duration::from_millis(250), "{sampled:?}");
        }
    }

    #[test]
    fn zero_jitter_is_the_identity() {
        assert_eq!(
            jitter(Duration::from_millis(200), 0.0),
            Duration::from_millis(200)
        );
    }
}
