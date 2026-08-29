//! The Redis-backed metadata store: the cluster's authoritative active state.
//!
//! # What carries correctness here
//!
//! > The lock buys throughput and the guarantee that a caller's callback runs
//! > exactly once. Correctness is carried by the `rev` and `execution_id`
//! > predicates inside the write scripts. Turn the lock off and this store
//! > still cannot write bad data — it will simply return `ConcurrentUpdate`
//! > under contention.
//!
//! That sentence is the design. Everything else in this module follows from it,
//! including why the lock's TTL is short, why there is no watchdog renewing it,
//! and why a slow callback is an error rather than something to retry.
//!
//! # Crash recovery
//!
//! Not the lock. A lock is released before any wait, so it never spans a whole
//! operation. What spans an operation is the transition key, and its TTL only
//! lets the *next* operation start — it does not repair the state a dead
//! replica left behind.
//!
//! e2b closes that gap with a branch in its expiry sweep that lets a sandbox
//! stuck in a transitional state through to eviction after a stale cutoff.
//! 🔴 That branch cannot close it for us: our `expires_at` is optional, and a
//! sandbox with `timeout = None` is not in the expiry index at all. Such a
//! sandbox, stuck in `Pausing` because the replica that was pausing it died,
//! is in no index any process ever reads. A user sees a sandbox that cannot be
//! deleted. Hence the fourth piece e2b does not have: a transition index and a
//! reaper over it, in [`transition`].
//!
//! # 🔴 Nothing constructs this store yet
//!
//! It is exercised only by its own tests. Code that nothing drives is code that
//! has not been shown to be right — three methods delivered ahead of their
//! driver earlier in this programme passed review as harmless and turned out to
//! hold three real defects the moment something called them. Treat everything
//! here as unverified until the assembly point exists, and see the audit list
//! in the design document before wiring it up.

mod config;
mod crud;
mod expiry;
mod keys;
mod lock;
mod notify;
mod record;
mod reserve;
mod scripts;
mod transition;

#[cfg(test)]
pub(crate) mod harness;
#[cfg(test)]
mod tests;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use tokio::sync::RwLock;

use super::{Result, SandboxMetadata, StoreError};
use crate::types::SandboxId;

pub use config::{RedisStoreConfig, RedisStoreConfigError, DEFAULT_KEY_PREFIX};
pub use record::{ActiveStateRecord, PausedStateRef, StoredSandboxRecord, RECORD_VERSION};

use keys::KeySpace;
use lock::LockManager;
use notify::Notifier;

/// A sampled listing, and the instant it was sampled at.
///
/// 🔴 The instant travels with the sample on purpose. Without it an operator
/// comparing two replicas' readings has no way to know they are comparing two
/// different moments, and will read the difference as drift.
struct ListingSample {
    sampled_at: Instant,
    records: Arc<Vec<SandboxMetadata>>,
}

/// Whether a background round may run.
///
/// 🔴 Three answers, and each is reported separately. "This round did nothing"
/// reads identically whether the task is switched off, still warming up, or ran
/// and found nothing to do — and those are three very different facts to be
/// looking at during an incident.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RoundReadiness {
    Disabled,
    WarmingUp,
    Ready,
}

/// Holds a background task down for its first interval.
///
/// 🔴 A restart makes every clock in the store look stale at once — not because
/// anything stopped, but because nobody was listening. A reaper with no warm-up
/// reads that as "everything is stuck" and acts on all of it in one round. The
/// healer's `heal_grace` does not cover this: that one is per *record*, and the
/// hazard here is per *process*.
///
/// The window is re-armed when the kill switch goes off and back on, because
/// switching a task back on has exactly the same shape as starting it.
struct WarmUp {
    window: Duration,
    armed_at: Mutex<Instant>,
    enabled: AtomicBool,
}

impl WarmUp {
    fn new(window: Duration) -> Self {
        Self {
            window,
            armed_at: Mutex::new(Instant::now()),
            enabled: AtomicBool::new(true),
        }
    }

    fn readiness(&self, enabled: bool) -> RoundReadiness {
        let was_enabled = self.enabled.swap(enabled, Ordering::Relaxed);
        let mut armed_at = self
            .armed_at
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if enabled && !was_enabled {
            *armed_at = Instant::now();
        }
        if !enabled {
            return RoundReadiness::Disabled;
        }
        if armed_at.elapsed() < self.window {
            return RoundReadiness::WarmingUp;
        }
        RoundReadiness::Ready
    }
}

pub struct StoreInner {
    connection: redis::aio::ConnectionManager,
    keys: KeySpace,
    config: RedisStoreConfig,
    locks: LockManager,
    notifier: Notifier,
    /// Memo for the aggregate listing path only. See
    /// [`RedisMetadataStore::last_listing_sample_at`].
    listing_memo: RwLock<Option<ListingSample>>,
    healer_warmup: WarmUp,
    reaper_warmup: WarmUp,
}

/// The cluster's active-state store.
pub struct RedisMetadataStore {
    inner: Arc<StoreInner>,
}

impl RedisMetadataStore {
    /// Connects and validates the configuration.
    pub async fn connect(config: RedisStoreConfig) -> Result<Self> {
        config.validate().map_err(|source| StoreError::Backend {
            source: anyhow::Error::from(source),
        })?;

        let client = redis::Client::open(config.url.as_str()).map_err(backend)?;
        let manager_config = redis::aio::ConnectionManagerConfig::new()
            .set_response_timeout(Some(config.response_timeout))
            .set_connection_timeout(Some(config.connect_timeout));
        let connection =
            redis::aio::ConnectionManager::new_with_config(client.clone(), manager_config)
                .await
                .map_err(backend)?;

        let keys = KeySpace::new(config.key_prefix.clone());
        let notifier = Notifier::start(client, connection.clone(), keys.notify_channel());
        let locks = LockManager::new(connection.clone(), config.clone());

        let healer_warmup = WarmUp::new(config.heal_interval);
        let reaper_warmup = WarmUp::new(config.reap_interval);

        Ok(Self {
            inner: Arc::new(StoreInner {
                connection,
                keys,
                config,
                locks,
                notifier,
                listing_memo: RwLock::new(None),
                healer_warmup,
                reaper_warmup,
            }),
        })
    }

    /// When the memoised listing was last refreshed.
    ///
    /// 🔴 A sample, not a point-in-time truth. `metrics_snapshot` runs on every
    /// Prometheus scrape of every replica, and answering it honestly would mean
    /// `SMEMBERS` plus an `MGET` of the entire keyspace per scrape per replica.
    /// The memo makes that affordable and makes the answer approximate, and the
    /// second half of that sentence is not optional to report.
    pub async fn last_listing_sample_at(&self) -> Option<Instant> {
        self.inner
            .listing_memo
            .read()
            .await
            .as_ref()
            .map(|sample| sample.sampled_at)
    }

    pub fn inner(&self) -> &Arc<StoreInner> {
        &self.inner
    }

    /// Deletes every key this store owns. Test support only.
    #[cfg(test)]
    pub async fn flush_namespace(&self) -> Result<()> {
        let mut connection = self.inner.connection.clone();
        let pattern = format!("{}:*", self.inner.config.key_prefix);
        let keys: Vec<String> = redis::cmd("KEYS")
            .arg(&pattern)
            .query_async(&mut connection)
            .await
            .map_err(backend)?;
        if !keys.is_empty() {
            let _: () = redis::cmd("DEL")
                .arg(&keys)
                .query_async(&mut connection)
                .await
                .map_err(backend)?;
        }
        Ok(())
    }
}

impl StoreInner {
    pub fn connection(&self) -> redis::aio::ConnectionManager {
        self.connection.clone()
    }

    pub fn keys(&self) -> &KeySpace {
        &self.keys
    }

    pub fn config(&self) -> &RedisStoreConfig {
        &self.config
    }

    /// The write script, or its predicate-free control in test builds that have
    /// asked for it.
    pub fn update_script(&self) -> &'static redis::Script {
        #[cfg(test)]
        if !self.config.cas_predicates_enabled() {
            return scripts::update_without_predicates();
        }
        scripts::update()
    }

    pub async fn read_record(&self, sandbox_id: &SandboxId) -> Result<Option<StoredSandboxRecord>> {
        let mut connection = self.connection();
        let raw: Option<Vec<u8>> = redis::cmd("GET")
            .arg(self.keys.record(sandbox_id))
            .query_async(&mut connection)
            .await
            .map_err(backend)?;
        raw.as_deref().map(StoredSandboxRecord::decode).transpose()
    }

    pub async fn require_record(&self, sandbox_id: &SandboxId) -> Result<StoredSandboxRecord> {
        self.read_record(sandbox_id)
            .await?
            .ok_or(StoreError::SandboxNotFound {
                sandbox_id: *sandbox_id,
            })
    }

    pub async fn notify(&self, routing_key: &str) {
        self.notifier.publish(routing_key).await;
    }

    pub fn subscribe(&self, routing_key: &str) -> tokio::sync::broadcast::Receiver<()> {
        self.notifier.subscribe(routing_key)
    }

    pub fn locks(&self) -> &LockManager {
        &self.locks
    }

    pub async fn invalidate_listing_memo(&self) {
        *self.listing_memo.write().await = None;
    }

    pub async fn memoised_listing(&self) -> Option<Arc<Vec<SandboxMetadata>>> {
        let memo = self.listing_memo.read().await;
        let sample = memo.as_ref()?;
        (sample.sampled_at.elapsed() < self.config.metrics_memo_ttl)
            .then(|| Arc::clone(&sample.records))
    }

    pub fn healer_readiness(&self) -> RoundReadiness {
        self.healer_warmup
            .readiness(self.config.expiry_healer_enabled)
    }

    pub fn reaper_readiness(&self) -> RoundReadiness {
        self.reaper_warmup
            .readiness(self.config.transition_reaper_enabled)
    }

    #[cfg(test)]
    pub fn skip_background_warmup(&self) {
        for warmup in [&self.healer_warmup, &self.reaper_warmup] {
            *warmup
                .armed_at
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                Instant::now() - Duration::from_secs(86_400);
        }
    }

    pub async fn store_listing_memo(
        &self,
        records: Vec<SandboxMetadata>,
    ) -> Arc<Vec<SandboxMetadata>> {
        let records = Arc::new(records);
        *self.listing_memo.write().await = Some(ListingSample {
            sampled_at: Instant::now(),
            records: Arc::clone(&records),
        });
        records
    }
}

/// Wraps a Redis transport failure.
///
/// 🔴 Always an error, never an absence. A store this call could not reach has
/// not told us that a sandbox does not exist; it has told us nothing. Callers
/// answer absence by deleting local artifacts and tearing down running VMs.
pub fn backend(source: redis::RedisError) -> StoreError {
    StoreError::Backend {
        source: anyhow::Error::from(source),
    }
}

pub fn backend_msg(message: impl Into<String>) -> StoreError {
    StoreError::Backend {
        source: anyhow::anyhow!(message.into()),
    }
}

pub fn now_millis(now: SystemTime) -> i64 {
    record::to_unix_millis(now)
}

pub fn duration_to_secs_ceil(duration: Duration) -> u64 {
    duration
        .as_secs()
        .saturating_add(u64::from(duration.subsec_nanos() > 0))
        .max(1)
}
