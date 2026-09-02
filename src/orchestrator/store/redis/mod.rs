//! Redis-backed authoritative metadata store.
//! Revision and execution predicates carry write correctness; locks manage contention.
//! Transition indexes and reapers recover operations whose owning replica dies.

mod config;
mod crud;
mod expiry;
mod keys;
mod lock;
mod notify;
mod record;
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
pub use record::{ActiveStateRecord, StoredSandboxRecord, RECORD_VERSION};

use keys::KeySpace;
use lock::LockManager;
use notify::Notifier;

// Cached aggregate listing paired with its sampling instant.
struct ListingSample {
    sampled_at: Instant,
    records: Arc<Vec<SandboxMetadata>>,
}

/// Readiness of a background repair round.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RoundReadiness {
    Disabled,
    WarmingUp,
    Ready,
}

// Holds repair tasks through their first interval and after re-enabling.
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
    listing_memo: RwLock<Option<ListingSample>>,
    healer_warmup: WarmUp,
    reaper_warmup: WarmUp,
}

/// Cluster authoritative active-state store.
pub struct RedisMetadataStore {
    inner: Arc<StoreInner>,
}

impl RedisMetadataStore {
    /// Validates configuration and connects.
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

    /// Returns when the aggregate listing sample was refreshed.
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

    /// Deletes this test store's namespace.
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

    /// Selects the real or test-only predicate-free update script.
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

/// Maps Redis transport failure to a backend error, never absence.
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
