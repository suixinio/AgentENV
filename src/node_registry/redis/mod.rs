//! Shares heartbeat-observed node state across `aenv-api` replicas.
//!
//! Local registry mutation remains synchronous and queues [`PublishOp`]s; one background
//! task owns Redis writes and periodic pulls. Newer receive-side `last_seen` wins, and
//! pull absence never deletes local state; discovery-driven removal publishes deletion.
//!
//! Staleness remains a registry decision. Redis pruning is only a crash-recovery backstop
//! after [`GC_GRACE_MULTIPLIER`] times each record's own report TTL.
//!
//! Large, near-static machine information lives in a side hash keyed by content digest.
//! Pulls resolve it before returning records and accept inline legacy records during rollout.

#[cfg(test)]
pub(crate) mod harness;
#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::time::Duration;

use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::warn;

use super::registry::AtomicNodeRegistry;

/// Period between shared-hash pulls, independent of heartbeat volume.
pub const DEFAULT_PULL_INTERVAL: Duration = Duration::from_secs(2);

/// Report-TTL multiple after which stale Redis fields are garbage-collected.
pub const GC_GRACE_MULTIPLIER: u64 = 4;

/// Redis connection and namespace settings.
#[derive(Debug, Clone)]
pub struct SharedObservedStoreConfig {
    pub url: String,
    /// Prefix for this store's independent Redis namespace.
    pub key_prefix: String,
    /// Maximum Redis command time.
    pub response_timeout: Duration,
    /// Maximum initial or reconnect handshake time.
    pub connect_timeout: Duration,
}

/// Default bounded Redis command timeout.
pub const DEFAULT_RESPONSE_TIMEOUT: Duration = Duration::from_millis(2000);

/// Default bounded Redis connection timeout.
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_millis(3000);

impl Default for SharedObservedStoreConfig {
    fn default() -> Self {
        Self {
            url: "redis://127.0.0.1:6379".to_string(),
            key_prefix: DEFAULT_KEY_PREFIX.to_string(),
            response_timeout: DEFAULT_RESPONSE_TIMEOUT,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
        }
    }
}

/// Independent node-registry Redis namespace.
pub const DEFAULT_KEY_PREFIX: &str = "agentenv:node-registry";

fn hash_key(key_prefix: &str) -> String {
    format!("{key_prefix}:observed")
}

/// Returns the side-hash key containing near-static machine information.
///
/// The hot record carries its digest. Inline machine data remains authoritative for
/// rolling compatibility with writers predating the split.
fn machine_hash_key(key_prefix: &str) -> String {
    format!("{key_prefix}:machine")
}

/// Computes the non-cryptographic digest used to skip unchanged machine-info transfers.
fn machine_info_digest(info: &StoredMachineInfo) -> String {
    let canonical = serde_json::to_vec(info)
        .expect("StoredMachineInfo serialization is infallible for this shape");
    crate::digest::sha256_digest(&canonical)
}

/// JSON-friendly shared form of a heartbeat observation.
///
/// Outside this store it carries resolved `machine_info`; digest-only records exist only
/// between hot-hash decoding and side-hash resolution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredObservedRecord {
    pub node_id: String,
    pub endpoint: String,
    pub cluster_id: String,
    pub service_instance_id: String,
    pub version: String,
    pub commit: String,
    pub machine_info: Option<StoredMachineInfo>,
    /// Machine-info digest, defaulting absent for records predating the split.
    #[serde(default)]
    pub machine_digest: Option<String>,
    pub snapshot: Option<StoredNodeSnapshot>,
    pub last_seen_unix_ms: i64,
    pub p2p_endpoint: Option<StoredP2pEndpoint>,
    pub report_ttl_secs: u64,
    pub entries: Vec<StoredRosterEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredMachineInfo {
    pub cpu_family: String,
    pub cpu_model: String,
    pub cpu_model_name: String,
    pub cpu_architecture: String,
    pub cpu_config_json: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredDiskMetric {
    pub mount_point: String,
    pub device: String,
    pub filesystem_type: String,
    pub used_bytes: u64,
    pub total_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredNodeSnapshot {
    pub status: i32,
    pub allocated_cpu: u32,
    pub allocated_memory_bytes: u64,
    pub cpu_percent: u32,
    pub cpu_count: u32,
    pub memory_used_bytes: u64,
    pub memory_total_bytes: u64,
    pub disks: Vec<StoredDiskMetric>,
    pub sandbox_count: u32,
    pub sandbox_starting_count: u32,
    pub create_successes: u64,
    pub create_fails: u64,
    pub reported_at_unix_ms: i64,
    pub paused_sandbox_count: u32,
    pub paused_allocated_cpu: u32,
    pub paused_allocated_memory_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredP2pEndpoint {
    pub backend: String,
    pub address: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredRosterEntry {
    pub sandbox_id: String,
    pub execution_id: String,
    pub projection_ttl_secs: u64,
    /// Defaults false so records predating this field remain decodable.
    #[serde(default)]
    pub paused: bool,
}

/// Shared-store mutation queued by the synchronous registry.
pub enum PublishOp {
    Upsert {
        node_id: String,
        record: Box<StoredObservedRecord>,
    },
    Remove {
        node_id: String,
    },
}

/// Redis connection, hot hash, side hash, and per-process digest caches.
pub struct SharedObservedStore {
    connection: redis::aio::ConnectionManager,
    hash_key: String,
    /// Side hash for near-static machine information.
    machine_hash_key: String,
    /// Digests this replica has successfully published.
    published_machine_digests: dashmap::DashMap<String, String>,
    /// Machine information this replica has fetched by digest.
    cached_machine_info: dashmap::DashMap<String, (String, StoredMachineInfo)>,
}

impl SharedObservedStore {
    /// Connects to Redis, failing startup when the shared store is unavailable.
    pub async fn connect(config: SharedObservedStoreConfig) -> anyhow::Result<Self> {
        let client = redis::Client::open(config.url.as_str())
            .map_err(|err| anyhow::anyhow!("opening the node registry redis client: {err}"))?;
        let manager_config = redis::aio::ConnectionManagerConfig::new()
            .set_response_timeout(Some(config.response_timeout))
            .set_connection_timeout(Some(config.connect_timeout));
        let connection = redis::aio::ConnectionManager::new_with_config(client, manager_config)
            .await
            .map_err(|err| {
                anyhow::anyhow!("connecting the node registry's observed store to redis: {err}")
            })?;
        Ok(Self {
            connection,
            hash_key: hash_key(&config.key_prefix),
            machine_hash_key: machine_hash_key(&config.key_prefix),
            published_machine_digests: dashmap::DashMap::new(),
            cached_machine_info: dashmap::DashMap::new(),
        })
    }

    /// Publishes the hot record after any changed machine information reaches the side hash.
    pub async fn upsert(&self, node_id: &str, record: &StoredObservedRecord) -> anyhow::Result<()> {
        let mut hot = record.clone();
        match record.machine_info.as_ref() {
            Some(machine_info) => {
                let digest = machine_info_digest(machine_info);
                let unchanged = self
                    .published_machine_digests
                    .get(node_id)
                    .is_some_and(|published| *published == digest);
                if !unchanged {
                    self.publish_machine_info(node_id, machine_info).await?;
                    self.published_machine_digests
                        .insert(node_id.to_string(), digest.clone());
                }
                hot.machine_digest = Some(digest);
            }
            None => {
                hot.machine_digest = None;
            }
        }
        hot.machine_info = None;
        let payload = serde_json::to_string(&hot)
            .map_err(|err| anyhow::anyhow!("encoding a node registry observed record: {err}"))?;
        let mut connection = self.connection.clone();
        let _: () = connection
            .hset(&self.hash_key, node_id, payload)
            .await
            .map_err(|err| anyhow::anyhow!("HSET on the node registry observed hash: {err}"))?;
        Ok(())
    }

    /// Publishes changed machine information to the side hash.
    async fn publish_machine_info(
        &self,
        node_id: &str,
        machine_info: &StoredMachineInfo,
    ) -> anyhow::Result<()> {
        let payload = serde_json::to_string(machine_info).map_err(|err| {
            anyhow::anyhow!("encoding a node registry observed record's machine info: {err}")
        })?;
        let mut connection = self.connection.clone();
        let _: () = connection
            .hset(&self.machine_hash_key, node_id, payload)
            .await
            .map_err(|err| anyhow::anyhow!("HSET on the node registry machine hash: {err}"))?;
        Ok(())
    }

    /// Removes a hot record and best-effort cleans its side-hash entry and caches.
    pub async fn remove(&self, node_id: &str) -> anyhow::Result<()> {
        let mut connection = self.connection.clone();
        let _: () = connection
            .hdel(&self.hash_key, node_id)
            .await
            .map_err(|err| anyhow::anyhow!("HDEL on the node registry observed hash: {err}"))?;
        let machine_hdel: redis::RedisResult<()> =
            connection.hdel(&self.machine_hash_key, node_id).await;
        if let Err(err) = machine_hdel {
            warn!(
                target: "agentenv",
                node_id = %node_id,
                error = %err,
                "node registry redis remove: HDEL on the machine hash failed; leaving an \
                 orphaned side-hash entry behind"
            );
        }
        self.published_machine_digests.remove(node_id);
        self.cached_machine_info.remove(node_id);
        Ok(())
    }

    /// Pulls and decodes observations, resolves machine information, and prunes expired fields.
    pub async fn pull_all(&self) -> anyhow::Result<HashMap<String, StoredObservedRecord>> {
        let mut connection = self.connection.clone();
        let raw: HashMap<String, String> = connection
            .hgetall(&self.hash_key)
            .await
            .map_err(|err| anyhow::anyhow!("HGETALL on the node registry observed hash: {err}"))?;

        let now_ms = crate::node_registry::registry::unix_millis(std::time::SystemTime::now());
        let mut fresh = HashMap::with_capacity(raw.len());
        for (node_id, payload) in raw {
            let mut record: StoredObservedRecord = match serde_json::from_str(&payload) {
                Ok(record) => record,
                Err(err) => {
                    warn!(
                        target: "agentenv",
                        node_id = %node_id,
                        error = %err,
                        "node registry redis pull: skipping an undecodable observed record"
                    );
                    continue;
                }
            };
            let grace_ms = Duration::from_secs(record.report_ttl_secs.max(1) * GC_GRACE_MULTIPLIER)
                .as_millis() as i64;
            if now_ms.saturating_sub(record.last_seen_unix_ms) > grace_ms {
                if let Err(err) = self.remove(&node_id).await {
                    warn!(
                        target: "agentenv",
                        node_id = %node_id,
                        error = %err,
                        "node registry redis pull: failed to prune a stale observed record"
                    );
                }
                continue;
            }
            self.resolve_machine_info(&node_id, &mut record).await;
            fresh.insert(node_id, record);
        }
        Ok(fresh)
    }

    /// Resolves digest-only machine information before returning a pulled record.
    ///
    /// Inline legacy data wins; misses remain unresolved for this tick and retry next pull.
    async fn resolve_machine_info(&self, node_id: &str, record: &mut StoredObservedRecord) {
        if let Some(info) = record.machine_info.as_ref() {
            let digest = record
                .machine_digest
                .clone()
                .unwrap_or_else(|| machine_info_digest(info));
            self.cached_machine_info
                .insert(node_id.to_string(), (digest, info.clone()));
            return;
        }
        let Some(digest) = record.machine_digest.clone() else {
            return;
        };
        if let Some(cached) = self.cached_machine_info.get(node_id) {
            if cached.0 == digest {
                record.machine_info = Some(cached.1.clone());
                return;
            }
        }
        let mut connection = self.connection.clone();
        let raw: Option<String> = match connection.hget(&self.machine_hash_key, node_id).await {
            Ok(raw) => raw,
            Err(err) => {
                warn!(
                    target: "agentenv",
                    node_id = %node_id,
                    error = %err,
                    "node registry redis pull: HGET on the machine hash failed; leaving \
                     machine_info empty for this tick"
                );
                return;
            }
        };
        let Some(raw) = raw else {
            // A missing side-hash value is retried on the next pull.
            warn!(
                target: "agentenv",
                node_id = %node_id,
                "node registry redis pull: hot record names a machine_digest with no matching \
                 machine-hash entry yet; leaving machine_info empty for this tick"
            );
            return;
        };
        match serde_json::from_str::<StoredMachineInfo>(&raw) {
            Ok(info) => {
                self.cached_machine_info
                    .insert(node_id.to_string(), (digest, info.clone()));
                record.machine_info = Some(info);
            }
            Err(err) => {
                warn!(
                    target: "agentenv",
                    node_id = %node_id,
                    error = %err,
                    "node registry redis pull: skipping an undecodable machine-hash entry"
                );
            }
        }
    }
}

/// Publishes queued mutations and periodically merges pulled observations until shutdown.
pub async fn run_shared_observed_sync(
    registry: std::sync::Arc<AtomicNodeRegistry>,
    mut rx: mpsc::UnboundedReceiver<PublishOp>,
    store: SharedObservedStore,
    pull_interval: Duration,
) {
    let mut ticker = tokio::time::interval(pull_interval);
    // Startup already pulled once; wait a full interval before repeating it.
    ticker.tick().await;
    loop {
        tokio::select! {
            op = rx.recv() => {
                match op {
                    Some(PublishOp::Upsert { node_id, record }) => {
                        if let Err(err) = store.upsert(&node_id, &record).await {
                            warn!(target: "agentenv", node_id = %node_id, error = %err, "node registry redis publish (upsert) failed");
                        }
                    }
                    Some(PublishOp::Remove { node_id }) => {
                        if let Err(err) = store.remove(&node_id).await {
                            warn!(target: "agentenv", node_id = %node_id, error = %err, "node registry redis publish (remove) failed");
                        }
                    }
                    None => {
                        return;
                    }
                }
            }
            _ = ticker.tick() => {
                match store.pull_all().await {
                    Ok(remote) => registry.merge_remote_snapshot(remote),
                    Err(err) => {
                        warn!(target: "agentenv", error = %err, "node registry redis pull failed; will retry next tick");
                    }
                }
            }
        }
    }
}
