//! Shares [`super::registry::AtomicNodeRegistry`]'s heartbeat-derived
//! ("observed") state across `--role api` replicas.
//!
//! # The split this closes
//!
//! `AtomicNodeRegistry` is fed by two sources: Kubernetes discovery (the
//! same `EndpointSlice`/`Pod` watch on every replica, so identical
//! everywhere already) and node heartbeats (a long-lived gRPC stream pinned
//! to whichever replica a node happened to dial through the `agentenv-api`
//! Service — different on every replica). Before this module existed, the
//! second half — `Inner::observed` in `super::registry` — was pure
//! per-process memory: replica A only ever knew about the nodes that
//! connected to *it*, replica B only about the nodes that connected to *it*,
//! and every reader (`/nodes`, `ListObservedNodes`, `ListP2pPeers`, and —
//! the one correctness-bearing case — the CPU-config intersection gate,
//! `Inner::all_configs_ready`) only ever saw its own replica's half.
//!
//! # The fix: publish on write, merge on a timer
//!
//! [`SharedObservedStore`] is a thin Redis-backed key/value store: one hash
//! field per node id, holding a [`StoredObservedRecord`] as JSON. Every
//! local heartbeat/departure still updates `Inner.observed` synchronously,
//! exactly as before (nothing about the hot path changes), and *also*
//! enqueues a [`PublishOp`] onto an unbounded channel — a cheap, lock-free
//! push, never a network call, so `AtomicNodeRegistry::heartbeat` remains
//! fully synchronous and never blocks a gRPC request on Redis. A single
//! background task, [`run_shared_observed_sync`], owns the actual
//! connection: it drains that channel (writing `HSET`/`HDEL` best-effort,
//! logging rather than panicking on failure — `redis::aio::ConnectionManager`
//! already retries the connection itself) and, on its own independent
//! timer (deliberately *not* driven by heartbeat volume — see
//! [`DEFAULT_PULL_INTERVAL`]'s own doc), pulls the whole shared hash once
//! and merges it into `Inner.observed` via
//! [`super::registry::AtomicNodeRegistry::merge_remote_snapshot`].
//!
//! This means every `NodeRegistry` trait method stays perfectly
//! synchronous — no `async fn` in the trait, no ripple into
//! `grpc_service.rs`, `dump.rs`, `warmup.rs`, or any test that constructs an
//! `AtomicNodeRegistry` directly. The only asynchrony this module adds is
//! this one background task, wired up by `src/bin/aenv-api.rs` alongside the
//! kube-discovery and metrics tasks `start_native_node_registry` already
//! spawns.
//!
//! # Merge semantics: last write wins by `last_seen`, absence never deletes
//!
//! A node's heartbeat is pinned to exactly one replica for the life of that
//! HTTP/2 connection, so at most one replica is ever pushing a given node's
//! key at a time. [`super::registry::Inner::merge_remote`] adopts a pulled
//! record only when its `last_seen` is strictly newer than whatever this
//! replica already has locally — so a replica's own fresher local write
//! (which may not have round-tripped through Redis yet) is never clobbered
//! by a stale pull of its own prior push, and a node that reconnects to a
//! *different* replica is picked up the next time that fact reaches Redis.
//!
//! A pull never deletes a node absent from the Redis hash — that would
//! race a replica's own not-yet-flushed first write for a brand new node.
//! Removal instead rides the same discovery-driven cleanup
//! `Inner::set`/`Inner::admit_pending`/`unregister_observed` already do
//! locally (identical on every replica, since discovery state already is);
//! each of those now also enqueues [`PublishOp::Remove`] so the shared hash
//! converges promptly rather than only via [`GC_GRACE_MULTIPLIER`]'s
//! backstop.
//!
//! # TTL: one source of truth, not two
//!
//! There is deliberately no Redis-native TTL/expiry on the hash or its
//! fields (Redis per-hash-field TTL is a 7.4+ feature this deployment
//! cannot assume). The one staleness judgment production already makes —
//! `Inner::derive_observed_node_view`'s `last_seen_unix_ms` vs.
//! `report_ttl` comparison — stays the only one. [`SharedObservedStore::pull_all`]
//! prunes (`HDEL`) hash fields whose own `last_seen_unix_ms` is older than
//! [`GC_GRACE_MULTIPLIER`] times their own `report_ttl_secs` — generous
//! enough that this is a crash-recovery backstop (a replica that died
//! before it could `HDEL` its own departed nodes) rather than a second,
//! competing notion of "expired."

#[cfg(test)]
mod harness;
#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::time::Duration;

use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::warn;

use super::registry::AtomicNodeRegistry;

/// How often [`run_shared_observed_sync`] pulls the whole shared hash and
/// merges it into this replica's local view.
///
/// Deliberately independent of heartbeat volume or count — a fixed
/// wall-clock cadence, the same discipline
/// `crate::orchestrator::paused_registry::postgres::replica_renewal::RENEWAL_INTERVAL`
/// already uses for a similarly-scoped per-replica background timer. Short
/// enough that a newly joined node's CPU config becomes visible to every
/// replica's intersection gate (`Inner::all_configs_ready`) within a couple
/// of seconds, not tied to the 5s heartbeat interval so it costs the same
/// one `HGETALL` per tick regardless of how many nodes are heartbeating or
/// how often.
pub const DEFAULT_PULL_INTERVAL: Duration = Duration::from_secs(2);

/// How many multiples of a record's own `report_ttl_secs` it may go
/// un-refreshed in the shared hash before [`SharedObservedStore::pull_all`]
/// prunes it outright. Generous on purpose — this is a backstop for a
/// replica that died before it could `HDEL` its own departed nodes' keys,
/// not a second staleness judgment competing with
/// `Inner::derive_observed_node_view`'s own (see this module's own doc
/// comment).
pub const GC_GRACE_MULTIPLIER: u64 = 4;

/// Redis-specific configuration, analogous to
/// `crate::binding_store::redis::RedisBindingStoreConfig`.
#[derive(Debug, Clone)]
pub struct SharedObservedStoreConfig {
    /// `redis://host:port[/db]`.
    pub url: String,
    /// Prefix for the single hash key this store owns
    /// (`{key_prefix}:observed`). Kept independent of
    /// `[binding_store].redis_key_prefix` /
    /// `[orchestrator.store].redis_key_prefix` — see `cfg.rs`'s own doc on
    /// why each Redis-backed subsystem in this codebase owns its prefix
    /// rather than sharing one.
    pub key_prefix: String,
}

impl Default for SharedObservedStoreConfig {
    fn default() -> Self {
        Self {
            url: "redis://127.0.0.1:6379".to_string(),
            key_prefix: DEFAULT_KEY_PREFIX.to_string(),
        }
    }
}

/// 🔴 Deliberately not `agentenv:api` (orchestrator store's default) or
/// `agentenv:scheduler:bindings` (binding store's — and gateway's — fixed
/// namespace): a third, independent namespace for a third, independent
/// piece of shared state.
pub const DEFAULT_KEY_PREFIX: &str = "agentenv:node-registry";

fn hash_key(key_prefix: &str) -> String {
    format!("{key_prefix}:observed")
}

/// One node's shared, heartbeat-derived state, as stored in Redis. A plain
/// field-for-field mirror of `super::registry::ObservedNodeRecord` (which
/// stays private to `registry.rs`) using only JSON-friendly primitives —
/// deliberately not protobuf-encoding the embedded `scheduler.v1` messages,
/// so a value written by this build is `redis-cli`-readable without a
/// decoder. `registry.rs` owns the `From`/conversion impls in both
/// directions, since it is the only module that can see the private type
/// this mirrors.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredObservedRecord {
    pub node_id: String,
    pub endpoint: String,
    pub cluster_id: String,
    pub service_instance_id: String,
    pub version: String,
    pub commit: String,
    pub machine_info: Option<StoredMachineInfo>,
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
}

/// A single update to publish to the shared hash. Constructed by
/// `registry.rs` on every local mutation of `Inner.observed` when a shared
/// store is enabled, consumed by [`run_shared_observed_sync`].
pub enum PublishOp {
    Upsert {
        node_id: String,
        record: Box<StoredObservedRecord>,
    },
    Remove {
        node_id: String,
    },
}

/// The Redis connection and key this store owns. Mirrors
/// `crate::binding_store::redis::RedisBindingStore`'s connection handling —
/// one `redis::aio::ConnectionManager` (which already retries/reconnects
/// under the hood), cloned per call the same way `ConnectionManager` is
/// designed to be shared.
pub struct SharedObservedStore {
    connection: redis::aio::ConnectionManager,
    hash_key: String,
}

impl SharedObservedStore {
    /// Connects to Redis. A failure here is a startup refusal for whichever
    /// caller awaits it (`src/bin/aenv-api.rs`'s `wire_shared_node_observed_store`),
    /// never a background retry — the same discipline
    /// `RedisBindingStore::connect`/`RedisMetadataStore::connect` already
    /// apply: a replica that cannot reach its shared store at all should
    /// fail loudly at boot, not run silently split.
    pub async fn connect(config: SharedObservedStoreConfig) -> anyhow::Result<Self> {
        let client = redis::Client::open(config.url.as_str())
            .map_err(|err| anyhow::anyhow!("opening the node registry redis client: {err}"))?;
        let connection = redis::aio::ConnectionManager::new(client)
            .await
            .map_err(|err| {
                anyhow::anyhow!("connecting the node registry's observed store to redis: {err}")
            })?;
        Ok(Self {
            connection,
            hash_key: hash_key(&config.key_prefix),
        })
    }

    /// `HSET`s one node's record. Best-effort from the caller's point of
    /// view — see [`run_shared_observed_sync`], the only production caller.
    pub async fn upsert(&self, node_id: &str, record: &StoredObservedRecord) -> anyhow::Result<()> {
        let payload = serde_json::to_string(record)
            .map_err(|err| anyhow::anyhow!("encoding a node registry observed record: {err}"))?;
        let mut connection = self.connection.clone();
        let _: () = connection
            .hset(&self.hash_key, node_id, payload)
            .await
            .map_err(|err| anyhow::anyhow!("HSET on the node registry observed hash: {err}"))?;
        Ok(())
    }

    /// `HDEL`s one node's record.
    pub async fn remove(&self, node_id: &str) -> anyhow::Result<()> {
        let mut connection = self.connection.clone();
        let _: () = connection
            .hdel(&self.hash_key, node_id)
            .await
            .map_err(|err| anyhow::anyhow!("HDEL on the node registry observed hash: {err}"))?;
        Ok(())
    }

    /// `HGETALL`s the whole shared hash, decodes every field, and prunes
    /// (`HDEL`) any entry stale enough to trip [`GC_GRACE_MULTIPLIER`]'s
    /// backstop (see this module's own "TTL" doc). A field that fails to
    /// decode (a version skew, a truncated write) is logged and skipped —
    /// mirroring `Inner::compute_intersection`'s own "one node's bad data
    /// must not take down the whole cluster's view" discipline — rather
    /// than failing the whole pull.
    pub async fn pull_all(&self) -> anyhow::Result<HashMap<String, StoredObservedRecord>> {
        let mut connection = self.connection.clone();
        let raw: HashMap<String, String> = connection
            .hgetall(&self.hash_key)
            .await
            .map_err(|err| anyhow::anyhow!("HGETALL on the node registry observed hash: {err}"))?;

        let now_ms = crate::node_registry::registry::unix_millis(std::time::SystemTime::now());
        let mut fresh = HashMap::with_capacity(raw.len());
        for (node_id, payload) in raw {
            let record: StoredObservedRecord = match serde_json::from_str(&payload) {
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
            fresh.insert(node_id, record);
        }
        Ok(fresh)
    }
}

/// Drains [`PublishOp`]s (writing them best-effort) and, on
/// [`DEFAULT_PULL_INTERVAL`]'s own timer, pulls the whole shared hash and
/// merges it into `registry`'s local view. Runs for the lifetime of the
/// process — callers drive it as a background task (`src/bin/aenv-api.rs`'s
/// `wire_shared_node_observed_store`), the same way
/// `start_native_node_registry` already spawns the kube-discovery and
/// metrics tasks. Returns only when `rx`'s sender is dropped, i.e. when the
/// owning `AtomicNodeRegistry` itself is gone.
pub async fn run_shared_observed_sync(
    registry: std::sync::Arc<AtomicNodeRegistry>,
    mut rx: mpsc::UnboundedReceiver<PublishOp>,
    store: SharedObservedStore,
    pull_interval: Duration,
) {
    let mut ticker = tokio::time::interval(pull_interval);
    // The first tick fires immediately; the caller already did the
    // startup-blocking initial pull (`wire_shared_node_observed_store`), so
    // this loop's first pull should wait a full interval rather than
    // repeating it right away.
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
                        // The registry itself is gone; nothing left to sync.
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
