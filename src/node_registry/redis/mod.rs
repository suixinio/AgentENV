//! Shares [`super::registry::AtomicNodeRegistry`]'s heartbeat-derived
//! ("observed") state across `aenv-api` replicas.
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
    /// How long a single Redis command may run before
    /// `redis::aio::ConnectionManager` times it out and (per its own retry
    /// policy) reconnects. See [`DEFAULT_RESPONSE_TIMEOUT`]'s own doc for
    /// why this is no longer the crate's built-in 500ms.
    pub response_timeout: Duration,
    /// How long a fresh TCP connection attempt — the initial connect, or a
    /// reconnect after a `response_timeout` — may take.
    pub connect_timeout: Duration,
}

/// 🔴 redis-rs 1.6.0's own default (`redis::client::DEFAULT_RESPONSE_TIMEOUT`)
/// is 500ms, applied to every command issued over a bare
/// `ConnectionManager::new(client)` — which is what this store, and the
/// other two Redis-backed subsystems in this codebase
/// (`crate::orchestrator::store::redis`, `crate::binding_store::redis`),
/// constructed with until this change. That default was measured too tight
/// against this store's own production traffic on the pve-sg cluster: a
/// `ZRANGEBYSCORE` on `crate::orchestrator::store::redis::expiry` (a
/// neighbor on the same Redis instance) was observed taking 292ms, and
/// this store's own `HGETALL`/`HSET` calls travel the same cross-node RTT
/// on top of whatever the server-side latency is — 500ms left too little
/// headroom for both to stack on an ordinary day, let alone a slow one, and
/// every `HGETALL`/`HSET` timeout surfaced as a `warn!` in production
/// (`node registry redis pull/publish failed ... timed out`) roughly every
/// heartbeat interval on two replicas at once.
///
/// 4x the crate default, which is still a bounded, short timeout — not a
/// "wait indefinitely" escape hatch. The trade-off this buys: a genuinely
/// unreachable Redis now takes up to this long (per call, not cumulative —
/// `ConnectionManager` does not queue commands behind a stuck one) to be
/// *detected*, instead of 500ms. That is acceptable here because nothing on
/// this path depends on a fast failure to stay correct or responsive:
/// neither `/health` nor any readiness/liveness probe touches this store
/// (`src/api/server.rs`'s `/health` handler is a bare `"ok"`, unconditional
/// on Redis), there is no `select!` racing this call against a shorter
/// deadline anywhere in `src/node_registry/`, and every caller of this
/// store's methods already treats a Redis error as best-effort/retryable —
/// `run_shared_observed_sync` logs and waits for the next heartbeat or the
/// next [`DEFAULT_PULL_INTERVAL`] tick either way, never blocking a gRPC
/// `Heartbeat` response on it. A dead Redis was already only detected on
/// the next tick, not synchronously; this change only moves how long *that*
/// tick's own call takes to give up.
pub const DEFAULT_RESPONSE_TIMEOUT: Duration = Duration::from_millis(2000);

/// 4x redis-rs's own 1s default (`redis::client::DEFAULT_CONNECTION_TIMEOUT`).
/// Bounds only the TCP+handshake step, not command execution
/// ([`DEFAULT_RESPONSE_TIMEOUT`] above covers that) — occasional connection
/// setup jitter (a busy node, a slow DNS resolution in-cluster) is not
/// itself evidence Redis is down, and `ConnectionManager` already retries
/// with backoff on top of this per-attempt bound.
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

/// 🔴 Deliberately not `agentenv:api` (orchestrator store's default) or
/// `agentenv:scheduler:bindings` (binding store's — and gateway's — fixed
/// namespace): a third, independent namespace for a third, independent
/// piece of shared state.
pub const DEFAULT_KEY_PREFIX: &str = "agentenv:node-registry";

fn hash_key(key_prefix: &str) -> String {
    format!("{key_prefix}:observed")
}

/// # Splitting the near-static machine payload off the hot path
///
/// `machine_info.cpu_config_json` (a raw CPUID dump) is the overwhelming
/// majority of one [`StoredObservedRecord`]'s bytes — measured around 47KB
/// on the pve-sg cluster, against a hot record otherwise well under 1KB —
/// and it changes only when a node's actual CPU changes, which in practice
/// is never for the life of a heartbeat stream. Every other field on this
/// type legitimately changes on every heartbeat (`last_seen_unix_ms` at
/// minimum). Before this split, [`SharedObservedStore::upsert`] `HSET` the
/// *whole* record — machine info included — on every single heartbeat, and
/// [`SharedObservedStore::pull_all`] `HGETALL` the *whole* hash — every
/// node's machine info included — every [`DEFAULT_PULL_INTERVAL`] tick.
/// Both costs scale with node count: two nodes was already ~94KB moved per
/// pull tick, ten nodes would be ~470KB, all to re-transmit bytes that had
/// not changed since the node booted.
///
/// The fix: `machine_info` now travels on its own side hash
/// (`{key_prefix}:machine`, one field per node id, [`StoredMachineInfo`]
/// JSON — see [`SharedObservedStore::publish_machine_info`]), written only
/// when its content digest ([`machine_info_digest`]) differs from what this
/// replica last confirmed publishing. The hot hash
/// (`{key_prefix}:observed`) carries [`StoredObservedRecord::machine_digest`]
/// in `machine_info`'s place, so a pull can tell *whether* to bother
/// fetching the side hash without fetching it. [`SharedObservedStore::pull_all`]
/// resolves the two back into one fully-populated record before handing it
/// to a caller — this split is entirely an implementation detail of this
/// store; `registry.rs`'s `merge_remote_snapshot` and every correctness
/// property downstream of it (the CPU-config intersection gate very much
/// included) keep seeing the same shape as before.
///
/// ## Rolling-upgrade compatibility
///
/// `aenv-api` is a 2-replica rolling `Deployment`: during an upgrade, one
/// replica may run this split while the other still runs the pre-split
/// code that `HSET`s a whole inline record with no `machine_digest` field
/// at all. [`SharedObservedStore::resolve_machine_info`] treats an inline
/// `machine_info` on a pulled record as authoritative on sight — the
/// *read* side is what has to tolerate both formats, because the *write*
/// side of an already-deployed old binary cannot be changed. The direction
/// this module does not have to handle is the reverse: an old replica
/// pulling a new-format record (inline `machine_info: null`, a
/// `machine_digest` its own `StoredObservedRecord` does not know the field
/// name for and so silently ignores) sees an *absent* machine info for
/// nodes whose heartbeat is pinned to an already-upgraded peer, until the
/// old replica is itself replaced. That is a bounded, self-healing
/// degradation, not data loss: `Inner::all_configs_ready`'s gate is
/// all-or-nothing (see its own doc), so a missing `machine_info` only
/// withholds that one replica's CPU-intersection cache a little longer —
/// it never computes a wrong intersection from a partial view, and the
/// side hash still holds the real data throughout, unharmed, for every
/// replica (old or new) that already has it cached or fetches it once
/// upgraded. `merge_remote`'s `last_seen`-wins rule (see this module's own
/// "Merge semantics" doc above) means the old replica's incomplete view
/// never gets *written back* over a peer's good one, either — it is purely
/// a reader-side, one-replica-at-a-time blind spot that clears itself the
/// moment that replica's Pod is replaced by the rollout already in
/// progress.
fn machine_hash_key(key_prefix: &str) -> String {
    format!("{key_prefix}:machine")
}

/// Content digest of a [`StoredMachineInfo`], used to decide whether the
/// side hash needs rewriting ([`SharedObservedStore::upsert`]) and whether
/// a cached copy is still current ([`SharedObservedStore::pull_all`]).
/// `StoredMachineInfo`'s field order is fixed, so `serde_json`'s
/// serialization of it is deterministic — good enough for a same-process,
/// same-build digest; this is not used as a security boundary.
fn machine_info_digest(info: &StoredMachineInfo) -> String {
    let canonical = serde_json::to_vec(info)
        .expect("StoredMachineInfo serialization is infallible for this shape");
    crate::digest::sha256_digest(&canonical)
}

/// One node's shared, heartbeat-derived state, as stored in Redis. A plain
/// field-for-field mirror of `super::registry::ObservedNodeRecord` (which
/// stays private to `registry.rs`) using only JSON-friendly primitives —
/// deliberately not protobuf-encoding the embedded `scheduler.v1` messages,
/// so a value written by this build is `redis-cli`-readable without a
/// decoder. `registry.rs` owns the `From`/conversion impls in both
/// directions, since it is the only module that can see the private type
/// this mirrors.
///
/// 🔴 `machine_info`/`machine_digest`: outside this file, treat this type
/// as always carrying a fully-resolved `machine_info` (when the node has
/// one at all) — that is what `registry.rs` hands in and what
/// [`SharedObservedStore::pull_all`] hands back out. Only *within* this
/// file's `SharedObservedStore::upsert`/`pull_all` is `machine_info` ever
/// absent with a non-`None` `machine_digest` in its place — see this
/// module's own "splitting the near-static machine payload off the hot
/// path" doc section above.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredObservedRecord {
    pub node_id: String,
    pub endpoint: String,
    pub cluster_id: String,
    pub service_instance_id: String,
    pub version: String,
    pub commit: String,
    pub machine_info: Option<StoredMachineInfo>,
    /// Content digest of `machine_info`. `None` when `machine_info` is
    /// `None` (the node has never reported one), or when this record was
    /// constructed directly by `registry.rs` rather than round-tripped
    /// through this module's hot/side-hash split — `registry.rs` never
    /// reads this field back out, only `SharedObservedStore` does.
    /// `#[serde(default)]` so a record decoded from a pre-split write (see
    /// the rolling-upgrade doc above) still deserializes.
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
    /// 🔴 `#[serde(default)]` on purpose: a record written by a replica that
    /// predates the field has to keep deserializing, and `false` is the same
    /// answer that replica would have given.
    #[serde(default)]
    pub paused: bool,
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

/// The Redis connection and keys this store owns. Mirrors
/// `crate::binding_store::redis::RedisBindingStore`'s connection handling —
/// one `redis::aio::ConnectionManager` (which already retries/reconnects
/// under the hood), cloned per call the same way `ConnectionManager` is
/// designed to be shared.
pub struct SharedObservedStore {
    connection: redis::aio::ConnectionManager,
    hash_key: String,
    /// The near-static machine-info side hash — see this module's own
    /// "splitting the near-static machine payload off the hot path" doc
    /// section below.
    machine_hash_key: String,
    /// Publish-side memo: the digest of the `machine_info` this replica most
    /// recently confirmed writing to `machine_hash_key` for a node id.
    /// [`Self::upsert`] consults this to skip re-`HSET`ing the (large,
    /// near-static) machine payload on every heartbeat. Empty on a fresh
    /// connect (including after a process restart), so the first heartbeat
    /// for a node after a restart always re-publishes once — harmless,
    /// self-correcting, and far cheaper than doing it every heartbeat.
    published_machine_digests: dashmap::DashMap<String, String>,
    /// Pull-side memo: the last `(digest, StoredMachineInfo)` this replica
    /// fetched from `machine_hash_key` for a node id, so a pull whose hot
    /// record carries an unchanged digest can skip the `HGET` entirely.
    /// Independent of `published_machine_digests` above — this one serves
    /// [`Self::pull_all`], not [`Self::upsert`], and a replica pulls
    /// records for nodes whose heartbeats never land on it directly.
    cached_machine_info: dashmap::DashMap<String, (String, StoredMachineInfo)>,
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

    /// `HSET`s one node's record onto the hot hash, after stripping
    /// `machine_info` out of it (see this module's own "splitting the
    /// near-static machine payload off the hot path" doc section) and,
    /// only when its content digest has changed since this replica last
    /// confirmed publishing it, `HSET`ing the full payload onto the side
    /// hash first. Best-effort from the caller's point of view — see
    /// [`run_shared_observed_sync`], the only production caller: an error
    /// anywhere in here (including the side-hash write) fails the whole
    /// call, so a heartbeat that could not publish its machine info also
    /// does not publish a hot record whose `machine_digest` promises a
    /// side-hash entry that was never actually written — the next
    /// heartbeat retries both together.
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

    /// `HSET`s the full `machine_info` payload onto the side hash. Never
    /// called unless [`Self::upsert`] has determined the digest actually
    /// changed (or has never been confirmed published by this replica).
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

    /// `HDEL`s one node's record from the hot hash, and best-effort cleans
    /// up its side-hash entry and both local caches. The side-hash cleanup
    /// failing does not fail the whole call — an orphaned side-hash field
    /// is harmless (no TTL either way, see this module's own "TTL" doc
    /// section; it is overwritten the next time this node id publishes a
    /// machine info, and pruned the same GC pass that would have caught it
    /// on the hot hash) — but the caller's own primary intent, removing the
    /// node from the roster every reader iterates, must not be masked by a
    /// failure in this secondary cleanup.
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

    /// `HGETALL`s the whole shared hash, decodes every field, resolves each
    /// record's `machine_info` (see [`Self::resolve_machine_info`]), and
    /// prunes (`HDEL`) any entry stale enough to trip
    /// [`GC_GRACE_MULTIPLIER`]'s backstop (see this module's own "TTL"
    /// doc). A field that fails to decode (a version skew, a truncated
    /// write) is logged and skipped — mirroring
    /// `Inner::compute_intersection`'s own "one node's bad data must not
    /// take down the whole cluster's view" discipline — rather than
    /// failing the whole pull.
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

    /// Fills in `record.machine_info` before it is handed back from
    /// [`Self::pull_all`] — the split this module's own "splitting the
    /// near-static machine payload off the hot path" doc section describes
    /// is an implementation detail of this store; every caller of
    /// `pull_all` (concretely, `registry.rs`'s `merge_remote_snapshot`)
    /// keeps expecting a fully-resolved record exactly as before.
    ///
    /// Three cases:
    ///
    /// 1. `record.machine_info` is already `Some` — an old-format record,
    ///    written by a peer that predates this split (a rolling upgrade in
    ///    progress: `aenv-api` runs two replicas, and a not-yet-upgraded
    ///    one still publishes the whole payload inline). Nothing to fetch;
    ///    the pull-side cache is seeded from it anyway so a later
    ///    new-format record with a matching digest can skip the fetch too.
    /// 2. `record.machine_digest` is `None` — a node that has never
    ///    reported a machine info at all (new format, legitimately empty).
    ///    Nothing to do.
    /// 3. `record.machine_digest` is `Some` and `machine_info` is `None` —
    ///    the normal new-format case. Serves from the pull-side cache when
    ///    its digest still matches; otherwise `HGET`s the side hash. A miss
    ///    or a decode failure here is logged and left as `machine_info:
    ///    None` for this tick only — self-healing: the digest mismatch
    ///    persists, so the very next pull tick tries again, and nothing
    ///    about the miss is treated as "this node has no machine info" (see
    ///    `Inner::ingest_observed`'s own `is_some_and` doc on why that
    ///    distinction is load-bearing for `Inner::all_configs_ready`).
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
            // The hot record's digest was published without (or ahead of)
            // its side-hash entry landing — see `Self::upsert`'s own
            // ordering. Self-heals on the next tick.
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
