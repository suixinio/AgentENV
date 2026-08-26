//! Task's own "D4": ports `services/scheduler/internal/store.go`'s
//! `InMemoryArtifactStore` (lines 508-651) — the P2P artifact-to-node hint
//! index (`src/p2p/discovery/mod.rs`'s `peers_for_key`/`record_key`/
//! `forget_key`, `LookupP2pArtifact`/`RecordP2pArtifact`/
//! `ForgetP2pArtifact`).
//!
//! # Accepted degradation, not a defect (task's own "D4"/Q5)
//!
//! Go's version is a single process-wide cache; every scheduler replica
//! (there was only ever one) held the whole index. Once this lives in api
//! (N replicas), each replica only knows the subset of records it
//! personally handled — there is no shared backend here, on purpose. This
//! was investigated and resolved before this file was written
//! (`docs/proposals/_sd-phase4-open-questions-resolved.md`'s Q5): the index
//! is consulted as an accelerator, never as the source of truth —
//! [`crate::p2p::iroh::transport::IrohBlobsP2pTransport::lookup_with_hints`]
//! unconditionally falls back to a full/hinted peer poll on a miss or an
//! error, and that fallback's candidate set comes from heartbeat discovery,
//! entirely independent of this index. A per-replica partial index
//! therefore degrades hit rate, never correctness — accept the degradation,
//! do not back this with Redis.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::SystemTime;

use crate::node_registry::registry::NodeRegistry;
use crate::proto::scheduler::P2pPeer;

/// Mirrors Go's `artifactIndexKey`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ArtifactIndexKey {
    cluster_id: String,
    backend: String,
    key: String,
}

/// The behavior every artifact index backend must provide. Mirrors Go's
/// implicit `ArtifactStore` interface — `record`/`forget`/`lookup`/
/// `forget_node`.
pub trait ArtifactStore: Send + Sync {
    fn record(&self, cluster_id: &str, backend: &str, key: &str, node_id: &str);
    fn forget(&self, cluster_id: &str, backend: &str, key: &str, node_id: &str);
    /// Every node id this index currently associates with the artifact,
    /// most-recently-touched first, capped at `limit` entries (`0` =
    /// unlimited).
    fn lookup(&self, cluster_id: &str, backend: &str, key: &str, limit: usize) -> Vec<String>;
    /// Drops every association naming `node_id` — the `UnregisterNode`
    /// cleanup path.
    fn forget_node(&self, node_id: &str);
}

struct Entry {
    nodes: Vec<String>,
    touched: u64,
}

struct Inner {
    entries: HashMap<ArtifactIndexKey, Entry>,
    /// Reverse index for `forget_node`, mirroring Go's `nodeKeys`.
    node_keys: HashMap<String, std::collections::HashSet<ArtifactIndexKey>>,
    clock: u64,
}

/// Ports Go's `InMemoryArtifactStore`. Capacity-bounded: once at capacity, a
/// new key evicts the least-recently-touched existing one. Eviction is O(n)
/// in the number of keys currently held — deliberately simple rather than a
/// true intrusive LRU list, since this index is an accelerator (see the
/// module doc) and a rare, occasional O(n) scan on eviction is not on any
/// hot path a caller waits synchronously on.
pub struct InMemoryArtifactStore {
    capacity: usize,
    inner: Mutex<Inner>,
}

impl InMemoryArtifactStore {
    /// Go's own default capacity is 1,000,000 keys.
    pub const DEFAULT_CAPACITY: usize = 1_000_000;

    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            inner: Mutex::new(Inner {
                entries: HashMap::new(),
                node_keys: HashMap::new(),
                clock: 0,
            }),
        }
    }

    fn evict_oldest_locked(inner: &mut Inner) {
        if let Some(oldest) = inner
            .entries
            .iter()
            .min_by_key(|(_, entry)| entry.touched)
            .map(|(k, _)| k.clone())
        {
            if let Some(entry) = inner.entries.remove(&oldest) {
                for node_id in entry.nodes {
                    if let Some(keys) = inner.node_keys.get_mut(&node_id) {
                        keys.remove(&oldest);
                    }
                }
            }
        }
    }
}

impl Default for InMemoryArtifactStore {
    fn default() -> Self {
        Self::new(Self::DEFAULT_CAPACITY)
    }
}

impl ArtifactStore for InMemoryArtifactStore {
    fn record(&self, cluster_id: &str, backend: &str, key: &str, node_id: &str) {
        if key.is_empty() || node_id.is_empty() {
            return;
        }
        let index_key = ArtifactIndexKey {
            cluster_id: cluster_id.to_string(),
            backend: backend.to_string(),
            key: key.to_string(),
        };
        let mut inner = self.inner.lock().expect("artifact index lock poisoned");
        inner.clock += 1;
        let now = inner.clock;

        if !inner.entries.contains_key(&index_key) && inner.entries.len() >= self.capacity {
            Self::evict_oldest_locked(&mut inner);
        }

        let entry = inner
            .entries
            .entry(index_key.clone())
            .or_insert_with(|| Entry {
                nodes: Vec::new(),
                touched: now,
            });
        entry.touched = now;
        if !entry.nodes.iter().any(|id| id == node_id) {
            entry.nodes.push(node_id.to_string());
        }
        inner
            .node_keys
            .entry(node_id.to_string())
            .or_default()
            .insert(index_key);
    }

    fn forget(&self, cluster_id: &str, backend: &str, key: &str, node_id: &str) {
        let index_key = ArtifactIndexKey {
            cluster_id: cluster_id.to_string(),
            backend: backend.to_string(),
            key: key.to_string(),
        };
        let mut inner = self.inner.lock().expect("artifact index lock poisoned");
        let mut now_empty = false;
        if let Some(entry) = inner.entries.get_mut(&index_key) {
            entry.nodes.retain(|id| id != node_id);
            now_empty = entry.nodes.is_empty();
        }
        if now_empty {
            inner.entries.remove(&index_key);
        }
        if let Some(keys) = inner.node_keys.get_mut(node_id) {
            keys.remove(&index_key);
        }
    }

    fn lookup(&self, cluster_id: &str, backend: &str, key: &str, limit: usize) -> Vec<String> {
        let index_key = ArtifactIndexKey {
            cluster_id: cluster_id.to_string(),
            backend: backend.to_string(),
            key: key.to_string(),
        };
        let mut inner = self.inner.lock().expect("artifact index lock poisoned");
        inner.clock += 1;
        let now = inner.clock;
        let Some(entry) = inner.entries.get_mut(&index_key) else {
            return Vec::new();
        };
        // A read also counts as a touch, keeping hot keys warm -- mirrors
        // Go's own `s.lru.Get(indexKey)` on the read path.
        entry.touched = now;
        if limit == 0 || entry.nodes.len() <= limit {
            entry.nodes.clone()
        } else {
            entry.nodes[..limit].to_vec()
        }
    }

    fn forget_node(&self, node_id: &str) {
        let mut inner = self.inner.lock().expect("artifact index lock poisoned");
        let Some(keys) = inner.node_keys.remove(node_id) else {
            return;
        };
        for index_key in keys {
            let mut now_empty = false;
            if let Some(entry) = inner.entries.get_mut(&index_key) {
                entry.nodes.retain(|id| id != node_id);
                now_empty = entry.nodes.is_empty();
            }
            if now_empty {
                inner.entries.remove(&index_key);
            }
        }
    }
}

/// Ports the `LookupP2pArtifact` RPC's second half: turns the index's raw
/// node ids into live peer descriptors via
/// [`NodeRegistry::filter_p2p_peers`], the same way `service.go:784-798`
/// does. A free function, not a method, since it needs both a
/// `dyn ArtifactStore` and a `dyn NodeRegistry` and belongs to neither.
#[allow(clippy::too_many_arguments)]
pub fn lookup_p2p_artifact_peers(
    artifacts: &dyn ArtifactStore,
    registry: &dyn NodeRegistry,
    cluster_id: &str,
    backend: &str,
    key: &str,
    exclude_node_id: &str,
    limit: usize,
    now: SystemTime,
) -> Vec<P2pPeer> {
    let node_ids = artifacts.lookup(cluster_id, backend, key, limit);
    registry.filter_p2p_peers(cluster_id, backend, &node_ids, exclude_node_id, now)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_then_lookup_round_trips() {
        let store = InMemoryArtifactStore::new(10);
        store.record("cluster-a", "overlaybd", "sha256:abc", "node-a");
        store.record("cluster-a", "overlaybd", "sha256:abc", "node-b");
        let peers = store.lookup("cluster-a", "overlaybd", "sha256:abc", 0);
        assert_eq!(peers, vec!["node-a".to_string(), "node-b".to_string()]);
    }

    #[test]
    fn lookup_of_an_unknown_key_is_empty() {
        let store = InMemoryArtifactStore::new(10);
        assert!(store
            .lookup("cluster-a", "overlaybd", "sha256:nope", 0)
            .is_empty());
    }

    #[test]
    fn record_is_idempotent_per_node() {
        let store = InMemoryArtifactStore::new(10);
        store.record("cluster-a", "overlaybd", "sha256:abc", "node-a");
        store.record("cluster-a", "overlaybd", "sha256:abc", "node-a");
        assert_eq!(
            store.lookup("cluster-a", "overlaybd", "sha256:abc", 0),
            vec!["node-a".to_string()]
        );
    }

    #[test]
    fn forget_removes_only_the_named_node() {
        let store = InMemoryArtifactStore::new(10);
        store.record("cluster-a", "overlaybd", "sha256:abc", "node-a");
        store.record("cluster-a", "overlaybd", "sha256:abc", "node-b");
        store.forget("cluster-a", "overlaybd", "sha256:abc", "node-a");
        assert_eq!(
            store.lookup("cluster-a", "overlaybd", "sha256:abc", 0),
            vec!["node-b".to_string()]
        );
    }

    #[test]
    fn forget_node_removes_every_association_that_node_had() {
        let store = InMemoryArtifactStore::new(10);
        store.record("cluster-a", "overlaybd", "sha256:abc", "node-a");
        store.record("cluster-a", "overlaybd", "sha256:def", "node-a");
        store.record("cluster-a", "overlaybd", "sha256:abc", "node-b");

        store.forget_node("node-a");

        assert_eq!(
            store.lookup("cluster-a", "overlaybd", "sha256:abc", 0),
            vec!["node-b".to_string()]
        );
        assert!(store
            .lookup("cluster-a", "overlaybd", "sha256:def", 0)
            .is_empty());
    }

    #[test]
    fn lookup_respects_the_limit() {
        let store = InMemoryArtifactStore::new(10);
        for node in ["node-a", "node-b", "node-c"] {
            store.record("cluster-a", "overlaybd", "sha256:abc", node);
        }
        assert_eq!(
            store
                .lookup("cluster-a", "overlaybd", "sha256:abc", 2)
                .len(),
            2
        );
        assert_eq!(
            store
                .lookup("cluster-a", "overlaybd", "sha256:abc", 0)
                .len(),
            3
        );
    }

    #[test]
    fn different_clusters_and_backends_do_not_collide() {
        let store = InMemoryArtifactStore::new(10);
        store.record("cluster-a", "overlaybd", "sha256:abc", "node-a");
        store.record("cluster-b", "overlaybd", "sha256:abc", "node-b");
        store.record("cluster-a", "snapshot", "sha256:abc", "node-c");

        assert_eq!(
            store.lookup("cluster-a", "overlaybd", "sha256:abc", 0),
            vec!["node-a".to_string()]
        );
        assert_eq!(
            store.lookup("cluster-b", "overlaybd", "sha256:abc", 0),
            vec!["node-b".to_string()]
        );
        assert_eq!(
            store.lookup("cluster-a", "snapshot", "sha256:abc", 0),
            vec!["node-c".to_string()]
        );
    }

    #[test]
    fn at_capacity_the_least_recently_touched_key_is_evicted() {
        let store = InMemoryArtifactStore::new(2);
        store.record("c", "b", "key-1", "node-a");
        store.record("c", "b", "key-2", "node-a");
        // Touch key-1 again so key-2 becomes the least-recently-touched.
        assert_eq!(
            store.lookup("c", "b", "key-1", 0),
            vec!["node-a".to_string()]
        );

        store.record("c", "b", "key-3", "node-a");

        assert!(
            !store.lookup("c", "b", "key-1", 0).is_empty(),
            "key-1 was touched most recently"
        );
        assert!(
            store.lookup("c", "b", "key-2", 0).is_empty(),
            "key-2 should have been evicted"
        );
        assert!(!store.lookup("c", "b", "key-3", 0).is_empty());
    }

    #[test]
    fn empty_key_or_node_id_records_nothing() {
        let store = InMemoryArtifactStore::new(10);
        store.record("c", "b", "", "node-a");
        store.record("c", "b", "key-1", "");
        assert!(store.lookup("c", "b", "", 0).is_empty());
        assert!(store.lookup("c", "b", "key-1", 0).is_empty());
    }
}
