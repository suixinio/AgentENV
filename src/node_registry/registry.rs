//! Port of `services/scheduler/internal/node_registry.go` (824 lines, minus
//! the Kubernetes-discovery wiring, which lives separately — see
//! `src/node_registry/mod.rs`) — the in-memory record of which nodes exist
//! (from discovery) and what they last reported (from heartbeats).
//!
//! [`AtomicNodeRegistry`] is the concrete type; [`NodeRegistry`] is the trait
//! every consumer should depend on (mirroring Go's implicit interface of the
//! same name), so a future consumer can be tested against a fake without
//! reaching for the concrete type.
//!
//! 🔴 Both are wired into a live runtime path from Stage A on:
//! `start_native_node_registry` (`src/bin/aenv-api.rs`) builds an
//! [`AtomicNodeRegistry`] and hands it to `NodeRegistryGrpcService`, whose
//! `Heartbeat` RPC is the process's real, network-reachable heartbeat
//! surface under `[cluster].node_placement_source = "native"`. This matters
//! below: a bug here is not a bug in a data structure nothing calls yet.
//!
//! Two data structures, kept in step under one lock:
//!
//! - **Discovery state** (`nodes_by_id` / `alias_to_id` / `lingering_ids`):
//!   who exists and where, replaced wholesale by [`AtomicNodeRegistry::set`]
//!   on every discovery sync.
//! - **Observed state** (`observed` / `cpu_intersection` /
//!   `intersection_sent` / `sandbox_holders`): what nodes have said about
//!   themselves in heartbeats, updated incrementally by
//!   [`AtomicNodeRegistry::heartbeat`].
//!
//! [`AtomicNodeRegistry::heartbeat`] is also where the CPU-config
//! intersection link (`super::cpu_template`, CLAUDE.md's "must keep
//! working" chain) actually runs: `all_configs_ready` gates
//! `compute_intersection` on every node *that has ever reported a heartbeat
//! for the cluster* having reported a non-empty `cpu_config_json` —
//! deliberately not "every node discovery currently knows about," which is
//! why every heartbeat test below that exercises the gate seeds each node
//! with one empty-config heartbeat first, establishing the cluster's size
//! before asking whether it is complete. Porting the intersection algorithm
//! without this gate would trade "wait for everyone who might still report"
//! for "compute against whoever has reported so far," which produces a
//! *different*, incorrect intersection that only happens to match once
//! every node has checked in — and, per
//! `cpu_intersection_is_withheld_again_when_a_new_node_joins_the_cluster`
//! below, must be invalidated again the moment a node this cache never
//! accounted for reports in for the first time.
//!
//! # Deliberate divergence from Go: an all-empty [`AtomicNodeRegistry::set`]
//! # is not applied immediately
//!
//! Go's `AtomicNodeRegistry.Set` (`node_registry.go`) applies every call it
//! receives immediately and unconditionally, including one that reports zero
//! active and zero lingering nodes. [`EmptySyncGuard`] below is this port's
//! one intentional behavioral difference from that, and it exists because of
//! a difference in what calling `set` with an empty list can mean here that
//! it never could on the Go side:
//!
//! - [`super::kubernetes_discovery::nodes_from_endpoint_slices`]'s own doc
//!   comment records that an empty result is not only an error path — a
//!   `Service` rename or recreation, a mistyped label selector, or every
//!   endpoint transiently reporting non-`Serving` all produce a **successful**
//!   re-LIST with zero entries, indistinguishable at `set`'s call site from
//!   "the cluster has genuinely scaled to zero nodes." The watcher rebuild
//!   this crate performs after `MAX_CONSECUTIVE_WATCH_ERRORS` (see that
//!   module) republishes exactly this shape on its first `InitDone`.
//! - Applying that call immediately does not just clear `nodes_by_id`. `set`'s
//!   own stale-cleanup pass then walks every `observed` record whose node id
//!   is no longer in the new (empty) `nodes_by_id` — which, for an all-empty
//!   sync, is every record — and calls `clear_roster` + `observed.remove` +
//!   `intersection_sent.remove` on each one. That is not a discovery-only
//!   change: it is this process's only record of which sandboxes exist on
//!   which node evaporating in one call.
//! - And it does not self-heal by itself: once a node is out of
//!   `nodes_by_id`, its `Heartbeat` calls come back `NodeNotInRegistry`
//!   (`heartbeat`'s own `canonical_id`/lookup below) until the *next*
//!   successful, non-empty `set`. A registry emptied by one bad sync stays
//!   empty — and every consumer reading it as "this node is gone" — until
//!   discovery recovers.
//!
//! Go's port never had a live consumer for which any of that mattered — see
//! this module's own correction above. This build's `Heartbeat` gRPC surface
//! does, from Stage A on, and its downstream consequence is concrete, not
//! theoretical: `RemoteSandboxStub::confirm_or_defer` reads "this node is
//! gone" out of exactly this state and reports `RuntimeConfirmedGone`, which
//! the orchestrator's pause path (`src/orchestrator/service.rs`) uses to
//! *delete a paused sandbox's record from the cluster store*. A transient,
//! self-correcting discovery blip must not be able to trigger that.
//!
//! [`EmptySyncGuard`] is the mitigation: an all-empty `set` while the
//! registry currently holds nodes (or while an earlier all-empty `set` is
//! already pending) is treated as *suspected*, not authoritative — it is
//! withheld until either [`EmptySyncGuard::confirmations`] consecutive
//! all-empty calls have arrived, or [`EmptySyncGuard::window`] has elapsed
//! since the first one, whichever comes first. Either threshold reaching
//! zero degrades to Go's original "apply immediately" behavior, which is
//! deliberately still reachable rather than special-cased away — a real
//! scale-to-zero must still actually take effect eventually, and does, via
//! either threshold. A single non-empty `set` call at any point resets the
//! pending state entirely: discovery reporting real nodes again is itself
//! proof the empty answer was transient.
//!
//! What this module does *not* attempt: making `heartbeat` self-heal by
//! auto-registering a node the discovery-derived `nodes_by_id` does not
//! currently know about. `NodeNotInRegistry` — refusing a heartbeat from a
//! node discovery has not (yet, or no longer) vouched for — mirrors Go's own
//! `ErrNodeNotInRegistry` deliberately: discovery is this registry's sole
//! source of truth for *which node ids are real*, matching the same RBAC
//! -gated `EndpointSlice`/`Pod` watches, not an unauthenticated claim in a
//! heartbeat payload. Letting a heartbeat insert an id `set` never vouched
//! for would let anything that can reach the gRPC port assert its own
//! identity into the registry, bypassing discovery entirely — a materially
//! larger change to the trust model than delaying a wipe. With
//! [`EmptySyncGuard`] in place, the remaining window where a genuinely live
//! node's heartbeat is refused because of a wipe is bounded by the same
//! confirmation/window thresholds and self-corrects on discovery's own next
//! successful sync — a live [`super::kubernetes_discovery::KubernetesDiscovery`]
//! watch re-lists continuously, not on some external trigger, so that next
//! sync is not something an operator has to cause.

use std::collections::{HashMap, HashSet};
use std::sync::{OnceLock, RwLock};
use std::time::{Duration, SystemTime};

use tokio::sync::mpsc;

use crate::proto::scheduler::{
    HeartbeatRequest, NodeSnapshot, NodeStatus, ObservedNode, P2pEndpoint, P2pPeer,
};

use super::cpu_template::intersect_cpu_configs;
use super::redis::{
    PublishOp, StoredDiskMetric, StoredMachineInfo, StoredNodeSnapshot, StoredObservedRecord,
    StoredP2pEndpoint, StoredRosterEntry,
};
use super::types::{Node, Roster, RosterEntry};

/// Mirrors Go's `defaultObservedReportTTL`.
pub const DEFAULT_OBSERVED_REPORT_TTL: Duration = Duration::from_secs(30);

const ROSTER_ENTRY_DROPPED_METRIC: &str = "node_registry_roster_entry_dropped_total";
const EMPTY_SYNC_PENDING_METRIC: &str = "agentenv_api_node_registry_empty_sync_pending";

/// How many consecutive all-empty [`AtomicNodeRegistry::set`] calls, or how
/// much wall-clock time since the first one — whichever is reached first —
/// before an all-empty sync is treated as confirmed rather than suspected.
/// See this module's own doc comment for why an all-empty `set` is not
/// applied on the first call the way Go's `Set` applies it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmptySyncGuard {
    /// How many consecutive all-empty `set` calls confirm the wipe. `0` or
    /// `1` both mean "the first call confirms it" — the same as Go's
    /// unconditional-apply behavior, reachable deliberately rather than
    /// special-cased away (see the module doc).
    pub confirmations: u32,
    /// How long the first all-empty `set` call may stand unconfirmed before
    /// wall-clock time alone confirms it, independent of how many calls
    /// arrived. `Duration::ZERO` means the first call confirms it
    /// immediately, the same as `confirmations <= 1`.
    pub window: Duration,
}

/// Mirrors `services/scheduler/internal/kubernetes_discovery.go`'s own retry
/// cadence loosely: long enough that one transient re-LIST (a Service
/// recreation, a momentary label-selector mismatch) is very unlikely to
/// still be reporting empty, short enough that a real scale-to-zero is not
/// held stale for long.
pub const DEFAULT_EMPTY_SYNC_CONFIRMATIONS: u32 = 3;
pub const DEFAULT_EMPTY_SYNC_WINDOW: Duration = Duration::from_secs(60);

impl Default for EmptySyncGuard {
    fn default() -> Self {
        Self {
            confirmations: DEFAULT_EMPTY_SYNC_CONFIRMATIONS,
            window: DEFAULT_EMPTY_SYNC_WINDOW,
        }
    }
}

/// Mirrors Go's `ErrNodeNotInRegistry`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("node is not in scheduler node list")]
pub struct NodeNotInRegistry;

/// Mirrors Go's `ErrServiceInstanceMismatch`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("service instance mismatch")]
pub struct ServiceInstanceMismatch;

/// The behavior every node registry implementation must provide. Mirrors
/// Go's `NodeRegistry` interface (`node_registry.go:17`) method-for-method.
pub trait NodeRegistry: Send + Sync {
    /// Discovered nodes filtered by their derived status. See `NodeStatus`
    /// in `scheduler.proto` for the full status derivation table.
    fn snapshot(&self, allow_lingering: bool) -> Vec<Node>;
    fn contains(&self, node: &Node) -> bool;
    fn resolve(&self, node_id: &str) -> Option<Node>;
    /// On success, the second element of the tuple is the cluster's CPU
    /// intersection JSON, non-empty only the first time it has something new
    /// to tell this node.
    fn heartbeat(
        &self,
        req: &HeartbeatRequest,
        now: SystemTime,
    ) -> Result<(Node, String), NodeNotInRegistry>;
    fn list_observed(&self, cluster_id: &str, now: SystemTime) -> Vec<ObservedNode>;
    fn list_p2p_peers(
        &self,
        cluster_id: &str,
        backend: &str,
        exclude_node_id: &str,
        now: SystemTime,
    ) -> Vec<P2pPeer>;
    fn filter_p2p_peers(
        &self,
        cluster_id: &str,
        backend: &str,
        node_ids: &[String],
        exclude_node_id: &str,
        now: SystemTime,
    ) -> Vec<P2pPeer>;
    fn get_observed(
        &self,
        node_id: &str,
        cluster_id: &str,
        now: SystemTime,
    ) -> Option<ObservedNode>;
    /// The latest heartbeat-reported [`NodeSnapshot`] for a node. Unlike
    /// [`NodeRegistry::get_observed`], this does not derive status from
    /// discovery state or TTL — it returns only the raw snapshot, suitable
    /// for scheduling decisions. `None` if the node has never sent a
    /// heartbeat.
    fn peek_observed(&self, node_id: &str) -> Option<NodeSnapshot>;
    /// The sandbox roster a node reported in its last heartbeat, and when it
    /// reported it.
    fn roster_of(&self, node_id: &str) -> Option<(Vec<RosterEntry>, SystemTime)>;
    /// Every node whose last heartbeat listed this sandbox. More than one is
    /// normal during a cross-node takeover: the origin keeps its paused
    /// record until its own reconciliation drops it.
    fn nodes_holding(&self, sandbox_id: &str) -> Vec<String>;
    /// One roster per node this scheduler answers for in a cluster, sorted
    /// by node id.
    fn rosters_in_cluster(&self, cluster_id: &str) -> Vec<Roster>;
    fn unregister_observed(
        &self,
        node_id: &str,
        service_instance_id: &str,
    ) -> Result<(), ServiceInstanceMismatch>;
    /// 🔴 P4 (`node_registry::dump`'s D6 recompute): the CPU-config
    /// intersection actually cached and handed to a node over `Heartbeat`
    /// for `cluster_id` — the *gated* value `all_configs_ready` unlocks, not
    /// a fresh recompute. `None` both before any node in the cluster has
    /// reported a `cpu_config_json` and, crucially, while the cluster is
    /// only *partially* reported: unlike [`dump`](super::dump)'s own
    /// recompute (which runs the algorithm over whichever configs happen to
    /// be present right now, gate or no gate), this is `None` for exactly as
    /// long as production is honestly withholding an answer. The two can
    /// disagree — recompute non-empty, applied still `None` — whenever a
    /// node discovery already knows about has not yet sent its first
    /// `cpu_config_json`.
    fn applied_cpu_intersection(&self, cluster_id: &str) -> Option<String>;
}

#[derive(Debug, Clone, PartialEq)]
struct ObservedNodeRecord {
    /// Always carries `Some` snapshot once constructed by
    /// [`AtomicNodeRegistry::heartbeat`] — mirrors Go's `cloneSnapshot`,
    /// which never returns a nil pointer.
    node: ObservedNode,
    p2p_endpoint: Option<P2pEndpoint>,
    report_ttl: Duration,
    /// The roster from this node's last heartbeat, normalized.
    entries: Vec<RosterEntry>,
    last_seen: SystemTime,
}

/// The wire-format conversions for `super::redis`'s shared observed store.
/// Live here, not in `super::redis`, because `ObservedNodeRecord` is
/// private to this module — see `super::redis`'s own module doc for why
/// that boundary is deliberate. `SystemTime`/`Duration` become
/// millisecond/second integers; everything else is a straight field copy.
impl From<&ObservedNodeRecord> for StoredObservedRecord {
    fn from(record: &ObservedNodeRecord) -> Self {
        Self {
            node_id: record.node.node_id.clone(),
            endpoint: record.node.endpoint.clone(),
            cluster_id: record.node.cluster_id.clone(),
            service_instance_id: record.node.service_instance_id.clone(),
            version: record.node.version.clone(),
            commit: record.node.commit.clone(),
            machine_info: record
                .node
                .machine_info
                .as_ref()
                .map(|m| StoredMachineInfo {
                    cpu_family: m.cpu_family.clone(),
                    cpu_model: m.cpu_model.clone(),
                    cpu_model_name: m.cpu_model_name.clone(),
                    cpu_architecture: m.cpu_architecture.clone(),
                    cpu_config_json: m.cpu_config_json.clone(),
                }),
            snapshot: record.node.snapshot.as_ref().map(|s| StoredNodeSnapshot {
                status: s.status,
                allocated_cpu: s.allocated_cpu,
                allocated_memory_bytes: s.allocated_memory_bytes,
                cpu_percent: s.cpu_percent,
                cpu_count: s.cpu_count,
                memory_used_bytes: s.memory_used_bytes,
                memory_total_bytes: s.memory_total_bytes,
                disks: s
                    .disks
                    .iter()
                    .map(|d| StoredDiskMetric {
                        mount_point: d.mount_point.clone(),
                        device: d.device.clone(),
                        filesystem_type: d.filesystem_type.clone(),
                        used_bytes: d.used_bytes,
                        total_bytes: d.total_bytes,
                    })
                    .collect(),
                sandbox_count: s.sandbox_count,
                sandbox_starting_count: s.sandbox_starting_count,
                create_successes: s.create_successes,
                create_fails: s.create_fails,
                reported_at_unix_ms: s.reported_at_unix_ms,
                paused_sandbox_count: s.paused_sandbox_count,
                paused_allocated_cpu: s.paused_allocated_cpu,
                paused_allocated_memory_bytes: s.paused_allocated_memory_bytes,
            }),
            last_seen_unix_ms: record.node.last_seen_unix_ms,
            p2p_endpoint: record.p2p_endpoint.as_ref().map(|p| StoredP2pEndpoint {
                backend: p.backend.clone(),
                address: p.address.clone(),
            }),
            report_ttl_secs: record.report_ttl.as_secs(),
            entries: record
                .entries
                .iter()
                .map(|e| StoredRosterEntry {
                    sandbox_id: e.sandbox_id.clone(),
                    execution_id: e.execution_id.clone(),
                    projection_ttl_secs: e.projection_ttl.as_secs(),
                })
                .collect(),
        }
    }
}

impl From<StoredObservedRecord> for ObservedNodeRecord {
    fn from(stored: StoredObservedRecord) -> Self {
        let last_seen_unix_ms = stored.last_seen_unix_ms;
        Self {
            node: ObservedNode {
                node_id: stored.node_id,
                endpoint: stored.endpoint,
                cluster_id: stored.cluster_id,
                service_instance_id: stored.service_instance_id,
                version: stored.version,
                commit: stored.commit,
                machine_info: stored
                    .machine_info
                    .map(|m| crate::proto::scheduler::MachineInfo {
                        cpu_family: m.cpu_family,
                        cpu_model: m.cpu_model,
                        cpu_model_name: m.cpu_model_name,
                        cpu_architecture: m.cpu_architecture,
                        cpu_config_json: m.cpu_config_json,
                    }),
                snapshot: stored.snapshot.map(|s| NodeSnapshot {
                    status: s.status,
                    allocated_cpu: s.allocated_cpu,
                    allocated_memory_bytes: s.allocated_memory_bytes,
                    cpu_percent: s.cpu_percent,
                    cpu_count: s.cpu_count,
                    memory_used_bytes: s.memory_used_bytes,
                    memory_total_bytes: s.memory_total_bytes,
                    disks: s
                        .disks
                        .into_iter()
                        .map(|d| crate::proto::scheduler::DiskMetric {
                            mount_point: d.mount_point,
                            device: d.device,
                            filesystem_type: d.filesystem_type,
                            used_bytes: d.used_bytes,
                            total_bytes: d.total_bytes,
                        })
                        .collect(),
                    sandbox_count: s.sandbox_count,
                    sandbox_starting_count: s.sandbox_starting_count,
                    create_successes: s.create_successes,
                    create_fails: s.create_fails,
                    reported_at_unix_ms: s.reported_at_unix_ms,
                    paused_sandbox_count: s.paused_sandbox_count,
                    paused_allocated_cpu: s.paused_allocated_cpu,
                    paused_allocated_memory_bytes: s.paused_allocated_memory_bytes,
                }),
                last_seen_unix_ms,
            },
            p2p_endpoint: stored.p2p_endpoint.map(|p| P2pEndpoint {
                backend: p.backend,
                address: p.address,
            }),
            report_ttl: Duration::from_secs(stored.report_ttl_secs),
            entries: stored
                .entries
                .into_iter()
                .map(|e| RosterEntry {
                    sandbox_id: e.sandbox_id,
                    execution_id: e.execution_id,
                    projection_ttl: Duration::from_secs(e.projection_ttl_secs),
                })
                .collect(),
            // `unix_millis`'s inverse: a stored record's own `last_seen_unix_ms`
            // is always a value `unix_millis(SystemTime::now())` produced on
            // some replica, so this reconstructs the same instant rather than
            // reusing whatever `SystemTime::now()` happens to be at merge
            // time.
            last_seen: SystemTime::UNIX_EPOCH
                + Duration::from_millis(last_seen_unix_ms.max(0) as u64),
        }
    }
}

struct Inner {
    nodes_by_id: HashMap<String, Node>,
    /// Maps a node's previous identity (its pod name) to its current one —
    /// see [`Node`]'s doc comment on `pod_name`.
    alias_to_id: HashMap<String, String>,
    lingering_ids: HashSet<String>,
    /// Task's own "D2": node ids admitted to `nodes_by_id` by
    /// [`AtomicNodeRegistry::admit_pending`] rather than by [`AtomicNodeRegistry::set`] —
    /// discovered (an `EndpointSlice` entry names them), but not yet
    /// `Serving`. See `admit_pending`'s own doc comment for why this is a
    /// third bucket rather than folded into `lingering_ids`.
    pending_ids: HashSet<String>,
    observed_ttl: Duration,
    observed: HashMap<String, ObservedNodeRecord>,
    cpu_intersection: HashMap<String, String>,
    intersection_sent: HashSet<String>,
    /// The reverse of the rosters: sandbox id -> the nodes that reported
    /// holding it.
    sandbox_holders: HashMap<String, HashSet<String>>,
    /// [`EmptySyncGuard`]'s bookkeeping: `None` when there is no pending
    /// all-empty [`AtomicNodeRegistry::set`] call awaiting confirmation.
    /// `Some((since, confirmations))` while one is pending — `since` is when
    /// the *first* consecutive all-empty call arrived, `confirmations` how
    /// many have arrived since (this one included).
    pending_empty_sync: Option<(SystemTime, u32)>,
}

impl Inner {
    /// Maps whatever identity a caller used onto the one discovery currently
    /// uses for that node. Unknown identities are returned unchanged, so the
    /// caller still gets its "not in the registry" answer.
    ///
    /// The order is the guarantee, not an optimization: a real node is
    /// always resolved as itself, so an alias can never shadow one and
    /// attribute one machine's heartbeat to another.
    fn canonical_id(&self, node_id: &str) -> String {
        if self.nodes_by_id.contains_key(node_id) {
            return node_id.to_string();
        }
        if let Some(canonical) = self.alias_to_id.get(node_id) {
            return canonical.clone();
        }
        node_id.to_string()
    }

    fn invalidate_intersection(&mut self, cluster_id: &str) {
        self.cpu_intersection.remove(cluster_id);
        let stale: Vec<String> = self
            .observed
            .iter()
            .filter(|(_, record)| record.node.cluster_id == cluster_id)
            .map(|(node_id, _)| node_id.clone())
            .collect();
        for node_id in stale {
            self.intersection_sent.remove(&node_id);
        }
    }

    fn all_configs_ready(&self, cluster_id: &str) -> bool {
        let mut total = 0u32;
        let mut with_config = 0u32;
        for record in self.observed.values() {
            if record.node.cluster_id != cluster_id {
                continue;
            }
            total += 1;
            if record
                .node
                .machine_info
                .as_ref()
                .is_some_and(|m| !m.cpu_config_json.is_empty())
            {
                with_config += 1;
            }
        }
        total > 0 && with_config == total
    }

    /// Mirrors Go's `computeIntersectionLocked`: a malformed config makes
    /// this an empty string, never an error the caller has to handle — one
    /// node's bad JSON must not take down the whole cluster's intersection.
    fn compute_intersection(&self, cluster_id: &str) -> String {
        let jsons: Vec<String> = self
            .observed
            .values()
            .filter(|record| record.node.cluster_id == cluster_id)
            .filter_map(|record| {
                record
                    .node
                    .machine_info
                    .as_ref()
                    .map(|m| m.cpu_config_json.clone())
                    .filter(|j| !j.is_empty())
            })
            .collect();
        intersect_cpu_configs(&jsons).unwrap_or_default()
    }

    /// Moves a node from its previous roster to a new one, keeping the
    /// reverse index in step. Must be called before the new record replaces
    /// the old one in `self.observed`.
    fn apply_roster(&mut self, node_id: &str, roster: &[RosterEntry]) {
        let next: HashSet<&str> = roster.iter().map(|e| e.sandbox_id.as_str()).collect();

        if let Some(previous) = self.observed.get(node_id) {
            let dropped: Vec<String> = previous
                .entries
                .iter()
                .filter(|e| !next.contains(e.sandbox_id.as_str()))
                .map(|e| e.sandbox_id.clone())
                .collect();
            for sandbox_id in dropped {
                self.remove_holder(&sandbox_id, node_id);
            }
        }

        for sandbox_id in next {
            self.sandbox_holders
                .entry(sandbox_id.to_string())
                .or_default()
                .insert(node_id.to_string());
        }
    }

    /// Drops a node from the reverse index entirely.
    fn clear_roster(&mut self, node_id: &str) {
        if let Some(record) = self.observed.get(node_id) {
            let sandbox_ids: Vec<String> = record
                .entries
                .iter()
                .map(|e| e.sandbox_id.clone())
                .collect();
            for sandbox_id in sandbox_ids {
                self.remove_holder(&sandbox_id, node_id);
            }
        }
    }

    fn remove_holder(&mut self, sandbox_id: &str, node_id: &str) {
        if let Some(holders) = self.sandbox_holders.get_mut(sandbox_id) {
            holders.remove(node_id);
            if holders.is_empty() {
                self.sandbox_holders.remove(sandbox_id);
            }
        }
    }

    /// Records a fresh observed record for `node_id` — keeps the roster
    /// reverse index (`apply_roster`) and the CPU-intersection cache
    /// (`invalidate_intersection`) in step exactly the same way regardless
    /// of whether the record came from this replica's own heartbeat
    /// (`AtomicNodeRegistry::heartbeat`) or was adopted from another
    /// replica via `merge_remote` (`super::redis`'s shared observed
    /// store). Shared so the two ingestion paths cannot drift apart.
    fn ingest_observed(&mut self, node_id: &str, record: ObservedNodeRecord) {
        let prev_cpu = self
            .observed
            .get(node_id)
            .and_then(|r| r.node.machine_info.as_ref())
            .map(|m| m.cpu_config_json.clone())
            .unwrap_or_default();
        let existed = self.observed.contains_key(node_id);
        let cluster_id = record.node.cluster_id.clone();
        // 🔴 `is_some_and`, not "treat a missing `machine_info` as an empty
        // `cpu_config_json` and compare that": an omitted `machine_info`
        // (as opposed to one explicitly sent with a blank
        // `cpu_config_json`) must never itself count as a CPU change, or a
        // heartbeat/merge that carries no machine info at all would wrongly
        // invalidate a cluster's already-computed intersection every time
        // one lands. Mirrors `AtomicNodeRegistry::heartbeat`'s pre-refactor
        // inline check exactly — `heartbeat_delivers_intersection_exactly_once_per_node`
        // and `multi_cluster_cpu_intersections_are_independent` pin this.
        let cpu_changed = record
            .node
            .machine_info
            .as_ref()
            .is_some_and(|m| m.cpu_config_json != prev_cpu);

        self.apply_roster(node_id, &record.entries);
        self.observed.insert(node_id.to_string(), record);

        if !existed || cpu_changed {
            self.invalidate_intersection(&cluster_id);
        }
    }

    /// Adopts a record pulled from the shared observed store, but only if
    /// it is actually newer than what this replica already has — see
    /// `super::redis`'s own module doc ("last write wins by `last_seen`")
    /// for why: a node's heartbeat is pinned to one replica at a time, so a
    /// stale pull of this replica's *own* not-yet-superseded write must
    /// never regress `last_seen` backward, and a node that has since
    /// reconnected to a different replica must win once that fact reaches
    /// Redis.
    fn merge_remote(&mut self, node_id: &str, record: ObservedNodeRecord) {
        let should_adopt = self
            .observed
            .get(node_id)
            .is_none_or(|current| record.last_seen > current.last_seen);
        if should_adopt {
            self.ingest_observed(node_id, record);
        }
    }

    /// Recomputes and caches a cluster's CPU intersection if it is not
    /// already cached and every node that has ever reported for the
    /// cluster now has a config. Factored out of `AtomicNodeRegistry::heartbeat`
    /// so `AtomicNodeRegistry::merge_remote_snapshot` can apply the exact
    /// same gate: a node whose heartbeat only ever reached a *different*
    /// replica must complete `all_configs_ready` the same way a locally
    /// received one does, promptly on the merge that adopts it — not only
    /// on whatever this replica's own next local heartbeat happens to be.
    /// This is the correctness property the shared observed store exists
    /// for; see `super::redis`'s own module doc.
    fn refresh_intersection_if_ready(&mut self, cluster_id: &str) {
        if !self.cpu_intersection.contains_key(cluster_id) && self.all_configs_ready(cluster_id) {
            let result = self.compute_intersection(cluster_id);
            if !result.is_empty() {
                self.cpu_intersection.insert(cluster_id.to_string(), result);
            }
        }
    }

    /// Builds the external `ObservedNode` view for a heartbeat record,
    /// overriding the endpoint and status based on the current discovery
    /// state. See `NodeStatus` in `scheduler.proto` for the full derivation
    /// table.
    fn derive_observed_node_view(&self, record: &ObservedNodeRecord, now_ms: i64) -> ObservedNode {
        let mut out = record.node.clone();
        if out.snapshot.is_none() {
            out.snapshot = Some(NodeSnapshot::default());
        }
        let last_seen_unix_ms = out.last_seen_unix_ms;

        let known_node = self.nodes_by_id.get(&out.node_id);
        let in_discovery = known_node.is_some();
        let is_lingering = self.lingering_ids.contains(&out.node_id);

        if let Some(known) = known_node {
            if !known.endpoint.trim().is_empty() {
                out.endpoint = known.endpoint.clone();
            }
        }

        let ttl = if record.report_ttl > Duration::ZERO {
            record.report_ttl
        } else {
            DEFAULT_OBSERVED_REPORT_TTL
        };

        let snapshot = out.snapshot.as_mut().expect("snapshot defaulted above");
        if last_seen_unix_ms > 0 && (now_ms - last_seen_unix_ms) > ttl.as_millis() as i64 {
            snapshot.status = NodeStatus::Unhealthy as i32;
        } else if !in_discovery {
            snapshot.status = NodeStatus::Connecting as i32;
        } else if is_lingering {
            snapshot.status = NodeStatus::Lingering as i32;
        } else if snapshot.status() == NodeStatus::Unspecified {
            // Active — keep the status reported by the node, defaulting an
            // unset one to CONNECTING.
            snapshot.status = NodeStatus::Connecting as i32;
        }

        out
    }
}

/// A node registry backed by discovery syncs (`set`) and heartbeats
/// (`heartbeat`), guarded by one `RwLock`. Ports Go's `AtomicNodeRegistry`
/// (a `sync.RWMutex`-guarded struct, not an atomics-based one — the name is
/// Go's, kept for the same reason `Node`/`RichNode` keep theirs: so a reader
/// moving between the two implementations recognizes the type).
pub struct AtomicNodeRegistry {
    inner: RwLock<Inner>,
    empty_sync_guard: EmptySyncGuard,
    /// Set at most once, by [`Self::enable_shared_observed_publishing`] —
    /// `Some` for exactly as long as `super::redis`'s shared observed store
    /// is wired up (`[cluster].node_placement_source = "native"` with
    /// `[cluster.node_registry_store].backend = "redis"`; see
    /// `src/bin/aenv-api.rs`'s `wire_shared_node_observed_store`). `None`
    /// (the default for every other caller, including every test in this
    /// module) makes [`Self::publish_upsert`]/[`Self::publish_remove`]
    /// no-ops, so nothing about local behavior changes when no shared store
    /// is configured — the same "absent means untouched" discipline
    /// `--role all` and `--role node` already get for free by never
    /// constructing this type's native-placement wiring at all.
    publish_tx: OnceLock<mpsc::UnboundedSender<PublishOp>>,
}

impl AtomicNodeRegistry {
    pub fn new(nodes: Vec<Node>, observed_ttl: Duration) -> Self {
        Self::with_empty_sync_guard(nodes, observed_ttl, EmptySyncGuard::default())
    }

    /// Same as [`Self::new`], with an explicit [`EmptySyncGuard`] instead of
    /// [`EmptySyncGuard::default`] — for `start_native_node_registry`
    /// (`src/bin/aenv-api.rs`), which wires `[cluster.kubernetes_discovery]`'s
    /// configured thresholds through, and for tests exercising the guard's
    /// own timing.
    pub fn with_empty_sync_guard(
        nodes: Vec<Node>,
        observed_ttl: Duration,
        empty_sync_guard: EmptySyncGuard,
    ) -> Self {
        let ttl = if observed_ttl > Duration::ZERO {
            observed_ttl
        } else {
            DEFAULT_OBSERVED_REPORT_TTL
        };
        let registry = Self {
            inner: RwLock::new(Inner {
                nodes_by_id: HashMap::new(),
                alias_to_id: HashMap::new(),
                lingering_ids: HashSet::new(),
                pending_ids: HashSet::new(),
                observed_ttl: ttl,
                observed: HashMap::new(),
                cpu_intersection: HashMap::new(),
                intersection_sent: HashSet::new(),
                sandbox_holders: HashMap::new(),
                pending_empty_sync: None,
            }),
            empty_sync_guard,
            publish_tx: OnceLock::new(),
        };
        // `inner.nodes_by_id` starts empty regardless of `nodes`, so this
        // first call is never an empty-to-empty transition and the guard
        // never engages here — not even when `nodes` is itself empty (the
        // common case: every caller but `start_native_node_registry`'s
        // bootstrap constructs with `Vec::new()` and populates through the
        // first real discovery sync). `SystemTime::now()` is inert in
        // exactly that situation; a test exercising the guard's own timing
        // constructs with `Vec::new()` and drives `set` explicitly with its
        // own clock instead.
        registry.set(nodes, Vec::new(), SystemTime::now());
        registry
    }

    /// Turns on publishing of local `observed` writes to `super::redis`'s
    /// shared store. Returns the receiving half of the channel — the caller
    /// (`src/bin/aenv-api.rs`'s `wire_shared_node_observed_store`) drives
    /// `super::redis::run_shared_observed_sync` with it as a background
    /// task.
    ///
    /// Call exactly once, before this registry starts receiving heartbeats
    /// — mirrored by the `expect` below, which turns a second call into an
    /// immediate panic rather than a silently dropped first channel.
    pub fn enable_shared_observed_publishing(&self) -> mpsc::UnboundedReceiver<PublishOp> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.publish_tx
            .set(tx)
            .unwrap_or_else(|_| panic!("enable_shared_observed_publishing called twice"));
        rx
    }

    fn publish_upsert(&self, node_id: &str, record: &StoredObservedRecord) {
        let Some(tx) = self.publish_tx.get() else {
            return;
        };
        let _ = tx.send(PublishOp::Upsert {
            node_id: node_id.to_string(),
            record: Box::new(record.clone()),
        });
    }

    fn publish_remove(&self, node_id: &str) {
        let Some(tx) = self.publish_tx.get() else {
            return;
        };
        let _ = tx.send(PublishOp::Remove {
            node_id: node_id.to_string(),
        });
    }

    /// Merges a snapshot pulled from the shared observed store
    /// (`super::redis::SharedObservedStore::pull_all`) into this replica's
    /// local view. See `Inner::merge_remote` for the per-node adoption
    /// rule, and `super::redis`'s own module doc for why a pull never
    /// deletes an entry absent from the snapshot.
    pub fn merge_remote_snapshot(&self, remote: HashMap<String, StoredObservedRecord>) {
        let mut inner = self.inner.write().expect("node registry lock poisoned");
        let mut touched_clusters: HashSet<String> = HashSet::new();
        for (node_id, stored) in remote {
            let record = ObservedNodeRecord::from(stored);
            if !record.node.cluster_id.is_empty() {
                touched_clusters.insert(record.node.cluster_id.clone());
            }
            inner.merge_remote(&node_id, record);
        }
        // Same gate `heartbeat` applies inline, applied here too — a node
        // whose heartbeat only ever reached a different replica must
        // complete `all_configs_ready` promptly on the merge that adopts
        // it, not only whenever this replica's own next local heartbeat
        // happens to land. See `Inner::refresh_intersection_if_ready`'s own
        // doc.
        for cluster_id in touched_clusters {
            inner.refresh_intersection_if_ready(&cluster_id);
        }
    }

    /// Replaces the discovered node list. `active` nodes are serving and not
    /// terminating; `lingering` nodes are serving but terminating (graceful
    /// shutdown).
    ///
    /// An all-empty call (`active` and `lingering` both empty) is not
    /// applied on the first sighting when the registry currently holds
    /// nodes, or when an earlier all-empty call is already pending — see
    /// [`EmptySyncGuard`] and this module's own doc comment for why, and for
    /// what "applied" means once it is.
    pub fn set(&self, active: Vec<Node>, lingering: Vec<Node>, now: SystemTime) {
        let incoming_is_empty = active.is_empty() && lingering.is_empty();

        let mut inner = self.inner.write().expect("node registry lock poisoned");

        if incoming_is_empty {
            let currently_populated = !inner.nodes_by_id.is_empty();
            if currently_populated || inner.pending_empty_sync.is_some() {
                let (since, prior_confirmations) = inner.pending_empty_sync.unwrap_or((now, 0));
                let confirmations = prior_confirmations + 1;
                let elapsed = now.duration_since(since).unwrap_or(Duration::ZERO);
                let confirmed = confirmations >= self.empty_sync_guard.confirmations.max(1)
                    || elapsed >= self.empty_sync_guard.window;

                if !confirmed {
                    inner.pending_empty_sync = Some((since, confirmations));
                    metrics::gauge!(EMPTY_SYNC_PENDING_METRIC).set(1.0);
                    tracing::warn!(
                        confirmations,
                        elapsed_secs = elapsed.as_secs(),
                        needs_confirmations = self.empty_sync_guard.confirmations,
                        needs_window_secs = self.empty_sync_guard.window.as_secs(),
                        "node registry: discovery reported zero nodes; withholding the wipe of \
                         discovery/observed state until confirmed (see AtomicNodeRegistry::set's \
                         own doc comment)"
                    );
                    return;
                }

                tracing::warn!(
                    confirmations,
                    elapsed_secs = elapsed.as_secs(),
                    "node registry: an all-empty discovery sync is now confirmed; clearing \
                     discovery and observed state"
                );
            }
        }

        inner.pending_empty_sync = None;
        metrics::gauge!(EMPTY_SYNC_PENDING_METRIC).set(0.0);

        let mut by_id: HashMap<String, Node> =
            HashMap::with_capacity(active.len() + lingering.len());
        for node in &active {
            by_id.insert(node.id.clone(), node.clone());
        }
        let mut lingering_ids: HashSet<String> = HashSet::with_capacity(lingering.len());
        for node in &lingering {
            lingering_ids.insert(node.id.clone());
            by_id.insert(node.id.clone(), node.clone());
        }

        let mut aliases: HashMap<String, String> = HashMap::new();
        for node in by_id.values() {
            if node.pod_name.is_empty() || node.pod_name == node.id {
                continue;
            }
            aliases.insert(node.pod_name.clone(), node.id.clone());
        }

        inner.nodes_by_id = by_id;
        inner.alias_to_id = aliases;
        inner.lingering_ids = lingering_ids;

        let stale: Vec<(String, String)> = inner
            .observed
            .iter()
            .filter(|(node_id, _)| {
                !inner.nodes_by_id.contains_key(*node_id) && !inner.pending_ids.contains(*node_id)
            })
            .map(|(node_id, record)| (node_id.clone(), record.node.cluster_id.clone()))
            .collect();

        let mut affected_clusters: HashSet<String> = HashSet::new();
        let mut departed: Vec<String> = Vec::new();
        for (node_id, cluster_id) in stale {
            if !cluster_id.is_empty() {
                affected_clusters.insert(cluster_id);
            }
            inner.clear_roster(&node_id);
            inner.observed.remove(&node_id);
            inner.intersection_sent.remove(&node_id);
            departed.push(node_id);
        }
        for cluster_id in affected_clusters {
            inner.invalidate_intersection(&cluster_id);
        }
        drop(inner);
        // Same discovery-driven cleanup on every replica (the Kubernetes
        // watch feeding `set` is identical everywhere), so this is a
        // harmless, idempotent `HDEL` no matter which replica gets here
        // first — see `super::redis`'s own module doc ("absence never
        // deletes... removal instead rides discovery-driven cleanup").
        for node_id in &departed {
            self.publish_remove(node_id);
        }
    }

    /// Task's own "D2" (`docs/proposals/_sd-phase4-stageDE-remainder.md`'s
    /// D2, the race this port intentionally preserved through Stage A --
    /// see this module's own doc comment on why `heartbeat` never
    /// auto-registers an unknown id, and `grpc_service.rs`'s module doc for
    /// where the race was left as-is pending this fix).
    ///
    /// Admits nodes discovery has vouched for -- they name a real
    /// `EndpointSlice` endpoint with a routable address -- but that are not
    /// yet `Serving`, so [`AtomicNodeRegistry::heartbeat`] accepts them and
    /// a rolling update's freshly recreated pod does not have to wait out a
    /// readiness probe before its binding-store roster (task's own "D3")
    /// refreshes.
    ///
    /// Deliberately **not** folded into [`AtomicNodeRegistry::set`]'s
    /// `active`/`lingering` split:
    ///
    /// - `lingering` already carries an established, different meaning --
    ///   `heartbeat`'s own status-derivation forces a lingering node's
    ///   reported [`NodeStatus`] to [`NodeStatus::Lingering`] regardless of
    ///   what it self-reports, and
    ///   [`super::kubernetes_discovery::filter_nodes_by_pod_labels`]'s
    ///   `no_schedule_pod_ids` arm deliberately *promotes* a node into
    ///   `lingering` to pull it out of scheduling while keeping it
    ///   otherwise indistinguishable from a genuinely draining one.
    ///   Reusing that bucket for "just starting up" would make a brand new
    ///   node's observed status read `Lingering` the moment its first
    ///   heartbeat lands -- the opposite of what it is.
    /// - `active` gates scheduling eligibility today only by the absence of
    ///   a consumer (`filter.rs`'s own doc says as much), but this task
    ///   also builds `Schedule`, and `filter_unschedulable`'s documented
    ///   policy is "fail open on what we do not know" -- a node with no
    ///   heartbeat snapshot yet is *kept*, not dropped. Admitting a
    ///   not-yet-ready node into `active` would make it an immediately
    ///   eligible `Schedule` candidate before it can actually run a
    ///   sandbox, trading one race (a stale binding address) for a worse
    ///   one (routing a brand new sandbox onto a node that cannot yet run
    ///   it). `pending_ids` is checked by [`AtomicNodeRegistry::snapshot`]
    ///   the same way `lingering_ids` is, so a pending node is excluded
    ///   from `snapshot(false)` (the scheduling view) without touching its
    ///   reported status at all.
    ///
    /// Must be called after [`AtomicNodeRegistry::set`] on every discovery
    /// sync, never before it: `set` unconditionally replaces `nodes_by_id`
    /// from `active`/`lingering` alone, so a call before `set` would have
    /// its admission wiped out immediately by the very next `set` call in
    /// the same sync. `set`'s own stale-cleanup filter already spares
    /// `pending_ids` members' observed state for exactly this ordering
    /// (see its own diff), so a node that stays pending across consecutive
    /// syncs keeps its roster instead of having it wiped and re-seeded
    /// every cycle.
    ///
    /// `pending` fully replaces the previous pending set, mirroring `set`'s
    /// own replace-not-merge semantics: a node this call does not name this
    /// cycle -- because it became `Serving` (and is now in
    /// `active`/`lingering` from the `set` call just before this one) or
    /// because it genuinely disappeared -- is dropped from `pending_ids`,
    /// and if `set` did not already promote it to active/lingering, its
    /// identity and observed state are cleared the same way `set`'s own
    /// stale-cleanup pass clears a node discovery stopped reporting
    /// entirely.
    pub fn admit_pending(&self, pending: Vec<Node>) {
        let mut inner = self.inner.write().expect("node registry lock poisoned");

        let previous_pending = std::mem::take(&mut inner.pending_ids);
        let mut new_pending_ids: HashSet<String> = HashSet::with_capacity(pending.len());

        for node in &pending {
            if node.id.is_empty() {
                continue;
            }
            // `set()` already classified this id active/lingering this
            // cycle -- it has become Serving, so it must not be
            // re-admitted (and thereby re-excluded from scheduling) as
            // merely pending.
            if inner.nodes_by_id.contains_key(&node.id) {
                continue;
            }
            inner.nodes_by_id.insert(node.id.clone(), node.clone());
            if !node.pod_name.is_empty() && node.pod_name != node.id {
                inner
                    .alias_to_id
                    .insert(node.pod_name.clone(), node.id.clone());
            }
            new_pending_ids.insert(node.id.clone());
        }

        let mut affected_clusters: HashSet<String> = HashSet::new();
        let mut departed: Vec<String> = Vec::new();
        for node_id in previous_pending {
            if new_pending_ids.contains(&node_id) || inner.nodes_by_id.contains_key(&node_id) {
                // Still pending, or `set()` promoted it to active/lingering
                // this cycle -- either way it is not gone.
                continue;
            }
            inner.nodes_by_id.remove(&node_id);
            if let Some(record) = inner.observed.remove(&node_id) {
                if !record.node.cluster_id.is_empty() {
                    affected_clusters.insert(record.node.cluster_id.clone());
                }
            }
            inner.clear_roster(&node_id);
            inner.intersection_sent.remove(&node_id);
            departed.push(node_id);
        }
        for cluster_id in affected_clusters {
            inner.invalidate_intersection(&cluster_id);
        }

        inner.pending_ids = new_pending_ids;
        drop(inner);
        for node_id in &departed {
            self.publish_remove(node_id);
        }
    }
}

impl NodeRegistry for AtomicNodeRegistry {
    fn snapshot(&self, allow_lingering: bool) -> Vec<Node> {
        let inner = self.inner.read().expect("node registry lock poisoned");
        let mut result: Vec<Node> = inner
            .nodes_by_id
            .values()
            .filter(|n| {
                allow_lingering
                    || (!inner.lingering_ids.contains(&n.id) && !inner.pending_ids.contains(&n.id))
            })
            .cloned()
            .collect();
        result.sort_by(|a, b| a.id.cmp(&b.id));
        result
    }

    fn contains(&self, node: &Node) -> bool {
        let inner = self.inner.read().expect("node registry lock poisoned");
        let id = inner.canonical_id(&node.id);
        inner
            .nodes_by_id
            .get(&id)
            .is_some_and(|known| known.endpoint == node.endpoint)
    }

    fn resolve(&self, node_id: &str) -> Option<Node> {
        let inner = self.inner.read().expect("node registry lock poisoned");
        let id = inner.canonical_id(node_id);
        inner.nodes_by_id.get(&id).cloned()
    }

    fn heartbeat(
        &self,
        req: &HeartbeatRequest,
        now: SystemTime,
    ) -> Result<(Node, String), NodeNotInRegistry> {
        let now_ms = unix_millis(now);
        let mut machine_info = req.machine_info.clone();

        let mut inner = self.inner.write().expect("node registry lock poisoned");

        // Everything below keys off the canonical ID, never the one the node
        // sent: a node mid-upgrade still reports its pod name, and recording
        // it under that would give the same machine two observed
        // identities.
        let node_id = inner.canonical_id(&req.node_id);
        let node = inner
            .nodes_by_id
            .get(&node_id)
            .cloned()
            .ok_or(NodeNotInRegistry)?;

        if let Some(previous) = inner.observed.get(&node_id) {
            let prev_cpu = previous
                .node
                .machine_info
                .as_ref()
                .map(|m| m.cpu_config_json.clone())
                .unwrap_or_default();
            if let Some(mi) = machine_info.as_mut() {
                if mi.cpu_config_json.is_empty() {
                    mi.cpu_config_json = prev_cpu;
                }
            }
        }

        let mut snapshot = req.snapshot.clone().unwrap_or_default();
        if snapshot.reported_at_unix_ms == 0 {
            snapshot.reported_at_unix_ms = now_ms;
        }
        if snapshot.status() == NodeStatus::Unspecified {
            snapshot.status = NodeStatus::Connecting as i32;
        }

        let entries = normalize_heartbeat_roster(req);

        let record = ObservedNodeRecord {
            node: ObservedNode {
                node_id: node_id.clone(),
                endpoint: node.endpoint.clone(),
                cluster_id: req.cluster_id.clone(),
                service_instance_id: req.service_instance_id.clone(),
                version: req.version.clone(),
                commit: req.commit.clone(),
                machine_info: machine_info.clone(),
                snapshot: Some(snapshot),
                last_seen_unix_ms: now_ms,
            },
            p2p_endpoint: req.p2p_endpoint.clone(),
            report_ttl: inner.observed_ttl,
            entries,
            last_seen: now,
        };

        let cluster_id = req.cluster_id.clone();
        // Published to the shared observed store (when one is configured)
        // below, after the local write — see `Self::publish_upsert`'s own
        // doc. Cloned because `ingest_observed` takes ownership of `record`
        // to insert it locally.
        let published = StoredObservedRecord::from(&record);
        inner.ingest_observed(&node_id, record);
        inner.refresh_intersection_if_ready(&cluster_id);

        let cached_intersection = inner.cpu_intersection.get(&cluster_id).cloned();
        let intersection_to_report = match cached_intersection {
            Some(intersection) if !inner.intersection_sent.contains(&node_id) => {
                inner.intersection_sent.insert(node_id.clone());
                Some(intersection)
            }
            _ => None,
        };
        drop(inner);

        // 🔴 Outside the write lock, and unconditional on whether a shared
        // store is even configured (`Self::publish_upsert` is a no-op then)
        // — a heartbeat is published every time it is locally received, not
        // only when something changed, because `last_seen`/resource usage
        // legitimately changes on every call and every reader of the shared
        // view (`list_observed`, `applied_cpu_intersection`'s own
        // `all_configs_ready` gate on another replica) needs that freshness.
        self.publish_upsert(&node_id, &published);

        match intersection_to_report {
            Some(intersection) => Ok((node, intersection)),
            None => Ok((node, String::new())),
        }
    }

    fn list_observed(&self, cluster_id: &str, now: SystemTime) -> Vec<ObservedNode> {
        let now_ms = unix_millis(now);
        let trimmed_cluster = cluster_id.trim();
        let inner = self.inner.read().expect("node registry lock poisoned");
        inner
            .observed
            .values()
            .filter(|record| {
                trimmed_cluster.is_empty() || record.node.cluster_id == trimmed_cluster
            })
            .map(|record| inner.derive_observed_node_view(record, now_ms))
            .collect()
    }

    fn list_p2p_peers(
        &self,
        cluster_id: &str,
        backend: &str,
        exclude_node_id: &str,
        now: SystemTime,
    ) -> Vec<P2pPeer> {
        let inner = self.inner.read().expect("node registry lock poisoned");
        filter_p2p_peers_locked(&inner, cluster_id, backend, None, exclude_node_id, now)
    }

    fn filter_p2p_peers(
        &self,
        cluster_id: &str,
        backend: &str,
        node_ids: &[String],
        exclude_node_id: &str,
        now: SystemTime,
    ) -> Vec<P2pPeer> {
        let allowed: HashSet<String> = node_ids.iter().cloned().collect();
        if allowed.is_empty() {
            return Vec::new();
        }
        let inner = self.inner.read().expect("node registry lock poisoned");
        filter_p2p_peers_locked(
            &inner,
            cluster_id,
            backend,
            Some(&allowed),
            exclude_node_id,
            now,
        )
    }

    fn get_observed(
        &self,
        node_id: &str,
        cluster_id: &str,
        now: SystemTime,
    ) -> Option<ObservedNode> {
        let now_ms = unix_millis(now);
        let trimmed_cluster = cluster_id.trim();
        let inner = self.inner.read().expect("node registry lock poisoned");
        let record = inner.observed.get(node_id)?;
        if !trimmed_cluster.is_empty() && record.node.cluster_id != trimmed_cluster {
            return None;
        }
        Some(inner.derive_observed_node_view(record, now_ms))
    }

    fn peek_observed(&self, node_id: &str) -> Option<NodeSnapshot> {
        let inner = self.inner.read().expect("node registry lock poisoned");
        inner.observed.get(node_id)?.node.snapshot.clone()
    }

    fn roster_of(&self, node_id: &str) -> Option<(Vec<RosterEntry>, SystemTime)> {
        let inner = self.inner.read().expect("node registry lock poisoned");
        let record = inner.observed.get(node_id)?;
        Some((record.entries.clone(), record.last_seen))
    }

    fn nodes_holding(&self, sandbox_id: &str) -> Vec<String> {
        let sandbox_id = sandbox_id.trim();
        if sandbox_id.is_empty() {
            return Vec::new();
        }
        let inner = self.inner.read().expect("node registry lock poisoned");
        let Some(holders) = inner.sandbox_holders.get(sandbox_id) else {
            return Vec::new();
        };
        let mut node_ids: Vec<String> = holders.iter().cloned().collect();
        node_ids.sort();
        node_ids
    }

    fn rosters_in_cluster(&self, cluster_id: &str) -> Vec<Roster> {
        let wanted = normalize_cluster_id(cluster_id);
        let inner = self.inner.read().expect("node registry lock poisoned");

        let mut rosters = Vec::with_capacity(inner.observed.len() + inner.nodes_by_id.len());
        for (node_id, record) in &inner.observed {
            if !wanted.is_empty() && normalize_cluster_id(&record.node.cluster_id) != wanted {
                continue;
            }
            rosters.push(Roster {
                node_id: node_id.clone(),
                entries: record.entries.clone(),
                last_seen: Some(record.last_seen),
            });
        }
        for node_id in inner.nodes_by_id.keys() {
            if inner.observed.contains_key(node_id) {
                continue;
            }
            rosters.push(Roster {
                node_id: node_id.clone(),
                entries: Vec::new(),
                last_seen: None,
            });
        }
        rosters.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        rosters
    }

    fn unregister_observed(
        &self,
        node_id: &str,
        service_instance_id: &str,
    ) -> Result<(), ServiceInstanceMismatch> {
        let mut inner = self.inner.write().expect("node registry lock poisoned");
        let Some(record) = inner.observed.get(node_id) else {
            return Ok(());
        };
        if record.node.service_instance_id != service_instance_id {
            return Err(ServiceInstanceMismatch);
        }
        let cluster_id = record.node.cluster_id.clone();
        inner.clear_roster(node_id);
        inner.observed.remove(node_id);
        inner.invalidate_intersection(&cluster_id);
        drop(inner);
        self.publish_remove(node_id);
        Ok(())
    }

    fn applied_cpu_intersection(&self, cluster_id: &str) -> Option<String> {
        let inner = self.inner.read().expect("node registry lock poisoned");
        inner.cpu_intersection.get(cluster_id).cloned()
    }
}

fn filter_p2p_peers_locked(
    inner: &Inner,
    cluster_id: &str,
    backend: &str,
    allowed: Option<&HashSet<String>>,
    exclude_node_id: &str,
    now: SystemTime,
) -> Vec<P2pPeer> {
    let now_ms = unix_millis(now);
    let trimmed_cluster = cluster_id.trim();
    let trimmed_backend = backend.trim();
    let trimmed_exclude = exclude_node_id.trim();

    let mut peers = Vec::new();
    for record in inner.observed.values() {
        if !trimmed_cluster.is_empty() && record.node.cluster_id != trimmed_cluster {
            continue;
        }
        let node = inner.derive_observed_node_view(record, now_ms);
        if let Some(allowed) = allowed {
            if !allowed.contains(&node.node_id) {
                continue;
            }
        }
        if node.node_id == trimmed_exclude {
            continue;
        }
        let status = node
            .snapshot
            .as_ref()
            .map(|s| s.status())
            .unwrap_or(NodeStatus::Unspecified);
        if status != NodeStatus::Ready {
            continue;
        }
        let Some(endpoint) = record.p2p_endpoint.as_ref() else {
            continue;
        };
        if endpoint.backend.is_empty() || endpoint.address.is_empty() {
            continue;
        }
        if !trimmed_backend.is_empty() && endpoint.backend != trimmed_backend {
            continue;
        }
        peers.push(P2pPeer {
            node_id: node.node_id.clone(),
            endpoint: Some(endpoint.clone()),
        });
    }
    peers
}

/// Puts two cluster ids in a comparable form. Both sides are UUID text that
/// travelled through a config file and an environment variable, and a
/// difference in case or padding between them would silently empty the
/// roster side of every comparison.
fn normalize_cluster_id(cluster_id: &str) -> String {
    cluster_id.trim().to_lowercase()
}

/// The one place a heartbeat's roster becomes the registry's.
fn normalize_heartbeat_roster(req: &HeartbeatRequest) -> Vec<RosterEntry> {
    roster_from_heartbeat(req).0
}

/// Collapses the two generations of the roster field into one shape, and
/// says which one it used.
///
/// ```text
/// roster present                    -> use it
/// roster empty, sandbox_ids present -> use those, with no incarnations
/// both empty                        -> a genuinely empty roster
/// ```
///
/// The fallback is not politeness towards old builds, it is the difference
/// between a rolling upgrade and an outage — see `node_registry.go`'s
/// `rosterFromHeartbeat` doc comment for the full argument.
///
/// 🔴 `sandbox_ids` is read deliberately, mirroring Go's
/// `//nolint:staticcheck` on the same line: the deprecated field is the
/// rollout fallback, not dead code to warn about.
#[allow(deprecated)]
pub(crate) fn roster_from_heartbeat(req: &HeartbeatRequest) -> (Vec<RosterEntry>, bool) {
    if !req.roster.is_empty() {
        let mut out = Vec::with_capacity(req.roster.len());
        let mut seen: HashSet<String> = HashSet::with_capacity(req.roster.len());
        for item in &req.roster {
            let sandbox_id = item.sandbox_id.trim().to_string();
            if sandbox_id.is_empty() {
                continue;
            }
            if !seen.insert(sandbox_id.clone()) {
                continue;
            }
            out.push(RosterEntry {
                sandbox_id,
                execution_id: normalize_execution_id(&item.execution_id),
                projection_ttl: projection_ttl_from_secs(item.projection_ttl_secs),
            });
        }
        if out.is_empty() {
            return (Vec::new(), false);
        }
        return (out, false);
    }

    if req.sandbox_ids.is_empty() {
        return (Vec::new(), false);
    }
    let mut out = Vec::with_capacity(req.sandbox_ids.len());
    let mut seen: HashSet<String> = HashSet::with_capacity(req.sandbox_ids.len());
    for sandbox_id in &req.sandbox_ids {
        let sandbox_id = sandbox_id.trim().to_string();
        if sandbox_id.is_empty() {
            continue;
        }
        if !seen.insert(sandbox_id.clone()) {
            continue;
        }
        out.push(RosterEntry {
            sandbox_id,
            execution_id: String::new(),
            projection_ttl: Duration::ZERO,
        });
    }
    if out.is_empty() {
        (Vec::new(), false)
    } else {
        (out, true)
    }
}

/// The one conversion from the wire's whole seconds. A zero value becomes
/// `Duration::ZERO`, which every reader of this treats as "no budget
/// offered" and never as "no expiry".
fn projection_ttl_from_secs(secs: u32) -> Duration {
    if secs == 0 {
        Duration::ZERO
    } else {
        Duration::from_secs(u64::from(secs))
    }
}

/// Trims, checks the shape, and lower-cases. A value that is not a canonical
/// UUID is dropped rather than carried — arbitration orders these as
/// strings, so anything that is not the shape it expects would order
/// unpredictably against everything else.
fn normalize_execution_id(raw: &str) -> String {
    let (normalized, reason) = normalize_execution_id_reason(raw);
    if let Some(reason) = reason {
        record_roster_dropped(reason);
    }
    normalized
}

/// The same rule as [`normalize_execution_id`] without the counter, and
/// returns why it dropped a value instead of counting it.
pub(crate) fn normalize_execution_id_reason(raw: &str) -> (String, Option<&'static str>) {
    let normalized = raw.trim().to_lowercase();
    if normalized.is_empty() {
        return (String::new(), Some("no_execution"));
    }
    if !is_canonical_uuid_text(&normalized) {
        return (String::new(), Some("bad_uuid"));
    }
    (normalized, None)
}

/// The shape check, deliberately narrow: ids come from a type whose display
/// form is always canonical, so anything else on this path came from a
/// caller this build does not recognize.
fn is_canonical_uuid_text(s: &str) -> bool {
    let bytes = s.as_bytes();
    if bytes.len() != 36 {
        return false;
    }
    for (i, &c) in bytes.iter().enumerate() {
        match i {
            8 | 13 | 18 | 23 => {
                if c != b'-' {
                    return false;
                }
            }
            _ => {
                let is_hex = c.is_ascii_digit() || c.is_ascii_hexdigit();
                if !is_hex {
                    return false;
                }
            }
        }
    }
    true
}

fn record_roster_dropped(reason: &'static str) {
    metrics::counter!(ROSTER_ENTRY_DROPPED_METRIC, "reason" => reason).increment(1);
}

pub(crate) fn unix_millis(t: SystemTime) -> i64 {
    match t.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(d) => d.as_millis() as i64,
        Err(e) => -(e.duration().as_millis() as i64),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::scheduler::{MachineInfo, P2pEndpoint as P2pEndpointProto};

    fn unix(secs: u64) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(secs)
    }

    fn node(id: &str, endpoint: &str) -> Node {
        Node {
            id: id.to_string(),
            endpoint: endpoint.to_string(),
            pod_name: String::new(),
        }
    }

    fn ready_heartbeat(node_id: &str, cluster_id: &str) -> HeartbeatRequest {
        HeartbeatRequest {
            node_id: node_id.to_string(),
            cluster_id: cluster_id.to_string(),
            service_instance_id: format!("svc-{node_id}"),
            snapshot: Some(NodeSnapshot {
                status: NodeStatus::Ready as i32,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    /// A single-leaf, single-register cpu_config_json with the given eax
    /// bitmap — enough surface for the intersection tests below without
    /// depending on `cpu_template`'s private test helpers.
    fn cpu_config_json(eax: u32) -> String {
        format!(
            r#"{{"kvm_capabilities":[],"cpuid_modifiers":[{{"leaf":"0x1","subleaf":"0x0","flags":0,"modifiers":[{{"register":"eax","bitmap":"0b{eax:032b}"}}]}}],"msr_modifiers":[]}}"#
        )
    }

    fn heartbeat_with_config(
        registry: &AtomicNodeRegistry,
        node_id: &str,
        cluster_id: &str,
        cpu_json: &str,
    ) -> String {
        let machine_info = if cpu_json.is_empty() {
            None
        } else {
            Some(MachineInfo {
                cpu_config_json: cpu_json.to_string(),
                ..Default::default()
            })
        };
        let (_, intersection) = registry
            .heartbeat(
                &HeartbeatRequest {
                    node_id: node_id.to_string(),
                    cluster_id: cluster_id.to_string(),
                    service_instance_id: format!("svc-{node_id}"),
                    machine_info,
                    ..Default::default()
                },
                unix(100),
            )
            .expect("heartbeat");
        intersection
    }

    // Mirrors the legacy `sandbox_ids` roster path deliberately — see
    // `roster_from_heartbeat`'s doc comment.
    #[allow(deprecated)]
    fn heartbeat_with_roster(
        registry: &AtomicNodeRegistry,
        node_id: &str,
        cluster_id: &str,
        now: SystemTime,
        sandbox_ids: &[&str],
    ) {
        registry
            .heartbeat(
                &HeartbeatRequest {
                    node_id: node_id.to_string(),
                    cluster_id: cluster_id.to_string(),
                    service_instance_id: format!("svc-{node_id}"),
                    snapshot: Some(NodeSnapshot {
                        status: NodeStatus::Ready as i32,
                        ..Default::default()
                    }),
                    sandbox_ids: sandbox_ids.iter().map(|s| s.to_string()).collect(),
                    ..Default::default()
                },
                now,
            )
            .expect("heartbeat");
    }

    fn roster_ids(entries: &[RosterEntry]) -> Vec<String> {
        entries.iter().map(|e| e.sandbox_id.clone()).collect()
    }

    /// A minimal `StoredObservedRecord` for `merge_remote_snapshot` tests —
    /// simulates what `super::redis::SharedObservedStore::pull_all` would
    /// hand back for a node whose heartbeat landed on a *different*
    /// replica than the one running the test.
    fn stored_record(
        node_id: &str,
        cluster_id: &str,
        cpu_config_json: Option<String>,
        last_seen: SystemTime,
    ) -> StoredObservedRecord {
        StoredObservedRecord {
            node_id: node_id.to_string(),
            endpoint: format!("http://{node_id}"),
            cluster_id: cluster_id.to_string(),
            service_instance_id: format!("svc-{node_id}"),
            version: String::new(),
            commit: String::new(),
            machine_info: cpu_config_json.map(|cpu_config_json| StoredMachineInfo {
                cpu_family: String::new(),
                cpu_model: String::new(),
                cpu_model_name: String::new(),
                cpu_architecture: String::new(),
                cpu_config_json,
            }),
            snapshot: None,
            last_seen_unix_ms: unix_millis(last_seen),
            p2p_endpoint: None,
            report_ttl_secs: 30,
            entries: Vec::new(),
        }
    }

    // ---- node_registry_test.go ----

    #[test]
    fn list_observed_filters_by_cluster() {
        let registry = AtomicNodeRegistry::new(
            vec![
                node("node-a", "http://node-a"),
                node("node-b", "http://node-b"),
            ],
            Duration::from_secs(30),
        );
        let now = unix(100);
        registry
            .heartbeat(&ready_heartbeat("node-a", "cluster-a"), now)
            .unwrap();
        registry
            .heartbeat(&ready_heartbeat("node-b", "cluster-b"), now)
            .unwrap();

        let nodes = registry.list_observed("cluster-a", now);
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].node_id, "node-a");
    }

    #[test]
    fn heartbeat_rejects_unknown_node() {
        let registry = AtomicNodeRegistry::new(Vec::new(), Duration::from_secs(30));
        let err = registry
            .heartbeat(&ready_heartbeat("node-a", "cluster-a"), unix(100))
            .unwrap_err();
        assert_eq!(err, NodeNotInRegistry);
    }

    #[test]
    fn observed_node_becomes_unhealthy_after_ttl() {
        let registry = AtomicNodeRegistry::new(
            vec![node("node-a", "http://node-a")],
            Duration::from_secs(1),
        );
        let start = unix(100);
        registry
            .heartbeat(&ready_heartbeat("node-a", "cluster-a"), start)
            .unwrap();

        let observed = registry
            .get_observed("node-a", "cluster-a", start + Duration::from_secs(2))
            .expect("observed node");
        assert_eq!(observed.snapshot.unwrap().status(), NodeStatus::Unhealthy);
    }

    #[test]
    fn observed_node_uses_latest_known_endpoint() {
        let registry = AtomicNodeRegistry::new(
            vec![node("node-a", "http://node-a")],
            Duration::from_secs(30),
        );
        let now = unix(100);
        registry
            .heartbeat(&ready_heartbeat("node-a", "cluster-a"), now)
            .unwrap();

        registry.set(vec![node("node-a", "http://node-a-new")], Vec::new(), now);

        let observed = registry
            .get_observed("node-a", "cluster-a", now)
            .expect("observed node");
        assert_eq!(observed.endpoint, "http://node-a-new");
    }

    #[test]
    fn heartbeat_stores_p2p_endpoint_for_peer_listing() {
        let registry = AtomicNodeRegistry::new(
            vec![node("node-a", "http://node-a")],
            Duration::from_secs(30),
        );
        let now = unix(100);
        let endpoint = P2pEndpointProto {
            backend: "iroh".to_string(),
            address: r#"{"id":"node-id"}"#.to_string(),
        };
        registry
            .heartbeat(
                &HeartbeatRequest {
                    p2p_endpoint: Some(endpoint.clone()),
                    ..ready_heartbeat("node-a", "cluster-a")
                },
                now,
            )
            .unwrap();

        let peers = registry.list_p2p_peers("cluster-a", "iroh", "", now);
        assert_eq!(peers.len(), 1);
        let got = peers[0].endpoint.clone().unwrap();
        assert_eq!(got.backend, endpoint.backend);
        assert_eq!(got.address, endpoint.address);

        // ObservedNode carries no p2p_endpoint field at all (unlike Go, which
        // asserts this via proto reflection) — the type simply has none.
        registry
            .get_observed("node-a", "cluster-a", now)
            .expect("observed node");
    }

    #[test]
    fn unregister_removes_p2p_endpoint_peer() {
        let registry = AtomicNodeRegistry::new(
            vec![node("node-a", "http://node-a")],
            Duration::from_secs(30),
        );
        let now = unix(100);
        registry
            .heartbeat(
                &HeartbeatRequest {
                    p2p_endpoint: Some(P2pEndpointProto {
                        backend: "iroh".to_string(),
                        address: r#"{"id":"node-id"}"#.to_string(),
                    }),
                    ..ready_heartbeat("node-a", "cluster-a")
                },
                now,
            )
            .unwrap();
        registry
            .unregister_observed("node-a", "svc-node-a")
            .unwrap();

        assert!(registry
            .list_p2p_peers("cluster-a", "iroh", "", now)
            .is_empty());
    }

    #[test]
    fn list_p2p_peers_returns_only_ready_matching_peers() {
        let registry = AtomicNodeRegistry::new(
            vec![
                node("node-a", "http://node-a"),
                node("node-b", "http://node-b"),
                node("node-c", "http://node-c"),
            ],
            DEFAULT_OBSERVED_REPORT_TTL,
        );
        let now = unix(100);
        for node_id in ["node-a", "node-b"] {
            registry
                .heartbeat(
                    &HeartbeatRequest {
                        p2p_endpoint: Some(P2pEndpointProto {
                            backend: "iroh".to_string(),
                            address: format!("{node_id}-iroh-endpoint"),
                        }),
                        ..ready_heartbeat(node_id, "cluster-1")
                    },
                    now,
                )
                .unwrap();
        }
        registry
            .heartbeat(
                &HeartbeatRequest {
                    p2p_endpoint: Some(P2pEndpointProto {
                        backend: "other".to_string(),
                        address: "node-c-other-endpoint".to_string(),
                    }),
                    ..ready_heartbeat("node-c", "cluster-1")
                },
                now,
            )
            .unwrap();

        let peers = registry.list_p2p_peers("cluster-1", "iroh", "node-a", now);
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].node_id, "node-b");
        assert_eq!(
            peers[0].endpoint.as_ref().unwrap().address,
            "node-b-iroh-endpoint"
        );
    }

    #[test]
    fn list_p2p_peers_drops_expired_and_unregistered_nodes() {
        let registry = AtomicNodeRegistry::new(
            vec![
                node("node-a", "http://node-a"),
                node("node-b", "http://node-b"),
            ],
            Duration::from_secs(1),
        );
        let start = unix(100);
        for node_id in ["node-a", "node-b"] {
            registry
                .heartbeat(
                    &HeartbeatRequest {
                        p2p_endpoint: Some(P2pEndpointProto {
                            backend: "iroh".to_string(),
                            address: format!("{node_id}-iroh-endpoint"),
                        }),
                        ..ready_heartbeat(node_id, "cluster-1")
                    },
                    start,
                )
                .unwrap();
        }
        registry
            .unregister_observed("node-b", "svc-node-b")
            .unwrap();

        let peers =
            registry.list_p2p_peers("cluster-1", "iroh", "", start + Duration::from_secs(2));
        assert!(peers.is_empty());
    }

    #[test]
    fn list_observed_returns_empty_when_no_nodes() {
        let registry = AtomicNodeRegistry::new(Vec::new(), Duration::from_secs(30));
        assert!(registry.list_observed("", unix(0)).is_empty());
    }

    #[test]
    fn list_observed_returns_empty_when_cluster_filter_matches_nothing() {
        let registry = AtomicNodeRegistry::new(
            vec![node("node-a", "http://node-a")],
            Duration::from_secs(30),
        );
        let now = unix(100);
        registry
            .heartbeat(&ready_heartbeat("node-a", "cluster-a"), now)
            .unwrap();
        assert!(registry.list_observed("cluster-z", now).is_empty());
    }

    #[test]
    fn list_observed_becomes_unhealthy_after_ttl() {
        let registry = AtomicNodeRegistry::new(
            vec![node("node-a", "http://node-a")],
            Duration::from_secs(1),
        );
        let start = unix(100);
        registry
            .heartbeat(&ready_heartbeat("node-a", "cluster-a"), start)
            .unwrap();

        let observed = registry.list_observed("", start + Duration::from_secs(2));
        assert_eq!(observed.len(), 1);
        assert_eq!(
            observed[0].snapshot.clone().unwrap().status(),
            NodeStatus::Unhealthy
        );
    }

    #[test]
    fn lingering_node_becomes_unhealthy_after_ttl() {
        let registry = AtomicNodeRegistry::new(Vec::new(), Duration::from_secs(1));
        let start = unix(100);
        registry.set(Vec::new(), vec![node("node-a", "http://node-a")], start);
        registry
            .heartbeat(&ready_heartbeat("node-a", "cluster-a"), start)
            .unwrap();

        let observed = registry.get_observed("node-a", "", start).unwrap();
        assert_eq!(
            observed.snapshot.clone().unwrap().status(),
            NodeStatus::Lingering
        );

        let observed = registry
            .get_observed("node-a", "", start + Duration::from_secs(2))
            .unwrap();
        assert_eq!(observed.snapshot.unwrap().status(), NodeStatus::Unhealthy);
    }

    #[test]
    fn heartbeat_returns_cpu_intersection_for_single_node() {
        let registry = AtomicNodeRegistry::new(
            vec![node("node-a", "http://node-a")],
            Duration::from_secs(30),
        );
        let cfg = cpu_config_json(0xFF);
        let result = heartbeat_with_config(&registry, "node-a", "cluster-1", &cfg);
        assert!(!result.is_empty());
        // 🔴 P6-f: exact string comparison, not `extract_eax`'s
        // parse-into-`u32`. The self-intersection of one config is defined
        // to be that config byte-for-byte (`cpu_template.rs`'s own golden
        // test), and a `u32` round-trip cannot tell `"0b1111"` apart from
        // `"0b00000000000000000000000000001111"` — both parse to 15 — so a
        // regression that dropped zero-padding from the real formatter
        // would pass this assertion silently while failing Go's own
        // byte-for-byte test.
        assert_eq!(result, cfg);
    }

    #[test]
    fn heartbeat_withholds_cpu_intersection_until_all_nodes_ready() {
        let registry = AtomicNodeRegistry::new(
            vec![
                node("node-a", "http://node-a"),
                node("node-b", "http://node-b"),
            ],
            Duration::from_secs(30),
        );
        let cfg = cpu_config_json(0xFF);
        heartbeat_with_config(&registry, "node-a", "cluster-1", "");
        heartbeat_with_config(&registry, "node-b", "cluster-1", "");

        let result = heartbeat_with_config(&registry, "node-a", "cluster-1", &cfg);
        assert!(result.is_empty());
    }

    /// 🔴 P4: `applied_cpu_intersection` is the gated value production
    /// actually cached and would hand a node on its next heartbeat — not a
    /// fresh recompute over whoever happens to have reported a non-empty
    /// config right now. While node-b has heartbeated but not yet with a
    /// `cpu_config_json` (the same "cluster size established, one member
    /// still pending" state `heartbeat_withholds_cpu_intersection_until_all_nodes_ready`
    /// above proves withholds the *heartbeat reply*), this must also stay
    /// `None` — this is the accessor `node_registry::dump`'s D6 recompute
    /// is checked against.
    #[test]
    fn applied_cpu_intersection_stays_none_while_the_cluster_is_only_partially_reported() {
        let registry = AtomicNodeRegistry::new(
            vec![
                node("node-a", "http://node-a"),
                node("node-b", "http://node-b"),
            ],
            Duration::from_secs(30),
        );
        let cfg_a = cpu_config_json(0xFF);
        heartbeat_with_config(&registry, "node-a", "cluster-1", &cfg_a);
        heartbeat_with_config(&registry, "node-b", "cluster-1", "");

        assert_eq!(
            registry.applied_cpu_intersection("cluster-1"),
            None,
            "node-b has reported but not yet with a cpu_config_json; the gate must stay shut"
        );

        let cfg_b = cpu_config_json(0x0F);
        heartbeat_with_config(&registry, "node-b", "cluster-1", &cfg_b);
        assert_eq!(
            registry.applied_cpu_intersection("cluster-1"),
            Some(cpu_config_json(0xFF & 0x0F)),
            "every known reporter now has a config; the gate opens and the applied value \
             catches up"
        );
    }

    #[test]
    fn heartbeat_delivers_intersection_exactly_once_per_node() {
        let registry = AtomicNodeRegistry::new(
            vec![
                node("node-a", "http://node-a"),
                node("node-b", "http://node-b"),
            ],
            Duration::from_secs(30),
        );
        let cfg_a = cpu_config_json(0xFF);
        let cfg_b = cpu_config_json(0x0F);

        heartbeat_with_config(&registry, "node-a", "cluster-1", "");
        heartbeat_with_config(&registry, "node-b", "cluster-1", "");

        assert!(heartbeat_with_config(&registry, "node-a", "cluster-1", &cfg_a).is_empty());

        let result_b = heartbeat_with_config(&registry, "node-b", "cluster-1", &cfg_b);
        assert!(!result_b.is_empty());

        let result_a2 = heartbeat_with_config(&registry, "node-a", "cluster-1", "");
        assert!(!result_a2.is_empty());

        assert!(heartbeat_with_config(&registry, "node-a", "cluster-1", "").is_empty());
        assert!(heartbeat_with_config(&registry, "node-b", "cluster-1", "").is_empty());

        assert_eq!(result_a2, cpu_config_json(0xFF & 0x0F));
    }

    #[test]
    fn multi_cluster_cpu_intersections_are_independent() {
        let registry = AtomicNodeRegistry::new(
            vec![
                node("x1", "http://x1"),
                node("x2", "http://x2"),
                node("y1", "http://y1"),
            ],
            Duration::from_secs(30),
        );
        let cfg_x1 = cpu_config_json(0xFF);
        let cfg_x2 = cpu_config_json(0x0F);
        let cfg_y = cpu_config_json(0xF0);

        heartbeat_with_config(&registry, "x1", "cluster-x", "");
        heartbeat_with_config(&registry, "x2", "cluster-x", "");
        heartbeat_with_config(&registry, "y1", "cluster-y", "");

        heartbeat_with_config(&registry, "x1", "cluster-x", &cfg_x1);
        heartbeat_with_config(&registry, "x2", "cluster-x", &cfg_x2);

        let result_y = heartbeat_with_config(&registry, "y1", "cluster-y", &cfg_y);
        assert!(!result_y.is_empty());

        let result_x = heartbeat_with_config(&registry, "x1", "cluster-x", "");
        assert!(!result_x.is_empty());
        assert_eq!(result_x, cpu_config_json(0xFF & 0x0F));
        assert_eq!(result_y, cpu_config_json(0xF0));

        assert!(heartbeat_with_config(&registry, "x2", "cluster-x", "").is_empty());
        assert!(heartbeat_with_config(&registry, "y1", "cluster-y", "").is_empty());
    }

    #[test]
    fn heartbeat_under_the_previous_pod_name_is_still_recognised() {
        let registry = AtomicNodeRegistry::new(Vec::new(), Duration::from_secs(30));
        registry.set(
            vec![Node {
                id: "aenv-worker-01".to_string(),
                endpoint: "http://10.0.0.1:8000".to_string(),
                pod_name: "agentenv-node-xk29f".to_string(),
            }],
            Vec::new(),
            unix(100),
        );

        let (node, _) = registry
            .heartbeat(
                &ready_heartbeat("agentenv-node-xk29f", "cluster-a"),
                unix(100),
            )
            .expect("the old identity must be accepted");
        assert_eq!(node.id, "aenv-worker-01");

        let observed = registry.list_observed("cluster-a", unix(100));
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].node_id, "aenv-worker-01");
        assert!(registry.peek_observed("agentenv-node-xk29f").is_none());
    }

    #[test]
    fn heartbeats_under_old_and_new_identity_collapse_to_one_node() {
        let registry = AtomicNodeRegistry::new(Vec::new(), Duration::from_secs(30));
        registry.set(
            vec![Node {
                id: "aenv-worker-01".to_string(),
                endpoint: "http://10.0.0.1:8000".to_string(),
                pod_name: "agentenv-node-xk29f".to_string(),
            }],
            Vec::new(),
            unix(100),
        );
        let now = unix(100);

        for id in ["agentenv-node-xk29f", "aenv-worker-01"] {
            registry
                .heartbeat(&ready_heartbeat(id, "cluster-a"), now)
                .unwrap();
        }

        assert_eq!(registry.list_observed("cluster-a", now).len(), 1);
    }

    #[test]
    fn an_alias_that_collides_with_a_real_node_is_ignored() {
        let registry = AtomicNodeRegistry::new(Vec::new(), Duration::from_secs(30));
        registry.set(
            vec![
                Node {
                    id: "aenv-worker-01".to_string(),
                    endpoint: "http://10.0.0.1:8000".to_string(),
                    pod_name: "aenv-worker-02".to_string(),
                },
                node("aenv-worker-02", "http://10.0.0.2:8000"),
            ],
            Vec::new(),
            unix(100),
        );

        let (node, _) = registry
            .heartbeat(&ready_heartbeat("aenv-worker-02", "cluster-a"), unix(100))
            .unwrap();
        assert_eq!(node.id, "aenv-worker-02");
        assert_eq!(node.endpoint, "http://10.0.0.2:8000");
    }

    #[test]
    fn heartbeat_from_an_unknown_identity_is_still_rejected() {
        let registry = AtomicNodeRegistry::new(Vec::new(), Duration::from_secs(30));
        registry.set(
            vec![Node {
                id: "aenv-worker-01".to_string(),
                endpoint: "http://10.0.0.1:8000".to_string(),
                pod_name: "agentenv-node-xk29f".to_string(),
            }],
            Vec::new(),
            unix(100),
        );

        let err = registry
            .heartbeat(
                &ready_heartbeat("agentenv-node-somewhere-else", "cluster-a"),
                unix(100),
            )
            .unwrap_err();
        assert_eq!(err, NodeNotInRegistry);
    }

    // ---- node_registry_roster_test.go ----

    #[test]
    fn roster_of_keeps_the_heartbeat_roster() {
        let registry = AtomicNodeRegistry::new(
            vec![node("node-a", "http://node-a")],
            DEFAULT_OBSERVED_REPORT_TTL,
        );
        let now = unix(1_700_000_000);

        assert!(registry.roster_of("node-a").is_none());

        heartbeat_with_roster(&registry, "node-a", "cluster-a", now, &["s1", "s2"]);

        let (roster, last_seen) = registry.roster_of("node-a").expect("a roster");
        assert_eq!(roster_ids(&roster), vec!["s1", "s2"]);
        assert_eq!(last_seen, now);

        // The returned Vec is a copy: mutating it must not corrupt the
        // registry (true by construction in Rust, but assert it anyway to
        // keep parity with the Go test that checks it explicitly).
        let (again, _) = registry.roster_of("node-a").unwrap();
        assert_eq!(again[0].sandbox_id, "s1");
    }

    #[test]
    fn roster_normalises_blanks_and_duplicates() {
        let registry = AtomicNodeRegistry::new(
            vec![node("node-a", "http://node-a")],
            DEFAULT_OBSERVED_REPORT_TTL,
        );
        let now = unix(1_700_000_000);

        heartbeat_with_roster(
            &registry,
            "node-a",
            "cluster-a",
            now,
            &[" s1 ", "", "s1", "s2"],
        );

        let (roster, _) = registry.roster_of("node-a").unwrap();
        assert_eq!(roster_ids(&roster), vec!["s1", "s2"]);
    }

    #[test]
    fn nodes_holding_is_the_reverse_index() {
        let registry = AtomicNodeRegistry::new(
            vec![
                node("node-a", "http://node-a"),
                node("node-b", "http://node-b"),
            ],
            DEFAULT_OBSERVED_REPORT_TTL,
        );
        let now = unix(1_700_000_000);

        heartbeat_with_roster(&registry, "node-a", "cluster-a", now, &["s1", "s2"]);
        heartbeat_with_roster(&registry, "node-b", "cluster-a", now, &["s2", "s3"]);

        assert_eq!(registry.nodes_holding("s1"), vec!["node-a"]);
        assert_eq!(registry.nodes_holding("s2"), vec!["node-a", "node-b"]);
        assert!(registry.nodes_holding("nobody").is_empty());

        heartbeat_with_roster(
            &registry,
            "node-a",
            "cluster-a",
            now + Duration::from_secs(1),
            &["s1"],
        );
        assert_eq!(registry.nodes_holding("s2"), vec!["node-b"]);

        heartbeat_with_roster(
            &registry,
            "node-a",
            "cluster-a",
            now + Duration::from_secs(2),
            &[],
        );
        assert!(registry.nodes_holding("s1").is_empty());
    }

    #[test]
    fn rosters_returns_every_observed_node_sorted() {
        let registry = AtomicNodeRegistry::new(
            vec![
                node("node-b", "http://node-b"),
                node("node-a", "http://node-a"),
            ],
            DEFAULT_OBSERVED_REPORT_TTL,
        );
        let now = unix(1_700_000_000);

        heartbeat_with_roster(&registry, "node-b", "cluster-a", now, &["s3"]);
        heartbeat_with_roster(
            &registry,
            "node-a",
            "cluster-a",
            now + Duration::from_secs(1),
            &["s1", "s2"],
        );

        let rosters = registry.rosters_in_cluster("");
        assert_eq!(rosters.len(), 2);
        assert_eq!(rosters[0].node_id, "node-a");
        assert_eq!(rosters[1].node_id, "node-b");
        assert_eq!(rosters[0].sandbox_ids(), vec!["s1", "s2"]);
        assert_eq!(rosters[0].last_seen, Some(now + Duration::from_secs(1)));
    }

    #[test]
    fn unregister_clears_the_roster() {
        let registry = AtomicNodeRegistry::new(
            vec![node("node-a", "http://node-a")],
            DEFAULT_OBSERVED_REPORT_TTL,
        );
        let now = unix(1_700_000_000);

        heartbeat_with_roster(&registry, "node-a", "cluster-a", now, &["s1"]);
        registry
            .unregister_observed("node-a", "svc-node-a")
            .unwrap();

        assert!(registry.nodes_holding("s1").is_empty());
        assert!(registry.roster_of("node-a").is_none());

        let rosters = registry.rosters_in_cluster("");
        assert_eq!(rosters.len(), 1);
        assert!(rosters[0].last_seen.is_none());
    }

    #[test]
    fn discovery_eviction_clears_the_roster() {
        let registry = AtomicNodeRegistry::new(
            vec![
                node("node-a", "http://node-a"),
                node("node-b", "http://node-b"),
            ],
            DEFAULT_OBSERVED_REPORT_TTL,
        );
        let now = unix(1_700_000_000);

        heartbeat_with_roster(&registry, "node-a", "cluster-a", now, &["s1"]);
        heartbeat_with_roster(&registry, "node-b", "cluster-a", now, &["s2"]);

        registry.set(vec![node("node-b", "http://node-b")], Vec::new(), now);

        assert!(registry.nodes_holding("s1").is_empty());
        assert_eq!(registry.nodes_holding("s2"), vec!["node-b"]);
    }

    #[test]
    fn rosters_in_cluster_scopes_to_one_cluster() {
        let registry = AtomicNodeRegistry::new(
            vec![
                node("node-a", "http://node-a"),
                node("node-b", "http://node-b"),
            ],
            DEFAULT_OBSERVED_REPORT_TTL,
        );
        let now = unix(1_700_000_000);

        heartbeat_with_roster(&registry, "node-a", "cluster-a", now, &["s1"]);
        heartbeat_with_roster(&registry, "node-b", "cluster-b", now, &["s2"]);

        let scoped = registry.rosters_in_cluster("cluster-a");
        assert_eq!(scoped.len(), 1);
        assert_eq!(scoped[0].node_id, "node-a");

        let upper = registry.rosters_in_cluster("CLUSTER-A");
        assert_eq!(upper.len(), 1);
        assert_eq!(upper[0].node_id, "node-a");

        assert_eq!(registry.rosters_in_cluster("").len(), 2);
    }

    #[test]
    fn rosters_in_cluster_reports_nodes_that_have_never_reported() {
        let registry = AtomicNodeRegistry::new(
            vec![
                node("node-a", "http://node-a"),
                node("node-silent", "http://node-silent"),
            ],
            DEFAULT_OBSERVED_REPORT_TTL,
        );
        let now = unix(1_700_000_000);

        heartbeat_with_roster(&registry, "node-a", "cluster-a", now, &["s1"]);

        let rosters = registry.rosters_in_cluster("cluster-a");
        assert_eq!(rosters.len(), 2);
        let silent = &rosters[1];
        assert_eq!(silent.node_id, "node-silent");
        assert!(silent.last_seen.is_none());
        assert!(silent.entries.is_empty());

        let z = registry.rosters_in_cluster("cluster-z");
        assert_eq!(z.len(), 1);
        assert_eq!(z[0].node_id, "node-silent");

        registry.set(vec![node("node-a", "http://node-a")], Vec::new(), now);
        let after = registry.rosters_in_cluster("cluster-a");
        assert_eq!(after.len(), 1);
        assert_eq!(after[0].node_id, "node-a");
    }

    // 🔴 Regression guard: the CPU-intersection cache must be invalidated —
    // not left standing — when a node neither of the previous two entries
    // has heard from joins the same cluster. `heartbeat`'s `!existed` check
    // is what does this (see the doc comment on `heartbeat` and on
    // `invalidate_intersection`); a simplification that only invalidated on
    // a *changed* config, and treated "brand new observation" as just
    // another unchanged one, would leave a stale two-node intersection
    // cached — and therefore deliverable — after a third node with a
    // narrower config has already joined the cluster it claims to describe.
    #[test]
    fn cpu_intersection_is_withheld_again_when_a_new_node_joins_the_cluster() {
        let registry = AtomicNodeRegistry::new(
            vec![
                node("node-a", "http://node-a"),
                node("node-b", "http://node-b"),
            ],
            Duration::from_secs(30),
        );
        let cfg = cpu_config_json(0xFF);

        // Both nodes report the identical config: the intersection is
        // immediately computable and delivered on the heartbeat that
        // completes the set.
        heartbeat_with_config(&registry, "node-a", "cluster-1", &cfg);
        let delivered = heartbeat_with_config(&registry, "node-b", "cluster-1", &cfg);
        assert!(
            !delivered.is_empty(),
            "expected the intersection once both nodes agree"
        );

        // A third node joins discovery and reports in for the first time,
        // with no CPU config yet. The cached intersection must not survive
        // this: the cluster's true size just changed, and the old answer no
        // longer reflects "every known node agrees".
        registry.set(
            vec![
                node("node-a", "http://node-a"),
                node("node-b", "http://node-b"),
                node("node-c", "http://node-c"),
            ],
            Vec::new(),
            unix(100),
        );
        let result = heartbeat_with_config(&registry, "node-c", "cluster-1", "");
        assert!(
            result.is_empty(),
            "a newcomer with no config yet must not receive a stale two-node intersection"
        );

        // Nor may node-a be handed the stale answer again — the cache was
        // invalidated, not merely withheld from the newcomer.
        let restale = heartbeat_with_config(&registry, "node-a", "cluster-1", &cfg);
        assert!(
            restale.is_empty(),
            "the intersection must stay withheld until node-c also reports a config"
        );
    }
    // ---- kubernetes_discovery_test.go's registry-focused cases ----
    //
    // These five lived in kubernetes_discovery_test.go on the Go side
    // (colocated with the discovery tests even though they exercise
    // AtomicNodeRegistry alone), not node_registry_test.go /
    // node_registry_roster_test.go. node_registry_reflects_endpoint_removal_across_syncs
    // is ported in src/node_registry/kubernetes_discovery.rs, next to
    // nodes_from_endpoint_slices, since it is the one that actually combines
    // discovery output with registry state. lingering_node_gets_no_schedule_status_in_observed_view
    // is Go's TestLingeringNodeGetsNoScheduleStatusInObservedView, whose
    // assertion is already the first half of
    // lingering_node_becomes_unhealthy_after_ttl above -- not duplicated
    // again here. The remaining three below (snapshot filtering, the
    // active/READY case, and discovery eviction clearing GetObserved/
    // ListObserved) were not covered by any existing test until now.

    #[test]
    fn snapshot_filters_lingering_nodes() {
        let registry = AtomicNodeRegistry::new(Vec::new(), DEFAULT_OBSERVED_REPORT_TTL);
        registry.set(
            vec![
                node("node-a", "http://node-a"),
                node("node-b", "http://node-b"),
            ],
            vec![node("node-c", "http://node-c")],
            unix(100),
        );

        let no_lingering = registry.snapshot(false);
        assert_eq!(no_lingering.len(), 2);

        let with_lingering = registry.snapshot(true);
        assert_eq!(with_lingering.len(), 3);
    }

    #[test]
    fn active_node_gets_ready_status_in_observed_view() {
        let registry = AtomicNodeRegistry::new(
            vec![node("node-a", "http://node-a")],
            DEFAULT_OBSERVED_REPORT_TTL,
        );
        let now = unix(100);
        registry
            .heartbeat(&ready_heartbeat("node-a", "cluster-a"), now)
            .unwrap();

        let observed = registry
            .get_observed("node-a", "", now)
            .expect("observed node");
        assert_eq!(observed.snapshot.unwrap().status(), NodeStatus::Ready);
    }

    /// 🔴 Renamed from `set_removes_observed_nodes_missing_from_discovery`:
    /// with [`EmptySyncGuard`] in place, one all-empty `set` no longer
    /// removes anything by itself — see this module's own doc comment on
    /// the divergence from Go's `Set`. The removal mechanism this test
    /// covers is unchanged; what changed is that it only fires once
    /// confirmed. `an_all_empty_sync_is_withheld_until_confirmed` below
    /// covers the withholding half on its own; this one drives the *default*
    /// [`EmptySyncGuard`] all the way through confirmation, so production's
    /// actual configuration is proven to still get there, not just a
    /// specially-weakened guard built for the test.
    #[test]
    fn set_removes_observed_nodes_missing_from_discovery_once_the_empty_sync_is_confirmed() {
        let registry = AtomicNodeRegistry::new(
            vec![node("node-a", "http://node-a")],
            DEFAULT_OBSERVED_REPORT_TTL,
        );
        let now = unix(100);
        registry
            .heartbeat(&ready_heartbeat("node-a", "cluster-a"), now)
            .unwrap();

        // The first all-empty sync is only suspected, not applied.
        registry.set(Vec::new(), Vec::new(), now);
        assert!(
            registry.get_observed("node-a", "", now).is_some(),
            "a single empty discovery sync must not immediately remove a previously known node"
        );

        // Reaching the default confirmation count applies it — one more
        // call, since the first one above already counted as the first
        // confirmation.
        for _ in 1..DEFAULT_EMPTY_SYNC_CONFIRMATIONS {
            registry.set(Vec::new(), Vec::new(), now);
        }

        assert!(registry.get_observed("node-a", "", now).is_none());
        assert!(registry.list_observed("", now).is_empty());
    }

    // ---- EmptySyncGuard: the divergence from Go's Set (this module's own
    //      doc comment) ----

    #[test]
    fn an_all_empty_sync_is_withheld_until_the_confirmation_count_is_reached() {
        let registry = AtomicNodeRegistry::with_empty_sync_guard(
            vec![node("node-a", "http://node-a")],
            DEFAULT_OBSERVED_REPORT_TTL,
            EmptySyncGuard {
                confirmations: 3,
                // Large enough that only the count, never the window, can
                // confirm within this test.
                window: Duration::from_secs(10_000),
            },
        );
        let now = unix(100);

        registry.set(Vec::new(), Vec::new(), now);
        assert!(
            registry.snapshot(true).iter().any(|n| n.id == "node-a"),
            "confirmation 1 of 3 must not yet apply the wipe"
        );

        registry.set(Vec::new(), Vec::new(), now);
        assert!(
            registry.snapshot(true).iter().any(|n| n.id == "node-a"),
            "confirmation 2 of 3 must not yet apply the wipe"
        );

        registry.set(Vec::new(), Vec::new(), now);
        assert!(
            registry.snapshot(true).is_empty(),
            "confirmation 3 of 3 must apply the wipe"
        );
    }

    #[test]
    fn an_all_empty_sync_confirms_via_the_time_window_even_short_of_the_confirmation_count() {
        let registry = AtomicNodeRegistry::with_empty_sync_guard(
            vec![node("node-a", "http://node-a")],
            DEFAULT_OBSERVED_REPORT_TTL,
            EmptySyncGuard {
                // High enough that the count alone never confirms within
                // this test — only the window may.
                confirmations: 1_000,
                window: Duration::from_secs(30),
            },
        );
        let start = unix(100);

        registry.set(Vec::new(), Vec::new(), start);
        assert!(
            registry.snapshot(true).iter().any(|n| n.id == "node-a"),
            "must not apply before the window elapses"
        );

        registry.set(Vec::new(), Vec::new(), start + Duration::from_secs(29));
        assert!(
            registry.snapshot(true).iter().any(|n| n.id == "node-a"),
            "must not apply one second short of the window"
        );

        registry.set(Vec::new(), Vec::new(), start + Duration::from_secs(30));
        assert!(
            registry.snapshot(true).is_empty(),
            "the window elapsing must apply the wipe even though the count never came close"
        );
    }

    #[test]
    fn a_non_empty_sync_resets_a_pending_empty_confirmation() {
        let registry = AtomicNodeRegistry::with_empty_sync_guard(
            vec![node("node-a", "http://node-a")],
            DEFAULT_OBSERVED_REPORT_TTL,
            EmptySyncGuard {
                confirmations: 2,
                window: Duration::from_secs(10_000),
            },
        );
        let now = unix(100);

        // One confirmation in — one more would apply the wipe.
        registry.set(Vec::new(), Vec::new(), now);
        assert!(registry.snapshot(true).iter().any(|n| n.id == "node-a"));

        // Discovery reports a real (even if different) node list again —
        // proof the empty answer was transient. This must reset the count,
        // not merely pause it.
        registry.set(vec![node("node-a", "http://node-a")], Vec::new(), now);
        assert!(registry.snapshot(true).iter().any(|n| n.id == "node-a"));

        // A fresh empty sync must need the full count again, not "one more".
        registry.set(Vec::new(), Vec::new(), now);
        assert!(
            registry.snapshot(true).iter().any(|n| n.id == "node-a"),
            "the reset must not be skipped — this is only the first confirmation since the \
             non-empty sync, not the second"
        );
        registry.set(Vec::new(), Vec::new(), now);
        assert!(
            registry.snapshot(true).is_empty(),
            "the second confirmation applies it"
        );
    }

    #[test]
    fn a_node_can_still_heartbeat_while_an_empty_sync_is_pending_confirmation() {
        let registry = AtomicNodeRegistry::with_empty_sync_guard(
            vec![node("node-a", "http://node-a")],
            DEFAULT_OBSERVED_REPORT_TTL,
            EmptySyncGuard {
                confirmations: 5,
                window: Duration::from_secs(10_000),
            },
        );
        let now = unix(100);

        registry.set(Vec::new(), Vec::new(), now);

        registry
            .heartbeat(&ready_heartbeat("node-a", "cluster-a"), now)
            .expect(
                "a node discovery has not (yet) confirmed missing must still be able to \
                 heartbeat — the withheld sync must not have removed it from nodes_by_id",
            );
    }

    #[test]
    fn a_zero_confirmation_guard_applies_an_empty_sync_immediately_matching_go() {
        // `EmptySyncGuard::confirmations = 0` (or `1`) with a zero window is
        // Go's original "apply every Set call unconditionally" behavior —
        // deliberately still reachable, not special-cased away. See this
        // module's own doc comment.
        let registry = AtomicNodeRegistry::with_empty_sync_guard(
            vec![node("node-a", "http://node-a")],
            DEFAULT_OBSERVED_REPORT_TTL,
            EmptySyncGuard {
                confirmations: 0,
                window: Duration::ZERO,
            },
        );

        registry.set(Vec::new(), Vec::new(), unix(100));
        assert!(registry.snapshot(true).is_empty());
    }

    // ---- admit_pending: task's own "D2" (this module's own doc comment on
    //      `admit_pending`) ----

    #[test]
    fn a_pending_node_cannot_heartbeat_before_admit_pending_is_called() {
        let registry = AtomicNodeRegistry::new(Vec::new(), DEFAULT_OBSERVED_REPORT_TTL);
        registry.set(Vec::new(), Vec::new(), unix(100));

        let err = registry
            .heartbeat(&ready_heartbeat("node-a", "cluster-a"), unix(100))
            .expect_err("node-a was never admitted, pending or otherwise");
        assert_eq!(err, NodeNotInRegistry);
    }

    #[test]
    fn admit_pending_lets_a_not_yet_serving_node_heartbeat() {
        let registry = AtomicNodeRegistry::new(Vec::new(), DEFAULT_OBSERVED_REPORT_TTL);
        registry.set(Vec::new(), Vec::new(), unix(100));
        registry.admit_pending(vec![node("node-a", "http://node-a")]);

        registry
            .heartbeat(&ready_heartbeat("node-a", "cluster-a"), unix(100))
            .expect(
                "a node discovery names but has not yet marked Serving must still be able to                  heartbeat — this is the exact race D2 closes",
            );
        assert!(
            registry
                .get_observed("node-a", "cluster-a", unix(100))
                .is_some(),
            "the heartbeat must have actually landed in observed state, not merely been accepted"
        );
    }

    #[test]
    fn a_pending_node_is_excluded_from_the_scheduling_snapshot_but_visible_in_the_full_one() {
        let registry = AtomicNodeRegistry::new(Vec::new(), DEFAULT_OBSERVED_REPORT_TTL);
        registry.set(Vec::new(), Vec::new(), unix(100));
        registry.admit_pending(vec![node("node-a", "http://node-a")]);

        assert!(
            !registry
                .snapshot(false)
                .iter()
                .any(|n| n.id == "node-a"),
            "a pending node must never be an eligible Schedule candidate —              filter_unschedulable's own fail-open policy would otherwise pick it up              immediately, before it can actually run a sandbox"
        );
        assert!(
            registry.snapshot(true).iter().any(|n| n.id == "node-a"),
            "the full inventory view (ListNodes) should still show a pending node as known"
        );
    }

    #[test]
    fn a_pending_nodes_reported_status_is_not_forced_to_lingering() {
        // Contrast with `lingering`, which `derive_observed_node_view`
        // forces to `NodeStatus::Lingering` regardless of what the node
        // self-reports. A pending node is not draining — it is starting up
        // — so its self-reported status must flow through unmodified.
        let registry = AtomicNodeRegistry::new(Vec::new(), DEFAULT_OBSERVED_REPORT_TTL);
        registry.set(Vec::new(), Vec::new(), unix(100));
        registry.admit_pending(vec![node("node-a", "http://node-a")]);
        registry
            .heartbeat(&ready_heartbeat("node-a", "cluster-a"), unix(100))
            .expect("node-a heartbeats while pending");

        let observed = registry
            .get_observed("node-a", "cluster-a", unix(100))
            .expect("node-a is observed");
        assert_eq!(
            observed.snapshot.expect("a snapshot").status(),
            NodeStatus::Ready,
            "a pending node's self-reported status must not be overridden the way lingering's is"
        );
    }

    #[test]
    fn a_node_promoted_from_pending_to_active_is_no_longer_excluded_from_scheduling() {
        let registry = AtomicNodeRegistry::new(Vec::new(), DEFAULT_OBSERVED_REPORT_TTL);
        registry.set(Vec::new(), Vec::new(), unix(100));
        registry.admit_pending(vec![node("node-a", "http://node-a")]);
        assert!(!registry.snapshot(false).iter().any(|n| n.id == "node-a"));

        // Discovery's next sync now marks node-a Serving.
        registry.set(vec![node("node-a", "http://node-a")], Vec::new(), unix(101));
        registry.admit_pending(Vec::new());

        assert!(
            registry.snapshot(false).iter().any(|n| n.id == "node-a"),
            "once set() promotes a node to active, admit_pending must not keep excluding it"
        );
    }

    #[test]
    fn a_stale_pending_report_for_an_already_active_node_does_not_downgrade_it() {
        // A discovery watch loop calling admit_pending can lag set() by
        // however long its own last pending computation took — if that
        // stale computation still names a node set() has *this cycle*
        // already promoted to active, admit_pending must not re-flag it
        // pending and pull it back out of the scheduling snapshot.
        let registry = AtomicNodeRegistry::new(Vec::new(), DEFAULT_OBSERVED_REPORT_TTL);
        registry.set(Vec::new(), Vec::new(), unix(100));
        registry.admit_pending(vec![node("node-a", "http://node-a")]);

        registry.set(vec![node("node-a", "http://node-a")], Vec::new(), unix(101));
        // Stale: still names node-a, as if the pending computation that fed
        // this call ran before set()'s promotion was known.
        registry.admit_pending(vec![node("node-a", "http://node-a")]);

        assert!(
            registry.snapshot(false).iter().any(|n| n.id == "node-a"),
            "a stale pending report must not downgrade a node set() already promoted to active"
        );
    }

    #[test]
    fn a_pending_nodes_roster_survives_consecutive_admit_pending_calls_while_still_pending() {
        // Zero-confirmation, zero-window guard so the all-empty set() calls
        // below actually run their stale-cleanup pass immediately, rather
        // than being withheld by EmptySyncGuard (which would make this test
        // pass without ever reaching the code it means to exercise — a
        // pending-only registry counts as "populated" for the guard's own
        // purposes, so the default guard intercepts an all-empty set() here
        // before set()'s cleanup filter even runs).
        let registry = AtomicNodeRegistry::with_empty_sync_guard(
            Vec::new(),
            DEFAULT_OBSERVED_REPORT_TTL,
            EmptySyncGuard {
                confirmations: 0,
                window: Duration::ZERO,
            },
        );
        registry.set(Vec::new(), Vec::new(), unix(100));
        registry.admit_pending(vec![node("node-a", "http://node-a")]);
        registry
            .heartbeat(
                &HeartbeatRequest {
                    node_id: "node-a".to_string(),
                    cluster_id: "cluster-a".to_string(),
                    service_instance_id: "svc-node-a".to_string(),
                    roster: vec![crate::proto::scheduler::SandboxRosterEntry {
                        sandbox_id: "sandbox-1".to_string(),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                unix(100),
            )
            .expect("node-a heartbeats while pending");
        assert_eq!(
            registry.nodes_holding("sandbox-1"),
            vec!["node-a".to_string()]
        );

        // The next discovery cycle: set() still does not know about node-a
        // (still not Serving), then admit_pending re-confirms it pending
        // again. set()'s own stale-cleanup must not have wiped node-a's
        // roster out from under this — that is exactly the bug this
        // module's own doc comment on `admit_pending` explains.
        registry.set(Vec::new(), Vec::new(), unix(105));
        registry.admit_pending(vec![node("node-a", "http://node-a")]);

        assert_eq!(
            registry.nodes_holding("sandbox-1"),
            vec!["node-a".to_string()],
            "a still-pending node's roster must survive the set() call in between two              admit_pending calls"
        );
    }

    #[test]
    fn a_pending_node_that_disappears_is_cleared_like_any_other_departed_node() {
        // A zero-confirmation, zero-window guard so this test isolates
        // admit_pending's own cleanup from EmptySyncGuard's separate,
        // deliberate withholding of an all-empty set() once the registry is
        // "populated" (which a pending-only admission also counts as) --
        // that interaction is real and desirable, just not what this test
        // is about.
        let registry = AtomicNodeRegistry::with_empty_sync_guard(
            Vec::new(),
            DEFAULT_OBSERVED_REPORT_TTL,
            EmptySyncGuard {
                confirmations: 0,
                window: Duration::ZERO,
            },
        );
        registry.set(Vec::new(), Vec::new(), unix(100));
        registry.admit_pending(vec![node("node-a", "http://node-a")]);
        registry
            .heartbeat(&ready_heartbeat("node-a", "cluster-a"), unix(100))
            .expect("node-a heartbeats while pending");
        assert!(registry
            .get_observed("node-a", "cluster-a", unix(100))
            .is_some());

        // node-a's EndpointSlice entry is gone entirely on the next sync —
        // never promoted, never renamed, just gone (pod deleted, not
        // recreated).
        registry.set(Vec::new(), Vec::new(), unix(110));
        registry.admit_pending(Vec::new());

        assert!(
            registry.get_observed("node-a", "cluster-a", unix(110)).is_none(),
            "a pending node that genuinely disappears must have its observed state cleared,              the same as a departed active/lingering node"
        );
        let err = registry
            .heartbeat(&ready_heartbeat("node-a", "cluster-a"), unix(111))
            .expect_err("node-a's identity must have been dropped along with its observed state");
        assert_eq!(err, NodeNotInRegistry);
    }

    // ---- the shared-roster fix (`super::redis`): merge semantics, the
    //      publish channel, and the one correctness-bearing property (CPU
    //      intersection over the *merged*, cluster-wide view) ----

    #[test]
    fn stored_observed_record_round_trips_every_field() {
        let record = ObservedNodeRecord {
            node: ObservedNode {
                node_id: "node-a".to_string(),
                endpoint: "http://node-a".to_string(),
                cluster_id: "cluster-1".to_string(),
                service_instance_id: "svc-node-a".to_string(),
                version: "v1.2.3".to_string(),
                commit: "deadbeef".to_string(),
                machine_info: Some(MachineInfo {
                    cpu_family: "6".to_string(),
                    cpu_model: "42".to_string(),
                    cpu_model_name: "Test CPU".to_string(),
                    cpu_architecture: "x86_64".to_string(),
                    cpu_config_json: cpu_config_json(0xFF),
                }),
                snapshot: Some(NodeSnapshot {
                    status: NodeStatus::Ready as i32,
                    allocated_cpu: 4,
                    allocated_memory_bytes: 1024,
                    cpu_percent: 50,
                    cpu_count: 8,
                    memory_used_bytes: 2048,
                    memory_total_bytes: 4096,
                    disks: vec![crate::proto::scheduler::DiskMetric {
                        mount_point: "/".to_string(),
                        device: "/dev/sda1".to_string(),
                        filesystem_type: "ext4".to_string(),
                        used_bytes: 100,
                        total_bytes: 200,
                    }],
                    sandbox_count: 3,
                    sandbox_starting_count: 1,
                    create_successes: 10,
                    create_fails: 2,
                    reported_at_unix_ms: 12345,
                    paused_sandbox_count: 1,
                    paused_allocated_cpu: 1,
                    paused_allocated_memory_bytes: 512,
                }),
                last_seen_unix_ms: 999_000,
            },
            p2p_endpoint: Some(P2pEndpointProto {
                backend: "iroh".to_string(),
                address: "node-a-p2p".to_string(),
            }),
            report_ttl: Duration::from_secs(45),
            entries: vec![RosterEntry {
                sandbox_id: "sbx-1".to_string(),
                execution_id: "018f0000-0000-7000-8000-000000000000".to_string(),
                projection_ttl: Duration::from_secs(60),
            }],
            last_seen: SystemTime::UNIX_EPOCH + Duration::from_millis(999_000),
        };

        let stored = StoredObservedRecord::from(&record);
        let round_tripped = ObservedNodeRecord::from(stored);
        assert_eq!(
            round_tripped, record,
            "every field must survive a StoredObservedRecord round trip byte-for-byte"
        );
    }

    #[test]
    fn merge_remote_adopts_a_node_never_seen_locally() {
        let registry = AtomicNodeRegistry::new(
            vec![
                node("node-a", "http://node-a"),
                node("node-b", "http://node-b"),
            ],
            Duration::from_secs(30),
        );
        assert!(registry
            .get_observed("node-b", "cluster-1", unix(100))
            .is_none());

        let stored = stored_record("node-b", "cluster-1", None, unix(100));
        registry.merge_remote_snapshot(HashMap::from([("node-b".to_string(), stored)]));

        let observed = registry
            .get_observed("node-b", "cluster-1", unix(100))
            .expect(
                "node-b must be visible after a merge, even though it never heartbeated \
                 through this replica",
            );
        assert_eq!(observed.endpoint, "http://node-b");
    }

    #[test]
    fn merge_remote_ignores_a_stale_pull_of_a_fresher_local_heartbeat() {
        let registry = AtomicNodeRegistry::new(
            vec![node("node-a", "http://node-a")],
            Duration::from_secs(30),
        );
        // A fresh local heartbeat at t=200.
        heartbeat_with_roster(&registry, "node-a", "cluster-1", unix(200), &["sbx-fresh"]);

        // A stale pull of this replica's own earlier write (t=100), as if
        // the async publish round-tripped through redis late and the
        // periodic pull picked up an older snapshot than what is already
        // local.
        let mut stale = stored_record("node-a", "cluster-1", None, unix(100));
        stale.entries = vec![StoredRosterEntry {
            sandbox_id: "sbx-stale".to_string(),
            execution_id: String::new(),
            projection_ttl_secs: 0,
        }];
        registry.merge_remote_snapshot(HashMap::from([("node-a".to_string(), stale)]));

        let (entries, _) = registry.roster_of("node-a").expect("node-a has a roster");
        assert_eq!(
            roster_ids(&entries),
            vec!["sbx-fresh".to_string()],
            "a stale pull must never regress a fresher local heartbeat's roster"
        );
    }

    #[test]
    fn merge_remote_adopts_a_newer_record_when_a_node_reconnects_elsewhere() {
        let registry = AtomicNodeRegistry::new(
            vec![node("node-a", "http://node-a")],
            Duration::from_secs(30),
        );
        heartbeat_with_roster(&registry, "node-a", "cluster-1", unix(100), &["sbx-old"]);

        // node-a reconnected to a different replica and heartbeated there
        // at t=200 with a different roster; this replica only learns of it
        // through the next merge.
        let mut newer = stored_record("node-a", "cluster-1", None, unix(200));
        newer.entries = vec![StoredRosterEntry {
            sandbox_id: "sbx-new".to_string(),
            execution_id: String::new(),
            projection_ttl_secs: 0,
        }];
        registry.merge_remote_snapshot(HashMap::from([("node-a".to_string(), newer)]));

        let (entries, last_seen) = registry.roster_of("node-a").expect("node-a has a roster");
        assert_eq!(roster_ids(&entries), vec!["sbx-new".to_string()]);
        assert_eq!(last_seen, unix(200));
    }

    #[test]
    fn merge_remote_never_deletes_an_entry_absent_from_the_pull() {
        let registry = AtomicNodeRegistry::new(
            vec![
                node("node-a", "http://node-a"),
                node("node-b", "http://node-b"),
            ],
            Duration::from_secs(30),
        );
        heartbeat_with_roster(&registry, "node-a", "cluster-1", unix(100), &["sbx-a"]);

        // A pull that only names node-b -- as if node-a's own
        // not-yet-flushed first publish simply has not landed in redis
        // yet.
        registry.merge_remote_snapshot(HashMap::from([(
            "node-b".to_string(),
            stored_record("node-b", "cluster-1", None, unix(100)),
        )]));

        assert!(
            registry
                .get_observed("node-a", "cluster-1", unix(100))
                .is_some(),
            "a merge must never delete a node solely because it was absent from the pull"
        );
    }

    /// The one correctness-bearing property of the whole fix: the CPU
    /// intersection gate (`Inner::all_configs_ready`/`compute_intersection`)
    /// must evaluate the merged, cluster-wide view — not just whatever
    /// heartbeats happened to land on this particular replica.
    #[test]
    fn applied_cpu_intersection_incorporates_a_node_only_known_through_a_redis_merge() {
        let registry = AtomicNodeRegistry::new(
            vec![
                node("node-a", "http://node-a"),
                node("node-b", "http://node-b"),
            ],
            Duration::from_secs(30),
        );
        let cfg_a = cpu_config_json(0xFF);
        heartbeat_with_config(&registry, "node-a", "cluster-1", &cfg_a);

        assert_eq!(
            registry.applied_cpu_intersection("cluster-1"),
            Some(cfg_a.clone()),
            "with only node-a ever observed, the trivial 'intersection' is its own config -- \
             see Inner::all_configs_ready's own doc on counting observed reporters, not \
             discovered nodes"
        );

        // node-b's heartbeat landed on a *different* replica in this
        // simulation: this replica only learns of it through a
        // shared-store merge, never a local `heartbeat` call.
        let cfg_b = cpu_config_json(0x0F);
        let stored_b = stored_record("node-b", "cluster-1", Some(cfg_b), unix(100));
        registry.merge_remote_snapshot(HashMap::from([("node-b".to_string(), stored_b)]));

        assert_eq!(
            registry.applied_cpu_intersection("cluster-1"),
            Some(cpu_config_json(0xFF & 0x0F)),
            "the cluster-wide intersection must be recomputed over both nodes once node-b is \
             merged in, even though its heartbeat was never received by this replica directly \
             -- proves all_configs_ready/compute_intersection evaluate the merged view, not \
             just this replica's own locally-received heartbeats"
        );
    }

    #[test]
    fn heartbeat_publishes_an_upsert_once_a_shared_store_is_enabled() {
        let registry = AtomicNodeRegistry::new(
            vec![node("node-a", "http://node-a")],
            Duration::from_secs(30),
        );
        let mut rx = registry.enable_shared_observed_publishing();

        registry
            .heartbeat(&ready_heartbeat("node-a", "cluster-1"), unix(100))
            .expect("heartbeat");

        match rx
            .try_recv()
            .expect("a heartbeat must publish an upsert once a shared store is enabled")
        {
            PublishOp::Upsert { node_id, record } => {
                assert_eq!(node_id, "node-a");
                assert_eq!(record.cluster_id, "cluster-1");
            }
            PublishOp::Remove { .. } => panic!("expected an upsert, got a remove"),
        }
    }

    /// The default, in-memory-only configuration every existing test in
    /// this module (and every `--role all` / `--role node` process) runs
    /// under — `enable_shared_observed_publishing` is never called, so
    /// `heartbeat` must not panic or otherwise misbehave with `publish_tx`
    /// unset. Compiling and returning normally is the assertion.
    #[test]
    fn heartbeat_does_not_require_a_shared_store_to_be_enabled() {
        let registry = AtomicNodeRegistry::new(
            vec![node("node-a", "http://node-a")],
            Duration::from_secs(30),
        );
        registry
            .heartbeat(&ready_heartbeat("node-a", "cluster-1"), unix(100))
            .expect("heartbeat");
    }

    #[test]
    fn a_departed_node_publishes_a_remove() {
        let registry = AtomicNodeRegistry::new(
            vec![
                node("node-a", "http://node-a"),
                node("node-b", "http://node-b"),
            ],
            Duration::from_secs(30),
        );
        let mut rx = registry.enable_shared_observed_publishing();
        registry
            .heartbeat(&ready_heartbeat("node-a", "cluster-1"), unix(100))
            .expect("heartbeat");
        let _ = rx.try_recv().expect("the heartbeat's own upsert");

        // node-a's EndpointSlice entry is gone on the next discovery sync;
        // node-b's is not, so this is an ordinary partial-departure sync,
        // not an all-empty one -- `EmptySyncGuard` never engages.
        registry.set(vec![node("node-b", "http://node-b")], Vec::new(), unix(110));

        match rx
            .try_recv()
            .expect("a departed node must publish a remove")
        {
            PublishOp::Remove { node_id } => assert_eq!(node_id, "node-a"),
            PublishOp::Upsert { .. } => panic!("expected a remove, got an upsert"),
        }
    }

    #[test]
    fn unregister_observed_publishes_a_remove() {
        let registry = AtomicNodeRegistry::new(
            vec![node("node-a", "http://node-a")],
            Duration::from_secs(30),
        );
        let mut rx = registry.enable_shared_observed_publishing();
        registry
            .heartbeat(&ready_heartbeat("node-a", "cluster-1"), unix(100))
            .expect("heartbeat");
        let _ = rx.try_recv().expect("the heartbeat's own upsert");

        registry
            .unregister_observed("node-a", "svc-node-a")
            .expect("unregister");

        match rx
            .try_recv()
            .expect("unregister_observed must publish a remove")
        {
            PublishOp::Remove { node_id } => assert_eq!(node_id, "node-a"),
            PublishOp::Upsert { .. } => panic!("expected a remove, got an upsert"),
        }
    }
}
