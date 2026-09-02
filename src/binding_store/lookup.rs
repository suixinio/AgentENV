//! Shared node-selection and sandbox-lookup logic for `Schedule` and the
//! in-process sandbox lookup.

use std::collections::HashMap;
use std::time::SystemTime;

use crate::binding_store::{BindingState, BindingStore};
use crate::node_registry::filter::filter_unschedulable;
use crate::node_registry::placement::score::SnapshotFreshness;
use crate::node_registry::placement::{
    request_from_hint, ShadowCandidate, ShadowPlacement, ShadowRequest, ShadowSource,
};
use crate::node_registry::registry::NodeRegistry;
use crate::node_registry::strategy::{NoNodesAvailable, RoundRobinStrategy};
use crate::node_registry::types::{Node, RichNode};
use crate::node_registry::warmup::WarmupGate;
use crate::orchestrator::{PausedRegistryState, PausedSandboxRegistry};
use crate::proto::scheduler::ScheduleRequestHint;
use crate::types::SandboxId;

/// Maximum age of a heartbeat roster entry eligible for routing.
pub use crate::node_registry::registry::DEFAULT_OBSERVED_REPORT_TTL as ROSTER_FRESH_TTL;

/// Dependencies shared by schedule and paused-lookup placement.
/// All callers must use the same round-robin strategy instance.
#[derive(Clone, Copy)]
pub struct ScheduleDeps<'a> {
    pub node_registry: &'a dyn NodeRegistry,
    pub strategy: &'a RoundRobinStrategy,
    /// Shadow scorer shared by both placement paths.
    pub shadow: &'a ShadowPlacement,
}

/// A selected node and the candidate counts considered.
#[derive(Debug, Clone)]
pub struct Placement {
    pub node: Node,
    pub candidates: usize,
    pub eligible: usize,
}

/// Selects an eligible node, honoring preference before shared round-robin.
/// Shadow scoring runs only after real selection using the same registry reads.
pub fn select_node(
    deps: &ScheduleDeps<'_>,
    hint: Option<&ScheduleRequestHint>,
    prefer_node_id: &str,
    source: ShadowSource,
    now: SystemTime,
) -> Result<Placement, NoNodesAvailable> {
    let discovered = deps.node_registry.snapshot(/* allow_lingering */ false);
    let candidates = discovered.len();
    // Preserve one snapshot/freshness read per node and the existing candidate set.
    let mut freshness_by_id: HashMap<String, Option<SnapshotFreshness>> =
        HashMap::with_capacity(candidates);
    let rich: Vec<RichNode> = discovered
        .into_iter()
        .map(|node| {
            let (snapshot, freshness) = match deps
                .node_registry
                .peek_observed_with_freshness(&node.id, now)
            {
                Some((snapshot, freshness)) => (Some(snapshot), Some(freshness)),
                None => (None, None),
            };
            freshness_by_id.insert(node.id.clone(), freshness);
            RichNode { node, snapshot }
        })
        .collect();
    let eligible_nodes = filter_unschedulable(rich);
    let eligible = eligible_nodes.len();

    let prefer_node_id = prefer_node_id.trim();
    if !prefer_node_id.is_empty() {
        let resolved_id = deps
            .node_registry
            .resolve(prefer_node_id)
            .map(|n| n.id)
            .unwrap_or_else(|| prefer_node_id.to_string());
        if let Some(candidate) = eligible_nodes.iter().find(|c| c.node.id == resolved_id) {
            return Ok(Placement {
                node: candidate.node.clone(),
                candidates,
                eligible,
            });
        }
    }

    let node = deps.strategy.select(&eligible_nodes, hint)?;

    let shadow_candidates: Vec<ShadowCandidate<'_>> = eligible_nodes
        .iter()
        .map(|rich| ShadowCandidate {
            // Snapshot and freshness come from the same combined registry read.
            freshness: freshness_by_id
                .get(&rich.node.id)
                .copied()
                .flatten()
                .filter(|_| rich.snapshot.is_some()),
            rich,
        })
        .collect();
    let request = match source {
        ShadowSource::Schedule => request_from_hint(hint),
        // Paused lookup has no request size to classify.
        ShadowSource::PausedLookup => ShadowRequest::PAUSED_LOOKUP,
    };
    deps.shadow
        .evaluate(&shadow_candidates, request, source, &node.node.id);

    Ok(Placement {
        node: node.node,
        candidates,
        eligible,
    })
}

/// Metric label for a lookup outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LookupResultLabel {
    BoundBinding,
    BoundRoster,
    BoundRegistry,
    Placed,
    Pinned,
    /// Pinned by a heartbeat roster alone: the node parks the sandbox and the
    /// paused registry has no row for it.
    PinnedRoster,
    NotFound,
    InvalidArgument,
    UnavailableBindingStore,
    UnavailableRegistry,
    UnavailableColdBindings,
    UnavailableNoNodes,
    UnavailableStarting,
    OriginUnschedulable,
    OriginNotReporting,
    HolderUnreachable,
}

impl LookupResultLabel {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BoundBinding => "bound_binding",
            Self::BoundRoster => "bound_roster",
            Self::BoundRegistry => "bound_registry",
            Self::Placed => "placed",
            Self::Pinned => "pinned",
            Self::PinnedRoster => "pinned_roster",
            Self::NotFound => "not_found",
            Self::InvalidArgument => "invalid_argument",
            Self::UnavailableBindingStore => "unavailable_binding_store",
            Self::UnavailableRegistry => "unavailable_registry",
            Self::UnavailableColdBindings => "unavailable_cold_bindings",
            Self::UnavailableNoNodes => "unavailable_no_nodes",
            Self::UnavailableStarting => "unavailable_starting",
            Self::OriginUnschedulable => "origin_unschedulable",
            Self::OriginNotReporting => "origin_not_reporting",
            Self::HolderUnreachable => "holder_unreachable",
        }
    }
}

/// Where a lookup answer places the sandbox relative to the node it names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxLocation {
    /// A node is known to hold the sandbox: from its binding, from the roster
    /// it reported in its last heartbeat, or from a registry row naming it.
    Bound,
    /// The sandbox is paused with its snapshot published, so any node can
    /// rebuild it; the node named is a placement decision taken with the
    /// origin as a soft preference.
    Placed,
    /// The only copy of the sandbox is on the origin node's disk (publishing,
    /// or local_only after a failed upload): that node or nothing.
    Pinned,
}

/// Whether an answer's `execution_id` may be used to refuse traffic. Only
/// `Registry` says yes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionAuthority {
    /// Nobody can name the current incarnation; the caller lets the request
    /// through.
    Unknown,
    /// `execution_id` names the incarnation that should be alive on the node
    /// named, right now. Never reported with an empty id.
    Registry,
    /// The node is about to mint a new incarnation (Placed and Pinned); any
    /// value carried names the previous one and must never refuse.
    Pending,
}

/// Successful lookup result.
#[derive(Debug, Clone)]
pub struct LookupAnswer {
    pub node: Node,
    pub location: SandboxLocation,
    pub origin_node_id: String,
    pub execution_id: String,
    pub execution_authority: ExecutionAuthority,
    pub label: LookupResultLabel,
}

/// Lookup result mapped by the caller to its transport status.
#[derive(Debug, Clone)]
pub enum LookupOutcome {
    Answer(LookupAnswer),
    NotFound,
    Unavailable(LookupResultLabel, &'static str),
    FailedPrecondition(LookupResultLabel, String),
}

impl LookupOutcome {
    pub fn label(&self) -> LookupResultLabel {
        match self {
            Self::Answer(answer) => answer.label,
            Self::NotFound => LookupResultLabel::NotFound,
            Self::Unavailable(label, _) | Self::FailedPrecondition(label, _) => *label,
        }
    }
}

/// Dependencies used by [`lookup_node`].
pub struct LookupDeps<'a> {
    pub place: ScheduleDeps<'a>,
    pub binding_store: &'a dyn BindingStore,
    /// Only a cluster-backed registry may authorize stage-three absence.
    pub paused_registry: Option<&'a dyn PausedSandboxRegistry>,
    pub warmup: &'a WarmupGate,
}

fn authority_for(execution_id: &str) -> ExecutionAuthority {
    if execution_id.trim().is_empty() {
        ExecutionAuthority::Unknown
    } else {
        ExecutionAuthority::Registry
    }
}

fn roster_fresh(last_seen: SystemTime, now: SystemTime) -> bool {
    match now.duration_since(last_seen) {
        Ok(age) => age <= ROSTER_FRESH_TTL,
        // Small clock skew must not make a fresh roster stale.
        Err(_) => true,
    }
}

fn live_node(node_registry: &dyn NodeRegistry, node_id: &str, now: SystemTime) -> Option<Node> {
    let node = node_registry.resolve(node_id.trim())?;
    let (_entries, last_seen) = node_registry.roster_of(&node.id)?;
    if !roster_fresh(last_seen, now) {
        return None;
    }
    Some(node)
}

enum NodeSchedulability {
    Schedulable(Node),
    NotReporting,
    NotAcceptingWork,
}

fn schedulable_node(
    node_registry: &dyn NodeRegistry,
    node_id: &str,
    now: SystemTime,
) -> NodeSchedulability {
    let Some(node) = live_node(node_registry, node_id, now) else {
        return NodeSchedulability::NotReporting;
    };
    let snapshot = node_registry.peek_observed(&node.id);
    let rich = RichNode {
        node: node.clone(),
        snapshot,
    };
    if filter_unschedulable(vec![rich]).is_empty() {
        return NodeSchedulability::NotAcceptingWork;
    }
    NodeSchedulability::Schedulable(node)
}

fn roster_prefers(
    execution: &str,
    last_seen: SystemTime,
    best_execution: &str,
    best_seen: SystemTime,
) -> bool {
    if execution == best_execution {
        return last_seen > best_seen;
    }
    if best_execution.is_empty() {
        return !execution.is_empty();
    }
    if execution.is_empty() {
        return false;
    }
    execution > best_execution
}

/// The fresh heartbeat roster entry that wins `roster_prefers` for a sandbox.
struct RosterHolder {
    node: Node,
    execution_id: String,
    /// The node parks the sandbox without a VM, so nothing here is routable.
    paused: bool,
}

fn roster_holder(
    node_registry: &dyn NodeRegistry,
    sandbox_id: &str,
    now: SystemTime,
) -> Option<RosterHolder> {
    let mut best: Option<(RosterHolder, SystemTime)> = None;
    for node_id in node_registry.nodes_holding(sandbox_id) {
        let Some(node) = node_registry.resolve(&node_id) else {
            continue;
        };
        let Some((entries, last_seen)) = node_registry.roster_of(&node.id) else {
            continue;
        };
        if !roster_fresh(last_seen, now) {
            continue;
        }
        let entry = entries.iter().find(|entry| entry.sandbox_id == sandbox_id);
        let holder = RosterHolder {
            node,
            execution_id: entry
                .map(|entry| entry.execution_id.clone())
                .unwrap_or_default(),
            paused: entry.is_some_and(|entry| entry.paused),
        };
        match &best {
            None => best = Some((holder, last_seen)),
            Some((best_holder, best_seen)) => {
                if roster_prefers(
                    &holder.execution_id,
                    last_seen,
                    &best_holder.execution_id,
                    *best_seen,
                ) {
                    best = Some((holder, last_seen));
                }
            }
        }
    }
    best.map(|(holder, _)| holder)
}

/// Answers a sandbox whose only copy is on `origin_node_id`, or refuses when
/// that node cannot take the resume.
fn pinned_to(
    node_registry: &dyn NodeRegistry,
    state: &str,
    origin_node_id: String,
    label: LookupResultLabel,
    now: SystemTime,
) -> LookupOutcome {
    match schedulable_node(node_registry, &origin_node_id, now) {
        NodeSchedulability::NotReporting => LookupOutcome::FailedPrecondition(
            LookupResultLabel::OriginNotReporting,
            format!("sandbox is {state} on node {origin_node_id}, which is not reporting"),
        ),
        NodeSchedulability::NotAcceptingWork => LookupOutcome::FailedPrecondition(
            LookupResultLabel::OriginUnschedulable,
            format!("sandbox is {state} on node {origin_node_id}, which is not accepting work"),
        ),
        NodeSchedulability::Schedulable(node) => LookupOutcome::Answer(LookupAnswer {
            node,
            location: SandboxLocation::Pinned,
            origin_node_id,
            // The stopped incarnation must not be reused.
            execution_id: String::new(),
            execution_authority: ExecutionAuthority::Pending,
            label,
        }),
    }
}

fn lookup_absent(warm: bool) -> LookupOutcome {
    if warm {
        LookupOutcome::NotFound
    } else {
        LookupOutcome::Unavailable(
            LookupResultLabel::UnavailableColdBindings,
            "scheduler is still seeding sandbox assignments",
        )
    }
}

/// Resolves a non-empty sandbox id through binding, roster, then paused registry.
pub async fn lookup_node(
    deps: &LookupDeps<'_>,
    sandbox_id: &str,
    now: SystemTime,
) -> LookupOutcome {
    match deps.binding_store.get(sandbox_id, now).await {
        Err(_err) => {
            return LookupOutcome::Unavailable(
                LookupResultLabel::UnavailableBindingStore,
                "binding store unavailable",
            );
        }
        // A reservation names the node a create was sent to, not a runtime it
        // acknowledged. Answering it as bound would route traffic at a VM that may
        // not exist; answering absence would license reaping one that does.
        Ok(Some(binding)) if binding.state == BindingState::Starting => {
            return LookupOutcome::Unavailable(
                LookupResultLabel::UnavailableStarting,
                "a create for this sandbox has not finished",
            );
        }
        Ok(Some(binding)) => {
            return LookupOutcome::Answer(LookupAnswer {
                node: binding.node,
                location: SandboxLocation::Bound,
                origin_node_id: String::new(),
                execution_authority: authority_for(&binding.execution_id),
                execution_id: binding.execution_id,
                label: LookupResultLabel::BoundBinding,
            });
        }
        Ok(None) => {}
    }

    // A paused roster entry says where the bytes are, not where a VM is. The
    // registry row decides how such a sandbox wakes; only when there is no row
    // does the roster's node become the answer, as a pin.
    let parked_on = match roster_holder(deps.place.node_registry, sandbox_id, now) {
        Some(holder) if !holder.paused => {
            return LookupOutcome::Answer(LookupAnswer {
                node: holder.node,
                location: SandboxLocation::Bound,
                origin_node_id: String::new(),
                execution_authority: authority_for(&holder.execution_id),
                execution_id: holder.execution_id,
                label: LookupResultLabel::BoundRoster,
            });
        }
        Some(holder) => Some(holder.node.id),
        None => None,
    };

    let warm = deps.warmup.warmed_up(now);

    let entry = match deps.paused_registry {
        Some(registry) if registry.is_cluster_backed() => {
            // A non-UUID sandbox id cannot have a registry row.
            match SandboxId::parse_str(sandbox_id) {
                Ok(id) => match registry.get(&id).await {
                    Ok(entry) => entry,
                    Err(_err) => {
                        return LookupOutcome::Unavailable(
                            LookupResultLabel::UnavailableRegistry,
                            "paused registry unavailable",
                        );
                    }
                },
                Err(_) => None,
            }
        }
        _ => None,
    };

    let Some(entry) = entry else {
        return match parked_on {
            Some(node_id) => pinned_to(
                deps.place.node_registry,
                "paused",
                node_id,
                LookupResultLabel::PinnedRoster,
                now,
            ),
            None => lookup_absent(warm),
        };
    };

    match entry.state {
        PausedRegistryState::Paused => {
            // Published snapshots may be placed anywhere; origin is only preferred.
            match select_node(
                &deps.place,
                None,
                &entry.origin_node_id,
                ShadowSource::PausedLookup,
                now,
            ) {
                Err(NoNodesAvailable) => LookupOutcome::Unavailable(
                    LookupResultLabel::UnavailableNoNodes,
                    "no nodes available",
                ),
                Ok(placement) => LookupOutcome::Answer(LookupAnswer {
                    node: placement.node,
                    location: SandboxLocation::Placed,
                    origin_node_id: entry.origin_node_id,
                    // The node about to resume will mint the new incarnation.
                    execution_id: String::new(),
                    execution_authority: ExecutionAuthority::Pending,
                    label: LookupResultLabel::Placed,
                }),
            }
        }
        PausedRegistryState::Publishing | PausedRegistryState::LocalOnly => {
            // Unpublished state is pinned to the origin node.
            pinned_to(
                deps.place.node_registry,
                entry.state.as_str(),
                entry.origin_node_id,
                LookupResultLabel::Pinned,
                now,
            )
        }
        PausedRegistryState::Running | PausedRegistryState::Resuming => {
            // The holder is always the origin node, never the claimant.
            let holder_id = entry.origin_node_id.clone();
            match live_node(deps.place.node_registry, &holder_id, now) {
                None if !warm => LookupOutcome::Unavailable(
                    LookupResultLabel::UnavailableColdBindings,
                    "scheduler is still seeding sandbox assignments",
                ),
                // A resuming row names where the capture was taken, not where a
                // live sandbox is: the claim holder has not started anything yet.
                // With that machine gone and a snapshot published, the resume may
                // run anywhere, so this arm answers the same way the paused one
                // would have before the claim moved the row out of it.
                None if entry.state == PausedRegistryState::Resuming
                    && entry.snapshot_id.is_some() =>
                {
                    // No preference: the origin is the one node already known not
                    // to be live, and naming it here would place the rebuild back
                    // on the machine this arm exists to route around.
                    match select_node(&deps.place, None, "", ShadowSource::PausedLookup, now) {
                        Err(NoNodesAvailable) => LookupOutcome::Unavailable(
                            LookupResultLabel::UnavailableNoNodes,
                            "no nodes available",
                        ),
                        Ok(placement) => LookupOutcome::Answer(LookupAnswer {
                            node: placement.node,
                            location: SandboxLocation::Placed,
                            origin_node_id: entry.origin_node_id,
                            // The node about to rebuild will mint the new incarnation.
                            execution_id: String::new(),
                            execution_authority: ExecutionAuthority::Pending,
                            label: LookupResultLabel::Placed,
                        }),
                    }
                }
                None => LookupOutcome::FailedPrecondition(
                    LookupResultLabel::HolderUnreachable,
                    format!(
                        "sandbox is {} on node {holder_id}, which is not reporting",
                        entry.state.as_str(),
                    ),
                ),
                Some(node) => {
                    let execution_id = entry
                        .execution_id
                        .map(|id| id.to_string())
                        .unwrap_or_default();
                    LookupOutcome::Answer(LookupAnswer {
                        node,
                        location: SandboxLocation::Bound,
                        origin_node_id: entry.origin_node_id,
                        execution_authority: authority_for(&execution_id),
                        execution_id,
                        label: LookupResultLabel::BoundRegistry,
                    })
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::node_registry::registry::{
        AtomicNodeRegistry, NodeNotInRegistry, ServiceInstanceMismatch, DEFAULT_OBSERVED_REPORT_TTL,
    };
    use crate::node_registry::types::{Roster, RosterEntry};
    use crate::proto::scheduler::{
        HeartbeatRequest, NodeSnapshot, NodeStatus, ObservedNode, P2pPeer,
    };

    struct CountingRegistry {
        inner: AtomicNodeRegistry,
        peeks: AtomicUsize,
        peeks_with_freshness: AtomicUsize,
    }

    impl CountingRegistry {
        fn new(nodes: Vec<Node>) -> Self {
            Self {
                inner: AtomicNodeRegistry::new(nodes, DEFAULT_OBSERVED_REPORT_TTL),
                peeks: AtomicUsize::new(0),
                peeks_with_freshness: AtomicUsize::new(0),
            }
        }
    }

    impl NodeRegistry for CountingRegistry {
        fn snapshot(&self, allow_lingering: bool) -> Vec<Node> {
            self.inner.snapshot(allow_lingering)
        }
        fn contains(&self, node: &Node) -> bool {
            self.inner.contains(node)
        }
        fn resolve(&self, node_id: &str) -> Option<Node> {
            self.inner.resolve(node_id)
        }
        fn heartbeat(
            &self,
            req: &HeartbeatRequest,
            now: SystemTime,
        ) -> Result<(Node, String), NodeNotInRegistry> {
            self.inner.heartbeat(req, now)
        }
        fn list_observed(&self, cluster_id: &str, now: SystemTime) -> Vec<ObservedNode> {
            self.inner.list_observed(cluster_id, now)
        }
        fn list_p2p_peers(
            &self,
            cluster_id: &str,
            backend: &str,
            exclude_node_id: &str,
            now: SystemTime,
        ) -> Vec<P2pPeer> {
            self.inner
                .list_p2p_peers(cluster_id, backend, exclude_node_id, now)
        }
        fn filter_p2p_peers(
            &self,
            cluster_id: &str,
            backend: &str,
            node_ids: &[String],
            exclude_node_id: &str,
            now: SystemTime,
        ) -> Vec<P2pPeer> {
            self.inner
                .filter_p2p_peers(cluster_id, backend, node_ids, exclude_node_id, now)
        }
        fn get_observed(
            &self,
            node_id: &str,
            cluster_id: &str,
            now: SystemTime,
        ) -> Option<ObservedNode> {
            self.inner.get_observed(node_id, cluster_id, now)
        }
        fn peek_observed(&self, node_id: &str) -> Option<NodeSnapshot> {
            self.peeks.fetch_add(1, Ordering::Relaxed);
            self.inner.peek_observed(node_id)
        }
        fn peek_observed_with_freshness(
            &self,
            node_id: &str,
            now: SystemTime,
        ) -> Option<(NodeSnapshot, SnapshotFreshness)> {
            self.peeks_with_freshness.fetch_add(1, Ordering::Relaxed);
            self.inner.peek_observed_with_freshness(node_id, now)
        }
        fn roster_of(&self, node_id: &str) -> Option<(Vec<RosterEntry>, SystemTime)> {
            self.inner.roster_of(node_id)
        }
        fn nodes_holding(&self, sandbox_id: &str) -> Vec<String> {
            self.inner.nodes_holding(sandbox_id)
        }
        fn rosters_in_cluster(&self, cluster_id: &str) -> Vec<Roster> {
            self.inner.rosters_in_cluster(cluster_id)
        }
        fn unregister_observed(
            &self,
            node_id: &str,
            service_instance_id: &str,
        ) -> Result<(), ServiceInstanceMismatch> {
            self.inner.unregister_observed(node_id, service_instance_id)
        }
        fn applied_cpu_intersection(&self, cluster_id: &str) -> Option<String> {
            self.inner.applied_cpu_intersection(cluster_id)
        }
    }

    fn node(id: &str) -> Node {
        Node {
            id: id.to_string(),
            endpoint: format!("http://{id}:8000"),
            pod_name: String::new(),
        }
    }

    fn heartbeat(node_id: &str) -> HeartbeatRequest {
        HeartbeatRequest {
            node_id: node_id.to_string(),
            cluster_id: "cluster-a".to_string(),
            service_instance_id: format!("{node_id}-instance"),
            snapshot: Some(NodeSnapshot {
                status: NodeStatus::Ready as i32,
                allocated_cpu: 1,
                allocated_memory_bytes: 1024 * 1024 * 1024,
                cpu_count: 8,
                memory_total_bytes: 8 * 1024 * 1024 * 1024,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn a_selection_reads_each_candidate_from_the_registry_exactly_once() {
        let registry = CountingRegistry::new(vec![node("node-a"), node("node-b"), node("node-c")]);
        let now = SystemTime::now();
        for node_id in ["node-a", "node-b", "node-c"] {
            registry
                .heartbeat(&heartbeat(node_id), now)
                .expect("heartbeat");
        }
        registry.peeks.store(0, Ordering::Relaxed);
        registry.peeks_with_freshness.store(0, Ordering::Relaxed);

        let strategy = RoundRobinStrategy::new();
        let shadow = ShadowPlacement::default();
        let deps = ScheduleDeps {
            node_registry: &registry,
            strategy: &strategy,
            shadow: &shadow,
        };

        let placement = select_node(&deps, None, "", ShadowSource::Schedule, now)
            .expect("three discovered nodes");
        assert_eq!(placement.candidates, 3);
        assert_eq!(placement.eligible, 3);

        assert_eq!(
            registry.peeks_with_freshness.load(Ordering::Relaxed),
            3,
            "exactly one combined read per candidate"
        );
        assert_eq!(
            registry.peeks.load(Ordering::Relaxed),
            0,
            "the split accessor is not called a second time on this path"
        );
    }

    #[test]
    fn a_preferred_node_neither_asks_the_strategy_nor_moves_its_cursor() {
        let registry = CountingRegistry::new(vec![node("node-a"), node("node-b")]);
        let now = SystemTime::now();
        for node_id in ["node-a", "node-b"] {
            registry
                .heartbeat(&heartbeat(node_id), now)
                .expect("heartbeat");
        }

        let strategy = RoundRobinStrategy::new();
        let shadow = ShadowPlacement::default();
        let deps = ScheduleDeps {
            node_registry: &registry,
            strategy: &strategy,
            shadow: &shadow,
        };

        // Repeated bare selections expose any cursor movement by preferred calls.
        for _ in 0..2 {
            let placement = select_node(&deps, None, "node-b", ShadowSource::PausedLookup, now)
                .expect("node-b is a candidate");
            assert_eq!(placement.node.id, "node-b");
        }
        let rotation: Vec<String> = (0..3)
            .map(|_| {
                select_node(&deps, None, "", ShadowSource::Schedule, now)
                    .expect("two discovered nodes")
                    .node
                    .id
            })
            .collect();
        assert_eq!(rotation, vec!["node-a", "node-b", "node-a"]);
    }

    /// Stage three of `lookup_node` answered from one fixed row; every write
    /// path panics, the same way `grpc_service.rs`'s `FakePausedRegistry` does.
    struct OneRowRegistry {
        entry: crate::orchestrator::PausedSandboxEntry,
    }

    #[async_trait::async_trait]
    impl PausedSandboxRegistry for OneRowRegistry {
        async fn get(
            &self,
            sandbox_id: &SandboxId,
        ) -> crate::orchestrator::RegistryResult<Option<crate::orchestrator::PausedSandboxEntry>>
        {
            Ok((self.entry.sandbox_id == *sandbox_id).then(|| self.entry.clone()))
        }

        fn is_cluster_backed(&self) -> bool {
            true
        }

        async fn begin_pause(
            &self,
            _entry: &crate::orchestrator::PausedSandboxEntry,
        ) -> crate::orchestrator::RegistryResult<crate::orchestrator::BeganPause> {
            unimplemented!("lookup_node never calls this")
        }
        async fn complete_pause(
            &self,
            _sandbox_id: &SandboxId,
            _generation: i64,
            _snapshot_id: &crate::snapshot::SnapshotId,
        ) -> crate::orchestrator::RegistryResult<()> {
            unimplemented!("lookup_node never calls this")
        }
        async fn mark_local_only(
            &self,
            _sandbox_id: &SandboxId,
            _generation: i64,
        ) -> crate::orchestrator::RegistryResult<()> {
            unimplemented!("lookup_node never calls this")
        }
        async fn get_many(
            &self,
            _sandbox_ids: &[SandboxId],
        ) -> crate::orchestrator::RegistryResult<crate::orchestrator::PausedRegistryRows> {
            unimplemented!("lookup_node never calls this")
        }
        async fn claim_for_resume(
            &self,
            _sandbox_id: &SandboxId,
            _node_id: &str,
            _execution_id: crate::types::ExecutionId,
        ) -> crate::orchestrator::RegistryResult<crate::orchestrator::ResumeClaim> {
            unimplemented!("lookup_node never calls this")
        }
        async fn release_claim(
            &self,
            _sandbox_id: &SandboxId,
            _generation: i64,
        ) -> crate::orchestrator::RegistryResult<bool> {
            unimplemented!("lookup_node never calls this")
        }
        async fn renew_lease(
            &self,
            _node_id: &str,
            _held: &[crate::orchestrator::HeldSandbox],
        ) -> crate::orchestrator::RegistryResult<u64> {
            unimplemented!("lookup_node never calls this")
        }
        async fn reclaim_expired_holdings(
            &self,
        ) -> crate::orchestrator::RegistryResult<crate::orchestrator::ReclaimedHoldings> {
            unimplemented!("lookup_node never calls this")
        }
        async fn mark_running(
            &self,
            _sandbox_id: &SandboxId,
            _node_id: &str,
            _holder_node_id: &str,
            _execution_id: crate::types::ExecutionId,
            _expires_at: Option<SystemTime>,
        ) -> crate::orchestrator::RegistryResult<crate::orchestrator::MarkRunningOutcome> {
            unimplemented!("lookup_node never calls this")
        }
        async fn renew_sandbox_deadline(
            &self,
            _sandbox_id: &SandboxId,
            _execution_id: crate::types::ExecutionId,
            _expires_at: Option<SystemTime>,
        ) -> crate::orchestrator::RegistryResult<crate::orchestrator::DeadlineRenewalOutcome>
        {
            unimplemented!("lookup_node never calls this")
        }
        async fn release_node_holdings(
            &self,
            _node_id: &str,
        ) -> crate::orchestrator::RegistryResult<crate::orchestrator::ReleasedHoldings> {
            unimplemented!("lookup_node never calls this")
        }
        async fn remove(
            &self,
            _sandbox_id: &SandboxId,
            _generation: i64,
        ) -> crate::orchestrator::RegistryResult<bool> {
            unimplemented!("lookup_node never calls this")
        }
        async fn list_all(
            &self,
        ) -> crate::orchestrator::RegistryResult<crate::orchestrator::PausedRegistryListing>
        {
            unimplemented!("lookup_node never calls this")
        }
    }

    fn registry_row(
        sandbox_id: SandboxId,
        state: PausedRegistryState,
        origin_node_id: &str,
    ) -> OneRowRegistry {
        let now = chrono::Utc::now();
        OneRowRegistry {
            entry: crate::orchestrator::PausedSandboxEntry {
                sandbox_id,
                cluster_id: uuid::Uuid::nil(),
                state,
                generation: 1,
                origin_node_id: origin_node_id.to_string(),
                claimed_by_node_id: None,
                snapshot_id: None,
                metadata: None,
                execution_id: None,
                paused_at: now,
                updated_at: now,
            },
        }
    }

    /// node-a reporting one sandbox, exactly as `roster_from_heartbeat` reads
    /// a node's `list_sandbox_roster`: parked sandboxes stay in the roster
    /// with `paused` set.
    fn node_a_reporting(
        sandbox_id: SandboxId,
        execution_id: &str,
        paused: bool,
    ) -> AtomicNodeRegistry {
        let registry = AtomicNodeRegistry::new(vec![node("node-a")], DEFAULT_OBSERVED_REPORT_TTL);
        registry
            .heartbeat(
                &HeartbeatRequest {
                    roster: vec![crate::proto::scheduler::SandboxRosterEntry {
                        sandbox_id: sandbox_id.to_string(),
                        execution_id: execution_id.to_string(),
                        projection_ttl_secs: 0,
                        paused,
                    }],
                    ..heartbeat("node-a")
                },
                SystemTime::now(),
            )
            .expect("node-a is discovered");
        registry
    }

    async fn look_up(
        registry: &AtomicNodeRegistry,
        paused_registry: Option<&dyn PausedSandboxRegistry>,
        sandbox_id: SandboxId,
    ) -> LookupOutcome {
        let now = SystemTime::now();
        let strategy = RoundRobinStrategy::new();
        let shadow = ShadowPlacement::default();
        let binding_store = crate::binding_store::InMemoryBindingStore::new(
            crate::binding_store::BindingStoreSettings::default(),
        );
        let warmup = WarmupGate::new(
            std::sync::Arc::new(AtomicNodeRegistry::new(vec![], DEFAULT_OBSERVED_REPORT_TTL)),
            std::time::Duration::from_secs(1),
            SystemTime::UNIX_EPOCH,
        );
        warmup.reported_in(now);
        let deps = LookupDeps {
            place: ScheduleDeps {
                node_registry: registry,
                strategy: &strategy,
                shadow: &shadow,
            },
            binding_store: &binding_store,
            paused_registry,
            warmup: &warmup,
        };
        lookup_node(&deps, &sandbox_id.to_string(), now).await
    }

    fn answer(outcome: LookupOutcome) -> LookupAnswer {
        match outcome {
            LookupOutcome::Answer(answer) => answer,
            other => panic!("expected an answer, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_roster_entry_is_bound_only_while_the_node_runs_it() {
        let sandbox_id = SandboxId::new();
        let execution_id = crate::types::ExecutionId::new().to_string();

        let running = answer(
            look_up(
                &node_a_reporting(sandbox_id, &execution_id, false),
                None,
                sandbox_id,
            )
            .await,
        );
        assert_eq!(running.location, SandboxLocation::Bound);
        assert_eq!(running.label, LookupResultLabel::BoundRoster);
        assert_eq!(running.execution_id, execution_id);
        assert_eq!(running.execution_authority, ExecutionAuthority::Registry);

        // The same entry with the flag set is where the bytes are, not a VM
        // to route to: with no registry row, the node that parks it is the
        // only place the sandbox can wake.
        let parked = answer(
            look_up(
                &node_a_reporting(sandbox_id, &execution_id, true),
                None,
                sandbox_id,
            )
            .await,
        );
        assert_eq!(parked.location, SandboxLocation::Pinned);
        assert_eq!(parked.label, LookupResultLabel::PinnedRoster);
        assert_eq!(parked.node.id, "node-a");
        assert_eq!(parked.origin_node_id, "node-a");
        assert_eq!(
            parked.execution_id, "",
            "the paused incarnation is not reused"
        );
        assert_eq!(parked.execution_authority, ExecutionAuthority::Pending);
    }

    #[tokio::test]
    async fn a_paused_roster_entry_defers_to_the_registry_row() {
        let sandbox_id = SandboxId::new();
        let execution_id = crate::types::ExecutionId::new().to_string();
        let registry = node_a_reporting(sandbox_id, &execution_id, true);

        let published = registry_row(sandbox_id, PausedRegistryState::Paused, "node-a");
        let placed = answer(look_up(&registry, Some(&published), sandbox_id).await);
        assert_eq!(placed.location, SandboxLocation::Placed);
        assert_eq!(placed.label, LookupResultLabel::Placed);
        assert_eq!(placed.node.id, "node-a", "the origin is preferred");

        let unpublished = registry_row(sandbox_id, PausedRegistryState::LocalOnly, "node-a");
        let pinned = answer(look_up(&registry, Some(&unpublished), sandbox_id).await);
        assert_eq!(pinned.location, SandboxLocation::Pinned);
        assert_eq!(pinned.label, LookupResultLabel::Pinned);
        assert_eq!(pinned.node.id, "node-a");

        // A row for some other sandbox is no row for this one.
        let other = registry_row(SandboxId::new(), PausedRegistryState::Paused, "node-a");
        let parked = answer(look_up(&registry, Some(&other), sandbox_id).await);
        assert_eq!(parked.label, LookupResultLabel::PinnedRoster);
    }
}
