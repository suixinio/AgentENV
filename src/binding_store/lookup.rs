//! Shared node-selection and sandbox-lookup logic for `Schedule` and the
//! in-process sandbox lookup.

use std::collections::HashMap;
use std::time::SystemTime;

use crate::binding_store::{BindingState, BindingStore};
use crate::node_registry::filter::{filter_unschedulable, filter_without_egress_broker};
use crate::node_registry::placement::score::SnapshotFreshness;
use crate::node_registry::placement::{
    request_from_hint, ShadowCandidate, ShadowPlacement, ShadowRequest, ShadowSource,
};
use crate::node_registry::registry::NodeRegistry;
use crate::node_registry::strategy::{NoNodesAvailable, RoundRobinStrategy};
use crate::node_registry::types::{Node, RichNode};
use crate::node_registry::warmup::WarmupGate;
use crate::proto::scheduler::ScheduleRequestHint;

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

/// Why `select_node` chose nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SelectNodeError {
    #[error(transparent)]
    NoNodes(#[from] NoNodesAvailable),
    /// Schedulable nodes exist, but the hint needs an egress broker and none
    /// of them reports one.
    #[error("no schedulable node reports a usable egress broker")]
    NoEgressBrokerNode,
}

/// Selects an eligible node, honoring preference before shared round-robin.
/// Nodes named in `excluded_node_ids` are never chosen, preferred or not.
/// Shadow scoring runs only after real selection using the same registry reads.
pub fn select_node(
    deps: &ScheduleDeps<'_>,
    hint: Option<&ScheduleRequestHint>,
    prefer_node_id: &str,
    excluded_node_ids: &[String],
    source: ShadowSource,
    now: SystemTime,
) -> Result<Placement, SelectNodeError> {
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
    let excluded: Vec<String> = excluded_node_ids
        .iter()
        .map(|id| {
            deps.node_registry
                .resolve(id.trim())
                .map(|n| n.id)
                .unwrap_or_else(|| id.trim().to_string())
        })
        .collect();
    let eligible_nodes: Vec<RichNode> = filter_unschedulable(rich)
        .into_iter()
        .filter(|rich| !excluded.contains(&rich.node.id))
        .collect();
    let requires_egress_broker = hint
        .and_then(|hint| match hint.kind.as_ref() {
            Some(crate::proto::scheduler::schedule_request_hint::Kind::NewSandbox(hint)) => {
                Some(hint.requires_egress_broker)
            }
            _ => None,
        })
        .unwrap_or(false);
    let eligible_nodes = if requires_egress_broker {
        let schedulable = eligible_nodes.len();
        let capable = filter_without_egress_broker(eligible_nodes);
        if capable.is_empty() && schedulable > 0 {
            return Err(SelectNodeError::NoEgressBrokerNode);
        }
        capable
    } else {
        eligible_nodes
    };
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
    NotFound,
    InvalidArgument,
    UnavailableBindingStore,
    UnavailableColdBindings,
    UnavailableStarting,
}

impl LookupResultLabel {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BoundBinding => "bound_binding",
            Self::BoundRoster => "bound_roster",
            Self::NotFound => "not_found",
            Self::InvalidArgument => "invalid_argument",
            Self::UnavailableBindingStore => "unavailable_binding_store",
            Self::UnavailableColdBindings => "unavailable_cold_bindings",
            Self::UnavailableStarting => "unavailable_starting",
        }
    }
}

/// Whether an answer's `execution_id` may be used to refuse traffic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionAuthority {
    /// Nobody can name the current incarnation; the caller lets the request
    /// through.
    Unknown,
    /// `execution_id` names the incarnation that should be alive on the node
    /// named, right now. Never reported with an empty id.
    Registry,
}

/// Successful lookup result: a node known to be running the sandbox.
#[derive(Debug, Clone)]
pub struct LookupAnswer {
    pub node: Node,
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
}

impl LookupOutcome {
    pub fn label(&self) -> LookupResultLabel {
        match self {
            Self::Answer(answer) => answer.label,
            Self::NotFound => LookupResultLabel::NotFound,
            Self::Unavailable(label, _) => *label,
        }
    }
}

/// Dependencies used by [`lookup_node`].
pub struct LookupDeps<'a> {
    pub place: ScheduleDeps<'a>,
    pub binding_store: &'a dyn BindingStore,
    pub warmup: &'a WarmupGate,
}

fn authority_for(execution_id: &str) -> ExecutionAuthority {
    if execution_id.is_empty() {
        ExecutionAuthority::Unknown
    } else {
        ExecutionAuthority::Registry
    }
}

fn roster_fresh(last_seen: SystemTime, now: SystemTime) -> bool {
    now.duration_since(last_seen)
        .map(|age| age <= ROSTER_FRESH_TTL)
        .unwrap_or(true)
}

/// The node whose freshest heartbeat roster names `sandbox_id`, preferring the
/// lexicographically newer incarnation when two nodes both claim it.
fn roster_holder(
    node_registry: &dyn NodeRegistry,
    sandbox_id: &str,
    now: SystemTime,
) -> Option<(Node, String)> {
    let mut best: Option<(Node, String)> = None;
    for node_id in node_registry.nodes_holding(sandbox_id) {
        let Some((entries, last_seen)) = node_registry.roster_of(&node_id) else {
            continue;
        };
        if !roster_fresh(last_seen, now) {
            continue;
        }
        let Some(entry) = entries.iter().find(|entry| entry.sandbox_id == sandbox_id) else {
            continue;
        };
        let Some(node) = node_registry.resolve(&node_id) else {
            continue;
        };
        let newer = best
            .as_ref()
            .map(|(_, execution_id)| entry.execution_id > *execution_id)
            .unwrap_or(true);
        if newer {
            best = Some((node, entry.execution_id.clone()));
        }
    }
    best
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

/// Resolves a non-empty sandbox id to the node running it: the binding first,
/// then the heartbeat roster. A paused sandbox has neither and is `NotFound`;
/// its snapshot row, not this lookup, is what a resume consults.
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
                execution_authority: authority_for(&binding.execution_id),
                execution_id: binding.execution_id,
                label: LookupResultLabel::BoundBinding,
            });
        }
        Ok(None) => {}
    }

    if let Some((node, execution_id)) = roster_holder(deps.place.node_registry, sandbox_id, now) {
        return LookupOutcome::Answer(LookupAnswer {
            node,
            execution_authority: authority_for(&execution_id),
            execution_id,
            label: LookupResultLabel::BoundRoster,
        });
    }

    lookup_absent(deps.warmup.warmed_up(now))
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
    use crate::types::SandboxId;

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

        let placement = select_node(&deps, None, "", &[], ShadowSource::Schedule, now)
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
            let placement =
                select_node(&deps, None, "node-b", &[], ShadowSource::PausedLookup, now)
                    .expect("node-b is a candidate");
            assert_eq!(placement.node.id, "node-b");
        }
        let rotation: Vec<String> = (0..3)
            .map(|_| {
                select_node(&deps, None, "", &[], ShadowSource::Schedule, now)
                    .expect("two discovered nodes")
                    .node
                    .id
            })
            .collect();
        assert_eq!(rotation, vec!["node-a", "node-b", "node-a"]);
    }

    fn heartbeat_with_broker(
        node_id: &str,
        state: crate::proto::scheduler::EgressBrokerState,
    ) -> HeartbeatRequest {
        let mut request = heartbeat(node_id);
        if let Some(snapshot) = request.snapshot.as_mut() {
            snapshot.egress_broker = state as i32;
        }
        request
    }

    fn new_sandbox_hint(requires_egress_broker: bool) -> ScheduleRequestHint {
        ScheduleRequestHint {
            kind: Some(
                crate::proto::scheduler::schedule_request_hint::Kind::NewSandbox(
                    crate::proto::scheduler::NewSandboxHint {
                        requires_egress_broker,
                        ..Default::default()
                    },
                ),
            ),
        }
    }

    #[test]
    fn a_preferred_node_without_a_broker_is_skipped_rather_than_preferred() {
        use crate::proto::scheduler::EgressBrokerState;

        let registry = AtomicNodeRegistry::new(
            vec![node("node-a"), node("node-b")],
            DEFAULT_OBSERVED_REPORT_TTL,
        );
        let now = SystemTime::now();
        registry
            .heartbeat(
                &heartbeat_with_broker("node-a", EgressBrokerState::LocalUnreachable),
                now,
            )
            .expect("node-a is discovered");
        registry
            .heartbeat(
                &heartbeat_with_broker("node-b", EgressBrokerState::LocalOk),
                now,
            )
            .expect("node-b is discovered");

        let strategy = RoundRobinStrategy::new();
        let shadow = ShadowPlacement::default();
        let deps = ScheduleDeps {
            node_registry: &registry,
            strategy: &strategy,
            shadow: &shadow,
        };

        // The preference names the node whose broker is down; a sandbox with
        // rules would fail on its first connection there.
        let hint = new_sandbox_hint(true);
        let placement = select_node(
            &deps,
            Some(&hint),
            "node-a",
            &[],
            ShadowSource::Schedule,
            now,
        )
        .expect("node-b can broker");
        assert_eq!(placement.node.id, "node-b");
        assert_eq!(placement.eligible, 1);

        // Without the requirement the same preference is honoured.
        let hint = new_sandbox_hint(false);
        let placement = select_node(
            &deps,
            Some(&hint),
            "node-a",
            &[],
            ShadowSource::Schedule,
            now,
        )
        .expect("both nodes are schedulable");
        assert_eq!(placement.node.id, "node-a");
    }

    #[test]
    fn a_fleet_with_no_broker_refuses_rather_than_placing_anywhere() {
        use crate::proto::scheduler::EgressBrokerState;

        let registry = AtomicNodeRegistry::new(vec![node("node-a")], DEFAULT_OBSERVED_REPORT_TTL);
        let now = SystemTime::now();
        registry
            .heartbeat(
                &heartbeat_with_broker("node-a", EgressBrokerState::Disabled),
                now,
            )
            .expect("node-a is discovered");

        let strategy = RoundRobinStrategy::new();
        let shadow = ShadowPlacement::default();
        let deps = ScheduleDeps {
            node_registry: &registry,
            strategy: &strategy,
            shadow: &shadow,
        };

        let hint = new_sandbox_hint(true);
        let err = select_node(&deps, Some(&hint), "", &[], ShadowSource::Schedule, now)
            .expect_err("no node reports a broker");
        assert!(matches!(err, SelectNodeError::NoEgressBrokerNode));
    }

    /// node-a reporting one sandbox, exactly as `roster_from_heartbeat` reads
    /// a node's `list_sandbox_roster`.
    fn node_a_reporting(
        sandbox_id: SandboxId,
        execution_id: &str,
        reported_at: SystemTime,
    ) -> AtomicNodeRegistry {
        let registry = AtomicNodeRegistry::new(vec![node("node-a")], DEFAULT_OBSERVED_REPORT_TTL);
        registry
            .heartbeat(
                &HeartbeatRequest {
                    roster: vec![crate::proto::scheduler::SandboxRosterEntry {
                        sandbox_id: sandbox_id.to_string(),
                        execution_id: execution_id.to_string(),
                        projection_ttl_secs: 0,
                    }],
                    ..heartbeat("node-a")
                },
                reported_at,
            )
            .expect("node-a is discovered");
        registry
    }

    async fn look_up(
        registry: &AtomicNodeRegistry,
        binding_store: &dyn BindingStore,
        warm: bool,
        sandbox_id: SandboxId,
        now: SystemTime,
    ) -> LookupOutcome {
        let strategy = RoundRobinStrategy::new();
        let shadow = ShadowPlacement::default();
        let warmup = WarmupGate::new(
            std::sync::Arc::new(AtomicNodeRegistry::new(vec![], DEFAULT_OBSERVED_REPORT_TTL)),
            if warm {
                std::time::Duration::from_secs(1)
            } else {
                std::time::Duration::from_secs(3600)
            },
            if warm { SystemTime::UNIX_EPOCH } else { now },
        );
        if warm {
            warmup.reported_in(now);
        }
        let deps = LookupDeps {
            place: ScheduleDeps {
                node_registry: registry,
                strategy: &strategy,
                shadow: &shadow,
            },
            binding_store,
            warmup: &warmup,
        };
        lookup_node(&deps, &sandbox_id.to_string(), now).await
    }

    fn empty_binding_store() -> crate::binding_store::InMemoryBindingStore {
        crate::binding_store::InMemoryBindingStore::new(
            crate::binding_store::BindingStoreSettings::default(),
        )
    }

    fn answer(outcome: LookupOutcome) -> LookupAnswer {
        match outcome {
            LookupOutcome::Answer(answer) => answer,
            other => panic!("expected an answer, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_fresh_roster_entry_binds_the_sandbox_to_the_node_reporting_it() {
        let sandbox_id = SandboxId::new();
        let execution_id = crate::types::ExecutionId::new().to_string();
        let now = SystemTime::now();

        let running = answer(
            look_up(
                &node_a_reporting(sandbox_id, &execution_id, now),
                &empty_binding_store(),
                true,
                sandbox_id,
                now,
            )
            .await,
        );
        assert_eq!(running.label, LookupResultLabel::BoundRoster);
        assert_eq!(running.node.id, "node-a");
        assert_eq!(running.execution_id, execution_id);
        assert_eq!(running.execution_authority, ExecutionAuthority::Registry);
    }

    #[tokio::test]
    async fn a_roster_entry_without_an_incarnation_binds_with_unknown_authority() {
        let sandbox_id = SandboxId::new();
        let now = SystemTime::now();

        let running = answer(
            look_up(
                &node_a_reporting(sandbox_id, "", now),
                &empty_binding_store(),
                true,
                sandbox_id,
                now,
            )
            .await,
        );
        assert_eq!(running.label, LookupResultLabel::BoundRoster);
        assert_eq!(running.execution_id, "");
        assert_eq!(running.execution_authority, ExecutionAuthority::Unknown);
    }

    #[tokio::test]
    async fn a_stale_roster_entry_is_an_absence() {
        let sandbox_id = SandboxId::new();
        let execution_id = crate::types::ExecutionId::new().to_string();
        let reported_at = SystemTime::now();
        let now = reported_at + ROSTER_FRESH_TTL + std::time::Duration::from_secs(1);

        let outcome = look_up(
            &node_a_reporting(sandbox_id, &execution_id, reported_at),
            &empty_binding_store(),
            true,
            sandbox_id,
            now,
        )
        .await;
        assert!(
            matches!(outcome, LookupOutcome::NotFound),
            "a roster older than ROSTER_FRESH_TTL must not route: {outcome:?}"
        );
    }

    #[tokio::test]
    async fn a_binding_answers_ahead_of_the_roster() {
        let sandbox_id = SandboxId::new();
        let roster_execution_id = crate::types::ExecutionId::new().to_string();
        let bound_execution_id = crate::types::ExecutionId::new().to_string();
        let now = SystemTime::now();
        let registry = node_a_reporting(sandbox_id, &roster_execution_id, now);
        let binding_store = empty_binding_store();
        binding_store
            .record(
                &sandbox_id.to_string(),
                crate::binding_store::Binding {
                    node: node("node-b"),
                    execution_id: bound_execution_id.clone(),
                    projection_ttl: std::time::Duration::ZERO,
                    state: BindingState::Confirmed,
                },
                now,
            )
            .await
            .expect("install the binding");

        let bound = answer(look_up(&registry, &binding_store, true, sandbox_id, now).await);
        assert_eq!(bound.label, LookupResultLabel::BoundBinding);
        assert_eq!(bound.node.id, "node-b");
        assert_eq!(bound.execution_id, bound_execution_id);
        assert_eq!(bound.execution_authority, ExecutionAuthority::Registry);
    }

    #[tokio::test]
    async fn a_starting_binding_is_neither_bound_nor_absent() {
        let sandbox_id = SandboxId::new();
        let now = SystemTime::now();
        let registry = AtomicNodeRegistry::new(vec![node("node-a")], DEFAULT_OBSERVED_REPORT_TTL);
        let binding_store = empty_binding_store();
        binding_store
            .record(
                &sandbox_id.to_string(),
                crate::binding_store::Binding {
                    node: node("node-a"),
                    execution_id: crate::types::ExecutionId::new().to_string(),
                    projection_ttl: std::time::Duration::from_secs(300),
                    state: BindingState::Starting,
                },
                now,
            )
            .await
            .expect("reserve");

        let outcome = look_up(&registry, &binding_store, true, sandbox_id, now).await;
        assert!(
            matches!(
                outcome,
                LookupOutcome::Unavailable(LookupResultLabel::UnavailableStarting, _)
            ),
            "{outcome:?}"
        );
    }

    #[tokio::test]
    async fn an_absent_sandbox_is_not_found_once_warm_and_unavailable_while_cold() {
        let sandbox_id = SandboxId::new();
        let now = SystemTime::now();
        let registry = AtomicNodeRegistry::new(vec![node("node-a")], DEFAULT_OBSERVED_REPORT_TTL);

        let warm = look_up(&registry, &empty_binding_store(), true, sandbox_id, now).await;
        assert!(matches!(warm, LookupOutcome::NotFound), "{warm:?}");
        assert_eq!(warm.label(), LookupResultLabel::NotFound);

        let cold = look_up(&registry, &empty_binding_store(), false, sandbox_id, now).await;
        assert!(
            matches!(
                cold,
                LookupOutcome::Unavailable(LookupResultLabel::UnavailableColdBindings, _)
            ),
            "{cold:?}"
        );
    }
}
