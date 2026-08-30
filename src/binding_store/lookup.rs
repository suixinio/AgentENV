//! Task's own "Stage D remainder": `Schedule`/`LookupNode`'s shared pure
//! logic, ported from `services/scheduler/internal/{lookup.go,service.go}`.
//!
//! Kept out of `src/node_registry/grpc_service.rs` on purpose (the phase's
//! own suggestion): the three-stage lookup ladder has roughly a dozen
//! distinct outcomes to get right, and testing each one through a live
//! gRPC service is both slow and awkward to set up. Everything here takes
//! borrowed trait objects (`&dyn BindingStore`, `&dyn NodeRegistry`, ...)
//! instead of the concrete `Arc`-wrapped types `NodeRegistryGrpcService`
//! holds, so a unit test can hand it a bare in-memory fake for exactly the
//! one dependency it wants to exercise.
//!
//! # Two Rust/Go shape differences, both narrowing the label set
//!
//! - Go's `pausedregistry.Reader` distinguishes "disabled" (`ErrDisabled`)
//!   from "not yet ready to answer" (`!Ready()`) from "configured and
//!   readable, no row" (`!found`) -- three states behind one `Get` call.
//!   Rust's [`PausedSandboxRegistry`] has no warm-up/cold-start concept at
//!   all (it talks to Postgres directly, no local cache to warm) and
//!   `get()` returns `Ok(None)` uniformly for "no row," whether or not a
//!   real backend is configured. So there is no Rust counterpart to
//!   `unavailable_registry_cold`, and "disabled" collapses into the same
//!   `Ok(None)` path ordinary absence takes -- gated instead on
//!   [`PausedSandboxRegistry::is_cluster_backed`], see [`lookup_node`]'s
//!   stage 3.
//! - Go's `nodePlacer` can be `nil` on a query-only replica, which has no
//!   discovery and no strategy at all -- hence `lookupResultNoPlacer`.
//!   `NodeRegistryGrpcService` has no query-only mode: a registry and a
//!   strategy exist the moment the service does. So there is no Rust
//!   counterpart to `unavailable_no_placer`, and none to
//!   `internal_placement_failed` either -- [`select_node`]'s only failure
//!   mode is [`NoNodesAvailable`], never a second, less-structured error
//!   Go's `Schedule` falls back to `codes.Internal` for. And `unknown_state`
//!   has no Rust counterpart for a different reason: [`PausedRegistryState`]
//!   is a closed Rust enum matched exhaustively below, so there is no "row
//!   written by a future build" case to refuse at runtime -- the compiler
//!   already refused it, at whatever call site could construct one.
//!
//! `silentExecution` (Go's rollback mode that blanks the two incarnation
//! fields) has no Rust build to roll back from -- this port only ever
//! existed with `execution_id`/`execution_authority` populated, so there is
//! nothing here to gate.

use std::collections::HashMap;
use std::time::SystemTime;

use crate::binding_store::BindingStore;
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
use crate::proto::scheduler::{ExecutionAuthority, SandboxLocation, ScheduleRequestHint};
use crate::types::SandboxId;

/// How long a heartbeat-reported roster entry stays "fresh enough to route
/// to" -- mirrors Go's `Service.reportTTL`, which defaults to
/// `defaultObservedReportTTL` (`lookup.go`'s `rosterFresh`) and, in this
/// codebase, is the same 30s literal `src/bin/aenv-api.rs`'s
/// `start_native_node_registry` already passes as `AtomicNodeRegistry`'s own
/// `observed_ttl`.
pub use crate::node_registry::registry::DEFAULT_OBSERVED_REPORT_TTL as ROSTER_FRESH_TTL;

/// Everything [`select_node`] needs. Shared between `Schedule` (bare
/// preference) and [`lookup_node`]'s `Paused` branch (origin-node
/// preference) -- Go's `Service.place` is exactly this reuse.
///
/// 🔴 `strategy` is a borrow of one shared [`RoundRobinStrategy`], never a
/// fresh one per call: the round-robin cursor is that instance's atomic,
/// so two `ScheduleDeps` pointing at two instances would silently give
/// `Schedule` and [`lookup_node`]'s `Paused` branch independent rotations.
#[derive(Clone, Copy)]
pub struct ScheduleDeps<'a> {
    pub node_registry: &'a dyn NodeRegistry,
    pub strategy: &'a RoundRobinStrategy,
    /// The shadow scorer, holding the sampling width and the metric handles
    /// its construction resolved once.
    ///
    /// 🔴 Deliberately *not* where the [`ShadowSource`] lives. Both call
    /// paths share one of these — that sharing is the point for `strategy`
    /// — so a source stored here would be whatever the last caller set it
    /// to. It is a `select_node` argument instead, supplied by each of the
    /// two real call sites.
    pub shadow: &'a ShadowPlacement,
}

/// One selection, with the candidate counts it was taken over -- mirrors
/// Go's `placement` struct.
#[derive(Debug, Clone)]
pub struct Placement {
    pub node: Node,
    pub candidates: usize,
    pub eligible: usize,
}

/// Ports `Service.selectNode` (`service.go:366-427`). `prefer_node_id`
/// empty is `Schedule`'s own call; non-empty is `place`'s (the `Paused`
/// branch below).
///
/// The preference is applied after filtering, never before: a preference
/// that could bring back a node the filters removed would let an isolated
/// or overloaded node be selected by the one path that never asked the
/// strategy.
///
/// # The shadow scorer sits at the end, and only at the end
///
/// [`crate::node_registry::placement`] runs *after* `strategy.select` has
/// already produced the answer this function returns, over the same
/// candidate slice, and its result is dropped. Three consequences that are
/// correctness properties rather than style:
///
/// - The `prefer_node_id` hit above returns before the strategy is ever
///   asked, so it does not score either. Scoring it would file a
///   deliberate origin affinity as a shadow disagreement and poison the
///   very evidence the shadow exists to collect.
/// - The strategy is asked exactly once. A second `select` — including one
///   "just for the shadow" — would advance the shared round-robin cursor a
///   second time and change real placement.
/// - `now` is captured by the caller and used for both the freshness
///   verdicts and the classification, so one selection cannot straddle two
///   clocks.
pub fn select_node(
    deps: &ScheduleDeps<'_>,
    hint: Option<&ScheduleRequestHint>,
    prefer_node_id: &str,
    source: ShadowSource,
    now: SystemTime,
) -> Result<Placement, NoNodesAvailable> {
    let discovered = deps.node_registry.snapshot(/* allow_lingering */ false);
    let candidates = discovered.len();
    // One registry read per node, answering both questions. `snapshot` is
    // byte-for-byte what `peek_observed` returned before the scorer existed,
    // so the candidate list the filter and the strategy see is unchanged;
    // the freshness half rides alongside in `freshness_by_id` and is never
    // written back into a `RichNode` (deriving `UNHEALTHY` here and feeding
    // it to `filter_unschedulable` would change the real candidate set).
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
            // The invariant `freshness.is_none()` iff `snapshot.is_none()`
            // holds because both halves came out of the single
            // `peek_observed_with_freshness` call above. A node that
            // somehow is not in the map at all reads as "no snapshot",
            // which is the same conclusion its absent snapshot would give.
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
        // K-3: the paused-restore path is told what it is, never inferred
        // from its `hint = None`. It has no request size to read and that
        // is not a caller omission, so it does not count as missing.
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

/// The closed label set for `agentenv_api_lookup_node_total{result}`. See
/// the module doc for the four Go labels with no Rust counterpart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LookupResultLabel {
    BoundBinding,
    BoundRoster,
    BoundRegistry,
    Placed,
    Pinned,
    NotFound,
    InvalidArgument,
    UnavailableBindingStore,
    UnavailableRegistry,
    UnavailableColdBindings,
    UnavailableNoNodes,
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
            Self::NotFound => "not_found",
            Self::InvalidArgument => "invalid_argument",
            Self::UnavailableBindingStore => "unavailable_binding_store",
            Self::UnavailableRegistry => "unavailable_registry",
            Self::UnavailableColdBindings => "unavailable_cold_bindings",
            Self::UnavailableNoNodes => "unavailable_no_nodes",
            Self::OriginUnschedulable => "origin_unschedulable",
            Self::OriginNotReporting => "origin_not_reporting",
            Self::HolderUnreachable => "holder_unreachable",
        }
    }
}

/// A successful lookup answer -- ports `lookupDeps.answer`'s parameters.
#[derive(Debug, Clone)]
pub struct LookupAnswer {
    pub node: Node,
    pub location: SandboxLocation,
    pub origin_node_id: String,
    pub execution_id: String,
    pub execution_authority: ExecutionAuthority,
    pub label: LookupResultLabel,
}

/// Every way [`lookup_node`] may end. Mirrors the `codes.*` Go returns:
/// `Answer` -> `OK`, `NotFound` -> `codes.NotFound`, `Unavailable` ->
/// `codes.Unavailable`, `FailedPrecondition` -> `codes.FailedPrecondition`.
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

/// Everything [`lookup_node`] may consult.
pub struct LookupDeps<'a> {
    pub place: ScheduleDeps<'a>,
    pub binding_store: &'a dyn BindingStore,
    /// `None` here (never wired via `NodeRegistryGrpcService::with_paused_registry`)
    /// is treated exactly like Go's disabled reader: stage 3 is skipped and
    /// every lookup that gets this far answers from [`lookup_absent`]. So is
    /// `Some(registry)` whose `is_cluster_backed()` is `false` -- a registry
    /// that cannot speak for the whole cluster has nothing stage 3 may trust
    /// an absence from.
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

/// Ports `Service.rosterFresh` (`lookup.go:566-574`).
fn roster_fresh(last_seen: SystemTime, now: SystemTime) -> bool {
    match now.duration_since(last_seen) {
        Ok(age) => age <= ROSTER_FRESH_TTL,
        // `last_seen` is after `now` -- clock skew between two calls to
        // `SystemTime::now()` a few lines apart, never a real staleness
        // signal. Go's signed `time.Duration` subtraction takes the same
        // branch here (a negative duration is always `<= ttl`).
        Err(_) => true,
    }
}

/// Ports `Service.liveNode` (`lookup.go:507-516`).
fn live_node(node_registry: &dyn NodeRegistry, node_id: &str, now: SystemTime) -> Option<Node> {
    let node = node_registry.resolve(node_id.trim())?;
    let (_entries, last_seen) = node_registry.roster_of(&node.id)?;
    if !roster_fresh(last_seen, now) {
        return None;
    }
    Some(node)
}

/// Ports `nodeSchedulability` (`lookup.go:57-70`).
enum NodeSchedulability {
    Schedulable(Node),
    NotReporting,
    NotAcceptingWork,
}

/// Ports `Service.schedulableNode` (`lookup.go:529-540`).
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

/// Ports `Service.rosterPrefers` (`lookup.go:487-501`) verbatim, including
/// the plain-string comparison in the last arm (execution ids are UUIDv7
/// strings, so this is chronological by construction).
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

/// Ports `Service.rosterHolder` (`lookup.go:452-479`).
fn roster_holder(
    node_registry: &dyn NodeRegistry,
    sandbox_id: &str,
    now: SystemTime,
) -> Option<(Node, String)> {
    let mut best: Option<(Node, String, SystemTime)> = None;
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
        let execution = entries
            .iter()
            .find(|entry| entry.sandbox_id == sandbox_id)
            .map(|entry| entry.execution_id.clone())
            .unwrap_or_default();
        match &best {
            None => best = Some((node, execution, last_seen)),
            Some((_, best_execution, best_seen)) => {
                if roster_prefers(&execution, last_seen, best_execution, *best_seen) {
                    best = Some((node, execution, last_seen));
                }
            }
        }
    }
    best.map(|(node, execution, _)| (node, execution))
}

/// Ports `lookupAbsent` (`lookup.go:377-396`).
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

/// Ports `lookupNode` (`lookup.go:134-373`). `sandbox_id` must already be
/// non-empty -- the caller's `InvalidArgument` check runs before this, the
/// same layering `RecordAssignment`'s own validation already uses in
/// `src/node_registry/grpc_service.rs`.
pub async fn lookup_node(
    deps: &LookupDeps<'_>,
    sandbox_id: &str,
    now: SystemTime,
) -> LookupOutcome {
    // 1. The binding. This is the hot path -- every proxied request lands
    // here -- so nothing below it may run on a hit.
    match deps.binding_store.get(sandbox_id, now).await {
        Err(_err) => {
            return LookupOutcome::Unavailable(
                LookupResultLabel::UnavailableBindingStore,
                "binding store unavailable",
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

    // 2. The roster. A binding expires on its own TTL while the roster that
    // wrote it stays as the node last reported it.
    if let Some((holder, execution_id)) = roster_holder(deps.place.node_registry, sandbox_id, now) {
        return LookupOutcome::Answer(LookupAnswer {
            node: holder,
            location: SandboxLocation::Bound,
            origin_node_id: String::new(),
            execution_authority: authority_for(&execution_id),
            execution_id,
            label: LookupResultLabel::BoundRoster,
        });
    }

    // Evaluated only now, matching Go: a roster hit above costs nothing.
    let warm = deps.warmup.warmed_up(now);

    // 3. The registry -- see the module doc and `LookupDeps::paused_registry`
    // for why `None` and "not cluster-backed" both fall straight through to
    // `lookup_absent`, exactly like Go's disabled reader does.
    let entry = match deps.paused_registry {
        Some(registry) if registry.is_cluster_backed() => {
            // A sandbox id that is not a UUID at all cannot have a registry
            // row (the table's primary key is one) -- the same "nothing to
            // find" answer a valid-but-absent id gets, not a validation
            // error stages 1 and 2 above never imposed either.
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
        return lookup_absent(warm);
    };

    match entry.state {
        PausedRegistryState::Paused => {
            // The snapshot is published, so any node can rebuild it. Origin
            // is only a preference, applied through the exact same pipeline
            // `Schedule` runs.
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
                    // PENDING with an empty incarnation, hard-coded rather
                    // than let flow from the row: a `paused` row names none
                    // by construction, and the node is about to mint one.
                    execution_id: String::new(),
                    execution_authority: ExecutionAuthority::Pending,
                    label: LookupResultLabel::Placed,
                }),
            }
        }
        PausedRegistryState::Publishing | PausedRegistryState::LocalOnly => {
            // No snapshot in shared storage: the only copy is on origin's
            // disk, so this is that node or nothing.
            match schedulable_node(deps.place.node_registry, &entry.origin_node_id, now) {
                NodeSchedulability::NotReporting => LookupOutcome::FailedPrecondition(
                    LookupResultLabel::OriginNotReporting,
                    format!(
                        "sandbox is {} on node {}, which is not reporting",
                        entry.state.as_str(),
                        entry.origin_node_id
                    ),
                ),
                NodeSchedulability::NotAcceptingWork => LookupOutcome::FailedPrecondition(
                    LookupResultLabel::OriginUnschedulable,
                    format!(
                        "sandbox is {} on node {}, which is not accepting work",
                        entry.state.as_str(),
                        entry.origin_node_id
                    ),
                ),
                NodeSchedulability::Schedulable(node) => LookupOutcome::Answer(LookupAnswer {
                    node,
                    location: SandboxLocation::Pinned,
                    origin_node_id: entry.origin_node_id,
                    // PENDING here too, and this half has to be hard-coded
                    // rather than inferred: `local_only` names no
                    // incarnation, but `publishing` names one that belongs
                    // to a VM stopped before the upload began.
                    execution_id: String::new(),
                    execution_authority: ExecutionAuthority::Pending,
                    label: LookupResultLabel::Pinned,
                }),
            }
        }
        PausedRegistryState::Running | PausedRegistryState::Resuming => {
            // Holder() (== origin_node_id): the real machine, never
            // claimed_by_node_id -- see `PausedSandboxEntry::origin_node_id`'s
            // own doc comment.
            let holder_id = entry.origin_node_id.clone();
            match live_node(deps.place.node_registry, &holder_id, now) {
                None if !warm => LookupOutcome::Unavailable(
                    LookupResultLabel::UnavailableColdBindings,
                    "scheduler is still seeding sandbox assignments",
                ),
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

    /// An [`AtomicNodeRegistry`] with a tally on the two snapshot accessors.
    ///
    /// 🔴 It delegates rather than fakes: the point of the assertion below
    /// is the *number of reads a real placement performs*, and a hand-rolled
    /// fake would let the count be whatever the fake happened to make easy.
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

    /// 🔴 One registry read per candidate, for the whole selection —
    /// candidate construction *and* scoring.
    ///
    /// The obvious way to add a scorer is to let it ask the registry for
    /// each candidate's freshness while it scores. That doubles the number
    /// of `RwLock` acquisitions on a path that runs on every sandbox
    /// create, and worse, the second read can land on the other side of a
    /// heartbeat — so a candidate could be scored against a freshness
    /// verdict belonging to a snapshot it was not built from. Both are
    /// prevented by reading once, and this is what fails if that stops
    /// being true.
    ///
    /// The count is also *exactly* one per node, not "at most": a scorer
    /// that skipped the accessor entirely and hardcoded `Fresh` would pass
    /// an upper bound.
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

    /// The `prefer_node_id` hit returns before the strategy is asked, so it
    /// must not advance the round-robin cursor either — the cursor is the
    /// one piece of placement state a shadow-adjacent change could move
    /// invisibly.
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

        // Two preferred selections in a row, then a bare one. If either
        // preferred call had advanced the cursor, the bare call would
        // answer `node-a` only by coincidence — so the bare call is made
        // three times and the full rotation is asserted.
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
}
