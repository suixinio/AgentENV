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

use std::time::SystemTime;

use crate::binding_store::BindingStore;
use crate::node_registry::filter::{
    filter_by_resource_limit, filter_unschedulable, NodeResourceLimit,
};
use crate::node_registry::registry::NodeRegistry;
use crate::node_registry::strategy::{NoNodesAvailable, Strategy};
use crate::node_registry::types::{Node, RichNode};
use crate::node_registry::warmup::WarmupGate;
use crate::orchestrator::{PausedRegistryState, PausedSandboxRegistry};
use crate::proto::scheduler::{ExecutionAuthority, SandboxLocation, ScheduleRequestHint};
use crate::types::SandboxId;

/// How long a heartbeat-reported roster entry stays "fresh enough to route
/// to" -- mirrors Go's `Service.reportTTL`, which defaults to
/// `defaultObservedReportTTL` (`lookup.go`'s `rosterFresh`) and, in this
/// codebase, is the same 30s literal `src/bin/server.rs`'s
/// `start_native_node_registry` already passes as `AtomicNodeRegistry`'s own
/// `observed_ttl`.
pub use crate::node_registry::registry::DEFAULT_OBSERVED_REPORT_TTL as ROSTER_FRESH_TTL;

/// Everything [`select_node`] needs. Shared between `Schedule` (bare
/// preference) and [`lookup_node`]'s `Paused` branch (origin-node
/// preference) -- Go's `Service.place` is exactly this reuse.
#[derive(Clone, Copy)]
pub struct ScheduleDeps<'a> {
    pub node_registry: &'a dyn NodeRegistry,
    pub strategy: &'a dyn Strategy,
    pub resource_limit: Option<&'a NodeResourceLimit>,
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
pub fn select_node(
    deps: &ScheduleDeps<'_>,
    hint: Option<&ScheduleRequestHint>,
    prefer_node_id: &str,
) -> Result<Placement, NoNodesAvailable> {
    let discovered = deps.node_registry.snapshot(/* allow_lingering */ false);
    let candidates = discovered.len();
    let rich: Vec<RichNode> = discovered
        .into_iter()
        .map(|node| {
            let snapshot = deps.node_registry.peek_observed(&node.id);
            RichNode { node, snapshot }
        })
        .collect();
    let eligible_nodes = filter_by_resource_limit(filter_unschedulable(rich), deps.resource_limit);
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

fn paused_state_label(state: PausedRegistryState) -> &'static str {
    match state {
        PausedRegistryState::Publishing => "publishing",
        PausedRegistryState::Paused => "paused",
        PausedRegistryState::Resuming => "resuming",
        PausedRegistryState::LocalOnly => "local_only",
        PausedRegistryState::Running => "running",
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
            match select_node(&deps.place, None, &entry.origin_node_id) {
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
                        paused_state_label(entry.state),
                        entry.origin_node_id
                    ),
                ),
                NodeSchedulability::NotAcceptingWork => LookupOutcome::FailedPrecondition(
                    LookupResultLabel::OriginUnschedulable,
                    format!(
                        "sandbox is {} on node {}, which is not accepting work",
                        paused_state_label(entry.state),
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
                        paused_state_label(entry.state),
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
