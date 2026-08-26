//! api's heartbeat-receiving plane (task's own "D5"), and the Rust home for
//! the five RPC method bodies `services/scheduler/internal/service.go` hangs
//! off `scheduler.Service` that belong to the node registry rather than to
//! binding/routing state (task's own "D6"/"D3" — the ~130-line estimate in
//! `docs/proposals/_sd-phase4-stageA-node-inventory.md` §1.2): `ListNodes`,
//! `Heartbeat`, `ListObservedNodes`, `GetNode`, `UnregisterNode`.
//!
//! # Why this reuses `scheduler.v1.Scheduler` instead of a new proto
//!
//! `services/api/proto/scheduler.proto` is already the one schema both Go
//! and Rust generate from (CLAUDE.md, `src/node_registry/mod.rs`'s sibling
//! modules). Implementing the *same* generated
//! [`crate::proto::scheduler::scheduler_server::Scheduler`] trait here,
//! rather than inventing a parallel "node registry" service, means:
//!
//! - a node's dual heartbeat (`src/observability/reporter.rs`, task's own
//!   "D5" sender side) dials this service with the exact same
//!   `SchedulerClient` it already uses for the real scheduler — no second
//!   generated client, no second wire format to keep in step;
//! - the task's own equivalence-dump requirement
//!   (`src/api/impls/admin.rs`'s node-registry dump) can call `list_nodes`/
//!   `list_observed_nodes` here and compare the response types directly
//!   against what a `grpcurl` against the real scheduler would print — same
//!   message shapes, nothing to translate.
//!
//! # What is deliberately `unimplemented`
//!
//! `record_assignment`/`record_p2p_artifact`/`forget_p2p_artifact`/
//! `lookup_p2p_artifact` **are** implemented (task's own "D3"/"D4" — see
//! below), each gated the same way `report_sandbox_event` is.
//! `list_p2p_peers` was deliberately left `unimplemented` by Stage A
//! (`_sd-phase4-stageA-node-inventory.md` §7 risk 3 — explicitly not to be
//! "helpfully" ported alongside the rest of Stage A) with a comment that
//! called that "permanent." It was not: `AtomicNodeRegistry` already carries
//! the method (`list_p2p_peers`, `src/node_registry/registry.rs`) with its
//! own test coverage, and the only work Stage A skipped was wiring this one
//! RPC body to it — now done, the same shape as `list_nodes` just above.
//! `list_registry_sandboxes` was Stage C's own paused registry and stayed
//! `unimplemented` here through Stage C for the identical reason
//! `list_p2p_peers` did -- naming which Stage owns it rather than silently
//! accepting and doing nothing, so a caller that dials the wrong half of
//! this split by mistake gets an answer that says so. It now answers for
//! real too, against [`PausedSandboxRegistry::list_all`]
//! (`src/orchestrator/paused_registry/mod.rs`), the same shape
//! `list_p2p_peers` follows: filtering/paging live in this file (mirroring
//! Go's own `listRegistrySandboxes`, `service.go:849-943`), the actual read
//! in the trait method's own backend.
//!
//! `schedule`/`lookup_node` (Stage D's remainder) **are** now implemented
//! too, against [`crate::binding_store::lookup`]'s pure port of
//! `lookup.go`/`service.go`'s `selectNode`. `schedule` needs nothing beyond
//! what `new` already requires — Go's own `Schedule` never touches the
//! binding store either, only `s.nodes`/`s.strategy` — so it is never
//! gated behind a builder call the way the binding-store-backed RPCs are.
//! `lookup_node` does need a binding store (its stage 1 is unconditional in
//! Go too); with none wired it answers `Status::unimplemented`, the same
//! shape `record_assignment` uses. Its stage 3 (the paused registry) is
//! optional in a different way — see
//! [`NodeRegistryGrpcService::with_paused_registry`] and
//! `crate::binding_store::lookup`'s own module doc for why a missing or
//! non-cluster-backed registry degrades gracefully rather than refusing.
//!
//! `report_sandbox_event` (task's own "D1") **is** implemented here now: it
//! ports `applyProjectionDelete` (`service.go:602-643`) against
//! [`crate::binding_store::BindingStore`], wired in via
//! [`NodeRegistryGrpcService::with_binding_store`]. Every default path that
//! does not call that builder method (every test that only calls `new`, and
//! `src/bin/server.rs`'s real wiring until the config/assembly commit that
//! follows this one) keeps answering `Unimplemented`, unchanged.
//!
//! # `Heartbeat`/`UnregisterNode`'s `ReconcileNode` half (task's own
//! "D3"/"D6" — `_sd-phase4-stageA-node-inventory.md` §7 risk 1, §10)
//!
//! The real Go `Heartbeat` calls `s.store.ReconcileNode(node, roster, now)`
//! right after `s.nodes.Heartbeat(...)` succeeds — ported here too now,
//! gated the same way `report_sandbox_event` is (`None` binding store ->
//! today's pre-D3 behavior unchanged: warm-up just means "the registry
//! accepted a heartbeat"). Unlike `report_sandbox_event`, a `ReconcileNode`
//! failure here **does** fail the RPC (`Unavailable`), matching Go exactly
//! — a heartbeat whose roster never reached the binding store must not be
//! silently treated as caught up.
//!
//! `UnregisterNode` also now calls `ReconcileNode` with an empty roster
//! (deletes every binding the node owns), but treats its failure as
//! best-effort rather than fatal: by the point it runs, the node's
//! *identity* is already unregistered, and failing the whole call over a
//! routes-cleanup hiccup would leave a caller unsure whether to retry an
//! unregister that already took effect. `ArtifactStore::forget_node`
//! (Go's `s.artifacts.ForgetNode(nodeID)`) is now also called there,
//! wired in via `with_artifact_store` alongside `with_binding_store` —
//! infallible (the in-memory index has no failure mode), so there is no
//! best-effort/fatal distinction to make for it the way there is for the
//! binding-store cleanup next to it.
//!
//! The heartbeat-admission race this paragraph used to describe (a node
//! whose Pod is not yet `Serving` in discovery gets no binding refresh from
//! its heartbeat, `errors.Is(err, ErrNodeNotInRegistry)`) has been fixed —
//! see `AtomicNodeRegistry::admit_pending`'s own doc comment (task's own
//! "D2"). This file already benefits from that fix without any change of
//! its own: `self.registry.heartbeat(&req, now)` now succeeds for a pending
//! node the same way it does for an active one.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use tonic::{Request, Response, Status};

use crate::binding_store::artifact_index::ArtifactStore;
use crate::binding_store::lookup::{
    self as lookup_logic, LookupDeps, LookupOutcome, LookupResultLabel, ScheduleDeps,
};
use crate::binding_store::{BindingDecision, BindingDeleteOutcome, BindingStore};
use crate::orchestrator::{PausedRegistryListEntry, PausedRegistryState, PausedSandboxRegistry};
use crate::proto::scheduler::scheduler_server::Scheduler;
use crate::proto::scheduler::{
    self, ForgetP2pArtifactRequest, ForgetP2pArtifactResponse, GetNodeRequest, GetNodeResponse,
    HeartbeatRequest, HeartbeatResponse, ListNodesRequest, ListNodesResponse,
    ListObservedNodesRequest, ListObservedNodesResponse, ListP2pPeersRequest, ListP2pPeersResponse,
    ListRegistrySandboxesRequest, ListRegistrySandboxesResponse, LookupNodeRequest,
    LookupNodeResponse, LookupP2pArtifactRequest, LookupP2pArtifactResponse,
    RecordAssignmentRequest, RecordAssignmentResponse, RecordP2pArtifactRequest,
    RecordP2pArtifactResponse, ReportSandboxEventRequest, ReportSandboxEventResponse, SandboxEvent,
    SandboxEventType, ScheduleRequest, ScheduleResponse, UnregisterNodeRequest,
    UnregisterNodeResponse,
};

use super::filter::NodeResourceLimit;
use super::registry::{
    AtomicNodeRegistry, NodeNotInRegistry, NodeRegistry, ServiceInstanceMismatch,
};
use super::strategy::{RoundRobinStrategy, Strategy};
use super::warmup::WarmupGate;

const NOT_STAGE_A: &str = "not served by api's Stage A node-registry service — see \
     src/node_registry/grpc_service.rs's module doc for which half owns this RPC";

/// The `Scheduler` service `--role api` serves for the node-registry subset
/// of its RPCs. See the module doc for the split.
///
/// `Clone` is cheap and intentional: both fields are `Arc`s, so a second
/// handle (`src/bin/server.rs`'s observed-nodes metrics loop needs one
/// alongside the copy `SchedulerServer::new` takes ownership of) is two
/// atomic increments, not a second registry.
#[derive(Clone)]
pub struct NodeRegistryGrpcService {
    registry: Arc<AtomicNodeRegistry>,
    warmup: Arc<WarmupGate>,
    /// Task's own "D1"/"D3": `None` until `with_binding_store` wires one in
    /// -- every default path that does not call it (every pre-existing
    /// test, and `src/bin/server.rs`'s real wiring until the config/
    /// assembly commit that follows this one) keeps `report_sandbox_event`
    /// answering `Unimplemented`, unchanged.
    binding_store: Option<Arc<dyn BindingStore>>,
    /// Mirrors Go's `Service.projectionAuthoritative`
    /// (`WithAuthoritativeProjection`). Must agree with whatever value the
    /// binding store passed to `with_binding_store` was itself constructed
    /// with -- the two are not the same runtime pointer, matching Go: it is
    /// a construction-time value the process wiring passes to both, not
    /// re-derived from one at call time.
    projection_authoritative: bool,
    /// Mirrors Go's `Service.maxProjectionTTL`, consumed by
    /// `resolve_projection_ttl` (`RecordAssignment`).
    max_projection_ttl: Duration,
    /// Task's own "D4": `None` until `with_artifact_store` wires one in --
    /// gates `record_p2p_artifact`/`forget_p2p_artifact`/
    /// `lookup_p2p_artifact` the same way `binding_store` gates
    /// `report_sandbox_event` et al.
    artifact_store: Option<Arc<dyn ArtifactStore>>,
    /// `lookup_node`'s stage 3. `None` until `with_paused_registry` wires
    /// one in -- see `crate::binding_store::lookup`'s module doc for why a
    /// missing (or non-cluster-backed) registry degrades gracefully rather
    /// than refusing the RPC the way a missing `binding_store` does.
    paused_registry: Option<Arc<dyn PausedSandboxRegistry>>,
    /// `Schedule`'s placement strategy, and `lookup_node`'s `Paused`
    /// branch's (`select_node`'s `hint = None` call). Defaults to
    /// round-robin, matching Go's own `NewStrategy` fallback -- neither is
    /// wired to `AppConfig` yet, mirroring `NodeResourceLimit`'s own
    /// "deliberately deferred" stance (`src/node_registry/strategy.rs`,
    /// `src/node_registry/filter.rs`).
    strategy: Arc<dyn Strategy>,
    /// `Schedule`'s resource ceiling. `None` (no limit) by default -- see
    /// `strategy`'s own doc for why this is not yet configurable here.
    resource_limit: Option<NodeResourceLimit>,
}

impl NodeRegistryGrpcService {
    pub fn new(registry: Arc<AtomicNodeRegistry>, warmup: Arc<WarmupGate>) -> Self {
        Self {
            registry,
            warmup,
            binding_store: None,
            projection_authoritative: false,
            max_projection_ttl: Duration::ZERO,
            artifact_store: None,
            paused_registry: None,
            strategy: Arc::new(RoundRobinStrategy::new()),
            resource_limit: None,
        }
    }

    /// `lookup_node`'s stage 3. Independent of `with_binding_store` -- a
    /// deployment could in principle wire one without the other, though
    /// `src/bin/server.rs` wires both (`build_paused_registry` always runs
    /// once native mode is on, even when its backend is the node-local
    /// `Local`/disabled one).
    #[must_use]
    pub fn with_paused_registry(mut self, paused_registry: Arc<dyn PausedSandboxRegistry>) -> Self {
        self.paused_registry = Some(paused_registry);
        self
    }

    /// Task's own "D4": wires the P2P artifact index. Independent of
    /// `with_binding_store` -- a deployment could in principle wire one
    /// without the other, though `src/bin/server.rs` wires both together.
    #[must_use]
    pub fn with_artifact_store(mut self, artifact_store: Arc<dyn ArtifactStore>) -> Self {
        self.artifact_store = Some(artifact_store);
        self
    }

    /// Task's own "D1"/"D3": wires the binding store this Stage builds.
    /// Ports the construction-time half of Go's `WithAuthoritativeProjection`
    /// `ServiceOption` (`service.go:166-173`) — see the struct field docs
    /// above for why `projection_authoritative` is a separate argument
    /// rather than read off the store.
    #[must_use]
    pub fn with_binding_store(
        mut self,
        binding_store: Arc<dyn BindingStore>,
        projection_authoritative: bool,
        max_projection_ttl: Duration,
    ) -> Self {
        self.binding_store = Some(binding_store);
        self.projection_authoritative = projection_authoritative;
        self.max_projection_ttl = max_projection_ttl;
        self
    }

    /// Ports `resolveProjectionTTL` (`service.go:656-673`): `<= 0` raw or
    /// the switch off both mean "use the receiver's own `binding_ttl`"
    /// (`Duration::ZERO`, the same sentinel `BindingStore::record`'s
    /// `projection_ttl` already treats that way); a positive raw value is
    /// clamped to `max_projection_ttl` when configured (`> Duration::ZERO`)
    /// and exceeded, otherwise passed through unchanged.
    fn resolve_projection_ttl(&self, raw: Duration) -> (Duration, &'static str) {
        if !self.projection_authoritative || raw.is_zero() {
            return (Duration::ZERO, "default");
        }
        if !self.max_projection_ttl.is_zero() && raw > self.max_projection_ttl {
            return (self.max_projection_ttl, "clamped");
        }
        (raw, "node")
    }

    /// Shared validation for the three P2P artifact RPCs: `cluster_id`/
    /// `backend`/`key` are always required; `node_id` is required and
    /// resolved through discovery only when the caller passes `Some`
    /// (`LookupP2pArtifact` has no node id of its own to resolve).
    fn validate_p2p_artifact_fields<'a>(
        &self,
        cluster_id: &'a str,
        backend: &'a str,
        key: &'a str,
        node_id: Option<&str>,
    ) -> Result<(&'a str, &'a str, &'a str, String), Status> {
        let cluster_id = cluster_id.trim();
        let backend = backend.trim();
        let key = key.trim();
        if cluster_id.is_empty() || backend.is_empty() || key.is_empty() {
            return Err(Status::invalid_argument(
                "cluster_id, backend, and key are required",
            ));
        }
        let resolved_node_id = match node_id {
            Some(raw) => {
                let raw = raw.trim();
                if raw.is_empty() {
                    return Err(Status::invalid_argument("node_id is required"));
                }
                self.registry
                    .resolve(raw)
                    .map(|n| n.id)
                    .unwrap_or_else(|| raw.to_string())
            }
            None => String::new(),
        };
        Ok((cluster_id, backend, key, resolved_node_id))
    }

    /// Ports `canonicalNodeID` (`reconcile.go:636-646`): resolves a node
    /// identity the same way every other lookup in this service does, so a
    /// row (or a caller's `node_id` filter) written under a node's previous
    /// name is still attributed to that node. `""` in, `""` out -- an empty
    /// filter/holder must never resolve to some node's real identity.
    ///
    /// Used by [`Self::list_registry_sandboxes`] to canonicalise both sides
    /// of its `node_id` filter comparison before comparing them, exactly
    /// like [`Self::validate_p2p_artifact_fields`]'s identical resolve does
    /// for a single id above.
    fn canonical_node_id(&self, node_id: &str) -> String {
        let trimmed = node_id.trim();
        if trimmed.is_empty() {
            return String::new();
        }
        self.registry
            .resolve(trimmed)
            .map(|n| n.id)
            .unwrap_or_else(|| trimmed.to_string())
    }

    fn record_projection_ttl_source(source: &str) {
        metrics::counter!(PROJECTION_TTL_SOURCE_METRIC, "source" => source.to_string())
            .increment(1);
    }

    /// Ports `sandboxEventTypeLabel` (`metrics.go`, used from
    /// `applyProjectionDelete`/`ReportSandboxEvent`).
    fn sandbox_event_type_label(event_type: SandboxEventType) -> &'static str {
        match event_type {
            SandboxEventType::Create => "create",
            SandboxEventType::Delete => "delete",
            SandboxEventType::Pause => "pause",
            SandboxEventType::Resume => "resume",
            SandboxEventType::Fork => "fork",
            SandboxEventType::Unspecified => "other",
        }
    }

    fn record_sandbox_event(event_type: &str, outcome: &str) {
        metrics::counter!(
            SANDBOX_EVENT_METRIC,
            "event_type" => event_type.to_string(),
            "outcome" => outcome.to_string()
        )
        .increment(1);
    }

    /// Ports `applyProjectionDelete` (`service.go:602-643`) exactly,
    /// including the metric outcome labels it emits at every early return.
    /// Returns whether the delete actually removed something (`Deleted` or
    /// `DeletedUnknownIncumbent`) — Go's own return value, currently unused
    /// by its only caller (`ReportSandboxEvent` never inspects it either;
    /// events are best-effort and the RPC never fails on their account) but
    /// kept for parity and for tests to assert against directly.
    async fn apply_projection_delete(
        &self,
        binding_store: &Arc<dyn BindingStore>,
        event: &SandboxEvent,
        now: SystemTime,
    ) -> bool {
        let label = Self::sandbox_event_type_label(event.event_type());
        if !self.projection_authoritative {
            Self::record_sandbox_event(label, "ignored_switch_off");
            return false;
        }
        let sandbox_id = event.sandbox_id.trim();
        if sandbox_id.is_empty() {
            Self::record_sandbox_event(label, "ignored_no_sandbox");
            return false;
        }
        // 🔴 `normalize_execution_id_reason`, not `normalize_execution_id`:
        // this is the event path, not the roster path, and must not double
        // count a dropped value into `node_registry_roster_entry_dropped_total`
        // — see `binding_store::record`'s own doc comment on this function.
        let (execution, _reason) =
            crate::binding_store::record::normalize_execution_id_reason(&event.execution_id);
        if execution.is_empty() {
            Self::record_sandbox_event(label, "ignored_unknown_execution");
            return false;
        }
        match binding_store.delete(sandbox_id, &execution, now).await {
            Err(_) => {
                Self::record_sandbox_event(label, "store_error");
                false
            }
            Ok(outcome) => {
                Self::record_sandbox_event(label, outcome.as_str());
                matches!(
                    outcome,
                    BindingDeleteOutcome::Deleted | BindingDeleteOutcome::DeletedUnknownIncumbent
                )
            }
        }
    }

    pub fn describe_metrics() {
        metrics::describe_gauge!(
            OBSERVED_NODES_METRIC,
            "Observed node count by derived status, as api's node registry currently has it \
             (Stage A's counterpart to the scheduler's agentenv_scheduler_observed_nodes)."
        );
        metrics::describe_counter!(
            LOOKUP_NODE_METRIC,
            "LookupNode outcomes by result label (Stage D's counterpart to the scheduler's \
             agentenv_scheduler_lookup_node_total)."
        );
        metrics::describe_counter!(
            LOOKUP_EXECUTION_AUTHORITY_METRIC,
            "Execution authority carried on every successful LookupNode answer (Stage D's \
             counterpart to the scheduler's agentenv_scheduler_lookup_execution_authority_total)."
        );
        metrics::describe_counter!(
            BINDING_EXECUTION_METRIC,
            "Binding-store arbitration outcomes by decision and write source (Stage D's \
             counterpart to the scheduler's agentenv_scheduler_binding_execution_total)."
        );
        metrics::describe_histogram!(
            SCHEDULE_DURATION_METRIC,
            "Schedule call latency by strategy and outcome (Stage D's counterpart to the \
             scheduler's agentenv_scheduler_schedule_duration_seconds)."
        );
        metrics::describe_counter!(
            SCHEDULE_ASSIGNMENTS_METRIC,
            "Successful Schedule placements by strategy (Stage D's counterpart to the \
             scheduler's agentenv_scheduler_schedule_assignments_total)."
        );
    }

    fn record_lookup_node(label: LookupResultLabel) {
        metrics::counter!(LOOKUP_NODE_METRIC, "result" => label.as_str()).increment(1);
    }

    /// Ports `executionAuthorityLabel` (`metrics.go`) and
    /// `recordLookupExecutionAuthority`'s call site (`lookupDeps.answer`,
    /// `lookup.go:403-427`) -- called once per successful `LookupNode`
    /// answer, never on an error return.
    fn record_lookup_execution_authority(authority: scheduler::ExecutionAuthority) {
        let label = match authority {
            scheduler::ExecutionAuthority::Registry => "registry",
            scheduler::ExecutionAuthority::Pending => "pending",
            scheduler::ExecutionAuthority::Unknown | scheduler::ExecutionAuthority::Unspecified => {
                "unknown"
            }
        };
        metrics::counter!(LOOKUP_EXECUTION_AUTHORITY_METRIC, "authority" => label).increment(1);
    }

    /// Ports `recordBindingArbitration` (`metrics.go:409-414`): a no-op for
    /// `BindingDecision::NotArbitrated` (arbitration off, Go's `""`), which
    /// carries nothing worth counting.
    fn record_binding_execution(source: &'static str, decision: BindingDecision) {
        let label = decision.as_str();
        if label.is_empty() {
            return;
        }
        metrics::counter!(BINDING_EXECUTION_METRIC, "decision" => label, "source" => source)
            .increment(1);
    }

    /// Ports Go's `refreshObservedNodesMetrics`/`recordObservedNodes`
    /// (`service.go:700-720`, `metrics.go`'s `schedulerObservedNodes`).
    /// Called once at startup and then on the interval `RunObservedNodesMetrics`
    /// wraps it in, by the caller in `src/bin/server.rs`.
    pub fn refresh_observed_nodes_metric(&self) {
        let mut counts: std::collections::HashMap<&'static str, u32> = [
            ("ready", 0),
            ("connecting", 0),
            ("unhealthy", 0),
            ("lingering", 0),
            ("draining", 0),
            ("unspecified", 0),
        ]
        .into_iter()
        .collect();
        for node in self.registry.list_observed("", SystemTime::now()) {
            let label = node_status_label(
                node.snapshot
                    .as_ref()
                    .map(|s| s.status())
                    .unwrap_or(scheduler::NodeStatus::Unspecified),
            );
            *counts.entry(label).or_insert(0) += 1;
        }
        for (label, count) in counts {
            metrics::gauge!(OBSERVED_NODES_METRIC, "status" => label).set(f64::from(count));
        }
    }
}

const OBSERVED_NODES_METRIC: &str = "agentenv_api_node_registry_observed_nodes";
/// Task's own "D1". Ports `agentenv_scheduler_sandbox_event_total`, renamed
/// per this codebase's own convention for a ported scheduler metric (drop
/// `scheduler`, use the Rust subsystem's own name — see
/// `agentenv_scheduler_observed_nodes` -> `OBSERVED_NODES_METRIC` above for
/// the precedent).
const SANDBOX_EVENT_METRIC: &str = "agentenv_api_sandbox_event_total";
/// Ports `agentenv_scheduler_heartbeat_legacy_roster_total`, renamed per
/// this module's own `scheduler` -> `api` convention.
const HEARTBEAT_LEGACY_ROSTER_METRIC: &str = "agentenv_api_heartbeat_legacy_roster_total";
/// Ports `agentenv_scheduler_projection_ttl_source_total`.
const PROJECTION_TTL_SOURCE_METRIC: &str = "agentenv_api_projection_ttl_source_total";
/// Ports `agentenv_scheduler_lookup_node_total`.
const LOOKUP_NODE_METRIC: &str = "agentenv_api_lookup_node_total";
/// Ports `agentenv_scheduler_lookup_execution_authority_total`.
const LOOKUP_EXECUTION_AUTHORITY_METRIC: &str = "agentenv_api_lookup_execution_authority_total";
/// Ports `agentenv_scheduler_binding_execution_total`. Named ahead of this
/// port in `src/binding_store/arbitration.rs`'s own doc comment on
/// `BindingDecision`.
const BINDING_EXECUTION_METRIC: &str = "agentenv_api_binding_execution_total";
/// Ports `agentenv_scheduler_schedule_duration_seconds`.
const SCHEDULE_DURATION_METRIC: &str = "agentenv_api_schedule_duration_seconds";
/// Ports `agentenv_scheduler_schedule_assignments_total`.
const SCHEDULE_ASSIGNMENTS_METRIC: &str = "agentenv_api_schedule_assignments_total";

/// The one conversion from the wire's whole seconds. A zero value becomes
/// `Duration::ZERO`, which every reader of this treats as "no budget
/// offered" and never as "no expiry" -- mirrors `registry.rs`'s own private
/// copy of this same conversion (`projection_ttl_from_secs`), duplicated
/// here rather than exposed across the module boundary for one call site.
fn projection_ttl_from_secs(secs: u32) -> Duration {
    if secs == 0 {
        Duration::ZERO
    } else {
        Duration::from_secs(u64::from(secs))
    }
}

fn node_status_label(status: scheduler::NodeStatus) -> &'static str {
    match status {
        scheduler::NodeStatus::Ready => "ready",
        scheduler::NodeStatus::Connecting => "connecting",
        scheduler::NodeStatus::Unhealthy => "unhealthy",
        scheduler::NodeStatus::Lingering => "lingering",
        scheduler::NodeStatus::Draining => "draining",
        scheduler::NodeStatus::Unspecified => "unspecified",
    }
}

#[tonic::async_trait]
impl Scheduler for NodeRegistryGrpcService {
    /// Ports `service.go:440-450` — `s.nodes.Snapshot(true)`, direct proto
    /// translation, no side effects.
    async fn list_nodes(
        &self,
        _request: Request<ListNodesRequest>,
    ) -> Result<Response<ListNodesResponse>, Status> {
        let nodes = self
            .registry
            .snapshot(/* allow_lingering */ true)
            .into_iter()
            .map(|node| scheduler::Node {
                node_id: node.id,
                endpoint: node.endpoint,
            })
            .collect();
        Ok(Response::new(ListNodesResponse { nodes }))
    }

    /// Ports `service.go:514-576` minus the `ReconcileNode`/warm-up-store
    /// half — see the module doc's "🔴" section.
    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let req = request.into_inner();
        let node_id = req.node_id.trim();
        let service_instance_id = req.service_instance_id.trim();
        if node_id.is_empty() || service_instance_id.is_empty() {
            return Err(Status::invalid_argument(
                "node_id and service_instance_id are required",
            ));
        }

        let now = SystemTime::now();
        let (node, cpu_config_json) = match self.registry.heartbeat(&req, now) {
            Ok(result) => result,
            Err(NodeNotInRegistry) => {
                // 🔴 Matches the real scheduler's exact message
                // (`node_registry.go`'s `ErrNodeNotInRegistry`,
                // `service.go:531`) byte-for-byte — a node's dual heartbeat
                // reporter (`src/observability/reporter.rs`) already matches
                // on this string for the primary scheduler target, and it
                // has to keep working the same way against this target.
                return Err(Status::invalid_argument(
                    "node is not in scheduler node list",
                ));
            }
        };

        // Task's own "D3": ports the `ReconcileNode` half of `Heartbeat`
        // (`service.go:538-553`) that `grpc_service.rs`'s module doc used to
        // list as still missing. `None` (no binding store wired yet, every
        // default path today) falls back to the pre-D3 behavior: warm-up
        // means "the registry accepted a heartbeat," nothing more.
        if let Some(binding_store) = &self.binding_store {
            let (roster, legacy) = super::registry::roster_from_heartbeat(&req);
            if legacy {
                // 🔴 Counted per node, not merely logged — mirrors Go's own
                // comment on `recordLegacyRoster`: this is the number that
                // has to reach zero before any consumer can refuse an
                // incarnation-less roster, and a log line does not answer
                // "how many nodes are still on the old build."
                metrics::counter!(HEARTBEAT_LEGACY_ROSTER_METRIC, "node" => node.id.clone())
                    .increment(1);
            }
            match binding_store.reconcile_node(node, roster, now).await {
                Err(err) => {
                    tracing::warn!(
                        node_id = %node_id,
                        error = %err,
                        "scheduler heartbeat binding reconcile failed"
                    );
                    return Err(Status::unavailable("binding store unavailable"));
                }
                Ok(decisions) => {
                    for (_sandbox_id, decision) in decisions {
                        Self::record_binding_execution("heartbeat", decision);
                    }
                }
            }
            // Only now, with the roster actually applied to the store:
            // warm-up is about the bindings being seeded, not about the
            // node having said hello (matches Go's own comment on this
            // exact ordering).
            self.warmup.reported_in(now);
        } else {
            self.warmup.reported_in(now);
        }

        Ok(Response::new(HeartbeatResponse { cpu_config_json }))
    }

    /// Ports `service.go:722-727` — `s.nodes.ListObserved(cluster_id, now)`.
    async fn list_observed_nodes(
        &self,
        request: Request<ListObservedNodesRequest>,
    ) -> Result<Response<ListObservedNodesResponse>, Status> {
        let req = request.into_inner();
        let nodes = self
            .registry
            .list_observed(&req.cluster_id, SystemTime::now());
        Ok(Response::new(ListObservedNodesResponse { nodes }))
    }

    /// Ports `service.go:800-812` — `s.nodes.GetObserved(...)`, `NotFound`
    /// when absent.
    async fn get_node(
        &self,
        request: Request<GetNodeRequest>,
    ) -> Result<Response<GetNodeResponse>, Status> {
        let req = request.into_inner();
        let node_id = req.node_id.trim();
        if node_id.is_empty() {
            return Err(Status::invalid_argument("node_id is required"));
        }
        let node = self
            .registry
            .get_observed(node_id, &req.cluster_id, SystemTime::now())
            .ok_or_else(|| Status::not_found("observed node not found"))?;
        Ok(Response::new(GetNodeResponse { node: Some(node) }))
    }

    /// Ports `service.go:814-847` minus the `ReconcileNode`/`ForgetNode`
    /// half — see the module doc's "🔴" section. The alias resolution step
    /// (`s.nodes.Resolve(nodeID)` before `UnregisterObserved`) is kept:
    /// bindings/observations are held under a node's *current* identity, so
    /// an unregister sent under a previous one (a pod name from before a
    /// fleet upgrade renamed it) has to be resolved first or it clears
    /// nothing — `node_registry.go`'s own comment on the same lines.
    async fn unregister_node(
        &self,
        request: Request<UnregisterNodeRequest>,
    ) -> Result<Response<UnregisterNodeResponse>, Status> {
        let req = request.into_inner();
        let node_id = req.node_id.trim();
        let service_instance_id = req.service_instance_id.trim();
        if node_id.is_empty() || service_instance_id.is_empty() {
            return Err(Status::invalid_argument(
                "node_id and service_instance_id are required",
            ));
        }
        let canonical_id = self
            .registry
            .resolve(node_id)
            .map(|node| node.id)
            .unwrap_or_else(|| node_id.to_string());

        match self
            .registry
            .unregister_observed(&canonical_id, service_instance_id)
        {
            Ok(()) => {
                // Task's own "D3": ports the `ReconcileNode` half of
                // `UnregisterNode` (`service.go:814-847`) — an empty roster
                // deletes every binding this node owns, the same as a
                // heartbeat reporting nothing held. Best-effort, not fatal
                // to the RPC: the node's identity is already unregistered
                // by this point, and failing the whole call because
                // cleanup of its *routes* hiccuped would leave a caller
                // unsure whether to retry an unregister that already took
                // effect. A stale binding this leaves behind still expires
                // on its own TTL.
                //
                if let Some(binding_store) = &self.binding_store {
                    let node = crate::node_registry::types::Node {
                        id: canonical_id.clone(),
                        ..Default::default()
                    };
                    if let Err(err) = binding_store
                        .reconcile_node(node, Vec::new(), SystemTime::now())
                        .await
                    {
                        tracing::warn!(
                            node_id = %canonical_id,
                            error = %err,
                            "scheduler unregister_node binding cleanup failed; any bindings it \
                             still owns will expire on their own TTL"
                        );
                    }
                }
                // Ports Go's `s.artifacts.ForgetNode(nodeID)` — drops every
                // P2P artifact association this node held. Infallible (the
                // in-memory index has no failure mode to report), so
                // there is no best-effort/fatal distinction to make here.
                if let Some(artifact_store) = &self.artifact_store {
                    artifact_store.forget_node(&canonical_id);
                }
                Ok(Response::new(UnregisterNodeResponse {}))
            }
            Err(ServiceInstanceMismatch) => {
                Err(Status::failed_precondition("service instance mismatch"))
            }
        }
    }

    /// Ports `Service.Schedule` (`service.go:326-343`): picks a node for a
    /// sandbox that does not exist yet, so there is no node it would rather
    /// be on (`prefer_node_id = ""`) -- see `crate::binding_store::lookup::select_node`
    /// for the shared pipeline this and `lookup_node`'s `Paused` branch both
    /// run. Needs nothing `new` does not already provide: no binding store,
    /// no paused registry -- Go's own `Schedule` never touches either.
    async fn schedule(
        &self,
        request: Request<ScheduleRequest>,
    ) -> Result<Response<ScheduleResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let deps = ScheduleDeps {
            node_registry: self.registry.as_ref(),
            strategy: self.strategy.as_ref(),
            resource_limit: self.resource_limit.as_ref(),
        };
        let result = lookup_logic::select_node(&deps, req.hint.as_ref(), "");
        let strategy_name = self.strategy.name();
        let status_label = crate::observability::prometheus::result_status(result.is_ok());
        metrics::histogram!(
            SCHEDULE_DURATION_METRIC,
            "strategy" => strategy_name,
            "status" => status_label,
        )
        .record(start.elapsed().as_secs_f64());
        match result {
            Ok(placement) => {
                metrics::counter!(SCHEDULE_ASSIGNMENTS_METRIC, "strategy" => strategy_name)
                    .increment(1);
                Ok(Response::new(ScheduleResponse {
                    node: Some(scheduler::Node {
                        node_id: placement.node.id,
                        endpoint: placement.node.endpoint,
                    }),
                }))
            }
            Err(_no_nodes) => Err(Status::unavailable("no nodes available")),
        }
    }

    /// Ports `lookupNode` (`lookup.go:134-373`) through
    /// `crate::binding_store::lookup::lookup_node` -- this method is only
    /// the parameter translation and the metric/status-code mapping, so
    /// every branch's own reasoning lives in that module instead of here.
    async fn lookup_node(
        &self,
        request: Request<LookupNodeRequest>,
    ) -> Result<Response<LookupNodeResponse>, Status> {
        let Some(binding_store) = self.binding_store.clone() else {
            return Err(Status::unimplemented(format!(
                "LookupNode needs a binding store, and this deployment has not wired one in \
                 yet: {NOT_STAGE_A}"
            )));
        };
        let req = request.into_inner();
        let sandbox_id = req.sandbox_id.trim();
        if sandbox_id.is_empty() {
            Self::record_lookup_node(LookupResultLabel::InvalidArgument);
            return Err(Status::invalid_argument("sandbox_id is required"));
        }

        let deps = LookupDeps {
            place: ScheduleDeps {
                node_registry: self.registry.as_ref(),
                strategy: self.strategy.as_ref(),
                resource_limit: self.resource_limit.as_ref(),
            },
            binding_store: binding_store.as_ref(),
            paused_registry: self.paused_registry.as_deref(),
            warmup: self.warmup.as_ref(),
        };
        let outcome = lookup_logic::lookup_node(&deps, sandbox_id, SystemTime::now()).await;
        Self::record_lookup_node(outcome.label());
        match outcome {
            LookupOutcome::Answer(answer) => {
                Self::record_lookup_execution_authority(answer.execution_authority);
                Ok(Response::new(LookupNodeResponse {
                    node: Some(scheduler::Node {
                        node_id: answer.node.id,
                        endpoint: answer.node.endpoint,
                    }),
                    location: answer.location as i32,
                    origin_node_id: answer.origin_node_id,
                    execution_id: answer.execution_id,
                    execution_authority: answer.execution_authority as i32,
                }))
            }
            LookupOutcome::NotFound => Err(Status::not_found("sandbox assignment not found")),
            LookupOutcome::Unavailable(_label, message) => Err(Status::unavailable(message)),
            LookupOutcome::FailedPrecondition(_label, message) => {
                Err(Status::failed_precondition(message))
            }
        }
    }

    /// Ports `RecordAssignment` (`service.go:463-512`, task's own "D3"):
    /// validates the required fields, resolves the caller's node identity
    /// through discovery (`AtomicNodeRegistry::resolve`, so an assignment
    /// naming a pod's previous identity after a fleet-upgrade rename still
    /// lands on the right binding), normalizes the execution id without the
    /// roster-drop metric (same reasoning as `apply_projection_delete`:
    /// this is not the roster path), resolves the projection TTL through
    /// the same `projection_authoritative`/`max_projection_ttl` gate
    /// `resolve_projection_ttl` implements, and records the binding through
    /// the same arbitration a heartbeat's `ReconcileNode` uses.
    async fn record_assignment(
        &self,
        request: Request<RecordAssignmentRequest>,
    ) -> Result<Response<RecordAssignmentResponse>, Status> {
        let Some(binding_store) = self.binding_store.clone() else {
            return Err(Status::unimplemented(format!(
                "RecordAssignment needs a binding store, and this deployment has not wired one \
                 in yet: {NOT_STAGE_A}"
            )));
        };
        let req = request.into_inner();
        let sandbox_id = req.sandbox_id.trim();
        if sandbox_id.is_empty() {
            return Err(Status::invalid_argument("sandbox_id is required"));
        }
        let Some(wire_node) = req.node else {
            return Err(Status::invalid_argument("node is required"));
        };
        let requested_id = wire_node.node_id.trim();
        if requested_id.is_empty() || wire_node.endpoint.trim().is_empty() {
            return Err(Status::invalid_argument(
                "node.node_id and node.endpoint are required",
            ));
        }
        let Some(node) = self.registry.resolve(requested_id) else {
            return Err(Status::invalid_argument(
                "node is not in scheduler node list",
            ));
        };

        let (execution, _reason) =
            crate::binding_store::record::normalize_execution_id_reason(&req.execution_id);
        let (projection_ttl, ttl_source) =
            self.resolve_projection_ttl(projection_ttl_from_secs(req.projection_ttl_secs));
        Self::record_projection_ttl_source(ttl_source);

        let now = SystemTime::now();
        match binding_store
            .record(
                sandbox_id,
                crate::binding_store::Binding {
                    node,
                    execution_id: execution,
                    projection_ttl,
                },
                now,
            )
            .await
        {
            Err(err) => {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    error = %err,
                    "scheduler record_assignment binding write failed"
                );
                Err(Status::unavailable("binding store unavailable"))
            }
            Ok(decision) => {
                Self::record_binding_execution("assignment", decision);
                Ok(Response::new(RecordAssignmentResponse {}))
            }
        }
    }

    /// Ports `service.go:576-598` (`ReportSandboxEvent`): PAUSE/DELETE
    /// events go through the guarded `apply_projection_delete`, every other
    /// event type is only observed (a metric, not a state change). Never
    /// fails on an individual event's account — events are best-effort
    /// (`src/observability/reporter.rs`'s sender drops on failure without
    /// retrying), so an unreachable binding store degrades this to "PAUSE/
    /// DELETE stop refreshing bindings until the next heartbeat
    /// reconciliation," not an RPC error.
    async fn report_sandbox_event(
        &self,
        request: Request<ReportSandboxEventRequest>,
    ) -> Result<Response<ReportSandboxEventResponse>, Status> {
        let Some(binding_store) = self.binding_store.clone() else {
            return Err(Status::unimplemented(format!(
                "ReportSandboxEvent needs a binding store, and this deployment has not wired \
                 one in yet: {NOT_STAGE_A}"
            )));
        };
        let req = request.into_inner();
        let now = SystemTime::now();
        for event in &req.events {
            match event.event_type() {
                SandboxEventType::Pause | SandboxEventType::Delete => {
                    self.apply_projection_delete(&binding_store, event, now)
                        .await;
                }
                other => {
                    Self::record_sandbox_event(
                        Self::sandbox_event_type_label(other),
                        "observed_only",
                    );
                }
            }
        }
        Ok(Response::new(ReportSandboxEventResponse {}))
    }

    // ── `list_p2p_peers` right below and `list_registry_sandboxes` further
    // down both now answer for real (see the module doc's correction
    // above). Everything else from here on (the P2P artifact RPCs) is Stage
    // D's own, gated behind whichever builder wires its dependency, the
    // same as every RPC above this line.

    /// Ports `service.go:728-734` — `s.nodes.ListP2pPeers(...)`, direct
    /// proto translation, no side effects. See
    /// [`NodeRegistry::list_p2p_peers`] for the filtering (ready peers,
    /// matching backend, TTL-derived liveness) it delegates to; this method
    /// exists only to unwrap the request and re-wrap the response, the same
    /// shape as [`Self::list_nodes`] just above.
    async fn list_p2p_peers(
        &self,
        request: Request<ListP2pPeersRequest>,
    ) -> Result<Response<ListP2pPeersResponse>, Status> {
        let req = request.into_inner();
        let peers = self.registry.list_p2p_peers(
            &req.cluster_id,
            &req.backend,
            &req.exclude_node_id,
            SystemTime::now(),
        );
        Ok(Response::new(ListP2pPeersResponse { peers }))
    }

    /// Ports `RecordP2pArtifact` (`service.go:739-760`, task's own "D4"):
    /// validates the four required fields, resolves the caller's node
    /// identity through discovery when possible (falling back to the
    /// supplied id verbatim otherwise -- this index is a soft accelerator,
    /// see `artifact_index`'s own module doc, and refusing a record over an
    /// unresolvable identity would cost more hit rate than a
    /// slightly-stale key ever would), and records the association.
    async fn record_p2p_artifact(
        &self,
        request: Request<RecordP2pArtifactRequest>,
    ) -> Result<Response<RecordP2pArtifactResponse>, Status> {
        let Some(artifact_store) = self.artifact_store.clone() else {
            return Err(Status::unimplemented(format!(
                "RecordP2pArtifact needs an ArtifactStore, and this deployment has not wired                  one in yet: {NOT_STAGE_A}"
            )));
        };
        let req = request.into_inner();
        let (cluster_id, backend, key, node_id) = self.validate_p2p_artifact_fields(
            &req.cluster_id,
            &req.backend,
            &req.key,
            Some(&req.node_id),
        )?;
        artifact_store.record(cluster_id, backend, key, &node_id);
        Ok(Response::new(RecordP2pArtifactResponse {}))
    }

    /// Ports `ForgetP2pArtifact` (`service.go:762-782`).
    async fn forget_p2p_artifact(
        &self,
        request: Request<ForgetP2pArtifactRequest>,
    ) -> Result<Response<ForgetP2pArtifactResponse>, Status> {
        let Some(artifact_store) = self.artifact_store.clone() else {
            return Err(Status::unimplemented(format!(
                "ForgetP2pArtifact needs an ArtifactStore, and this deployment has not wired                  one in yet: {NOT_STAGE_A}"
            )));
        };
        let req = request.into_inner();
        let (cluster_id, backend, key, node_id) = self.validate_p2p_artifact_fields(
            &req.cluster_id,
            &req.backend,
            &req.key,
            Some(&req.node_id),
        )?;
        artifact_store.forget(cluster_id, backend, key, &node_id);
        Ok(Response::new(ForgetP2pArtifactResponse {}))
    }

    /// Ports `LookupP2pArtifact` (`service.go:784-798`): the index lookup,
    /// then `NodeRegistry::filter_p2p_peers` to turn raw node ids into live
    /// peer descriptors -- `artifact_index::lookup_p2p_artifact_peers`
    /// carries both halves so the same logic is reachable from a test
    /// without a gRPC round trip.
    async fn lookup_p2p_artifact(
        &self,
        request: Request<LookupP2pArtifactRequest>,
    ) -> Result<Response<LookupP2pArtifactResponse>, Status> {
        let Some(artifact_store) = self.artifact_store.clone() else {
            return Err(Status::unimplemented(format!(
                "LookupP2pArtifact needs an ArtifactStore, and this deployment has not wired                  one in yet: {NOT_STAGE_A}"
            )));
        };
        let req = request.into_inner();
        let (cluster_id, backend, key, _) =
            self.validate_p2p_artifact_fields(&req.cluster_id, &req.backend, &req.key, None)?;
        let peers = crate::binding_store::artifact_index::lookup_p2p_artifact_peers(
            artifact_store.as_ref(),
            self.registry.as_ref(),
            cluster_id,
            backend,
            key,
            req.exclude_node_id.trim(),
            0,
            SystemTime::now(),
        );
        Ok(Response::new(LookupP2pArtifactResponse { peers }))
    }

    /// Ports `listRegistrySandboxes` (`service.go:849-943`) against
    /// [`PausedSandboxRegistry::list_all`]. Argument validation
    /// (`page_size`, `state`) runs before the registry is even consulted,
    /// matching Go: the answer to a bad request must not depend on whether
    /// this deployment happens to run a registry at all.
    ///
    /// `self.paused_registry` being unwired and a wired registry whose
    /// [`is_cluster_backed`](PausedSandboxRegistry::is_cluster_backed) is
    /// `false` answer identically -- `FailedPrecondition` -- mirroring Go's
    /// own default: `Service.registry` is never a literal nil pointer, it
    /// defaults to `pausedregistry.Disabled()`, whose `List` answers
    /// `ErrDisabled` the same way. See `crate::binding_store::lookup`'s
    /// module doc for the identical reasoning `lookup_node`'s stage 3
    /// already applies to this exact pair of cases.
    ///
    /// Filtering (`state` exact match, `node_id` matched against each row's
    /// [`holder`](crate::orchestrator::PausedRegistryListEntry::holder),
    /// both canonicalised through [`Self::canonical_node_id`] the same way
    /// Go's `canonicalNodeID` is) and keyset paging (`page_token` a
    /// strictly-greater sandbox id) happen here, over the unfiltered,
    /// unpaginated listing [`PausedSandboxRegistry::list_all`] hands back --
    /// that method carries no pagination of its own, matching Go's
    /// `PostgresReader.List` having none either.
    async fn list_registry_sandboxes(
        &self,
        request: Request<ListRegistrySandboxesRequest>,
    ) -> Result<Response<ListRegistrySandboxesResponse>, Status> {
        let req = request.into_inner();
        if req.page_size < 0 {
            return Err(Status::invalid_argument("page_size must not be negative"));
        }
        let state_filter = parse_registry_state_filter(&req.state)?;

        let registry = match &self.paused_registry {
            Some(registry) if registry.is_cluster_backed() => registry,
            _ => {
                return Err(Status::failed_precondition(
                    "paused registry is not configured",
                ))
            }
        };

        let listing = registry
            .list_all()
            .await
            .map_err(|err| Status::unavailable(format!("paused registry unavailable: {err}")))?;

        let node_filter = self.canonical_node_id(&req.node_id);
        let page_token = req.page_token.trim();

        let mut matched: Vec<_> = listing
            .sandboxes
            .into_iter()
            .filter(|entry| state_filter.is_none_or(|state| entry.state == state))
            .filter(|entry| {
                node_filter.is_empty() || self.canonical_node_id(entry.holder()) == node_filter
            })
            .filter(|entry| {
                page_token.is_empty() || entry.sandbox_id.to_string().as_str() > page_token
            })
            .collect();

        // The registry promises no ordering; paging over an unordered list
        // would silently skip rows -- matches Go's own comment
        // (`service.go:911-914`) verbatim.
        matched.sort_by_key(|entry| entry.sandbox_id);

        let mut next_page_token = String::new();
        let page_size = req.page_size as usize;
        if page_size > 0 && page_size < matched.len() {
            matched.truncate(page_size);
            next_page_token = matched
                .last()
                .expect("truncate to a positive page_size leaves at least one row")
                .sandbox_id
                .to_string();
        }

        let sandboxes = matched.iter().map(registry_sandbox_to_proto).collect();

        Ok(Response::new(ListRegistrySandboxesResponse {
            sandboxes,
            next_page_token,
            database_now_unix_ms: listing.now.timestamp_millis(),
        }))
    }
}

/// The five values [`PausedRegistryState`]'s `state` column may hold, in
/// the order Go's `KnownStates()` presents them (`registry.go:44-46`) --
/// used both to encode a row's state onto the wire and to name the
/// accepted set in a `ListRegistrySandboxes` "unknown state" error.
const KNOWN_REGISTRY_STATES: [PausedRegistryState; 5] = [
    PausedRegistryState::Publishing,
    PausedRegistryState::Paused,
    PausedRegistryState::Resuming,
    PausedRegistryState::LocalOnly,
    PausedRegistryState::Running,
];

/// The literal each state encodes as on the wire -- matches the column's own
/// CHECK-constrained values (`sql::ENTRY_COLUMNS`' decode side,
/// `PausedRegistryState::parse`), duplicated here rather than reused because
/// that decode is private to `orchestrator::paused_registry` -- the same
/// duplication `binding_store::lookup::paused_state_label` and
/// `paused_registry::postgres::reconcile`'s own copy already carry.
fn registry_state_str(state: PausedRegistryState) -> &'static str {
    match state {
        PausedRegistryState::Publishing => "publishing",
        PausedRegistryState::Paused => "paused",
        PausedRegistryState::Resuming => "resuming",
        PausedRegistryState::LocalOnly => "local_only",
        PausedRegistryState::Running => "running",
    }
}

/// Ports `parseRegistryStateFilter` (`service.go:947-965`): an empty filter
/// means every state; anything else is matched case-insensitively (Go's
/// `ParseState` uses `strings.EqualFold`) against the five known values, and
/// anything that matches none of them is refused by name rather than
/// silently matching no row -- see [`PausedRegistryState::parse`]'s own doc
/// (via `ParseState`'s identical Go-side comment) for why a state no row can
/// hold must never be filtered on.
fn parse_registry_state_filter(raw: &str) -> Result<Option<PausedRegistryState>, Status> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    for state in KNOWN_REGISTRY_STATES {
        if registry_state_str(state).eq_ignore_ascii_case(trimmed) {
            return Ok(Some(state));
        }
    }
    let known: Vec<&str> = KNOWN_REGISTRY_STATES
        .iter()
        .copied()
        .map(registry_state_str)
        .collect();
    Err(Status::invalid_argument(format!(
        "unknown state '{trimmed}', must be one of {}",
        known.join(", ")
    )))
}

/// Ports `registrySandboxToProto` (`service.go:966-984`): direct field
/// translation, `Option`s collapsing to the wire's own "empty/zero means
/// absent" convention (matches every other proto conversion in this file --
/// `claimed_by_node_id`/`snapshot_id`/`execution_id`, and the lease/deadline
/// pair, all follow the same rule the response's own proto comments name).
fn registry_sandbox_to_proto(entry: &PausedRegistryListEntry) -> scheduler::RegistrySandbox {
    scheduler::RegistrySandbox {
        sandbox_id: entry.sandbox_id.to_string(),
        cluster_id: entry.cluster_id.to_string(),
        state: registry_state_str(entry.state).to_string(),
        generation: entry.generation,
        origin_node_id: entry.origin_node_id.clone(),
        claimed_by_node_id: entry.claimed_by_node_id.clone().unwrap_or_default(),
        snapshot_id: entry
            .snapshot_id
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default(),
        paused_at_unix_ms: entry.paused_at.timestamp_millis(),
        updated_at_unix_ms: entry.updated_at.timestamp_millis(),
        lease_expires_at_unix_ms: entry
            .lease_expires_at
            .map(|t| t.timestamp_millis())
            .unwrap_or(0),
        sandbox_expires_at_unix_ms: entry
            .sandbox_expires_at
            .map(|t| t.timestamp_millis())
            .unwrap_or(0),
        holder_node_id: entry.holder().to_string(),
        execution_id: entry
            .execution_id
            .map(|id| id.to_string())
            .unwrap_or_default(),
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::time::Duration;

    use crate::binding_store::Binding;

    use tokio::sync::oneshot;
    use tonic::transport::Channel;

    use crate::node_registry::types::Node;
    use crate::proto::scheduler::scheduler_client::SchedulerClient;
    use crate::proto::scheduler::scheduler_server::SchedulerServer;
    use crate::proto::scheduler::MachineInfo;

    use super::*;

    async fn service_on_a_socket(
        nodes: Vec<Node>,
    ) -> (
        Arc<AtomicNodeRegistry>,
        SchedulerClient<Channel>,
        oneshot::Sender<()>,
    ) {
        let registry = Arc::new(AtomicNodeRegistry::new(nodes, Duration::from_secs(30)));
        let warmup = Arc::new(WarmupGate::new(
            Arc::clone(&registry) as Arc<dyn NodeRegistry>,
            Duration::from_secs(15),
            SystemTime::now(),
        ));
        let service = NodeRegistryGrpcService::new(Arc::clone(&registry), warmup);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind a port");
        let addr: SocketAddr = listener.local_addr().expect("the bound address");
        let (tx, rx) = oneshot::channel();

        tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(SchedulerServer::new(service))
                .serve_with_incoming_shutdown(
                    tonic::transport::server::TcpIncoming::from(listener),
                    async {
                        let _ = rx.await;
                    },
                )
                .await;
        });
        for _ in 0..200 {
            if tokio::net::TcpStream::connect(addr).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
            .expect("valid endpoint")
            .connect()
            .await
            .expect("connect to the service");
        (registry, SchedulerClient::new(channel), tx)
    }

    fn node(id: &str, endpoint: &str) -> Node {
        Node {
            id: id.to_string(),
            endpoint: endpoint.to_string(),
            pod_name: String::new(),
        }
    }

    // ---- report_sandbox_event: task's own "D1", parametrized over BOTH
    //      binding store backends -- the exact RPC-layer gap the task
    //      called out in Go's own test suite (every
    //      `projection_service_test.go` `Service` used
    //      `NewInMemoryBindingStore`; the Redis backend was only ever
    //      exercised at the store layer). ----

    async fn service_with_binding_store(
        nodes: Vec<Node>,
        binding_store: Arc<dyn BindingStore>,
        projection_authoritative: bool,
    ) -> (SchedulerClient<Channel>, oneshot::Sender<()>) {
        let registry = Arc::new(AtomicNodeRegistry::new(nodes, Duration::from_secs(30)));
        let warmup = Arc::new(WarmupGate::new(
            Arc::clone(&registry) as Arc<dyn NodeRegistry>,
            Duration::from_secs(15),
            SystemTime::now(),
        ));
        let service = NodeRegistryGrpcService::new(registry, warmup).with_binding_store(
            binding_store,
            projection_authoritative,
            Duration::ZERO,
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind a port");
        let addr: SocketAddr = listener.local_addr().expect("the bound address");
        let (tx, rx) = oneshot::channel();

        tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(SchedulerServer::new(service))
                .serve_with_incoming_shutdown(
                    tonic::transport::server::TcpIncoming::from(listener),
                    async {
                        let _ = rx.await;
                    },
                )
                .await;
        });
        for _ in 0..200 {
            if tokio::net::TcpStream::connect(addr).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
            .expect("valid endpoint")
            .connect()
            .await
            .expect("connect to the service");
        (SchedulerClient::new(channel), tx)
    }

    async fn service_with_artifact_store(
        nodes: Vec<Node>,
        artifact_store: Arc<dyn crate::binding_store::artifact_index::ArtifactStore>,
    ) -> (
        Arc<AtomicNodeRegistry>,
        SchedulerClient<Channel>,
        oneshot::Sender<()>,
    ) {
        let registry = Arc::new(AtomicNodeRegistry::new(nodes, Duration::from_secs(30)));
        let warmup = Arc::new(WarmupGate::new(
            Arc::clone(&registry) as Arc<dyn NodeRegistry>,
            Duration::from_secs(15),
            SystemTime::now(),
        ));
        let service = NodeRegistryGrpcService::new(Arc::clone(&registry), warmup)
            .with_artifact_store(artifact_store);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind a port");
        let addr: SocketAddr = listener.local_addr().expect("the bound address");
        let (tx, rx) = oneshot::channel();

        tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(SchedulerServer::new(service))
                .serve_with_incoming_shutdown(
                    tonic::transport::server::TcpIncoming::from(listener),
                    async {
                        let _ = rx.await;
                    },
                )
                .await;
        });
        for _ in 0..200 {
            if tokio::net::TcpStream::connect(addr).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
            .expect("valid endpoint")
            .connect()
            .await
            .expect("connect to the service");
        (registry, SchedulerClient::new(channel), tx)
    }

    fn sandbox_event(
        sandbox_id: &str,
        event_type: crate::proto::scheduler::SandboxEventType,
        execution_id: &str,
    ) -> crate::proto::scheduler::SandboxEvent {
        crate::proto::scheduler::SandboxEvent {
            sandbox_id: sandbox_id.to_string(),
            event_type: event_type as i32,
            execution_id: execution_id.to_string(),
            ..Default::default()
        }
    }

    async fn report(
        client: &mut SchedulerClient<Channel>,
        events: Vec<crate::proto::scheduler::SandboxEvent>,
    ) {
        client
            .report_sandbox_event(crate::proto::scheduler::ReportSandboxEventRequest {
                node_id: "node-a".to_string(),
                cluster_id: "cluster-a".to_string(),
                service_instance_id: "node-a-instance".to_string(),
                events,
            })
            .await
            .expect("ReportSandboxEvent must never fail on an individual event's account");
    }

    /// The full `applyProjectionDelete` behavior matrix, run once per
    /// backend by the two `#[tokio::test]`s below. `binding_store` is
    /// asserted against directly (not only through the RPC) so a failure
    /// names exactly which guard broke.
    async fn assert_report_sandbox_event_behavior_matrix(binding_store: Arc<dyn BindingStore>) {
        use crate::proto::scheduler::SandboxEventType;
        const EXEC_1: &str = "00000000-0000-7000-8000-000000000001";
        const EXEC_2: &str = "00000000-0000-7000-8000-000000000002";

        // 1. Switch off: even a matching PAUSE must not delete.
        {
            let (mut client, _stop) = service_with_binding_store(
                vec![],
                Arc::clone(&binding_store),
                /* projection_authoritative = */ false,
            )
            .await;
            binding_store
                .record(
                    "sbx-off",
                    Binding {
                        node: node("node-a", "http://node-a"),
                        execution_id: EXEC_1.to_string(),
                        projection_ttl: Duration::ZERO,
                    },
                    SystemTime::now(),
                )
                .await
                .unwrap();
            report(
                &mut client,
                vec![sandbox_event("sbx-off", SandboxEventType::Pause, EXEC_1)],
            )
            .await;
            assert!(
                binding_store.get("sbx-off", SystemTime::now()).await.unwrap().is_some(),
                "projection_authoritative=false must leave the switch off, even for a matching event"
            );
        }

        // 2. Switch on, matching incarnation, PAUSE: deletes.
        {
            let (mut client, _stop) =
                service_with_binding_store(vec![], Arc::clone(&binding_store), true).await;
            binding_store
                .record(
                    "sbx-pause",
                    Binding {
                        node: node("node-a", "http://node-a"),
                        execution_id: EXEC_1.to_string(),
                        projection_ttl: Duration::ZERO,
                    },
                    SystemTime::now(),
                )
                .await
                .unwrap();
            report(
                &mut client,
                vec![sandbox_event("sbx-pause", SandboxEventType::Pause, EXEC_1)],
            )
            .await;
            assert!(
                binding_store
                    .get("sbx-pause", SystemTime::now())
                    .await
                    .unwrap()
                    .is_none(),
                "a matching PAUSE event must delete the binding"
            );
        }

        // 3. Switch on, matching incarnation, DELETE: also deletes (not
        //    PAUSE-only).
        {
            let (mut client, _stop) =
                service_with_binding_store(vec![], Arc::clone(&binding_store), true).await;
            binding_store
                .record(
                    "sbx-delete",
                    Binding {
                        node: node("node-a", "http://node-a"),
                        execution_id: EXEC_1.to_string(),
                        projection_ttl: Duration::ZERO,
                    },
                    SystemTime::now(),
                )
                .await
                .unwrap();
            report(
                &mut client,
                vec![sandbox_event(
                    "sbx-delete",
                    SandboxEventType::Delete,
                    EXEC_1,
                )],
            )
            .await;
            assert!(binding_store
                .get("sbx-delete", SystemTime::now())
                .await
                .unwrap()
                .is_none());
        }

        // 4. Switch on, stale incarnation: the record survives -- "the
        //    guard is the whole point," end to end through the actual RPC.
        {
            let (mut client, _stop) =
                service_with_binding_store(vec![], Arc::clone(&binding_store), true).await;
            binding_store
                .record(
                    "sbx-stale",
                    Binding {
                        node: node("node-a", "http://node-a"),
                        execution_id: EXEC_2.to_string(),
                        projection_ttl: Duration::ZERO,
                    },
                    SystemTime::now(),
                )
                .await
                .unwrap();
            report(
                &mut client,
                vec![sandbox_event("sbx-stale", SandboxEventType::Pause, EXEC_1)],
            )
            .await;
            let binding = binding_store
                .get("sbx-stale", SystemTime::now())
                .await
                .unwrap()
                .expect("a stale event must not delete the live record");
            assert_eq!(binding.execution_id, EXEC_2);
        }

        // 5. Switch on, empty execution id (an old reporter): ignored, not
        //    an unguarded delete.
        {
            let (mut client, _stop) =
                service_with_binding_store(vec![], Arc::clone(&binding_store), true).await;
            binding_store
                .record(
                    "sbx-unnamed",
                    Binding {
                        node: node("node-a", "http://node-a"),
                        execution_id: EXEC_1.to_string(),
                        projection_ttl: Duration::ZERO,
                    },
                    SystemTime::now(),
                )
                .await
                .unwrap();
            report(
                &mut client,
                vec![sandbox_event("sbx-unnamed", SandboxEventType::Pause, "")],
            )
            .await;
            assert!(
                binding_store.get("sbx-unnamed", SystemTime::now()).await.unwrap().is_some(),
                "an event with no execution id must never delete -- that would be the                  unguarded delete the guard exists to prevent"
            );
        }

        // 6. Switch on, a non-PAUSE/DELETE type naming the same sandbox and
        //    a matching execution id: observed only, never deletes.
        {
            let (mut client, _stop) =
                service_with_binding_store(vec![], Arc::clone(&binding_store), true).await;
            binding_store
                .record(
                    "sbx-create",
                    Binding {
                        node: node("node-a", "http://node-a"),
                        execution_id: EXEC_1.to_string(),
                        projection_ttl: Duration::ZERO,
                    },
                    SystemTime::now(),
                )
                .await
                .unwrap();
            report(
                &mut client,
                vec![sandbox_event(
                    "sbx-create",
                    SandboxEventType::Create,
                    EXEC_1,
                )],
            )
            .await;
            assert!(
                binding_store
                    .get("sbx-create", SystemTime::now())
                    .await
                    .unwrap()
                    .is_some(),
                "CREATE/RESUME/FORK events must never delete a binding"
            );
        }
    }

    #[tokio::test]
    async fn report_sandbox_event_behavior_matrix_in_memory() {
        let store: Arc<dyn BindingStore> =
            Arc::new(crate::binding_store::InMemoryBindingStore::new(
                crate::binding_store::BindingStoreSettings::default(),
            ));
        assert_report_sandbox_event_behavior_matrix(store).await;
    }

    #[tokio::test]
    async fn report_sandbox_event_behavior_matrix_redis() {
        let Some(store) = crate::binding_store::redis::harness::store_for(
            "report_sandbox_event_behavior_matrix_redis",
            crate::binding_store::BindingStoreSettings::default(),
            |_| {},
        )
        .await
        else {
            return;
        };
        let store: Arc<dyn BindingStore> = Arc::new(store);
        assert_report_sandbox_event_behavior_matrix(store).await;
    }

    /// Before `with_binding_store` is called (every default path today),
    /// `ReportSandboxEvent` must answer `Unimplemented`, not silently
    /// accept and do nothing -- same discipline as every other
    /// not-yet-wired RPC in this file.
    #[tokio::test]
    async fn report_sandbox_event_without_a_binding_store_is_unimplemented() {
        let (_registry, mut client, _stop) = service_on_a_socket(vec![]).await;
        let status = client
            .report_sandbox_event(crate::proto::scheduler::ReportSandboxEventRequest {
                node_id: "node-a".to_string(),
                cluster_id: "cluster-a".to_string(),
                service_instance_id: "node-a-instance".to_string(),
                events: vec![sandbox_event(
                    "sbx-1",
                    crate::proto::scheduler::SandboxEventType::Pause,
                    "exec-1",
                )],
            })
            .await
            .expect_err("no binding store has been wired in");
        assert_eq!(status.code(), tonic::Code::Unimplemented);
    }

    fn heartbeat_with_cpu_config(node_id: &str, cpu_config_json: &str) -> HeartbeatRequest {
        HeartbeatRequest {
            node_id: node_id.to_string(),
            cluster_id: "cluster-a".to_string(),
            service_instance_id: format!("{node_id}-instance"),
            machine_info: Some(MachineInfo {
                cpu_config_json: cpu_config_json.to_string(),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    /// A heartbeat that makes its node a live P2P peer:
    /// `filter_p2p_peers_locked` requires `NodeStatus::Ready` *and* a
    /// non-empty `P2pEndpoint`, neither of which `heartbeat_with_cpu_config`
    /// sets (its snapshot is absent, which lands on `Connecting`, not
    /// `Ready`).
    fn heartbeat_as_a_ready_p2p_peer(node_id: &str) -> HeartbeatRequest {
        HeartbeatRequest {
            node_id: node_id.to_string(),
            cluster_id: "cluster-a".to_string(),
            service_instance_id: format!("{node_id}-instance"),
            snapshot: Some(crate::proto::scheduler::NodeSnapshot {
                status: scheduler::NodeStatus::Ready as i32,
                ..Default::default()
            }),
            p2p_endpoint: Some(crate::proto::scheduler::P2pEndpoint {
                backend: "iroh".to_string(),
                address: format!("{node_id}-p2p-address"),
            }),
            ..Default::default()
        }
    }

    /// `ListNodes` is a direct, side-effect-free translation of discovery
    /// state — the control every other test in this file implicitly relies
    /// on to prove discovery was seeded correctly.
    #[tokio::test]
    async fn list_nodes_reflects_discovery() {
        let (_registry, mut client, _stop) = service_on_a_socket(vec![
            node("node-a", "http://10.0.0.7:8000"),
            node("node-b", "http://10.0.0.9:8000"),
        ])
        .await;

        let response = client
            .list_nodes(ListNodesRequest {})
            .await
            .expect("list_nodes answers")
            .into_inner();
        let mut ids: Vec<String> = response.nodes.iter().map(|n| n.node_id.clone()).collect();
        ids.sort();
        assert_eq!(ids, vec!["node-a".to_string(), "node-b".to_string()]);
    }

    /// The exact error message a node's dual heartbeat matches on
    /// (`src/observability/reporter.rs`'s `HeartbeatNodeNotConfigured`
    /// detection) has to come out of *this* service byte-for-byte, not just
    /// "an InvalidArgument of some kind".
    #[tokio::test]
    async fn heartbeat_from_an_unknown_node_matches_the_real_schedulers_wording() {
        let (_registry, mut client, _stop) = service_on_a_socket(vec![]).await;

        let status = client
            .heartbeat(heartbeat_with_cpu_config("node-a", ""))
            .await
            .expect_err("node-a is not discovered");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert_eq!(status.message(), "node is not in scheduler node list");
    }

    /// 🔴 D6 (task's own label), end to end through the actual gRPC service
    /// rather than through `AtomicNodeRegistry::heartbeat` directly (already
    /// covered in `super::super::registry`'s own tests): three nodes
    /// heartbeat their `cpu_config_json` over the wire, and the third node's
    /// `HeartbeatResponse.cpu_config_json` carries the cluster's bitwise-AND
    /// intersection — computed by `super::super::cpu_template`, whose own
    /// tests separately prove byte-for-byte agreement with the real Go
    /// `IntersectCpuConfigs`. Chained together, this is the proof that the
    /// wire path this file adds does not lose or reorder anything between
    /// "a node's heartbeat lands" and "the algorithm runs on it".
    #[tokio::test]
    async fn heartbeat_carries_the_cluster_cpu_intersection_back_once_everyone_has_reported() {
        let (_registry, mut client, _stop) = service_on_a_socket(vec![
            node("node-a", "http://10.0.0.7:8000"),
            node("node-b", "http://10.0.0.9:8000"),
        ])
        .await;

        let cfg_a =
            r#"{"kvm_capabilities":["cap.a","cap.b"],"cpuid_modifiers":[],"msr_modifiers":[]}"#;
        let cfg_b =
            r#"{"kvm_capabilities":["cap.a","cap.c"],"cpuid_modifiers":[],"msr_modifiers":[]}"#;

        // 🔴 Seed both nodes with an empty-config heartbeat first, the same
        // discipline `registry.rs`'s own intersection tests use and document
        // why: "all configs ready" is gated on every node that has *ever
        // heartbeated for this cluster* having a non-empty config, not on
        // every node discovery knows about. Without this seeding step,
        // node-a's very first heartbeat below would already look like "the
        // whole (one-node) cluster has reported" and deliver an intersection
        // of one config with itself — which is exactly the wrong thing this
        // test needs to rule out.
        client
            .heartbeat(heartbeat_with_cpu_config("node-a", ""))
            .await
            .expect("node-a seed heartbeat");
        client
            .heartbeat(heartbeat_with_cpu_config("node-b", ""))
            .await
            .expect("node-b seed heartbeat");

        // node-a reports its real config: the cluster is not "all configs
        // ready" yet (node-b's seeded config is still empty), so no
        // intersection comes back.
        let first = client
            .heartbeat(heartbeat_with_cpu_config("node-a", cfg_a))
            .await
            .expect("node-a heartbeats")
            .into_inner();
        assert_eq!(
            first.cpu_config_json, "",
            "intersection was computed before every node had reported"
        );

        // node-b reports second: now every node has a config, and the
        // reporting node (node-b) gets the intersection on its own response.
        let second = client
            .heartbeat(heartbeat_with_cpu_config("node-b", cfg_b))
            .await
            .expect("node-b heartbeats")
            .into_inner();
        assert_eq!(
            second.cpu_config_json,
            r#"{"kvm_capabilities":["cap.a"],"cpuid_modifiers":[],"msr_modifiers":[]}"#,
            "the intersection delivered over the wire does not match \
             cpu_template::intersect_cpu_configs's own algorithm"
        );
    }

    /// `GetNode` is `NotFound` for a node that has never heartbeated, even
    /// if discovery knows about it — the same semantics
    /// `node_client::NativeNodePlacement`'s tests prove against the registry
    /// directly, proven here against the actual RPC.
    #[tokio::test]
    async fn get_node_is_not_found_before_any_heartbeat() {
        let (_registry, mut client, _stop) =
            service_on_a_socket(vec![node("node-a", "http://10.0.0.7:8000")]).await;

        let status = client
            .get_node(GetNodeRequest {
                node_id: "node-a".to_string(),
                cluster_id: String::new(),
            })
            .await
            .expect_err("node-a has never heartbeated");
        assert_eq!(status.code(), tonic::Code::NotFound);

        client
            .heartbeat(heartbeat_with_cpu_config("node-a", ""))
            .await
            .expect("node-a heartbeats");

        let response = client
            .get_node(GetNodeRequest {
                node_id: "node-a".to_string(),
                cluster_id: String::new(),
            })
            .await
            .expect("node-a has now heartbeated")
            .into_inner();
        assert_eq!(response.node.expect("a node").node_id, "node-a");
    }

    /// `UnregisterNode` refuses a service-instance mismatch (a stale
    /// unregister racing a restart under the same node id) rather than
    /// deleting a fresher record out from under it.
    #[tokio::test]
    async fn unregister_node_refuses_a_service_instance_mismatch() {
        let (registry, mut client, _stop) =
            service_on_a_socket(vec![node("node-a", "http://10.0.0.7:8000")]).await;
        client
            .heartbeat(heartbeat_with_cpu_config("node-a", ""))
            .await
            .expect("node-a heartbeats");

        let status = client
            .unregister_node(UnregisterNodeRequest {
                node_id: "node-a".to_string(),
                service_instance_id: "a-different-instance".to_string(),
            })
            .await
            .expect_err("service instance mismatch");
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert_eq!(status.message(), "service instance mismatch");
        assert!(
            registry
                .get_observed("node-a", "", SystemTime::now())
                .is_some(),
            "a rejected unregister must not have cleared the record"
        );

        client
            .unregister_node(UnregisterNodeRequest {
                node_id: "node-a".to_string(),
                service_instance_id: "node-a-instance".to_string(),
            })
            .await
            .expect("the matching instance id is accepted");
        assert!(
            registry
                .get_observed("node-a", "", SystemTime::now())
                .is_none(),
            "the matching unregister should have cleared the record"
        );
    }

    /// E2 (task's own label): `ListP2pPeers` now answers for real, end to
    /// end through the actual gRPC service rather than through
    /// `AtomicNodeRegistry::list_p2p_peers` directly (already covered in
    /// `super::super::registry`'s own tests,
    /// `list_p2p_peers_returns_only_ready_matching_peers` /
    /// `list_p2p_peers_drops_expired_and_unregistered_nodes`). Three nodes
    /// heartbeat as ready P2P peers, one under a different backend and one
    /// excluded by id, and the response must reflect both filters — a
    /// mutant that dropped either filter (e.g. ignored `backend` or
    /// `exclude_node_id`, or returned every discovered node regardless of
    /// whether it ever heartbeated) would still pass a test that only
    /// checked "the RPC no longer errors."
    #[tokio::test]
    async fn list_p2p_peers_answers_from_the_registry() {
        let (_registry, mut client, _stop) = service_on_a_socket(vec![
            node("node-a", "http://10.0.0.1:8000"),
            node("node-b", "http://10.0.0.2:8000"),
            node("node-c", "http://10.0.0.3:8000"),
        ])
        .await;

        client
            .heartbeat(heartbeat_as_a_ready_p2p_peer("node-a"))
            .await
            .expect("node-a heartbeats in");
        client
            .heartbeat(heartbeat_as_a_ready_p2p_peer("node-b"))
            .await
            .expect("node-b heartbeats in");
        // node-c heartbeats under a different P2P backend, and must not show
        // up in an "iroh" lookup.
        let mut other_backend = heartbeat_as_a_ready_p2p_peer("node-c");
        other_backend.p2p_endpoint = Some(crate::proto::scheduler::P2pEndpoint {
            backend: "smb".to_string(),
            address: "node-c-smb-address".to_string(),
        });
        client
            .heartbeat(other_backend)
            .await
            .expect("node-c heartbeats in under a different backend");

        let response = client
            .list_p2p_peers(ListP2pPeersRequest {
                cluster_id: "cluster-a".to_string(),
                backend: "iroh".to_string(),
                exclude_node_id: "node-a".to_string(),
            })
            .await
            .expect("list_p2p_peers answers")
            .into_inner();

        assert_eq!(
            response.peers,
            vec![crate::proto::scheduler::P2pPeer {
                node_id: "node-b".to_string(),
                endpoint: Some(crate::proto::scheduler::P2pEndpoint {
                    backend: "iroh".to_string(),
                    address: "node-b-p2p-address".to_string(),
                }),
            }],
            "must exclude node-a (excluded by id), node-c (different backend), and never \
             invent a peer for a node that has not heartbeated"
        );
    }

    // ---- heartbeat/unregister_node's ReconcileNode wiring: task's own "D3" ----

    /// A `BindingStore` double that always fails, for proving the two
    /// different failure disciplines `heartbeat` and `unregister_node` use
    /// (fatal vs. best-effort) without needing to actually take a real
    /// Redis down mid-test.
    struct FailingBindingStore;

    #[async_trait::async_trait]
    impl BindingStore for FailingBindingStore {
        async fn get(
            &self,
            _sandbox_id: &str,
            _now: SystemTime,
        ) -> Result<Option<Binding>, crate::binding_store::BindingStoreError> {
            Err(crate::binding_store::BindingStoreError::new("always fails"))
        }
        async fn record(
            &self,
            _sandbox_id: &str,
            _binding: Binding,
            _now: SystemTime,
        ) -> Result<crate::binding_store::BindingDecision, crate::binding_store::BindingStoreError>
        {
            Err(crate::binding_store::BindingStoreError::new("always fails"))
        }
        async fn reconcile_node(
            &self,
            _node: crate::node_registry::types::Node,
            _roster: Vec<crate::node_registry::types::RosterEntry>,
            _now: SystemTime,
        ) -> Result<
            Vec<(String, crate::binding_store::BindingDecision)>,
            crate::binding_store::BindingStoreError,
        > {
            Err(crate::binding_store::BindingStoreError::new("always fails"))
        }
        async fn delete(
            &self,
            _sandbox_id: &str,
            _execution_id: &str,
            _now: SystemTime,
        ) -> Result<BindingDeleteOutcome, crate::binding_store::BindingStoreError> {
            Err(crate::binding_store::BindingStoreError::new("always fails"))
        }
    }

    #[tokio::test]
    async fn heartbeat_reconciles_the_roster_into_the_binding_store() {
        let store: Arc<dyn BindingStore> =
            Arc::new(crate::binding_store::InMemoryBindingStore::new(
                crate::binding_store::BindingStoreSettings::default(),
            ));
        let (mut client, _stop) = service_with_binding_store(
            vec![node("node-a", "http://node-a")],
            Arc::clone(&store),
            true,
        )
        .await;

        client
            .heartbeat(crate::proto::scheduler::HeartbeatRequest {
                node_id: "node-a".to_string(),
                cluster_id: "cluster-a".to_string(),
                service_instance_id: "node-a-instance".to_string(),
                roster: vec![crate::proto::scheduler::SandboxRosterEntry {
                    sandbox_id: "sbx-1".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await
            .expect("node-a heartbeats");

        let binding = store
            .get("sbx-1", SystemTime::now())
            .await
            .unwrap()
            .expect("the roster entry must have been reconciled into the binding store");
        assert_eq!(binding.node.id, "node-a");
    }

    #[tokio::test]
    async fn heartbeat_fails_with_unavailable_when_the_binding_store_reconcile_fails() {
        let store: Arc<dyn BindingStore> = Arc::new(FailingBindingStore);
        let (mut client, _stop) =
            service_with_binding_store(vec![node("node-a", "http://node-a")], store, true).await;

        let status = client
            .heartbeat(heartbeat_with_cpu_config("node-a", ""))
            .await
            .expect_err("a ReconcileNode failure must fail the RPC, unlike ReportSandboxEvent");
        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert_eq!(status.message(), "binding store unavailable");
    }

    #[tokio::test]
    async fn unregister_node_removes_the_bindings_it_owns() {
        let store: Arc<dyn BindingStore> =
            Arc::new(crate::binding_store::InMemoryBindingStore::new(
                crate::binding_store::BindingStoreSettings::default(),
            ));
        store
            .record(
                "sbx-1",
                Binding {
                    node: node("node-a", "http://node-a"),
                    ..Default::default()
                },
                SystemTime::now(),
            )
            .await
            .unwrap();
        let (mut client, _stop) = service_with_binding_store(
            vec![node("node-a", "http://node-a")],
            Arc::clone(&store),
            true,
        )
        .await;
        client
            .heartbeat(heartbeat_with_cpu_config("node-a", ""))
            .await
            .expect("node-a heartbeats so unregister_node can resolve its identity");

        client
            .unregister_node(UnregisterNodeRequest {
                node_id: "node-a".to_string(),
                service_instance_id: "node-a-instance".to_string(),
            })
            .await
            .expect("unregister succeeds");

        assert!(
            store
                .get("sbx-1", SystemTime::now())
                .await
                .unwrap()
                .is_none(),
            "unregistering a node must release the bindings it owned"
        );
    }

    #[tokio::test]
    async fn unregister_node_still_succeeds_when_binding_cleanup_fails() {
        let store: Arc<dyn BindingStore> = Arc::new(FailingBindingStore);
        let (mut client, _stop) =
            service_with_binding_store(vec![node("node-a", "http://node-a")], store, true).await;
        client
            .heartbeat(heartbeat_with_cpu_config("node-a", ""))
            .await
            .expect_err("FailingBindingStore also fails the heartbeat reconcile in this setup");

        // The node is still discoverable even though the heartbeat above
        // never got recorded as observed (ReconcileNode failed before
        // warmup/observed state would matter here) -- unregister_node only
        // needs `resolve`, which comes from discovery, not from a
        // successful heartbeat.
        client
            .unregister_node(UnregisterNodeRequest {
                node_id: "node-a".to_string(),
                service_instance_id: "node-a-instance".to_string(),
            })
            .await
            .expect("a binding-cleanup failure must not fail unregister_node itself");
    }

    // ---- record_assignment: task's own "D3" ----

    #[tokio::test]
    async fn record_assignment_writes_a_binding_resolved_through_discovery() {
        let store: Arc<dyn BindingStore> =
            Arc::new(crate::binding_store::InMemoryBindingStore::new(
                crate::binding_store::BindingStoreSettings::default(),
            ));
        let (mut client, _stop) = service_with_binding_store(
            vec![node("node-a", "http://10.0.0.7:8000")],
            Arc::clone(&store),
            true,
        )
        .await;

        client
            .record_assignment(RecordAssignmentRequest {
                sandbox_id: "sbx-1".to_string(),
                node: Some(crate::proto::scheduler::Node {
                    node_id: "node-a".to_string(),
                    // Deliberately a stale endpoint the caller might have
                    // cached -- resolving through discovery must use the
                    // registry's own current endpoint, not this one.
                    endpoint: "http://stale:9999".to_string(),
                }),
                execution_id: "00000000-0000-7000-8000-000000000001".to_string(),
                projection_ttl_secs: 0,
            })
            .await
            .expect("record_assignment succeeds");

        let binding = store
            .get("sbx-1", SystemTime::now())
            .await
            .unwrap()
            .expect("bound");
        assert_eq!(
            binding.node.endpoint, "http://10.0.0.7:8000",
            "the binding must carry discovery's current endpoint, not the caller's stale one"
        );
        assert_eq!(binding.execution_id, "00000000-0000-7000-8000-000000000001");
    }

    #[tokio::test]
    async fn record_assignment_rejects_an_unknown_node() {
        let store: Arc<dyn BindingStore> =
            Arc::new(crate::binding_store::InMemoryBindingStore::new(
                crate::binding_store::BindingStoreSettings::default(),
            ));
        let (mut client, _stop) = service_with_binding_store(vec![], store, true).await;

        let status = client
            .record_assignment(RecordAssignmentRequest {
                sandbox_id: "sbx-1".to_string(),
                node: Some(crate::proto::scheduler::Node {
                    node_id: "node-a".to_string(),
                    endpoint: "http://10.0.0.7:8000".to_string(),
                }),
                execution_id: String::new(),
                projection_ttl_secs: 0,
            })
            .await
            .expect_err("node-a is not discovered");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn record_assignment_rejects_missing_required_fields() {
        let store: Arc<dyn BindingStore> =
            Arc::new(crate::binding_store::InMemoryBindingStore::new(
                crate::binding_store::BindingStoreSettings::default(),
            ));
        let (mut client, _stop) = service_with_binding_store(vec![], store, true).await;

        let status = client
            .record_assignment(RecordAssignmentRequest {
                sandbox_id: String::new(),
                node: Some(crate::proto::scheduler::Node {
                    node_id: "node-a".to_string(),
                    endpoint: "http://10.0.0.7:8000".to_string(),
                }),
                ..Default::default()
            })
            .await
            .expect_err("empty sandbox_id");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);

        let status = client
            .record_assignment(RecordAssignmentRequest {
                sandbox_id: "sbx-1".to_string(),
                node: None,
                ..Default::default()
            })
            .await
            .expect_err("missing node");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn resolve_projection_ttl_matches_the_go_table() {
        let off = NodeRegistryGrpcService::new(
            Arc::new(AtomicNodeRegistry::new(vec![], Duration::from_secs(30))),
            Arc::new(WarmupGate::new(
                Arc::new(AtomicNodeRegistry::new(vec![], Duration::from_secs(30)))
                    as Arc<dyn NodeRegistry>,
                Duration::from_secs(15),
                SystemTime::now(),
            )),
        );
        assert_eq!(
            off.resolve_projection_ttl(Duration::from_secs(60)),
            (Duration::ZERO, "default"),
            "the switch off must always answer default, regardless of what the node offered"
        );

        let on = off.with_binding_store(
            Arc::new(crate::binding_store::InMemoryBindingStore::new(
                crate::binding_store::BindingStoreSettings::default(),
            )),
            true,
            Duration::from_secs(3600),
        );
        assert_eq!(
            on.resolve_projection_ttl(Duration::ZERO),
            (Duration::ZERO, "default"),
            "a non-positive raw value is always default, even with the switch on"
        );
        assert_eq!(
            on.resolve_projection_ttl(Duration::from_secs(60)),
            (Duration::from_secs(60), "node"),
            "a raw value under the ceiling passes through unchanged"
        );
        assert_eq!(
            on.resolve_projection_ttl(Duration::from_secs(7200)),
            (Duration::from_secs(3600), "clamped"),
            "a raw value over the ceiling is clamped to it"
        );
    }

    // ---- P2P artifact index RPCs: task's own "D4" ----

    #[tokio::test]
    async fn record_then_lookup_p2p_artifact_round_trips_through_the_rpc() {
        let store: Arc<dyn crate::binding_store::artifact_index::ArtifactStore> =
            Arc::new(crate::binding_store::artifact_index::InMemoryArtifactStore::new(10));
        let (_registry, mut client, _stop) =
            service_with_artifact_store(vec![node("node-a", "http://10.0.0.7:8000")], store).await;
        // filter_p2p_peers (LookupP2pArtifact's second half) requires the
        // node to be a live, Ready P2P peer -- see
        // heartbeat_as_a_ready_p2p_peer's own doc comment.
        client
            .heartbeat(heartbeat_as_a_ready_p2p_peer("node-a"))
            .await
            .expect("node-a heartbeats");

        client
            .record_p2p_artifact(RecordP2pArtifactRequest {
                cluster_id: "cluster-a".to_string(),
                backend: "iroh".to_string(),
                key: "sha256:abc".to_string(),
                node_id: "node-a".to_string(),
            })
            .await
            .expect("record succeeds");

        let response = client
            .lookup_p2p_artifact(LookupP2pArtifactRequest {
                cluster_id: "cluster-a".to_string(),
                backend: "iroh".to_string(),
                key: "sha256:abc".to_string(),
                exclude_node_id: String::new(),
            })
            .await
            .expect("lookup succeeds")
            .into_inner();

        assert_eq!(response.peers.len(), 1);
        assert_eq!(response.peers[0].node_id, "node-a");
    }

    #[tokio::test]
    async fn lookup_p2p_artifact_excludes_the_requested_node() {
        let artifact_store =
            Arc::new(crate::binding_store::artifact_index::InMemoryArtifactStore::new(10));
        artifact_store.record("cluster-a", "iroh", "sha256:abc", "node-a");
        artifact_store.record("cluster-a", "iroh", "sha256:abc", "node-b");
        let store: Arc<dyn crate::binding_store::artifact_index::ArtifactStore> = artifact_store;
        let (_registry, mut client, _stop) = service_with_artifact_store(
            vec![
                node("node-a", "http://10.0.0.7:8000"),
                node("node-b", "http://10.0.0.9:8000"),
            ],
            store,
        )
        .await;
        // node-a is also a live P2P peer -- proves the exclusion filters
        // it out specifically, rather than the index simply having nothing
        // for it.
        client
            .heartbeat(heartbeat_as_a_ready_p2p_peer("node-a"))
            .await
            .expect("node-a heartbeats");
        client
            .heartbeat(heartbeat_as_a_ready_p2p_peer("node-b"))
            .await
            .expect("node-b heartbeats");

        let response = client
            .lookup_p2p_artifact(LookupP2pArtifactRequest {
                cluster_id: "cluster-a".to_string(),
                backend: "iroh".to_string(),
                key: "sha256:abc".to_string(),
                exclude_node_id: "node-a".to_string(),
            })
            .await
            .expect("lookup succeeds")
            .into_inner();

        assert_eq!(
            response
                .peers
                .iter()
                .map(|p| p.node_id.clone())
                .collect::<Vec<_>>(),
            vec!["node-b".to_string()],
            "node-b must appear and node-a must be excluded -- not both empty"
        );
    }

    #[tokio::test]
    async fn forget_p2p_artifact_removes_the_association() {
        let artifact_store =
            Arc::new(crate::binding_store::artifact_index::InMemoryArtifactStore::new(10));
        artifact_store.record("cluster-a", "iroh", "sha256:abc", "node-a");
        let store: Arc<dyn crate::binding_store::artifact_index::ArtifactStore> = artifact_store;
        let (_registry, mut client, _stop) =
            service_with_artifact_store(vec![node("node-a", "http://10.0.0.7:8000")], store).await;

        client
            .forget_p2p_artifact(ForgetP2pArtifactRequest {
                cluster_id: "cluster-a".to_string(),
                backend: "iroh".to_string(),
                key: "sha256:abc".to_string(),
                node_id: "node-a".to_string(),
            })
            .await
            .expect("forget succeeds");

        let response = client
            .lookup_p2p_artifact(LookupP2pArtifactRequest {
                cluster_id: "cluster-a".to_string(),
                backend: "iroh".to_string(),
                key: "sha256:abc".to_string(),
                exclude_node_id: String::new(),
            })
            .await
            .expect("lookup succeeds")
            .into_inner();
        assert!(response.peers.is_empty());
    }

    #[tokio::test]
    async fn unregister_node_forgets_its_p2p_artifact_associations() {
        let artifact_store =
            Arc::new(crate::binding_store::artifact_index::InMemoryArtifactStore::new(10));
        artifact_store.record("cluster-a", "iroh", "sha256:abc", "node-a");
        let store: Arc<dyn crate::binding_store::artifact_index::ArtifactStore> =
            Arc::clone(&artifact_store)
                as Arc<dyn crate::binding_store::artifact_index::ArtifactStore>;
        let (_registry, mut client, _stop) =
            service_with_artifact_store(vec![node("node-a", "http://10.0.0.7:8000")], store).await;
        client
            .heartbeat(heartbeat_with_cpu_config("node-a", ""))
            .await
            .expect("node-a heartbeats");

        client
            .unregister_node(UnregisterNodeRequest {
                node_id: "node-a".to_string(),
                service_instance_id: "node-a-instance".to_string(),
            })
            .await
            .expect("unregister succeeds");

        assert!(
            artifact_store
                .lookup("cluster-a", "iroh", "sha256:abc", 0)
                .is_empty(),
            "unregistering a node must forget its P2P artifact associations too"
        );
    }

    #[tokio::test]
    async fn p2p_artifact_rpcs_without_an_artifact_store_are_unimplemented() {
        let (_registry, mut client, _stop) = service_on_a_socket(vec![]).await;

        let status = client
            .record_p2p_artifact(RecordP2pArtifactRequest {
                cluster_id: "c".to_string(),
                backend: "b".to_string(),
                key: "k".to_string(),
                node_id: "node-a".to_string(),
            })
            .await
            .expect_err("no artifact store wired");
        assert_eq!(status.code(), tonic::Code::Unimplemented);

        let status = client
            .lookup_p2p_artifact(LookupP2pArtifactRequest {
                cluster_id: "c".to_string(),
                backend: "b".to_string(),
                key: "k".to_string(),
                exclude_node_id: String::new(),
            })
            .await
            .expect_err("no artifact store wired");
        assert_eq!(status.code(), tonic::Code::Unimplemented);
    }

    #[tokio::test]
    async fn record_p2p_artifact_rejects_missing_required_fields() {
        let store: Arc<dyn crate::binding_store::artifact_index::ArtifactStore> =
            Arc::new(crate::binding_store::artifact_index::InMemoryArtifactStore::new(10));
        let (_registry, mut client, _stop) = service_with_artifact_store(vec![], store).await;

        let status = client
            .record_p2p_artifact(RecordP2pArtifactRequest {
                cluster_id: String::new(),
                backend: "b".to_string(),
                key: "k".to_string(),
                node_id: "node-a".to_string(),
            })
            .await
            .expect_err("empty cluster_id");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);

        let status = client
            .record_p2p_artifact(RecordP2pArtifactRequest {
                cluster_id: "c".to_string(),
                backend: "b".to_string(),
                key: "k".to_string(),
                node_id: String::new(),
            })
            .await
            .expect_err("empty node_id");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    // ---- Schedule / LookupNode: Stage D's remainder ----
    //
    // Every test below calls the service directly (`service.schedule(...)`/
    // `service.lookup_node(...)`) rather than over a socket -- there is
    // nothing gRPC-transport-specific left to exercise once
    // `record_assignment`/`heartbeat`'s own socket-based tests above prove
    // the server wiring works, and a direct call is what lets
    // `record_assignment_and_heartbeat_reconcile_report_binding_execution_decisions`
    // hold a thread-local metrics recorder across the `.await` -- see that
    // test's own comment, copied from `src/api/grpc/tests.rs`'s established
    // pattern.

    use std::collections::HashMap as StdHashMap;

    use crate::binding_store::{BindingStoreSettings, InMemoryBindingStore};
    use crate::orchestrator::{
        BeganPause, DeadlineRenewalOutcome, HeldSandbox, MarkRunningOutcome, PausedRegistryError,
        PausedRegistryListEntry, PausedRegistryListing, PausedRegistryState, PausedSandboxEntry,
        ReclaimedHoldings, RegistryResult, ReleasedHoldings, ResumeClaim,
    };
    use crate::types::{ExecutionId, SandboxId};

    /// A minimal `PausedSandboxRegistry` test double: `get`/
    /// `is_cluster_backed` answer from a fixed table, everything else
    /// panics. `lookup_node`'s pure logic never calls the other eleven
    /// methods -- a test that (incorrectly) drove this deep enough to need
    /// one fails loudly instead of silently getting a made-up default.
    struct FakePausedRegistry {
        entries: StdHashMap<SandboxId, PausedSandboxEntry>,
        cluster_backed: bool,
        erroring: bool,
    }

    impl FakePausedRegistry {
        fn with_entry(entry: PausedSandboxEntry, cluster_backed: bool) -> Self {
            let mut entries = StdHashMap::new();
            entries.insert(entry.sandbox_id, entry);
            Self {
                entries,
                cluster_backed,
                erroring: false,
            }
        }

        fn erroring() -> Self {
            Self {
                entries: StdHashMap::new(),
                cluster_backed: true,
                erroring: true,
            }
        }
    }

    #[tonic::async_trait]
    impl PausedSandboxRegistry for FakePausedRegistry {
        async fn get(&self, sandbox_id: &SandboxId) -> RegistryResult<Option<PausedSandboxEntry>> {
            if self.erroring {
                return Err(PausedRegistryError::Backend {
                    operation: "test",
                    source: anyhow::anyhow!("boom"),
                });
            }
            Ok(self.entries.get(sandbox_id).cloned())
        }

        fn is_cluster_backed(&self) -> bool {
            self.cluster_backed
        }

        async fn begin_pause(&self, _entry: &PausedSandboxEntry) -> RegistryResult<BeganPause> {
            unimplemented!("lookup_node never calls this")
        }
        async fn complete_pause(
            &self,
            _sandbox_id: &SandboxId,
            _generation: i64,
            _snapshot_id: &crate::snapshot::SnapshotId,
        ) -> RegistryResult<()> {
            unimplemented!("lookup_node never calls this")
        }
        async fn mark_local_only(
            &self,
            _sandbox_id: &SandboxId,
            _generation: i64,
        ) -> RegistryResult<()> {
            unimplemented!("lookup_node never calls this")
        }
        async fn get_many(
            &self,
            _sandbox_ids: &[SandboxId],
        ) -> RegistryResult<StdHashMap<SandboxId, PausedSandboxEntry>> {
            unimplemented!("lookup_node never calls this")
        }
        async fn claim_for_resume(
            &self,
            _sandbox_id: &SandboxId,
            _node_id: &str,
            _execution_id: ExecutionId,
        ) -> RegistryResult<ResumeClaim> {
            unimplemented!("lookup_node never calls this")
        }
        async fn release_claim(
            &self,
            _sandbox_id: &SandboxId,
            _generation: i64,
        ) -> RegistryResult<bool> {
            unimplemented!("lookup_node never calls this")
        }
        async fn renew_lease(&self, _node_id: &str, _held: &[HeldSandbox]) -> RegistryResult<u64> {
            unimplemented!("lookup_node never calls this")
        }
        async fn reclaim_expired_holdings(&self) -> RegistryResult<ReclaimedHoldings> {
            unimplemented!("lookup_node never calls this")
        }
        async fn mark_running(
            &self,
            _sandbox_id: &SandboxId,
            _node_id: &str,
            _holder_node_id: &str,
            _execution_id: ExecutionId,
            _expires_at: Option<SystemTime>,
        ) -> RegistryResult<MarkRunningOutcome> {
            unimplemented!("lookup_node never calls this")
        }
        async fn renew_sandbox_deadline(
            &self,
            _sandbox_id: &SandboxId,
            _execution_id: ExecutionId,
            _expires_at: Option<SystemTime>,
        ) -> RegistryResult<DeadlineRenewalOutcome> {
            unimplemented!("lookup_node never calls this")
        }
        async fn release_node_holdings(&self, _node_id: &str) -> RegistryResult<ReleasedHoldings> {
            unimplemented!("lookup_node never calls this")
        }
        async fn remove(&self, _sandbox_id: &SandboxId, _generation: i64) -> RegistryResult<bool> {
            unimplemented!("lookup_node never calls this")
        }
        async fn list_all(&self) -> RegistryResult<PausedRegistryListing> {
            unimplemented!("lookup_node never calls this")
        }
    }

    fn paused_entry(
        sandbox_id: SandboxId,
        state: PausedRegistryState,
        origin_node_id: &str,
        execution_id: Option<ExecutionId>,
    ) -> PausedSandboxEntry {
        let now = chrono::Utc::now();
        PausedSandboxEntry {
            sandbox_id,
            cluster_id: uuid::Uuid::nil(),
            state,
            generation: 1,
            origin_node_id: origin_node_id.to_string(),
            claimed_by_node_id: None,
            snapshot_id: None,
            metadata: None,
            execution_id,
            paused_at: now,
            updated_at: now,
        }
    }

    /// A gate that is already warm regardless of when `warmed_up` is
    /// called: its deadline is anchored at the Unix epoch, which every
    /// `SystemTime::now()` a test observes is already past -- mirrors
    /// `src/node_client/native_placement.rs`'s own `warm_gate` helper.
    ///
    /// 🔴 P1: `reported_in` is called explicitly, once, right here. Since
    /// `WarmupGate::warmed_up`'s fix (a wall-clock deadline alone can no
    /// longer latch `warm` on a registry that has received zero
    /// heartbeats — see that method's own doc), a gate this helper hands
    /// out has to actually report in at least once to go warm at all; the
    /// already-past deadline above is what makes that one report latch
    /// `warm` for good immediately, rather than requiring every node
    /// discovery knows about to also be observed.
    fn warm_gate(registry: &Arc<AtomicNodeRegistry>) -> Arc<WarmupGate> {
        let gate = Arc::new(WarmupGate::new(
            Arc::clone(registry) as Arc<dyn NodeRegistry>,
            Duration::from_secs(1),
            SystemTime::UNIX_EPOCH,
        ));
        gate.reported_in(SystemTime::now());
        gate
    }

    /// A gate that stays cold for the lifetime of a test: an hour-long
    /// timeout anchored at "now," with no heartbeat ever fed to it.
    fn cold_gate(registry: &Arc<AtomicNodeRegistry>) -> Arc<WarmupGate> {
        Arc::new(WarmupGate::new(
            Arc::clone(registry) as Arc<dyn NodeRegistry>,
            Duration::from_secs(3600),
            SystemTime::now(),
        ))
    }

    fn in_memory_binding_store() -> Arc<dyn BindingStore> {
        Arc::new(InMemoryBindingStore::new(BindingStoreSettings::default()))
    }

    /// A minimal `PausedSandboxRegistry` test double for
    /// `list_registry_sandboxes`: `list_all`/`is_cluster_backed` answer from
    /// a fixed listing, everything else panics -- mirrors `FakePausedRegistry`'s
    /// own shape/doc for `lookup_node` above: one RPC group per fake, so a
    /// test that (incorrectly) drives this one into a write path fails
    /// loudly instead of silently getting a made-up default.
    struct FakeListingRegistry {
        listing: PausedRegistryListing,
        cluster_backed: bool,
        erroring: bool,
    }

    impl FakeListingRegistry {
        fn new(
            sandboxes: Vec<PausedRegistryListEntry>,
            now: chrono::DateTime<chrono::Utc>,
        ) -> Self {
            Self {
                listing: PausedRegistryListing { sandboxes, now },
                cluster_backed: true,
                erroring: false,
            }
        }

        fn not_cluster_backed() -> Self {
            Self {
                listing: PausedRegistryListing {
                    sandboxes: Vec::new(),
                    now: chrono::Utc::now(),
                },
                cluster_backed: false,
                erroring: false,
            }
        }

        fn erroring() -> Self {
            Self {
                listing: PausedRegistryListing {
                    sandboxes: Vec::new(),
                    now: chrono::Utc::now(),
                },
                cluster_backed: true,
                erroring: true,
            }
        }
    }

    #[tonic::async_trait]
    impl PausedSandboxRegistry for FakeListingRegistry {
        async fn list_all(&self) -> RegistryResult<PausedRegistryListing> {
            // 🔴 The RPC must never reach here while `cluster_backed` is
            // `false` -- it has to answer `FailedPrecondition` straight off
            // the `is_cluster_backed` gate instead. A mutant that dropped or
            // inverted that guard would sail past every assertion in
            // `list_registry_sandboxes_...not_cluster_backed...` if this
            // just quietly answered too; panicking here turns "the guard
            // was skipped" into a hard test failure instead of a passing
            // test with the wrong reason.
            assert!(
                self.cluster_backed,
                "list_registry_sandboxes must gate on is_cluster_backed before calling list_all"
            );
            if self.erroring {
                return Err(PausedRegistryError::Backend {
                    operation: "test",
                    source: anyhow::anyhow!("boom"),
                });
            }
            Ok(self.listing.clone())
        }

        fn is_cluster_backed(&self) -> bool {
            self.cluster_backed
        }

        async fn get(&self, _sandbox_id: &SandboxId) -> RegistryResult<Option<PausedSandboxEntry>> {
            unimplemented!("list_registry_sandboxes never calls this")
        }
        async fn begin_pause(&self, _entry: &PausedSandboxEntry) -> RegistryResult<BeganPause> {
            unimplemented!("list_registry_sandboxes never calls this")
        }
        async fn complete_pause(
            &self,
            _sandbox_id: &SandboxId,
            _generation: i64,
            _snapshot_id: &crate::snapshot::SnapshotId,
        ) -> RegistryResult<()> {
            unimplemented!("list_registry_sandboxes never calls this")
        }
        async fn mark_local_only(
            &self,
            _sandbox_id: &SandboxId,
            _generation: i64,
        ) -> RegistryResult<()> {
            unimplemented!("list_registry_sandboxes never calls this")
        }
        async fn get_many(
            &self,
            _sandbox_ids: &[SandboxId],
        ) -> RegistryResult<StdHashMap<SandboxId, PausedSandboxEntry>> {
            unimplemented!("list_registry_sandboxes never calls this")
        }
        async fn claim_for_resume(
            &self,
            _sandbox_id: &SandboxId,
            _node_id: &str,
            _execution_id: ExecutionId,
        ) -> RegistryResult<ResumeClaim> {
            unimplemented!("list_registry_sandboxes never calls this")
        }
        async fn release_claim(
            &self,
            _sandbox_id: &SandboxId,
            _generation: i64,
        ) -> RegistryResult<bool> {
            unimplemented!("list_registry_sandboxes never calls this")
        }
        async fn renew_lease(&self, _node_id: &str, _held: &[HeldSandbox]) -> RegistryResult<u64> {
            unimplemented!("list_registry_sandboxes never calls this")
        }
        async fn reclaim_expired_holdings(&self) -> RegistryResult<ReclaimedHoldings> {
            unimplemented!("list_registry_sandboxes never calls this")
        }
        async fn mark_running(
            &self,
            _sandbox_id: &SandboxId,
            _node_id: &str,
            _holder_node_id: &str,
            _execution_id: ExecutionId,
            _expires_at: Option<SystemTime>,
        ) -> RegistryResult<MarkRunningOutcome> {
            unimplemented!("list_registry_sandboxes never calls this")
        }
        async fn renew_sandbox_deadline(
            &self,
            _sandbox_id: &SandboxId,
            _execution_id: ExecutionId,
            _expires_at: Option<SystemTime>,
        ) -> RegistryResult<DeadlineRenewalOutcome> {
            unimplemented!("list_registry_sandboxes never calls this")
        }
        async fn release_node_holdings(&self, _node_id: &str) -> RegistryResult<ReleasedHoldings> {
            unimplemented!("list_registry_sandboxes never calls this")
        }
        async fn remove(&self, _sandbox_id: &SandboxId, _generation: i64) -> RegistryResult<bool> {
            unimplemented!("list_registry_sandboxes never calls this")
        }
    }

    fn list_entry(
        sandbox_id: SandboxId,
        state: PausedRegistryState,
        origin_node_id: &str,
        generation: i64,
    ) -> PausedRegistryListEntry {
        let now = chrono::Utc::now();
        PausedRegistryListEntry {
            sandbox_id,
            cluster_id: uuid::Uuid::nil(),
            state,
            generation,
            origin_node_id: origin_node_id.to_string(),
            claimed_by_node_id: None,
            snapshot_id: Some(crate::snapshot::SnapshotId::generate()),
            paused_at: now,
            updated_at: now,
            lease_expires_at: None,
            sandbox_expires_at: None,
            execution_id: None,
        }
    }

    /// `status: Ready` is deliberate, not incidental: `schedulable_node`
    /// (the `Publishing`/`LocalOnly` branch) also runs `filter_unschedulable`
    /// on top of freshness, and a heartbeat with no snapshot at all
    /// defaults to `Connecting`, which `can_accept_new_requests()` refuses --
    /// every test that wants a node to be *schedulable*, not merely
    /// *reporting*, needs this.
    fn heartbeat_req(node_id: &str, roster: Vec<(&str, &str)>) -> HeartbeatRequest {
        HeartbeatRequest {
            node_id: node_id.to_string(),
            cluster_id: "cluster-a".to_string(),
            service_instance_id: format!("{node_id}-instance"),
            snapshot: Some(scheduler::NodeSnapshot {
                status: scheduler::NodeStatus::Ready as i32,
                ..Default::default()
            }),
            roster: roster
                .into_iter()
                .map(|(sandbox_id, execution_id)| scheduler::SandboxRosterEntry {
                    sandbox_id: sandbox_id.to_string(),
                    execution_id: execution_id.to_string(),
                    projection_ttl_secs: 0,
                })
                .collect(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn schedule_places_a_node_with_nothing_else_wired_and_round_robins() {
        // The exact `service_on_a_socket(vec![])`-style bare service the
        // now-removed `every_other_stages_rpc_is_unimplemented_not_silently_accepted`
        // used to prove `Unimplemented` for -- `Schedule` now works with
        // none of `with_binding_store`/`with_paused_registry` ever called,
        // matching Go's own `Schedule`, which never touches either.
        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![
                node("node-a", "http://10.0.0.1:8000"),
                node("node-b", "http://10.0.0.2:8000"),
            ],
            Duration::from_secs(30),
        ));
        let service = NodeRegistryGrpcService::new(Arc::clone(&registry), warm_gate(&registry));

        let mut seen = Vec::new();
        for _ in 0..2 {
            let resp = service
                .schedule(Request::new(ScheduleRequest { hint: None }))
                .await
                .expect("two discovered nodes, nothing wired")
                .into_inner();
            seen.push(resp.node.expect("a node was chosen").node_id);
        }
        seen.sort();
        assert_eq!(
            seen,
            vec!["node-a".to_string(), "node-b".to_string()],
            "round-robin over two calls must visit both nodes exactly once"
        );
    }

    #[tokio::test]
    async fn schedule_returns_unavailable_when_no_nodes_are_discovered() {
        let registry = Arc::new(AtomicNodeRegistry::new(vec![], Duration::from_secs(30)));
        let service = NodeRegistryGrpcService::new(Arc::clone(&registry), warm_gate(&registry));

        let status = service
            .schedule(Request::new(ScheduleRequest { hint: None }))
            .await
            .expect_err("no nodes at all");
        assert_eq!(status.code(), tonic::Code::Unavailable);
    }

    #[tokio::test]
    async fn lookup_node_without_a_binding_store_is_unimplemented() {
        let registry = Arc::new(AtomicNodeRegistry::new(vec![], Duration::from_secs(30)));
        let service = NodeRegistryGrpcService::new(Arc::clone(&registry), warm_gate(&registry));

        let status = service
            .lookup_node(Request::new(LookupNodeRequest {
                sandbox_id: SandboxId::new().to_string(),
            }))
            .await
            .expect_err("no binding store wired");
        assert_eq!(status.code(), tonic::Code::Unimplemented);
    }

    #[tokio::test]
    async fn lookup_node_invalid_argument_for_an_empty_sandbox_id() {
        let registry = Arc::new(AtomicNodeRegistry::new(vec![], Duration::from_secs(30)));
        let service = NodeRegistryGrpcService::new(Arc::clone(&registry), warm_gate(&registry))
            .with_binding_store(in_memory_binding_store(), false, Duration::ZERO);

        let status = service
            .lookup_node(Request::new(LookupNodeRequest {
                sandbox_id: "   ".to_string(),
            }))
            .await
            .expect_err("blank sandbox_id");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn lookup_node_answers_bound_from_the_binding_store() {
        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![node("node-a", "http://10.0.0.1:8000")],
            Duration::from_secs(30),
        ));
        let binding_store = in_memory_binding_store();
        let sandbox_id = SandboxId::new();
        let execution_id = ExecutionId::new().to_string();
        binding_store
            .record(
                &sandbox_id.to_string(),
                Binding {
                    node: node("node-a", "http://10.0.0.1:8000"),
                    execution_id: execution_id.clone(),
                    projection_ttl: Duration::ZERO,
                },
                SystemTime::now(),
            )
            .await
            .expect("install the binding");
        let service = NodeRegistryGrpcService::new(Arc::clone(&registry), warm_gate(&registry))
            .with_binding_store(binding_store, false, Duration::ZERO);

        let resp = service
            .lookup_node(Request::new(LookupNodeRequest {
                sandbox_id: sandbox_id.to_string(),
            }))
            .await
            .expect("a binding exists")
            .into_inner();
        assert_eq!(resp.node.as_ref().unwrap().node_id, "node-a");
        assert_eq!(resp.location(), scheduler::SandboxLocation::Bound);
        assert_eq!(resp.execution_id, execution_id);
        assert_eq!(
            resp.execution_authority(),
            scheduler::ExecutionAuthority::Registry
        );
    }

    #[tokio::test]
    async fn lookup_node_falls_back_to_the_roster_when_the_binding_store_misses() {
        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![node("node-a", "http://10.0.0.1:8000")],
            Duration::from_secs(30),
        ));
        let sandbox_id = SandboxId::new();
        registry
            .heartbeat(
                &heartbeat_req("node-a", vec![(&sandbox_id.to_string(), "")]),
                SystemTime::now(),
            )
            .expect("node-a is in discovery");
        let service = NodeRegistryGrpcService::new(Arc::clone(&registry), warm_gate(&registry))
            .with_binding_store(in_memory_binding_store(), false, Duration::ZERO);

        let resp = service
            .lookup_node(Request::new(LookupNodeRequest {
                sandbox_id: sandbox_id.to_string(),
            }))
            .await
            .expect("no binding, but node-a's roster lists it")
            .into_inner();
        assert_eq!(resp.node.as_ref().unwrap().node_id, "node-a");
        assert_eq!(resp.location(), scheduler::SandboxLocation::Bound);
    }

    /// 🔴 Ports `Service.rosterPrefers`'s `default: execution > bestExecution`
    /// arm: two nodes both report the same sandbox, both fresh, with
    /// different named incarnations -- the lexicographically greater one
    /// (execution ids are UUIDv7, so this is "newer") must win regardless
    /// of iteration order. Proven both ways (`node-a`/`node-b` each go
    /// first once) so an implementation that just "picks the first
    /// reporter" cannot pass by accident.
    #[tokio::test]
    async fn lookup_node_roster_prefers_the_lexicographically_newer_incarnation() {
        let sandbox_id = SandboxId::new();
        // `nodes_holding` always iterates in sorted node-id order (node-a
        // before node-b), regardless of which one actually heartbeated
        // first -- so proving the winner tracks the *incarnation*, not
        // iteration position, means running this with the newer incarnation
        // on each side of that fixed order in turn.
        for (lower, higher) in [("node-a", "node-b"), ("node-b", "node-a")] {
            let registry = Arc::new(AtomicNodeRegistry::new(
                vec![
                    node("node-a", "http://10.0.0.1:8000"),
                    node("node-b", "http://10.0.0.2:8000"),
                ],
                Duration::from_secs(30),
            ));
            let now = SystemTime::now();
            registry
                .heartbeat(
                    &heartbeat_req(
                        lower,
                        vec![(
                            &sandbox_id.to_string(),
                            "00000000-0000-7000-8000-000000000001",
                        )],
                    ),
                    now,
                )
                .expect(lower);
            registry
                .heartbeat(
                    &heartbeat_req(
                        higher,
                        vec![(
                            &sandbox_id.to_string(),
                            "00000000-0000-7000-8000-000000000002",
                        )],
                    ),
                    now,
                )
                .expect(higher);
            let service = NodeRegistryGrpcService::new(Arc::clone(&registry), warm_gate(&registry))
                .with_binding_store(in_memory_binding_store(), false, Duration::ZERO);

            let resp = service
                .lookup_node(Request::new(LookupNodeRequest {
                    sandbox_id: sandbox_id.to_string(),
                }))
                .await
                .expect("a live roster hit")
                .into_inner();
            // "...0002" > "...0001" lexicographically: whichever node
            // reported it (`higher`) must win, whether that is the node
            // `nodes_holding` visits first (node-a) or second (node-b).
            assert_eq!(
                resp.node.as_ref().unwrap().node_id,
                higher,
                "lower={lower} higher={higher}: the newer incarnation must win regardless of \
                 iteration order"
            );
            assert_eq!(resp.execution_id, "00000000-0000-7000-8000-000000000002");
        }
    }

    #[tokio::test]
    async fn lookup_node_withholds_not_found_while_the_registry_is_cold() {
        let registry = Arc::new(AtomicNodeRegistry::new(vec![], Duration::from_secs(30)));
        let service = NodeRegistryGrpcService::new(Arc::clone(&registry), cold_gate(&registry))
            .with_binding_store(in_memory_binding_store(), false, Duration::ZERO);

        let status = service
            .lookup_node(Request::new(LookupNodeRequest {
                sandbox_id: SandboxId::new().to_string(),
            }))
            .await
            .expect_err("nothing has ever reported in, and the deadline has not passed");
        assert_eq!(status.code(), tonic::Code::Unavailable);
    }

    #[tokio::test]
    async fn lookup_node_reports_not_found_once_warm_with_nothing_wired() {
        let registry = Arc::new(AtomicNodeRegistry::new(vec![], Duration::from_secs(30)));
        let service = NodeRegistryGrpcService::new(Arc::clone(&registry), warm_gate(&registry))
            .with_binding_store(in_memory_binding_store(), false, Duration::ZERO);

        let status = service
            .lookup_node(Request::new(LookupNodeRequest {
                sandbox_id: SandboxId::new().to_string(),
            }))
            .await
            .expect_err("no binding, no roster, no registry row");
        assert_eq!(status.code(), tonic::Code::NotFound);
    }

    /// 🔴 A registry that answers `Some(entry)` but is not cluster-backed
    /// must never reach the row -- the control that proves stage 3's
    /// `is_cluster_backed()` guard has teeth, not just the ordinary "no
    /// registry wired at all" path every other `NotFound` test exercises.
    #[tokio::test]
    async fn lookup_node_skips_a_registry_row_when_the_registry_is_not_cluster_backed() {
        let registry = Arc::new(AtomicNodeRegistry::new(vec![], Duration::from_secs(30)));
        let sandbox_id = SandboxId::new();
        let paused: Arc<dyn PausedSandboxRegistry> = Arc::new(FakePausedRegistry::with_entry(
            paused_entry(sandbox_id, PausedRegistryState::Paused, "node-a", None),
            /* cluster_backed */ false,
        ));
        let service = NodeRegistryGrpcService::new(Arc::clone(&registry), warm_gate(&registry))
            .with_binding_store(in_memory_binding_store(), false, Duration::ZERO)
            .with_paused_registry(paused);

        let status = service
            .lookup_node(Request::new(LookupNodeRequest {
                sandbox_id: sandbox_id.to_string(),
            }))
            .await
            .expect_err("the registry has a row, but it may not be trusted");
        assert_eq!(status.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn lookup_node_reports_unavailable_when_the_paused_registry_errors() {
        let registry = Arc::new(AtomicNodeRegistry::new(vec![], Duration::from_secs(30)));
        let paused: Arc<dyn PausedSandboxRegistry> = Arc::new(FakePausedRegistry::erroring());
        let service = NodeRegistryGrpcService::new(Arc::clone(&registry), warm_gate(&registry))
            .with_binding_store(in_memory_binding_store(), false, Duration::ZERO)
            .with_paused_registry(paused);

        let status = service
            .lookup_node(Request::new(LookupNodeRequest {
                sandbox_id: SandboxId::new().to_string(),
            }))
            .await
            .expect_err("the registry could not be read");
        assert_eq!(status.code(), tonic::Code::Unavailable);
    }

    /// 🔴 `Paused` never needs a heartbeat at all: `select_node` (the same
    /// pipeline `Schedule` runs) only requires the origin to be in
    /// discovery, not to have reported. A version that required liveness
    /// here would refuse every paused sandbox for a full report interval
    /// after every scheduler restart -- exactly the asymmetry
    /// `lookup.go`'s own comment on `schedulableNode` calls out.
    #[tokio::test]
    async fn lookup_node_places_a_paused_sandbox_preferring_its_origin_node() {
        let sandbox_id = SandboxId::new();
        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![
                node("node-a", "http://10.0.0.1:8000"),
                node("node-b", "http://10.0.0.2:8000"),
            ],
            Duration::from_secs(30),
        ));
        let paused: Arc<dyn PausedSandboxRegistry> = Arc::new(FakePausedRegistry::with_entry(
            paused_entry(sandbox_id, PausedRegistryState::Paused, "node-b", None),
            true,
        ));
        let service = NodeRegistryGrpcService::new(Arc::clone(&registry), warm_gate(&registry))
            .with_binding_store(in_memory_binding_store(), false, Duration::ZERO)
            .with_paused_registry(paused);

        let resp = service
            .lookup_node(Request::new(LookupNodeRequest {
                sandbox_id: sandbox_id.to_string(),
            }))
            .await
            .expect("a paused row, no heartbeat needed")
            .into_inner();
        assert_eq!(
            resp.node.as_ref().unwrap().node_id,
            "node-b",
            "origin is preferred"
        );
        assert_eq!(resp.location(), scheduler::SandboxLocation::Placed);
        assert_eq!(resp.origin_node_id, "node-b");
        assert_eq!(resp.execution_id, "", "PLACED never carries an incarnation");
        assert_eq!(
            resp.execution_authority(),
            scheduler::ExecutionAuthority::Pending
        );
    }

    #[tokio::test]
    async fn lookup_node_pins_a_publishing_sandbox_to_a_live_schedulable_origin() {
        let sandbox_id = SandboxId::new();
        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![node("node-a", "http://10.0.0.1:8000")],
            Duration::from_secs(30),
        ));
        registry
            .heartbeat(&heartbeat_req("node-a", vec![]), SystemTime::now())
            .expect("node-a is in discovery");
        let paused: Arc<dyn PausedSandboxRegistry> = Arc::new(FakePausedRegistry::with_entry(
            paused_entry(sandbox_id, PausedRegistryState::Publishing, "node-a", None),
            true,
        ));
        let service = NodeRegistryGrpcService::new(Arc::clone(&registry), warm_gate(&registry))
            .with_binding_store(in_memory_binding_store(), false, Duration::ZERO)
            .with_paused_registry(paused);

        let resp = service
            .lookup_node(Request::new(LookupNodeRequest {
                sandbox_id: sandbox_id.to_string(),
            }))
            .await
            .expect("origin is live and schedulable")
            .into_inner();
        assert_eq!(resp.node.as_ref().unwrap().node_id, "node-a");
        assert_eq!(resp.location(), scheduler::SandboxLocation::Pinned);
        assert_eq!(
            resp.execution_authority(),
            scheduler::ExecutionAuthority::Pending
        );
    }

    #[tokio::test]
    async fn lookup_node_refuses_a_local_only_sandbox_when_origin_is_not_reporting() {
        let sandbox_id = SandboxId::new();
        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![node("node-a", "http://10.0.0.1:8000")],
            Duration::from_secs(30),
        ));
        // node-a is discovered but has never heartbeated -- no roster at all.
        let paused: Arc<dyn PausedSandboxRegistry> = Arc::new(FakePausedRegistry::with_entry(
            paused_entry(sandbox_id, PausedRegistryState::LocalOnly, "node-a", None),
            true,
        ));
        let service = NodeRegistryGrpcService::new(Arc::clone(&registry), warm_gate(&registry))
            .with_binding_store(in_memory_binding_store(), false, Duration::ZERO)
            .with_paused_registry(paused);

        let status = service
            .lookup_node(Request::new(LookupNodeRequest {
                sandbox_id: sandbox_id.to_string(),
            }))
            .await
            .expect_err("origin has never reported");
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert!(
            status.message().contains("not reporting"),
            "{}",
            status.message()
        );
    }

    #[tokio::test]
    async fn lookup_node_answers_bound_from_a_running_registry_row() {
        let sandbox_id = SandboxId::new();
        let execution_id = ExecutionId::new();
        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![node("node-a", "http://10.0.0.1:8000")],
            Duration::from_secs(30),
        ));
        registry
            .heartbeat(&heartbeat_req("node-a", vec![]), SystemTime::now())
            .expect("node-a is in discovery");
        let paused: Arc<dyn PausedSandboxRegistry> = Arc::new(FakePausedRegistry::with_entry(
            paused_entry(
                sandbox_id,
                PausedRegistryState::Running,
                "node-a",
                Some(execution_id),
            ),
            true,
        ));
        let service = NodeRegistryGrpcService::new(Arc::clone(&registry), warm_gate(&registry))
            .with_binding_store(in_memory_binding_store(), false, Duration::ZERO)
            .with_paused_registry(paused);

        let resp = service
            .lookup_node(Request::new(LookupNodeRequest {
                sandbox_id: sandbox_id.to_string(),
            }))
            .await
            .expect("the holder is live")
            .into_inner();
        assert_eq!(resp.node.as_ref().unwrap().node_id, "node-a");
        assert_eq!(resp.location(), scheduler::SandboxLocation::Bound);
        assert_eq!(resp.execution_id, execution_id.to_string());
        assert_eq!(
            resp.execution_authority(),
            scheduler::ExecutionAuthority::Registry
        );
    }

    #[tokio::test]
    async fn lookup_node_refuses_a_running_row_when_the_holder_is_unreachable_and_warm() {
        let sandbox_id = SandboxId::new();
        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![node("node-a", "http://10.0.0.1:8000")],
            Duration::from_secs(30),
        ));
        // node-a is discovered but has never heartbeated.
        let paused: Arc<dyn PausedSandboxRegistry> = Arc::new(FakePausedRegistry::with_entry(
            paused_entry(sandbox_id, PausedRegistryState::Running, "node-a", None),
            true,
        ));
        let service = NodeRegistryGrpcService::new(Arc::clone(&registry), warm_gate(&registry))
            .with_binding_store(in_memory_binding_store(), false, Duration::ZERO)
            .with_paused_registry(paused);

        let status = service
            .lookup_node(Request::new(LookupNodeRequest {
                sandbox_id: sandbox_id.to_string(),
            }))
            .await
            .expect_err("the holder has never reported, and the gate is warm");
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    }

    /// 🔴 The same `Running` row as the previous test, but with a cold gate:
    /// the holder being unreachable must not be asserted as a
    /// `FailedPrecondition` fact while bindings are still being seeded --
    /// it has to come back `Unavailable` (retryable) instead, exactly like
    /// an ordinary `NotFound` would under the same cold gate.
    #[tokio::test]
    async fn lookup_node_withholds_a_running_rows_holder_unreachable_verdict_while_cold() {
        let sandbox_id = SandboxId::new();
        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![node("node-a", "http://10.0.0.1:8000")],
            Duration::from_secs(30),
        ));
        let paused: Arc<dyn PausedSandboxRegistry> = Arc::new(FakePausedRegistry::with_entry(
            paused_entry(sandbox_id, PausedRegistryState::Running, "node-a", None),
            true,
        ));
        let service = NodeRegistryGrpcService::new(Arc::clone(&registry), cold_gate(&registry))
            .with_binding_store(in_memory_binding_store(), false, Duration::ZERO)
            .with_paused_registry(paused);

        let status = service
            .lookup_node(Request::new(LookupNodeRequest {
                sandbox_id: sandbox_id.to_string(),
            }))
            .await
            .expect_err("cold, so this must not be asserted as a fact yet");
        assert_eq!(status.code(), tonic::Code::Unavailable);
    }

    /// 🔴 Task's own independent metric wiring: `RecordAssignment`'s
    /// `binding_store.record` and `Heartbeat`'s `binding_store.reconcile_node`
    /// both used to discard the `Ok(BindingDecision)` they got back. This
    /// proves both call sites now report `agentenv_api_binding_execution_total`
    /// under their own `source` label, using the exact thread-local-recorder
    /// pattern `src/api/grpc/tests.rs` established (`with_local_recorder`
    /// only works for a full async round trip when the handler runs on the
    /// calling thread, which calling the service directly guarantees and a
    /// real socket would not).
    #[tokio::test]
    async fn record_assignment_and_heartbeat_reconcile_report_binding_execution_decisions() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let guard = metrics::set_default_local_recorder(&recorder);

        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![node("node-a", "http://10.0.0.1:8000")],
            Duration::from_secs(30),
        ));
        let binding_store = in_memory_binding_store();
        let service = NodeRegistryGrpcService::new(Arc::clone(&registry), warm_gate(&registry))
            .with_binding_store(Arc::clone(&binding_store), false, Duration::ZERO);

        let sandbox_id = SandboxId::new().to_string();
        service
            .record_assignment(Request::new(RecordAssignmentRequest {
                sandbox_id: sandbox_id.clone(),
                node: Some(scheduler::Node {
                    node_id: "node-a".to_string(),
                    endpoint: "http://10.0.0.1:8000".to_string(),
                }),
                execution_id: String::new(),
                projection_ttl_secs: 0,
            }))
            .await
            .expect("record_assignment succeeds");

        service
            .heartbeat(Request::new(heartbeat_req(
                "node-a",
                vec![(&sandbox_id, "")],
            )))
            .await
            .expect("heartbeat succeeds");

        drop(guard);
        let mut by_source: StdHashMap<String, u64> = StdHashMap::new();
        for (composite, _unit, _description, value) in snapshotter.snapshot().into_vec() {
            let key = composite.key();
            if key.name() != "agentenv_api_binding_execution_total" {
                continue;
            }
            if let DebugValue::Counter(count) = value {
                if let Some(source) = key.labels().find(|label| label.key() == "source") {
                    *by_source.entry(source.value().to_string()).or_insert(0) += count;
                }
            }
        }

        assert!(
            by_source.get("assignment").copied().unwrap_or(0) >= 1,
            "record_assignment must report a decision under source=assignment: {by_source:?}"
        );
        assert!(
            by_source.get("heartbeat").copied().unwrap_or(0) >= 1,
            "heartbeat's reconcile_node must report a decision under source=heartbeat: {by_source:?}"
        );
    }

    // ---- list_registry_sandboxes: ports `listRegistrySandboxes`
    //      (`service.go:849-943`) against `PausedSandboxRegistry::list_all`.
    //      Every test below calls the service directly, mirroring the
    //      `Schedule`/`LookupNode` section's own reasoning: there is
    //      nothing gRPC-transport-specific left to prove once this file's
    //      socket-based tests elsewhere already cover server wiring. ----

    fn fixed_sandbox_id(n: u8) -> SandboxId {
        SandboxId::parse_str(&format!("00000000-0000-0000-0000-{n:012x}"))
            .expect("a well-formed fixed uuid")
    }

    #[tokio::test]
    async fn list_registry_sandboxes_without_a_paused_registry_is_failed_precondition() {
        let registry = Arc::new(AtomicNodeRegistry::new(vec![], Duration::from_secs(30)));
        let service = NodeRegistryGrpcService::new(Arc::clone(&registry), warm_gate(&registry));

        let status = service
            .list_registry_sandboxes(Request::new(ListRegistrySandboxesRequest::default()))
            .await
            .expect_err("no paused registry wired");
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    }

    /// Mirrors Go's own default: `Service.registry` is never a literal nil
    /// pointer, it defaults to `pausedregistry.Disabled()`, whose `List`
    /// answers `ErrDisabled` (-> `FailedPrecondition`) exactly the same way
    /// a registry that is wired but not cluster-backed does here. The fake's
    /// own `list_all` panics if this ever reaches it -- see its doc.
    #[tokio::test]
    async fn list_registry_sandboxes_with_a_non_cluster_backed_registry_is_failed_precondition() {
        let registry = Arc::new(AtomicNodeRegistry::new(vec![], Duration::from_secs(30)));
        let service = NodeRegistryGrpcService::new(Arc::clone(&registry), warm_gate(&registry))
            .with_paused_registry(Arc::new(FakeListingRegistry::not_cluster_backed()));

        let status = service
            .list_registry_sandboxes(Request::new(ListRegistrySandboxesRequest::default()))
            .await
            .expect_err("a non-cluster-backed registry must refuse, not answer empty");
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn list_registry_sandboxes_maps_a_backend_error_to_unavailable() {
        let registry = Arc::new(AtomicNodeRegistry::new(vec![], Duration::from_secs(30)));
        let service = NodeRegistryGrpcService::new(Arc::clone(&registry), warm_gate(&registry))
            .with_paused_registry(Arc::new(FakeListingRegistry::erroring()));

        let status = service
            .list_registry_sandboxes(Request::new(ListRegistrySandboxesRequest::default()))
            .await
            .expect_err("the backend failed");
        assert_eq!(status.code(), tonic::Code::Unavailable);
    }

    #[tokio::test]
    async fn list_registry_sandboxes_rejects_a_negative_page_size() {
        let registry = Arc::new(AtomicNodeRegistry::new(vec![], Duration::from_secs(30)));
        let service = NodeRegistryGrpcService::new(Arc::clone(&registry), warm_gate(&registry))
            .with_paused_registry(Arc::new(FakeListingRegistry::new(
                Vec::new(),
                chrono::Utc::now(),
            )));

        let status = service
            .list_registry_sandboxes(Request::new(ListRegistrySandboxesRequest {
                page_size: -1,
                ..Default::default()
            }))
            .await
            .expect_err("negative page_size");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    /// Argument validation must run before the registry is even consulted --
    /// mirrors Go's own comment on `parseRegistryStateFilter` running first:
    /// the answer to a bad request must not depend on whether a registry
    /// happens to be configured. Proved here by using the exact same
    /// "no paused registry wired" service the `FailedPrecondition` test
    /// above uses: an unknown state must still come back `InvalidArgument`,
    /// never `FailedPrecondition`.
    #[tokio::test]
    async fn list_registry_sandboxes_rejects_an_unknown_state_before_consulting_the_registry() {
        let registry = Arc::new(AtomicNodeRegistry::new(vec![], Duration::from_secs(30)));
        let service = NodeRegistryGrpcService::new(Arc::clone(&registry), warm_gate(&registry));

        let status = service
            .list_registry_sandboxes(Request::new(ListRegistrySandboxesRequest {
                state: "not_a_real_state".to_string(),
                ..Default::default()
            }))
            .await
            .expect_err("unknown state");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(
            status.message().contains("not_a_real_state"),
            "the refusal must name the bad value, got: {}",
            status.message()
        );
    }

    #[tokio::test]
    async fn list_registry_sandboxes_returns_every_row_with_the_database_clock() {
        let registry = Arc::new(AtomicNodeRegistry::new(vec![], Duration::from_secs(30)));
        let now = chrono::Utc::now();
        let sandbox_id = fixed_sandbox_id(1);
        let entries = vec![list_entry(
            sandbox_id,
            PausedRegistryState::Running,
            "node-a",
            7,
        )];
        let service = NodeRegistryGrpcService::new(Arc::clone(&registry), warm_gate(&registry))
            .with_paused_registry(Arc::new(FakeListingRegistry::new(entries, now)));

        let resp = service
            .list_registry_sandboxes(Request::new(ListRegistrySandboxesRequest::default()))
            .await
            .expect("a cluster-backed registry with one row")
            .into_inner();

        assert_eq!(resp.sandboxes.len(), 1);
        let row = &resp.sandboxes[0];
        assert_eq!(row.sandbox_id, sandbox_id.to_string());
        assert_eq!(row.state, "running");
        assert_eq!(row.generation, 7);
        assert_eq!(row.origin_node_id, "node-a");
        assert_eq!(
            row.holder_node_id, "node-a",
            "holder_node_id must be origin_node_id, never claimed_by_node_id"
        );
        assert_eq!(resp.database_now_unix_ms, now.timestamp_millis());
        assert_eq!(resp.next_page_token, "");
    }

    #[tokio::test]
    async fn list_registry_sandboxes_filters_by_state_case_insensitively() {
        let registry = Arc::new(AtomicNodeRegistry::new(vec![], Duration::from_secs(30)));
        let entries = vec![
            list_entry(
                fixed_sandbox_id(1),
                PausedRegistryState::Paused,
                "node-a",
                1,
            ),
            list_entry(
                fixed_sandbox_id(2),
                PausedRegistryState::Running,
                "node-a",
                1,
            ),
            list_entry(
                fixed_sandbox_id(3),
                PausedRegistryState::LocalOnly,
                "node-a",
                1,
            ),
        ];
        let service = NodeRegistryGrpcService::new(Arc::clone(&registry), warm_gate(&registry))
            .with_paused_registry(Arc::new(FakeListingRegistry::new(
                entries,
                chrono::Utc::now(),
            )));

        let resp = service
            .list_registry_sandboxes(Request::new(ListRegistrySandboxesRequest {
                state: "RUNNING".to_string(),
                ..Default::default()
            }))
            .await
            .expect("a known state, differently cased")
            .into_inner();

        assert_eq!(resp.sandboxes.len(), 1, "only the running row must match");
        assert_eq!(
            resp.sandboxes[0].sandbox_id,
            fixed_sandbox_id(2).to_string()
        );
    }

    #[tokio::test]
    async fn list_registry_sandboxes_filters_by_node_id() {
        let registry = Arc::new(AtomicNodeRegistry::new(vec![], Duration::from_secs(30)));
        let entries = vec![
            list_entry(
                fixed_sandbox_id(1),
                PausedRegistryState::Running,
                "node-a",
                1,
            ),
            list_entry(
                fixed_sandbox_id(2),
                PausedRegistryState::Running,
                "node-b",
                1,
            ),
        ];
        let service = NodeRegistryGrpcService::new(Arc::clone(&registry), warm_gate(&registry))
            .with_paused_registry(Arc::new(FakeListingRegistry::new(
                entries,
                chrono::Utc::now(),
            )));

        let resp = service
            .list_registry_sandboxes(Request::new(ListRegistrySandboxesRequest {
                node_id: "node-b".to_string(),
                ..Default::default()
            }))
            .await
            .expect("a node_id filter")
            .into_inner();

        assert_eq!(resp.sandboxes.len(), 1);
        assert_eq!(
            resp.sandboxes[0].sandbox_id,
            fixed_sandbox_id(2).to_string()
        );
        assert_eq!(resp.sandboxes[0].origin_node_id, "node-b");
    }

    /// Keyset paging: `page_size` truncates the sorted (by sandbox id)
    /// listing and reports the last row's id as `next_page_token`; a
    /// second call with that token as `page_token` picks up exactly where
    /// the first left off. Sandbox ids are fixed (not `SandboxId::new()`)
    /// so the expected page split is deterministic rather than depending
    /// on UUIDv7 generation order within the same test.
    #[tokio::test]
    async fn list_registry_sandboxes_pages_with_a_token() {
        let registry = Arc::new(AtomicNodeRegistry::new(vec![], Duration::from_secs(30)));
        let entries = vec![
            list_entry(
                fixed_sandbox_id(3),
                PausedRegistryState::Running,
                "node-a",
                1,
            ),
            list_entry(
                fixed_sandbox_id(1),
                PausedRegistryState::Running,
                "node-a",
                1,
            ),
            list_entry(
                fixed_sandbox_id(2),
                PausedRegistryState::Running,
                "node-a",
                1,
            ),
        ];
        let service = NodeRegistryGrpcService::new(Arc::clone(&registry), warm_gate(&registry))
            .with_paused_registry(Arc::new(FakeListingRegistry::new(
                entries,
                chrono::Utc::now(),
            )));

        let first = service
            .list_registry_sandboxes(Request::new(ListRegistrySandboxesRequest {
                page_size: 2,
                ..Default::default()
            }))
            .await
            .expect("first page")
            .into_inner();
        assert_eq!(
            first
                .sandboxes
                .iter()
                .map(|s| s.sandbox_id.clone())
                .collect::<Vec<_>>(),
            vec![
                fixed_sandbox_id(1).to_string(),
                fixed_sandbox_id(2).to_string()
            ],
            "the first page must be the two lowest ids, sorted"
        );
        assert_eq!(first.next_page_token, fixed_sandbox_id(2).to_string());

        let second = service
            .list_registry_sandboxes(Request::new(ListRegistrySandboxesRequest {
                page_size: 2,
                page_token: first.next_page_token,
                ..Default::default()
            }))
            .await
            .expect("second page")
            .into_inner();
        assert_eq!(
            second
                .sandboxes
                .iter()
                .map(|s| s.sandbox_id.clone())
                .collect::<Vec<_>>(),
            vec![fixed_sandbox_id(3).to_string()],
            "the second page must hold exactly the row the first page did not"
        );
        assert_eq!(
            second.next_page_token, "",
            "the last page must report no further token"
        );
    }
}
