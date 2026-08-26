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
//! `schedule`/`lookup_node` need the binding store's write and the
//! three-stage lookup ladder respectively (still pending in this file —
//! see below); `record_assignment`/`record_p2p_artifact`/
//! `forget_p2p_artifact`/`lookup_p2p_artifact` **are** implemented (task's
//! own "D3"/"D4" — see below), each gated the same way
//! `report_sandbox_event` is. `list_p2p_peers` stays on the scheduler
//! permanently (`_sd-phase4-stageA-node-inventory.md` §7 risk 3 —
//! explicitly not to be "helpfully" ported alongside Stage A, and this
//! file's own test proves it still refuses); `list_registry_sandboxes` is
//! Stage C's. Every one of those returns `Status::unimplemented` naming
//! which Stage owns it, rather than silently accepting and doing nothing —
//! a caller that dials the wrong half of this split by mistake gets an
//! answer that says so.
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
use std::time::{Duration, SystemTime};

use tonic::{Request, Response, Status};

use crate::binding_store::artifact_index::ArtifactStore;
use crate::binding_store::{BindingDeleteOutcome, BindingStore};
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

use super::registry::{
    AtomicNodeRegistry, NodeNotInRegistry, NodeRegistry, ServiceInstanceMismatch,
};
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
        }
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
            if let Err(err) = binding_store.reconcile_node(node, roster, now).await {
                tracing::warn!(
                    node_id = %node_id,
                    error = %err,
                    "scheduler heartbeat binding reconcile failed"
                );
                return Err(Status::unavailable("binding store unavailable"));
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

    // ── Everything below belongs to another Stage. Each refusal names which
    // one, so a caller that dials the wrong half of the split by mistake
    // gets an answer that says so rather than silence or a stub 200.

    async fn schedule(
        &self,
        _request: Request<ScheduleRequest>,
    ) -> Result<Response<ScheduleResponse>, Status> {
        Err(Status::unimplemented(format!(
            "Schedule needs the Stage D binding store: {NOT_STAGE_A}"
        )))
    }

    async fn lookup_node(
        &self,
        _request: Request<LookupNodeRequest>,
    ) -> Result<Response<LookupNodeResponse>, Status> {
        Err(Status::unimplemented(format!(
            "LookupNode needs the Stage D binding store: {NOT_STAGE_A}"
        )))
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
        if let Err(err) = binding_store
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
            tracing::warn!(
                sandbox_id = %sandbox_id,
                error = %err,
                "scheduler record_assignment binding write failed"
            );
            return Err(Status::unavailable("binding store unavailable"));
        }
        Ok(Response::new(RecordAssignmentResponse {}))
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

    async fn list_p2p_peers(
        &self,
        _request: Request<ListP2pPeersRequest>,
    ) -> Result<Response<ListP2pPeersResponse>, Status> {
        Err(Status::unimplemented(format!(
            "ListP2pPeers stays on the scheduler with the rest of the P2P discovery group \
             (Stage A explicitly does not split this RPC group): {NOT_STAGE_A}"
        )))
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

    async fn list_registry_sandboxes(
        &self,
        _request: Request<ListRegistrySandboxesRequest>,
    ) -> Result<Response<ListRegistrySandboxesResponse>, Status> {
        Err(Status::unimplemented(format!(
            "ListRegistrySandboxes is Stage C's paused registry: {NOT_STAGE_A}"
        )))
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

    /// Every RPC this Stage does not own refuses with `Unimplemented` rather
    /// than a stub success — a caller that dialled the wrong half by mistake
    /// gets told so instead of silently getting nothing done.
    #[tokio::test]
    async fn every_other_stages_rpc_is_unimplemented_not_silently_accepted() {
        let (_registry, mut client, _stop) = service_on_a_socket(vec![]).await;

        let status = client
            .schedule(ScheduleRequest::default())
            .await
            .expect_err("Schedule is Stage D's");
        assert_eq!(status.code(), tonic::Code::Unimplemented);

        let status = client
            .lookup_node(LookupNodeRequest::default())
            .await
            .expect_err("LookupNode is Stage D's");
        assert_eq!(status.code(), tonic::Code::Unimplemented);

        let status = client
            .list_p2p_peers(ListP2pPeersRequest::default())
            .await
            .expect_err("ListP2pPeers stays on the scheduler");
        assert_eq!(status.code(), tonic::Code::Unimplemented);
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
}
