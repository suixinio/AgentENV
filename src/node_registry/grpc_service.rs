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
//! `schedule`/`lookup_node`/`record_assignment` need the binding store
//! (Stage D, still pending in this file — see below);
//! `list_p2p_peers`/`record_p2p_artifact`/`forget_p2p_artifact`/
//! `lookup_p2p_artifact` are the P2P discovery group
//! (`_sd-phase4-stageA-node-inventory.md` §7 risk 3 — explicitly not to be
//! "helpfully" ported alongside Stage A, `list_p2p_peers` stays on the
//! scheduler permanently, the artifact-index RPCs are still pending here);
//! `list_registry_sandboxes` is Stage C's. Every one of those returns
//! `Status::unimplemented` naming which Stage owns it, rather than silently
//! accepting and doing nothing — a caller that dials the wrong half of this
//! split by mistake gets an answer that says so.
//!
//! `report_sandbox_event` (task's own "D1") **is** implemented here now: it
//! ports `applyProjectionDelete` (`service.go:602-643`) against
//! [`crate::binding_store::BindingStore`], wired in via
//! [`NodeRegistryGrpcService::with_binding_store`]. Every default path that
//! does not call that builder method (every test that only calls `new`, and
//! `src/bin/server.rs`'s real wiring until the config/assembly commit that
//! follows this one) keeps answering `Unimplemented`, unchanged.
//!
//! # 🔴 What `Heartbeat`/`UnregisterNode` still do *not* do (task's own
//! "D3"/"D6" — `_sd-phase4-stageA-node-inventory.md` §7 risk 1, §10)
//!
//! The real Go `Heartbeat` calls `s.store.ReconcileNode(node, roster, now)`
//! right after `s.nodes.Heartbeat(...)` succeeds, and `UnregisterNode` calls
//! the same `ReconcileNode` plus `s.artifacts.ForgetNode(nodeID)` after
//! `UnregisterObserved`. This file does not call either equivalent yet —
//! that wiring, plus the `ArtifactStore`-backed P2P RPCs above, is the next
//! commit, once `with_binding_store` has a real caller to also thread
//! through an `ArtifactStore` handle.
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
    /// Mirrors Go's `Service.maxProjectionTTL`, used by a caller this Stage
    /// has not wired yet (`RecordAssignment`'s `resolveProjectionTTL`) --
    /// carried here now so that wiring is additive when it lands.
    #[allow(dead_code)]
    max_projection_ttl: Duration,
}

impl NodeRegistryGrpcService {
    pub fn new(registry: Arc<AtomicNodeRegistry>, warmup: Arc<WarmupGate>) -> Self {
        Self {
            registry,
            warmup,
            binding_store: None,
            projection_authoritative: false,
            max_projection_ttl: Duration::ZERO,
        }
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
        let (_node, cpu_config_json) = match self.registry.heartbeat(&req, now) {
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

        // Warm-up is about the bindings being seeded in the real scheduler
        // (`s.warmup.reportedIn(now)` runs only after `ReconcileNode`
        // succeeds there); this half has no binding store to seed, so
        // `reported_in` here only means "the registry accepted a heartbeat
        // for this node", not "routing state is caught up". Stage D's
        // consumer (`docs/proposals/_sd-phase4-stageA-node-inventory.md` §7
        // risk 7) is what will give this call its real meaning.
        self.warmup.reported_in(now);

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
            Ok(()) => Ok(Response::new(UnregisterNodeResponse {})),
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

    async fn record_assignment(
        &self,
        _request: Request<RecordAssignmentRequest>,
    ) -> Result<Response<RecordAssignmentResponse>, Status> {
        Err(Status::unimplemented(format!(
            "RecordAssignment needs the Stage D binding store: {NOT_STAGE_A}"
        )))
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

    async fn record_p2p_artifact(
        &self,
        _request: Request<RecordP2pArtifactRequest>,
    ) -> Result<Response<RecordP2pArtifactResponse>, Status> {
        Err(Status::unimplemented(format!(
            "RecordP2pArtifact is Stage D's ArtifactStore: {NOT_STAGE_A}"
        )))
    }

    async fn forget_p2p_artifact(
        &self,
        _request: Request<ForgetP2pArtifactRequest>,
    ) -> Result<Response<ForgetP2pArtifactResponse>, Status> {
        Err(Status::unimplemented(format!(
            "ForgetP2pArtifact is Stage D's ArtifactStore: {NOT_STAGE_A}"
        )))
    }

    async fn lookup_p2p_artifact(
        &self,
        _request: Request<LookupP2pArtifactRequest>,
    ) -> Result<Response<LookupP2pArtifactResponse>, Status> {
        Err(Status::unimplemented(format!(
            "LookupP2pArtifact is Stage D's ArtifactStore: {NOT_STAGE_A}"
        )))
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
}
