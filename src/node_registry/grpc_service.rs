//! Implements the node-registry RPC subset of `scheduler.v1.Scheduler` for `aenv-api`.
//!
//! Heartbeats store the full roster in the node registry, including paused entries
//! that renew paused-registry leases, but reconcile only routable entries into the
//! binding store. Unregister removes identity first, so route cleanup is best-effort.
//! Optional binding, artifact, and paused-registry dependencies gate their RPC groups.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use tonic::{Request, Response, Status};

use crate::binding_store::artifact_index::ArtifactStore;
use crate::binding_store::lookup::{
    self as lookup_logic, LookupDeps, LookupOutcome, LookupResultLabel, ScheduleDeps,
};
use crate::binding_store::{
    Binding, BindingDecision, BindingDeleteOutcome, BindingState, BindingStore,
};
use crate::orchestrator::{PausedRegistryListEntry, PausedRegistryState, PausedSandboxRegistry};
use crate::proto::scheduler::scheduler_server::Scheduler;
use crate::proto::scheduler::{
    self, ForgetP2pArtifactRequest, ForgetP2pArtifactResponse, GetNodeRequest, GetNodeResponse,
    HeartbeatRequest, HeartbeatResponse, ListObservedNodesRequest, ListObservedNodesResponse,
    ListP2pPeersRequest, ListP2pPeersResponse, ListRegistrySandboxesRequest,
    ListRegistrySandboxesResponse, LookupNodeRequest, LookupNodeResponse, LookupP2pArtifactRequest,
    LookupP2pArtifactResponse, RecordAssignmentRequest, RecordAssignmentResponse,
    RecordP2pArtifactRequest, RecordP2pArtifactResponse, ReportSandboxEventRequest,
    ReportSandboxEventResponse, SandboxEvent, SandboxEventType, ScheduleRequest, ScheduleResponse,
    UnregisterNodeRequest, UnregisterNodeResponse,
};

use super::fleet;
use super::placement::{ShadowPlacement, ShadowSource};
use super::registry::{
    AtomicNodeRegistry, NodeNotInRegistry, NodeRegistry, ServiceInstanceMismatch,
};
use super::strategy::RoundRobinStrategy;
use super::warmup::WarmupGate;

/// Suffix on `unimplemented` errors for the RPC groups an optional dependency gates.
const NOT_WIRED: &str = "an assembled aenv-api wires every optional dependency, so a caller \
     reaching this arm is talking to an in-process or test build";

/// Serves the node-registry subset of `scheduler.v1.Scheduler`.
///
/// Clones share registry, warm-up, placement, and metric state through `Arc`.
#[derive(Clone)]
pub struct NodeRegistryGrpcService {
    registry: Arc<AtomicNodeRegistry>,
    warmup: Arc<WarmupGate>,
    binding_store: Option<Arc<dyn BindingStore>>,
    /// Must match the binding store's construction-time authoritative setting.
    projection_authoritative: bool,
    max_projection_ttl: Duration,
    artifact_store: Option<Arc<dyn ArtifactStore>>,
    paused_registry: Option<Arc<dyn PausedSandboxRegistry>>,
    /// Shared by `schedule` and paused lookup so both advance one round-robin cursor.
    strategy: Arc<RoundRobinStrategy>,
    /// Resolves metric handles at construction, outside the placement hot path.
    shadow: Arc<ShadowPlacement>,
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
            shadow: Arc::new(ShadowPlacement::default()),
        }
    }

    /// Replaces the shadow scorer with one sampling `k` candidates.
    #[must_use]
    pub fn with_placement_shadow_k(mut self, k: u32) -> Self {
        self.shadow = Arc::new(ShadowPlacement::new(k));
        self
    }

    /// Replaces the shadow scorer with a test-scripted scorer.
    #[cfg(any(test, feature = "test-support"))]
    #[must_use]
    pub fn with_placement_shadow(mut self, shadow: ShadowPlacement) -> Self {
        self.shadow = Arc::new(shadow);
        self
    }

    /// Wires the optional cluster-backed paused-sandbox registry.
    #[must_use]
    pub fn with_paused_registry(mut self, paused_registry: Arc<dyn PausedSandboxRegistry>) -> Self {
        self.paused_registry = Some(paused_registry);
        self
    }

    /// Wires the optional P2P artifact index.
    #[must_use]
    pub fn with_artifact_store(mut self, artifact_store: Arc<dyn ArtifactStore>) -> Self {
        self.artifact_store = Some(artifact_store);
        self
    }

    /// Wires the binding store and its matching authoritative-projection setting.
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

    // The judgement belongs here and is named explicitly rather than left for the
    // node to derive: only this side can tell a sandbox that was never recorded
    // from one whose row moved to another machine. Everything this side cannot
    // settle — no registry, one that is not cluster-backed, a read that failed, an
    // id the batch did not answer for — is left out, so a node that hears nothing
    // keeps everything.
    async fn disowned_paused_sandboxes(
        &self,
        node_id: &str,
        roster: &[crate::node_registry::types::RosterEntry],
    ) -> Vec<String> {
        let Some(registry) = self.paused_registry.as_ref() else {
            return Vec::new();
        };
        if !registry.is_cluster_backed() {
            return Vec::new();
        }
        let paused: Vec<crate::types::SandboxId> = roster
            .iter()
            .filter(|entry| entry.paused)
            .filter_map(|entry| crate::types::SandboxId::parse_str(entry.sandbox_id.trim()).ok())
            .collect();
        if paused.is_empty() {
            return Vec::new();
        }
        let rows = match registry.get_many(&paused).await {
            Ok(rows) => rows,
            Err(err) => {
                tracing::warn!(
                    node_id = %node_id,
                    error = %err,
                    "could not read the paused registry for this node's roster; naming nothing"
                );
                return Vec::new();
            }
        };
        let answered = rows.answered();
        paused
            .iter()
            .filter(|sandbox_id| match answered.get(sandbox_id) {
                None => false,
                Some(None) => true,
                Some(Some(entry)) => entry.origin_node_id != node_id,
            })
            .map(|sandbox_id| sandbox_id.to_string())
            .collect()
    }

    /// Reserves a sandbox's routing record before the node that will run it is asked to.
    ///
    /// In-process only: it is the create path's half of the invariant that every
    /// runtime has a control-plane record older than itself, and no `Scheduler`
    /// caller may reserve on another's behalf.
    pub async fn reserve_assignment(
        &self,
        sandbox_id: &str,
        node_id: &str,
        execution_id: &str,
        reservation_ttl: Duration,
    ) -> Result<BindingDecision, Status> {
        let Some(binding_store) = self.binding_store.clone() else {
            return Err(Status::unimplemented(format!(
                "reserving an assignment needs a binding store, and this deployment has none \
                 wired: {NOT_WIRED}"
            )));
        };
        let sandbox_id = sandbox_id.trim();
        if sandbox_id.is_empty() {
            return Err(Status::invalid_argument("sandbox_id is required"));
        }
        let Some(node) = self.registry.resolve(node_id.trim()) else {
            return Err(Status::invalid_argument(
                "node is not in scheduler node list",
            ));
        };
        let (execution, _reason) =
            crate::binding_store::record::normalize_execution_id_reason(execution_id);
        binding_store
            .record(
                sandbox_id,
                Binding {
                    node,
                    execution_id: execution,
                    projection_ttl: reservation_ttl,
                    state: BindingState::Starting,
                },
                SystemTime::now(),
            )
            .await
            .inspect(|decision| Self::record_binding_execution("assignment", *decision))
            .map_err(|err| {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    error = %err,
                    "scheduler reserve_assignment binding write failed"
                );
                Status::unavailable("binding store unavailable")
            })
    }

    /// Withdraws a reservation this process wrote, leaving a confirmation alone.
    pub async fn release_assignment_reservation(
        &self,
        sandbox_id: &str,
        execution_id: &str,
    ) -> Result<BindingDeleteOutcome, Status> {
        let Some(binding_store) = self.binding_store.clone() else {
            return Err(Status::unimplemented(format!(
                "releasing a reservation needs a binding store, and this deployment has none \
                 wired: {NOT_WIRED}"
            )));
        };
        let sandbox_id = sandbox_id.trim();
        if sandbox_id.is_empty() {
            return Err(Status::invalid_argument("sandbox_id is required"));
        }
        let (execution, _reason) =
            crate::binding_store::record::normalize_execution_id_reason(execution_id);
        binding_store
            .release_reservation(sandbox_id, &execution, SystemTime::now())
            .await
            .map_err(|err| {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    error = %err,
                    "scheduler release_assignment_reservation failed"
                );
                Status::unavailable("binding store unavailable")
            })
    }

    /// Resolves the received projection TTL, applying the authoritative gate and cap.
    fn resolve_projection_ttl(&self, raw: Duration) -> (Duration, &'static str) {
        if !self.projection_authoritative || raw.is_zero() {
            return (Duration::ZERO, "default");
        }
        if !self.max_projection_ttl.is_zero() && raw > self.max_projection_ttl {
            return (self.max_projection_ttl, "clamped");
        }
        (raw, "node")
    }

    /// Validates required P2P artifact fields and resolves an optional node identity.
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

    /// Resolves aliases while preserving the empty filter sentinel.
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

    /// Applies an incarnation-guarded projection delete and reports its outcome.
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
        // This event path must not increment the roster-entry drop metric.
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
            "Observed node count by derived status, as api's node registry currently has it."
        );
        metrics::describe_counter!(LOOKUP_NODE_METRIC, "LookupNode outcomes by result label.");
        metrics::describe_counter!(
            LOOKUP_EXECUTION_AUTHORITY_METRIC,
            "Execution authority carried on every successful LookupNode answer."
        );
        metrics::describe_counter!(
            BINDING_EXECUTION_METRIC,
            "Binding-store arbitration outcomes by decision and write source."
        );
        metrics::describe_histogram!(
            SCHEDULE_DURATION_METRIC,
            "Schedule call latency by strategy and outcome."
        );
        metrics::describe_counter!(
            SCHEDULE_ASSIGNMENTS_METRIC,
            "Successful Schedule placements by strategy."
        );
    }

    fn record_lookup_node(label: LookupResultLabel) {
        metrics::counter!(LOOKUP_NODE_METRIC, "result" => label.as_str()).increment(1);
    }

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

    fn record_binding_execution(source: &'static str, decision: BindingDecision) {
        let label = decision.as_str();
        if label.is_empty() {
            return;
        }
        metrics::counter!(BINDING_EXECUTION_METRIC, "decision" => label, "source" => source)
            .increment(1);
    }

    /// Refreshes the observed-node gauge from the current registry snapshot.
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
const SANDBOX_EVENT_METRIC: &str = "agentenv_api_sandbox_event_total";
const PROJECTION_TTL_SOURCE_METRIC: &str = "agentenv_api_projection_ttl_source_total";
const LOOKUP_NODE_METRIC: &str = "agentenv_api_lookup_node_total";
const LOOKUP_EXECUTION_AUTHORITY_METRIC: &str = "agentenv_api_lookup_execution_authority_total";
const BINDING_EXECUTION_METRIC: &str = "agentenv_api_binding_execution_total";
const SCHEDULE_DURATION_METRIC: &str = "agentenv_api_schedule_duration_seconds";
const SCHEDULE_ASSIGNMENTS_METRIC: &str = "agentenv_api_schedule_assignments_total";

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
                // The dual-heartbeat reporter matches this scheduler error text exactly.
                return Err(Status::invalid_argument(
                    "node is not in scheduler node list",
                ));
            }
        };

        let roster = super::registry::roster_from_heartbeat(&req);

        // Warm-up means bindings were seeded when a binding store is configured.
        if let Some(binding_store) = &self.binding_store {
            // Paused entries must renew their registry leases but must not become routing
            // projections. Reconciliation also removes projections for newly paused entries.
            let routable: Vec<_> = roster.iter().filter(|e| !e.paused).cloned().collect();
            let withheld = roster.len() - routable.len();
            if withheld > 0 {
                tracing::debug!(
                    node_id = %node_id,
                    withheld,
                    "withholding paused sandboxes from binding reconciliation"
                );
            }
            match binding_store.reconcile_node(node, routable, now).await {
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
            // Latch warm-up only after the roster reaches the binding store.
            self.warmup.reported_in(now);
        } else {
            self.warmup.reported_in(now);
        }

        Ok(Response::new(HeartbeatResponse {
            cpu_config_json,
            disowned_sandbox_ids: self.disowned_paused_sandboxes(node_id, &roster).await,
        }))
    }

    async fn list_observed_nodes(
        &self,
        request: Request<ListObservedNodesRequest>,
    ) -> Result<Response<ListObservedNodesResponse>, Status> {
        let req = request.into_inner();
        let nodes =
            fleet::observed_nodes(self.registry.as_ref(), &req.cluster_id, SystemTime::now());
        Ok(Response::new(ListObservedNodesResponse { nodes }))
    }

    async fn get_node(
        &self,
        request: Request<GetNodeRequest>,
    ) -> Result<Response<GetNodeResponse>, Status> {
        let req = request.into_inner();
        let node_id = req.node_id.trim();
        if node_id.is_empty() {
            return Err(Status::invalid_argument("node_id is required"));
        }
        let node = fleet::observed_node(
            self.registry.as_ref(),
            node_id,
            &req.cluster_id,
            SystemTime::now(),
        )
        .ok_or_else(|| Status::not_found("observed node not found"))?;
        Ok(Response::new(GetNodeResponse { node: Some(node) }))
    }

    /// Resolves aliases before unregistering and cleaning up dependent indexes.
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
                // Identity is already removed, so route cleanup is best-effort.
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
                // Artifact cleanup is infallible.
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

    /// Places a new sandbox through the shared scheduling pipeline.
    async fn schedule(
        &self,
        request: Request<ScheduleRequest>,
    ) -> Result<Response<ScheduleResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let deps = ScheduleDeps {
            node_registry: self.registry.as_ref(),
            strategy: self.strategy.as_ref(),
            shadow: self.shadow.as_ref(),
        };
        let result = lookup_logic::select_node(
            &deps,
            req.hint.as_ref(),
            "",
            ShadowSource::Schedule,
            SystemTime::now(),
        );
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

    /// Resolves a sandbox location through the binding and paused registries.
    async fn lookup_node(
        &self,
        request: Request<LookupNodeRequest>,
    ) -> Result<Response<LookupNodeResponse>, Status> {
        let Some(binding_store) = self.binding_store.clone() else {
            return Err(Status::unimplemented(format!(
                "LookupNode needs a binding store, and this deployment has none wired: \
                 {NOT_WIRED}"
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
                shadow: self.shadow.as_ref(),
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

    /// Records an assignment after identity, execution-id, and projection-TTL normalization.
    async fn record_assignment(
        &self,
        request: Request<RecordAssignmentRequest>,
    ) -> Result<Response<RecordAssignmentResponse>, Status> {
        let Some(binding_store) = self.binding_store.clone() else {
            return Err(Status::unimplemented(format!(
                "RecordAssignment needs a binding store, and this deployment has none wired: \
                 {NOT_WIRED}"
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
                    state: BindingState::Confirmed,
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

    /// Applies best-effort PAUSE/DELETE projection removal and observes other events.
    async fn report_sandbox_event(
        &self,
        request: Request<ReportSandboxEventRequest>,
    ) -> Result<Response<ReportSandboxEventResponse>, Status> {
        let Some(binding_store) = self.binding_store.clone() else {
            return Err(Status::unimplemented(format!(
                "ReportSandboxEvent needs a binding store, and this deployment has none \
                 wired: {NOT_WIRED}"
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

    /// Lists live P2P peers matching the requested backend and exclusion.
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

    /// Validates and records a P2P artifact association.
    async fn record_p2p_artifact(
        &self,
        request: Request<RecordP2pArtifactRequest>,
    ) -> Result<Response<RecordP2pArtifactResponse>, Status> {
        let Some(artifact_store) = self.artifact_store.clone() else {
            return Err(Status::unimplemented(format!(
                "RecordP2pArtifact needs an ArtifactStore, and this deployment has none \
                 wired: {NOT_WIRED}"
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

    async fn forget_p2p_artifact(
        &self,
        request: Request<ForgetP2pArtifactRequest>,
    ) -> Result<Response<ForgetP2pArtifactResponse>, Status> {
        let Some(artifact_store) = self.artifact_store.clone() else {
            return Err(Status::unimplemented(format!(
                "ForgetP2pArtifact needs an ArtifactStore, and this deployment has none \
                 wired: {NOT_WIRED}"
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

    /// Looks up an artifact and returns only currently live peer descriptors.
    async fn lookup_p2p_artifact(
        &self,
        request: Request<LookupP2pArtifactRequest>,
    ) -> Result<Response<LookupP2pArtifactResponse>, Status> {
        let Some(artifact_store) = self.artifact_store.clone() else {
            return Err(Status::unimplemented(format!(
                "LookupP2pArtifact needs an ArtifactStore, and this deployment has none \
                 wired: {NOT_WIRED}"
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

    /// Lists, filters, and keyset-pages cluster-backed paused-registry entries.
    ///
    /// Argument validation precedes registry availability checks.
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

        // Sorting is required before keyset paging over the backend's unordered result.
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

fn parse_registry_state_filter(raw: &str) -> Result<Option<PausedRegistryState>, Status> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    for state in PausedRegistryState::ALL {
        if state.as_str().eq_ignore_ascii_case(trimmed) {
            return Ok(Some(state));
        }
    }
    let known: Vec<&str> = PausedRegistryState::ALL
        .iter()
        .copied()
        .map(PausedRegistryState::as_str)
        .collect();
    Err(Status::invalid_argument(format!(
        "unknown state '{trimmed}', must be one of {}",
        known.join(", ")
    )))
}

fn registry_sandbox_to_proto(entry: &PausedRegistryListEntry) -> scheduler::RegistrySandbox {
    scheduler::RegistrySandbox {
        sandbox_id: entry.sandbox_id.to_string(),
        cluster_id: entry.cluster_id.to_string(),
        state: entry.state.as_str().to_string(),
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

    async fn assert_report_sandbox_event_behavior_matrix(binding_store: Arc<dyn BindingStore>) {
        use crate::proto::scheduler::SandboxEventType;
        const EXEC_1: &str = "00000000-0000-7000-8000-000000000001";
        const EXEC_2: &str = "00000000-0000-7000-8000-000000000002";

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
                        state: BindingState::Confirmed,
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
                        state: BindingState::Confirmed,
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
                        state: BindingState::Confirmed,
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
                        state: BindingState::Confirmed,
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
                        state: BindingState::Confirmed,
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
                        state: BindingState::Confirmed,
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

    /// Produces a ready P2P heartbeat; the generic helper produces `Connecting`.
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

        // Seed both nodes so readiness requires both real CPU configurations.
        client
            .heartbeat(heartbeat_with_cpu_config("node-a", ""))
            .await
            .expect("node-a seed heartbeat");
        client
            .heartbeat(heartbeat_with_cpu_config("node-b", ""))
            .await
            .expect("node-b seed heartbeat");

        let first = client
            .heartbeat(heartbeat_with_cpu_config("node-a", cfg_a))
            .await
            .expect("node-a heartbeats")
            .into_inner();
        assert_eq!(
            first.cpu_config_json, "",
            "intersection was computed before every node had reported"
        );

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
        // A peer using another backend must be excluded.
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

    /// Fails every binding-store operation to exercise RPC failure policy.
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
        async fn release_reservation(
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

    #[derive(Default)]
    struct RecordingBindingStore {
        reconciled: std::sync::Mutex<Vec<Vec<crate::node_registry::types::RosterEntry>>>,
        written: std::sync::Mutex<Vec<(String, Binding)>>,
    }

    impl RecordingBindingStore {
        fn last_roster(&self) -> Vec<crate::node_registry::types::RosterEntry> {
            self.reconciled
                .lock()
                .expect("not poisoned")
                .last()
                .cloned()
                .expect("reconcile_node must have been called at least once")
        }

        fn last_written(&self) -> (String, Binding) {
            self.written
                .lock()
                .expect("not poisoned")
                .last()
                .cloned()
                .expect("record must have been called at least once")
        }
    }

    #[async_trait::async_trait]
    impl BindingStore for RecordingBindingStore {
        async fn get(
            &self,
            sandbox_id: &str,
            _now: SystemTime,
        ) -> Result<Option<Binding>, crate::binding_store::BindingStoreError> {
            Ok(self
                .written
                .lock()
                .expect("not poisoned")
                .iter()
                .rev()
                .find(|(id, _)| id == sandbox_id)
                .map(|(_, binding)| binding.clone()))
        }
        async fn record(
            &self,
            sandbox_id: &str,
            binding: Binding,
            _now: SystemTime,
        ) -> Result<BindingDecision, crate::binding_store::BindingStoreError> {
            self.written
                .lock()
                .expect("not poisoned")
                .push((sandbox_id.to_string(), binding));
            Ok(BindingDecision::Installed)
        }
        async fn reconcile_node(
            &self,
            _node: crate::node_registry::types::Node,
            roster: Vec<crate::node_registry::types::RosterEntry>,
            _now: SystemTime,
        ) -> Result<Vec<(String, BindingDecision)>, crate::binding_store::BindingStoreError>
        {
            self.reconciled.lock().expect("not poisoned").push(roster);
            Ok(Vec::new())
        }
        async fn delete(
            &self,
            _sandbox_id: &str,
            _execution_id: &str,
            _now: SystemTime,
        ) -> Result<BindingDeleteOutcome, crate::binding_store::BindingStoreError> {
            Ok(BindingDeleteOutcome::Absent)
        }
        // Carries the real state fence so a reservation test cannot pass on a
        // fake that withdraws confirmations too.
        async fn release_reservation(
            &self,
            sandbox_id: &str,
            execution_id: &str,
            _now: SystemTime,
        ) -> Result<BindingDeleteOutcome, crate::binding_store::BindingStoreError> {
            let mut written = self.written.lock().expect("not poisoned");
            let Some(position) = written.iter().rposition(|(id, _)| id == sandbox_id) else {
                return Ok(BindingDeleteOutcome::Absent);
            };
            let binding = &written[position].1;
            if binding.state != BindingState::Starting {
                return Ok(BindingDeleteOutcome::RejectedConfirmed);
            }
            if !binding.execution_id.is_empty() && binding.execution_id != execution_id {
                return Ok(BindingDeleteOutcome::RejectedStale);
            }
            written.remove(position);
            Ok(BindingDeleteOutcome::Deleted)
        }
    }

    fn service_with_registry(
        nodes: Vec<Node>,
        binding_store: Arc<dyn BindingStore>,
    ) -> (Arc<AtomicNodeRegistry>, NodeRegistryGrpcService) {
        let registry = Arc::new(AtomicNodeRegistry::new(nodes, Duration::from_secs(30)));
        let warmup = Arc::new(WarmupGate::new(
            Arc::clone(&registry) as Arc<dyn NodeRegistry>,
            Duration::from_secs(15),
            SystemTime::now(),
        ));
        let service = NodeRegistryGrpcService::new(Arc::clone(&registry), warmup)
            .with_binding_store(binding_store, true, Duration::ZERO);
        (registry, service)
    }

    fn heartbeat_with_a_paused_sandbox() -> HeartbeatRequest {
        HeartbeatRequest {
            node_id: "node-a".to_string(),
            cluster_id: "cluster-a".to_string(),
            service_instance_id: "node-a-instance".to_string(),
            roster: vec![
                scheduler::SandboxRosterEntry {
                    sandbox_id: "sbx-running".to_string(),
                    execution_id: "0199a000-0000-7000-8000-000000000001".to_string(),
                    paused: false,
                    ..Default::default()
                },
                scheduler::SandboxRosterEntry {
                    sandbox_id: "sbx-parked".to_string(),
                    execution_id: "0199a000-0000-7000-8000-000000000002".to_string(),
                    paused: true,
                    ..Default::default()
                },
            ],
            ..Default::default()
        }
    }

    fn heartbeat_holding(paused: SandboxId, running: SandboxId) -> HeartbeatRequest {
        HeartbeatRequest {
            node_id: "node-a".to_string(),
            cluster_id: "cluster-a".to_string(),
            service_instance_id: "node-a-instance".to_string(),
            roster: vec![
                scheduler::SandboxRosterEntry {
                    sandbox_id: running.to_string(),
                    execution_id: "0199a000-0000-7000-8000-000000000001".to_string(),
                    paused: false,
                    ..Default::default()
                },
                scheduler::SandboxRosterEntry {
                    sandbox_id: paused.to_string(),
                    execution_id: "0199a000-0000-7000-8000-000000000002".to_string(),
                    paused: true,
                    ..Default::default()
                },
            ],
            ..Default::default()
        }
    }

    async fn disowned_by(
        registry: FakePausedRegistry,
        paused: SandboxId,
        running: SandboxId,
    ) -> Vec<String> {
        let (_registry, service) = service_with_registry(
            vec![node("node-a", "http://node-a")],
            in_memory_binding_store(),
        );
        service
            .with_paused_registry(Arc::new(registry))
            .heartbeat(Request::new(heartbeat_holding(paused, running)))
            .await
            .expect("node-a heartbeats")
            .into_inner()
            .disowned_sandbox_ids
    }

    #[tokio::test]
    async fn a_heartbeat_names_a_paused_sandbox_no_row_records() {
        let paused = SandboxId::new();
        let running = SandboxId::new();
        assert_eq!(
            disowned_by(FakePausedRegistry::empty(true), paused, running).await,
            vec![paused.to_string()],
            "a paused sandbox the registry has no row for is this node's alone and unreachable"
        );
    }

    #[tokio::test]
    async fn a_heartbeat_names_a_paused_sandbox_whose_row_names_another_node() {
        let paused = SandboxId::new();
        let running = SandboxId::new();
        let registry = FakePausedRegistry::with_entry(
            paused_entry(paused, PausedRegistryState::Running, "node-b", None),
            true,
        );
        assert_eq!(
            disowned_by(registry, paused, running).await,
            vec![paused.to_string()],
            "the row names node-b, so node-a is holding a ghost of it"
        );
    }

    #[tokio::test]
    async fn a_heartbeat_names_nothing_while_the_row_still_names_this_node() {
        let paused = SandboxId::new();
        let running = SandboxId::new();
        let registry = FakePausedRegistry::with_entry(
            paused_entry(paused, PausedRegistryState::Paused, "node-a", None),
            true,
        );
        assert!(disowned_by(registry, paused, running).await.is_empty());
    }

    #[tokio::test]
    async fn a_heartbeat_names_nothing_when_the_registry_could_not_answer() {
        let paused = SandboxId::new();
        let running = SandboxId::new();
        assert!(
            disowned_by(FakePausedRegistry::erroring(), paused, running)
                .await
                .is_empty(),
            "a registry that could not be read named a sandbox for deletion"
        );
    }

    #[tokio::test]
    async fn a_heartbeat_names_nothing_when_the_batch_did_not_answer_for_the_sandbox() {
        let paused = SandboxId::new();
        let running = SandboxId::new();
        assert!(
            disowned_by(
                FakePausedRegistry::not_answering_for(paused),
                paused,
                running
            )
            .await
            .is_empty(),
            "an id the batch left out of its coverage was read as a confirmed absence"
        );
    }

    #[tokio::test]
    async fn a_heartbeat_names_nothing_without_a_cluster_backed_registry() {
        let paused = SandboxId::new();
        let running = SandboxId::new();
        assert!(
            disowned_by(FakePausedRegistry::empty(false), paused, running)
                .await
                .is_empty(),
            "a deployment whose rows are node-local would have every paused sandbox deleted"
        );
    }

    #[tokio::test]
    async fn a_heartbeat_never_names_a_running_sandbox() {
        let paused = SandboxId::new();
        let running = SandboxId::new();
        let named = disowned_by(FakePausedRegistry::empty(true), paused, running).await;
        assert!(
            !named.contains(&running.to_string()),
            "a running sandbox has no registry row by design; naming it would tear down live VMs"
        );
    }

    #[tokio::test]
    async fn heartbeat_withholds_paused_sandboxes_from_binding_reconciliation() {
        let store = Arc::new(RecordingBindingStore::default());
        let (_registry, service) = service_with_registry(
            vec![node("node-a", "http://node-a")],
            Arc::clone(&store) as Arc<dyn BindingStore>,
        );

        service
            .heartbeat(Request::new(heartbeat_with_a_paused_sandbox()))
            .await
            .expect("node-a heartbeats");

        let reconciled: Vec<String> = store
            .last_roster()
            .into_iter()
            .map(|entry| entry.sandbox_id)
            .collect();

        // Exact equality also catches accidental removal of every roster entry.
        assert_eq!(
            reconciled,
            vec!["sbx-running".to_string()],
            "binding reconciliation must receive the running sandbox and only the running \
             sandbox: a projection for a parked sandbox is what makes the gateway answer the \
             data plane with 410 instead of waking it"
        );
    }

    #[tokio::test]
    async fn heartbeat_still_registers_paused_sandboxes_so_their_leases_keep_renewing() {
        let store = Arc::new(RecordingBindingStore::default());
        let (registry, service) = service_with_registry(
            vec![node("node-a", "http://node-a")],
            Arc::clone(&store) as Arc<dyn BindingStore>,
        );

        service
            .heartbeat(Request::new(heartbeat_with_a_paused_sandbox()))
            .await
            .expect("node-a heartbeats");

        let (roster, _last_seen) = registry
            .roster_of("node-a")
            .expect("the node reported, so it has a roster");
        let held: Vec<&str> = roster
            .iter()
            .map(|entry| entry.sandbox_id.as_str())
            .collect();
        assert_eq!(
            held,
            vec!["sbx-running", "sbx-parked"],
            "🔴 the registry roster must keep the paused sandbox. It is the only thing that \
             renews that sandbox's lease in the cluster paused registry \
             (`candidates_from_rosters` reads exactly these entries); dropping it here lets the \
             lease lapse and another node claim a row whose snapshot lives on this node's disk \
             alone"
        );
        assert!(
            roster
                .iter()
                .any(|entry| entry.sandbox_id == "sbx-parked" && entry.paused),
            "and it must keep it flagged as paused, not launder it into a running entry"
        );

        assert_eq!(
            registry.nodes_holding("sbx-parked"),
            vec!["node-a".to_string()],
            "🔴 the reverse index must still name the node holding the paused sandbox: this is \
             what answers 'who has this snapshot' after the projection is gone"
        );
        assert_eq!(
            registry.nodes_holding("sbx-running"),
            vec!["node-a".to_string()],
            "and the running sandbox is unaffected"
        );
    }

    #[tokio::test]
    async fn heartbeat_removes_the_projection_of_a_sandbox_that_has_since_paused() {
        let store: Arc<dyn BindingStore> =
            Arc::new(crate::binding_store::InMemoryBindingStore::new(
                crate::binding_store::BindingStoreSettings::default(),
            ));
        let (_registry, service) =
            service_with_registry(vec![node("node-a", "http://node-a")], Arc::clone(&store));

        let mut running = heartbeat_with_a_paused_sandbox();
        running.roster[1].paused = false;
        service
            .heartbeat(Request::new(running))
            .await
            .expect("node-a heartbeats while both sandboxes run");
        assert!(
            store
                .get("sbx-parked", SystemTime::now())
                .await
                .unwrap()
                .is_some(),
            "precondition: while it is running the sandbox does hold a projection"
        );

        service
            .heartbeat(Request::new(heartbeat_with_a_paused_sandbox()))
            .await
            .expect("node-a heartbeats again, now with the sandbox paused");

        assert!(
            store
                .get("sbx-parked", SystemTime::now())
                .await
                .unwrap()
                .is_none(),
            "the projection must be gone once the node reports the sandbox as paused, so the \
             gateway takes its wake path instead of routing to a VM that is not running"
        );
        assert!(
            store
                .get("sbx-running", SystemTime::now())
                .await
                .unwrap()
                .is_some(),
            "and the running sandbox must keep its projection through the same reconcile"
        );
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

        client
            .unregister_node(UnregisterNodeRequest {
                node_id: "node-a".to_string(),
                service_instance_id: "node-a-instance".to_string(),
            })
            .await
            .expect("a binding-cleanup failure must not fail unregister_node itself");
    }

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
                    // Discovery, not this stale caller value, supplies the endpoint.
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

    #[tokio::test]
    async fn record_then_lookup_p2p_artifact_round_trips_through_the_rpc() {
        let store: Arc<dyn crate::binding_store::artifact_index::ArtifactStore> =
            Arc::new(crate::binding_store::artifact_index::InMemoryArtifactStore::new(10));
        let (_registry, mut client, _stop) =
            service_with_artifact_store(vec![node("node-a", "http://10.0.0.7:8000")], store).await;
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
        // Keep node-a live so exclusion, rather than liveness, removes it.
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

    use std::collections::HashMap as StdHashMap;

    use crate::binding_store::{BindingStoreSettings, InMemoryBindingStore};
    use crate::orchestrator::{
        BeganPause, DeadlineRenewalOutcome, HeldSandbox, MarkRunningOutcome, PausedRegistryError,
        PausedRegistryListEntry, PausedRegistryListing, PausedRegistryRows, PausedRegistryState,
        PausedSandboxEntry, ReclaimedHoldings, RegistryResult, ReleasedHoldings, ResumeClaim,
    };
    use crate::types::{ExecutionId, SandboxId};

    struct FakePausedRegistry {
        entries: StdHashMap<SandboxId, PausedSandboxEntry>,
        cluster_backed: bool,
        erroring: bool,
        // Ids a batch read leaves out of its coverage, as an undecodable row does.
        uncovered: std::collections::HashSet<SandboxId>,
    }

    impl FakePausedRegistry {
        fn with_entry(entry: PausedSandboxEntry, cluster_backed: bool) -> Self {
            let mut entries = StdHashMap::new();
            entries.insert(entry.sandbox_id, entry);
            Self {
                entries,
                cluster_backed,
                erroring: false,
                uncovered: Default::default(),
            }
        }

        fn empty(cluster_backed: bool) -> Self {
            Self {
                entries: StdHashMap::new(),
                cluster_backed,
                erroring: false,
                uncovered: Default::default(),
            }
        }

        fn not_answering_for(sandbox_id: SandboxId) -> Self {
            let mut registry = Self::empty(true);
            registry.uncovered.insert(sandbox_id);
            registry
        }

        fn erroring() -> Self {
            Self {
                entries: StdHashMap::new(),
                cluster_backed: true,
                erroring: true,
                uncovered: Default::default(),
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
        // `uncovered` ids are left out of both the entries and the coverage list,
        // so a caller acting on their absence is acting on an answer this fixture
        // never gave.
        async fn get_many(&self, sandbox_ids: &[SandboxId]) -> RegistryResult<PausedRegistryRows> {
            if self.erroring {
                return Err(PausedRegistryError::Backend {
                    operation: "test",
                    source: anyhow::anyhow!("boom"),
                });
            }
            let covered: Vec<SandboxId> = sandbox_ids
                .iter()
                .copied()
                .filter(|id| !self.uncovered.contains(id))
                .collect();
            let entries = covered
                .iter()
                .filter_map(|id| self.entries.get(id).map(|entry| (*id, entry.clone())))
                .collect();
            Ok(PausedRegistryRows { entries, covered })
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

    /// Returns a gate made warm by one report against an elapsed deadline.
    fn warm_gate(registry: &Arc<AtomicNodeRegistry>) -> Arc<WarmupGate> {
        let gate = Arc::new(WarmupGate::new(
            Arc::clone(registry) as Arc<dyn NodeRegistry>,
            Duration::from_secs(1),
            SystemTime::UNIX_EPOCH,
        ));
        gate.reported_in(SystemTime::now());
        gate
    }

    /// Returns a gate that stays cold for the test lifetime.
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

    /// Lists fixed paused-registry entries and panics on unrelated operations.
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
            // A non-cluster-backed registry must be rejected before `list_all`.
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
        async fn get_many(&self, _sandbox_ids: &[SandboxId]) -> RegistryResult<PausedRegistryRows> {
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

    /// Produces a `Ready` heartbeat suitable for schedulability tests.
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
                    paused: false,
                })
                .collect(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn schedule_places_a_node_with_nothing_else_wired_and_round_robins() {
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
    async fn schedule_and_lookup_node_advance_one_shared_round_robin_cursor() {
        let sandbox_id = SandboxId::new();
        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![
                node("node-a", "http://10.0.0.1:8000"),
                node("node-b", "http://10.0.0.2:8000"),
                node("node-c", "http://10.0.0.3:8000"),
            ],
            Duration::from_secs(30),
        ));
        // No origin preference, so paused lookup reaches the shared strategy.
        let paused: Arc<dyn PausedSandboxRegistry> = Arc::new(FakePausedRegistry::with_entry(
            paused_entry(sandbox_id, PausedRegistryState::Paused, "", None),
            true,
        ));
        let service = NodeRegistryGrpcService::new(Arc::clone(&registry), warm_gate(&registry))
            .with_binding_store(in_memory_binding_store(), false, Duration::ZERO)
            .with_paused_registry(paused);

        let mut seen = Vec::new();
        for _ in 0..2 {
            let scheduled = service
                .schedule(Request::new(ScheduleRequest { hint: None }))
                .await
                .expect("three discovered nodes")
                .into_inner();
            seen.push(scheduled.node.expect("a node was chosen").node_id);

            let looked_up = service
                .lookup_node(Request::new(LookupNodeRequest {
                    sandbox_id: sandbox_id.to_string(),
                }))
                .await
                .expect("a paused row with no origin preference")
                .into_inner();
            assert_eq!(looked_up.location(), scheduler::SandboxLocation::Placed);
            seen.push(looked_up.node.expect("a node was placed").node_id);
        }

        assert_eq!(
            seen,
            vec![
                "node-a".to_string(),
                "node-b".to_string(),
                "node-c".to_string(),
                "node-a".to_string(),
            ],
            "Schedule and LookupNode must advance ONE cursor; a, a, b, b means              they were each handed their own RoundRobinStrategy"
        );
    }

    #[tokio::test]
    async fn schedule_metrics_are_named_and_labelled_exactly() {
        use metrics_util::debugging::DebuggingRecorder;

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let guard = metrics::set_default_local_recorder(&recorder);

        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![node("node-a", "http://10.0.0.1:8000")],
            Duration::from_secs(30),
        ));
        let service = NodeRegistryGrpcService::new(Arc::clone(&registry), warm_gate(&registry));
        service
            .schedule(Request::new(ScheduleRequest { hint: None }))
            .await
            .expect("one discovered node");

        drop(guard);

        let series: Vec<(String, Vec<(String, String)>)> = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .map(|(composite, _unit, _description, _value)| {
                let key = composite.key();
                let mut labels: Vec<(String, String)> = key
                    .labels()
                    .map(|label| (label.key().to_string(), label.value().to_string()))
                    .collect();
                labels.sort();
                (key.name().to_string(), labels)
            })
            .collect();

        assert!(
            series.contains(&(
                "agentenv_api_schedule_duration_seconds".to_string(),
                vec![
                    ("status".to_string(), "ok".to_string()),
                    ("strategy".to_string(), "round_robin".to_string()),
                ],
            )),
            "the duration histogram must keep its name and both of its              strategy=round_robin / status=ok labels: {series:?}"
        );
        assert!(
            series.contains(&(
                "agentenv_api_schedule_assignments_total".to_string(),
                vec![("strategy".to_string(), "round_robin".to_string())],
            )),
            "the assignments counter must keep its name and its              strategy=round_robin label: {series:?}"
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
                    state: BindingState::Confirmed,
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
    async fn lookup_node_answers_neither_bound_nor_absent_while_a_create_is_in_flight() {
        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![node("node-a", "http://10.0.0.1:8000")],
            Duration::from_secs(30),
        ));
        let sandbox_id = SandboxId::new();
        let execution_id = ExecutionId::new().to_string();
        let service = NodeRegistryGrpcService::new(Arc::clone(&registry), warm_gate(&registry))
            .with_binding_store(in_memory_binding_store(), false, Duration::ZERO);
        service
            .reserve_assignment(
                &sandbox_id.to_string(),
                "node-a",
                &execution_id,
                Duration::from_secs(300),
            )
            .await
            .expect("reserve");

        let status = service
            .lookup_node(Request::new(LookupNodeRequest {
                sandbox_id: sandbox_id.to_string(),
            }))
            .await
            .expect_err("a reservation is not a runtime to route at");
        assert_eq!(
            status.code(),
            tonic::Code::Unavailable,
            "a create in flight answered {:?}: NotFound would license reaping the runtime it is \
             about to start",
            status.code()
        );

        service
            .record_assignment(Request::new(scheduler::RecordAssignmentRequest {
                sandbox_id: sandbox_id.to_string(),
                node: Some(scheduler::Node {
                    node_id: "node-a".to_string(),
                    endpoint: "http://10.0.0.1:8000".to_string(),
                }),
                execution_id: execution_id.clone(),
                projection_ttl_secs: 0,
            }))
            .await
            .expect("the node acknowledged it");

        let resp = service
            .lookup_node(Request::new(LookupNodeRequest {
                sandbox_id: sandbox_id.to_string(),
            }))
            .await
            .expect("confirmed")
            .into_inner();
        assert_eq!(resp.node.as_ref().unwrap().node_id, "node-a");
        assert_eq!(resp.execution_id, execution_id);
    }

    #[tokio::test]
    async fn a_withdrawn_reservation_leaves_the_sandbox_absent_again() {
        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![node("node-a", "http://10.0.0.1:8000")],
            Duration::from_secs(30),
        ));
        let sandbox_id = SandboxId::new();
        let execution_id = ExecutionId::new().to_string();
        let service = NodeRegistryGrpcService::new(Arc::clone(&registry), warm_gate(&registry))
            .with_binding_store(in_memory_binding_store(), false, Duration::ZERO);
        service
            .reserve_assignment(
                &sandbox_id.to_string(),
                "node-a",
                &execution_id,
                Duration::from_secs(300),
            )
            .await
            .expect("reserve");
        assert_eq!(
            service
                .release_assignment_reservation(&sandbox_id.to_string(), &execution_id)
                .await
                .expect("release"),
            BindingDeleteOutcome::Deleted
        );

        let status = service
            .lookup_node(Request::new(LookupNodeRequest {
                sandbox_id: sandbox_id.to_string(),
            }))
            .await
            .expect_err("nothing holds it any more");
        assert_eq!(status.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn a_reservation_is_refused_for_a_node_the_registry_does_not_know() {
        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![node("node-a", "http://10.0.0.1:8000")],
            Duration::from_secs(30),
        ));
        let service = NodeRegistryGrpcService::new(Arc::clone(&registry), warm_gate(&registry))
            .with_binding_store(in_memory_binding_store(), false, Duration::ZERO);
        let status = service
            .reserve_assignment(
                &SandboxId::new().to_string(),
                "node-z",
                &ExecutionId::new().to_string(),
                Duration::from_secs(300),
            )
            .await
            .expect_err("node-z is not in the registry");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn a_reservation_write_says_it_is_a_reservation() {
        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![node("node-a", "http://10.0.0.1:8000")],
            Duration::from_secs(30),
        ));
        let store = Arc::new(RecordingBindingStore::default());
        let service = NodeRegistryGrpcService::new(Arc::clone(&registry), warm_gate(&registry))
            .with_binding_store(
                Arc::clone(&store) as Arc<dyn BindingStore>,
                false,
                Duration::ZERO,
            );
        let sandbox_id = SandboxId::new();
        service
            .reserve_assignment(
                &sandbox_id.to_string(),
                "node-a",
                &ExecutionId::new().to_string(),
                Duration::from_secs(300),
            )
            .await
            .expect("reserve");
        let (id, binding) = store.last_written();
        assert_eq!(id, sandbox_id.to_string());
        assert_eq!(binding.state, BindingState::Starting);
        assert_eq!(
            binding.projection_ttl,
            Duration::from_secs(300),
            "a reservation with the store's ordinary TTL would outlive a slow create or die \
             during one, depending on which is shorter"
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

    #[tokio::test]
    async fn lookup_node_roster_prefers_the_lexicographically_newer_incarnation() {
        let sandbox_id = SandboxId::new();
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

    /// A row mid-resume, shaped the way the claim leaves it before placement runs.
    fn claimed_resuming_row(
        sandbox_id: SandboxId,
        origin_node_id: &str,
        published: bool,
    ) -> PausedSandboxEntry {
        let mut entry = paused_entry(
            sandbox_id,
            PausedRegistryState::Resuming,
            origin_node_id,
            Some(ExecutionId::new()),
        );
        entry.claimed_by_node_id = Some("agentenv-api-replica-0".to_string());
        entry.snapshot_id = published.then(crate::snapshot::SnapshotId::generate);
        entry
    }

    // The resume path's half of the invariant: the claim CAS writes the row before
    // anything is restored, so a row in any state must keep the lookup off the
    // absence the delete path reads as a verdict.
    #[tokio::test]
    async fn a_registry_row_in_any_state_is_never_a_warm_absence() {
        for state in [
            PausedRegistryState::Paused,
            PausedRegistryState::Publishing,
            PausedRegistryState::LocalOnly,
            PausedRegistryState::Running,
            PausedRegistryState::Resuming,
        ] {
            for origin_is_live in [true, false] {
                let sandbox_id = SandboxId::new();
                let registry = Arc::new(AtomicNodeRegistry::new(
                    vec![node("node-a", "http://10.0.0.1:8000")],
                    Duration::from_secs(30),
                ));
                if origin_is_live {
                    registry
                        .heartbeat(&heartbeat_req("node-a", vec![]), SystemTime::now())
                        .expect("node-a is in discovery");
                }
                let origin = if origin_is_live {
                    "node-a"
                } else {
                    "node-gone"
                };
                let paused: Arc<dyn PausedSandboxRegistry> =
                    Arc::new(FakePausedRegistry::with_entry(
                        paused_entry(sandbox_id, state, origin, Some(ExecutionId::new())),
                        true,
                    ));
                let service =
                    NodeRegistryGrpcService::new(Arc::clone(&registry), warm_gate(&registry))
                        .with_binding_store(in_memory_binding_store(), false, Duration::ZERO)
                        .with_paused_registry(paused);

                let outcome = service
                    .lookup_node(Request::new(LookupNodeRequest {
                        sandbox_id: sandbox_id.to_string(),
                    }))
                    .await;
                let code = outcome.err().map(|status| status.code());
                assert_ne!(
                    code,
                    Some(tonic::Code::NotFound),
                    "a {state:?} row with origin_is_live={origin_is_live} answered NotFound, so \
                     the row that fences this sandbox does not stop a delete from reaping it"
                );
            }
        }
    }

    #[tokio::test]
    async fn lookup_node_places_a_resuming_row_elsewhere_when_its_origin_is_gone() {
        let sandbox_id = SandboxId::new();
        // node-b is the origin and has left the cluster: discovery has dropped it,
        // exactly as deleting its pod does, while the row still names it.
        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![node("node-a", "http://10.0.0.1:8000")],
            Duration::from_secs(30),
        ));
        registry
            .heartbeat(&heartbeat_req("node-a", vec![]), SystemTime::now())
            .expect("node-a is in discovery");
        let paused: Arc<dyn PausedSandboxRegistry> = Arc::new(FakePausedRegistry::with_entry(
            claimed_resuming_row(sandbox_id, "node-b", true),
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
            .expect(
                "🔴 the assertion. Taking the claim is what flips a row to resuming, and it \
                 happens before placement is ever asked, so refusing here refuses every resume \
                 whose origin is gone -- the whole of criterion (b)",
            )
            .into_inner();

        assert_eq!(
            resp.node.as_ref().unwrap().node_id,
            "node-a",
            "the only node still reporting"
        );
        assert_eq!(resp.location(), scheduler::SandboxLocation::Placed);
        assert_eq!(
            resp.origin_node_id, "node-b",
            "the hint is reported as it still stands; the rewrite happens where the rebuild lands"
        );
        assert_eq!(resp.execution_id, "", "PLACED never carries an incarnation");
    }

    #[tokio::test]
    async fn lookup_node_keeps_a_resuming_row_on_a_live_origin() {
        let sandbox_id = SandboxId::new();
        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![
                node("node-a", "http://10.0.0.1:8000"),
                node("node-b", "http://10.0.0.2:8000"),
            ],
            Duration::from_secs(30),
        ));
        registry
            .heartbeat(&heartbeat_req("node-a", vec![]), SystemTime::now())
            .expect("node-a is in discovery");
        registry
            .heartbeat(&heartbeat_req("node-b", vec![]), SystemTime::now())
            .expect("node-b is in discovery");
        let paused: Arc<dyn PausedSandboxRegistry> = Arc::new(FakePausedRegistry::with_entry(
            claimed_resuming_row(sandbox_id, "node-b", true),
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
            .expect("the origin is live")
            .into_inner();

        assert_eq!(
            resp.node.as_ref().unwrap().node_id,
            "node-b",
            "the control: a live origin still holds the warm capture, and a resume in flight \
             there must not be routed away from it"
        );
        assert_eq!(resp.location(), scheduler::SandboxLocation::Bound);
    }

    #[tokio::test]
    async fn lookup_node_refuses_an_unpublished_resuming_row_whose_origin_is_gone() {
        let sandbox_id = SandboxId::new();
        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![
                node("node-a", "http://10.0.0.1:8000"),
                node("node-b", "http://10.0.0.2:8000"),
            ],
            Duration::from_secs(30),
        ));
        registry
            .heartbeat(&heartbeat_req("node-a", vec![]), SystemTime::now())
            .expect("node-a is in discovery");
        let paused: Arc<dyn PausedSandboxRegistry> = Arc::new(FakePausedRegistry::with_entry(
            claimed_resuming_row(sandbox_id, "node-b", false),
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
            .expect_err(
                "the control: with nothing published there is nothing another node could \
                 rebuild from, so naming one would send the resume somewhere it must fail",
            );
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    }

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

    // Placement arithmetic is covered in `placement`; these tests cover RPC wiring.

    use crate::node_registry::placement::ShadowPlacement;
    use metrics_util::debugging::{DebugValue, Snapshotter};

    /// Drains incremented counter series once; snapshots consume recorder state.
    struct DrainedCounters(StdHashMap<String, u64>);

    fn drain_counters(snapshotter: &Snapshotter) -> DrainedCounters {
        let mut out: StdHashMap<String, u64> = StdHashMap::new();
        for (composite, _unit, _description, value) in snapshotter.snapshot().into_vec() {
            let DebugValue::Counter(count) = value else {
                continue;
            };
            if count == 0 {
                continue;
            }
            let key = composite.key();
            let mut labels: Vec<String> = key
                .labels()
                .map(|label| format!("{}={}", label.key(), label.value()))
                .collect();
            labels.sort();
            *out.entry(format!("{}|{}", key.name(), labels.join(",")))
                .or_insert(0) += count;
        }
        DrainedCounters(out)
    }

    impl DrainedCounters {
        fn get(&self, name: &str, labels: &str) -> u64 {
            self.0
                .get(&format!("{name}|{labels}"))
                .copied()
                .unwrap_or(0)
        }

        fn series(&self, name: &str) -> StdHashMap<String, u64> {
            let prefix = format!("{name}|");
            self.0
                .iter()
                .filter_map(|(key, count)| {
                    key.strip_prefix(&prefix)
                        .map(|labels| (labels.to_string(), *count))
                })
                .collect()
        }
    }

    fn sized_snapshot(
        allocated_cpu: u32,
        cpu_count: u32,
        allocated_memory_bytes: u64,
        memory_total_bytes: u64,
    ) -> scheduler::NodeSnapshot {
        scheduler::NodeSnapshot {
            status: scheduler::NodeStatus::Ready as i32,
            allocated_cpu,
            allocated_memory_bytes,
            cpu_count,
            memory_total_bytes,
            ..Default::default()
        }
    }

    fn sized_heartbeat(node_id: &str, snapshot: scheduler::NodeSnapshot) -> HeartbeatRequest {
        let mut req = heartbeat_req(node_id, Vec::new());
        req.snapshot = Some(snapshot);
        req
    }

    const GIB: u64 = 1024 * 1024 * 1024;

    #[tokio::test]
    async fn the_shadow_disagrees_without_moving_the_placement() {
        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let guard = metrics::set_default_local_recorder(&recorder);

        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![
                node("node-a", "http://10.0.0.1:8000"),
                node("node-z", "http://10.0.0.2:8000"),
            ],
            Duration::from_secs(30),
        ));
        registry
            .heartbeat(
                &sized_heartbeat("node-a", sized_snapshot(7, 8, 8 * GIB, 64 * GIB)),
                SystemTime::now(),
            )
            .expect("node-a heartbeats");
        registry
            .heartbeat(
                &sized_heartbeat("node-z", sized_snapshot(0, 8, 8 * GIB, 64 * GIB)),
                SystemTime::now(),
            )
            .expect("node-z heartbeats");

        let service = NodeRegistryGrpcService::new(Arc::clone(&registry), warm_gate(&registry))
            // K = 2 over scripted candidates [node-a, node-z].
            .with_placement_shadow(ShadowPlacement::new(2).with_scripted_rng([0, 0]));

        let placed = service
            .schedule(Request::new(ScheduleRequest {
                hint: Some(scheduler::ScheduleRequestHint {
                    kind: Some(scheduler::schedule_request_hint::Kind::NewSandbox(
                        scheduler::NewSandboxHint {
                            metadata: Default::default(),
                            cpu_count: Some(1),
                            memory_mib: Some(1),
                        },
                    )),
                }),
            }))
            .await
            .expect("two discovered nodes")
            .into_inner();

        drop(guard);

        assert_eq!(
            placed.node.expect("a node was chosen").node_id,
            "node-a",
            "the round-robin cursor decides placement; the shadow must not"
        );
        let counters = drain_counters(&snapshotter);
        let agreement = counters.series("agentenv_api_placement_shadow_agreement_total");
        assert_eq!(
            counters.get(
                "agentenv_api_placement_shadow_agreement_total",
                "agrees=false,source=schedule"
            ),
            1,
            "the shadow must have run and disagreed: {agreement:?}"
        );
        assert_eq!(
            counters.get(
                "agentenv_api_placement_shadow_agreement_total",
                "agrees=true,source=schedule"
            ),
            0,
            "the shadow picked node-z, so nothing may be filed as agreement: {agreement:?}"
        );
        assert_eq!(
            counters.get(
                "agentenv_api_placement_shadow_classification_total",
                "class=scored,source=schedule"
            ),
            2,
            "{:?}",
            counters.series("agentenv_api_placement_shadow_classification_total")
        );
        assert!(
            counters
                .series("agentenv_api_placement_missing_request_resources_total")
                .is_empty(),
            "a fully stated hint must not count as missing"
        );
    }

    #[tokio::test]
    async fn place_new_hands_placement_the_resources_it_was_given() {
        use crate::node_client::{NativeNodePlacement, NodePlacement};
        use crate::types::SandboxResources;

        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let guard = metrics::set_default_local_recorder(&recorder);

        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![
                node("node-a", "http://10.0.0.1:8000"),
                node("node-z", "http://10.0.0.2:8000"),
            ],
            Duration::from_secs(30),
        ));
        registry
            .heartbeat(
                &sized_heartbeat("node-a", sized_snapshot(0, 8, 512 * 1024 * 1024, GIB)),
                SystemTime::now(),
            )
            .expect("node-a heartbeats");
        registry
            .heartbeat(
                &sized_heartbeat("node-z", sized_snapshot(5, 8, 8 * GIB, 64 * GIB)),
                SystemTime::now(),
            )
            .expect("node-z heartbeats");

        let service = NodeRegistryGrpcService::new(Arc::clone(&registry), warm_gate(&registry))
            .with_binding_store(in_memory_binding_store(), false, Duration::ZERO)
            .with_placement_shadow(ShadowPlacement::new(2).with_scripted_rng([0, 0]));
        let placement =
            NativeNodePlacement::new(Arc::clone(&registry), 8001, warm_gate(&registry), service);

        let chosen = placement
            .place_new(
                SandboxId::new(),
                SandboxResources {
                    cpu_count: 2,
                    memory_mib: 512,
                    disk_size_mib: 1024,
                },
            )
            .await
            .expect("two discovered nodes");

        drop(guard);

        assert_eq!(chosen.node_id, "node-a", "round-robin still decides");
        let counters = drain_counters(&snapshotter);
        let agreement = counters.series("agentenv_api_placement_shadow_agreement_total");
        assert_eq!(
            counters.get(
                "agentenv_api_placement_shadow_agreement_total",
                "agrees=false,source=schedule"
            ),
            1,
            "the shadow saw a 2 vCPU / 512 MiB request and preferred node-z; \
             a producer that dropped its resources would have made it agree: {agreement:?}"
        );
        assert!(
            counters
                .series("agentenv_api_placement_missing_request_resources_total")
                .is_empty(),
            "the production producer always states both fields"
        );
    }

    /// Covers every request-hint shape, including an unknown oneof tag decoded from bytes.
    #[tokio::test]
    async fn every_hint_shape_maps_onto_the_missing_request_resources_counter() {
        use prost::Message as _;

        let raw_unknown_kind =
            ScheduleRequest::decode(&[0x12u8, 0x02, 0x1a, 0x00][..]).expect("a decodable request");
        assert!(
            raw_unknown_kind.hint.is_some(),
            "the outer hint must survive"
        );
        assert!(
            raw_unknown_kind
                .hint
                .as_ref()
                .expect("outer hint")
                .kind
                .is_none(),
            "an unknown oneof tag must leave `kind` empty rather than fail the decode"
        );

        let new_sandbox = |cpu_count, memory_mib| ScheduleRequest {
            hint: Some(scheduler::ScheduleRequestHint {
                kind: Some(scheduler::schedule_request_hint::Kind::NewSandbox(
                    scheduler::NewSandboxHint {
                        metadata: Default::default(),
                        cpu_count,
                        memory_mib,
                    },
                )),
            }),
        };

        let cases: Vec<(&str, ScheduleRequest, bool)> = vec![
            ("hint = None", ScheduleRequest { hint: None }, true),
            ("Some(hint { kind: None })", raw_unknown_kind, true),
            ("NewSandbox { None, None }", new_sandbox(None, None), true),
            ("CPU-only", new_sandbox(Some(4), None), true),
            ("memory-only", new_sandbox(None, Some(2048)), true),
            ("both stated", new_sandbox(Some(2), Some(512)), false),
            ("explicit zeroes", new_sandbox(Some(0), Some(0)), false),
            (
                "NewColdSandbox",
                ScheduleRequest {
                    hint: Some(scheduler::ScheduleRequestHint {
                        kind: Some(scheduler::schedule_request_hint::Kind::NewColdSandbox(
                            scheduler::NewColdSandboxHint {
                                cpu_count: 3,
                                memory_mb: 4096,
                                images: Vec::new(),
                                metadata: Default::default(),
                            },
                        )),
                    }),
                },
                false,
            ),
        ];

        for (label, request, expect_missing) in cases {
            let recorder = metrics_util::debugging::DebuggingRecorder::new();
            let snapshotter = recorder.snapshotter();
            let guard = metrics::set_default_local_recorder(&recorder);

            let registry = Arc::new(AtomicNodeRegistry::new(
                vec![node("node-a", "http://10.0.0.1:8000")],
                Duration::from_secs(30),
            ));
            registry
                .heartbeat(
                    &sized_heartbeat("node-a", sized_snapshot(0, 8, 0, 8 * GIB)),
                    SystemTime::now(),
                )
                .expect("node-a heartbeats");
            let service = NodeRegistryGrpcService::new(Arc::clone(&registry), warm_gate(&registry));
            service
                .schedule(Request::new(request))
                .await
                .unwrap_or_else(|err| panic!("{label} must still place: {err}"));

            drop(guard);
            let counters = drain_counters(&snapshotter);
            assert_eq!(
                counters.get(
                    "agentenv_api_placement_missing_request_resources_total",
                    "source=schedule"
                ),
                u64::from(expect_missing),
                "{label}: missing-resources accounting is wrong: {:?}",
                counters.series("agentenv_api_placement_missing_request_resources_total")
            );
        }
    }

    #[tokio::test]
    async fn each_call_path_reports_under_its_own_source_and_a_preference_reports_nothing() {
        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![
                node("node-a", "http://10.0.0.1:8000"),
                node("node-b", "http://10.0.0.2:8000"),
            ],
            Duration::from_secs(30),
        ));
        for node_id in ["node-a", "node-b"] {
            registry
                .heartbeat(
                    &sized_heartbeat(node_id, sized_snapshot(0, 8, 0, 8 * GIB)),
                    SystemTime::now(),
                )
                .expect("heartbeat");
        }

        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let guard = metrics::set_default_local_recorder(&recorder);

        let unpreferred = SandboxId::new();
        let service = NodeRegistryGrpcService::new(Arc::clone(&registry), warm_gate(&registry))
            .with_binding_store(in_memory_binding_store(), false, Duration::ZERO)
            .with_paused_registry(Arc::new(FakePausedRegistry::with_entry(
                paused_entry(unpreferred, PausedRegistryState::Paused, "", None),
                true,
            )));
        service
            .schedule(Request::new(ScheduleRequest { hint: None }))
            .await
            .expect("two discovered nodes");
        service
            .lookup_node(Request::new(LookupNodeRequest {
                sandbox_id: unpreferred.to_string(),
            }))
            .await
            .expect("a paused row with no origin preference");

        drop(guard);
        let agreement =
            drain_counters(&snapshotter).series("agentenv_api_placement_shadow_agreement_total");
        let total_under = |source: &str| -> u64 {
            agreement
                .iter()
                .filter(|(labels, _)| labels.ends_with(&format!("source={source}")))
                .map(|(_, count)| *count)
                .sum()
        };
        assert_eq!(total_under("schedule"), 1, "{agreement:?}");
        assert_eq!(total_under("paused_lookup"), 1, "{agreement:?}");

        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let guard = metrics::set_default_local_recorder(&recorder);

        let preferred = SandboxId::new();
        let service = NodeRegistryGrpcService::new(Arc::clone(&registry), warm_gate(&registry))
            .with_binding_store(in_memory_binding_store(), false, Duration::ZERO)
            .with_paused_registry(Arc::new(FakePausedRegistry::with_entry(
                paused_entry(preferred, PausedRegistryState::Paused, "node-b", None),
                true,
            )));
        let answer = service
            .lookup_node(Request::new(LookupNodeRequest {
                sandbox_id: preferred.to_string(),
            }))
            .await
            .expect("a paused row preferring node-b")
            .into_inner();

        drop(guard);
        assert_eq!(answer.node.expect("a node").node_id, "node-b");
        let counters = drain_counters(&snapshotter);
        assert!(
            counters
                .series("agentenv_api_placement_shadow_agreement_total")
                .is_empty(),
            "a preferred node short-circuits the strategy, so it must not be scored"
        );
        assert!(
            counters
                .series("agentenv_api_placement_shadow_classification_total")
                .is_empty(),
            "and it must not classify the candidates either"
        );
    }

    #[tokio::test]
    async fn a_never_reported_node_stays_a_candidate_and_classifies_as_no_snapshot() {
        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let guard = metrics::set_default_local_recorder(&recorder);

        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![
                node("node-a", "http://10.0.0.1:8000"),
                node("node-silent", "http://10.0.0.2:8000"),
            ],
            Duration::from_secs(30),
        ));
        registry
            .heartbeat(
                &sized_heartbeat("node-a", sized_snapshot(0, 8, 0, 8 * GIB)),
                SystemTime::now(),
            )
            .expect("node-a heartbeats");

        let service = NodeRegistryGrpcService::new(Arc::clone(&registry), warm_gate(&registry))
            .with_placement_shadow(ShadowPlacement::new(2).with_scripted_rng([0, 0]));
        let mut placed = Vec::new();
        for _ in 0..2 {
            placed.push(
                service
                    .schedule(Request::new(ScheduleRequest { hint: None }))
                    .await
                    .expect("two discovered nodes")
                    .into_inner()
                    .node
                    .expect("a node")
                    .node_id,
            );
        }

        drop(guard);
        assert_eq!(placed, vec!["node-a", "node-silent"]);
        let counters = drain_counters(&snapshotter);
        let classification = counters.series("agentenv_api_placement_shadow_classification_total");
        assert_eq!(
            counters.get(
                "agentenv_api_placement_shadow_classification_total",
                "class=no_snapshot,source=schedule"
            ),
            2,
            "one silent candidate per call: {classification:?}"
        );
        assert_eq!(
            counters.get(
                "agentenv_api_placement_shadow_classification_total",
                "class=scored,source=schedule"
            ),
            2,
            "and one scoreable candidate per call: {classification:?}"
        );
    }

    #[tokio::test]
    async fn a_concurrent_burst_distributes_exactly_as_round_robin() {
        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![
                node("node-a", "http://10.0.0.1:8000"),
                node("node-b", "http://10.0.0.2:8000"),
                node("node-c", "http://10.0.0.3:8000"),
            ],
            Duration::from_secs(30),
        ));
        for node_id in ["node-a", "node-b", "node-c"] {
            registry
                .heartbeat(
                    &sized_heartbeat(node_id, sized_snapshot(0, 8, 0, 8 * GIB)),
                    SystemTime::now(),
                )
                .expect("heartbeat");
        }
        let service = NodeRegistryGrpcService::new(Arc::clone(&registry), warm_gate(&registry));

        let mut handles = Vec::new();
        for _ in 0..90 {
            let service = service.clone();
            handles.push(tokio::spawn(async move {
                service
                    .schedule(Request::new(ScheduleRequest { hint: None }))
                    .await
                    .expect("three discovered nodes")
                    .into_inner()
                    .node
                    .expect("a node")
                    .node_id
            }));
        }

        let mut counts: StdHashMap<String, usize> = StdHashMap::new();
        for handle in handles {
            *counts
                .entry(handle.await.expect("the task joined"))
                .or_insert(0) += 1;
        }
        assert_eq!(counts.get("node-a").copied(), Some(30), "{counts:?}");
        assert_eq!(counts.get("node-b").copied(), Some(30), "{counts:?}");
        assert_eq!(counts.get("node-c").copied(), Some(30), "{counts:?}");
    }
}
