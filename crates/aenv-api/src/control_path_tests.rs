//! The api half's own assembly, driven against a node on a real socket.
//!
//! What is assembled here is what `bin/aenv-api.rs` assembles: `SandboxControl`
//! over `NativeNodePlacement`, the binding store behind it,
//! `PlacementRuntimeRouting` and `CommittingPausePublisher`. The metadata store
//! is the in-memory one because the store is not the subject; the Redis
//! contract suite covers that.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use tokio::sync::oneshot;
use tonic::{Request, Response, Status};

use crate::binding_store::{
    BindingState, BindingStore, BindingStoreSettings, InMemoryBindingStore,
};
use crate::control::{SandboxControl, SandboxControlParts};
use crate::node_client::placement::{NodeEndpoint, NodePlacement};
use crate::node_client::{NativeNodePlacement, PlacementRuntimeRouting, StoreRecordOwner};
use crate::node_registry::grpc_service::NodeRegistryGrpcService;
use crate::node_registry::registry::AtomicNodeRegistry;
use crate::node_registry::types::Node;
use crate::node_registry::warmup::WarmupGate;
use crate::orchestrator::store::SandboxMetadata;
use crate::orchestrator::{
    CommittingPausePublisher, CreateSandboxRequest, GrantIssuer, InMemoryMetadataStore,
    MetadataStore, SandboxExpiry, SandboxLaunchSource, SandboxState, SandboxTimeoutAction,
};
use crate::proto::node as pb;
use crate::proto::node::node_sandbox_service_server::{
    NodeSandboxService, NodeSandboxServiceServer,
};
use crate::proto::scheduler::scheduler_server::Scheduler;
use crate::sandbox::AccessTokenSeedPolicy;
use crate::snapshot::mock::{recording_snapshot_manager, RecordingSnapshotRepository};
use crate::snapshot::repository::{SnapshotCommit, StagedSnapshot};
use crate::snapshot::{CommittedSnapshot, SnapshotId, SnapshotPublishSource, SnapshotRecord};
use crate::types::{ExecutionId, SandboxId};

const NODE_ID: &str = "node-under-test";
const CLUSTER_ID: &str = "cluster-under-test";

// The value the shipped kustomization sets, kept honest by
// `the_fixture_runs_the_projection_authoritative_value_the_cluster_deploys`.
const DEPLOYED_PROJECTION_AUTHORITATIVE: bool = true;

/// The api half as `bin/aenv-api.rs` wires it, plus the two doubles the test
/// reads its decisions out of.
struct ApiHalf {
    orchestrator: Arc<SandboxControl<TimelineStore>>,
    routing: Arc<RecordingRouting>,
    store: TimelineStore,
    bindings: Arc<InMemoryBindingStore>,
    snapshots: Arc<RecordingSnapshotRepository>,
    grants: Arc<RecordingGrants>,
    calls: Arc<Mutex<Vec<String>>>,
    launch_calls: Arc<Mutex<Vec<String>>>,
    node: Arc<ScriptedNode>,
    _node_shutdown: oneshot::Sender<()>,
}

/// Records what the orchestrator granted and revoked, in order.
#[derive(Default)]
struct RecordingGrants {
    granted: Mutex<Vec<(SandboxId, ExecutionId, Vec<String>)>>,
    revoked: Mutex<Vec<(SandboxId, ExecutionId)>>,
}

#[async_trait::async_trait]
impl GrantIssuer for RecordingGrants {
    async fn grant(
        &self,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
        names: &std::collections::BTreeSet<String>,
    ) -> anyhow::Result<()> {
        self.granted.lock().expect("lock").push((
            sandbox_id,
            execution_id,
            names.iter().cloned().collect(),
        ));
        Ok(())
    }

    async fn revoke(&self, sandbox_id: SandboxId, execution_id: ExecutionId) -> anyhow::Result<()> {
        self.revoked
            .lock()
            .expect("lock")
            .push((sandbox_id, execution_id));
        Ok(())
    }
}

impl ApiHalf {
    async fn binding(&self, sandbox_id: SandboxId) -> Option<crate::binding_store::Binding> {
        self.bindings
            .get(&sandbox_id.to_string(), SystemTime::now())
            .await
            .expect("the binding store answers")
    }

    async fn record_state(&self, sandbox_id: SandboxId) -> Option<SandboxState> {
        self.orchestrator
            .get_sandbox(&sandbox_id)
            .await
            .expect("the metadata store answers")
            .map(|metadata| metadata.state)
    }

    fn placement_calls(&self) -> Vec<String> {
        self.calls.lock().expect("lock").clone()
    }

    fn launch_reservation_calls(&self) -> Vec<String> {
        self.launch_calls.lock().expect("lock").clone()
    }

    fn creates_seen(&self) -> Vec<pb::SandboxCreateRequest> {
        self.node.seen_create.lock().expect("lock").clone()
    }

    fn granted(&self) -> Vec<(SandboxId, ExecutionId, Vec<String>)> {
        self.grants.granted.lock().expect("lock").clone()
    }

    fn revoked(&self) -> Vec<(SandboxId, ExecutionId)> {
        self.grants.revoked.lock().expect("lock").clone()
    }

    /// Fixes what the cluster answers about every sandbox's runtime.
    fn routing_answers(&self, routed: bool) {
        *self.routing.routed.lock().expect("lock") = Some(routed);
    }
}

/// Retires bindings for real, and records whether the sandbox's record was
/// still in the store at the moment each retirement was asked for.
struct RecordingRouting {
    inner: Arc<dyn crate::orchestrator::RuntimeRouting>,
    store: InMemoryMetadataStore,
    forgotten: Mutex<Vec<(SandboxId, ExecutionId, bool)>>,
    /// A fixed verdict, for a test about what an unrouted runtime does to a
    /// record rather than about the binding store's own answer.
    routed: Mutex<Option<bool>>,
}

impl RecordingRouting {
    fn forgotten(&self) -> Vec<(SandboxId, ExecutionId, bool)> {
        self.forgotten.lock().expect("lock").clone()
    }
}

#[async_trait::async_trait]
impl crate::orchestrator::RuntimeRouting for RecordingRouting {
    async fn is_routed(&self, sandbox_id: SandboxId) -> anyhow::Result<bool> {
        if let Some(routed) = *self.routed.lock().expect("lock") {
            return Ok(routed);
        }
        self.inner.is_routed(sandbox_id).await
    }

    async fn forget(&self, sandbox_id: SandboxId, execution_id: ExecutionId) -> anyhow::Result<()> {
        let record_present = self
            .store
            .get(&sandbox_id)
            .await
            .expect("the in-memory store never fails")
            .is_some();
        self.forgotten
            .lock()
            .expect("lock")
            .push((sandbox_id, execution_id, record_present));
        self.inner.forget(sandbox_id, execution_id).await
    }
}

/// The in-memory store with two seams: every record write lands on the launch
/// timeline beside the reservation calls, and one launch can be stopped inside
/// `add` so a test can see what the sandbox id still holds while it is there.
#[derive(Clone)]
struct TimelineStore {
    inner: InMemoryMetadataStore,
    timeline: Arc<Mutex<Vec<String>>>,
    add_gate: Arc<Mutex<Option<AddGate>>>,
}

/// Holds the next `add` until the test lets it through.
struct AddGate {
    reached: oneshot::Sender<()>,
    proceed: oneshot::Receiver<()>,
}

impl TimelineStore {
    /// Stops the next record write and reports when it is standing there.
    fn stop_the_next_add(&self) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (reached, reached_rx) = oneshot::channel();
        let (proceed_tx, proceed) = oneshot::channel();
        *self.add_gate.lock().expect("lock") = Some(AddGate { reached, proceed });
        (reached_rx, proceed_tx)
    }
}

#[async_trait::async_trait]
impl MetadataStore for TimelineStore {
    async fn add(&self, metadata: SandboxMetadata) -> crate::orchestrator::store::Result<()> {
        self.timeline
            .lock()
            .expect("lock")
            .push("store.add".to_string());
        let gate = self.add_gate.lock().expect("lock").take();
        if let Some(gate) = gate {
            let _ = gate.reached.send(());
            let _ = gate.proceed.await;
        }
        self.inner.add(metadata).await
    }

    async fn update(&self, metadata: SandboxMetadata) -> crate::orchestrator::store::Result<()> {
        self.inner.update(metadata).await
    }

    async fn update_state_if_state(
        &self,
        sandbox_id: &SandboxId,
        new_state: SandboxState,
        expected_states: &[SandboxState],
    ) -> crate::orchestrator::store::Result<SandboxState> {
        self.inner
            .update_state_if_state(sandbox_id, new_state, expected_states)
            .await
    }

    async fn update_if_state<F>(
        &self,
        sandbox_id: &SandboxId,
        expected_states: &[SandboxState],
        update: F,
    ) -> crate::orchestrator::store::Result<crate::orchestrator::store::MetadataUpdateResult>
    where
        F: FnOnce(&mut SandboxMetadata) + Send,
    {
        self.inner
            .update_if_state(sandbox_id, expected_states, update)
            .await
    }

    async fn get(
        &self,
        sandbox_id: &SandboxId,
    ) -> crate::orchestrator::store::Result<Option<SandboxMetadata>> {
        self.inner.get(sandbox_id).await
    }

    async fn remove(
        &self,
        sandbox_id: &SandboxId,
    ) -> crate::orchestrator::store::Result<Option<SandboxMetadata>> {
        self.inner.remove(sandbox_id).await
    }

    async fn remove_if_execution(
        &self,
        sandbox_id: &SandboxId,
        expected_execution_id: ExecutionId,
        expected_states: &[SandboxState],
    ) -> crate::orchestrator::store::Result<crate::orchestrator::store::FencedRemoval> {
        self.inner
            .remove_if_execution(sandbox_id, expected_execution_id, expected_states)
            .await
    }

    async fn list(&self) -> crate::orchestrator::store::Result<Vec<SandboxMetadata>> {
        self.inner.list().await
    }

    async fn list_with_callback<F>(&self, callback: F) -> crate::orchestrator::store::Result<()>
    where
        F: FnMut(&SandboxMetadata) + Send,
    {
        self.inner.list_with_callback(callback).await
    }

    async fn list_filtered(
        &self,
        filter: crate::orchestrator::store::SandboxListFilter,
    ) -> crate::orchestrator::store::Result<Vec<SandboxMetadata>> {
        self.inner.list_filtered(filter).await
    }

    async fn list_expired(
        &self,
        now: SystemTime,
    ) -> crate::orchestrator::store::Result<Vec<SandboxMetadata>> {
        self.inner.list_expired(now).await
    }

    async fn list_ids(&self) -> crate::orchestrator::store::Result<Vec<SandboxId>> {
        self.inner.list_ids().await
    }

    async fn expired_batch(
        &self,
        now: SystemTime,
        limit: usize,
    ) -> crate::orchestrator::store::Result<Vec<SandboxMetadata>> {
        self.inner.expired_batch(now, limit).await
    }

    async fn start_transition(
        &self,
        sandbox_id: &SandboxId,
        request: crate::orchestrator::store::TransitionRequest,
    ) -> crate::orchestrator::store::Result<crate::orchestrator::store::TransitionOutcome> {
        self.inner.start_transition(sandbox_id, request).await
    }

    async fn wait_while_in_states(
        &self,
        sandbox_id: &SandboxId,
        transitional_states: &[SandboxState],
    ) -> crate::orchestrator::store::Result<Option<SandboxMetadata>> {
        self.inner
            .wait_while_in_states(sandbox_id, transitional_states)
            .await
    }
}

/// Records every placement decision in order while doing the real thing.
struct RecordingPlacement {
    inner: Arc<dyn NodePlacement>,
    calls: Arc<Mutex<Vec<String>>>,
    /// Launch reservations are recorded apart from the node-bound decisions:
    /// they precede placement and belong to no node.
    launch_calls: Arc<Mutex<Vec<String>>>,
}

impl RecordingPlacement {
    fn note(&self, what: &str) {
        self.calls.lock().expect("lock").push(what.to_string());
    }

    fn note_launch(&self, what: &str) {
        self.launch_calls
            .lock()
            .expect("lock")
            .push(what.to_string());
    }
}

#[async_trait::async_trait]
impl NodePlacement for RecordingPlacement {
    async fn place_new(
        &self,
        sandbox_id: SandboxId,
        resources: crate::types::SandboxResources,
        preferred_node_id: Option<&str>,
        excluded_node_ids: &[String],
    ) -> anyhow::Result<NodeEndpoint> {
        self.note("place_new");
        self.inner
            .place_new(sandbox_id, resources, preferred_node_id, excluded_node_ids)
            .await
    }

    async fn place_new_with(
        &self,
        sandbox_id: SandboxId,
        resources: crate::types::SandboxResources,
        preferred_node_id: Option<&str>,
        excluded_node_ids: &[String],
        needs: crate::node_client::placement::PlacementNeeds,
    ) -> anyhow::Result<NodeEndpoint> {
        self.note("place_new");
        self.inner
            .place_new_with(
                sandbox_id,
                resources,
                preferred_node_id,
                excluded_node_ids,
                needs,
            )
            .await
    }

    async fn place_existing(&self, sandbox_id: SandboxId) -> anyhow::Result<Option<NodeEndpoint>> {
        self.note("place_existing");
        self.inner.place_existing(sandbox_id).await
    }

    async fn resolve_node(&self, node_id: &str) -> anyhow::Result<NodeEndpoint> {
        self.note("resolve_node");
        self.inner.resolve_node(node_id).await
    }

    async fn node_membership(
        &self,
        node_id: &str,
    ) -> anyhow::Result<crate::node_client::placement::NodeMembership> {
        self.note("node_membership");
        self.inner.node_membership(node_id).await
    }

    async fn record_placement(
        &self,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
        node: &NodeEndpoint,
        projection_ttl_secs: u32,
    ) -> anyhow::Result<()> {
        self.note("record_placement");
        self.inner
            .record_placement(sandbox_id, execution_id, node, projection_ttl_secs)
            .await
    }

    async fn reserve_placement(
        &self,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
        node: &NodeEndpoint,
    ) -> anyhow::Result<()> {
        self.note("reserve_placement");
        self.inner
            .reserve_placement(sandbox_id, execution_id, node)
            .await
    }

    async fn release_placement_reservation(
        &self,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
    ) -> anyhow::Result<()> {
        self.note("release_placement_reservation");
        self.inner
            .release_placement_reservation(sandbox_id, execution_id)
            .await
    }

    async fn reserve_launch(
        &self,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
    ) -> anyhow::Result<()> {
        self.note_launch("reserve_launch");
        self.inner.reserve_launch(sandbox_id, execution_id).await
    }

    async fn release_launch(
        &self,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
    ) -> anyhow::Result<()> {
        self.note_launch("release_launch");
        self.inner.release_launch(sandbox_id, execution_id).await
    }

    async fn forget_placement(
        &self,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
    ) -> anyhow::Result<()> {
        self.note("forget_placement");
        self.inner.forget_placement(sandbox_id, execution_id).await
    }
}

/// What the node answers, and what it saw. `create` succeeds by echoing the
/// incarnation the api half minted unless a test scripts otherwise.
#[derive(Default)]
struct ScriptedNode {
    create: Mutex<Option<Result<pb::SandboxCreateResponse, Status>>>,
    pause: Mutex<Option<Result<pb::SandboxPauseResponse, Status>>>,
    delete: Mutex<Option<Result<pb::SandboxDeleteResponse, Status>>>,
    /// Read inside the create handler, so what it sees is the state the api
    /// half had reached before the node was asked to start anything.
    bindings_when_created: Mutex<Vec<Option<BindingState>>>,
    binding_store: Mutex<Option<Arc<InMemoryBindingStore>>>,
    seen_create: Mutex<Vec<pb::SandboxCreateRequest>>,
    seen_pause: Mutex<Vec<pb::SandboxPauseRequest>>,
    seen_delete: Mutex<Vec<pb::SandboxDeleteRequest>>,
}

#[derive(Clone)]
struct ScriptedNodeService(Arc<ScriptedNode>);

#[tonic::async_trait]
impl NodeSandboxService for ScriptedNodeService {
    async fn create(
        &self,
        request: Request<pb::SandboxCreateRequest>,
    ) -> Result<Response<pb::SandboxCreateResponse>, Status> {
        let request = request.into_inner();
        let store = self.0.binding_store.lock().expect("lock").clone();
        let seen = match store {
            Some(store) => store
                .get(&request.sandbox_id, SystemTime::now())
                .await
                .expect("the binding store answers")
                .map(|binding| binding.state),
            None => None,
        };
        self.0
            .bindings_when_created
            .lock()
            .expect("lock")
            .push(seen);
        let scripted = self.0.create.lock().expect("lock").take();
        self.0
            .seen_create
            .lock()
            .expect("lock")
            .push(request.clone());
        match scripted {
            Some(answer) => answer.map(Response::new),
            None => Ok(Response::new(pb::SandboxCreateResponse {
                sandbox_id: request.sandbox_id,
                execution_id: request.execution_id,
                // A sandbox with no interaction address has nowhere for the
                // proxy to send traffic, and the launch is rolled back.
                host_interaction_ip: "10.0.0.2".to_string(),
                ..Default::default()
            })),
        }
    }

    async fn delete(
        &self,
        request: Request<pb::SandboxDeleteRequest>,
    ) -> Result<Response<pb::SandboxDeleteResponse>, Status> {
        self.0
            .seen_delete
            .lock()
            .expect("lock")
            .push(request.into_inner());
        match self.0.delete.lock().expect("lock").take() {
            Some(answer) => answer.map(Response::new),
            None => Ok(Response::new(pb::SandboxDeleteResponse {})),
        }
    }

    async fn pause(
        &self,
        request: Request<pb::SandboxPauseRequest>,
    ) -> Result<Response<pb::SandboxPauseResponse>, Status> {
        self.0
            .seen_pause
            .lock()
            .expect("lock")
            .push(request.into_inner());
        match self.0.pause.lock().expect("lock").take() {
            Some(answer) => answer.map(Response::new),
            None => Err(Status::unimplemented("no pause was scripted")),
        }
    }

    async fn checkpoint(
        &self,
        _request: Request<pb::SandboxCheckpointRequest>,
    ) -> Result<Response<pb::SandboxCheckpointResponse>, Status> {
        Err(Status::unimplemented("no checkpoint was scripted"))
    }

    async fn fork(
        &self,
        _request: Request<pb::SandboxForkRequest>,
    ) -> Result<Response<pb::SandboxForkResponse>, Status> {
        Err(Status::unimplemented("no fork was scripted"))
    }

    async fn update_network(
        &self,
        _request: Request<pb::SandboxNetworkRequest>,
    ) -> Result<Response<pb::SandboxNetworkResponse>, Status> {
        Ok(Response::new(pb::SandboxNetworkResponse {}))
    }

    async fn update_params(
        &self,
        _request: Request<pb::SandboxParamsRequest>,
    ) -> Result<Response<pb::SandboxParamsResponse>, Status> {
        Ok(Response::new(pb::SandboxParamsResponse {}))
    }

    async fn describe(
        &self,
        request: Request<pb::SandboxDescribeRequest>,
    ) -> Result<Response<pb::SandboxDescribeResponse>, Status> {
        Err(Status::not_found(format!(
            "sandbox {} is not running on this node",
            request.into_inner().sandbox_id
        )))
    }

    async fn list_sandboxes(
        &self,
        _request: Request<pb::ListSandboxesRequest>,
    ) -> Result<Response<pb::SandboxListResponse>, Status> {
        Ok(Response::new(pb::SandboxListResponse {
            sandboxes: Vec::new(),
        }))
    }

    async fn override_status(
        &self,
        _request: Request<pb::NodeStatusOverrideRequest>,
    ) -> Result<Response<pb::NodeStatusOverrideResponse>, Status> {
        Err(Status::unimplemented("no status override was scripted"))
    }

    async fn build_template(
        &self,
        _request: Request<pb::TemplateBuildRequest>,
    ) -> Result<Response<pb::TemplateBuildResponse>, Status> {
        Err(Status::unimplemented("no template build was scripted"))
    }
}

async fn api_half() -> ApiHalf {
    crate::logging::init_for_tests();

    let node = Arc::new(ScriptedNode::default());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a port");
    let addr: SocketAddr = listener.local_addr().expect("the bound address");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let service = ScriptedNodeService(Arc::clone(&node));
    tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(NodeSandboxServiceServer::new(service))
            .serve_with_incoming_shutdown(
                tonic::transport::server::TcpIncoming::from(listener),
                async {
                    let _ = shutdown_rx.await;
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

    let registry = Arc::new(AtomicNodeRegistry::new(
        vec![Node {
            id: NODE_ID.to_string(),
            endpoint: format!("http://{addr}"),
            pod_name: String::new(),
        }],
        Duration::from_secs(30),
    ));
    let warmup = Arc::new(WarmupGate::new(
        Arc::clone(&registry) as Arc<dyn crate::node_registry::registry::NodeRegistry>,
        Duration::from_secs(1),
        SystemTime::UNIX_EPOCH,
    ));
    warmup.reported_in(SystemTime::now());
    let bindings = Arc::new(InMemoryBindingStore::new(BindingStoreSettings::default()));
    // The deployed values rather than picked ones: the TTL ceiling is the shipped
    // compiled default, while `projection_authoritative` -- what a heartbeat does to a
    // binding's deadline -- is a kustomization literal `AppConfig::default()` cannot see.
    let binding_config = crate::cfg::AppConfig::default().binding_store;
    let grpc_service = NodeRegistryGrpcService::new(Arc::clone(&registry), Arc::clone(&warmup))
        .with_binding_store(
            Arc::clone(&bindings) as Arc<dyn BindingStore>,
            DEPLOYED_PROJECTION_AUTHORITATIVE,
            Duration::from_secs(binding_config.max_projection_ttl_secs),
        );
    *node.binding_store.lock().expect("lock") = Some(Arc::clone(&bindings));

    // The node reports in the way a real one does. A policy that names a secret
    // is placed only on a node whose latest heartbeat says it can broker.
    Scheduler::heartbeat(
        &grpc_service,
        Request::new(crate::proto::scheduler::HeartbeatRequest {
            node_id: NODE_ID.to_string(),
            cluster_id: CLUSTER_ID.to_string(),
            service_instance_id: format!("{NODE_ID}-instance"),
            snapshot: Some(crate::proto::scheduler::NodeSnapshot {
                status: crate::proto::scheduler::NodeStatus::Ready as i32,
                egress_broker: crate::proto::scheduler::EgressBrokerState::LocalOk as i32,
                ..Default::default()
            }),
            ..Default::default()
        }),
    )
    .await
    .expect("the node under test reports in");

    let calls: Arc<Mutex<Vec<String>>> = Arc::default();
    let launch_calls: Arc<Mutex<Vec<String>>> = Arc::default();
    let placement: Arc<dyn NodePlacement> = Arc::new(RecordingPlacement {
        inner: Arc::new(NativeNodePlacement::new(
            registry,
            addr.port(),
            warmup,
            grpc_service,
        )),
        calls: Arc::clone(&calls),
        launch_calls: Arc::clone(&launch_calls),
    });

    let inner_store = InMemoryMetadataStore::new();
    let store = TimelineStore {
        inner: inner_store.clone(),
        timeline: Arc::clone(&launch_calls),
        add_gate: Arc::default(),
    };
    let record_owner = StoreRecordOwner::shared(store.clone());
    let grants = Arc::new(RecordingGrants::default());
    let (snapshot_manager, snapshots) = recording_snapshot_manager();
    let routing = Arc::new(RecordingRouting {
        inner: PlacementRuntimeRouting::shared(Arc::clone(&placement)),
        store: inner_store,
        forgotten: Mutex::new(Vec::new()),
        routed: Mutex::new(None),
    });
    let orchestrator = SandboxControl::new(SandboxControlParts {
        // The one thing this assembly cannot reproduce: `MustBeConfigured`
        // needs the shared seed, which no test process has.
        access_token_seed_policy: AccessTokenSeedPolicy::MayGenerate,
        store: store.clone(),
        placement: Arc::clone(&placement),
        record_owner,
        routing: Arc::clone(&routing) as Arc<dyn crate::orchestrator::RuntimeRouting>,
        pause_publisher: Arc::new(CommittingPausePublisher::new(Arc::new(snapshot_manager))),
        grants: Arc::clone(&grants) as Arc<dyn GrantIssuer>,
    })
    .await
    .expect("the api half assembles");

    ApiHalf {
        orchestrator,
        routing,
        store,
        bindings,
        snapshots,
        grants,
        calls,
        launch_calls,
        node,
        _node_shutdown: shutdown_tx,
    }
}

/// A cold create: the api half holds an image reference and nothing resolved.
fn cold_create() -> CreateSandboxRequest {
    CreateSandboxRequest {
        source: SandboxLaunchSource::UnresolvedImage {
            image_ref: "registry.invalid/base:latest".to_string(),
            resources: Default::default(),
            attached_drives: Vec::new(),
            extra_boot_args: None,
        },
        // Long enough that no eviction tick can be what moved a sandbox out
        // from under an assertion.
        expiry: SandboxExpiry::After(Duration::from_secs(600)),
        timeout_action: SandboxTimeoutAction::Pause,
        auto_resume: false,
        user_metadata: None,
        env_vars: None,
        network_policy: Default::default(),
        secure: false,
        traffic_access_token: None,
        custom_extension_params: None,
        control_plane_config: None,
        execution_id: None,
        preferred_node_id: None,
    }
}

/// A create whose policy names one secret: the shape in which starting a
/// sandbox includes issuing a grant for it.
fn cold_create_naming_a_secret() -> CreateSandboxRequest {
    let mut rules = std::collections::BTreeMap::new();
    rules.insert(
        "api.example.com".to_string(),
        vec![crate::sandbox::network::policy::DomainRule {
            transform: crate::sandbox::network::policy::HeaderTransform {
                headers: [(
                    "authorization".to_string(),
                    "Bearer ${aenv.secrets.openai}".to_string(),
                )]
                .into_iter()
                .collect(),
            },
        }],
    );
    CreateSandboxRequest {
        network_policy: crate::sandbox::SandboxNetworkPolicy::new(
            Default::default(),
            crate::sandbox::SandboxNetworkEgressPolicy::with_rules(None, None, Some(rules))
                .expect("the policy validates"),
        ),
        ..cold_create()
    }
}

/// A resume: the api half holds the pause row and names the node that staged it.
fn resume_from(record: SnapshotRecord) -> CreateSandboxRequest {
    CreateSandboxRequest {
        source: SandboxLaunchSource::SnapshotRecord(Box::new(record)),
        preferred_node_id: Some(NODE_ID.to_string()),
        ..cold_create()
    }
}

fn paused_row() -> SnapshotRecord {
    SnapshotRecord::mock_ready(CommittedSnapshot::mock())
}

fn staged_by_the_node(sandbox_id: SandboxId) -> pb::SandboxPauseResponse {
    let staged = StagedSnapshot {
        commit: SnapshotCommit {
            id: SnapshotId::generate(),
            alias: None,
            source: SnapshotPublishSource::Sandbox {
                source_sandbox_id: sandbox_id.to_string(),
            },
            resources: Default::default(),
            created_at_unix_ms: Some(1_700_000_000_000),
            origin_node_id: Some(NODE_ID.to_string()),
            committed: CommittedSnapshot::mock(),
        },
        staged_at_unix_ms: 1_700_000_000_000,
        origin_node_id: NODE_ID.to_string(),
    };
    pb::SandboxPauseResponse {
        staged: Some(pb::StagedSnapshot {
            value: Some(
                crate::node_client::wire::serialize(&staged, "staged snapshot")
                    .expect("encode the staged row"),
            ),
        }),
    }
}

#[test]
fn the_fixture_runs_the_projection_authoritative_value_the_cluster_deploys() {
    // A kustomize literal, anchored at the start of the list item so a line that
    // merely mentions the key does not answer for the one that sets it.
    const LITERAL: &str = "- AENV_BINDING_STORE_PROJECTION_AUTHORITATIVE=";
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../deploy/k8s/base/kustomization.yaml");
    let manifest = std::fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("read {}: {err}", path.display()));

    let set: Vec<&str> = manifest
        .lines()
        .filter_map(|line| line.trim_start().strip_prefix(LITERAL))
        .collect();
    assert_eq!(
        set.len(),
        1,
        "expected exactly one kustomize literal setting the switch, found {set:?} -- the \
         fixture below claims to run what the cluster runs and can no longer tell"
    );
    let deployed: bool = set[0]
        .trim()
        .parse()
        .unwrap_or_else(|err| panic!("{LITERAL}{} is not a bool: {err}", set[0]));
    assert_eq!(
        deployed, DEPLOYED_PROJECTION_AUTHORITATIVE,
        "the deployment changed this switch and the fixture kept the old value, so every \
         binding-deadline assertion below is being made against a value nothing runs"
    );
}

#[tokio::test]
async fn a_cold_create_reserves_the_binding_before_the_node_is_asked_and_confirms_it_after() {
    let half = api_half().await;

    let metadata = Arc::clone(&half.orchestrator)
        .create_sandbox(cold_create())
        .await
        .expect("the api half creates a sandbox on the node");

    assert_eq!(
        half.placement_calls(),
        vec![
            "place_new".to_string(),
            "reserve_placement".to_string(),
            "record_placement".to_string(),
        ],
        "placement is asked, the binding is reserved, and only then is it confirmed"
    );

    let creates = half.creates_seen();
    assert_eq!(
        creates.len(),
        1,
        "the node was not asked to create anything"
    );
    assert_eq!(creates[0].sandbox_id, metadata.id.to_string());
    assert_eq!(creates[0].execution_id, metadata.execution_id.to_string());
    assert!(
        matches!(
            &creates[0].source,
            Some(pb::sandbox_create_request::Source::Image(image))
                if image.image_ref == "registry.invalid/base:latest"
        ),
        "a cold create ships the reference for the node to resolve: {:?}",
        creates[0].source
    );

    assert_eq!(
        half.node
            .bindings_when_created
            .lock()
            .expect("lock")
            .clone(),
        vec![Some(BindingState::Starting)],
        "no runtime may exist before a record of it does"
    );

    assert_eq!(
        half.record_state(metadata.id).await,
        Some(SandboxState::Running)
    );
    let binding = half.binding(metadata.id).await.expect("a routing binding");
    assert_eq!(binding.state, BindingState::Confirmed);
    assert_eq!(binding.node.id, NODE_ID);
    assert_eq!(binding.execution_id, metadata.execution_id.to_string());
}

#[tokio::test]
async fn a_cold_create_the_node_refuses_withdraws_the_reservation_and_writes_no_record() {
    let half = api_half().await;
    *half.node.create.lock().expect("lock") =
        Some(Err(Status::internal("this node cannot pull that image")));

    let err = Arc::clone(&half.orchestrator)
        .create_sandbox(cold_create())
        .await
        .expect_err("a node that refuses the create must not look like a success");
    assert!(
        format!("{err:#}").contains(NODE_ID),
        "the refusal must name the node that refused: {err:#}"
    );

    assert!(
        half.placement_calls()
            .contains(&"release_placement_reservation".to_string()),
        "a launch that failed must withdraw its reservation: {:?}",
        half.placement_calls()
    );
    assert!(
        half.orchestrator
            .list_sandboxes()
            .await
            .expect("the metadata store answers")
            .is_empty(),
        "a create the node refused left a record behind"
    );
    let leaked = half.creates_seen();
    let sandbox_id = SandboxId::parse_str(&leaked[0].sandbox_id).expect("a sandbox id");
    assert!(
        half.binding(sandbox_id).await.is_none(),
        "a create the node refused left a routing binding behind"
    );
}

#[tokio::test]
async fn a_resume_starts_the_pause_row_under_the_sandboxs_own_id() {
    let half = api_half().await;
    let sandbox_id = SandboxId::new();
    let row = paused_row();

    let metadata = Arc::clone(&half.orchestrator)
        .restore_sandbox(sandbox_id, resume_from(row.clone()))
        .await
        .expect("the api half resumes the sandbox from its pause row");
    assert_eq!(metadata.id, sandbox_id, "a resume keeps the sandbox's id");

    let creates = half.creates_seen();
    assert_eq!(creates.len(), 1);
    assert_eq!(creates[0].sandbox_id, sandbox_id.to_string());
    assert!(
        matches!(
            &creates[0].source,
            Some(pb::sandbox_create_request::Source::Snapshot(snapshot))
                if snapshot.snapshot_id == row.id.to_string()
        ),
        "a resume ships the row it is restoring: {:?}",
        creates[0].source
    );

    assert_eq!(
        half.record_state(sandbox_id).await,
        Some(SandboxState::Running)
    );
    assert_eq!(
        half.binding(sandbox_id).await.expect("a binding").state,
        BindingState::Confirmed
    );
}

#[tokio::test]
async fn a_resume_the_node_refuses_leaves_the_sandbox_id_free() {
    let half = api_half().await;
    let sandbox_id = SandboxId::new();
    *half.node.create.lock().expect("lock") =
        Some(Err(Status::internal("this node cannot read that snapshot")));

    Arc::clone(&half.orchestrator)
        .restore_sandbox(sandbox_id, resume_from(paused_row()))
        .await
        .expect_err("a node that refuses the resume must not look like a success");

    assert_eq!(half.record_state(sandbox_id).await, None);
    assert!(
        half.binding(sandbox_id).await.is_none(),
        "a resume the node refused left a routing binding behind"
    );
}

#[tokio::test]
async fn a_pause_commits_what_the_node_staged_and_retires_both_the_record_and_the_binding() {
    let half = api_half().await;
    let metadata = Arc::clone(&half.orchestrator)
        .create_sandbox(cold_create())
        .await
        .expect("a sandbox to pause");
    *half.node.pause.lock().expect("lock") = Some(Ok(staged_by_the_node(metadata.id)));

    Arc::clone(&half.orchestrator)
        .pause_sandbox(metadata.id)
        .await
        .expect("the api half pauses the sandbox");

    assert_eq!(
        half.node.seen_pause.lock().expect("lock").len(),
        1,
        "the pause did not reach the node"
    );
    assert_eq!(
        half.snapshots.committed().len(),
        1,
        "the api half did not commit what the node staged"
    );
    assert_eq!(half.record_state(metadata.id).await, None);
    assert!(
        half.binding(metadata.id).await.is_none(),
        "a paused sandbox that still routes somewhere"
    );
}

#[tokio::test]
async fn a_pause_the_node_refuses_leaves_the_sandbox_running_and_routable() {
    let half = api_half().await;
    let metadata = Arc::clone(&half.orchestrator)
        .create_sandbox(cold_create())
        .await
        .expect("a sandbox to pause");
    // No classification: the node's verdict is unknown, and the api half must
    // not read unknown as "nothing happened".
    *half.node.pause.lock().expect("lock") =
        Some(Err(Status::internal("the capture never started")));

    Arc::clone(&half.orchestrator)
        .pause_sandbox(metadata.id)
        .await
        .expect_err("a pause the node refused must not look like a success");

    assert!(
        half.snapshots.committed().is_empty(),
        "a pause that never staged anything committed a row"
    );
    assert_eq!(
        half.record_state(metadata.id).await,
        Some(SandboxState::Running),
        "the sandbox the node would not pause is still running, and its record has to say so"
    );
    let binding = half
        .binding(metadata.id)
        .await
        .expect("a sandbox that is still running is still routable");
    assert_eq!(binding.state, BindingState::Confirmed);
    assert_eq!(binding.execution_id, metadata.execution_id.to_string());
}

#[tokio::test]
async fn a_create_grants_the_secrets_its_policy_names_and_a_delete_revokes_them() {
    let half = api_half().await;

    let metadata = Arc::clone(&half.orchestrator)
        .create_sandbox(cold_create_naming_a_secret())
        .await
        .expect("the api half creates a sandbox whose policy names a secret");

    assert_eq!(
        half.granted(),
        vec![(
            metadata.id,
            metadata.execution_id,
            vec!["openai".to_string()]
        )],
        "the incarnation started without the grant its policy needs"
    );
    assert!(
        half.revoked().is_empty(),
        "a running sandbox's grant was revoked under it"
    );

    Arc::clone(&half.orchestrator)
        .delete_sandbox(metadata.id)
        .await
        .expect("the api half deletes the sandbox");

    assert_eq!(
        half.revoked(),
        vec![(metadata.id, metadata.execution_id)],
        "a deleted incarnation whose grant still resolves is one the broker still serves"
    );
}

#[tokio::test]
async fn a_pause_revokes_the_grant_the_create_issued() {
    let half = api_half().await;
    let metadata = Arc::clone(&half.orchestrator)
        .create_sandbox(cold_create_naming_a_secret())
        .await
        .expect("a sandbox to pause");
    assert_eq!(
        half.granted(),
        vec![(
            metadata.id,
            metadata.execution_id,
            vec!["openai".to_string()]
        )],
        "there is no grant for the pause to revoke unless the create issued one"
    );
    *half.node.pause.lock().expect("lock") = Some(Ok(staged_by_the_node(metadata.id)));

    Arc::clone(&half.orchestrator)
        .pause_sandbox(metadata.id)
        .await
        .expect("the api half pauses the sandbox");

    assert_eq!(
        half.revoked(),
        vec![(metadata.id, metadata.execution_id)],
        "a paused sandbox has no runtime to read a secret, so its grant goes with it"
    );
}

#[tokio::test]
async fn a_delete_stops_the_sandbox_on_its_node_and_retires_the_binding() {
    let half = api_half().await;
    let metadata = Arc::clone(&half.orchestrator)
        .create_sandbox(cold_create())
        .await
        .expect("a sandbox to delete");

    Arc::clone(&half.orchestrator)
        .delete_sandbox(metadata.id)
        .await
        .expect("the api half deletes the sandbox");

    let deletes = half.node.seen_delete.lock().expect("lock").clone();
    assert_eq!(deletes.len(), 1, "the delete did not reach the node");
    assert_eq!(deletes[0].sandbox_id, metadata.id.to_string());
    assert_eq!(
        deletes[0].execution_id,
        metadata.execution_id.to_string(),
        "a delete names the incarnation it was written for"
    );

    assert_eq!(half.record_state(metadata.id).await, None);
    assert!(
        half.binding(metadata.id).await.is_none(),
        "a deleted sandbox still routes somewhere"
    );
}

#[tokio::test]
async fn a_delete_the_node_refuses_keeps_the_record_rather_than_forgetting_a_live_runtime() {
    let half = api_half().await;
    let metadata = Arc::clone(&half.orchestrator)
        .create_sandbox(cold_create())
        .await
        .expect("a sandbox to delete");
    *half.node.delete.lock().expect("lock") =
        Some(Err(Status::internal("the node could not stop it")));

    Arc::clone(&half.orchestrator)
        .delete_sandbox(metadata.id)
        .await
        .expect_err("a delete the node refused must not look like a success");

    assert!(
        half.record_state(metadata.id).await.is_some(),
        "a delete the node refused forgot a sandbox that may still be running"
    );
}

#[tokio::test]
async fn a_launch_another_replica_holds_never_reaches_placement() {
    let half = api_half().await;
    let sandbox_id = SandboxId::new();
    let holder = ExecutionId::new();
    half.bindings
        .reserve_launch(
            &sandbox_id.to_string(),
            &holder.to_string(),
            SystemTime::now(),
        )
        .await
        .expect("the binding store takes the id for the other replica");

    let err = Arc::clone(&half.orchestrator)
        .restore_sandbox(sandbox_id, cold_create())
        .await
        .expect_err("a sandbox id another replica is launching is not this launch's to take");

    assert!(
        crate::orchestrator::LaunchHeldElsewhere::refused(&err),
        "the caller has to read this as a launch to wait for, not a failure: {err:#}"
    );
    assert!(
        !half.placement_calls().contains(&"place_new".to_string()),
        "the id is taken before a node is chosen, so no placement decision may be spent: {:?}",
        half.placement_calls()
    );
    assert!(
        half.creates_seen().is_empty(),
        "no node may be asked for a runtime under an id someone else is launching"
    );
}

#[tokio::test]
async fn a_cold_create_gives_the_sandbox_id_back_once_a_routing_record_fences_it() {
    let half = api_half().await;

    let metadata = Arc::clone(&half.orchestrator)
        .create_sandbox(cold_create())
        .await
        .expect("the api half creates a sandbox on the node");

    assert_eq!(
        half.launch_reservation_calls(),
        vec![
            "reserve_launch".to_string(),
            "store.add".to_string(),
            "release_launch".to_string()
        ],
        "the id is taken for the launch and given back once the record fences it"
    );
    assert_eq!(
        half.bindings
            .reserve_launch(
                &metadata.id.to_string(),
                &ExecutionId::new().to_string(),
                SystemTime::now()
            )
            .await
            .expect("the binding store answers"),
        crate::binding_store::LaunchReservationOutcome::Claimed,
        "past the launch it is the routing record that fences the id, not the reservation"
    );
}

#[tokio::test]
async fn a_cold_create_the_node_refuses_gives_the_sandbox_id_back_too() {
    let half = api_half().await;
    *half.node.create.lock().expect("lock") =
        Some(Err(Status::internal("this node cannot pull that image")));

    Arc::clone(&half.orchestrator)
        .create_sandbox(cold_create())
        .await
        .expect_err("a node that refuses the create must not look like a success");

    assert_eq!(
        half.launch_reservation_calls(),
        vec!["reserve_launch".to_string(), "release_launch".to_string()],
        "a launch that failed must not hold the id until its window runs out"
    );
    let sandbox_id =
        SandboxId::parse_str(&half.creates_seen()[0].sandbox_id).expect("a sandbox id");
    assert_eq!(
        half.bindings
            .reserve_launch(
                &sandbox_id.to_string(),
                &ExecutionId::new().to_string(),
                SystemTime::now()
            )
            .await
            .expect("the binding store answers"),
        crate::binding_store::LaunchReservationOutcome::Claimed
    );
}

#[tokio::test]
async fn a_delete_retires_the_routing_binding_before_it_removes_the_record() {
    let half = api_half().await;
    let metadata = Arc::clone(&half.orchestrator)
        .create_sandbox(cold_create())
        .await
        .expect("a sandbox to delete");

    Arc::clone(&half.orchestrator)
        .delete_sandbox(metadata.id)
        .await
        .expect("the api half deletes the sandbox");

    assert_eq!(
        half.routing.forgotten(),
        vec![(metadata.id, metadata.execution_id, true)],
        "the delete must retire its own incarnation while the record is still there"
    );
}

#[tokio::test]
async fn a_pause_retires_the_routing_binding_before_it_removes_the_record() {
    let half = api_half().await;
    let metadata = Arc::clone(&half.orchestrator)
        .create_sandbox(cold_create())
        .await
        .expect("a sandbox to pause");
    *half.node.pause.lock().expect("lock") = Some(Ok(staged_by_the_node(metadata.id)));

    Arc::clone(&half.orchestrator)
        .pause_sandbox(metadata.id)
        .await
        .expect("the api half pauses the sandbox");

    assert_eq!(
        half.routing.forgotten(),
        vec![(metadata.id, metadata.execution_id, true)],
        "the pause must retire its own incarnation while the record is still there"
    );
}

#[tokio::test]
async fn a_launch_rolled_back_after_it_was_recorded_retires_its_binding_first() {
    let half = api_half().await;
    let sandbox_id = SandboxId::new();
    let execution_id = ExecutionId::new();
    // A node that answers a create without an address leaves the launch with
    // nowhere to send traffic, so the record it just wrote is taken back.
    *half.node.create.lock().expect("lock") = Some(Ok(pb::SandboxCreateResponse {
        sandbox_id: sandbox_id.to_string(),
        execution_id: execution_id.to_string(),
        host_interaction_ip: String::new(),
        ..Default::default()
    }));

    Arc::clone(&half.orchestrator)
        .restore_sandbox(
            sandbox_id,
            CreateSandboxRequest {
                execution_id: Some(execution_id),
                ..cold_create()
            },
        )
        .await
        .expect_err("a sandbox with no address is not a sandbox");

    assert_eq!(
        half.routing.forgotten(),
        vec![(sandbox_id, execution_id, true)],
        "the only other path that removes a record must retire the binding it wrote, under its \
         own fence and while the record is still there"
    );
    assert_eq!(half.record_state(sandbox_id).await, None);
    assert!(half.binding(sandbox_id).await.is_none());
}

#[tokio::test]
async fn an_eviction_forgets_a_sandbox_the_cluster_no_longer_routes_to() {
    let half = api_half().await;
    let metadata = Arc::clone(&half.orchestrator)
        .create_sandbox(CreateSandboxRequest {
            expiry: SandboxExpiry::After(Duration::from_millis(1)),
            ..cold_create()
        })
        .await
        .expect("a sandbox to evict");
    // The node has no pause scripted, so the pause fails; what decides the
    // record's fate is that nothing routes to the sandbox any more.
    half.routing_answers(false);
    tokio::time::sleep(Duration::from_millis(50)).await;

    let evicted = Arc::clone(&half.orchestrator)
        .evict_expired_sandboxes()
        .await
        .expect("the eviction round runs");

    assert!(evicted.is_empty(), "the pause did not produce a snapshot");
    assert_eq!(
        half.record_state(metadata.id).await,
        None,
        "a record whose runtime nothing routes to must not survive the round that found it"
    );
}

#[tokio::test]
async fn a_keepalive_on_a_runtime_nothing_routes_to_drops_the_record() {
    let half = api_half().await;
    let metadata = Arc::clone(&half.orchestrator)
        .create_sandbox(cold_create())
        .await
        .expect("a sandbox to keep alive");
    half.routing_answers(false);

    let err = half
        .orchestrator
        .keep_alive_for(metadata.id, Some(Duration::from_secs(600)), false)
        .await
        .expect_err("a runtime nobody can reach is not one to keep alive");

    assert!(
        matches!(err, crate::orchestrator::OrchestratorError::SandboxNotFound(id) if id == metadata.id),
        "{err:?}"
    );
    assert_eq!(half.record_state(metadata.id).await, None);
}

#[tokio::test]
async fn a_keepalive_on_a_routed_runtime_extends_it() {
    let half = api_half().await;
    let metadata = Arc::clone(&half.orchestrator)
        .create_sandbox(cold_create())
        .await
        .expect("a sandbox to keep alive");
    half.routing_answers(true);

    let extended = half
        .orchestrator
        .keep_alive_for(metadata.id, Some(Duration::from_secs(1200)), false)
        .await
        .expect("a sandbox the cluster still routes to keeps its record")
        .expect("the keep-alive answers with the record it wrote");

    assert_eq!(extended.state, SandboxState::Running);
    assert!(extended.expires_at > metadata.expires_at);
}

#[tokio::test]
async fn a_launch_takes_an_id_whose_runtime_the_cluster_no_longer_routes_to() {
    let half = api_half().await;
    let created = Arc::clone(&half.orchestrator)
        .create_sandbox(cold_create())
        .await
        .expect("a sandbox whose record outlives its runtime");
    // A pause performed by another replica leaves this one's record behind.
    half.routing_answers(false);

    let restored = Arc::clone(&half.orchestrator)
        .restore_sandbox(created.id, cold_create())
        .await
        .expect("a record nothing routes to must not refuse this sandbox's resume");

    assert_eq!(restored.id, created.id);
    assert_ne!(
        restored.execution_id, created.execution_id,
        "the resume is a new run under the same id"
    );
}

#[tokio::test]
async fn a_caller_waiting_out_another_replicas_launch_answers_with_the_record_it_left() {
    let half = api_half().await;
    let created = Arc::clone(&half.orchestrator)
        .create_sandbox(cold_create())
        .await
        .expect("the record the other replica leaves behind");

    let waited = half
        .orchestrator
        .await_launch_elsewhere(
            created.id,
            crate::orchestrator::OrchestratorError::InternalError(
                "the reservation was refused".to_string(),
            ),
        )
        .await
        .expect("a launch that produced a running record is one to answer with");

    assert_eq!(waited.execution_id, created.execution_id);
    assert_eq!(waited.state, SandboxState::Running);
}

#[tokio::test]
async fn a_caller_waiting_out_a_launch_of_a_sandbox_being_deleted_is_told_it_is_gone() {
    let half = api_half().await;
    let created = Arc::clone(&half.orchestrator)
        .create_sandbox(cold_create())
        .await
        .expect("a sandbox to tear down");
    half.orchestrator
        .set_metadata_state_for_test(created.id, SandboxState::Killing)
        .await
        .expect("a killing record should be writable");

    let err = half
        .orchestrator
        .await_launch_elsewhere(
            created.id,
            crate::orchestrator::OrchestratorError::InternalError(
                "the reservation was refused".to_string(),
            ),
        )
        .await
        .expect_err("a sandbox being torn down is not one to wait for");

    assert!(
        matches!(err, crate::orchestrator::OrchestratorError::SandboxNotFound(id) if id == created.id),
        "{err:?}"
    );
}

#[tokio::test]
async fn a_launch_of_an_id_a_record_already_names_took_the_id_before_it_read_that_record() {
    let half = api_half().await;
    let metadata = Arc::clone(&half.orchestrator)
        .create_sandbox(cold_create())
        .await
        .expect("a sandbox whose id is already recorded");

    Arc::clone(&half.orchestrator)
        .restore_sandbox(metadata.id, cold_create())
        .await
        .expect_err("an id this control plane already records is not another launch's to take");

    assert_eq!(
        half.launch_reservation_calls(),
        vec![
            "reserve_launch".to_string(),
            "store.add".to_string(),
            "release_launch".to_string(),
            "reserve_launch".to_string(),
            "release_launch".to_string(),
        ],
        "the reservation has to be held across the read of the record that refuses the launch, \
         or a concurrent launch reads that record between the two"
    );
}

#[tokio::test]
async fn a_second_launch_of_one_id_is_held_until_the_first_has_recorded_it() {
    let half = api_half().await;
    let sandbox_id = SandboxId::new();
    let (reached_the_write, let_the_write_through) = half.store.stop_the_next_add();

    let first = tokio::spawn({
        let orchestrator = Arc::clone(&half.orchestrator);
        async move {
            orchestrator
                .restore_sandbox(sandbox_id, cold_create())
                .await
        }
    });
    reached_the_write
        .await
        .expect("the first launch reaches the record write");

    let refusal = Arc::clone(&half.orchestrator)
        .restore_sandbox(sandbox_id, cold_create())
        .await
        .expect_err("an id a launch has not finished recording is not a second launch's to take");
    assert!(
        crate::orchestrator::LaunchHeldElsewhere::refused(&refusal),
        "the second launch has to read this as a launch to wait out, not a failure: {refusal:#}"
    );
    assert_eq!(
        half.creates_seen().len(),
        1,
        "a second node create under an id whose record is still being written runs the same \
         sandbox twice"
    );

    let _ = let_the_write_through.send(());
    first
        .await
        .expect("the first launch task finishes")
        .expect("the first launch records its sandbox");
}
