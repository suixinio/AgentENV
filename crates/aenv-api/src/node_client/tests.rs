//! Node-client behavior exercised over a real gRPC socket.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tokio::sync::oneshot;
use tonic::{Request, Response, Status};

use crate::orchestrator::{
    DiscardingPausePublisher, InMemoryMetadataStore, MetadataStore, NodeOrchestration,
    Orchestrator, StagingPausePublisher,
};
use crate::proto::node as pb;
use crate::proto::node::node_sandbox_service_server::{
    NodeSandboxService, NodeSandboxServiceServer,
};
use crate::runtime_snapshot::RunnableSnapshot;
use crate::sandbox::mock::{MockBackendFactory, MockBehavior};
use crate::sandbox::{
    CustomExtensionParams, SandboxBackend, SandboxBackendFactory, SandboxForkSpec,
    SandboxLaunchConfig,
};
use crate::snapshot::mock::RecordingSnapshotRepository;
use crate::snapshot::repository::{
    RepositoryResult, SnapshotCatalog, SnapshotCommit, SnapshotListFilter, SnapshotListPage,
    SnapshotRepository, SnapshotRuntimeResolver, StagedSnapshot, StartedBuild,
};
use crate::snapshot::CapturedSandboxSnapshot;
use crate::snapshot::{CommittedSnapshot, SnapshotId, SnapshotManager, SnapshotRecord};
use crate::types::ExecutionId;

use crate::node_client::factory::RemoteSandboxBackendFactory;
use crate::node_client::placement::{
    FixedNodePlacement, NodeEndpoint, NodeMembership, NodePlacement,
};
use crate::node_client::wire;

struct RunningNode {
    endpoint: NodeEndpoint,
    _shutdown: oneshot::Sender<()>,
    orchestration: Option<Arc<dyn NodeOrchestration>>,
}

impl RunningNode {
    fn placement(&self) -> Arc<FixedNodePlacement> {
        Arc::new(FixedNodePlacement::new(self.endpoint.clone()))
    }
}

async fn serve<S>(service: S, orchestration: Option<Arc<dyn NodeOrchestration>>) -> RunningNode
where
    S: NodeSandboxService,
{
    // Hand the bound listener directly to the server to avoid a port-reuse race.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a port");
    let addr: SocketAddr = listener.local_addr().expect("the bound address");
    let (tx, rx) = oneshot::channel();

    tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(NodeSandboxServiceServer::new(service))
            .serve_with_incoming_shutdown(
                tonic::transport::server::TcpIncoming::from(listener),
                async {
                    let _ = rx.await;
                },
            )
            .await;
    });

    // Wait until the spawned accept loop has been polled.
    for _ in 0..200 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    RunningNode {
        endpoint: NodeEndpoint::same_address("node-under-test", format!("http://{addr}")),
        _shutdown: tx,
        orchestration,
    }
}

async fn real_node() -> RunningNode {
    let behavior = Arc::new(MockBehavior::new());
    behavior.make_captures_stageable();
    real_node_with_factory(MockBackendFactory::with_behavior(behavior)).await
}

async fn real_node_with_factory(factory: MockBackendFactory) -> RunningNode {
    crate::logging::init_for_tests();
    let orchestrator = Orchestrator::new(
        crate::sandbox::AccessTokenSeedPolicy::MayGenerate,
        InMemoryMetadataStore::new(),
        factory,
        crate::image::DisabledRuntimeImageRefs::shared(),
    )
    .await
    .expect("an in-memory orchestrator");
    let snapshots = Arc::new(resolvable_snapshot_manager());
    orchestrator.set_pause_publisher(Arc::new(StagingPausePublisher::new(Arc::clone(&snapshots))));
    let orchestration: Arc<dyn NodeOrchestration> = orchestrator;
    let service = aenv_node::node_server::NodeSandboxService::new(
        Arc::clone(&orchestration),
        snapshots,
        "node-under-test".to_string(),
    );
    serve(service, Some(orchestration)).await
}

async fn real_node_with_image_resolution() -> (RunningNode, std::path::PathBuf) {
    crate::logging::init_for_tests();

    let root = tempfile::tempdir().expect("a temp dir");
    let deps_path = root.path().join("deps");
    let regctl = crate::cfg::regctl_path(&deps_path);
    let regctl_dir = regctl
        .parent()
        .expect("the regctl path has a parent")
        .to_path_buf();
    std::fs::create_dir_all(&regctl_dir).expect("create the fake regctl's directory");
    std::fs::write(regctl_dir.join("stdout"), "").expect("write the stdout fixture");
    std::fs::write(
        regctl_dir.join("stderr"),
        "request failed: not found [http 404]: {}\n",
    )
    .expect("write the stderr fixture");
    std::fs::write(regctl_dir.join("exit_code"), "1\n").expect("write the exit code fixture");
    std::os::unix::fs::symlink(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../tests/fixtures/regctl-recorder.sh"),
        &regctl,
    )
    .expect("link the fake regctl");

    let config = crate::cfg::AppConfig {
        deps_path,
        ..Default::default()
    };
    let image_resolver = Arc::new(aenv_node::image::ImageResolver::new(&config));
    let template_builder = Arc::new(aenv_node::template::TemplateBuilder::new());

    let orchestrator = Orchestrator::new(
        crate::sandbox::AccessTokenSeedPolicy::MayGenerate,
        InMemoryMetadataStore::new(),
        MockBackendFactory::new(),
        crate::image::DisabledRuntimeImageRefs::shared(),
    )
    .await
    .expect("an in-memory orchestrator");
    let orchestration: Arc<dyn NodeOrchestration> = orchestrator;
    let service = aenv_node::node_server::NodeSandboxService::new(
        Arc::clone(&orchestration),
        Arc::new(resolvable_snapshot_manager()),
        "node-under-test".to_string(),
    )
    .with_template_build(image_resolver, template_builder);

    let node = serve(service, Some(orchestration)).await;
    // Held for the fixture's lifetime, matching `sandbox.rs`'s own
    // `surface_as`: the fake `regctl` symlink and the argv it writes both
    // live under it.
    std::mem::forget(root);
    (node, regctl_dir)
}

fn resolvable_snapshot_manager() -> SnapshotManager {
    struct OneSnapshot;

    #[async_trait]
    impl SnapshotCatalog for OneSnapshot {
        async fn create(&self, _record: SnapshotRecord) -> RepositoryResult<SnapshotRecord> {
            unreachable!("not used by these tests")
        }
        async fn publish_commit(
            &self,
            _commit: SnapshotCommit,
        ) -> RepositoryResult<SnapshotRecord> {
            unreachable!("not used by these tests")
        }
        async fn get(&self, id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>> {
            Ok((id_or_alias != "no-such-snapshot")
                .then(|| SnapshotRecord::mock_ready(CommittedSnapshot::mock())))
        }
        async fn list_page(
            &self,
            _filter: SnapshotListFilter,
        ) -> RepositoryResult<SnapshotListPage> {
            Ok(SnapshotListPage::single(Vec::new()))
        }
        async fn delete_record(&self, _record: &SnapshotRecord) -> RepositoryResult<()> {
            Ok(())
        }
        async fn resolve_alias(&self, _alias: &str) -> RepositoryResult<Option<SnapshotId>> {
            Ok(None)
        }
        async fn try_start_build(&self, _id: &SnapshotId) -> RepositoryResult<StartedBuild> {
            unreachable!("not used by these tests")
        }
        async fn mark_build_error(
            &self,
            _id: &SnapshotId,
            _reason: crate::snapshot::TemplateBuildErrorReason,
        ) -> RepositoryResult<()> {
            Ok(())
        }
    }

    struct AlwaysRunnable;

    #[async_trait]
    impl SnapshotRuntimeResolver for AlwaysRunnable {
        async fn resolve(
            &self,
            _snapshot: Arc<SnapshotRecord>,
        ) -> RepositoryResult<RunnableSnapshot> {
            Ok(RunnableSnapshot::mock())
        }
    }

    SnapshotManager::from_parts(
        Arc::new(SnapshotRepository::new(
            Arc::new(OneSnapshot),
            Arc::new(RecordingSnapshotRepository::default()),
        )),
        Some(Arc::new(AlwaysRunnable)),
        None,
    )
}

#[derive(Default)]
struct ScriptedNode {
    create: Mutex<Option<Result<pb::SandboxCreateResponse, Status>>>,
    /// Answer to a create issued after the first one was refused.
    create_again: Mutex<Option<Result<pb::SandboxCreateResponse, Status>>>,
    pause: Mutex<Option<Result<pb::SandboxPauseResponse, Status>>>,
    checkpoint: Mutex<Option<Result<pb::SandboxCheckpointResponse, Status>>>,
    fork: Mutex<Option<Result<pb::SandboxForkResponse, Status>>>,
    delete: Mutex<Option<Result<pb::SandboxDeleteResponse, Status>>>,
    describe: Mutex<Option<Result<pb::SandboxDescribeResponse, Status>>>,
    update_params: Mutex<Option<Result<pb::SandboxParamsResponse, Status>>>,
    seen_create: Mutex<Vec<pb::SandboxCreateRequest>>,
    seen_pause: Mutex<Vec<pb::SandboxPauseRequest>>,
    seen_delete: Mutex<Vec<pb::SandboxDeleteRequest>>,
    seen_describe: Mutex<Vec<pb::SandboxDescribeRequest>>,
    seen_update_params: Mutex<Vec<pb::SandboxParamsRequest>>,
}

impl ScriptedNode {
    fn take<T>(slot: &Mutex<Option<Result<T, Status>>>, what: &str) -> Result<Response<T>, Status> {
        match slot.lock().expect("lock").take() {
            Some(Ok(value)) => Ok(Response::new(value)),
            Some(Err(status)) => Err(status),
            None => Err(Status::unimplemented(format!("no {what} was scripted"))),
        }
    }
}

#[derive(Clone)]
struct ScriptedNodeService(Arc<ScriptedNode>);

impl std::ops::Deref for ScriptedNodeService {
    type Target = ScriptedNode;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[tonic::async_trait]
impl NodeSandboxService for ScriptedNodeService {
    async fn create(
        &self,
        request: Request<pb::SandboxCreateRequest>,
    ) -> Result<Response<pb::SandboxCreateResponse>, Status> {
        self.seen_create
            .lock()
            .expect("lock")
            .push(request.into_inner());
        if let Some(scripted) = self.create.lock().expect("lock").take() {
            return scripted.map(Response::new);
        }
        ScriptedNode::take(&self.create_again, "create")
    }

    async fn delete(
        &self,
        request: Request<pb::SandboxDeleteRequest>,
    ) -> Result<Response<pb::SandboxDeleteResponse>, Status> {
        self.seen_delete
            .lock()
            .expect("lock")
            .push(request.into_inner());
        match self.delete.lock().expect("lock").take() {
            Some(Ok(value)) => Ok(Response::new(value)),
            Some(Err(status)) => Err(status),
            None => Ok(Response::new(pb::SandboxDeleteResponse {})),
        }
    }

    async fn pause(
        &self,
        request: Request<pb::SandboxPauseRequest>,
    ) -> Result<Response<pb::SandboxPauseResponse>, Status> {
        self.seen_pause
            .lock()
            .expect("lock")
            .push(request.into_inner());
        ScriptedNode::take(&self.pause, "pause")
    }

    async fn checkpoint(
        &self,
        _request: Request<pb::SandboxCheckpointRequest>,
    ) -> Result<Response<pb::SandboxCheckpointResponse>, Status> {
        ScriptedNode::take(&self.checkpoint, "checkpoint")
    }

    async fn fork(
        &self,
        _request: Request<pb::SandboxForkRequest>,
    ) -> Result<Response<pb::SandboxForkResponse>, Status> {
        ScriptedNode::take(&self.fork, "fork")
    }

    async fn update_network(
        &self,
        _request: Request<pb::SandboxNetworkRequest>,
    ) -> Result<Response<pb::SandboxNetworkResponse>, Status> {
        Ok(Response::new(pb::SandboxNetworkResponse {}))
    }

    async fn update_params(
        &self,
        request: Request<pb::SandboxParamsRequest>,
    ) -> Result<Response<pb::SandboxParamsResponse>, Status> {
        self.seen_update_params
            .lock()
            .expect("lock")
            .push(request.into_inner());
        match self.update_params.lock().expect("lock").take() {
            Some(Ok(value)) => Ok(Response::new(value)),
            Some(Err(status)) => Err(status),
            None => Ok(Response::new(pb::SandboxParamsResponse {})),
        }
    }

    async fn describe(
        &self,
        request: Request<pb::SandboxDescribeRequest>,
    ) -> Result<Response<pb::SandboxDescribeResponse>, Status> {
        let request = request.into_inner();
        let sandbox_id = request.sandbox_id.clone();
        self.seen_describe.lock().expect("lock").push(request);
        match self.describe.lock().expect("lock").take() {
            Some(Ok(value)) => Ok(Response::new(value)),
            Some(Err(status)) => Err(status),
            None => Err(Status::not_found(format!(
                "sandbox {sandbox_id} is not running on this node"
            ))),
        }
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
        Err(Status::unimplemented(
            "ScriptedNode does not script override_status",
        ))
    }

    // Unscripted so accidental use fails loudly.
    async fn build_template(
        &self,
        _request: Request<pb::TemplateBuildRequest>,
    ) -> Result<Response<pb::TemplateBuildResponse>, Status> {
        Err(Status::unimplemented(
            "ScriptedNode does not script build_template",
        ))
    }
}

async fn scripted_node() -> (Arc<ScriptedNode>, RunningNode) {
    crate::logging::init_for_tests();
    let node = Arc::new(ScriptedNode::default());
    let running = serve(ScriptedNodeService(Arc::clone(&node)), None).await;
    (node, running)
}

fn launch_config() -> SandboxLaunchConfig {
    SandboxLaunchConfig {
        snapshot_id: "snapshot".to_string(),
        ..Default::default()
    }
}

#[tokio::test]
async fn a_sandbox_built_here_starts_on_the_node() {
    let node = real_node().await;
    let factory = RemoteSandboxBackendFactory::new(node.placement());
    let snapshot = RunnableSnapshot::mock();
    let execution_id = ExecutionId::new();
    let config = launch_config();
    let sandbox_id = config.sandbox_id;

    let mut backend = match factory.build_from_snapshot(&snapshot, config, execution_id) {
        Ok(backend) => backend,
        Err(err) => panic!("building a stub should not fail: {err:#}"),
    };

    assert!(backend.host_interaction_ip().is_none());
    assert!(
        backend.holding_node_id().is_none(),
        "a stub nobody has started has no machine to name yet"
    );

    backend.start().await.expect("start on the node");
    assert_eq!(backend.execution_id(), execution_id);
    assert_eq!(backend.holding_node_id(), Some("node-under-test"));

    let live = node
        .orchestration
        .as_ref()
        .expect("a real node")
        .list_live_sandboxes()
        .await
        .expect("the node can list what it is running");
    assert_eq!(live.len(), 1);
    assert_eq!(live[0].sandbox_id, sandbox_id);
    assert_eq!(
        live[0].execution_id,
        Some(execution_id),
        "the node ran a different incarnation from the one it was asked for"
    );
}

#[tokio::test]
async fn a_status_override_lands_on_the_nodes_own_scheduling_switch() {
    let node = real_node().await;
    let orchestration = node.orchestration.as_ref().expect("a real node");
    assert!(!orchestration.scheduling_disabled());

    // The caller holds the registry-advertised address, whose port answers
    // nothing — reaching the node at all proves the service-port rewrite.
    let node_service_port: u16 = node
        .endpoint
        .endpoint
        .rsplit(':')
        .next()
        .expect("the endpoint carries a port")
        .parse()
        .expect("the port is numeric");
    let advertised = "http://127.0.0.1:1";

    crate::node_client::override_node_status(advertised, node_service_port, true)
        .await
        .expect("drain the node");
    assert!(
        orchestration.scheduling_disabled(),
        "the drain must reach the node's own scheduling switch"
    );

    crate::node_client::override_node_status(advertised, node_service_port, false)
        .await
        .expect("ready the node");
    assert!(
        !orchestration.scheduling_disabled(),
        "the switch must flip back the same way it was set"
    );
}

#[tokio::test]
async fn custom_extension_params_update_lands_in_the_real_node() {
    let behavior = Arc::new(MockBehavior::new());
    let node =
        real_node_with_factory(MockBackendFactory::with_behavior(Arc::clone(&behavior))).await;
    let factory = RemoteSandboxBackendFactory::new(node.placement());
    let execution_id = ExecutionId::new();
    let config = launch_config();
    let sandbox_id = config.sandbox_id;
    let mut backend = factory
        .build_from_snapshot(&RunnableSnapshot::mock(), config, execution_id)
        .expect("building a stub should not fail");
    backend.start().await.expect("start on the node");

    let orchestration = node.orchestration.as_ref().expect("a real node");
    assert_eq!(
        orchestration
            .get_sandbox(&sandbox_id)
            .await
            .expect("read")
            .expect("sandbox metadata should exist")
            .custom_extension_params,
        None,
        "nothing has been applied yet"
    );

    let mut params: CustomExtensionParams = serde_json::Map::new();
    params.insert("mode".to_string(), serde_json::json!("fast"));
    backend
        .update_custom_extension_params(Some(params.clone()))
        .await
        .expect("the node applied the value");
    // The runtime itself, not only the node's store echo of it — this is the
    // check that would have stayed green through the original bug, where the
    // store updated and the mock backend never saw the call at all.
    assert_eq!(
        behavior.last_custom_extension_params(),
        Some(Some(params.clone())),
        "the node's own backend must have actually been asked to hold this value"
    );
    assert_eq!(
        orchestration
            .get_sandbox(&sandbox_id)
            .await
            .expect("read")
            .expect("sandbox metadata should exist")
            .custom_extension_params,
        Some(params),
        "the node's own record must hold exactly the value that was sent"
    );

    backend
        .update_custom_extension_params(None)
        .await
        .expect("the node applied the clear");
    assert_eq!(
        behavior.last_custom_extension_params(),
        Some(None),
        "the node's own backend must have actually been asked to clear the value"
    );
    assert_eq!(
        orchestration
            .get_sandbox(&sandbox_id)
            .await
            .expect("read")
            .expect("sandbox metadata should exist")
            .custom_extension_params,
        None,
        "clearing the value must reach the node too, not just setting it"
    );
}

#[tokio::test]
async fn a_node_that_started_another_run_fails_the_start_and_is_told_to_stop() {
    let (script, node) = scripted_node().await;
    let asked_for = ExecutionId::new();
    let started = ExecutionId::new();
    *script.create.lock().expect("lock") = Some(Ok(pb::SandboxCreateResponse {
        sandbox_id: launch_config().sandbox_id.to_string(),
        execution_id: started.to_string(),
        ..Default::default()
    }));

    let factory = RemoteSandboxBackendFactory::new(node.placement());
    let mut backend =
        match factory.build_from_snapshot(&RunnableSnapshot::mock(), launch_config(), asked_for) {
            Ok(backend) => backend,
            Err(err) => panic!("building a stub should not fail: {err:#}"),
        };

    let err = backend.start().await.expect_err("the node ran another run");
    assert!(err.to_string().contains(&started.to_string()), "{err:#}");

    let deletes = script.seen_delete.lock().expect("lock").clone();
    assert_eq!(deletes.len(), 1, "the sandbox the node started was left up");
    assert_eq!(
        deletes[0].execution_id,
        started.to_string(),
        "the teardown named the wrong run"
    );
}

struct FixedRecordOwner(anyhow::Result<Option<ExecutionId>>);

#[async_trait]
impl crate::node_client::SandboxRecordOwner for FixedRecordOwner {
    async fn recorded_execution(
        &self,
        _sandbox_id: crate::types::SandboxId,
    ) -> anyhow::Result<Option<ExecutionId>> {
        match &self.0 {
            Ok(execution_id) => Ok(*execution_id),
            Err(error) => Err(anyhow::anyhow!("{error}")),
        }
    }
}

async fn start_against_a_node_that_already_holds_the_sandbox(
    recorded: anyhow::Result<Option<ExecutionId>>,
    launching: ExecutionId,
) -> (Arc<ScriptedNode>, anyhow::Result<()>) {
    let (script, node) = scripted_node().await;
    *script.create.lock().expect("lock") = Some(Err(Status::already_exists(
        "sandbox is already on this node",
    )));

    let factory = RemoteSandboxBackendFactory::new(node.placement())
        .with_record_owner(Arc::new(FixedRecordOwner(recorded)));
    let mut backend = factory
        .build_from_snapshot(&RunnableSnapshot::mock(), launch_config(), launching)
        .expect("building a stub should not fail");
    let outcome = backend.start().await;
    (script, outcome)
}

#[tokio::test]
async fn a_node_holding_a_copy_of_this_id_fails_the_launch_and_is_left_alone() {
    let launching = ExecutionId::new();
    let (script, outcome) =
        start_against_a_node_that_already_holds_the_sandbox(Ok(None), launching).await;

    let err = outcome.expect_err("a node that already holds the id refuses this launch");
    assert!(
        err.to_string().contains("orphan"),
        "the launch must say who decides whether the node's copy is one: {err:#}"
    );
    assert!(
        script.seen_delete.lock().expect("lock").is_empty(),
        "a launch must not delete a node's copy: from here it is indistinguishable from a \
         sibling launch's live runtime"
    );
    assert!(
        script.seen_describe.lock().expect("lock").is_empty(),
        "the node is not asked what it holds, because no answer would license a delete"
    );
    assert_eq!(
        script.seen_create.lock().expect("lock").len(),
        1,
        "one create, one refusal"
    );
}

#[tokio::test]
async fn a_node_holding_a_sandbox_a_newer_run_owns_fails_without_touching_it() {
    let newer = ExecutionId::new();
    let launching = ExecutionId::new();
    let (script, outcome) =
        start_against_a_node_that_already_holds_the_sandbox(Ok(Some(newer)), launching).await;

    let err = outcome.expect_err("a launch that lost the id must not take the winner's runtime");
    assert!(err.to_string().contains(&newer.to_string()), "{err:#}");
    assert!(
        script.seen_delete.lock().expect("lock").is_empty(),
        "a superseded launch must delete nothing"
    );
    assert_eq!(
        script.seen_create.lock().expect("lock").len(),
        1,
        "a superseded launch must not create again"
    );
}

#[tokio::test]
async fn a_record_nobody_can_read_stops_the_launch_rather_than_clearing_the_node() {
    let (script, outcome) = start_against_a_node_that_already_holds_the_sandbox(
        Err(anyhow::anyhow!("the store is unreachable")),
        ExecutionId::new(),
    )
    .await;

    outcome.expect_err("an unreadable record is not permission to clear a node");
    assert!(script.seen_delete.lock().expect("lock").is_empty());
    assert_eq!(script.seen_create.lock().expect("lock").len(), 1);
}

#[tokio::test]
async fn a_create_from_an_unresolved_record_is_the_same_bytes_as_one_from_a_resolved_snapshot() {
    use prost::Message;

    let snapshot = RunnableSnapshot::mock();
    let config = launch_config();
    let execution_id = ExecutionId::new();

    async fn one_create(
        build: impl FnOnce(
            &RemoteSandboxBackendFactory,
        ) -> anyhow::Result<Box<dyn crate::sandbox::SandboxBackend>>,
        config: &SandboxLaunchConfig,
        execution_id: ExecutionId,
    ) -> pb::SandboxCreateRequest {
        let (script, node) = scripted_node().await;
        *script.create.lock().expect("lock") = Some(Ok(pb::SandboxCreateResponse {
            sandbox_id: config.sandbox_id.to_string(),
            execution_id: execution_id.to_string(),
            ..Default::default()
        }));
        let factory = RemoteSandboxBackendFactory::new(node.placement());
        let mut backend = build(&factory).expect("building a stub should not fail");
        backend.start().await.expect("start");
        let creates = script.seen_create.lock().expect("lock").clone();
        assert_eq!(creates.len(), 1, "one create, one request");
        creates.into_iter().next().expect("the one create")
    }

    let resolved = one_create(
        |factory| factory.build_from_snapshot(&snapshot, config.clone(), execution_id),
        &config,
        execution_id,
    )
    .await;
    let unresolved = one_create(
        |factory| {
            factory.build_from_snapshot_record(snapshot.record(), config.clone(), execution_id)
        },
        &config,
        execution_id,
    )
    .await;

    assert_eq!(
        resolved, unresolved,
        "🔴 the api half resolving a snapshot changed what the node was asked for, which means          the deployment that stops resolving is not the deployment that was running"
    );
    assert_eq!(
        resolved.encode_to_vec(),
        unresolved.encode_to_vec(),
        "the two requests compare equal but do not encode equal, so what reaches the node over \
         the wire is not the same"
    );
}

#[tokio::test]
async fn the_create_names_the_run_the_caller_minted() {
    let (script, node) = scripted_node().await;
    let execution_id = ExecutionId::new();
    let config = launch_config();
    let sandbox_id = config.sandbox_id;
    *script.create.lock().expect("lock") = Some(Ok(pb::SandboxCreateResponse {
        sandbox_id: sandbox_id.to_string(),
        execution_id: execution_id.to_string(),
        host_interaction_ip: "10.1.2.3".to_string(),
        rootfs_virtual_size: 8192,
        ..Default::default()
    }));

    let factory = RemoteSandboxBackendFactory::new(node.placement());
    let mut backend =
        match factory.build_from_snapshot(&RunnableSnapshot::mock(), config, execution_id) {
            Ok(backend) => backend,
            Err(err) => panic!("building a stub should not fail: {err:#}"),
        };
    backend.start().await.expect("start");

    let creates = script.seen_create.lock().expect("lock").clone();
    assert_eq!(creates.len(), 1);
    assert_eq!(creates[0].sandbox_id, sandbox_id.to_string());
    assert_eq!(creates[0].execution_id, execution_id.to_string());
    assert!(creates[0].control_plane_config.is_empty());

    assert_eq!(
        backend.host_interaction_ip(),
        Some(std::net::Ipv4Addr::new(10, 1, 2, 3))
    );
    assert_eq!(backend.runtime_info().rootfs_virtual_size, Some(8192));
    assert!(backend.runtime_info().runtime_artifacts.is_empty());
    assert!(backend.startup_artifacts().is_empty());
}

#[tokio::test]
async fn the_create_tells_the_node_that_this_half_keeps_the_deadline() {
    use pb::sandbox_create_request::Expiry;

    let (script, node) = scripted_node().await;
    let execution_id = ExecutionId::new();
    let config = launch_config();
    let sandbox_id = config.sandbox_id;
    *script.create.lock().expect("lock") = Some(Ok(pb::SandboxCreateResponse {
        sandbox_id: sandbox_id.to_string(),
        execution_id: execution_id.to_string(),
        ..Default::default()
    }));

    let factory = RemoteSandboxBackendFactory::new(node.placement());
    let mut backend = factory
        .build_from_snapshot(&RunnableSnapshot::mock(), config, execution_id)
        .expect("building a stub should not fail");
    backend.start().await.expect("start");

    let creates = script.seen_create.lock().expect("lock").clone();
    assert_eq!(creates.len(), 1);
    assert_eq!(
        creates[0].expiry,
        Some(Expiry::CallerKept(pb::CallerKeptExpiry {})),
        "the node was not told that this half keeps the sandbox's deadline"
    );

    assert_ne!(
        creates[0].expiry,
        Some(Expiry::NodeKeptDefault(pb::NodeDefaultExpiry {})),
        "the node was told to apply its own default, which is the outage"
    );
    assert!(
        !matches!(creates[0].expiry, Some(Expiry::NodeKeptTimeoutMs(_))),
        "the node was handed a deadline to keep, and it keeps none for this half's sandboxes"
    );
    assert!(
        creates[0].expiry.is_some(),
        "the field was left unset, which the node refuses"
    );
}

#[tokio::test]
async fn custom_extension_params_update_is_a_real_round_trip() {
    let (script, node) = scripted_node().await;
    let execution_id = ExecutionId::new();
    *script.create.lock().expect("lock") = Some(Ok(pb::SandboxCreateResponse {
        sandbox_id: launch_config().sandbox_id.to_string(),
        execution_id: execution_id.to_string(),
        ..Default::default()
    }));
    let factory = RemoteSandboxBackendFactory::new(node.placement());
    let mut backend =
        match factory.build_from_snapshot(&RunnableSnapshot::mock(), launch_config(), execution_id)
        {
            Ok(backend) => backend,
            Err(err) => panic!("building a stub should not fail: {err:#}"),
        };
    backend.start().await.expect("start");

    let mut first: CustomExtensionParams = serde_json::Map::new();
    first.insert("mode".to_string(), serde_json::json!("fast"));
    backend
        .update_custom_extension_params(Some(first.clone()))
        .await
        .expect("the node accepted the first value");

    let mut second: CustomExtensionParams = serde_json::Map::new();
    second.insert("mode".to_string(), serde_json::json!("slow"));
    backend
        .update_custom_extension_params(Some(second.clone()))
        .await
        .expect("the node accepted the second value");

    let seen = script.seen_update_params.lock().expect("lock").clone();
    assert_eq!(seen.len(), 2, "both calls must have reached the node");
    let decoded_first: Option<CustomExtensionParams> =
        wire::serialized(seen[0].custom_extension_params.as_ref(), "params")
            .expect("decode the first request");
    let decoded_second: Option<CustomExtensionParams> =
        wire::serialized(seen[1].custom_extension_params.as_ref(), "params")
            .expect("decode the second request");
    assert_eq!(
        decoded_first,
        Some(first),
        "the node did not see the first value as sent"
    );
    assert_eq!(
        decoded_second,
        Some(second),
        "the second call must not repeat the first"
    );

    // A node refusal is now something the caller can see, instead of being
    // logged on a Pod nobody making the call is looking at.
    *script.update_params.lock().expect("lock") = Some(Err(Status::unimplemented(
        "update_params is not served on this node",
    )));
    let err = backend
        .update_custom_extension_params(None)
        .await
        .expect_err("a node refusal must surface as an error, not a silent success");
    assert!(
        format!("{err:#}").contains("not served"),
        "the failure lost what the node said: {err:#}"
    );
}

#[tokio::test]
async fn an_unreachable_node_does_not_mean_the_sandbox_stopped() {
    let (script, node) = scripted_node().await;
    let execution_id = ExecutionId::new();
    *script.create.lock().expect("lock") = Some(Ok(pb::SandboxCreateResponse {
        sandbox_id: launch_config().sandbox_id.to_string(),
        execution_id: execution_id.to_string(),
        ..Default::default()
    }));
    let factory = RemoteSandboxBackendFactory::new(node.placement());
    let mut backend =
        match factory.build_from_snapshot(&RunnableSnapshot::mock(), launch_config(), execution_id)
        {
            Ok(backend) => backend,
            Err(err) => panic!("building a stub should not fail: {err:#}"),
        };
    backend.start().await.expect("start");

    *script.delete.lock().expect("lock") = Some(Err(Status::unavailable("the node is restarting")));
    let err = backend.stop().await.expect_err("an unreachable node");
    assert!(
        format!("{err:#}").contains("the node is restarting"),
        "the failure lost what the node said: {err:#}"
    );

    *script.delete.lock().expect("lock") = Some(Err(Status::not_found("no such sandbox here")));
    backend
        .stop()
        .await
        .expect("a node that says the sandbox is not there has answered");
}

struct ReplacementNodePlacement {
    node_id: String,
    initial: NodeEndpoint,
    resolved: NodeEndpoint,
    resolve_calls: std::sync::atomic::AtomicUsize,
    resolve_budget: usize,
}

#[async_trait]
impl NodePlacement for ReplacementNodePlacement {
    async fn place_new(
        &self,
        _sandbox_id: crate::types::SandboxId,
        _resources: crate::types::SandboxResources,
        _preferred_node_id: Option<&str>,
        _excluded_node_ids: &[String],
    ) -> anyhow::Result<NodeEndpoint> {
        Ok(self.initial.clone())
    }

    async fn place_existing(
        &self,
        _sandbox_id: crate::types::SandboxId,
    ) -> anyhow::Result<Option<NodeEndpoint>> {
        Ok(Some(self.initial.clone()))
    }

    async fn resolve_node(&self, node_id: &str) -> anyhow::Result<NodeEndpoint> {
        assert_eq!(
            node_id, self.node_id,
            "asked to resolve a node this test never placed a sandbox on"
        );
        let call_number = self
            .resolve_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        if call_number > self.resolve_budget {
            anyhow::bail!(
                "this test's re-resolve budget ({}) is exhausted; a build that hands a fork \
                 child the pre-retry connection needs a second re-resolve to self-heal, and \
                 this refusal is what stops that from happening invisibly",
                self.resolve_budget
            );
        }
        Ok(self.resolved.clone())
    }

    async fn node_membership(&self, _node_id: &str) -> anyhow::Result<NodeMembership> {
        Ok(NodeMembership::Present)
    }

    async fn record_placement(
        &self,
        _sandbox_id: crate::types::SandboxId,
        _execution_id: ExecutionId,
        _node: &NodeEndpoint,
        _projection_ttl_secs: u32,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn reserve_placement(
        &self,
        _sandbox_id: crate::types::SandboxId,
        _execution_id: ExecutionId,
        _node: &NodeEndpoint,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn release_placement_reservation(
        &self,
        _sandbox_id: crate::types::SandboxId,
        _execution_id: ExecutionId,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn forget_placement(
        &self,
        _sandbox_id: crate::types::SandboxId,
        _execution_id: ExecutionId,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn a_stale_node_address_is_retried_once_against_a_freshly_resolved_one() {
    let (script_a, node_a) = scripted_node().await;
    let (script_b, node_b) = scripted_node().await;
    let execution_id = ExecutionId::new();
    *script_a.create.lock().expect("lock") = Some(Ok(pb::SandboxCreateResponse {
        sandbox_id: launch_config().sandbox_id.to_string(),
        execution_id: execution_id.to_string(),
        ..Default::default()
    }));

    let placement = Arc::new(ReplacementNodePlacement {
        node_id: "the-rolling-node".to_string(),
        initial: NodeEndpoint::same_address("the-rolling-node", node_a.endpoint.endpoint.clone()),
        resolved: NodeEndpoint::same_address("the-rolling-node", node_b.endpoint.endpoint.clone()),
        resolve_calls: std::sync::atomic::AtomicUsize::new(0),
        resolve_budget: usize::MAX,
    });
    let factory =
        RemoteSandboxBackendFactory::new(Arc::clone(&placement) as Arc<dyn NodePlacement>);
    let mut backend = factory
        .build_from_snapshot(&RunnableSnapshot::mock(), launch_config(), execution_id)
        .expect("build a stub");
    backend
        .start()
        .await
        .expect("start against the initial node");

    // Kill the node this stub is currently placed on, and wait for the OS to
    // agree it is gone — the same failure a rolling restart produces once the
    // old pod's socket is actually closed, rather than a status either half
    // of this process ever manufactured by hand.
    let dead_addr = node_a.endpoint.endpoint.clone();
    drop(node_a);
    wait_until_unreachable(&dead_addr).await;

    backend
        .stop()
        .await
        .expect("a stale address is retried against the freshly resolved one");

    assert_eq!(
        placement
            .resolve_calls
            .load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the retry must re-resolve exactly once"
    );
    let deletes = script_b.seen_delete.lock().expect("lock").clone();
    assert_eq!(
        deletes.len(),
        1,
        "the retried delete never reached the re-resolved node"
    );
    assert_eq!(deletes[0].execution_id, execution_id.to_string());
    // And the node the retry gave up on was never asked twice — there was no
    // connection left to ask it on.
    assert!(script_a.seen_delete.lock().expect("lock").is_empty());
}

fn scripted_fork_of(
    child: &SandboxForkSpec,
    outcome: pb::fork_child_result::Outcome,
) -> pb::SandboxForkResponse {
    pb::SandboxForkResponse {
        children: vec![pb::ForkChildResult {
            sandbox_id: child.sandbox_id.to_string(),
            execution_id: child.execution_id.to_string(),
            outcome: Some(outcome),
        }],
    }
}

async fn started_parent_on(
    script: &ScriptedNode,
    placement: Arc<ClusterPlacement>,
) -> Box<dyn crate::sandbox::SandboxBackend> {
    let execution_id = ExecutionId::new();
    *script.create.lock().expect("lock") = Some(Ok(pb::SandboxCreateResponse {
        sandbox_id: launch_config().sandbox_id.to_string(),
        execution_id: execution_id.to_string(),
        ..Default::default()
    }));
    let factory = RemoteSandboxBackendFactory::new(placement as Arc<dyn NodePlacement>);
    let mut backend = factory
        .build_from_snapshot(&RunnableSnapshot::mock(), launch_config(), execution_id)
        .expect("build a stub");
    backend.start().await.expect("start the parent");
    backend
}

#[tokio::test]
async fn a_fork_child_the_node_started_is_routable() {
    let (script, node) = scripted_node().await;
    let placement = ClusterPlacement::recording(node.endpoint.clone());
    let mut parent = started_parent_on(&script, Arc::clone(&placement)).await;
    let child_spec = SandboxForkSpec {
        sandbox_id: crate::types::SandboxId::new(),
        execution_id: ExecutionId::new(),
        envd_access_token: None,
    };
    *script.fork.lock().expect("lock") = Some(Ok(scripted_fork_of(
        &child_spec,
        pb::fork_child_result::Outcome::Started(pb::SandboxCreateResponse {
            sandbox_id: child_spec.sandbox_id.to_string(),
            execution_id: child_spec.execution_id.to_string(),
            ..Default::default()
        }),
    )));

    let results = parent
        .fork(std::slice::from_ref(&child_spec))
        .await
        .expect("fork");
    assert!(results[0].is_ok(), "the child started");
    assert!(
        placement.is_bound(child_spec.sandbox_id),
        "a started fork child is not routable"
    );
    assert!(!placement.is_reserved(child_spec.sandbox_id));
    let recorded = placement.recorded();
    let child_record = recorded
        .iter()
        .find(|(id, _, _, _)| *id == child_spec.sandbox_id)
        .expect("the child's placement was confirmed");
    assert_eq!(child_record.1, child_spec.execution_id);
    assert_eq!(child_record.2.node_id, node.endpoint.node_id);
}

#[tokio::test]
async fn a_fork_child_the_node_failed_to_start_leaves_no_routing_record() {
    let (script, node) = scripted_node().await;
    let placement = ClusterPlacement::recording(node.endpoint.clone());
    let mut parent = started_parent_on(&script, Arc::clone(&placement)).await;
    let child_spec = SandboxForkSpec {
        sandbox_id: crate::types::SandboxId::new(),
        execution_id: ExecutionId::new(),
        envd_access_token: None,
    };
    *script.fork.lock().expect("lock") = Some(Ok(scripted_fork_of(
        &child_spec,
        pb::fork_child_result::Outcome::Error("no room".to_string()),
    )));

    let results = parent
        .fork(std::slice::from_ref(&child_spec))
        .await
        .expect("fork answered");
    assert!(results[0].is_err(), "the child did not start");
    assert!(!placement.is_bound(child_spec.sandbox_id));
    assert!(
        !placement.is_reserved(child_spec.sandbox_id),
        "a reservation for a child that never started must not linger"
    );
}

#[tokio::test]
async fn a_forks_children_carry_the_connection_the_retry_actually_used() {
    let (script_a, node_a) = scripted_node().await;
    let (script_b, node_b) = scripted_node().await;
    let execution_id = ExecutionId::new();
    *script_a.create.lock().expect("lock") = Some(Ok(pb::SandboxCreateResponse {
        sandbox_id: launch_config().sandbox_id.to_string(),
        execution_id: execution_id.to_string(),
        ..Default::default()
    }));

    let child_spec = SandboxForkSpec {
        sandbox_id: crate::types::SandboxId::new(),
        execution_id: ExecutionId::new(),
        envd_access_token: None,
    };
    *script_b.fork.lock().expect("lock") = Some(Ok(pb::SandboxForkResponse {
        children: vec![pb::ForkChildResult {
            sandbox_id: child_spec.sandbox_id.to_string(),
            execution_id: child_spec.execution_id.to_string(),
            outcome: Some(pb::fork_child_result::Outcome::Started(
                pb::SandboxCreateResponse {
                    sandbox_id: child_spec.sandbox_id.to_string(),
                    execution_id: child_spec.execution_id.to_string(),
                    ..Default::default()
                },
            )),
        }],
    }));

    let placement = Arc::new(ReplacementNodePlacement {
        node_id: "the-forking-node".to_string(),
        initial: NodeEndpoint::same_address("the-forking-node", node_a.endpoint.endpoint.clone()),
        resolved: NodeEndpoint::same_address("the-forking-node", node_b.endpoint.endpoint.clone()),
        resolve_calls: std::sync::atomic::AtomicUsize::new(0),
        // See the field doc: this is the test that needs the cap to bite.
        resolve_budget: 1,
    });
    let factory =
        RemoteSandboxBackendFactory::new(Arc::clone(&placement) as Arc<dyn NodePlacement>);
    let mut backend = factory
        .build_from_snapshot(&RunnableSnapshot::mock(), launch_config(), execution_id)
        .expect("build a stub");
    backend
        .start()
        .await
        .expect("start against the initial node");

    let dead_addr = node_a.endpoint.endpoint.clone();
    drop(node_a);
    wait_until_unreachable(&dead_addr).await;

    let results = backend
        .fork(&[child_spec])
        .await
        .expect("the fork retries against the re-resolved node");
    let mut child = results
        .into_iter()
        .next()
        .expect("one child")
        .expect("the child started");

    // The proof: operating on the child must reach node_b, the node the
    // retry actually used — not fail against the dead node_a, which is what
    // it would do if the child had been built from the pre-retry client.
    child
        .stop()
        .await
        .expect("the child's connection must be the one the retry used, not the abandoned one");
    assert_eq!(script_b.seen_delete.lock().expect("lock").len(), 1);
    assert!(script_a.seen_delete.lock().expect("lock").is_empty());
}

#[tokio::test]
async fn a_status_the_node_sent_is_not_retried_against_a_different_address() {
    let (script, node) = scripted_node().await;
    let (script_elsewhere, node_elsewhere) = scripted_node().await;
    let execution_id = ExecutionId::new();
    *script.create.lock().expect("lock") = Some(Ok(pb::SandboxCreateResponse {
        sandbox_id: launch_config().sandbox_id.to_string(),
        execution_id: execution_id.to_string(),
        ..Default::default()
    }));

    let placement = Arc::new(ReplacementNodePlacement {
        node_id: "the-node".to_string(),
        initial: NodeEndpoint::same_address("the-node", node.endpoint.endpoint.clone()),
        resolved: NodeEndpoint::same_address("the-node", node_elsewhere.endpoint.endpoint.clone()),
        resolve_calls: std::sync::atomic::AtomicUsize::new(0),
        resolve_budget: usize::MAX,
    });
    let factory =
        RemoteSandboxBackendFactory::new(Arc::clone(&placement) as Arc<dyn NodePlacement>);
    let mut backend = factory
        .build_from_snapshot(&RunnableSnapshot::mock(), launch_config(), execution_id)
        .expect("build a stub");
    backend.start().await.expect("start");

    *script.delete.lock().expect("lock") = Some(Err(Status::unavailable("the node is restarting")));
    let err = backend
        .stop()
        .await
        .expect_err("the node's own answer must not be swallowed");
    assert!(
        format!("{err:#}").contains("the node is restarting"),
        "the failure lost what the node said: {err:#}"
    );
    assert_eq!(
        placement
            .resolve_calls
            .load(std::sync::atomic::Ordering::SeqCst),
        0,
        "an application-level Unavailable must never trigger a re-resolve"
    );
    assert!(
        script_elsewhere
            .seen_delete
            .lock()
            .expect("lock")
            .is_empty(),
        "a status the node sent must never be replayed against another address"
    );
}

#[tokio::test]
async fn a_slow_reresolve_is_bounded_by_the_retry_budget() {
    struct SlowReresolve {
        node_id: String,
        initial: NodeEndpoint,
        resolve_calls: std::sync::atomic::AtomicUsize,
    }

    #[async_trait]
    impl NodePlacement for SlowReresolve {
        async fn place_new(
            &self,
            _sandbox_id: crate::types::SandboxId,
            _resources: crate::types::SandboxResources,
            _preferred_node_id: Option<&str>,
            _excluded_node_ids: &[String],
        ) -> anyhow::Result<NodeEndpoint> {
            Ok(self.initial.clone())
        }
        async fn place_existing(
            &self,
            _sandbox_id: crate::types::SandboxId,
        ) -> anyhow::Result<Option<NodeEndpoint>> {
            Ok(Some(self.initial.clone()))
        }
        async fn resolve_node(&self, node_id: &str) -> anyhow::Result<NodeEndpoint> {
            assert_eq!(node_id, self.node_id);
            self.resolve_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            // Comfortably past the retry budget, so a build that dropped the
            // timeout would make this test hang here instead of failing fast.
            tokio::time::sleep(
                crate::node_client::stub::STALE_PLACEMENT_RETRY_BUDGET
                    + std::time::Duration::from_secs(5),
            )
            .await;
            Ok(self.initial.clone())
        }
        async fn node_membership(&self, _node_id: &str) -> anyhow::Result<NodeMembership> {
            Ok(NodeMembership::Present)
        }
        async fn record_placement(
            &self,
            _sandbox_id: crate::types::SandboxId,
            _execution_id: ExecutionId,
            _node: &NodeEndpoint,
            _projection_ttl_secs: u32,
        ) -> anyhow::Result<()> {
            Ok(())
        }
        async fn reserve_placement(
            &self,
            _sandbox_id: crate::types::SandboxId,
            _execution_id: ExecutionId,
            _node: &NodeEndpoint,
        ) -> anyhow::Result<()> {
            Ok(())
        }
        async fn release_placement_reservation(
            &self,
            _sandbox_id: crate::types::SandboxId,
            _execution_id: ExecutionId,
        ) -> anyhow::Result<()> {
            Ok(())
        }

        async fn forget_placement(
            &self,
            _sandbox_id: crate::types::SandboxId,
            _execution_id: ExecutionId,
        ) -> anyhow::Result<()> {
            Ok(())
        }
    }

    let (script_a, node_a) = scripted_node().await;
    let execution_id = ExecutionId::new();
    *script_a.create.lock().expect("lock") = Some(Ok(pb::SandboxCreateResponse {
        sandbox_id: launch_config().sandbox_id.to_string(),
        execution_id: execution_id.to_string(),
        ..Default::default()
    }));

    let placement = Arc::new(SlowReresolve {
        node_id: "the-slow-node".to_string(),
        initial: NodeEndpoint::same_address("the-slow-node", node_a.endpoint.endpoint.clone()),
        resolve_calls: std::sync::atomic::AtomicUsize::new(0),
    });
    let factory =
        RemoteSandboxBackendFactory::new(Arc::clone(&placement) as Arc<dyn NodePlacement>);
    let mut backend = factory
        .build_from_snapshot(&RunnableSnapshot::mock(), launch_config(), execution_id)
        .expect("build a stub");
    backend.start().await.expect("start");

    let dead_addr = node_a.endpoint.endpoint.clone();
    drop(node_a);
    wait_until_unreachable(&dead_addr).await;

    let started = std::time::Instant::now();
    let err = backend
        .stop()
        .await
        .expect_err("the node is genuinely gone; a slow re-resolve cannot save this call");
    let elapsed = started.elapsed();

    assert!(
        elapsed
            < crate::node_client::stub::STALE_PLACEMENT_RETRY_BUDGET
                + std::time::Duration::from_millis(1500),
        "the retry budget did not bound the call: it took {elapsed:?}, and the re-resolve alone \
         sleeps for {:?}",
        crate::node_client::stub::STALE_PLACEMENT_RETRY_BUDGET + std::time::Duration::from_secs(5)
    );
    assert!(
        format!("{err:#}").contains("tcp connect error"),
        "the original transport failure must be surfaced when the retry times out, got: {err:#}"
    );
    assert_eq!(
        placement
            .resolve_calls
            .load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the re-resolve was attempted exactly once"
    );
}

// Black-hole endpoint used to exercise the explicit stub connect timeout.

const BLACK_HOLE_ENDPOINT: &str = "http://10.255.255.1:1";

#[tokio::test]
async fn a_black_holed_dial_gives_up_at_the_connect_timeout_not_the_kernels_syn_ceiling() {
    let started = std::time::Instant::now();
    let outcome = tokio::time::timeout(
        std::time::Duration::from_secs(20),
        crate::node_client::stub::RemoteSandboxStub::connect(BLACK_HOLE_ENDPOINT),
    )
    .await;
    let elapsed = started.elapsed();

    let result = outcome.expect(
        "the dial did not return within 20s: STUB_CONNECT_TIMEOUT is not bounding it any more",
    );
    assert!(
        result.is_err(),
        "a black-holed address must fail to connect, not succeed"
    );
    assert!(
        elapsed
            < crate::node_client::stub::STUB_CONNECT_TIMEOUT + std::time::Duration::from_secs(2),
        "the dial took {elapsed:?}, past STUB_CONNECT_TIMEOUT ({:?}) plus slack — the kernel's \
         own SYN-retry ceiling is back in control",
        crate::node_client::stub::STUB_CONNECT_TIMEOUT
    );
}

#[test]
fn stub_connect_timeout_leaves_headroom_in_the_retry_budget() {
    assert!(
        crate::node_client::stub::STUB_CONNECT_TIMEOUT
            < crate::node_client::stub::STALE_PLACEMENT_RETRY_BUDGET,
        "STUB_CONNECT_TIMEOUT ({:?}) must be smaller than STALE_PLACEMENT_RETRY_BUDGET ({:?}): \
         otherwise the retry path's own reconnect can consume the whole budget and leave nothing \
         for the resolve_node RPC or the retried call that follow it",
        crate::node_client::stub::STUB_CONNECT_TIMEOUT,
        crate::node_client::stub::STALE_PLACEMENT_RETRY_BUDGET
    );
}

#[tokio::test]
async fn a_pause_comes_back_as_the_row_the_node_staged() {
    let (script, node) = scripted_node().await;
    let staged = staged_snapshot();
    let (mut backend, _) = paused_stub(&script, &node, &staged).await;

    let capture = backend.pause().await.expect("pause");
    let sent = script.seen_pause.lock().expect("lock").clone();
    assert_eq!(sent.len(), 1, "the pause did not reach the node");
    let CapturedSandboxSnapshot::Staged(decoded) = capture else {
        panic!("a remote pause carries the staged row, not local artifacts");
    };
    assert_eq!(decoded.id(), staged.id());
    assert_eq!(decoded.origin_node_id, "node-under-test");
}

#[tokio::test]
async fn a_pause_the_node_staged_nothing_for_is_terminal() {
    let (script, node) = scripted_node().await;
    let execution_id = ExecutionId::new();
    *script.create.lock().expect("lock") = Some(Ok(pb::SandboxCreateResponse {
        sandbox_id: launch_config().sandbox_id.to_string(),
        execution_id: execution_id.to_string(),
        ..Default::default()
    }));
    *script.pause.lock().expect("lock") = Some(Ok(pb::SandboxPauseResponse { staged: None }));

    let factory = RemoteSandboxBackendFactory::new(node.placement());
    let mut backend = factory
        .build_from_snapshot(&RunnableSnapshot::mock(), launch_config(), execution_id)
        .expect("build a stub");
    backend.start().await.expect("start");

    let err = match backend.pause().await {
        Err(err) => err,
        Ok(_) => panic!("a pause with no row to resume from must not look like a success"),
    };
    assert!(err.is_terminal(), "{err}");
}

#[tokio::test]
async fn a_capture_failure_keeps_its_classification_across_the_wire() {
    for (terminal, expected) in [(false, false), (true, true)] {
        let (script, node) = scripted_node().await;
        let execution_id = ExecutionId::new();
        *script.create.lock().expect("lock") = Some(Ok(pb::SandboxCreateResponse {
            sandbox_id: launch_config().sandbox_id.to_string(),
            execution_id: execution_id.to_string(),
            ..Default::default()
        }));
        *script.checkpoint.lock().expect("lock") =
            Some(Err(crate::proto::node::capture_failure_status(
                tonic::Code::Internal,
                "capture failed",
                terminal,
                "the memory snapshot did not land",
            )));

        let factory = RemoteSandboxBackendFactory::new(node.placement());
        let mut backend = factory
            .build_from_snapshot(&RunnableSnapshot::mock(), launch_config(), execution_id)
            .expect("a stub");
        backend.start().await.expect("start");

        let err = backend.snapshot().await.expect_err("the node refused");
        assert_eq!(err.is_terminal(), expected, "{err}");
    }

    let (script, node) = scripted_node().await;
    let execution_id = ExecutionId::new();
    *script.create.lock().expect("lock") = Some(Ok(pb::SandboxCreateResponse {
        sandbox_id: launch_config().sandbox_id.to_string(),
        execution_id: execution_id.to_string(),
        ..Default::default()
    }));
    *script.checkpoint.lock().expect("lock") = Some(Err(Status::internal("something went wrong")));
    let factory = RemoteSandboxBackendFactory::new(node.placement());
    let mut backend =
        match factory.build_from_snapshot(&RunnableSnapshot::mock(), launch_config(), execution_id)
        {
            Ok(backend) => backend,
            Err(err) => panic!("building a stub should not fail: {err:#}"),
        };
    backend.start().await.expect("start");
    let unclassified = backend.snapshot().await.expect_err("the node refused");
    assert!(unclassified.is_unknown(), "{unclassified}");
    assert!(!unclassified.is_terminal(), "{unclassified}");
}

fn staged_snapshot() -> StagedSnapshot {
    StagedSnapshot {
        commit: SnapshotCommit {
            id: SnapshotId::generate(),
            alias: None,
            source: crate::snapshot::SnapshotPublishSource::Template,
            resources: Default::default(),
            created_at_unix_ms: Some(1_700_000_000_000),
            origin_node_id: Some("node-under-test".to_string()),
            committed: CommittedSnapshot::mock(),
        },
        staged_at_unix_ms: 1_700_000_000_000,
        origin_node_id: "node-under-test".to_string(),
    }
}

#[tokio::test]
async fn a_checkpoint_comes_back_as_a_row_that_has_not_been_announced() {
    let (script, node) = scripted_node().await;
    let execution_id = ExecutionId::new();
    *script.create.lock().expect("lock") = Some(Ok(pb::SandboxCreateResponse {
        sandbox_id: launch_config().sandbox_id.to_string(),
        execution_id: execution_id.to_string(),
        ..Default::default()
    }));

    let staged = staged_snapshot();
    *script.checkpoint.lock().expect("lock") = Some(Ok(pb::SandboxCheckpointResponse {
        staged: Some(pb::StagedSnapshot {
            value: Some(wire::serialize(&staged, "staged snapshot").expect("encode")),
        }),
    }));

    let factory = RemoteSandboxBackendFactory::new(node.placement());
    let mut backend =
        match factory.build_from_snapshot(&RunnableSnapshot::mock(), launch_config(), execution_id)
        {
            Ok(backend) => backend,
            Err(err) => panic!("building a stub should not fail: {err:#}"),
        };
    backend.start().await.expect("start");

    let captured = backend.snapshot().await.expect("checkpoint");
    let CapturedSandboxSnapshot::Staged(decoded) = captured else {
        panic!("the capture carries a staged snapshot and not something else");
    };
    let decoded: StagedSnapshot = *decoded;
    assert_eq!(decoded.origin_node_id, "node-under-test");
    assert_eq!(decoded.id(), staged.id());
}

#[tokio::test]
async fn a_fork_answered_with_the_wrong_shape_is_refused() {
    let (script, node) = scripted_node().await;
    let execution_id = ExecutionId::new();
    *script.create.lock().expect("lock") = Some(Ok(pb::SandboxCreateResponse {
        sandbox_id: launch_config().sandbox_id.to_string(),
        execution_id: execution_id.to_string(),
        ..Default::default()
    }));
    *script.fork.lock().expect("lock") = Some(Ok(pb::SandboxForkResponse {
        children: Vec::new(),
    }));

    let factory = RemoteSandboxBackendFactory::new(node.placement());
    let mut backend =
        match factory.build_from_snapshot(&RunnableSnapshot::mock(), launch_config(), execution_id)
        {
            Ok(backend) => backend,
            Err(err) => panic!("building a stub should not fail: {err:#}"),
        };
    backend.start().await.expect("start");

    let specs = vec![SandboxForkSpec {
        sandbox_id: crate::types::SandboxId::new(),
        execution_id: ExecutionId::new(),
        envd_access_token: None,
    }];
    let err = match backend.fork(&specs).await {
        Err(err) => err,
        Ok(_) => panic!("a fork answered with no results must not look like a success"),
    };
    assert!(err.is_terminal(), "{err}");
}

#[tokio::test]
async fn a_fork_keeps_each_childs_outcome_with_that_child() {
    let (script, node) = scripted_node().await;
    let execution_id = ExecutionId::new();
    *script.create.lock().expect("lock") = Some(Ok(pb::SandboxCreateResponse {
        sandbox_id: launch_config().sandbox_id.to_string(),
        execution_id: execution_id.to_string(),
        ..Default::default()
    }));

    let specs = (0..2)
        .map(|_| SandboxForkSpec {
            sandbox_id: crate::types::SandboxId::new(),
            execution_id: ExecutionId::new(),
            envd_access_token: None,
        })
        .collect::<Vec<_>>();
    *script.fork.lock().expect("lock") = Some(Ok(pb::SandboxForkResponse {
        children: vec![
            pb::ForkChildResult {
                sandbox_id: specs[0].sandbox_id.to_string(),
                execution_id: specs[0].execution_id.to_string(),
                outcome: Some(pb::fork_child_result::Outcome::Started(
                    pb::SandboxCreateResponse {
                        sandbox_id: specs[0].sandbox_id.to_string(),
                        execution_id: specs[0].execution_id.to_string(),
                        ..Default::default()
                    },
                )),
            },
            pb::ForkChildResult {
                sandbox_id: specs[1].sandbox_id.to_string(),
                execution_id: String::new(),
                outcome: Some(pb::fork_child_result::Outcome::Error(
                    "no network slot left".to_string(),
                )),
            },
        ],
    }));

    let factory = RemoteSandboxBackendFactory::new(node.placement());
    let mut backend =
        match factory.build_from_snapshot(&RunnableSnapshot::mock(), launch_config(), execution_id)
        {
            Ok(backend) => backend,
            Err(err) => panic!("building a stub should not fail: {err:#}"),
        };
    backend.start().await.expect("start");

    let results = backend.fork(&specs).await.expect("fork");
    assert_eq!(results.len(), 2);
    let child: Box<dyn SandboxBackend> = results
        .into_iter()
        .next()
        .expect("the first child")
        .expect("which started");
    assert_eq!(child.execution_id(), specs[0].execution_id);
}

#[tokio::test]
async fn a_cold_create_is_refused_with_the_reason() {
    let (_script, node) = scripted_node().await;
    let factory = RemoteSandboxBackendFactory::new(node.placement());

    let err = match factory.build(
        crate::sandbox::FreshSandboxBuildSpec {
            image_config_path: std::path::PathBuf::from("/var/lib/agentenv/image.json"),
            context: Default::default(),
            resources: Default::default(),
            extra_drives: Vec::new(),
            extra_boot_args: None,
        },
        launch_config(),
        ExecutionId::new(),
    ) {
        Err(err) => err,
        Ok(_) => panic!("a cold create was accepted, and there is nothing to send"),
    };
    assert!(err.to_string().contains("image reference"), "{err:#}");
}

#[tokio::test]
async fn an_unresolved_image_reference_ships_to_the_node_which_resolves_it_itself() {
    const IMAGE: &str = "registry.invalid/agentenv/cold-start:pinned";

    let (node, regctl_dir) = real_node_with_image_resolution().await;
    let factory = RemoteSandboxBackendFactory::new(node.placement());

    let mut backend = factory
        .build_from_image_ref(
            crate::sandbox::UnresolvedImageBuildSpec {
                image_ref: IMAGE.to_string(),
                resources: crate::types::SandboxResources {
                    cpu_count: 1,
                    memory_mib: 256,
                    disk_size_mib: 0,
                },
                attached_drives: Vec::new(),
                extra_boot_args: None,
            },
            launch_config(),
            ExecutionId::new(),
        )
        .expect("building from an unresolved image reference is accepted, unlike `build`");

    let err = backend
        .start()
        .await
        .expect_err("the fake registry answers every lookup with a 404");
    let message = format!("{err:#}");
    assert!(
        !message.to_lowercase().contains("unimplemented"),
        "this must be the node's own resolve failure, not the old blanket refusal — reaching \
         Unimplemented here would mean create's Source::Image arm regressed to refusing again, \
         got {message:?}"
    );
    assert!(
        message.contains(IMAGE),
        "the node's own resolve failure should name the exact reference this test sent — proof \
         that the reference, not a path this process invented, is what crossed the wire, got \
         {message:?}"
    );

    let argv = std::fs::read_to_string(regctl_dir.join("argv"))
        .expect("the fake regctl on the node's own deps_path recorded a run");
    assert!(
        argv.contains(IMAGE),
        "the node must have asked its own regctl to resolve exactly the reference this process \
         sent, unresolved — this is the proof that `create`'s Source::Image arm calls its own \
         ImageResolver, on the node, rather than trusting a value this process never had, got \
         argv {argv:?}"
    );
}

#[test]
fn the_remote_factory_sends_no_blank_ownership_marker() {
    const BLANK_MARKER: &str = "control_plane_config: Vec::new()";

    // The scanned factory source lives in `aenv-core`.
    let factory = include_str!("factory.rs");

    assert!(
        !factory.contains(BLANK_MARKER),
        "the remote factory sends an empty ownership marker on every create. Every sandbox the \
         api half started would be one the control plane does not recognise as its own: absent \
         from ListSandboxes, and absent from the reconciliation that decides which bindings are \
         still live. The marker is per sandbox — it is the control plane's record of that \
         sandbox — so it has to arrive from the caller that decides what that record is, not \
         from a constant here"
    );
}

#[tokio::test]
async fn the_marker_the_orchestrator_stamped_is_the_marker_on_the_wire() {
    let node = real_node().await;
    let factory = RemoteSandboxBackendFactory::new(node.placement());

    assert!(
        factory.stamps_control_plane_ownership(),
        "a factory whose sandboxes run elsewhere has to ask for the marker"
    );

    let marker = b"the control plane's record of this sandbox".to_vec();
    let config = SandboxLaunchConfig {
        control_plane_config: Some(marker.clone()),
        ..launch_config()
    };
    let sandbox_id = config.sandbox_id;

    let mut backend = factory
        .build_from_snapshot(&RunnableSnapshot::mock(), config, ExecutionId::new())
        .expect("build a stub");
    backend.start().await.expect("start on the node");

    let live = node
        .orchestration
        .as_ref()
        .expect("a real node")
        .list_live_sandboxes()
        .await
        .expect("the node can list what it is running");
    assert_eq!(live.len(), 1);
    assert_eq!(live[0].sandbox_id, sandbox_id);
    assert_eq!(
        live[0]
            .control_plane_config
            .as_ref()
            .expect("the node stored the marker")
            .as_bytes(),
        marker,
        "the node handed back different bytes from the ones it was sent"
    );
    assert_eq!(
        aenv_node::node_server::owned_by_control_plane(&live).len(),
        1,
        "a sandbox the control plane created must be one the control plane recognises"
    );
}

#[tokio::test]
async fn a_sandbox_that_arrived_without_a_marker_is_not_the_control_planes() {
    let node = real_node().await;
    let factory = RemoteSandboxBackendFactory::new(node.placement());

    let config = launch_config();
    let sandbox_id = config.sandbox_id;
    assert!(
        config.control_plane_config.is_none(),
        "this is the case being tested, so it has to be the case being set up"
    );

    let mut backend = factory
        .build_from_snapshot(&RunnableSnapshot::mock(), config, ExecutionId::new())
        .expect("build a stub");
    backend.start().await.expect("start on the node");

    let live = node
        .orchestration
        .as_ref()
        .expect("a real node")
        .list_live_sandboxes()
        .await
        .expect("the node can list what it is running");

    assert_eq!(live.len(), 1);
    assert_eq!(live[0].sandbox_id, sandbox_id);
    assert!(
        live[0].control_plane_config.is_none(),
        "an empty marker on the wire must not become a marker on the node"
    );
    assert!(
        aenv_node::node_server::owned_by_control_plane(&live).is_empty(),
        "a sandbox nobody claimed must not be offered up for reconciliation"
    );
}

#[tokio::test]
async fn the_gate_refuses_a_node_rpc_that_carries_no_credential() {
    crate::logging::init_for_tests();
    let orchestrator = Orchestrator::new(
        crate::sandbox::AccessTokenSeedPolicy::MayGenerate,
        InMemoryMetadataStore::new(),
        MockBackendFactory::new(),
        crate::image::DisabledRuntimeImageRefs::shared(),
    )
    .await
    .expect("an in-memory orchestrator");
    let orchestration: Arc<dyn NodeOrchestration> = orchestrator;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a port");
    let addr = listener.local_addr().expect("the bound address");
    let (stop, stopped) = oneshot::channel::<()>();

    let gate = aenv_node::node_server::NodeGrpcGate::new(crate::api::ControlPlaneGate::new(
        vec!["node-token".to_string()],
        "",
    ));
    let serving = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(aenv_node::node_server::server_with_gate(
                orchestration,
                Arc::new(resolvable_snapshot_manager()),
                "node-under-test".to_string(),
                Arc::new(aenv_node::image::ImageResolver::new(
                    &crate::cfg::AppConfig::default(),
                )),
                Arc::new(aenv_node::template::TemplateBuilder::new()),
                gate,
            ))
            .serve_with_incoming_shutdown(
                tonic::transport::server::TcpIncoming::from(listener),
                async {
                    let _ = stopped.await;
                },
            )
            .await
    });

    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .expect("a valid endpoint")
        .connect()
        .await
        .expect("the node service is listening");
    let mut client = crate::proto::node::node_sandbox_service_client::NodeSandboxServiceClient::new(
        channel.clone(),
    );

    let request = || crate::proto::node::SandboxNetworkRequest {
        sandbox_id: crate::types::SandboxId::new().to_string(),
        execution_id: ExecutionId::new().to_string(),
        network_policy: None,
    };

    let refused = client
        .update_network(request())
        .await
        .expect_err("an uncredentialed caller reaches no RPC");
    assert_eq!(refused.code(), tonic::Code::Unauthenticated);

    let mut wrong = tonic::Request::new(request());
    wrong.metadata_mut().insert(
        crate::api::CONTROL_PLANE_HEADER,
        "gateway-token".parse().expect("a valid metadata value"),
    );
    let refused = client
        .update_network(wrong)
        .await
        .expect_err("another half's credential is not this gate's");
    assert_eq!(refused.code(), tonic::Code::Unauthenticated);

    let mut allowed = tonic::Request::new(request());
    allowed.metadata_mut().insert(
        crate::api::CONTROL_PLANE_HEADER,
        "node-token".parse().expect("a valid metadata value"),
    );
    let answered = client
        .update_network(allowed)
        .await
        .expect_err("no such sandbox is running");
    assert_ne!(
        answered.code(),
        tonic::Code::Unauthenticated,
        "the credential passed the gate and the service answered: {answered:?}"
    );

    let _ = stop.send(());
    let _ = serving.await;
}

#[tokio::test]
async fn the_node_service_answers_through_the_entry_point_a_binary_uses() {
    crate::logging::init_for_tests();
    let orchestrator = Orchestrator::new(
        crate::sandbox::AccessTokenSeedPolicy::MayGenerate,
        InMemoryMetadataStore::new(),
        MockBackendFactory::new(),
        crate::image::DisabledRuntimeImageRefs::shared(),
    )
    .await
    .expect("an in-memory orchestrator");
    let orchestration: Arc<dyn NodeOrchestration> = orchestrator;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a port");
    let addr = listener.local_addr().expect("the bound address");
    let (stop, stopped) = oneshot::channel::<()>();

    let served = Arc::clone(&orchestration);
    let serving = tokio::spawn(async move {
        aenv_node::node_server::serve_on(
            listener,
            served,
            Arc::new(resolvable_snapshot_manager()),
            "node-under-test".to_string(),
            Arc::new(aenv_node::image::ImageResolver::new(
                &crate::cfg::AppConfig::default(),
            )),
            Arc::new(aenv_node::template::TemplateBuilder::new()),
            async {
                let _ = stopped.await;
            },
        )
        .await
    });

    let placement = Arc::new(FixedNodePlacement::new(NodeEndpoint::same_address(
        "node-under-test",
        format!("http://{addr}"),
    )));
    let factory = RemoteSandboxBackendFactory::new(placement);
    let config = SandboxLaunchConfig {
        control_plane_config: Some(b"a marker".to_vec()),
        ..launch_config()
    };
    let sandbox_id = config.sandbox_id;

    let mut backend = factory
        .build_from_snapshot(&RunnableSnapshot::mock(), config, ExecutionId::new())
        .expect("build a stub");
    backend
        .start()
        .await
        .expect("the entry point a binary uses serves the same service");

    let live = orchestration
        .list_live_sandboxes()
        .await
        .expect("the node can list what it is running");
    assert_eq!(live.len(), 1);
    assert_eq!(live[0].sandbox_id, sandbox_id);

    // The stub holds an open HTTP/2 connection to the port under test; the
    // listener is only free of it once the client side is gone too.
    drop(backend);
    drop(stop);
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(10), serving)
        .await
        .expect("the surface stops when the shutdown signal fires")
        .expect("the serving task did not panic");
    assert!(outcome.is_ok(), "{outcome:?}");
    // The returned future is the evidence that the surface stopped. The port
    // is not: tonic closes the listener when the signal fires and only then
    // drains, so by the time this line runs any other test in this binary may
    // have been handed the same ephemeral port.
}

async fn paused_stub(
    script: &Arc<ScriptedNode>,
    node: &RunningNode,
    staged: &StagedSnapshot,
) -> (Box<dyn SandboxBackend>, ExecutionId) {
    let execution_id = ExecutionId::new();
    *script.create.lock().expect("lock") = Some(Ok(pb::SandboxCreateResponse {
        sandbox_id: launch_config().sandbox_id.to_string(),
        execution_id: execution_id.to_string(),
        ..Default::default()
    }));
    *script.pause.lock().expect("lock") = Some(Ok(pb::SandboxPauseResponse {
        staged: Some(pb::StagedSnapshot {
            value: Some(wire::serialize(staged, "staged snapshot").expect("encode")),
        }),
    }));
    let factory = RemoteSandboxBackendFactory::new(node.placement());
    let mut backend = factory
        .build_from_snapshot(&RunnableSnapshot::mock(), launch_config(), execution_id)
        .expect("build a stub");
    backend.start().await.expect("start");
    (backend, execution_id)
}

#[tokio::test]
async fn stopping_a_sandbox_that_was_just_paused_does_not_reach_the_node() {
    let (script, node) = scripted_node().await;
    let staged = staged_snapshot();

    let (mut paused, _) = paused_stub(&script, &node, &staged).await;
    paused.pause().await.expect("pause");
    paused.stop().await.expect("stop");
    assert!(
        script.seen_delete.lock().expect("lock").is_empty(),
        "the node, which already forgot the VM, was sent a delete by the stop that follows \
         every pause: {:?}",
        script.seen_delete.lock().expect("lock")
    );

    // The same call on a stub that was not paused: this one is a teardown and
    // has to reach the node.
    let (mut running, execution_id) = paused_stub(&script, &node, &staged).await;
    running.stop().await.expect("stop");
    let deletes = script.seen_delete.lock().expect("lock").clone();
    assert_eq!(deletes.len(), 1, "a running sandbox was not torn down");
    assert_eq!(deletes[0].execution_id, execution_id.to_string());
}

#[tokio::test]
async fn a_pause_crosses_both_halves() {
    let real = real_node().await;
    let orchestration = real.orchestration.as_ref().expect("a real node").clone();
    let factory = RemoteSandboxBackendFactory::new(real.placement());
    let execution_id = ExecutionId::new();
    let config = launch_config();
    let sandbox_id = config.sandbox_id;
    let mut backend = factory
        .build_from_snapshot(&RunnableSnapshot::mock(), config, execution_id)
        .expect("build a stub");
    backend.start().await.expect("start on the node");

    let capture = backend.pause().await.expect("the pause this half sends");
    assert!(
        orchestration
            .list_live_sandboxes()
            .await
            .expect("list")
            .is_empty(),
        "the pause this half sends did not pause anything"
    );
    assert!(
        orchestration
            .get_sandbox(&sandbox_id)
            .await
            .expect("read")
            .is_none(),
        "the node kept a record of a sandbox it paused"
    );

    let CapturedSandboxSnapshot::Staged(staged) = capture else {
        panic!("the node's answer is the row it staged, not local artifacts");
    };
    assert_eq!(
        staged.origin_node_id,
        crate::identity::local_node_id(),
        "the row must name the machine holding the bytes"
    );
    assert_ne!(staged.origin_node_id, "");
    assert!(
        matches!(
            &staged.commit.source,
            crate::snapshot::SnapshotPublishSource::Sandbox { source_sandbox_id }
                if source_sandbox_id == &sandbox_id.to_string()
        ),
        "the row names {:?} rather than the sandbox it came from",
        staged.commit.source
    );
}

// Exercises multiple API replicas sharing one ledger while node handles remain
// process-local.

#[derive(Clone)]
struct SharedLedger(Arc<InMemoryMetadataStore>);

type StoreResult<T> = std::result::Result<T, crate::orchestrator::StoreError>;

#[async_trait]
impl crate::orchestrator::MetadataStore for SharedLedger {
    async fn add(&self, metadata: crate::orchestrator::SandboxMetadata) -> StoreResult<()> {
        self.0.add(metadata).await
    }
    async fn update(&self, metadata: crate::orchestrator::SandboxMetadata) -> StoreResult<()> {
        self.0.update(metadata).await
    }
    async fn update_state_if_state(
        &self,
        sandbox_id: &crate::types::SandboxId,
        new_state: crate::orchestrator::SandboxState,
        expected_states: &[crate::orchestrator::SandboxState],
    ) -> StoreResult<crate::orchestrator::SandboxState> {
        self.0
            .update_state_if_state(sandbox_id, new_state, expected_states)
            .await
    }
    async fn update_if_state<F>(
        &self,
        sandbox_id: &crate::types::SandboxId,
        expected_states: &[crate::orchestrator::SandboxState],
        update: F,
    ) -> StoreResult<crate::orchestrator::MetadataUpdateResult>
    where
        F: FnOnce(&mut crate::orchestrator::SandboxMetadata) + Send,
    {
        self.0
            .update_if_state(sandbox_id, expected_states, update)
            .await
    }
    async fn get(
        &self,
        sandbox_id: &crate::types::SandboxId,
    ) -> StoreResult<Option<crate::orchestrator::SandboxMetadata>> {
        self.0.get(sandbox_id).await
    }
    async fn remove(
        &self,
        sandbox_id: &crate::types::SandboxId,
    ) -> StoreResult<Option<crate::orchestrator::SandboxMetadata>> {
        self.0.remove(sandbox_id).await
    }
    async fn remove_if_execution(
        &self,
        sandbox_id: &crate::types::SandboxId,
        expected_execution_id: ExecutionId,
        expected_states: &[crate::orchestrator::SandboxState],
    ) -> StoreResult<crate::orchestrator::FencedRemoval> {
        self.0
            .remove_if_execution(sandbox_id, expected_execution_id, expected_states)
            .await
    }
    async fn list(&self) -> StoreResult<Vec<crate::orchestrator::SandboxMetadata>> {
        self.0.list().await
    }
    async fn list_with_callback<F>(&self, callback: F) -> StoreResult<()>
    where
        F: FnMut(&crate::orchestrator::SandboxMetadata) + Send,
    {
        self.0.list_with_callback(callback).await
    }
    async fn list_filtered(
        &self,
        filter: crate::orchestrator::SandboxListFilter,
    ) -> StoreResult<Vec<crate::orchestrator::SandboxMetadata>> {
        self.0.list_filtered(filter).await
    }
    async fn list_expired(
        &self,
        now: std::time::SystemTime,
    ) -> StoreResult<Vec<crate::orchestrator::SandboxMetadata>> {
        self.0.list_expired(now).await
    }
    async fn list_ids(&self) -> StoreResult<Vec<crate::types::SandboxId>> {
        self.0.list_ids().await
    }
    async fn wait_while_in_states(
        &self,
        sandbox_id: &crate::types::SandboxId,
        transitional_states: &[crate::orchestrator::SandboxState],
    ) -> StoreResult<Option<crate::orchestrator::SandboxMetadata>> {
        self.0
            .wait_while_in_states(sandbox_id, transitional_states)
            .await
    }
    async fn expired_batch(
        &self,
        now: std::time::SystemTime,
        limit: usize,
    ) -> StoreResult<Vec<crate::orchestrator::SandboxMetadata>> {
        self.0.expired_batch(now, limit).await
    }
    async fn get_many(
        &self,
        ids: &[crate::types::SandboxId],
    ) -> StoreResult<crate::orchestrator::MetadataRows> {
        self.0.get_many(ids).await
    }
    async fn start_transition(
        &self,
        sandbox_id: &crate::types::SandboxId,
        request: crate::orchestrator::TransitionRequest,
    ) -> StoreResult<crate::orchestrator::TransitionOutcome> {
        self.0.start_transition(sandbox_id, request).await
    }
    async fn transition_settlement(
        &self,
        sandbox_id: &crate::types::SandboxId,
        transition_id: &str,
    ) -> StoreResult<crate::orchestrator::TransitionSettlement> {
        self.0
            .transition_settlement(sandbox_id, transition_id)
            .await
    }
    async fn heal_expiry_index(&self) -> StoreResult<usize> {
        self.0.heal_expiry_index().await
    }
    async fn reap_stuck_transitions(
        &self,
        now: std::time::SystemTime,
    ) -> StoreResult<Vec<crate::types::SandboxId>> {
        self.0.reap_stuck_transitions(now).await
    }
}

type ApiReplica = Arc<Orchestrator<SharedLedger, RemoteSandboxBackendFactory>>;

async fn api_replica(node: &RunningNode, ledger: &SharedLedger) -> ApiReplica {
    let replica = Orchestrator::new(
        // Test replicas may generate an otherwise irrelevant access-token seed.
        crate::sandbox::AccessTokenSeedPolicy::MayGenerate,
        ledger.clone(),
        RemoteSandboxBackendFactory::new(node.placement()),
        crate::image::DisabledRuntimeImageRefs::shared(),
    )
    .await
    .expect("a replica of the deciding half");
    replica.set_pause_publisher(DiscardingPausePublisher::shared());
    replica
}

async fn local_half() -> Arc<Orchestrator<InMemoryMetadataStore, MockBackendFactory>> {
    let local = Orchestrator::new(
        crate::sandbox::AccessTokenSeedPolicy::MayGenerate,
        InMemoryMetadataStore::new(),
        MockBackendFactory::new(),
        crate::image::DisabledRuntimeImageRefs::shared(),
    )
    .await
    .expect("a machine-local orchestrator");
    local.set_pause_publisher(DiscardingPausePublisher::shared());
    local
}

fn cluster_create_request() -> crate::orchestrator::CreateSandboxRequest {
    crate::orchestrator::CreateSandboxRequest {
        traffic_access_token: None,
        source: crate::orchestrator::SandboxLaunchSource::Snapshot(Box::new(
            RunnableSnapshot::mock(),
        )),
        // Long enough that neither replica's eviction loop can be what moved a
        // sandbox out from under an assertion.
        expiry: crate::orchestrator::SandboxExpiry::After(std::time::Duration::from_secs(600)),
        timeout_action: crate::orchestrator::SandboxTimeoutAction::Pause,
        user_metadata: None,
        env_vars: None,
        network_policy: Default::default(),
        custom_extension_params: None,
        control_plane_config: None,
        execution_id: None,
        auto_resume: false,
        secure: false,
        preferred_node_id: None,
    }
}

async fn running_on(node: &RunningNode) -> Vec<crate::types::SandboxId> {
    let mut ids: Vec<_> = node
        .orchestration
        .as_ref()
        .expect("a real node")
        .list_live_sandboxes()
        .await
        .expect("the node can list what it is running")
        .into_iter()
        .map(|sandbox| sandbox.sandbox_id)
        .collect();
    ids.sort();
    ids
}

async fn wait_until_unreachable(endpoint: &str) {
    let addr = endpoint
        .trim_start_matches("http://")
        .trim_end_matches('/')
        .to_string();
    for _ in 0..500 {
        if tokio::net::TcpStream::connect(&addr).await.is_err() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("the node kept answering after it was told to stop");
}

#[tokio::test]
async fn a_pause_pauses_the_same_vm_whichever_replica_it_lands_on() {
    let node = real_node().await;
    let on_the_node = node.orchestration.as_ref().expect("a real node").clone();
    let ledger = SharedLedger(Arc::new(InMemoryMetadataStore::new()));
    let started_here = api_replica(&node, &ledger).await;
    let landed_elsewhere = api_replica(&node, &ledger).await;

    let owned = Arc::clone(&started_here)
        .create_sandbox(cluster_create_request())
        .await
        .expect("a replica creates a sandbox on the node");
    let stray = Arc::clone(&started_here)
        .create_sandbox(cluster_create_request())
        .await
        .expect("a replica creates a second sandbox on the node");

    let mut both = vec![owned.id, stray.id];
    both.sort();
    assert_eq!(
        running_on(&node).await,
        both,
        "the node is not running both"
    );

    // Face 1: the replica that started it. This is the request that always
    // worked, and it is here so that face 2 means something.
    Arc::clone(&started_here)
        .pause_sandbox(owned.id)
        .await
        .expect("the replica that started the sandbox pauses it");

    Arc::clone(&landed_elsewhere)
        .pause_sandbox(stray.id)
        .await
        .expect("a replica that did not start the sandbox pauses it");

    assert_eq!(
        running_on(&node).await,
        Vec::<crate::types::SandboxId>::new(),
        "a pause reported success and left the VM running"
    );

    for sandbox_id in [owned.id, stray.id] {
        assert!(
            on_the_node
                .get_sandbox(&sandbox_id)
                .await
                .expect("the node's own record")
                .is_none(),
            "the node kept a record of a sandbox it paused"
        );
        assert!(
            ledger
                .0
                .get(&sandbox_id)
                .await
                .expect("read the ledger")
                .is_none(),
            "the shared record outlived the pause"
        );
    }
}

#[tokio::test]
async fn a_missing_handle_is_an_absence_only_where_the_runtime_would_be_in_process() {
    let node = real_node().await;
    let ledger = SharedLedger(Arc::new(InMemoryMetadataStore::new()));
    let started_here = api_replica(&node, &ledger).await;
    let landed_elsewhere = api_replica(&node, &ledger).await;

    // Face 1: sandboxes on other machines. The replica taking the call holds no
    // handle, and the pause still has to reach the machine.
    let remote = Arc::clone(&started_here)
        .create_sandbox(cluster_create_request())
        .await
        .expect("create on the node");
    Arc::clone(&landed_elsewhere)
        .pause_sandbox(remote.id)
        .await
        .expect("a replica holding no handle pauses the sandbox");
    assert!(
        running_on(&node).await.is_empty(),
        "a replica holding no handle answered the pause without reaching the machine"
    );

    // Face 2: sandboxes in this process. The same missing handle, and here it
    // really does mean the runtime is gone — so the record goes.
    let local = local_half().await;
    let mine = Arc::clone(&local)
        .create_sandbox(cluster_create_request())
        .await
        .expect("create in this process");
    assert!(
        local.forget_sandbox_handle_for_test(&mine.id).await,
        "the sandbox this process started had no handle to forget"
    );
    let err = Arc::clone(&local)
        .pause_sandbox(mine.id)
        .await
        .expect_err("a runtime that is gone cannot be paused");
    assert!(
        matches!(err, crate::orchestrator::OrchestratorError::SandboxNotFound(id) if id == mine.id),
        "{err:?}"
    );
    assert!(
        local
            .get_sandbox(&mine.id)
            .await
            .expect("read the record")
            .is_none(),
        "the record of a runtime that is gone was kept"
    );
}

#[tokio::test]
async fn a_pause_that_cannot_reach_the_machine_leaves_the_record_alone() {
    let node = real_node().await;
    let endpoint = node.endpoint.endpoint.clone();
    let ledger = SharedLedger(Arc::new(InMemoryMetadataStore::new()));
    let started_here = api_replica(&node, &ledger).await;
    let landed_elsewhere = api_replica(&node, &ledger).await;

    let reachable = Arc::clone(&started_here)
        .create_sandbox(cluster_create_request())
        .await
        .expect("create on the node");
    let unreachable = Arc::clone(&started_here)
        .create_sandbox(cluster_create_request())
        .await
        .expect("create on the node");

    // Control face: the machine is up, so the pause goes through.
    Arc::clone(&landed_elsewhere)
        .pause_sandbox(reachable.id)
        .await
        .expect("the machine is up");

    // And now it is not.
    drop(node);
    wait_until_unreachable(&endpoint).await;

    let err = Arc::clone(&landed_elsewhere)
        .pause_sandbox(unreachable.id)
        .await
        .expect_err("a pause cannot succeed against a machine that is not answering");
    assert!(
        format!("{err}").contains("could not be reached"),
        "the failure did not say the machine was unreachable: {err}"
    );

    let record = ledger
        .0
        .get(&unreachable.id)
        .await
        .expect("read the ledger")
        .expect("🔴 an unreachable machine caused the shared record to be deleted");
    assert_eq!(record.state, crate::orchestrator::SandboxState::Running);

    assert!(
        ledger
            .0
            .get(&reachable.id)
            .await
            .expect("read the ledger")
            .is_none(),
        "the sandbox the pause did reach kept its record"
    );
}

#[tokio::test]
async fn a_delete_on_a_replica_that_did_not_start_the_sandbox_reaches_the_machine() {
    let node = real_node().await;
    let endpoint = node.endpoint.endpoint.clone();
    let ledger = SharedLedger(Arc::new(InMemoryMetadataStore::new()));
    let started_here = api_replica(&node, &ledger).await;
    let landed_elsewhere = api_replica(&node, &ledger).await;

    let torn_down = Arc::clone(&started_here)
        .create_sandbox(cluster_create_request())
        .await
        .expect("create on the node");
    let kept = Arc::clone(&started_here)
        .create_sandbox(cluster_create_request())
        .await
        .expect("create on the node");

    Arc::clone(&landed_elsewhere)
        .delete_sandbox(torn_down.id)
        .await
        .expect("a replica that did not start the sandbox deletes it");
    assert_eq!(
        running_on(&node).await,
        vec![kept.id],
        "the delete answered success without telling the machine"
    );
    assert!(
        ledger
            .0
            .get(&torn_down.id)
            .await
            .expect("read the ledger")
            .is_none(),
        "a completed delete left the record behind"
    );

    // Second face: the machine stops answering, so the delete must fail with
    // the record intact rather than forget a VM that may still be up.
    drop(node);
    wait_until_unreachable(&endpoint).await;

    let err = Arc::clone(&landed_elsewhere)
        .delete_sandbox(kept.id)
        .await
        .expect_err("a delete cannot complete against a machine that is not answering");
    assert!(
        format!("{err}").contains("could not be reached"),
        "the failure did not say the machine was unreachable: {err}"
    );
    let record = ledger
        .0
        .get(&kept.id)
        .await
        .expect("read the ledger")
        .expect("🔴 a delete that never reached the machine forgot the sandbox anyway");
    assert_eq!(record.state, crate::orchestrator::SandboxState::Running);
}

#[tokio::test]
async fn a_replica_going_away_leaves_the_clusters_sandboxes_running() {
    let node = real_node().await;
    let ledger = SharedLedger(Arc::new(InMemoryMetadataStore::new()));
    let replica = api_replica(&node, &ledger).await;

    let elsewhere = Arc::clone(&replica)
        .create_sandbox(cluster_create_request())
        .await
        .expect("create on the node");
    Arc::clone(&replica)
        .shutdown()
        .await
        .expect("the replica shuts down");

    assert_eq!(
        running_on(&node).await,
        vec![elsewhere.id],
        "🔴 a replica shutting down paused a sandbox on a machine that is still up"
    );
    assert_eq!(
        ledger
            .0
            .get(&elsewhere.id)
            .await
            .expect("read the ledger")
            .expect("the shared record survived a replica restart")
            .state,
        crate::orchestrator::SandboxState::Running,
    );

    // Control face: a half whose VMs are in its own process stops them on the
    // way out.
    let local = local_half().await;
    let mine = Arc::clone(&local)
        .create_sandbox(cluster_create_request())
        .await
        .expect("create in this process");
    Arc::clone(&local)
        .shutdown()
        .await
        .expect("the local half shuts down");
    assert!(
        local
            .get_sandbox(&mine.id)
            .await
            .expect("read the record")
            .is_none(),
        "a half that runs its own VMs left one behind on the way out"
    );
    assert!(
        local.list_live_sandboxes().await.expect("list").is_empty(),
        "a half that runs its own VMs shut down with one still up"
    );
}

#[tokio::test]
async fn an_egress_policy_set_on_a_replica_that_did_not_start_the_sandbox_reaches_the_machine() {
    let node = real_node().await;
    let on_the_node = node.orchestration.as_ref().expect("a real node").clone();
    let ledger = SharedLedger(Arc::new(InMemoryMetadataStore::new()));
    let started_here = api_replica(&node, &ledger).await;
    let landed_elsewhere = api_replica(&node, &ledger).await;

    let sandbox = Arc::clone(&started_here)
        .create_sandbox(cluster_create_request())
        .await
        .expect("create on the node");

    let policy = crate::sandbox::SandboxNetworkPolicy::new(
        crate::sandbox::BaseSandboxNetworkPolicy::Deny,
        Default::default(),
    );
    Arc::clone(&landed_elsewhere)
        .replace_sandbox_network_policy(sandbox.id, policy.clone())
        .await
        .expect("a replica that did not start the sandbox sets its egress policy");

    assert_eq!(
        on_the_node
            .get_sandbox(&sandbox.id)
            .await
            .expect("the node's own record")
            .expect("the node kept a record")
            .network_policy,
        policy,
        "the policy never reached the machine running the sandbox"
    );

    // Control face: in a half that runs its own VMs, a handle that is not there
    // is a sandbox that is not there to reconfigure.
    let local = local_half().await;
    let mine = Arc::clone(&local)
        .create_sandbox(cluster_create_request())
        .await
        .expect("create in this process");
    assert!(
        local.forget_sandbox_handle_for_test(&mine.id).await,
        "the sandbox this process started had no handle to forget"
    );
    let err = Arc::clone(&local)
        .replace_sandbox_network_policy(mine.id, policy)
        .await
        .expect_err("a runtime that is gone cannot be reconfigured");
    assert!(
        matches!(
            err,
            crate::orchestrator::OrchestratorError::SandboxOperationConflict { sandbox_id, .. }
                if sandbox_id == mine.id
        ),
        "{err:?}"
    );
}

async fn routed_to(
    replica: &ApiReplica,
    sandbox_id: crate::types::SandboxId,
) -> std::net::Ipv4Addr {
    match replica
        .proxy_lookup_for(&sandbox_id)
        .await
        .expect("the replica can look a route up")
    {
        crate::orchestrator::ProxyLookupResult::Ready(target) => target.ip,
        other => panic!("sandbox {sandbox_id} is not routable: {other:?}"),
    }
}

#[tokio::test]
async fn a_fork_driven_by_the_deciding_half_routes_each_child_to_its_own_vm() {
    let node = real_node().await;
    let ledger = SharedLedger(Arc::new(InMemoryMetadataStore::new()));
    let replica = api_replica(&node, &ledger).await;

    let source = Arc::clone(&replica)
        .create_sandbox(cluster_create_request())
        .await
        .expect("a replica creates the sandbox it will fork");

    let outcomes = Arc::clone(&replica)
        .fork_sandbox(
            source.id,
            crate::orchestrator::ForkChildren::Fresh(2),
            crate::orchestrator::NewTimeout::UseExisting,
        )
        .await
        .expect("the fork ran");
    assert_eq!(outcomes.len(), 2);
    let children = outcomes
        .into_iter()
        .map(|outcome| outcome.expect("a fork child whose VM started on the node"))
        .collect::<Vec<_>>();

    let source_address = routed_to(&replica, source.id).await;
    let first = routed_to(&replica, children[0].id).await;
    let second = routed_to(&replica, children[1].id).await;

    assert_ne!(
        first, second,
        "two fork children were routed to one address"
    );
    assert_ne!(
        first, source_address,
        "a fork child was routed to the sandbox it was forked from"
    );
    assert_ne!(
        second, source_address,
        "a fork child was routed to the sandbox it was forked from"
    );

    let mut running = vec![source.id, children[0].id, children[1].id];
    running.sort();
    assert_eq!(
        running_on(&node).await,
        running,
        "the machine is not running what this half just routed traffic to"
    );
}

#[tokio::test]
async fn a_child_the_node_reported_no_address_for_arrives_here_with_none() {
    let (script, node) = scripted_node().await;
    let execution_id = ExecutionId::new();
    *script.create.lock().expect("lock") = Some(Ok(pb::SandboxCreateResponse {
        sandbox_id: launch_config().sandbox_id.to_string(),
        execution_id: execution_id.to_string(),
        host_interaction_ip: "10.9.9.9".to_string(),
        rootfs_virtual_size: 1024,
        ..Default::default()
    }));

    let specs = (0..2)
        .map(|_| SandboxForkSpec {
            sandbox_id: crate::types::SandboxId::new(),
            execution_id: ExecutionId::new(),
            envd_access_token: None,
        })
        .collect::<Vec<_>>();
    *script.fork.lock().expect("lock") = Some(Ok(pb::SandboxForkResponse {
        children: vec![
            pb::ForkChildResult {
                sandbox_id: specs[0].sandbox_id.to_string(),
                execution_id: specs[0].execution_id.to_string(),
                outcome: Some(pb::fork_child_result::Outcome::Started(
                    pb::SandboxCreateResponse {
                        sandbox_id: specs[0].sandbox_id.to_string(),
                        execution_id: specs[0].execution_id.to_string(),
                        host_interaction_ip: "10.4.5.6".to_string(),
                        rootfs_virtual_size: 8192,
                        ..Default::default()
                    },
                )),
            },
            // The same child, one value each way: no address, no size.
            pb::ForkChildResult {
                sandbox_id: specs[1].sandbox_id.to_string(),
                execution_id: specs[1].execution_id.to_string(),
                outcome: Some(pb::fork_child_result::Outcome::Started(
                    pb::SandboxCreateResponse {
                        sandbox_id: specs[1].sandbox_id.to_string(),
                        execution_id: specs[1].execution_id.to_string(),
                        host_interaction_ip: String::new(),
                        rootfs_virtual_size: 0,
                        ..Default::default()
                    },
                )),
            },
        ],
    }));

    let factory = RemoteSandboxBackendFactory::new(node.placement());
    let mut backend =
        match factory.build_from_snapshot(&RunnableSnapshot::mock(), launch_config(), execution_id)
        {
            Ok(backend) => backend,
            Err(err) => panic!("building a stub should not fail: {err:#}"),
        };
    backend.start().await.expect("start");

    let mut children = backend.fork(&specs).await.expect("fork").into_iter();
    let reported: Box<dyn SandboxBackend> = children
        .next()
        .expect("the first child")
        .expect("which started");
    let blank: Box<dyn SandboxBackend> = children
        .next()
        .expect("the second child")
        .expect("which started");

    assert_eq!(
        reported.host_interaction_ip(),
        Some(std::net::Ipv4Addr::new(10, 4, 5, 6)),
        "the address the node reported for a fork child did not reach this half"
    );
    assert_eq!(
        reported.runtime_info().rootfs_virtual_size,
        Some(8192),
        "the rootfs size the node reported for a fork child did not reach this half"
    );

    assert_eq!(
        blank.host_interaction_ip(),
        None,
        "a fork child the node gave no address for was made to look routable"
    );
    assert_eq!(
        blank.runtime_info().rootfs_virtual_size,
        None,
        "a fork child the node gave no rootfs size for was given one anyway"
    );
}

async fn adopted(
    node: &RunningNode,
    sandbox: &crate::orchestrator::SandboxMetadata,
) -> Box<dyn SandboxBackend> {
    let factory = RemoteSandboxBackendFactory::new(node.placement());
    let mut backend = factory
        .adopt_running(sandbox.id, sandbox.execution_id, sandbox.resources)
        .expect("a remote factory can say where its sandboxes live")
        .expect("a remote factory's sandboxes outlive the process that started them");
    backend
        .start()
        .await
        .expect("attaching starts nothing; it finds the machine");
    backend
}

#[tokio::test]
async fn a_replica_that_did_not_start_a_sandbox_says_the_same_address_as_the_one_that_did() {
    let node = real_node().await;
    let ledger = SharedLedger(Arc::new(InMemoryMetadataStore::new()));
    let owner = api_replica(&node, &ledger).await;

    let source = Arc::clone(&owner)
        .create_sandbox(cluster_create_request())
        .await
        .expect("a replica creates a sandbox");
    let children = Arc::clone(&owner)
        .fork_sandbox(
            source.id,
            crate::orchestrator::ForkChildren::Fresh(2),
            crate::orchestrator::NewTimeout::UseExisting,
        )
        .await
        .expect("the fork ran")
        .into_iter()
        .map(|outcome| outcome.expect("a fork child whose VM started on the node"))
        .collect::<Vec<_>>();

    let sandboxes = [source, children[0].clone(), children[1].clone()];

    let mut published = Vec::new();
    let mut adopted_addresses = Vec::new();
    for sandbox in &sandboxes {
        published.push(routed_to(&owner, sandbox.id).await);
        adopted_addresses.push(
            adopted(&node, sandbox)
                .await
                .host_interaction_ip()
                .expect("🔴 a replica that did not start this sandbox could not say where it is"),
        );
    }

    assert_eq!(
        adopted_addresses, published,
        "the replica that adopted these sandboxes and the one that started them do not agree on \
         where they are"
    );

    assert_ne!(
        adopted_addresses[0], adopted_addresses[1],
        "a sandbox and its fork child were adopted at one address"
    );
    assert_ne!(
        adopted_addresses[1], adopted_addresses[2],
        "two fork children were adopted at one address"
    );
    assert_ne!(
        adopted_addresses[0], adopted_addresses[2],
        "a sandbox and its fork child were adopted at one address"
    );

    // And the machine agrees all three are up, so nothing above is satisfied by
    // an address for a VM that is not there.
    let mut running = sandboxes
        .iter()
        .map(|sandbox| sandbox.id)
        .collect::<Vec<_>>();
    running.sort();
    assert_eq!(
        running_on(&node).await,
        running,
        "the machine is not running what this half just said it could reach"
    );
}

#[tokio::test]
async fn a_machine_that_could_not_be_asked_is_not_one_that_is_not_running_it() {
    let (script, node) = scripted_node().await;
    let factory = RemoteSandboxBackendFactory::new(node.placement());

    let stranger = crate::types::SandboxId::new();
    let resources = crate::types::SandboxResources {
        cpu_count: 1,
        memory_mib: 256,
        disk_size_mib: 1024,
    };

    // The scripted node answers `NOT_FOUND` when it was told to run nothing.
    let mut answered = factory
        .adopt_running(stranger, ExecutionId::new(), resources)
        .expect("a remote factory can say where its sandboxes live")
        .expect("a remote factory's sandboxes are on other machines");
    answered
        .start()
        .await
        .expect("a machine that says it is running nothing under this id has answered");
    assert_eq!(
        answered.host_interaction_ip(),
        None,
        "the machine said it is running nothing under this id, and an address appeared anyway"
    );
    assert_eq!(
        answered.runtime_info().rootfs_virtual_size,
        None,
        "the machine said it is running nothing under this id, and a rootfs size appeared anyway"
    );

    // The same call, to the same machine, one value different.
    *script.describe.lock().expect("lock") = Some(Err(Status::unavailable(
        "the node could not look right now",
    )));
    let mut unanswered = factory
        .adopt_running(stranger, ExecutionId::new(), resources)
        .expect("a remote factory can say where its sandboxes live")
        .expect("a remote factory's sandboxes are on other machines");
    let err = unanswered
        .start()
        .await
        .expect_err("🔴 a machine that could not answer was read as one with no such sandbox");
    assert!(
        format!("{err:#}").contains(&stranger.to_string()),
        "the failure did not name the sandbox it could not ask about: {err:#}"
    );
}

#[tokio::test]
async fn an_address_the_node_read_off_no_handle_is_refused_rather_than_recorded() {
    let (script, node) = scripted_node().await;
    let factory = RemoteSandboxBackendFactory::new(node.placement());
    let sandbox_id = crate::types::SandboxId::new();
    let execution_id = ExecutionId::new();
    let resources = crate::types::SandboxResources {
        cpu_count: 1,
        memory_mib: 256,
        disk_size_mib: 1024,
    };
    let reply = |facts_from_handle: bool| pb::SandboxDescribeResponse {
        execution_id: execution_id.to_string(),
        host_interaction_ip: "10.7.7.7".to_string(),
        rootfs_virtual_size: 4096,
        facts_from_handle,
    };

    *script.describe.lock().expect("lock") = Some(Ok(reply(false)));
    let mut from_a_record = factory
        .adopt_running(sandbox_id, execution_id, resources)
        .expect("a remote factory can say where its sandboxes live")
        .expect("a remote factory's sandboxes are on other machines");
    let err = from_a_record
        .start()
        .await
        .expect_err("🔴 a blank the node could not fill was taken as this sandbox's address");
    assert!(
        format!("{err:#}").contains("busy"),
        "the failure did not say why the facts were not facts: {err:#}"
    );

    // The same reply, one bit different.
    *script.describe.lock().expect("lock") = Some(Ok(reply(true)));
    let mut from_a_handle = factory
        .adopt_running(sandbox_id, execution_id, resources)
        .expect("a remote factory can say where its sandboxes live")
        .expect("a remote factory's sandboxes are on other machines");
    from_a_handle
        .start()
        .await
        .expect("facts the node read off the handle are facts");
    assert_eq!(
        from_a_handle.host_interaction_ip(),
        Some(std::net::Ipv4Addr::new(10, 7, 7, 7)),
        "the address the node read off the live handle did not reach this half"
    );
    assert_eq!(
        from_a_handle.runtime_info().rootfs_virtual_size,
        Some(4096),
        "the rootfs size the node read off the live handle did not reach this half"
    );
}

// Exercises the interval between sandbox creation and the cluster learning its
// node binding.

struct ClusterPlacement {
    node: NodeEndpoint,
    bindings: Mutex<std::collections::HashSet<crate::types::SandboxId>>,
    /// Reservations written before a create and cleared by its confirmation, the
    /// way the binding store's `Starting` state is.
    reservations: Mutex<std::collections::HashSet<crate::types::SandboxId>>,
    records: bool,
    record_fails: bool,
    /// Whether the reservation write lands. `false` is a cluster that cannot
    /// record, which must refuse the launch outright.
    reserves: bool,
    /// Whether a reservation is written at all. `false` is a build without the
    /// create-time reservation, kept so a test can show what the delete verdict
    /// does without it.
    reserves_at_all: bool,
    recorded: Mutex<Vec<(crate::types::SandboxId, ExecutionId, NodeEndpoint, u32)>>,
    /// The placement preference each launch arrived with, in order.
    preferred: Mutex<Vec<Option<String>>>,
    /// The exclusions each placement call arrived with, in order.
    excluded: Mutex<Vec<Vec<String>>>,
    /// Nodes offered once `node` is excluded, in order.
    spares: Vec<NodeEndpoint>,
    membership: Mutex<MembershipAnswer>,
}

#[derive(Clone, Copy)]
enum MembershipAnswer {
    Present,
    Gone,
    Unavailable,
}

impl ClusterPlacement {
    fn recording(node: NodeEndpoint) -> Arc<Self> {
        Arc::new(Self {
            bindings: Mutex::new(Default::default()),
            reservations: Mutex::new(Default::default()),
            records: true,
            record_fails: false,
            reserves: true,
            reserves_at_all: true,
            recorded: Mutex::new(Vec::new()),
            preferred: Mutex::new(Vec::new()),
            excluded: Mutex::new(Vec::new()),
            spares: Vec::new(),
            membership: Mutex::new(MembershipAnswer::Present),
            node,
        })
    }

    // A cluster with a second node to offer once the first is excluded.
    fn recording_with_spare(node: NodeEndpoint, spare: NodeEndpoint) -> Arc<Self> {
        let mut placement = Arc::try_unwrap(Self::recording(node))
            .ok()
            .expect("sole owner");
        placement.spares = vec![spare];
        Arc::new(placement)
    }

    // A cluster whose binding store refuses the reservation write.
    fn refusing_reservations(node: NodeEndpoint) -> Arc<Self> {
        let mut placement = Arc::try_unwrap(Self::recording(node))
            .ok()
            .expect("sole owner");
        placement.reserves = false;
        Arc::new(placement)
    }

    // A cluster that records confirmations but never reservations.
    fn never_reserving(node: NodeEndpoint) -> Arc<Self> {
        let mut placement = Arc::try_unwrap(Self::recording(node))
            .ok()
            .expect("sole owner");
        placement.reserves_at_all = false;
        Arc::new(placement)
    }

    fn refusing_records(node: NodeEndpoint) -> Arc<Self> {
        let mut placement = Arc::try_unwrap(Self::recording(node))
            .ok()
            .expect("sole owner");
        placement.record_fails = true;
        Arc::new(placement)
    }

    fn heartbeat(&self, ids: &[crate::types::SandboxId]) {
        let mut bindings = self.bindings.lock().expect("lock");
        let mut reservations = self.reservations.lock().expect("lock");
        for id in ids {
            bindings.insert(*id);
            // A node reporting the sandbox is the node acknowledging it, which is
            // what `reconcile_node` promotes a reservation on.
            reservations.remove(id);
        }
    }

    fn is_bound(&self, id: crate::types::SandboxId) -> bool {
        self.bindings.lock().expect("lock").contains(&id)
    }

    fn is_reserved(&self, id: crate::types::SandboxId) -> bool {
        self.reservations.lock().expect("lock").contains(&id)
    }

    // Drops every record of a sandbox, as a pause plus an expired reservation does.
    fn forget(&self, id: crate::types::SandboxId) {
        self.bindings.lock().expect("lock").remove(&id);
        self.reservations.lock().expect("lock").remove(&id);
    }

    // Installs a reservation without a launch, as another replica's in-flight
    // rebuild of this sandbox looks from here.
    fn reserve_elsewhere(&self, id: crate::types::SandboxId) {
        self.reservations.lock().expect("lock").insert(id);
    }

    fn recorded(&self) -> Vec<(crate::types::SandboxId, ExecutionId, NodeEndpoint, u32)> {
        self.recorded.lock().expect("lock").clone()
    }

    fn preferred(&self) -> Vec<Option<String>> {
        self.preferred.lock().expect("lock").clone()
    }

    fn exclusions_seen(&self) -> Vec<Vec<String>> {
        self.excluded.lock().expect("lock").clone()
    }

    fn set_membership(&self, answer: MembershipAnswer) {
        *self.membership.lock().expect("lock") = answer;
    }
}

#[async_trait]
impl NodePlacement for ClusterPlacement {
    async fn place_new(
        &self,
        _sandbox_id: crate::types::SandboxId,
        _resources: crate::types::SandboxResources,
        preferred_node_id: Option<&str>,
        excluded_node_ids: &[String],
    ) -> anyhow::Result<NodeEndpoint> {
        self.preferred
            .lock()
            .expect("lock")
            .push(preferred_node_id.map(str::to_string));
        self.excluded
            .lock()
            .expect("lock")
            .push(excluded_node_ids.to_vec());
        // The first node not excluded; with none left, the excluded first
        // node again, which is what a placement source with nowhere else says.
        Ok(std::iter::once(&self.node)
            .chain(self.spares.iter())
            .find(|node| !excluded_node_ids.contains(&node.node_id))
            .unwrap_or(&self.node)
            .clone())
    }

    async fn place_existing(
        &self,
        sandbox_id: crate::types::SandboxId,
    ) -> anyhow::Result<Option<NodeEndpoint>> {
        // A reservation is neither an answer nor an absence, exactly as
        // `lookup_node` treats a `Starting` binding.
        if self.is_reserved(sandbox_id) {
            return Err(anyhow::Error::new(tonic::Status::unavailable(
                "a create for this sandbox has not finished",
            ))
            .context(format!("the local scheduler could not locate {sandbox_id}")));
        }
        Ok(self.is_bound(sandbox_id).then(|| self.node.clone()))
    }

    async fn resolve_node(&self, _node_id: &str) -> anyhow::Result<NodeEndpoint> {
        Ok(self.node.clone())
    }

    async fn node_membership(&self, node_id: &str) -> anyhow::Result<NodeMembership> {
        assert_eq!(
            node_id, self.node.node_id,
            "asked about the membership of a node this test never placed a sandbox on"
        );
        match *self.membership.lock().expect("lock") {
            MembershipAnswer::Present => Ok(NodeMembership::Present),
            MembershipAnswer::Gone => Ok(NodeMembership::Gone),
            MembershipAnswer::Unavailable => anyhow::bail!(
                "the scheduler could not say whether node {node_id} is still in the cluster: \
                 unavailable"
            ),
        }
    }

    async fn record_placement(
        &self,
        sandbox_id: crate::types::SandboxId,
        execution_id: ExecutionId,
        node: &NodeEndpoint,
        projection_ttl_secs: u32,
    ) -> anyhow::Result<()> {
        self.recorded.lock().expect("lock").push((
            sandbox_id,
            execution_id,
            node.clone(),
            projection_ttl_secs,
        ));
        if self.record_fails {
            anyhow::bail!("the scheduler refused an assignment for {sandbox_id}");
        }
        if self.records {
            // Only a write that lands replaces the reservation it was written over;
            // a lost confirmation leaves the reservation to expire on its own.
            self.reservations.lock().expect("lock").remove(&sandbox_id);
            self.bindings.lock().expect("lock").insert(sandbox_id);
        }
        Ok(())
    }

    async fn reserve_placement(
        &self,
        sandbox_id: crate::types::SandboxId,
        _execution_id: ExecutionId,
        _node: &NodeEndpoint,
    ) -> anyhow::Result<()> {
        if !self.reserves {
            anyhow::bail!("the scheduler refused to reserve {sandbox_id}");
        }
        if self.reserves_at_all {
            self.reservations.lock().expect("lock").insert(sandbox_id);
        }
        Ok(())
    }

    async fn release_placement_reservation(
        &self,
        sandbox_id: crate::types::SandboxId,
        _execution_id: ExecutionId,
    ) -> anyhow::Result<()> {
        self.reservations.lock().expect("lock").remove(&sandbox_id);
        Ok(())
    }

    async fn forget_placement(
        &self,
        _sandbox_id: crate::types::SandboxId,
        _execution_id: ExecutionId,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn a_create_reserves_the_routing_record_before_it_asks_for_a_runtime() {
    let (script, node) = scripted_node().await;
    let execution_id = ExecutionId::new();
    let config = launch_config();
    *script.create.lock().expect("lock") = Some(Ok(pb::SandboxCreateResponse {
        sandbox_id: config.sandbox_id.to_string(),
        execution_id: execution_id.to_string(),
        ..Default::default()
    }));

    let placement = ClusterPlacement::refusing_reservations(node.endpoint.clone());
    let factory =
        RemoteSandboxBackendFactory::new(Arc::clone(&placement) as Arc<dyn NodePlacement>);
    let mut backend = factory
        .build_from_snapshot(&RunnableSnapshot::mock(), config, execution_id)
        .expect("building a stub does no I/O");

    let err = backend
        .start()
        .await
        .expect_err("a cluster that cannot record the assignment must not start a runtime");
    assert!(
        format!("{err:#}").contains("reserve a routing record"),
        "the refusal did not say the reservation was what failed: {err:#}"
    );
    assert!(
        script.seen_create.lock().expect("lock").is_empty(),
        "the node was asked for a runtime the cluster had no record of: recording after the \
         create is what leaves an orphan the delete path then reads as a live sandbox"
    );
}

#[tokio::test]
async fn a_reservation_becomes_a_binding_when_the_node_acknowledges_the_create() {
    let (script, node) = scripted_node().await;
    let execution_id = ExecutionId::new();
    let config = launch_config();
    let sandbox_id = config.sandbox_id;
    *script.create.lock().expect("lock") = Some(Ok(pb::SandboxCreateResponse {
        sandbox_id: sandbox_id.to_string(),
        execution_id: execution_id.to_string(),
        ..Default::default()
    }));

    let placement = ClusterPlacement::recording(node.endpoint.clone());
    let factory =
        RemoteSandboxBackendFactory::new(Arc::clone(&placement) as Arc<dyn NodePlacement>);
    let mut backend = factory
        .build_from_snapshot(&RunnableSnapshot::mock(), config, execution_id)
        .expect("building a stub does no I/O");
    backend.start().await.expect("start");

    assert!(
        placement.is_bound(sandbox_id),
        "a started sandbox is not routable"
    );
    assert!(
        !placement.is_reserved(sandbox_id),
        "the reservation outlived the create it was written for, so the sandbox stays \
         unroutable until it expires"
    );
}

#[tokio::test]
async fn a_create_the_node_refused_withdraws_its_reservation() {
    let (script, node) = scripted_node().await;
    let execution_id = ExecutionId::new();
    let config = launch_config();
    let sandbox_id = config.sandbox_id;
    *script.create.lock().expect("lock") =
        Some(Err(Status::resource_exhausted("no room on this node")));

    let placement = ClusterPlacement::recording(node.endpoint.clone());
    let factory =
        RemoteSandboxBackendFactory::new(Arc::clone(&placement) as Arc<dyn NodePlacement>);
    let mut backend = factory
        .build_from_snapshot(&RunnableSnapshot::mock(), config, execution_id)
        .expect("building a stub does no I/O");
    backend.start().await.expect_err("the node refused");

    assert!(
        !placement.is_reserved(sandbox_id),
        "a launch that never started anything left its id reserved, so nothing can reap it \
         until the reservation expires"
    );
    assert!(!placement.is_bound(sandbox_id));
}

#[tokio::test]
async fn a_create_a_draining_node_refused_is_placed_on_another_node() {
    let (script, refusing) = scripted_node().await;
    *script.create.lock().expect("lock") = Some(Err(Status::unavailable(
        "node is isolated and is not taking new sandboxes",
    )));
    let refusing_endpoint =
        NodeEndpoint::same_address("node-draining", refusing.endpoint.endpoint.clone());
    let accepting = real_node().await;

    let placement =
        ClusterPlacement::recording_with_spare(refusing_endpoint, accepting.endpoint.clone());
    let factory =
        RemoteSandboxBackendFactory::new(Arc::clone(&placement) as Arc<dyn NodePlacement>);
    let config = launch_config();
    let sandbox_id = config.sandbox_id;
    let mut backend = factory
        .build_from_snapshot(&RunnableSnapshot::mock(), config, ExecutionId::new())
        .expect("building a stub does no I/O");

    backend
        .start()
        .await
        .expect("the refusal of one node is not the answer while another can take it");

    assert_eq!(
        backend.holding_node_id(),
        Some(accepting.endpoint.node_id.as_str()),
        "the sandbox is running where the second placement put it"
    );
    assert_eq!(
        placement.exclusions_seen(),
        vec![Vec::new(), vec!["node-draining".to_string()]],
        "the second placement was asked to avoid the node that refused"
    );
    assert_eq!(
        script.seen_create.lock().expect("lock").len(),
        1,
        "the refusing node was asked exactly once"
    );
    assert_eq!(running_on(&accepting).await, vec![sandbox_id]);
    assert!(
        placement.is_bound(sandbox_id),
        "the placement that succeeded is the one recorded"
    );
}

#[tokio::test]
async fn a_create_refused_for_its_request_is_not_retried_elsewhere() {
    let (script, refusing) = scripted_node().await;
    *script.create.lock().expect("lock") =
        Some(Err(Status::invalid_argument("source is required")));
    let refusing_endpoint =
        NodeEndpoint::same_address("node-strict", refusing.endpoint.endpoint.clone());
    let accepting = real_node().await;

    let placement =
        ClusterPlacement::recording_with_spare(refusing_endpoint, accepting.endpoint.clone());
    let factory =
        RemoteSandboxBackendFactory::new(Arc::clone(&placement) as Arc<dyn NodePlacement>);
    let config = launch_config();
    let sandbox_id = config.sandbox_id;
    let mut backend = factory
        .build_from_snapshot(&RunnableSnapshot::mock(), config, ExecutionId::new())
        .expect("building a stub does no I/O");

    let error = backend
        .start()
        .await
        .expect_err("a request another node would refuse the same way fails here");

    assert!(
        format!("{error:#}").contains("source is required"),
        "the node's own reason is the error: {error:#}"
    );
    assert_eq!(
        placement.exclusions_seen().len(),
        1,
        "placement was not asked a second time"
    );
    assert!(running_on(&accepting).await.is_empty());
    assert!(!placement.is_reserved(sandbox_id));
}

#[tokio::test]
async fn a_refusal_with_nowhere_else_to_go_is_the_answer() {
    let (script, refusing) = scripted_node().await;
    *script.create.lock().expect("lock") = Some(Err(Status::unavailable(
        "node is isolated and is not taking new sandboxes",
    )));
    let placement = ClusterPlacement::recording(NodeEndpoint::same_address(
        "node-draining",
        refusing.endpoint.endpoint.clone(),
    ));
    let factory =
        RemoteSandboxBackendFactory::new(Arc::clone(&placement) as Arc<dyn NodePlacement>);
    let config = launch_config();
    let sandbox_id = config.sandbox_id;
    let mut backend = factory
        .build_from_snapshot(&RunnableSnapshot::mock(), config, ExecutionId::new())
        .expect("building a stub does no I/O");

    let error = backend.start().await.expect_err("the only node refused");

    assert!(
        format!("{error:#}").contains("not taking new sandboxes"),
        "the refusal, not a placement error, is what the caller sees: {error:#}"
    );
    assert_eq!(
        placement.exclusions_seen(),
        vec![Vec::new(), vec!["node-draining".to_string()]],
        "placement was asked once more, excluding the refuser, and offered it again"
    );
    assert!(!placement.is_reserved(sandbox_id));
}

async fn api_replica_on(placement: Arc<ClusterPlacement>, ledger: &SharedLedger) -> ApiReplica {
    let replica = Orchestrator::new(
        crate::sandbox::AccessTokenSeedPolicy::MayGenerate,
        ledger.clone(),
        RemoteSandboxBackendFactory::new(placement as Arc<dyn NodePlacement>),
        crate::image::DisabledRuntimeImageRefs::shared(),
    )
    .await
    .expect("a replica of the deciding half");
    replica.set_pause_publisher(DiscardingPausePublisher::shared());
    replica
}

#[tokio::test]
async fn a_create_tells_the_cluster_which_machine_the_sandbox_is_on() {
    let node = real_node().await;
    let dialled = node.endpoint.endpoint.clone();
    let advertised = "http://10.0.0.7:8000".to_string();
    assert_ne!(
        dialled, advertised,
        "the test's two addresses have to differ for it to be able to tell them apart"
    );
    let placement = ClusterPlacement::recording(NodeEndpoint {
        node_id: node.endpoint.node_id.clone(),
        endpoint: dialled.clone(),
        advertised_endpoint: advertised.clone(),
    });
    let factory =
        RemoteSandboxBackendFactory::new(Arc::clone(&placement) as Arc<dyn NodePlacement>);

    let mut started = Vec::new();
    for _ in 0..2 {
        let config = launch_config();
        let sandbox_id = config.sandbox_id;
        let execution_id = ExecutionId::new();
        let mut backend = factory
            .build_from_snapshot(&RunnableSnapshot::mock(), config, execution_id)
            .expect("build a stub");
        backend.start().await.expect("start on the node");
        started.push((sandbox_id, execution_id));
    }
    assert_ne!(
        started[0].1, started[1].1,
        "two mints produced one incarnation"
    );

    let recorded = placement.recorded();
    assert_eq!(recorded.len(), 2, "a create told the cluster nothing");
    for (index, (sandbox_id, execution_id)) in started.iter().enumerate() {
        let (recorded_id, recorded_execution, recorded_node, _) = &recorded[index];
        assert_eq!(recorded_id, sandbox_id);
        assert_eq!(
            recorded_execution, execution_id,
            "the assignment named a run other than the one the node acknowledged"
        );
        assert_eq!(recorded_node.node_id, node.endpoint.node_id);
        assert_eq!(
            recorded_node.advertised_endpoint, advertised,
            "the assignment carried an address the scheduler would refuse as an unknown node"
        );
        assert_ne!(
            recorded_node.advertised_endpoint, dialled,
            "the assignment carried the node-service address instead of the advertised one"
        );
        assert!(
            placement.is_bound(*sandbox_id),
            "the cluster still cannot say where this sandbox is"
        );
    }
}

#[tokio::test]
async fn a_create_budgets_the_routing_record_from_the_sandboxs_own_lifetime() {
    let node = real_node().await;
    let ledger = SharedLedger(Arc::new(InMemoryMetadataStore::new()));
    let told = ClusterPlacement::recording(node.endpoint.clone());
    let replica = api_replica_on(Arc::clone(&told), &ledger).await;

    let sandbox = Arc::clone(&replica)
        .create_sandbox(cluster_create_request())
        .await
        .expect("create on the node");
    let budget = ledger
        .0
        .get(&sandbox.id)
        .await
        .expect("read the ledger")
        .expect("the record the create wrote")
        .projection_ttl_secs(std::time::SystemTime::now());
    assert!(
        budget > 0,
        "the configured lifetime ceiling gives every new sandbox a budget of its own"
    );

    let recorded = told.recorded();
    assert_eq!(recorded.len(), 1, "a create told the cluster nothing");
    let recorded_ttl = recorded[0].3;
    // 🔴 Zero here is the store's default, a fraction of the sandbox's life:
    // once it lapses every request pays a resume RPC until the next heartbeat
    // rewrites the record with the budget the create already knew.
    assert!(
        recorded_ttl.abs_diff(budget) <= 2,
        "the create recorded a {recorded_ttl} s budget for a sandbox whose own record says \
         {budget} s"
    );
}

#[tokio::test]
async fn a_create_survives_a_cluster_that_refuses_the_assignment() {
    let node = real_node().await;

    // Face 1: the cluster says no.
    let refusing = ClusterPlacement::refusing_records(node.endpoint.clone());
    let refused = {
        let factory =
            RemoteSandboxBackendFactory::new(Arc::clone(&refusing) as Arc<dyn NodePlacement>);
        let config = launch_config();
        let sandbox_id = config.sandbox_id;
        let mut backend = factory
            .build_from_snapshot(&RunnableSnapshot::mock(), config, ExecutionId::new())
            .expect("build a stub");
        backend
            .start()
            .await
            .expect("a refused assignment failed a create that had already succeeded");
        sandbox_id
    };
    assert_eq!(
        refusing.recorded().len(),
        1,
        "the create did not even try to tell the cluster"
    );
    assert!(
        !refusing.is_bound(refused),
        "a refused write left a binding behind"
    );

    // Face 2: the same create against a cluster that accepts. One value
    // different, and the observable difference is the binding.
    let accepting = ClusterPlacement::recording(node.endpoint.clone());
    let accepted = {
        let factory =
            RemoteSandboxBackendFactory::new(Arc::clone(&accepting) as Arc<dyn NodePlacement>);
        let config = launch_config();
        let sandbox_id = config.sandbox_id;
        let mut backend = factory
            .build_from_snapshot(&RunnableSnapshot::mock(), config, ExecutionId::new())
            .expect("build a stub");
        backend.start().await.expect("start on the node");
        sandbox_id
    };
    assert_eq!(accepting.recorded().len(), 1);
    assert!(
        accepting.is_bound(accepted),
        "an accepted write recorded nothing"
    );

    assert_eq!(running_on(&node).await.len(), 2, "a create started no VM");
}

#[tokio::test]
async fn a_launch_tells_the_cluster_which_machine_last_held_the_bytes() {
    let node = real_node().await;
    let placement = ClusterPlacement::recording(node.endpoint.clone());
    let factory =
        RemoteSandboxBackendFactory::new(Arc::clone(&placement) as Arc<dyn NodePlacement>);

    let mut fresh = factory
        .build_from_snapshot(
            &RunnableSnapshot::mock(),
            launch_config(),
            ExecutionId::new(),
        )
        .expect("build a stub");
    fresh.start().await.expect("start on the node");

    let warm = SandboxLaunchConfig {
        sandbox_id: crate::types::SandboxId::new(),
        preferred_node_id: Some("node-that-held-the-bytes".to_string()),
        ..launch_config()
    };
    let mut resumed = factory
        .build_from_snapshot(&RunnableSnapshot::mock(), warm, ExecutionId::new())
        .expect("build a stub");
    resumed.start().await.expect("start on the node");

    assert_eq!(
        placement.preferred(),
        vec![None, Some("node-that-held-the-bytes".to_string())],
        "the cluster was not told where the bytes were last warm"
    );
    assert_eq!(
        running_on(&node).await.len(),
        2,
        "a preference the cluster did not honour stopped the launch"
    );
}

// Both sides of the invariant a create-time reservation establishes: a sandbox
// nothing in a warm cluster names has no runtime left to protect, while one a
// reservation names has a runtime on the way.
#[tokio::test]
async fn a_delete_reaps_an_orphan_but_never_a_sandbox_a_create_has_reserved() {
    let node = real_node().await;
    let on_the_node = node.orchestration.as_ref().expect("a real node").clone();
    let ledger = SharedLedger(Arc::new(InMemoryMetadataStore::new()));

    // Face 1: the create records where the sandbox is. No heartbeat in this face.
    let told = ClusterPlacement::recording(node.endpoint.clone());
    let replica = api_replica_on(Arc::clone(&told), &ledger).await;
    let sandbox = Arc::clone(&replica)
        .create_sandbox(cluster_create_request())
        .await
        .expect("create on the node");
    assert!(
        told.is_bound(sandbox.id),
        "the create left the cluster unable to say where the sandbox is"
    );
    // The machine stops holding it before any heartbeat could say so.
    Arc::clone(&on_the_node)
        .delete_sandbox(sandbox.id)
        .await
        .expect("the node tears its own sandbox down");
    assert!(
        replica.forget_sandbox_handle_for_test(&sandbox.id).await,
        "the replica held no handle to forget, so this face proves nothing"
    );
    Arc::clone(&replica)
        .delete_sandbox(sandbox.id)
        .await
        .expect("🔴 a sandbox the machine lost before its first heartbeat could not be deleted");
    assert!(
        ledger
            .0
            .get(&sandbox.id)
            .await
            .expect("read the ledger")
            .is_none(),
        "a completed delete left the record behind"
    );
    assert!(
        on_the_node
            .get_sandbox(&sandbox.id)
            .await
            .expect("the node's own record")
            .is_none(),
        "the delete answered success without telling the machine"
    );

    // Face 2: a cluster that cannot record refuses to start a runtime at all, so
    // the shape the old face guarded — a live sandbox nothing in the cluster names
    // — has no way to come into being. This is the half that licenses face 3.
    let unrecordable = ClusterPlacement::refusing_reservations(node.endpoint.clone());
    let replica = api_replica_on(Arc::clone(&unrecordable), &ledger).await;
    let refused = Arc::clone(&replica)
        .create_sandbox(cluster_create_request())
        .await
        .expect_err("a runtime must not be started that the cluster has no record of");
    assert!(
        format!("{refused:#}").contains("reserve a routing record"),
        "the refusal did not name the reservation: {refused:#}"
    );
    assert_eq!(
        on_the_node
            .list_sandboxes()
            .await
            .expect("the node's own records")
            .len(),
        0,
        "the node is running a sandbox the cluster refused to record"
    );

    // Face 3: the orphan the old face refused to reap. Nothing names this sandbox
    // — no binding, no reservation, no registry row — and the lookup is warm, so
    // that is a verdict and the delete must clear the record rather than 500.
    let told2 = ClusterPlacement::recording(node.endpoint.clone());
    let replica = api_replica_on(Arc::clone(&told2), &ledger).await;
    let orphan = Arc::clone(&replica)
        .create_sandbox(cluster_create_request())
        .await
        .expect("create on the node");
    Arc::clone(&on_the_node)
        .delete_sandbox(orphan.id)
        .await
        .expect("the node tears its own sandbox down");
    assert!(
        replica.forget_sandbox_handle_for_test(&orphan.id).await,
        "the replica held no handle to forget, so this face proves nothing"
    );
    // What a machine that lost the sandbox, plus an expired reservation, leaves
    // behind.
    told2.forget(orphan.id);
    Arc::clone(&replica)
        .delete_sandbox(orphan.id)
        .await
        .expect("an orphan nothing in a warm cluster names must be reapable");
    assert!(
        ledger
            .0
            .get(&orphan.id)
            .await
            .expect("read the ledger")
            .is_none(),
        "the orphan's record survived its own delete, so it keeps answering GET"
    );

    // Face 4: the other side of the same verdict. A reservation is in flight for
    // this sandbox — another replica is starting it — and the delete must not
    // read that as the absence face 3 reaps on.
    let racing = ClusterPlacement::recording(node.endpoint.clone());
    let replica = api_replica_on(Arc::clone(&racing), &ledger).await;
    let contested = Arc::clone(&replica)
        .create_sandbox(cluster_create_request())
        .await
        .expect("create on the node");
    assert!(
        replica.forget_sandbox_handle_for_test(&contested.id).await,
        "the replica held no handle to forget, so this face proves nothing"
    );
    racing.forget(contested.id);
    racing.reserve_elsewhere(contested.id);
    Arc::clone(&replica)
        .delete_sandbox(contested.id)
        .await
        .expect_err("a delete reaped a sandbox another replica was in the middle of starting");
    assert!(
        ledger
            .0
            .get(&contested.id)
            .await
            .expect("read the ledger")
            .is_some(),
        "the refused delete forgot the sandbox anyway"
    );

    // Face 5: one heartbeat later the node has acknowledged that same start, which
    // promotes the reservation. The same delete on the same sandbox through the
    // same replica now goes through — which is what says face 4 refused over the
    // reservation and not over the sandbox.
    racing.heartbeat(&[contested.id]);
    Arc::clone(&replica)
        .delete_sandbox(contested.id)
        .await
        .expect("the node acknowledged the start and the delete still could not reach it");
    assert!(
        on_the_node
            .get_sandbox(&contested.id)
            .await
            .expect("the node's own record")
            .is_none(),
        "the delete answered success without telling the machine"
    );
}

// The counterfactual face 3 rests on: reaping on a warm absence is safe only
// because a create reserves first. A build that skips the reservation reaches the
// same absence with a runtime still on the machine, and reaps it.
#[tokio::test]
async fn without_the_create_time_reservation_the_delete_verdict_reaps_a_live_sandbox() {
    let node = real_node().await;
    let on_the_node = node.orchestration.as_ref().expect("a real node").clone();
    let ledger = SharedLedger(Arc::new(InMemoryMetadataStore::new()));

    let no_reservations = ClusterPlacement::never_reserving(node.endpoint.clone());
    let replica = api_replica_on(Arc::clone(&no_reservations), &ledger).await;
    let sandbox = Arc::clone(&replica)
        .create_sandbox(cluster_create_request())
        .await
        .expect("a build without reservations still creates");
    assert!(
        replica.forget_sandbox_handle_for_test(&sandbox.id).await,
        "the replica held no handle to forget, so this face proves nothing"
    );
    // The window a reservation exists to cover: the cluster's record of this
    // sandbox is gone while the machine still holds it.
    no_reservations.forget(sandbox.id);

    Arc::clone(&replica)
        .delete_sandbox(sandbox.id)
        .await
        .expect("the warm absence reads as a verdict");
    assert!(
        ledger
            .0
            .get(&sandbox.id)
            .await
            .expect("read the ledger")
            .is_none(),
        "control: this face only means something if the delete actually reaped the record"
    );
    assert!(
        on_the_node
            .get_sandbox(&sandbox.id)
            .await
            .expect("the node's own record")
            .is_some(),
        "the machine was told after all, which would make the reservation unnecessary"
    );
}

#[tokio::test]
async fn a_delete_forgets_a_sandbox_only_once_its_node_has_left_the_cluster() {
    let node = real_node().await;
    let endpoint = node.endpoint.endpoint.clone();
    let placement = ClusterPlacement::recording(node.endpoint.clone());
    let ledger = SharedLedger(Arc::new(InMemoryMetadataStore::new()));
    let started_here = api_replica_on(Arc::clone(&placement), &ledger).await;
    let landed_elsewhere = api_replica_on(Arc::clone(&placement), &ledger).await;

    let still_listed = Arc::clone(&started_here)
        .create_sandbox(cluster_create_request())
        .await
        .expect("create on the node");
    let evicted = Arc::clone(&started_here)
        .create_sandbox(cluster_create_request())
        .await
        .expect("create on the node");
    let unanswerable = Arc::clone(&started_here)
        .create_sandbox(cluster_create_request())
        .await
        .expect("create on the node");

    // The node is gone for the rest of this test. Every dial from here on
    // fails identically; what differs across the three faces is only what the
    // placement source says about the node's own membership.
    drop(node);
    wait_until_unreachable(&endpoint).await;

    // Face 1: still listed. Refuse, and keep the record.
    placement.set_membership(MembershipAnswer::Present);
    let err = Arc::clone(&landed_elsewhere)
        .delete_sandbox(still_listed.id)
        .await
        .expect_err(
            "🔴 a node the registry still lists must not be forgotten over one failed dial",
        );
    assert!(
        format!("{err}").contains("could not be reached"),
        "the refusal did not say why: {err}"
    );
    assert_eq!(
        ledger
            .0
            .get(&still_listed.id)
            .await
            .expect("read the ledger")
            .expect("🔴 a still-listed node's sandbox was forgotten anyway")
            .state,
        crate::orchestrator::SandboxState::Running,
        "a refused delete left the record somewhere other than where it found it"
    );

    // Face 2: evicted from the registry entirely. Forget it.
    placement.set_membership(MembershipAnswer::Gone);
    Arc::clone(&landed_elsewhere)
        .delete_sandbox(evicted.id)
        .await
        .expect("🔴 a sandbox on a node confirmed gone from the cluster could not be forgotten");
    assert!(
        ledger
            .0
            .get(&evicted.id)
            .await
            .expect("read the ledger")
            .is_none(),
        "a node confirmed gone from the cluster left its sandbox's record behind"
    );

    // Face 3: the membership question itself could not be answered. Land
    // exactly on face 1 — refuse, and keep the record.
    placement.set_membership(MembershipAnswer::Unavailable);
    let err = Arc::clone(&landed_elsewhere)
        .delete_sandbox(unanswerable.id)
        .await
        .expect_err(
            "🔴 a delete must not forget a sandbox it could not confirm was gone from the cluster",
        );
    assert!(
        format!("{err}").contains("could not be reached"),
        "the refusal did not say why: {err}"
    );
    assert_eq!(
        ledger
            .0
            .get(&unanswerable.id)
            .await
            .expect("read the ledger")
            .expect("🔴 an unanswerable membership check forgot the sandbox anyway")
            .state,
        crate::orchestrator::SandboxState::Running,
    );
}
