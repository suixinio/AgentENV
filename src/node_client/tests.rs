//! Driving a sandbox on "another machine" — a node service in this process,
//! reached over a real socket.
//!
//! 🔴 Over a socket rather than by calling the trait directly. Everything this
//! module is for lives in the gap between an in-process handle and a wire: a
//! reply that lost a field, a node that answered about the wrong run, a call
//! that never came back. None of those exist if the two halves are the same
//! object.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tokio::sync::oneshot;
use tonic::{Request, Response, Status};

use crate::orchestrator::{
    DisabledSandboxPersister, InMemoryMetadataStore, Orchestrator, SandboxOrchestration,
};
use crate::proto::node as pb;
use crate::proto::node::node_sandbox_service_server::{
    NodeSandboxService, NodeSandboxServiceServer,
};
use crate::role::ServerRole;
use crate::sandbox::mock::MockBackendFactory;
use crate::sandbox::{SandboxBackend, SandboxBackendFactory, SandboxForkSpec, SandboxLaunchConfig};
use crate::snapshot::mock::MockSnapshotArtifactStore;
use crate::snapshot::repository::{
    RepositoryResult, SnapshotCatalog, SnapshotCommit, SnapshotListFilter, SnapshotRepository,
    SnapshotRuntimeResolver, StagedSnapshot, StartedBuild,
};
use crate::snapshot::{
    CommittedSnapshot, RunnableSnapshot, SnapshotId, SnapshotManager, SnapshotRecord,
};
use crate::types::ExecutionId;

use super::factory::RemoteSandboxBackendFactory;
use super::paused_state::RemotePausedState;
use super::placement::{FixedNodePlacement, NodeEndpoint};
use super::wire;

// ---------------------------------------------------------------------------
// A node in this process, on a real socket
// ---------------------------------------------------------------------------

struct RunningNode {
    endpoint: NodeEndpoint,
    _shutdown: oneshot::Sender<()>,
    orchestration: Option<Arc<dyn SandboxOrchestration>>,
}

impl RunningNode {
    fn placement(&self) -> Arc<FixedNodePlacement> {
        Arc::new(FixedNodePlacement::new(self.endpoint.clone()))
    }
}

async fn serve<S>(service: S, orchestration: Option<Arc<dyn SandboxOrchestration>>) -> RunningNode
where
    S: NodeSandboxService,
{
    // 🔴 Bound here and handed over, rather than bound-probed-and-released.
    // Two reasons, and the second is why it is worth the extra line: there is
    // no window in which another test in this binary can take the port, and
    // the listener is accepting before this function returns — so the loop
    // that used to wait for the bind, and could time out while a test looked
    // like a wire failure, is gone.
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

    // 🔴 Still waited for, and for a narrower reason than before: the socket is
    // bound, but the accept loop is in a task that may not have been polled.
    // A connect that raced it comes back as "connection refused", which is
    // indistinguishable from the failure half of these tests are about.
    for _ in 0..200 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    RunningNode {
        endpoint: NodeEndpoint {
            node_id: "node-under-test".to_string(),
            endpoint: format!("http://{addr}"),
        },
        _shutdown: tx,
        orchestration,
    }
}

/// A node backed by the real service, over a real orchestrator with mock
/// sandboxes.
async fn real_node() -> RunningNode {
    crate::logging::init_for_tests();
    let orchestrator = Orchestrator::new(
        ServerRole::All,
        InMemoryMetadataStore::new(),
        MockBackendFactory::new(),
        DisabledSandboxPersister,
    )
    .await
    .expect("an in-memory orchestrator");
    let orchestration: Arc<dyn SandboxOrchestration> = orchestrator;
    let service = crate::node_server::NodeSandboxService::new(
        Arc::clone(&orchestration),
        Arc::new(resolvable_snapshot_manager()),
        "node-under-test".to_string(),
    );
    serve(service, Some(orchestration)).await
}

/// A catalog and resolver that answer with one mock snapshot, so a create can
/// get as far as starting a sandbox.
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
        async fn list(&self, _filter: SnapshotListFilter) -> RepositoryResult<Vec<SnapshotRecord>> {
            Ok(Vec::new())
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
            Arc::new(MockSnapshotArtifactStore),
        )),
        Arc::new(AlwaysRunnable),
        None,
    )
}

// ---------------------------------------------------------------------------
// A node that answers whatever a test told it to
// ---------------------------------------------------------------------------

#[derive(Default)]
struct ScriptedNode {
    create: Mutex<Option<Result<pb::SandboxCreateResponse, Status>>>,
    pause: Mutex<Option<Result<pb::SandboxPauseResponse, Status>>>,
    checkpoint: Mutex<Option<Result<pb::SandboxCheckpointResponse, Status>>>,
    fork: Mutex<Option<Result<pb::SandboxForkResponse, Status>>>,
    delete: Mutex<Option<Result<pb::SandboxDeleteResponse, Status>>>,
    resume: Mutex<Option<Result<pb::SandboxResumeResponse, Status>>>,
    seen_create: Mutex<Vec<pb::SandboxCreateRequest>>,
    seen_delete: Mutex<Vec<pb::SandboxDeleteRequest>>,
    seen_resume: Mutex<Vec<pb::SandboxResumeRequest>>,
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

#[tonic::async_trait]
impl NodeSandboxService for Arc<ScriptedNode> {
    async fn create(
        &self,
        request: Request<pb::SandboxCreateRequest>,
    ) -> Result<Response<pb::SandboxCreateResponse>, Status> {
        self.seen_create
            .lock()
            .expect("lock")
            .push(request.into_inner());
        ScriptedNode::take(&self.create, "create")
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
        _request: Request<pb::SandboxPauseRequest>,
    ) -> Result<Response<pb::SandboxPauseResponse>, Status> {
        ScriptedNode::take(&self.pause, "pause")
    }

    async fn checkpoint(
        &self,
        _request: Request<pb::SandboxCheckpointRequest>,
    ) -> Result<Response<pb::SandboxCheckpointResponse>, Status> {
        ScriptedNode::take(&self.checkpoint, "checkpoint")
    }

    async fn resume(
        &self,
        request: Request<pb::SandboxResumeRequest>,
    ) -> Result<Response<pb::SandboxResumeResponse>, Status> {
        self.seen_resume
            .lock()
            .expect("lock")
            .push(request.into_inner());
        ScriptedNode::take(&self.resume, "resume")
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
        _request: Request<pb::SandboxParamsRequest>,
    ) -> Result<Response<pb::SandboxParamsResponse>, Status> {
        Ok(Response::new(pb::SandboxParamsResponse {}))
    }

    async fn list_sandboxes(
        &self,
        _request: Request<pb::ListSandboxesRequest>,
    ) -> Result<Response<pb::SandboxListResponse>, Status> {
        Ok(Response::new(pb::SandboxListResponse {
            sandboxes: Vec::new(),
        }))
    }
}

async fn scripted_node() -> (Arc<ScriptedNode>, RunningNode) {
    crate::logging::init_for_tests();
    let node = Arc::new(ScriptedNode::default());
    let running = serve(Arc::clone(&node), None).await;
    (node, running)
}

fn launch_config() -> SandboxLaunchConfig {
    SandboxLaunchConfig {
        snapshot_id: "snapshot".to_string(),
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// A sandbox built here, started there, and visible in that node's own listing
/// of what it is running.
///
/// 🔴 End to end over a socket: factory → stub → gRPC → the real node service →
/// an orchestrator with mock sandboxes.
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

    // 🔴 Nothing has happened yet. `build` is synchronous and may not touch a
    // network, so a stub that had already placed the sandbox would mean the
    // seam does not actually hold.
    assert!(backend.host_interaction_ip().is_none());

    backend.start().await.expect("start on the node");
    assert_eq!(backend.execution_id(), execution_id);

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

/// 🔴 A node that answers about a different run fails the start, and the
/// sandbox it did start is torn down.
///
/// The caller has already written the incarnation into its own record, so
/// adopting the node's would leave two records of one sandbox naming two
/// different runs — and fencing compares exactly that value.
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

/// The create carries the incarnation the factory was built for.
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
    // 🔴 The factory attaches no ownership marker: it is a mechanism the
    // control plane drives, not the thing that holds the control plane's
    // record.
    assert!(creates[0].control_plane_config.is_empty());

    assert_eq!(
        backend.host_interaction_ip(),
        Some(std::net::Ipv4Addr::new(10, 1, 2, 3))
    );
    assert_eq!(backend.runtime_info().rootfs_virtual_size, Some(8192));
    // See the table at the top of `stub.rs`: the deciding half pins no local
    // artifacts, because it has none.
    assert!(backend.runtime_info().runtime_artifacts.is_empty());
    assert!(backend.startup_artifacts().is_empty());
}

/// 🔴 A node that cannot be reached is an error, never "the sandbox is gone".
///
/// `stop` is where the temptation is strongest — both endings have nothing left
/// to do — and taking the first for the second is how a cluster stops
/// accounting for a VM that is still running.
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

    // 🔴 The control probe: a node that says it does not have the sandbox *is*
    // an answer, and it makes `stop` idempotent as the trait requires.
    *script.delete.lock().expect("lock") = Some(Err(Status::not_found("no such sandbox here")));
    backend
        .stop()
        .await
        .expect("a node that says the sandbox is not there has answered");
}

/// A pause comes back as a reference to bytes on the node, not as a handle.
#[tokio::test]
async fn a_pause_comes_back_as_a_reference_to_the_nodes_bytes() {
    let (script, node) = scripted_node().await;
    let execution_id = ExecutionId::new();
    *script.create.lock().expect("lock") = Some(Ok(pb::SandboxCreateResponse {
        sandbox_id: launch_config().sandbox_id.to_string(),
        execution_id: execution_id.to_string(),
        ..Default::default()
    }));
    *script.pause.lock().expect("lock") = Some(Ok(pb::SandboxPauseResponse {
        paused_state: Some(pb::PausedState {
            artifact_root: "/var/lib/agentenv/paused/7".to_string(),
            state: Some(
                wire::serialize(&serde_json::json!({"memory": "mem.json"}), "state")
                    .expect("encode"),
            ),
        }),
        staged: None,
    }));

    let factory = RemoteSandboxBackendFactory::new(node.placement());
    let mut backend =
        match factory.build_from_snapshot(&RunnableSnapshot::mock(), launch_config(), execution_id)
        {
            Ok(backend) => backend,
            Err(err) => panic!("building a stub should not fail: {err:#}"),
        };
    backend.start().await.expect("start");

    let capture = backend.pause(None).await.expect("pause");
    // 🔴 Always absent, and not because the node had nothing: by the time this
    // reply exists the bytes are already durable there, so what a caller needs
    // is the staged row and not a handle owning a directory on another disk.
    assert!(capture.publishable.is_none());

    let encoded = capture.state.encode().expect("encode");
    assert_eq!(encoded["origin_node_id"], "node-under-test");
    assert_eq!(encoded["artifact_root"], "/var/lib/agentenv/paused/7");
    assert_eq!(encoded["state"]["memory"], "mem.json");
    assert!(capture.state.runtime_artifacts().is_empty());

    // And it survives the round trip a record makes it take.
    let restored = factory
        .decode_paused_state(std::path::PathBuf::from("/ignored"), encoded)
        .expect("decode");
    assert_eq!(
        restored.encode().expect("re-encode")["origin_node_id"],
        "node-under-test"
    );
}

/// A pause the node reported as successful but answered without state is
/// terminal.
///
/// 🔴 Terminal rather than recoverable: a successful pause means the VM is
/// already stopped, so a reply with nothing to reopen it is a sandbox that is
/// down and cannot come back. Calling that recoverable would have the caller
/// mark it running again.
#[tokio::test]
async fn a_pause_with_nothing_to_reopen_is_terminal() {
    let (script, node) = scripted_node().await;
    let execution_id = ExecutionId::new();
    *script.create.lock().expect("lock") = Some(Ok(pb::SandboxCreateResponse {
        sandbox_id: launch_config().sandbox_id.to_string(),
        execution_id: execution_id.to_string(),
        ..Default::default()
    }));
    *script.pause.lock().expect("lock") = Some(Ok(pb::SandboxPauseResponse {
        paused_state: None,
        staged: None,
    }));

    let factory = RemoteSandboxBackendFactory::new(node.placement());
    let mut backend =
        match factory.build_from_snapshot(&RunnableSnapshot::mock(), launch_config(), execution_id)
        {
            Ok(backend) => backend,
            Err(err) => panic!("building a stub should not fail: {err:#}"),
        };
    backend.start().await.expect("start");

    let err = match backend.pause(None).await {
        Err(err) => err,
        Ok(_) => panic!("a pause with no state to reopen must not look like a success"),
    };
    assert!(err.is_terminal(), "{err}");
}

/// A capture failure keeps the classification the node gave it, and gains a
/// terminal one when it gave none.
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

    // 🔴 And the direction that matters: a failure with no classification is
    // read as terminal, because "I do not know whether the runtime was mutated"
    // and "it was not" are not the same answer.
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
    assert!(backend
        .snapshot()
        .await
        .expect_err("the node refused")
        .is_terminal());
}

/// A checkpoint comes back as the staged snapshot itself, ready to commit.
#[tokio::test]
async fn a_checkpoint_comes_back_as_a_row_that_has_not_been_announced() {
    let (script, node) = scripted_node().await;
    let execution_id = ExecutionId::new();
    *script.create.lock().expect("lock") = Some(Ok(pb::SandboxCreateResponse {
        sandbox_id: launch_config().sandbox_id.to_string(),
        execution_id: execution_id.to_string(),
        ..Default::default()
    }));

    let staged = StagedSnapshot {
        commit: SnapshotCommit {
            id: SnapshotId::generate(),
            alias: None,
            source: crate::snapshot::SnapshotPublishSource::Template,
            resources: Default::default(),
            created_at_unix_ms: Some(1_700_000_000_000),
            committed: CommittedSnapshot::mock(),
        },
        staged_at_unix_ms: 1_700_000_000_000,
        origin_node_id: "node-under-test".to_string(),
        execution_id: Some(execution_id),
    };
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
    let decoded: StagedSnapshot = captured
        .downcast()
        .expect("the capture carries a staged snapshot and not something else");
    assert_eq!(decoded.origin_node_id, "node-under-test");
    assert_eq!(decoded.execution_id, Some(execution_id));
    assert_eq!(decoded.id(), staged.id());
}

/// A fork answered with the wrong number of results is a terminal failure.
///
/// 🔴 The caller pairs results with the children it asked for by position and
/// writes a record for each; a short list would give some child another child's
/// record.
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

/// A fork keeps each child's outcome with the child it belongs to, failures
/// included.
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

/// 🔴 A cold create is refused here rather than approximated.
///
/// `build` is handed a spec whose image config and drives have already been
/// resolved into paths on the local disk, and the reference a node would need
/// to resolve them itself is no longer in it.
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

/// A paused state with no machine attached to it is refused.
///
/// 🔴 The path inside is on one particular disk. A record that carried it and
/// not the machine would be a resume sent wherever placement happened to
/// point, failing there in a way that looks like the bytes are corrupt.
#[tokio::test]
async fn a_paused_state_without_a_node_is_refused() {
    let (_script, node) = scripted_node().await;
    let factory = RemoteSandboxBackendFactory::new(node.placement());

    let err = match factory.decode_paused_state(
        std::path::PathBuf::from("/ignored"),
        serde_json::json!({"artifact_root": "/var/lib/x", "state": {}}),
    ) {
        Err(err) => err,
        Ok(_) => panic!("a paused state with no machine on it was accepted"),
    };
    assert!(err.to_string().contains("which node"), "{err:#}");

    // The control probe: the same value with the machine on it decodes.
    factory
        .decode_paused_state(
            std::path::PathBuf::from("/ignored"),
            serde_json::json!({
                "origin_node_id": "node-a",
                "artifact_root": "/var/lib/x",
                "execution_id": ExecutionId::new().to_string(),
                "state": {},
            }),
        )
        .expect("a paused state that says where its bytes are");
}

/// 🔴 The blank ownership marker and the api role that cannot start are one
/// fact, and must stop being true together.
///
/// `RemoteSandboxBackendFactory` sends `control_plane_config: Vec::new()` on
/// every create. A grep for who sets the marker therefore comes back with tests
/// and nothing else, which reads exactly like a defect — and would be one, the
/// moment anything drove this factory for real: every sandbox it created would
/// be one the control plane does not recognise as its own, invisible to
/// `ListSandboxes` and to the reconciliation that runs off it.
///
/// It is not one today for a reason that lives in a different file: `--role
/// api` refuses to assemble, so nothing constructs this factory outside these
/// tests. That reason is load-bearing and nothing else records it, so it is
/// asserted here rather than left to a reader to reconstruct.
#[test]
fn the_blank_ownership_marker_outlives_only_an_api_role_that_cannot_start() {
    const UNASSEMBLABLE: &str = "--role api cannot be assembled yet";
    const BLANK_MARKER: &str = "control_plane_config: Vec::new()";

    let factory = include_str!("factory.rs");
    let server = include_str!("../bin/server.rs");

    if server.contains(UNASSEMBLABLE) {
        // 🔴 Resolution. Without this half the test is a scan for a string that
        // may no longer exist, and a scan that finds nothing passes forever —
        // including on the tree where the marker had just been wired up and
        // this note had gone stale.
        assert!(
            factory.contains(BLANK_MARKER),
            "the remote factory no longer sends a blank ownership marker. That is the fix this \
             test is waiting for; delete it, or update {BLANK_MARKER:?} to whatever replaced it"
        );
        return;
    }

    assert!(
        !factory.contains(BLANK_MARKER),
        "--role api can be assembled, and the remote factory still sends an empty ownership \
         marker on every create. Every sandbox the api half started would be one the control \
         plane does not recognise as its own: absent from ListSandboxes, and absent from the \
         reconciliation that decides which bindings are still live. The marker is per sandbox — \
         it is the control plane's record of that sandbox — so it has to arrive from the caller \
         that decides what that record is, not from a constant here"
    );
}

/// The marker the orchestrator put on the launch config is the marker the node
/// stores and hands back.
///
/// 🔴 End to end over a socket, and over the *real* node service, because every
/// place this could be lost is between the two: the factory could drop it, the
/// proto could carry it in a field nothing reads, the node could parse it
/// instead of storing it. A test that called the trait directly would prove
/// none of that.
#[tokio::test]
async fn the_marker_the_orchestrator_stamped_is_the_marker_on_the_wire() {
    let node = real_node().await;
    let factory = RemoteSandboxBackendFactory::new(node.placement());

    // 🔴 The factory answers `true` to this, which is what makes the
    // orchestrator above it fill the field the rest of this test follows. If
    // it ever answered `false`, every assertion below would still hold — on a
    // launch config a test wrote by hand — while production sent nothing.
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
        crate::node_server::owned_by_control_plane(&live).len(),
        1,
        "a sandbox the control plane created must be one the control plane recognises"
    );
}

/// 🔴 The control probe, and the one that says what an unmarked sandbox is
/// *for*: it is left alone, not killed.
///
/// A create that reached a node without a marker is a sandbox no control plane
/// claims. The direction that costs nothing is to leave it out of the listing
/// the reconciliation reads; the direction that ends a user's session is to
/// treat it as an orphan. Same node, same service, same call as the test above
/// — one field different.
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

    // 🔴 The sandbox is running. Without this the two emptiness assertions
    // below would be satisfied by a create that never happened.
    assert_eq!(live.len(), 1);
    assert_eq!(live[0].sandbox_id, sandbox_id);
    assert!(
        live[0].control_plane_config.is_none(),
        "an empty marker on the wire must not become a marker on the node"
    );
    assert!(
        crate::node_server::owned_by_control_plane(&live).is_empty(),
        "a sandbox nobody claimed must not be offered up for reconciliation"
    );
}

/// The node service, stood up the way a binary stands it up.
///
/// 🔴 Through `node_server::serve_on` rather than through this file's own
/// `serve` helper, because that is the function `assemble_node` calls and it
/// is the one nothing had ever run: the harness above builds its own tonic
/// server so it can serve scripted services, so it proves the *service*
/// works and says nothing about the entry point. This also exercises the
/// shutdown channel, which is what `main` sends on SIGTERM.
#[tokio::test]
async fn the_node_service_answers_through_the_entry_point_a_binary_uses() {
    crate::logging::init_for_tests();
    let orchestrator = Orchestrator::new(
        ServerRole::All,
        InMemoryMetadataStore::new(),
        MockBackendFactory::new(),
        DisabledSandboxPersister,
    )
    .await
    .expect("an in-memory orchestrator");
    let orchestration: Arc<dyn SandboxOrchestration> = orchestrator;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a port");
    let addr = listener.local_addr().expect("the bound address");
    let (stop, stopped) = oneshot::channel::<()>();

    let served = Arc::clone(&orchestration);
    let serving = tokio::spawn(async move {
        crate::node_server::serve_on(
            listener,
            served,
            Arc::new(resolvable_snapshot_manager()),
            "node-under-test".to_string(),
            async {
                let _ = stopped.await;
            },
        )
        .await
    });

    let placement = Arc::new(FixedNodePlacement::new(NodeEndpoint {
        node_id: "node-under-test".to_string(),
        endpoint: format!("http://{addr}"),
    }));
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

    // 🔴 And it stops when told. `main` sends on this channel from the
    // graceful-shutdown closure; a surface that ignored it would keep the port
    // bound and hold the process open past the Pod's termination grace.
    drop(stop);
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(10), serving)
        .await
        .expect("the surface stops when the shutdown signal fires")
        .expect("the serving task did not panic");
    assert!(outcome.is_ok(), "{outcome:?}");
    assert!(
        tokio::net::TcpStream::connect(addr).await.is_err(),
        "the port is still bound after the surface was told to stop"
    );
}

// ---------------------------------------------------------------------------
// Reopening a capture the node is holding
// ---------------------------------------------------------------------------

/// A paused sandbox comes back on the machine whose disk its capture is on, and
/// under the run the resume claimed.
///
/// 🔴 End to end over a socket and over the *real* node service, because every
/// place this could go wrong is between the two halves: the factory could send
/// the wrong incarnation, the proto could carry the fence in a field nothing
/// reads, the node could mint its own run. A test that called the trait
/// directly would prove none of it.
///
/// The refusal comes first and the success second, on purpose. The refusal's
/// evidence is that the node is running nothing — and "running nothing" is what
/// a node that never started anything also looks like, so the second half is
/// what gives the first half its resolution.
#[tokio::test]
async fn a_paused_sandbox_is_reopened_on_the_machine_that_holds_its_capture() {
    let node = real_node().await;
    let orchestration = node.orchestration.as_ref().expect("a real node").clone();
    let factory = RemoteSandboxBackendFactory::new(node.placement());

    let paused_execution_id = ExecutionId::new();
    let config = launch_config();
    let sandbox_id = config.sandbox_id;
    let mut backend = factory
        .build_from_snapshot(&RunnableSnapshot::mock(), config, paused_execution_id)
        .expect("build a stub");
    backend.start().await.expect("start on the node");

    // The node pauses it through its own orchestrator, which is what the
    // `Pause` RPC will drive once it is served: what matters here is that the
    // capture and the record end up on the node.
    Arc::clone(&orchestration)
        .pause_sandbox(sandbox_id)
        .await
        .expect("the node pauses its own sandbox");
    assert!(
        orchestration
            .list_live_sandboxes()
            .await
            .expect("list")
            .is_empty(),
        "a paused sandbox is still running"
    );

    let resumed_execution_id = ExecutionId::new();
    let capture_on = |node_id: &str| {
        RemotePausedState::new(
            node_id.to_string(),
            "/var/lib/agentenv/paused/7".to_string(),
            paused_execution_id,
            serde_json::json!({}),
        )
    };

    // 🔴 A capture the placement answer does not match is refused here rather
    // than sent. Sent, it would come back `NotFound` from a machine that has
    // simply never seen this sandbox — which reads exactly like "the only copy
    // is gone".
    let elsewhere = capture_on("node-that-holds-nothing");
    let mut wrong = factory
        .build_from_paused_state(sandbox_id, resumed_execution_id, &elsewhere, None)
        .expect("build a stub");
    let err = wrong
        .start()
        .await
        .expect_err("a capture on another machine was reopened here");
    assert!(
        format!("{err:#}").contains("node-that-holds-nothing"),
        "the refusal did not say where the capture actually is: {err:#}"
    );
    assert!(
        orchestration
            .list_live_sandboxes()
            .await
            .expect("list")
            .is_empty(),
        "a refused resume started something anyway"
    );

    // 🔴 The same call, one value different: the machine the capture is on.
    let here = capture_on("node-under-test");
    let mut backend = factory
        .build_from_paused_state(sandbox_id, resumed_execution_id, &here, None)
        .expect("build a stub");
    backend.start().await.expect("reopen on the node");
    assert_eq!(backend.execution_id(), resumed_execution_id);

    let live = orchestration.list_live_sandboxes().await.expect("list");
    assert_eq!(live.len(), 1, "the sandbox did not come back: {live:?}");
    assert_eq!(live[0].sandbox_id, sandbox_id);
    assert_eq!(
        live[0].execution_id,
        Some(resumed_execution_id),
        "the node brought the sandbox back under a run nobody claimed"
    );
    assert_ne!(
        live[0].execution_id,
        Some(paused_execution_id),
        "the node reopened the capture as the run it was paused under"
    );
}

/// The resume says which run it is reopening and which run it is starting, in
/// that order, in those fields.
///
/// 🔴 A test rather than a reading of the proto. Field names are not covered by
/// this project's mutation testing — a `.proto` is generated code by the time
/// anything mutates — so a swap of these two would type-check, compile, and
/// resume a sandbox under the run it had just been paused under while fencing
/// against the run that has not happened yet. The control face is the swap
/// itself: the two values are different, and each is asserted absent from the
/// other's field.
#[tokio::test]
async fn the_resume_names_the_run_it_reopens_and_the_run_it_starts() {
    let (script, node) = scripted_node().await;
    let paused_execution_id = ExecutionId::new();
    let resumed_execution_id = ExecutionId::new();
    assert_ne!(paused_execution_id, resumed_execution_id);
    let sandbox_id = launch_config().sandbox_id;

    *script.resume.lock().expect("lock") = Some(Ok(pb::SandboxResumeResponse {
        started: Some(pb::SandboxCreateResponse {
            sandbox_id: sandbox_id.to_string(),
            execution_id: resumed_execution_id.to_string(),
            host_interaction_ip: "10.4.5.6".to_string(),
            rootfs_virtual_size: 4096,
            resources: Some(pb::SandboxResources {
                cpu_count: 4,
                memory_mib: 2048,
                disk_size_mib: 10240,
            }),
            ..Default::default()
        }),
    }));

    let factory = RemoteSandboxBackendFactory::new(node.placement());
    let state = RemotePausedState::new(
        "node-under-test".to_string(),
        "/var/lib/agentenv/paused/7".to_string(),
        paused_execution_id,
        serde_json::json!({}),
    );
    let mut backend = factory
        .build_from_paused_state(sandbox_id, resumed_execution_id, &state, None)
        .expect("build a stub");
    backend.start().await.expect("reopen");

    let seen = script.seen_resume.lock().expect("lock").clone();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].sandbox_id, sandbox_id.to_string());
    assert_eq!(
        seen[0].execution_id,
        paused_execution_id.to_string(),
        "the fence named a run other than the one the capture is of"
    );
    assert_eq!(
        seen[0].resumed_execution_id,
        resumed_execution_id.to_string(),
        "the node was told to start a run other than the one the claim allocated"
    );
    // 🔴 The swap, ruled out from both directions.
    assert_ne!(seen[0].execution_id, resumed_execution_id.to_string());
    assert_ne!(
        seen[0].resumed_execution_id,
        paused_execution_id.to_string()
    );
    // 🔴 Zero, meaning "keep what it was paused with". A deadline is the
    // orchestrator's to decide and it does not reach a backend.
    assert_eq!(seen[0].timeout_ms, 0);

    // And what the node reported is what this half now holds.
    assert_eq!(
        backend.host_interaction_ip(),
        Some(std::net::Ipv4Addr::new(10, 4, 5, 6))
    );
    assert_eq!(backend.runtime_info().rootfs_virtual_size, Some(4096));
}

/// 🔴 A node that could not be reached is not a capture that is gone.
///
/// One call shape, one value different — what the node answered — and the two
/// answers license opposite moves: `NotFound` says the only copy of this
/// sandbox is not on that machine, which is grounds for rebuilding it from a
/// published snapshot or giving it up, and `Unavailable` says a machine is
/// down, which is grounds for nothing at all.
#[tokio::test]
async fn a_node_that_could_not_be_reached_is_not_a_capture_that_is_gone() {
    let paused_execution_id = ExecutionId::new();
    let resumed_execution_id = ExecutionId::new();
    let sandbox_id = launch_config().sandbox_id;

    async fn attempt(
        status: Status,
        sandbox_id: crate::types::SandboxId,
        paused_execution_id: ExecutionId,
        resumed_execution_id: ExecutionId,
    ) -> anyhow::Error {
        let (script, node) = scripted_node().await;
        *script.resume.lock().expect("lock") = Some(Err(status));
        let factory = RemoteSandboxBackendFactory::new(node.placement());
        let state = RemotePausedState::new(
            "node-under-test".to_string(),
            "/var/lib/agentenv/paused/7".to_string(),
            paused_execution_id,
            serde_json::json!({}),
        );
        let mut backend = factory
            .build_from_paused_state(sandbox_id, resumed_execution_id, &state, None)
            .expect("build a stub");
        backend
            .start()
            .await
            .expect_err("the node refused the resume")
    }

    let absent = attempt(
        Status::not_found("no paused capture for that sandbox here"),
        sandbox_id,
        paused_execution_id,
        resumed_execution_id,
    )
    .await;
    assert!(
        matches!(
            absent.downcast_ref::<wire::RemoteResumeFailure>(),
            Some(wire::RemoteResumeFailure::CaptureAbsent { .. })
        ),
        "{absent:#}"
    );

    let unreachable = attempt(
        Status::unavailable("the node is restarting"),
        sandbox_id,
        paused_execution_id,
        resumed_execution_id,
    )
    .await;
    assert!(
        matches!(
            unreachable.downcast_ref::<wire::RemoteResumeFailure>(),
            Some(wire::RemoteResumeFailure::NodeUnreachable { .. })
        ),
        "an unreachable node was read as a capture that is gone: {unreachable:#}"
    );
}

/// A node that reported a resume and said nothing about the run it started is a
/// failure, not a success with fields missing.
///
/// 🔴 The sandbox is up over there either way. What this half loses is the
/// ability to fence anything it sends next, so accepting the reply would leave
/// it addressing a VM it cannot name.
#[tokio::test]
async fn a_resume_that_named_no_run_is_a_failure() {
    let (script, node) = scripted_node().await;
    let sandbox_id = launch_config().sandbox_id;
    let paused_execution_id = ExecutionId::new();
    let resumed_execution_id = ExecutionId::new();
    let state = RemotePausedState::new(
        "node-under-test".to_string(),
        "/var/lib/agentenv/paused/7".to_string(),
        paused_execution_id,
        serde_json::json!({}),
    );
    let factory = RemoteSandboxBackendFactory::new(node.placement());

    *script.resume.lock().expect("lock") = Some(Ok(pb::SandboxResumeResponse { started: None }));
    let mut backend = factory
        .build_from_paused_state(sandbox_id, resumed_execution_id, &state, None)
        .expect("build a stub");
    let err = backend
        .start()
        .await
        .expect_err("a reply with no run in it must not look like a success");
    assert!(format!("{err:#}").contains("said nothing"), "{err:#}");

    // 🔴 The control face: the same reply carrying the run does start.
    let (script, node) = scripted_node().await;
    *script.resume.lock().expect("lock") = Some(Ok(pb::SandboxResumeResponse {
        started: Some(pb::SandboxCreateResponse {
            sandbox_id: sandbox_id.to_string(),
            execution_id: resumed_execution_id.to_string(),
            ..Default::default()
        }),
    }));
    let factory = RemoteSandboxBackendFactory::new(node.placement());
    let mut backend = factory
        .build_from_paused_state(sandbox_id, resumed_execution_id, &state, None)
        .expect("build a stub");
    backend.start().await.expect("a reply that named the run");
}

/// A node that brought the sandbox back under some other run fails the start,
/// and — unlike a create — the sandbox is *not* torn down.
///
/// 🔴 The asymmetry is the point. A create that went wrong can be undone
/// because the sandbox did not exist before the call; this one did, its capture
/// has just been consumed by whatever the node started, and a teardown would
/// destroy the user's only copy of their work over a protocol disagreement.
#[tokio::test]
async fn a_node_that_reopened_another_run_fails_the_start_and_is_not_told_to_delete() {
    let (script, node) = scripted_node().await;
    let sandbox_id = launch_config().sandbox_id;
    let claimed = ExecutionId::new();
    let started = ExecutionId::new();
    *script.resume.lock().expect("lock") = Some(Ok(pb::SandboxResumeResponse {
        started: Some(pb::SandboxCreateResponse {
            sandbox_id: sandbox_id.to_string(),
            execution_id: started.to_string(),
            ..Default::default()
        }),
    }));

    let factory = RemoteSandboxBackendFactory::new(node.placement());
    let state = RemotePausedState::new(
        "node-under-test".to_string(),
        "/var/lib/agentenv/paused/7".to_string(),
        ExecutionId::new(),
        serde_json::json!({}),
    );
    let mut backend = factory
        .build_from_paused_state(sandbox_id, claimed, &state, None)
        .expect("build a stub");

    let err = backend.start().await.expect_err("the node ran another run");
    assert!(format!("{err:#}").contains(&started.to_string()), "{err:#}");
    assert!(
        script.seen_delete.lock().expect("lock").is_empty(),
        "a resume that disagreed about the run tore the user's sandbox down"
    );

    // 🔴 The control face for the emptiness above: the same node, the same
    // scripted delete, and a resume that *did* agree about the run — stopped,
    // it produces exactly the delete the run above did not. Without this half
    // an empty log would be evidence about the harness rather than about the
    // resume.
    *script.resume.lock().expect("lock") = Some(Ok(pb::SandboxResumeResponse {
        started: Some(pb::SandboxCreateResponse {
            sandbox_id: sandbox_id.to_string(),
            execution_id: claimed.to_string(),
            ..Default::default()
        }),
    }));
    let mut agreed = factory
        .build_from_paused_state(sandbox_id, claimed, &state, None)
        .expect("build a stub");
    agreed.start().await.expect("reopen");
    agreed.stop().await.expect("stop");
    let deletes = script.seen_delete.lock().expect("lock").clone();
    assert_eq!(deletes.len(), 1);
    assert_eq!(deletes[0].execution_id, claimed.to_string());
}

/// A paused state that does not say which run it captured is refused.
///
/// 🔴 It is the fence a resume carries. Without it the call says only *which
/// sandbox*, and a node still holding a stale paused record — one whose sandbox
/// was resumed elsewhere and paused there — would reopen the run the user
/// abandoned two runs ago while their newer work sat on another disk.
#[tokio::test]
async fn a_paused_state_that_does_not_say_which_run_it_captured_is_refused() {
    let (_script, node) = scripted_node().await;
    let factory = RemoteSandboxBackendFactory::new(node.placement());
    let execution_id = ExecutionId::new();

    let err = factory
        .decode_paused_state(
            std::path::PathBuf::from("/ignored"),
            serde_json::json!({
                "origin_node_id": "node-a",
                "artifact_root": "/var/lib/x",
                "state": {},
            }),
        )
        .expect_err("a paused state with no run on it was accepted");
    assert!(err.to_string().contains("which run"), "{err:#}");

    // A value that is present but is not an incarnation is refused too, rather
    // than becoming one.
    let err = factory
        .decode_paused_state(
            std::path::PathBuf::from("/ignored"),
            serde_json::json!({
                "origin_node_id": "node-a",
                "artifact_root": "/var/lib/x",
                "execution_id": "the-last-one",
                "state": {},
            }),
        )
        .expect_err("a paused state naming something that is not a run");
    assert!(err.to_string().contains("the-last-one"), "{err:#}");

    // 🔴 The control face: the same document with a real incarnation decodes,
    // and decodes to *that* incarnation.
    let decoded = factory
        .decode_paused_state(
            std::path::PathBuf::from("/ignored"),
            serde_json::json!({
                "origin_node_id": "node-a",
                "artifact_root": "/var/lib/x",
                "execution_id": execution_id.to_string(),
                "state": {},
            }),
        )
        .expect("a paused state that says which run it captured");
    assert_eq!(
        decoded
            .downcast_ref::<RemotePausedState>()
            .expect("a remote paused state")
            .paused_execution_id(),
        execution_id
    );
}

/// A resume that would run under the incarnation the sandbox was paused under
/// is refused before anything is sent.
///
/// 🔴 A resume starts a new run. One that reused the paused run's identity
/// would leave every command written before the pause indistinguishable from
/// one written after it, which is the whole thing the incarnation on every call
/// exists to tell apart.
#[tokio::test]
async fn a_resume_that_would_reuse_the_paused_run_is_refused() {
    let (_script, node) = scripted_node().await;
    let factory = RemoteSandboxBackendFactory::new(node.placement());
    let sandbox_id = launch_config().sandbox_id;
    let paused_execution_id = ExecutionId::new();
    let state = RemotePausedState::new(
        "node-under-test".to_string(),
        "/var/lib/agentenv/paused/7".to_string(),
        paused_execution_id,
        serde_json::json!({}),
    );

    let err = factory
        .build_from_paused_state(sandbox_id, paused_execution_id, &state, None)
        .err()
        .expect("a resume into the run it is replacing");
    assert!(err.to_string().contains("starts a new one"), "{err:#}");

    // 🔴 The control face: a different run builds, so this is not a method that
    // refuses everything.
    factory
        .build_from_paused_state(sandbox_id, ExecutionId::new(), &state, None)
        .expect("a resume under a run of its own");
}

/// A paused state some other factory produced is refused rather than sent
/// wherever placement points.
#[tokio::test]
async fn a_paused_state_this_factory_did_not_produce_is_refused() {
    let (_script, node) = scripted_node().await;
    let factory = RemoteSandboxBackendFactory::new(node.placement());
    let sandbox_id = launch_config().sandbox_id;

    let err = factory
        .build_from_paused_state(
            sandbox_id,
            ExecutionId::new(),
            &crate::sandbox::mock::MockSnapshot,
            None,
        )
        .err()
        .expect("a local paused state was accepted by the remote factory");
    assert!(err.to_string().contains("which machine"), "{err:#}");

    // 🔴 The control face: one this factory did produce builds.
    factory
        .build_from_paused_state(
            sandbox_id,
            ExecutionId::new(),
            &RemotePausedState::new(
                "node-under-test".to_string(),
                "/var/lib/agentenv/paused/7".to_string(),
                ExecutionId::new(),
                serde_json::json!({}),
            ),
            None,
        )
        .expect("a paused state from this factory");
}
