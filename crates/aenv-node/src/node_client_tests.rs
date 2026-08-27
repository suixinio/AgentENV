//! Driving a sandbox on "another machine" — a node service in this process,
//! reached over a real socket.
//!
//! 🔴 In `aenv-node` even though the code under test (`aenv_core::node_client`)
//! is the deciding half's. "Another machine" here is a real
//! `NodeSandboxService` on a real socket, and that service is this crate's —
//! a test in `aenv-core` could not link it. Everything below still exercises
//! `aenv-core`'s client; this crate only supplies the other end of the wire.
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
    DisabledSandboxPersister, InMemoryMetadataStore, MetadataStore, Orchestrator,
    SandboxOrchestration,
};
use crate::proto::node as pb;
use crate::proto::node::node_sandbox_service_server::{
    NodeSandboxService, NodeSandboxServiceServer,
};
use crate::role::ServerRole;
use crate::sandbox::mock::{MockBackendFactory, MockBehavior};
use crate::sandbox::{
    CustomExtensionParams, SandboxBackend, SandboxBackendFactory, SandboxForkSpec,
    SandboxLaunchConfig,
};
use crate::snapshot::mock::MockSnapshotArtifactStore;
use crate::snapshot::repository::{
    RepositoryResult, SnapshotCatalog, SnapshotCommit, SnapshotListFilter, SnapshotRepository,
    SnapshotRuntimeResolver, StagedSnapshot, StartedBuild,
};
use crate::snapshot::CapturedSandboxSnapshot;
use crate::snapshot::{
    CommittedSnapshot, RunnableSnapshot, SnapshotId, SnapshotManager, SnapshotRecord,
};
use crate::types::ExecutionId;

use crate::node_client::factory::RemoteSandboxBackendFactory;
use crate::node_client::paused_state::RemotePausedState;
use crate::node_client::placement::{
    FixedNodePlacement, NodeEndpoint, NodeMembership, NodePlacement,
};
use crate::node_client::wire;

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
        endpoint: NodeEndpoint::same_address("node-under-test", format!("http://{addr}")),
        _shutdown: tx,
        orchestration,
    }
}

/// A node backed by the real service, over a real orchestrator with mock
/// sandboxes.
async fn real_node() -> RunningNode {
    real_node_with_factory(MockBackendFactory::new()).await
}

/// Like [`real_node`], but with a caller-supplied backend factory — so a test
/// can hold the `MockBehavior` and read back what the *runtime* actually
/// received, not only what the node's own metadata store echoes.
async fn real_node_with_factory(factory: MockBackendFactory) -> RunningNode {
    crate::logging::init_for_tests();
    let orchestrator = Orchestrator::new(
        ServerRole::All,
        InMemoryMetadataStore::new(),
        factory,
        DisabledSandboxPersister,
        crate::image::DisabledRuntimeImageRefs::shared(),
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

/// A node wired to resolve OCI images itself, via a fake `regctl` that
/// records every reference it is asked to resolve into `{regctl_dir}/argv`
/// and answers with a registry 404 — a *user* error, so `ImageResolver`
/// refuses in one round trip rather than retrying (see
/// `tests/fixtures/regctl-recorder.sh`).
///
/// # 🔴 This is the only fixture in this file that exercises
/// `NodeSandboxService::with_template_build`
///
/// Every other real-node fixture here builds the service through `new`
/// alone, deliberately — see the note on `NodeSandboxService::template_build`
/// — because the RPCs those fixtures exercise never touch image resolution.
/// This one does: it is the fixture for `create`'s `Source::Image` arm and
/// `RemoteSandboxBackendFactory::build_from_image_ref`, and neither reaches
/// any further than `Unimplemented` without a wired `ImageResolver`.
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
    let image_resolver = Arc::new(crate::image::ImageResolver::new(&config));
    let template_builder = Arc::new(crate::template::TemplateBuilder::new());

    let orchestrator = Orchestrator::new(
        ServerRole::All,
        InMemoryMetadataStore::new(),
        MockBackendFactory::new(),
        DisabledSandboxPersister,
        crate::image::DisabledRuntimeImageRefs::shared(),
    )
    .await
    .expect("an in-memory orchestrator");
    let orchestration: Arc<dyn SandboxOrchestration> = orchestrator;
    let service = crate::node_server::NodeSandboxService::new(
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
        Some(Arc::new(AlwaysRunnable)),
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
    describe: Mutex<Option<Result<pb::SandboxDescribeResponse, Status>>>,
    update_params: Mutex<Option<Result<pb::SandboxParamsResponse, Status>>>,
    seen_create: Mutex<Vec<pb::SandboxCreateRequest>>,
    seen_pause: Mutex<Vec<pb::SandboxPauseRequest>>,
    seen_delete: Mutex<Vec<pb::SandboxDeleteRequest>>,
    seen_resume: Mutex<Vec<pb::SandboxResumeRequest>>,
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

/// 🔴 A newtype rather than `impl NodeSandboxService for Arc<ScriptedNode>`.
/// The trait is `aenv-core`'s (tonic generates it there, next to the proto),
/// and `Arc` is `std`'s, so the orphan rule refuses that impl from this crate.
/// A local wrapper is the smallest thing that is local.
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

    /// 🔴 `NOT_FOUND` when nothing was scripted, not `unimplemented`. A
    /// scripted node runs what a test told it to run and nothing else, and
    /// "this node is running nothing under that id" is the honest answer for a
    /// node that was told nothing — it is also the answer the attach path has
    /// to keep working through.
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

    // 🔴 Not scripted like the calls above: nothing in this module drives
    // `BuildTemplate` through a `ScriptedNode` — the node-side behavior is
    // covered directly in `src/node_server/tests.rs`, against the real
    // `NodeSandboxService`, and this fake exists to test `RemoteSandboxStub`
    // against the *other* RPCs. Unimplemented rather than unreachable so a
    // future test that does script this call fails loudly instead of hanging.
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
    assert!(
        backend.holding_node_id().is_none(),
        "a stub nobody has started has no machine to name yet"
    );

    backend.start().await.expect("start on the node");
    assert_eq!(backend.execution_id(), execution_id);
    // 🔴 The one fact `mark_running`'s fix depends on: once placed, this
    // backend must answer with the real node — not `None`, which the paused
    // sandbox registry's write path would silently read as "this process is
    // the machine", the exact bug this backend exists to not reproduce.
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

/// A custom extension params update sent through the stub lands in the real
/// node's own record of the sandbox — not merely in whatever this half
/// believes happened.
///
/// # 🔴 Why the real node and not a script
///
/// `custom_extension_params_update_is_a_real_round_trip` proves the stub
/// sends a real RPC and surfaces a real failure. This proves the other half
/// of the old lie: with a real `NodeSandboxService` behind the wire — the
/// same one `--role node` runs — the value the stub sent is the value the
/// node's own orchestrator now has on file for this sandbox, read back
/// through the node's own `get_sandbox`, which is the node-local analogue of
/// what a `GET` on the API half would answer. `None` and `Some(..)` in
/// succession is the contrasting pair: a build that only ever wrote the
/// first value, or only ever cleared it, fails one direction of this.
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

/// 🔴 The api half sends the same bytes whether or not it resolved the
/// snapshot first.
///
/// `--role api` used to reach `SnapshotRuntimeResolver::resolve` before every
/// warm create — downloading `vm_state.bin` onto its own disk, materializing
/// two overlaybd `image.json` files and leasing them — and then throw all of
/// it away here, because a `SnapshotSource` carries the catalog row and
/// nothing else. `build_from_snapshot_record` is that create without the
/// download, and the *only* thing that makes it safe to switch a live
/// deployment onto it is that the node cannot tell the difference.
///
/// So this compares the encoded request, not a field of it: a divergence in
/// any field, including one added later, changes these bytes.
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

/// 🔴 The API half tells the node, in as many words, that it keeps this
/// sandbox's deadline itself.
///
/// This is the half of the fix that lives on the sending side, and it is worth
/// a test of its own because the failure it prevents is a *silence*: the field
/// used to be a `uint64` left at its zero value, the node read that zero as
/// "the caller named no deadline, use yours", and its
/// `default_sandbox_timeout_secs` paused a VM this half went on reporting as
/// running. Nothing logged a disagreement, because neither half knew there was
/// one.
///
/// The two values it must not send are built here as well. Both are what a
/// plausible edit produces — `node_kept_default` is literally the old
/// behaviour, and a `node_kept_timeout_ms` is what "just send the timeout"
/// produces — and neither is distinguishable from the right answer by anything
/// else in this crate.
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

    // 🔴 The three answers this field has, and the two that are wrong here.
    // `None` is the one that used to be sent, by way of a zero.
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

/// A custom extension params update is a real round trip now, not a
/// fire-and-forget task: the node sees exactly the value that was sent, and a
/// node refusal comes back to the caller as an error rather than a log line
/// nobody but that Pod can read.
///
/// # 🔴 Why this test exists
///
/// `RemoteSandboxStub::update_custom_extension_params` used to spawn a task
/// and return before the RPC was even sent — a caller could not tell success
/// from failure, both looked exactly like `()`, and the only trace of a
/// refusal was an `error!` line on a different process. The faces here, all
/// in one round:
///
/// * two different values sent in succession each reach the node as
///   themselves — not as each other and not as a stale copy of the first —
///   proving the wire payload tracks the call rather than something fixed;
/// * a node refusal is returned to the caller as `Err` and carries the
///   node's own message, rather than being swallowed.
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

// ---------------------------------------------------------------------------
// A stale node address: retried once against a freshly resolved one, and
// never against an answer the node actually sent
// ---------------------------------------------------------------------------

/// A placement source that hands out one address for the *initial* placement
/// and a different one — for the same node id — once asked to re-resolve.
///
/// Models the split the stale-node-address retry depends on: a scheduler's
/// per-sandbox binding cache (`place_existing`) can go on naming a node's old
/// address long after `resolve_node` — backed by node discovery rather than
/// that cache — already knows the new one.
struct ReplacementNodePlacement {
    node_id: String,
    initial: NodeEndpoint,
    resolved: NodeEndpoint,
    resolve_calls: std::sync::atomic::AtomicUsize,
    /// How many calls to `resolve_node` answer with `resolved` before every
    /// call after that refuses instead.
    ///
    /// 🔴 Load-bearing for
    /// `a_forks_children_carry_the_connection_the_retry_actually_used`, and
    /// only for that test: every remote call retries once on its own, so a
    /// fork child built from a stale, pre-retry connection would silently
    /// self-heal on its *own* first call — the very next `resolve_node` —
    /// and the bug that test exists to catch would go unnoticed. Setting
    /// this to `1` removes that safety net: a second re-resolve, which only
    /// happens if the child was handed the connection the retry had already
    /// abandoned, fails instead of quietly succeeding. Every other test
    /// leaves this at `usize::MAX`, where the cap never bites.
    resolve_budget: usize,
}

#[async_trait]
impl NodePlacement for ReplacementNodePlacement {
    async fn place_new(
        &self,
        _sandbox_id: crate::types::SandboxId,
        _resources: crate::types::SandboxResources,
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

    /// Unused by every test this double serves: they all exercise the stale
    /// *connection* retry, never a dial that fails outright, so `attach`'s
    /// membership check is never reached.
    async fn node_membership(&self, _node_id: &str) -> anyhow::Result<NodeMembership> {
        Ok(NodeMembership::Present)
    }

    async fn record_placement(
        &self,
        _sandbox_id: crate::types::SandboxId,
        _execution_id: ExecutionId,
        _node: &NodeEndpoint,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

/// A stale node address — a connection that once worked and now cannot be
/// reached at all — is retried exactly once against a freshly resolved
/// address, and the retry reaches the node the fresh address actually names.
///
/// 🔴 Deleting the retry turns this red outright. Wiring the re-resolve to
/// `place_existing` instead of `resolve_node` — exactly the stale binding
/// cache this exists to route around — also turns it red: `place_existing`
/// here answers with `initial` again, the same dead address, so the retried
/// call would fail exactly like the first one instead of reaching
/// `script_b`.
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

/// A fork's child stubs carry the connection the retry actually used, not the
/// one it had already abandoned by the time the fork succeeded.
///
/// 🔴 Reading `node`/`client` from a value captured *before* the retried
/// `fork` call — instead of from `self.placed` afterward — turns this red:
/// every child would be built from the dead node's connection, and operating
/// on one would fail exactly like the parent's first attempt did, rather than
/// reaching the node the fork was actually run on.
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

/// The control face: a status the node *sent* — even the identically-coded
/// `Unavailable` a transport failure also produces — is never retried against
/// a different address. Only a connection that never reached the node at all
/// is.
///
/// 🔴 This is the half `an_unreachable_node_does_not_mean_the_sandbox_stopped`
/// cannot cover on its own: that test's node answers the same canned status on
/// every call, so a build that wrongly retried an application-level
/// `Unavailable` would still surface the same message text and pass it.
/// Pointing the re-resolve target at a second node that would visibly answer
/// instead — and asserting it is never touched — is what catches that
/// mutation.
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

/// The re-resolve-and-retry path is bounded: a re-resolve that would take
/// longer than `STALE_PLACEMENT_RETRY_BUDGET` does not turn a fast failure
/// into a slow one. The original transport failure is surfaced once the
/// budget elapses, not once the slow re-resolve eventually finishes.
///
/// 🔴 Deleting the `tokio::time::timeout` wrapper around the retry — while
/// leaving everything else intact — turns this red: without it, this test
/// hangs for the placement's full multi-second `resolve_node` delay instead
/// of returning within the budget, and the elapsed-time assertion below
/// catches that directly rather than via a flaky sleep-and-hope race.
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
        /// Unused: this test is about the reconnect retry budget, never
        /// reached from `attach`'s dial-failure path.
        async fn node_membership(&self, _node_id: &str) -> anyhow::Result<NodeMembership> {
            Ok(NodeMembership::Present)
        }
        async fn record_placement(
            &self,
            _sandbox_id: crate::types::SandboxId,
            _execution_id: ExecutionId,
            _node: &NodeEndpoint,
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

// ---------------------------------------------------------------------------
// A black-holed address: bounded by `STUB_CONNECT_TIMEOUT`, not by the
// kernel's own SYN-retry ceiling
// ---------------------------------------------------------------------------

/// A private, non-routable address whose SYN is never answered — chosen
/// empirically for this suite rather than assumed.
///
/// 🔴 Not `127.0.0.1:1` (or any other closed local port): that fails with an
/// instant RST regardless of any `connect_timeout`, which is exactly why it
/// tests nothing about this fix — see
/// `a_node_nobody_answers_does_not_hang_the_remote_build` in
/// `src/api/impls/template.rs`, which uses exactly that address to test a
/// *different* thing (a refused connection must not hang a build lease),
/// and its own doc comment says so.
///
/// 🔴 Also not an RFC 5737 documentation address (`192.0.2.0/24` and
/// friends), despite those being the standard textbook choice for "an
/// address nothing will ever answer": verified directly against this
/// repository's own dev sandbox before writing this test, a raw
/// `TcpStream::connect` to `192.0.2.1` returns *successfully* in well under
/// a millisecond — some outbound network layer between this container and
/// the internet answers on behalf of the whole public documentation range,
/// which would make a test built on it pass by accident regardless of
/// whether `STUB_CONNECT_TIMEOUT` does anything at all. A private,
/// unassigned address such as this one is not proxied the same way: a raw
/// socket connect to it was confirmed to block with no answer at all (not
/// even an ICMP unreachable) for as long as it was given, which is the
/// actual shape of failure a deleted Kubernetes pod's address produces on a
/// real cluster.
const BLACK_HOLE_ENDPOINT: &str = "http://10.255.255.1:1";

/// The bug this whole change fixes, isolated to the one function it lives
/// in: dialing an address whose SYN is never answered used to be gated only
/// by the kernel's own SYN-retry ceiling (`net.ipv4.tcp_syn_retries`,
/// exponential backoff capped around two minutes at Linux's default) — which
/// is what a real cluster incident measured at ~71 seconds before this fix.
/// `RemoteSandboxStub::connect` now carries `STUB_CONNECT_TIMEOUT`; this
/// asserts the dial gives up at that bound instead.
///
/// 🔴 Deleting `.connect_timeout(STUB_CONNECT_TIMEOUT)` from
/// `RemoteSandboxStub::connect` turns this red: without it, the dial this
/// test makes does not return within the outer `tokio::time::timeout`
/// below, and the test fails on that outer bound rather than on the
/// elapsed-time assertion — deliberately, so a regression here fails this
/// suite in 20 seconds instead of hanging it for the kernel's own ceiling.
/// Confirmed by temporarily reverting that one line and re-running this test
/// alone: the outer 20s timeout fired every time.
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

/// The two timing constants are sized together, not independently: see
/// `STUB_CONNECT_TIMEOUT`'s doc in `stub.rs` for why the reconnect inside
/// `STALE_PLACEMENT_RETRY_BUDGET` has to leave room, in the same budget, for
/// the `resolve_node` RPC and the retried call that follow it. Mirrors the
/// style of `RedisStoreConfig::validate`'s paired-field checks
/// (`src/orchestrator/store/redis/config.rs`), as a plain assertion rather
/// than a new validation system for two `const`s.
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
        staging_error: String::new(),
    }));

    let factory = RemoteSandboxBackendFactory::new(node.placement());
    let mut backend =
        match factory.build_from_snapshot(&RunnableSnapshot::mock(), launch_config(), execution_id)
        {
            Ok(backend) => backend,
            Err(err) => panic!("building a stub should not fail: {err:#}"),
        };
    backend.start().await.expect("start");

    let capture = backend.pause(None, false).await.expect("pause");
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
        staging_error: String::new(),
    }));

    let factory = RemoteSandboxBackendFactory::new(node.placement());
    let mut backend =
        match factory.build_from_snapshot(&RunnableSnapshot::mock(), launch_config(), execution_id)
        {
            Ok(backend) => backend,
            Err(err) => panic!("building a stub should not fail: {err:#}"),
        };
    backend.start().await.expect("start");

    let err = match backend.pause(None, false).await {
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

/// A row a node might hand back, staged on the node under test.
fn staged_snapshot(execution_id: ExecutionId) -> StagedSnapshot {
    StagedSnapshot {
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
    }
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

    let staged = staged_snapshot(execution_id);
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

/// The counterpart of the refusal just above, for the method that exists
/// precisely because that one refuses: `build_from_image_ref` is handed a
/// reference — not a path — and this pins that the reference *is* what
/// crosses the wire, and that the node resolves it with its own
/// `ImageResolver` rather than trusting a value this process never had.
///
/// # 🔴 Why the fake `regctl`'s argv is the only evidence that matters
///
/// This process never calls `regctl` at all — `real_node_with_image_resolution`
/// wires the fake into the *node's* `deps_path`, not this process's. So the
/// fake recording exactly the reference this test sent is proof of two
/// things at once: that `RemoteSandboxBackendFactory::build_from_image_ref`
/// shipped `spec.image_ref` unresolved (a resolved reference would have shown
/// up as a local overlaybd config path, and there would have been no regctl
/// call here to record at all), and that `NodeSandboxService::create`'s
/// `Source::Image` arm is the one that called it, on the node.
///
/// A real image resolve is out of scope here — it needs a real manifest, real
/// blobs and real overlaybd conversion — so the fake answers every lookup
/// with a registry 404 and the create fails there. That failure is itself
/// checked: `start` must fail with the *node's* resolve error, not with
/// `Unimplemented` — the answer this call used to give unconditionally, and
/// would still give if `create`'s `Source::Image` arm regressed to refusing.
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

/// 🔴 The remote factory must not send a blank ownership marker.
///
/// `RemoteSandboxBackendFactory` used to send `control_plane_config:
/// Vec::new()` on every create. A grep for who sets the marker therefore came
/// back with tests and nothing else, which read exactly like a defect — and
/// would have been one the moment anything drove this factory for real: every
/// sandbox it created would be one the control plane does not recognise as its
/// own, invisible to `ListSandboxes` and to the reconciliation that runs off
/// it. It was not one at the time only because `--role api` refused to
/// assemble, so nothing constructed this factory outside these tests.
///
/// Both halves of that have since been settled: `--role api` assembles (it is
/// its own binary now, in `crates/aenv-api`), and the marker arrives from the
/// caller. What is left is the assertion that mattered, kept on its own.
///
/// 🔴 This used to `include_str!` the api binary to decide which of two
/// branches to assert. That branch is dead — the binary lives in another crate
/// and assembles — and reaching across the crate boundary with a relative path
/// to read it would be a coupling with nothing left to check.
#[test]
fn the_remote_factory_sends_no_blank_ownership_marker() {
    const BLANK_MARKER: &str = "control_plane_config: Vec::new()";

    // 🔴 A relative path into `aenv-core`: the file under scan is that
    // crate's, and there is no other way to read its source text.
    let factory = include_str!("../../../src/node_client/factory.rs");

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
        crate::image::DisabledRuntimeImageRefs::shared(),
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
            Arc::new(crate::image::ImageResolver::new(
                &crate::cfg::AppConfig::default(),
            )),
            Arc::new(crate::template::TemplateBuilder::new()),
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

// ---------------------------------------------------------------------------
// Pause, and what must not follow it
// ---------------------------------------------------------------------------

/// Scripts a create and a pause on a node, and returns a stub that has done
/// both.
async fn paused_stub(
    script: &Arc<ScriptedNode>,
    node: &RunningNode,
) -> (Box<dyn SandboxBackend>, ExecutionId) {
    let execution_id = ExecutionId::new();
    *script.create.lock().expect("lock") = Some(Ok(pb::SandboxCreateResponse {
        sandbox_id: launch_config().sandbox_id.to_string(),
        execution_id: execution_id.to_string(),
        ..Default::default()
    }));
    *script.pause.lock().expect("lock") = Some(Ok(pb::SandboxPauseResponse {
        paused_state: Some(pb::PausedState {
            artifact_root: "/var/lib/agentenv/paused/7".to_string(),
            state: Some(wire::serialize(&serde_json::json!({}), "state").expect("encode")),
        }),
        staged: None,
        staging_error: String::new(),
    }));
    let factory = RemoteSandboxBackendFactory::new(node.placement());
    let mut backend = factory
        .build_from_snapshot(&RunnableSnapshot::mock(), launch_config(), execution_id)
        .expect("build a stub");
    backend.start().await.expect("start");
    (backend, execution_id)
}

/// 🔴 Stopping a sandbox that has just been paused does not delete it.
///
/// `Orchestrator::pause_sandbox` calls `stop` on the backend right after a
/// successful pause, "to free up resources". Locally that tears down a VM
/// process and leaves the capture on disk. Over this wire the only teardown is
/// `Delete`, which takes the paused record and its artifacts with it — so a
/// `stop` sent as a `Delete` erases the capture the pause has just promised the
/// user, and the sandbox then comes back `NotFound` from the one machine that
/// had it.
///
/// The control face is the same `stop` on a stub that was never paused, which
/// *must* delete: without it this test passes on a `stop` that does nothing at
/// all, which is how a cluster stops accounting for VMs that are still running.
#[tokio::test]
async fn stopping_a_sandbox_that_was_just_paused_does_not_delete_its_capture() {
    let (script, node) = scripted_node().await;

    let (mut paused, _) = paused_stub(&script, &node).await;
    paused.pause(None, false).await.expect("pause");
    paused.stop().await.expect("stop");
    assert!(
        script.seen_delete.lock().expect("lock").is_empty(),
        "the capture was deleted by the stop that follows every pause: {:?}",
        script.seen_delete.lock().expect("lock")
    );

    // The same call on a stub that was not paused: this one is a teardown and
    // has to reach the node.
    let (mut running, execution_id) = paused_stub(&script, &node).await;
    running.stop().await.expect("stop");
    let deletes = script.seen_delete.lock().expect("lock").clone();
    assert_eq!(deletes.len(), 1, "a running sandbox was not torn down");
    assert_eq!(deletes[0].execution_id, execution_id.to_string());
}

/// The pause this half sends asks for exactly what its caller promised to
/// commit, and brings the row back only when it did.
///
/// 🔴 Both values of the flag, in one test, because a `publish` flag is one bit
/// and nothing in this project's mutation testing covers a `.proto` field.
/// "The request said false" on its own is a fact about a constant; the pair —
/// the flag on the wire tracking the argument, and the row coming back only on
/// the arm that asked for it — is a fact about behaviour.
///
/// 🔴 The staged row on the `true` arm is what stops this passing on a build
/// that sets the flag and drops the reply. That drop is not a cosmetic
/// failure: the node has written a whole snapshot into durable storage by the
/// time it answers, and a caller that discards the row leaves those bytes with
/// nothing to announce them and no way to find them again.
#[tokio::test]
async fn a_pause_asks_for_a_row_only_when_its_caller_will_commit_one() {
    let (script, node) = scripted_node().await;

    let staged = staged_snapshot(ExecutionId::new());
    // 🔴 After `paused_stub`, which writes its own pause script on the way to
    // starting the sandbox. Setting it first would be overwritten and this test
    // would assert against a reply with no row in it.
    let (mut backend, _) = paused_stub(&script, &node).await;
    *script.pause.lock().expect("lock") = Some(Ok(pb::SandboxPauseResponse {
        paused_state: Some(pb::PausedState {
            artifact_root: "/var/lib/agentenv/paused/one".to_string(),
            state: Some(wire::serialize(&serde_json::json!({}), "state").expect("encode")),
        }),
        staged: Some(pb::StagedSnapshot {
            value: Some(wire::serialize(&staged, "staged snapshot").expect("encode")),
        }),
        staging_error: String::new(),
    }));
    let capture = backend
        .pause(None, true)
        .await
        .expect("a pause whose caller will commit");
    let sent = script.seen_pause.lock().expect("lock").clone();
    assert_eq!(sent.len(), 1, "the pause did not reach the node");
    assert!(
        sent[0].publish,
        "a caller that promised to commit asked the node for nothing"
    );
    let publishable = capture
        .publishable
        .expect("a pause that asked to publish came back with nothing to publish");
    let CapturedSandboxSnapshot::Staged(decoded) = publishable else {
        panic!("the capture carries a staged snapshot and not something else");
    };
    let decoded: crate::snapshot::repository::StagedSnapshot = *decoded;
    assert_eq!(decoded.id(), staged.id());
    assert_eq!(decoded.origin_node_id, "node-under-test");

    // The other arm: nobody is going to commit, so nothing is asked for — and
    // the node's row, if it sent one anyway, is not adopted.
    let (script, node) = scripted_node().await;
    let (mut backend, _) = paused_stub(&script, &node).await;
    *script.pause.lock().expect("lock") = Some(Ok(pb::SandboxPauseResponse {
        paused_state: Some(pb::PausedState {
            artifact_root: "/var/lib/agentenv/paused/two".to_string(),
            state: Some(wire::serialize(&serde_json::json!({}), "state").expect("encode")),
        }),
        staged: Some(pb::StagedSnapshot {
            value: Some(wire::serialize(&staged, "staged snapshot").expect("encode")),
        }),
        staging_error: String::new(),
    }));
    let capture = backend
        .pause(None, false)
        .await
        .expect("a pause whose caller will not commit");
    let sent = script.seen_pause.lock().expect("lock").clone();
    assert!(
        !sent[0].publish,
        "this half asked a node to stage a row it does not commit"
    );
    assert!(
        capture.publishable.is_none(),
        "a row nobody asked for was adopted anyway"
    );
}

/// The whole published pause, across both halves: this half asks, the real node
/// service stages, and the row that comes back is the one the node wrote.
///
/// 🔴 The scripted test above proves the stub reads a reply. This one proves
/// there is a reply to read: it drives the request through the real service,
/// whose refusal of this exact flag is what the split shipped with.
#[tokio::test]
async fn a_published_pause_crosses_both_halves() {
    let real = real_node().await;
    let orchestration = real.orchestration.as_ref().expect("a real node").clone();
    let factory = RemoteSandboxBackendFactory::new(real.placement());
    let execution_id = ExecutionId::new();
    let config = launch_config();
    let mut backend = factory
        .build_from_snapshot(&RunnableSnapshot::mock(), config, execution_id)
        .expect("build a stub");
    backend.start().await.expect("start on the node");

    let capture = backend
        .pause(None, true)
        .await
        .expect("the published pause this half sends");
    assert!(
        orchestration
            .list_live_sandboxes()
            .await
            .expect("list")
            .is_empty(),
        "the pause this half sends did not pause anything"
    );

    // 🔴 `real_node` gives the service a snapshot manager that refuses to
    // stage, which is the honest default for a harness that writes no bytes. So
    // the assertion this arm can make is the one that matters for the split:
    // the flag is no longer refused at the door, and the sandbox was paused
    // either way. Whether a row comes back when staging works is
    // `a_published_pause_stages_the_bytes_here_and_leaves_the_row_to_the_caller`.
    assert!(
        capture.publishable.is_none(),
        "a node whose repository refuses to stage handed back a row anyway"
    );
}

/// The whole of it: a sandbox started from here, paused from here, and reopened
/// from here on the machine that held its bytes.
///
/// 🔴 Nothing else in this file crosses both halves. The pause tests script the
/// node's reply, and the resume tests pause the sandbox through the node's own
/// orchestrator; either passes on a build whose `Pause` writes a record no
/// `Resume` can find. This one takes the paused state the pause reply produced,
/// puts it through the round trip a record makes it take, and hands the result
/// back to the factory.
///
/// The control face is the machine: the same encoded state with a different
/// origin node is refused rather than sent, and the refusal happens with the
/// node still holding the capture — so "the resume worked" is about the machine
/// that has the bytes and not about any machine.
#[tokio::test]
async fn a_sandbox_paused_from_here_is_reopened_where_its_bytes_are() {
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

    let capture = backend.pause(None, false).await.expect("pause on the node");
    // 🔴 The stop the orchestrator sends after every successful pause. It is
    // here rather than left out because leaving it out is what made this round
    // trip look like it worked.
    backend.stop().await.expect("stop after the pause");
    assert!(
        orchestration
            .list_live_sandboxes()
            .await
            .expect("list")
            .is_empty(),
        "the paused sandbox is still running"
    );

    // Through the encoding a record forces on it, and back.
    let encoded = capture.state.encode().expect("encode");
    assert_eq!(encoded["origin_node_id"], "node-under-test");
    assert_eq!(
        encoded["execution_id"],
        paused_execution_id.to_string(),
        "the capture named a run other than the one it was taken from"
    );
    let restored = factory
        .decode_paused_state(std::path::PathBuf::from("/ignored"), encoded.clone())
        .expect("decode");

    // 🔴 The control face: the same capture, one value different — the machine.
    let mut elsewhere = encoded.clone();
    elsewhere["origin_node_id"] = serde_json::json!("node-that-holds-nothing");
    let elsewhere = factory
        .decode_paused_state(std::path::PathBuf::from("/ignored"), elsewhere)
        .expect("decode");
    let resumed_execution_id = ExecutionId::new();
    let mut wrong = factory
        .build_from_paused_state(sandbox_id, resumed_execution_id, elsewhere.as_ref(), None)
        .expect("build a stub");
    wrong
        .start()
        .await
        .expect_err("a capture on another machine was reopened here");
    assert!(
        orchestration
            .list_live_sandboxes()
            .await
            .expect("list")
            .is_empty(),
        "a refused resume started something anyway"
    );

    let mut back = factory
        .build_from_paused_state(sandbox_id, resumed_execution_id, restored.as_ref(), None)
        .expect("build a stub");
    back.start().await.expect("reopen on the node that has it");

    let live = orchestration.list_live_sandboxes().await.expect("list");
    assert_eq!(live.len(), 1, "the sandbox did not come back: {live:?}");
    assert_eq!(live[0].sandbox_id, sandbox_id);
    assert_eq!(live[0].execution_id, Some(resumed_execution_id));
    assert_ne!(
        live[0].execution_id,
        Some(paused_execution_id),
        "the sandbox came back as the run it was paused under"
    );
}

// ---------------------------------------------------------------------------
// Two replicas over one ledger
// ---------------------------------------------------------------------------
//
// 🔴 What this section is for, stated once.
//
// `Orchestrator` keeps its live backends in a process-local map. That map is
// the whole truth when there is one process, and `--role api` is deployed as
// two replicas behind a Service with **no session affinity** — so the replica a
// request lands on is not usually the replica that started the sandbox.
//
// The pause path read "no handle here" as "the sandbox is gone" and deleted the
// shared record. On a cluster that meant: the user's pause answered 404, the VM
// went on running on its node, and the only thing that could still name it had
// just been erased. Two of those filled half a machine, and no API call could
// reach either.
//
// Everything below is written as two faces that differ in exactly one value —
// which replica the call lands on, whether the machine can be reached, whether
// the factory's sandboxes are on other machines — because the failure is a
// *silence* and an assertion with one face passes on a build that does nothing
// at all.

/// One `InMemoryMetadataStore` behind more than one orchestrator.
///
/// 🔴 Every method forwards, including the ones with defaults. A newtype that
/// let a default stand would be a second store implementation wearing the first
/// one's name, and the whole point here is that two replicas read and write the
/// *same* records.
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
    async fn paused_handle(
        &self,
        sandbox_id: &crate::types::SandboxId,
    ) -> StoreResult<crate::orchestrator::PausedHandle> {
        self.0.paused_handle(sandbox_id).await
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
    async fn reserve(
        &self,
        sandbox_id: &crate::types::SandboxId,
    ) -> StoreResult<crate::orchestrator::Reservation> {
        self.0.reserve(sandbox_id).await
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

type ApiReplica =
    Arc<Orchestrator<SharedLedger, RemoteSandboxBackendFactory, DisabledSandboxPersister>>;

/// One replica of the deciding half: the cluster ledger, and a factory whose
/// sandboxes are on `node`.
async fn api_replica(node: &RunningNode, ledger: &SharedLedger) -> ApiReplica {
    Orchestrator::new(
        // 🔴 `All` rather than `Api`, and it changes nothing this file is
        // about: the role is read once, to decide whether the process may
        // invent its own envd access-token seed, and a test config has none to
        // find. Everything that makes this a replica of the deciding half is
        // the store and the factory below.
        ServerRole::All,
        ledger.clone(),
        RemoteSandboxBackendFactory::new(node.placement()),
        DisabledSandboxPersister,
        crate::image::DisabledRuntimeImageRefs::shared(),
    )
    .await
    .expect("a replica of the deciding half")
}

/// A machine-local half: its sandboxes are in its own process.
async fn local_half(
) -> Arc<Orchestrator<InMemoryMetadataStore, MockBackendFactory, DisabledSandboxPersister>> {
    Orchestrator::new(
        ServerRole::All,
        InMemoryMetadataStore::new(),
        MockBackendFactory::new(),
        DisabledSandboxPersister,
        crate::image::DisabledRuntimeImageRefs::shared(),
    )
    .await
    .expect("a machine-local orchestrator")
}

fn cluster_create_request() -> crate::orchestrator::CreateSandboxRequest {
    crate::orchestrator::CreateSandboxRequest {
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
    }
}

/// The ids the node is running right now.
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

/// Waits until nothing is listening on the node's address any more.
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

/// A pause pauses the same VM whichever replica it lands on.
///
/// 🔴 Two faces differing in exactly one value: which of two replicas of one
/// deciding half the call was made on. Both must succeed **and** both VMs must
/// actually stop on the node — a build where the second face merely returned a
/// different error, or returned `Ok` without asking anyone, passes neither
/// half of that.
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

    // Face 2: the same call, on the replica that did not. The one value that
    // differs between the two.
    Arc::clone(&landed_elsewhere)
        .pause_sandbox(stray.id)
        .await
        .expect("a replica that did not start the sandbox pauses it");

    // 🔴 Both VMs are actually down on the machine that was running them.
    // Without this the test passes on a pause that answered `Ok` and told
    // nobody, which is the other half of the same bug.
    assert_eq!(
        running_on(&node).await,
        Vec::<crate::types::SandboxId>::new(),
        "a pause reported success and left the VM running"
    );

    for sandbox_id in [owned.id, stray.id] {
        assert_eq!(
            on_the_node
                .get_sandbox(&sandbox_id)
                .await
                .expect("the node's own record")
                .expect("the node kept a record")
                .state,
            crate::orchestrator::SandboxState::Paused,
        );
        assert_eq!(
            ledger
                .0
                .get(&sandbox_id)
                .await
                .expect("read the ledger")
                .expect("the shared record survived the pause")
                .state,
            crate::orchestrator::SandboxState::Paused,
        );
    }
}

/// A missing handle takes the shared record with it **only** when there is
/// nothing anywhere to address.
///
/// 🔴 The second face is the one that gives the first its resolution. "The
/// record is still there" is satisfied by a build whose clean-up code never
/// runs at all, so the same test drives a case where a record genuinely *is*
/// removed. The single value that differs is whether the factory's sandboxes
/// live on other machines.
#[tokio::test]
async fn a_missing_handle_removes_the_record_only_when_nothing_can_be_addressed() {
    let node = real_node().await;
    let ledger = SharedLedger(Arc::new(InMemoryMetadataStore::new()));
    let started_here = api_replica(&node, &ledger).await;
    let landed_elsewhere = api_replica(&node, &ledger).await;

    // Face 1: sandboxes on other machines. The replica taking the call holds no
    // handle, and the record must survive.
    let remote = Arc::clone(&started_here)
        .create_sandbox(cluster_create_request())
        .await
        .expect("create on the node");
    Arc::clone(&landed_elsewhere)
        .pause_sandbox(remote.id)
        .await
        .expect("a replica holding no handle pauses the sandbox");
    let record = ledger
        .0
        .get(&remote.id)
        .await
        .expect("read the ledger")
        .expect("🔴 the shared record was deleted by a pause on a replica that held no handle");
    assert_eq!(record.state, crate::orchestrator::SandboxState::Paused);

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

/// A pause that cannot reach the machine leaves the sandbox exactly as it was.
///
/// 🔴 "I could not find out" is neither of the other two answers, and folding
/// it into "the sandbox is gone" is what turned a network hiccup into a deleted
/// record. The control face is the same call, on the same replica, against the
/// same sandbox — with the machine up.
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

    // 🔴 The record is untouched, and it is back in the state it started in —
    // not left parked in `Pausing`, which is a sandbox no later call can act
    // on.
    let record = ledger
        .0
        .get(&unreachable.id)
        .await
        .expect("read the ledger")
        .expect("🔴 an unreachable machine caused the shared record to be deleted");
    assert_eq!(record.state, crate::orchestrator::SandboxState::Running);

    // The control's record, for contrast, moved.
    assert_eq!(
        ledger
            .0
            .get(&reachable.id)
            .await
            .expect("read the ledger")
            .expect("the paused sandbox kept its record")
            .state,
        crate::orchestrator::SandboxState::Paused,
    );
}

/// A delete on a replica that did not start the sandbox reaches the machine.
///
/// 🔴 This is the other half of the same fault, and it failed the opposite way
/// round: the delete found no handle, skipped the teardown entirely, removed
/// the record and answered 204. The VM stayed up with nothing left to name it.
///
/// The second face is a delete that could not reach the machine, which must
/// keep the record — and which is what stops the first face from passing on a
/// build that deletes records unconditionally.
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

/// A replica going away does not pause the cluster's sandboxes.
///
/// 🔴 The shutdown path preserves everything in the record store by pausing it,
/// which is what a machine that is about to stop running VMs owes them. A
/// deciding half's record store is the *cluster's* ledger and it runs no VMs at
/// all, so the same loop there pauses every running sandbox in the cluster once
/// per replica rolled.
///
/// The control face is a half whose sandboxes really are its own: its shutdown
/// must still pause them, or this test passes on a build that has simply
/// stopped preserving anything.
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

    // Control face: a half whose VMs are in its own process still preserves
    // them on the way out.
    let local = local_half().await;
    let mine = Arc::clone(&local)
        .create_sandbox(cluster_create_request())
        .await
        .expect("create in this process");
    Arc::clone(&local)
        .shutdown()
        .await
        .expect("the local half shuts down");
    assert_eq!(
        local
            .get_sandbox(&mine.id)
            .await
            .expect("read the record")
            .expect("a preserved sandbox keeps its record")
            .state,
        crate::orchestrator::SandboxState::Paused,
        "a half that runs its own VMs stopped preserving them"
    );
}

/// An egress policy set on a replica that did not start the sandbox reaches the
/// machine.
///
/// 🔴 This path failed less loudly than pause and delete — it answered
/// `SandboxOperationConflict`, a 409 telling the user something else was busy
/// with their sandbox when nothing was — but it failed for exactly the same
/// reason, on whichever replica the request happened to land.
///
/// The control face is a half whose sandboxes really are in its own process:
/// there a missing handle *is* a conflict, and it must still be reported as
/// one.
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

    // 🔴 On the machine, not merely in the ledger. A build that recorded the
    // policy and told nobody satisfies every assertion that stops at the store.
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

// ---------------------------------------------------------------------------
// Fork, driven by the deciding half
// ---------------------------------------------------------------------------

/// Where this replica would send traffic for a sandbox.
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

/// A fork driven by the API half makes every child routable, at the address the
/// child's own VM answers on.
///
/// # 🔴 This is the shape the bug had in production
///
/// `--role api` forks by asking a node, and the node's answer is the only thing
/// this half ever learns about where a child is. The node used to answer with
/// an empty address, so `proxy_target_from_sandbox` refused every child with
/// *missing host interaction IP after start* — deterministically, on children
/// whose VMs were up and healthy on the node, which were then torn down again.
///
/// The faces, against one node in one round:
///
/// * Every child both **starts** and is **routable**. A build that starts the
///   children and cannot route them fails the second half, which is exactly
///   what the bug did.
/// * The three addresses — two children and their source — are all different.
///   That is what says each was read from that child's own handle on the node,
///   rather than being a constant or a copy of the source's.
/// * The node itself agrees all three are running, so nothing here is satisfied
///   by a route to a VM that is not there.
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

/// A child the node reported no address for arrives here with none.
///
/// 🔴 Two halves one wire value apart, in one answer to one fork: the first
/// child comes back with an address and a rootfs size, the second with the
/// blanks proto3 cannot tell from "unset". The first has to arrive as facts.
/// The second has to arrive as *nothing at all* — never as a default, never as
/// its sibling's — because that `None` is what makes the orchestrator above
/// refuse the child loudly instead of publishing a route to an address nothing
/// is listening on.
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

// ---------------------------------------------------------------------------
// Attaching to a sandbox this process did not start
// ---------------------------------------------------------------------------

/// A backend for a sandbox this replica did not start, built exactly the way
/// `Orchestrator::absent_handle` builds one.
///
/// 🔴 The factory call and the `start` that follows are that method's two
/// lines. The orchestrator does not hand back the backend it adopts, and
/// repeating the two calls here is closer to the path under test than adding an
/// accessor to production code for a test to read.
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

/// A replica that did not start a sandbox says the same address for it as the
/// replica that did.
///
/// # 🔴 The gap this closes
///
/// `--role api` runs several replicas behind a Service with no session
/// affinity, so a call about a sandbox usually lands on a replica that did not
/// start it. That replica adopts the sandbox — and the adopted stub used to be
/// placed with its address and rootfs size left as `None`, on the reasoning
/// that this half never made the call that would have reported them.
///
/// `None` is not a neutral value here. It is what a node reports for a sandbox
/// that has no address, and the orchestrator above turns that into
/// *sandbox missing host interaction IP after start* and tears the sandbox
/// down — which is exactly the production failure `1d5cd05` fixed for fork
/// children. The blank did not read as "nobody asked"; it read as a verdict
/// about a healthy sandbox.
///
/// # 🔴 The faces, one node and one round
///
/// * **the replica that started it against the one that did not.** Both must
///   answer, and both must answer the *same* address — it is one sandbox. A
///   build where the second answers `None` fails, and so does one where the
///   first stopped answering.
/// * **three addresses, all different.** A source and its two fork children
///   each get their own network slot on the node. A constant, or a value copied
///   from a sibling, fails all three equalities at once.
/// * **two of the three carry no ownership marker.** A fork child driven by
///   this half reaches the node unmarked, and `ListSandboxes` leaves an
///   unmarked sandbox out entirely. The adopting replica answers for all three,
///   which is what says these facts did not come from that listing.
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

/// A machine that could not be asked and a machine that is not running the
/// sandbox are two different answers.
///
/// # 🔴 One value apart, and it is the status
///
/// Both faces reach the machine and both get a reply. In the first the machine
/// says it is running nothing under that id — an answer, and the one a delete
/// exists to act on: its whole job is to reconcile a record against a machine
/// that no longer has the sandbox, so failing there would leave such a record
/// undeletable. In the second the machine could not tell, and that is a
/// failure. Folding the second into the first is how a replica concludes a
/// running sandbox has no address because a node was busy — and, one caller up,
/// how a delete decides a VM is gone because nobody could answer.
///
/// 🔴 Deliberately not written as "shut the socket down": a dead socket fails
/// in `connect`, a round trip before the branch under test, so a build that
/// folded every status into "no such sandbox" would pass it.
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

/// An address the node did not read off a live handle is refused rather than
/// recorded.
///
/// 🔴 Two faces one bit apart, in the same reply shape: the node reports an
/// address and a rootfs size, and says whether it read them from the sandbox's
/// live handle or from its record. Its record holds neither field, so a `false`
/// there means both values are blanks wearing the shape of facts. The bit has
/// to be what decides: a build that took the values regardless would pass the
/// second face here and quietly record a busy sandbox's blanks as its address.
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

// ---------------------------------------------------------------------------
// The window between a create and the cluster hearing about it
// ---------------------------------------------------------------------------
//
// 🔴 What this section is for, stated once.
//
// A binding is the cluster's answer to *which machine is this sandbox on*.
// Nothing in this process used to write one. The gateway did — it read the node
// off the create it had just routed — and it stopped being able to the day
// user-facing REST began going to the API half, which is the deployment this
// module exists for. Its own comment says so and names this half as the owner
// of the write it gave up (`services/gateway/internal/rest_upstream.go`).
//
// The only thing left that wrote a binding was the node's heartbeat roster, one
// interval behind. While a sandbox is running that is invisible: the replica
// holding the handle never asks anyone where the sandbox is. A pause drops the
// handle, and from that instant every call has to ask — so a sandbox created and
// paused inside one heartbeat interval could be neither deleted nor resumed, and
// both answered 500 until a heartbeat landed.
//
// Every test below is written as faces differing in one value — whether the
// write happened, whether a heartbeat has landed, which machine the cluster
// names — because the failure is an *absence*, and an assertion with one face
// passes on a build that never asks anybody anything.

/// What a placement source answers when asked where a sandbox is.
#[derive(Clone)]
enum LookupAnswer {
    /// The binding store decides. A bound sandbox resolves to the node; an
    /// unbound one is `Ok(None)`, which is the scheduler's `NOT_FOUND`.
    FromBindings,
    /// The cluster names some other machine as the holder — a binding, a
    /// heartbeat roster, or a registry row in `running`/`resuming` that points
    /// somewhere else.
    Holder(NodeEndpoint),
    /// The placement source could not be consulted at all.
    Unavailable,
}

/// A stand-in for the cluster scheduler's placement surface.
///
/// 🔴 It has a binding store, and the store starts *empty* — which is the whole
/// point. `FixedNodePlacement` answers every question about every sandbox
/// without being told anything, so it can express neither the window this
/// section is about nor the write that closes it.
struct ClusterPlacement {
    node: NodeEndpoint,
    bindings: Mutex<std::collections::HashSet<crate::types::SandboxId>>,
    /// Whether `record_placement` writes a binding. Off is the world before
    /// this change: the call existed on the wire and nothing in `src/` made it.
    records: bool,
    /// Whether `record_placement` refuses. A cluster can say no, and a create
    /// must not fail because it did.
    record_fails: bool,
    lookup: LookupAnswer,
    /// What `resolve_node` answers, whatever it is asked about. `None` refuses.
    resolves: Option<NodeEndpoint>,
    recorded: Mutex<Vec<(crate::types::SandboxId, ExecutionId, NodeEndpoint)>>,
    resolve_calls: Mutex<usize>,
    /// What `node_membership` answers, whatever node it is asked about.
    /// `Present` by default: a node this test never told to leave the cluster
    /// has not left it.
    membership: Mutex<MembershipAnswer>,
}

/// What `ClusterPlacement::node_membership` answers.
///
/// 🔴 A separate type from `NodeMembership` rather than a reuse of it: this
/// one needs a third face — the placement source could not be asked at all —
/// that the production type deliberately has no variant for (it lives in
/// `Err` there instead, the same way `LookupAnswer::Unavailable` stands in for
/// `place_existing`'s `Err`).
#[derive(Clone, Copy)]
enum MembershipAnswer {
    Present,
    Gone,
    Unavailable,
}

impl ClusterPlacement {
    /// A cluster this half tells where sandboxes went.
    fn recording(node: NodeEndpoint) -> Arc<Self> {
        Arc::new(Self {
            bindings: Mutex::new(Default::default()),
            records: true,
            record_fails: false,
            lookup: LookupAnswer::FromBindings,
            resolves: Some(node.clone()),
            recorded: Mutex::new(Vec::new()),
            resolve_calls: Mutex::new(0),
            membership: Mutex::new(MembershipAnswer::Present),
            node,
        })
    }

    /// The same cluster, told nothing. This is the shape of the deployment the
    /// bug was reproduced on.
    fn silent(node: NodeEndpoint) -> Arc<Self> {
        let mut placement = Arc::try_unwrap(Self::recording(node))
            .ok()
            .expect("sole owner");
        placement.records = false;
        Arc::new(placement)
    }

    fn refusing_records(node: NodeEndpoint) -> Arc<Self> {
        let mut placement = Arc::try_unwrap(Self::recording(node))
            .ok()
            .expect("sole owner");
        placement.record_fails = true;
        Arc::new(placement)
    }

    fn answering(node: NodeEndpoint, lookup: LookupAnswer) -> Arc<Self> {
        let mut placement = Arc::try_unwrap(Self::silent(node))
            .ok()
            .expect("sole owner");
        placement.lookup = lookup;
        Arc::new(placement)
    }

    fn resolving_to(node: NodeEndpoint, resolves: Option<NodeEndpoint>) -> Arc<Self> {
        let mut placement = Arc::try_unwrap(Self::silent(node))
            .ok()
            .expect("sole owner");
        placement.resolves = resolves;
        Arc::new(placement)
    }

    /// The node's heartbeat roster landing: every sandbox it holds becomes
    /// bound. This is the repair path the cluster has always had, and the one
    /// interval of it is what the field reproduction measured.
    fn heartbeat(&self, ids: &[crate::types::SandboxId]) {
        let mut bindings = self.bindings.lock().expect("lock");
        for id in ids {
            bindings.insert(*id);
        }
    }

    fn is_bound(&self, id: crate::types::SandboxId) -> bool {
        self.bindings.lock().expect("lock").contains(&id)
    }

    fn recorded(&self) -> Vec<(crate::types::SandboxId, ExecutionId, NodeEndpoint)> {
        self.recorded.lock().expect("lock").clone()
    }

    fn resolve_calls(&self) -> usize {
        *self.resolve_calls.lock().expect("lock")
    }

    /// Tells this placement what to answer the next time it is asked whether
    /// its one node is still part of the cluster.
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
    ) -> anyhow::Result<NodeEndpoint> {
        Ok(self.node.clone())
    }

    async fn place_existing(
        &self,
        sandbox_id: crate::types::SandboxId,
    ) -> anyhow::Result<Option<NodeEndpoint>> {
        match &self.lookup {
            LookupAnswer::FromBindings => Ok(self.is_bound(sandbox_id).then(|| self.node.clone())),
            LookupAnswer::Holder(holder) => Ok(Some(holder.clone())),
            LookupAnswer::Unavailable => {
                anyhow::bail!("the scheduler could not locate {sandbox_id}: unavailable")
            }
        }
    }

    async fn resolve_node(&self, node_id: &str) -> anyhow::Result<NodeEndpoint> {
        *self.resolve_calls.lock().expect("lock") += 1;
        self.resolves
            .clone()
            .ok_or_else(|| anyhow::anyhow!("the scheduler could not say where node {node_id} is"))
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
    ) -> anyhow::Result<()> {
        self.recorded
            .lock()
            .expect("lock")
            .push((sandbox_id, execution_id, node.clone()));
        if self.record_fails {
            anyhow::bail!("the scheduler refused an assignment for {sandbox_id}");
        }
        if self.records {
            self.bindings.lock().expect("lock").insert(sandbox_id);
        }
        Ok(())
    }
}

/// One replica of the deciding half, over a placement source a test controls.
async fn api_replica_on(placement: Arc<ClusterPlacement>, ledger: &SharedLedger) -> ApiReplica {
    Orchestrator::new(
        ServerRole::All,
        ledger.clone(),
        RemoteSandboxBackendFactory::new(placement as Arc<dyn NodePlacement>),
        DisabledSandboxPersister,
        crate::image::DisabledRuntimeImageRefs::shared(),
    )
    .await
    .expect("a replica of the deciding half")
}

/// A create tells the cluster which machine the sandbox went to, with the
/// address the cluster names that machine by.
///
/// 🔴 The address is the load-bearing half and it is asserted against its own
/// control. A `NodeEndpoint` carries two: the one this process dials, which is
/// the node service's port, and the one the placement source named, which is
/// where user traffic goes. The scheduler compares the address on a
/// `RecordAssignment` byte-for-byte against its discovery entry
/// (`AtomicNodeRegistry.Contains`) and refuses anything else — so a write that
/// carried the dialled address would be rejected on every single create, and
/// rejected as *an unknown node*, which reads like a discovery fault. The two
/// addresses differ here on purpose, and both are asserted.
///
/// 🔴 Two sandboxes, so the incarnation is a control rather than a constant: a
/// build that recorded a fixed value, an empty string, or the first sandbox's
/// run for both would agree with itself and fail this.
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
        let (recorded_id, recorded_execution, recorded_node) = &recorded[index];
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

/// A cluster that refuses the assignment does not cost the user their sandbox.
///
/// 🔴 The two faces differ in exactly one value — whether the placement source
/// accepts the write — and both must produce a running sandbox. The refusal is
/// *reached* in the first face, which is what stops this passing on a build that
/// simply stopped making the call: an untried write and a rejected one both
/// leave the binding absent, and only the attempt count tells them apart.
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

    // 🔴 And both VMs are up on the machine. Without this the test passes on a
    // build where `start` returned `Ok` without creating anything.
    assert_eq!(running_on(&node).await.len(), 2, "a create started no VM");
}

/// A resume tells the cluster the sandbox is live again, under the run it woke
/// it as.
///
/// 🔴 The control is the paused run, which is in scope and is the value a build
/// that recorded the wrong incarnation would most plausibly record: it is the
/// one the resume request carries as its fence. Recording it would point the
/// gateway's fencing at a run that is over.
#[tokio::test]
async fn a_resume_tells_the_cluster_the_sandbox_is_live_again() {
    let node = real_node().await;
    let placement = ClusterPlacement::recording(node.endpoint.clone());
    let factory =
        RemoteSandboxBackendFactory::new(Arc::clone(&placement) as Arc<dyn NodePlacement>);

    let paused_execution_id = ExecutionId::new();
    let config = launch_config();
    let sandbox_id = config.sandbox_id;
    let mut backend = factory
        .build_from_snapshot(&RunnableSnapshot::mock(), config, paused_execution_id)
        .expect("build a stub");
    backend.start().await.expect("start on the node");
    let capture = backend.pause(None, false).await.expect("pause on the node");
    backend.stop().await.expect("stop after the pause");

    let restored = factory
        .decode_paused_state(
            std::path::PathBuf::from("/ignored"),
            capture.state.encode().expect("encode"),
        )
        .expect("decode");
    let resumed_execution_id = ExecutionId::new();
    assert_ne!(resumed_execution_id, paused_execution_id);
    let mut back = factory
        .build_from_paused_state(sandbox_id, resumed_execution_id, restored.as_ref(), None)
        .expect("build a stub");
    back.start().await.expect("reopen on the node that has it");

    let recorded = placement.recorded();
    assert_eq!(
        recorded.len(),
        2,
        "the create and the resume did not both tell the cluster: {recorded:?}"
    );
    assert_eq!(recorded[0].1, paused_execution_id, "the create's run");
    assert_eq!(
        recorded[1].1, resumed_execution_id,
        "the resume told the cluster about a run other than the one it started"
    );
    assert_ne!(
        recorded[1].1, paused_execution_id,
        "the resume recorded the run it reopened instead of the run it started"
    );
}

/// A sandbox created and paused inside one heartbeat interval can still be
/// deleted.
///
/// This is the reproduction, with the heartbeat made explicit instead of waited
/// for. On the cluster it read: a delete 0.2 seconds after a pause answered 500,
/// and the same delete on the same sandbox a minute later answered 204.
///
/// 🔴 Three faces over one value — what the cluster has been told. The second is
/// the world before this change and must still fail, or the first passes on a
/// build that has stopped consulting placement at all; the third is the same
/// world one heartbeat later and must succeed, which is what proves the second
/// face failed over the *absence* rather than over the sandbox.
#[tokio::test]
async fn a_sandbox_can_be_deleted_before_the_cluster_has_heard_of_it() {
    let node = real_node().await;
    let on_the_node = node.orchestration.as_ref().expect("a real node").clone();
    let ledger = SharedLedger(Arc::new(InMemoryMetadataStore::new()));

    // Face 1: the write this change adds. No heartbeat anywhere in this face.
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
    Arc::clone(&replica)
        .pause_sandbox(sandbox.id)
        .await
        .expect("pause");
    Arc::clone(&replica)
        .delete_sandbox(sandbox.id)
        .await
        .expect("🔴 a sandbox paused before its first heartbeat could not be deleted");
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

    // Face 2: the same three calls against a cluster nothing tells. This is
    // exactly what shipped, and it must still refuse.
    let untold = ClusterPlacement::silent(node.endpoint.clone());
    let replica = api_replica_on(Arc::clone(&untold), &ledger).await;
    let stranded = Arc::clone(&replica)
        .create_sandbox(cluster_create_request())
        .await
        .expect("create on the node");
    assert!(
        !untold.is_bound(stranded.id),
        "the silent face bound the sandbox anyway, so it is not the control it claims to be"
    );
    Arc::clone(&replica)
        .pause_sandbox(stranded.id)
        .await
        .expect("pause");
    let err = Arc::clone(&replica)
        .delete_sandbox(stranded.id)
        .await
        .expect_err("a delete completed against a cluster that could not name the machine");
    assert!(
        format!("{err}").contains("could not be reached") || format!("{err}").contains("no record"),
        "the refusal did not say why: {err}"
    );
    assert_eq!(
        ledger
            .0
            .get(&stranded.id)
            .await
            .expect("read the ledger")
            .expect("🔴 a delete that reached no machine forgot the sandbox anyway")
            .state,
        crate::orchestrator::SandboxState::Paused,
        "a refused delete left the record somewhere other than where it found it"
    );

    // Face 3: one heartbeat later, nothing else changed. The same delete on the
    // same sandbox through the same replica now succeeds — which is what says
    // face 2 failed over the missing binding and not over the sandbox.
    untold.heartbeat(&[stranded.id]);
    Arc::clone(&replica)
        .delete_sandbox(stranded.id)
        .await
        .expect("a heartbeat landed and the delete still could not reach the machine");
    assert!(
        on_the_node
            .get_sandbox(&stranded.id)
            .await
            .expect("the node's own record")
            .is_none(),
        "the delete answered success without telling the machine"
    );
}

/// A delete forgets a running sandbox's record once the node holding it has
/// left the cluster — and refuses, exactly as before this change, for as long
/// as the cluster still lists it.
///
/// # 🔴 Three faces over one value: what the placement source says about the
/// node's own membership once a delete's dial to it fails
///
/// * **Face 1 — still listed.** A node whose registry entry is merely stale
///   is not a node that is gone: it may yet report back on its own, and a
///   single failed dial is not proof otherwise. This is today's behaviour —
///   the world before this test's fix — and it is the regression guard: a
///   build that started forgetting on *any* dial failure, not only a
///   confirmed-gone one, passes every other face here and fails this one.
/// * **Face 2 — evicted.** The node's own record is gone from the registry:
///   explicitly unregistered, or dropped once discovery stopped listing it.
///   Nothing on it is coming back to report anything, so the sandbox's record
///   may be forgotten even though the machine itself was never reached again.
/// * **Face 3 — unanswerable.** The membership question itself could not be
///   asked. This must land exactly on face 1's answer: not knowing whether a
///   node is gone is never licence to conclude that it is.
///
/// The node is shut down once, before any of the three deletes: every face is
/// about what the *placement source* says, not about a dial that might
/// happen to succeed.
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

/// A capture is reopened on the machine holding it when the cluster has no
/// record of the sandbox — and on no other machine, for any other answer.
///
/// 🔴 This is the fencing argument, asserted. The fallback is entered on
/// *absence* only, and the three faces that must still refuse are the three
/// shapes of "not absence": the cluster names another holder, the cluster could
/// not be consulted, and the fallback itself answered about another machine.
/// Each refusal is checked against the node running nothing afterwards, and each
/// wrong answer carries the *real* node's address — so a build that dropped the
/// identity check would not merely fail to refuse, it would succeed, and these
/// assertions would go red rather than staying silent.
///
/// The three refusals come before the success on purpose: "the node is running
/// nothing" is also what a build that can reopen nothing at all looks like.
#[tokio::test]
async fn a_capture_is_reopened_on_its_own_machine_when_the_cluster_has_no_record() {
    let node = real_node().await;
    let setup = RemoteSandboxBackendFactory::new(node.placement());

    let paused_execution_id = ExecutionId::new();
    let config = launch_config();
    let sandbox_id = config.sandbox_id;
    let mut backend = setup
        .build_from_snapshot(&RunnableSnapshot::mock(), config, paused_execution_id)
        .expect("build a stub");
    backend.start().await.expect("start on the node");
    let capture = backend.pause(None, false).await.expect("pause on the node");
    backend.stop().await.expect("stop after the pause");
    let encoded = capture.state.encode().expect("encode");
    assert_eq!(encoded["origin_node_id"], node.endpoint.node_id.as_str());
    assert!(
        running_on(&node).await.is_empty(),
        "the pause left the VM running"
    );

    let resumed_execution_id = ExecutionId::new();
    let reopen = |placement: Arc<ClusterPlacement>| {
        let encoded = encoded.clone();
        async move {
            let factory = RemoteSandboxBackendFactory::new(placement as Arc<dyn NodePlacement>);
            let state = factory
                .decode_paused_state(std::path::PathBuf::from("/ignored"), encoded)
                .expect("decode");
            let mut backend = factory
                .build_from_paused_state(sandbox_id, resumed_execution_id, state.as_ref(), None)
                .expect("build a stub");
            backend.start().await
        }
    };

    // A machine that is not the origin, wearing the origin's *address*: if the
    // identity check went, this would be reopened successfully.
    let impostor = NodeEndpoint {
        node_id: "node-that-holds-nothing".to_string(),
        endpoint: node.endpoint.endpoint.clone(),
        advertised_endpoint: node.endpoint.advertised_endpoint.clone(),
    };

    // Face 1: the cluster names another holder. An answer, so it is refused.
    let named_elsewhere = ClusterPlacement::answering(
        node.endpoint.clone(),
        LookupAnswer::Holder(impostor.clone()),
    );
    reopen(Arc::clone(&named_elsewhere))
        .await
        .expect_err("a capture was reopened although the cluster named another holder");
    assert!(
        running_on(&node).await.is_empty(),
        "a refused resume started something anyway"
    );

    // Face 2: the cluster could not be consulted. Not an absence, so not a
    // fallback — an error.
    let unavailable = ClusterPlacement::answering(node.endpoint.clone(), LookupAnswer::Unavailable);
    reopen(Arc::clone(&unavailable))
        .await
        .expect_err("a capture was reopened although the placement source could not be asked");
    assert_eq!(
        unavailable.resolve_calls(),
        0,
        "an unreadable placement source was treated as an absent record"
    );
    assert!(running_on(&node).await.is_empty());

    // Face 3: no record, and the fallback answers about a different machine.
    let misdirected = ClusterPlacement::resolving_to(node.endpoint.clone(), Some(impostor.clone()));
    reopen(Arc::clone(&misdirected))
        .await
        .expect_err("a capture was reopened on a machine the fallback misnamed");
    assert_eq!(
        misdirected.resolve_calls(),
        1,
        "the fallback was not reached, so the refusal above is about something else"
    );
    assert!(running_on(&node).await.is_empty());

    // Face 4: no record, and the fallback answers about the machine the capture
    // names. The one case the fallback exists for.
    let absent = ClusterPlacement::silent(node.endpoint.clone());
    reopen(Arc::clone(&absent))
        .await
        .expect("🔴 a capture could not be reopened because the cluster had never heard of it");
    assert_eq!(
        absent.resolve_calls(),
        1,
        "the resume did not go through the fallback"
    );
    let live = node
        .orchestration
        .as_ref()
        .expect("a real node")
        .list_live_sandboxes()
        .await
        .expect("list");
    assert_eq!(live.len(), 1, "the sandbox did not come back: {live:?}");
    assert_eq!(live[0].sandbox_id, sandbox_id);
    assert_eq!(live[0].execution_id, Some(resumed_execution_id));

    // 🔴 And the fallback belongs to the resume alone. The same placement
    // source, the same missing binding, and an attach — which holds a sandbox
    // id and nothing that names a machine — must refuse rather than reach for
    // it. The resolve count is the evidence, and it is meaningful because the
    // face above pushed it to one in this same round.
    let attaching = setup_attach(Arc::clone(&absent), sandbox_id, resumed_execution_id).await;
    assert!(
        attaching.is_err(),
        "an attach invented a machine for a sandbox the cluster could not place"
    );
    assert_eq!(
        absent.resolve_calls(),
        1,
        "an attach took the resume's fallback"
    );
}

/// Starts an attaching stub — what a delete, a pause or a snapshot builds when
/// the handle is not in this process — and reports what `start` said.
async fn setup_attach(
    placement: Arc<ClusterPlacement>,
    sandbox_id: crate::types::SandboxId,
    execution_id: ExecutionId,
) -> anyhow::Result<()> {
    let factory = RemoteSandboxBackendFactory::new(placement as Arc<dyn NodePlacement>);
    let mut backend = factory
        .adopt_running(sandbox_id, execution_id, Default::default())
        .expect("a remote factory adopts every sandbox")
        .expect("a remote factory never answers that the runtime is gone");
    backend.start().await
}
