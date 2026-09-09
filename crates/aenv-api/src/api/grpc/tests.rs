//! Exercises resume transport metadata and trailers over a real socket.
//!
//! Refusal cases are paired with admitted cases to avoid vacuous tests.

use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::oneshot;
use tonic::Code;

use crate::api::impls::{PlacedNode, PlacementRefusal, ResumePlacement, ResumePlacementSource};
use crate::api::{ApiImpl, ResumeWiring};
use crate::cfg::ConfigManager;
use crate::node_registry::grpc_service::NodeRegistryGrpcService;
use crate::node_registry::registry::{AtomicNodeRegistry, NodeRegistry};
use crate::node_registry::warmup::WarmupGate;
use aenv_node::orchestrator::Orchestrator;

use crate::orchestrator::{SandboxMetadata, SandboxState};
use crate::proto::apiproxy::{
    self as pb, sandbox_resume_service_client::SandboxResumeServiceClient,
};
use crate::sandbox::mock::MockBackendFactory;
use crate::snapshot::mock::{
    in_memory_snapshot_manager, mock_paused_sandbox_config, paused_sandbox_record,
    InMemorySnapshotCatalog,
};
use crate::snapshot::PausedSandboxConfig;
use crate::types::{ExecutionId, SandboxId};

/// Node on which the process under test runs sandboxes when it is a node.
/// A different node the placement source may name.
const OTHER_NODE: &str = "node-elsewhere";

struct StubPlacement(Result<ResumePlacement, PlacementRefusal>);

#[async_trait]
impl ResumePlacementSource for StubPlacement {
    async fn locate(&self, _sandbox_id: SandboxId) -> Result<ResumePlacement, PlacementRefusal> {
        self.0.clone()
    }

    async fn address_of(&self, node_id: &str) -> Option<String> {
        Some(address_of(node_id))
    }
}

fn address_of(node_id: &str) -> String {
    format!("http://{node_id}:8000")
}

fn placed(node_id: &str) -> PlacedNode {
    PlacedNode {
        node_id: node_id.to_string(),
        address: address_of(node_id),
    }
}

fn running_on(node_id: &str, execution_id: ExecutionId) -> ResumePlacement {
    ResumePlacement::Running {
        node: placed(node_id),
        execution_id,
        from_projection: true,
    }
}

fn api_half(answer: Result<ResumePlacement, PlacementRefusal>) -> ResumeWiring {
    ResumeWiring::new(Some(Arc::new(StubPlacement(answer))), None)
}

struct BuiltApi {
    api: Arc<ApiImpl>,
    catalog: Arc<InMemorySnapshotCatalog>,
}

async fn build_api(resume_wiring: ResumeWiring) -> BuiltApi {
    let orchestrator = Orchestrator::with_in_memory_store(MockBackendFactory::new()).await;
    let (snapshot_manager, catalog) = in_memory_snapshot_manager();
    let api = Arc::new(ApiImpl::new(
        orchestrator,
        Arc::new(snapshot_manager),
        None,
        Vec::new(),
        resume_wiring,
    ));
    BuiltApi { api, catalog }
}

struct RunningApi {
    addr: SocketAddr,
    api: Arc<ApiImpl>,
    catalog: Arc<InMemorySnapshotCatalog>,
    _shutdown: oneshot::Sender<()>,
}

impl RunningApi {
    async fn client(&self) -> SandboxResumeServiceClient<tonic::transport::Channel> {
        SandboxResumeServiceClient::connect(format!("http://{}", self.addr))
            .await
            .expect("connect to the wake-up surface")
    }

    async fn resume(
        &self,
        sandbox_id: &str,
        target_port: Option<&str>,
        access_token: Option<&str>,
    ) -> Result<pb::SandboxResumeResponse, tonic::Status> {
        let mut request = tonic::Request::new(pb::SandboxResumeRequest {
            sandbox_id: sandbox_id.to_string(),
        });
        if let Some(port) = target_port {
            request.metadata_mut().insert(
                pb::TARGET_PORT_METADATA,
                port.parse().expect("a header-safe port"),
            );
        }
        if let Some(token) = access_token {
            request.metadata_mut().insert(
                pb::ACCESS_TOKEN_METADATA,
                token.parse().expect("a header-safe token"),
            );
        }
        self.client()
            .await
            .resume_sandbox(request)
            .await
            .map(|response| response.into_inner())
    }

    /// Seeds a paused row for a fresh sandbox and returns its id.
    fn paused(&self, paused: PausedSandboxConfig) -> SandboxId {
        let sandbox_id = SandboxId::new();
        self.catalog.seed(paused_sandbox_record(
            sandbox_id,
            Some(OTHER_NODE),
            paused,
            1_700_000_000_000,
        ));
        sandbox_id
    }

    async fn running(&self, auto_resume: bool) -> SandboxId {
        let sandbox_id = SandboxId::new();
        // A running record, not a proxy route: nothing in this half serves one.
        self.api
            .orchestrator()
            .set_metadata_state_for_test(sandbox_id, SandboxState::Running)
            .await
            .expect("seed a running sandbox");
        self.api
            .orchestrator()
            .set_auto_resume_for_test(&sandbox_id, auto_resume)
            .await
            .expect("set the flag on a running sandbox");
        sandbox_id
    }

    async fn state_of(&self, sandbox_id: SandboxId) -> Option<SandboxState> {
        self.api
            .orchestrator()
            .get_sandbox(&sandbox_id)
            .await
            .expect("the store answers")
            .map(|metadata| metadata.state)
    }
}

fn dummy_node_registry_service() -> NodeRegistryGrpcService {
    let registry = Arc::new(AtomicNodeRegistry::new(
        Vec::new(),
        std::time::Duration::from_secs(30),
    ));
    let warmup = Arc::new(WarmupGate::new(
        Arc::clone(&registry) as Arc<dyn NodeRegistry>,
        std::time::Duration::from_secs(15),
        std::time::SystemTime::now(),
    ));
    NodeRegistryGrpcService::new(registry, warmup)
}

async fn serve_api(resume_wiring: ResumeWiring) -> RunningApi {
    crate::logging::init_for_tests();
    let BuiltApi { api, catalog } = build_api(resume_wiring).await;

    // Bind before spawning so the port cannot be taken by another test.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a port");
    let addr: SocketAddr = listener.local_addr().expect("the bound address");
    let (tx, rx) = oneshot::channel();

    let served = Arc::clone(&api);
    tokio::spawn(async move {
        let _ = super::serve_on(listener, served, dummy_node_registry_service(), async {
            let _ = rx.await;
        })
        .await;
    });

    // Wait until the spawned accept loop is ready.
    for _ in 0..200 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    RunningApi {
        addr,
        api,
        catalog,
        _shutdown: tx,
    }
}

fn refusal_reason(status: &tonic::Status) -> Option<&str> {
    status
        .metadata()
        .get(pb::REFUSAL_REASON_TRAILER)
        .and_then(|value| value.to_str().ok())
}

#[tokio::test]
async fn a_placement_that_names_a_running_node_is_answered_as_it_stands() {
    let execution_id = ExecutionId::new();
    let api = serve_api(api_half(Ok(running_on(OTHER_NODE, execution_id)))).await;
    let sandbox_id = SandboxId::new();

    let woken = api
        .resume(&sandbox_id.to_string(), None, None)
        .await
        .expect("a running sandbox is a successful wake-up, not a conflict");

    assert_eq!(woken.node_id, OTHER_NODE);
    assert_eq!(woken.node_address, address_of(OTHER_NODE));
    assert_eq!(
        woken.execution_id,
        execution_id.to_string(),
        "the incarnation the placement vouches for is what fences the forwarded request"
    );
    assert_eq!(
        api.state_of(sandbox_id).await,
        None,
        "nothing was created here: the answer came from the placement alone"
    );

    let unplaced = serve_api(api_half(Ok(ResumePlacement::NotRunning))).await;
    assert_eq!(
        unplaced
            .resume(&sandbox_id.to_string(), None, None)
            .await
            .expect_err("nothing runs it and no row describes it")
            .code(),
        Code::NotFound,
        "the placement above is what made the sandbox exist; without it the same id is unknown"
    );
}

#[tokio::test]
async fn an_unreachable_placement_source_is_unavailable_and_a_missing_sandbox_is_not_found() {
    let sandbox_id = SandboxId::new().to_string();

    let unreachable = serve_api(api_half(Err(PlacementRefusal::Unavailable(
        "scheduler is still seeding sandbox assignments".to_string(),
    ))))
    .await;
    let undecided = unreachable
        .resume(&sandbox_id, None, None)
        .await
        .expect_err("nobody could be asked");
    assert_eq!(
        undecided.code(),
        Code::Unavailable,
        "'nobody could be asked' is not an answer about whether the sandbox exists; \
         answering NotFound here tells the platform to rebuild a sandbox that may be \
         perfectly alive"
    );

    let broken = serve_api(api_half(Err(PlacementRefusal::Failed(
        "the placement source answered nonsense".to_string(),
    ))))
    .await;
    assert_eq!(
        broken
            .resume(&sandbox_id, None, None)
            .await
            .expect_err("the placement source failed")
            .code(),
        Code::Internal
    );

    let known_absent = serve_api(api_half(Ok(ResumePlacement::NotRunning))).await;
    let absent = known_absent
        .resume(&sandbox_id, None, None)
        .await
        .expect_err("nothing runs it and no row describes it");
    assert_eq!(
        absent.code(),
        Code::NotFound,
        "a sandbox that is neither running nor paused anywhere is gone, and softening \
         that into a retry would retry a genuinely dead sandbox forever"
    );
}

#[tokio::test]
async fn a_malformed_sandbox_id_is_invalid_argument_and_a_well_formed_one_reaches_the_decision() {
    let api = serve_api(api_half(Ok(ResumePlacement::NotRunning))).await;

    let malformed = api
        .resume("not-a-sandbox-id", None, None)
        .await
        .expect_err("a malformed id cannot name a sandbox");
    assert_eq!(
        malformed.code(),
        Code::InvalidArgument,
        "answering NotFound would tell the gateway a sandbox is gone that it never \
         named, and the gateway's NotFound handling rebuilds from template"
    );

    let well_formed = api
        .resume(&SandboxId::new().to_string(), None, None)
        .await
        .expect_err("no such sandbox");
    assert_eq!(
        well_formed.code(),
        Code::NotFound,
        "a syntactically valid id gets past parsing and reaches the wake-up decision, \
         which is what makes the InvalidArgument above a statement about the id rather \
         than about the surface"
    );
}

#[tokio::test]
async fn the_envd_credential_check_refuses_what_it_must_and_admits_what_it_must() {
    let api = serve_api(api_half(Ok(ResumePlacement::NotRunning))).await;
    let secure = || PausedSandboxConfig {
        secure: true,
        ..mock_paused_sandbox_config()
    };
    let token_of = |sandbox_id: SandboxId| {
        api.api
            .orchestrator()
            .get_envd_access_token(&SandboxMetadata {
                id: sandbox_id,
                secure: true,
                ..Default::default()
            })
            .expect("a secure sandbox has a token")
    };
    let envd_port = ConfigManager::global_config()
        .tools
        .control_plane_port
        .to_string();
    let other_port = (ConfigManager::global_config().tools.control_plane_port + 1).to_string();

    let refused_id = api.paused(secure());
    let id = refused_id.to_string();
    for (port, token, what) in [
        (
            Some(envd_port.as_str()),
            None,
            "the envd port with no token",
        ),
        (
            Some(envd_port.as_str()),
            Some("not-the-token"),
            "the envd port with the wrong token",
        ),
        (
            Some("banana"),
            None,
            "an unparseable port with no token, the hole a caller would use to skip \
             the check entirely",
        ),
        (None, None, "no port and no token"),
    ] {
        let status = api
            .resume(&id, port, token)
            .await
            .expect_err(&format!("{what} must be refused"));
        assert_eq!(
            status.code(),
            Code::PermissionDenied,
            "{what} must be PermissionDenied"
        );
    }
    assert_eq!(
        api.state_of(refused_id).await,
        None,
        "a refused wake-up must not have built anything"
    );

    let with_token = api.paused(secure());
    let token = token_of(with_token);
    api.resume(
        &with_token.to_string(),
        Some(&envd_port),
        Some(token.expose()),
    )
    .await
    .expect("the envd port with the sandbox's own token wakes it");

    let other_port_no_token = api.paused(secure());
    api.resume(&other_port_no_token.to_string(), Some(&other_port), None)
        .await
        .expect("a port that is not envd's is not covered by the check");

    let insecure = api.paused(mock_paused_sandbox_config());
    api.resume(&insecure.to_string(), Some(&envd_port), None)
        .await
        .expect("a sandbox without a token has nothing to present");

    for sandbox_id in [with_token, other_port_no_token, insecure] {
        assert_eq!(
            api.state_of(sandbox_id).await,
            Some(SandboxState::Running),
            "an admitted wake-up rebuilt the sandbox"
        );
    }
}

#[test]
fn every_outcome_of_the_wake_up_surface_is_published_before_the_first_request() {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    // The recorder is thread-local, so describe metrics synchronously.
    metrics::with_local_recorder(&recorder, super::resume::describe_metrics);

    let mut published: Vec<String> = snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .filter_map(|(composite, _unit, _description, value)| {
            let key = composite.key();
            if key.name() != "agentenv_api_resume_grpc_total" {
                return None;
            }
            assert!(
                matches!(value, DebugValue::Counter(0)),
                "a pre-published series must start at 0, not at {value:?}"
            );
            key.labels()
                .find(|label| label.key() == "result")
                .map(|label| label.value().to_string())
        })
        .collect();
    published.sort();

    let mut expected = vec![
        "ok",
        "invalid_argument",
        "permission_denied",
        "not_found",
        "transition_in_progress",
        "auto_resume_disabled",
        "resource_exhausted",
        "unavailable",
        "internal",
        "timed_out",
    ];
    expected.sort_unstable();

    assert_eq!(
        published, expected,
        "every outcome this surface can record must exist as a zeroed series \
         before the first request, or the acceptance probe cannot tell 0 from absent"
    );
}

#[tokio::test]
async fn the_metric_label_is_the_same_string_the_refusal_trailer_carries() {
    use crate::proto::apiproxy::sandbox_resume_service_server::SandboxResumeService as ServiceTrait;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    struct Observed {
        moved: Vec<(String, u64)>,
        outcome: Result<pb::SandboxResumeResponse, tonic::Status>,
    }

    async fn observe(seed: impl FnOnce(&BuiltApi) -> SandboxId) -> Observed {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        // Keep the thread-local recorder and service future on this test thread.
        let guard = metrics::set_default_local_recorder(&recorder);

        let built = build_api(api_half(Ok(ResumePlacement::NotRunning))).await;
        let sandbox_id = seed(&built);
        let service = super::resume::SandboxResumeService::new(Arc::clone(&built.api));
        let outcome = ServiceTrait::resume_sandbox(
            &service,
            tonic::Request::new(pb::SandboxResumeRequest {
                sandbox_id: sandbox_id.to_string(),
            }),
        )
        .await
        .map(|response| response.into_inner());
        drop(guard);

        let mut moved = Vec::new();
        for (composite, _unit, _description, value) in snapshotter.snapshot().into_vec() {
            let key = composite.key();
            if key.name() != "agentenv_api_resume_grpc_total" {
                continue;
            }
            if let DebugValue::Counter(count) = value {
                if count == 0 {
                    continue;
                }
                if let Some(label) = key.labels().find(|label| label.key() == "result") {
                    moved.push((label.value().to_string(), count));
                }
            }
        }
        Observed { moved, outcome }
    }

    let refused = observe(|built| {
        let sandbox_id = SandboxId::new();
        built.catalog.seed(paused_sandbox_record(
            sandbox_id,
            Some(OTHER_NODE),
            PausedSandboxConfig {
                auto_resume: false,
                ..mock_paused_sandbox_config()
            },
            1_700_000_000_000,
        ));
        sandbox_id
    })
    .await;
    let status = refused
        .outcome
        .expect_err("a row with auto-resume off refuses traffic-triggered wake-ups");
    let reason = refusal_reason(&status)
        .expect("the refusal names its reason in the trailer")
        .to_string();
    assert_eq!(
        refused.moved,
        vec![(reason, 1)],
        "the label must be the refusal's own wire spelling, the same string that \
         travelled in the trailer, or the gateway's log and this scrape cannot be joined"
    );

    let woken = observe(|built| {
        let sandbox_id = SandboxId::new();
        built.catalog.seed(paused_sandbox_record(
            sandbox_id,
            Some(OTHER_NODE),
            mock_paused_sandbox_config(),
            1_700_000_000_000,
        ));
        sandbox_id
    })
    .await;
    woken.outcome.expect("a resumable row wakes");
    assert_eq!(
        woken.moved,
        vec![("ok".to_string(), 1)],
        "and a wake-up that worked must record exactly one success: without this \
         the assertion above would also hold for a build that recorded a refusal \
         for everything"
    );
}

#[tokio::test]
async fn a_paused_sandbox_with_auto_resume_off_is_refused_and_the_other_two_are_not() {
    let api = serve_api(api_half(Ok(ResumePlacement::NotRunning))).await;

    let refused_id = api.paused(PausedSandboxConfig {
        auto_resume: false,
        ..mock_paused_sandbox_config()
    });
    let status = api
        .resume(&refused_id.to_string(), None, None)
        .await
        .expect_err("a paused sandbox with auto-resume off must not wake on traffic");
    assert_eq!(
        status.code(),
        Code::FailedPrecondition,
        "not NotFound: the sandbox exists and still resumes through the REST route. \
         The gateway turns NotFound into a 404, which the platform reads as 'rebuild \
         it from its template', and that resets the user's workspace"
    );
    assert_eq!(
        refusal_reason(&status),
        Some("auto_resume_disabled"),
        "the reason is what the gateway keys its 410 off; a FailedPrecondition with \
         any other reason is a 503, which advertises 'try again' for a sandbox that \
         is never going to answer"
    );
    assert_eq!(
        api.state_of(refused_id).await,
        None,
        "the refusal must have built nothing"
    );

    let woken_id = api.paused(PausedSandboxConfig {
        auto_resume: true,
        user_metadata: Some([("owner".to_string(), "row".to_string())].into()),
        ..mock_paused_sandbox_config()
    });
    let woken = api
        .resume(&woken_id.to_string(), None, None)
        .await
        .expect("a sandbox with the flag on wakes from its row");
    assert!(!woken.execution_id.is_empty());
    let rebuilt = api
        .api
        .orchestrator()
        .get_sandbox(&woken_id)
        .await
        .expect("the store answers")
        .expect("the wake-up rebuilt the sandbox under its own id");
    assert_eq!(rebuilt.state, SandboxState::Running);
    assert_eq!(rebuilt.execution_id.to_string(), woken.execution_id);
    assert_eq!(
        rebuilt.user_metadata,
        Some([("owner".to_string(), "row".to_string())].into()),
        "the rebuilt sandbox carries the configuration its row paused with"
    );

    // The flag governs starting a sandbox, not one already running.
    let running_id = api.running(false).await;
    let already = api
        .resume(&running_id.to_string(), None, None)
        .await
        .expect("an already-running sandbox is a success regardless of the flag");
    assert!(!already.execution_id.is_empty());
}

#[tokio::test]
async fn a_wake_up_during_a_pause_waits_for_it_and_rebuilds_from_the_row() {
    let api = serve_api(api_half(Ok(ResumePlacement::NotRunning))).await;
    let pausing = SandboxId::new();
    api.catalog.seed(paused_sandbox_record(
        pausing,
        Some(OTHER_NODE),
        mock_paused_sandbox_config(),
        1_700_000_000_000,
    ));
    api.api
        .orchestrator()
        .set_metadata_state_for_test(pausing, SandboxState::Pausing)
        .await
        .expect("seed a sandbox mid-pause");
    let orchestrator = api.api.orchestrator();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        orchestrator
            .remove_sandbox_for_test(&pausing)
            .await
            .expect("the pause finishes by forgetting the record");
    });

    api.resume(&pausing.to_string(), None, None)
        .await
        .expect("the wake-up waits for the pause and rebuilds from its row");
    assert_eq!(
        api.state_of(pausing).await,
        Some(SandboxState::Running),
        "the pause finished first; what answered was a rebuild, not the pausing VM"
    );
}

#[tokio::test]
async fn a_sandbox_mid_snapshot_is_refused_and_one_being_killed_is_gone() {
    let api = serve_api(api_half(Ok(ResumePlacement::NotRunning))).await;
    let snapshotting = SandboxId::new();
    api.api
        .orchestrator()
        .set_metadata_state_for_test(snapshotting, SandboxState::Snapshotting)
        .await
        .expect("seed a sandbox mid-snapshot");

    let status = api
        .resume(&snapshotting.to_string(), None, None)
        .await
        .expect_err("a sandbox mid-snapshot cannot be woken yet");
    assert_eq!(status.code(), Code::FailedPrecondition);
    assert_eq!(
        refusal_reason(&status),
        Some("transition_in_progress"),
        "the gateway backs off briefly on this reason and gives up on the other"
    );

    let killing = SandboxId::new();
    api.api
        .orchestrator()
        .set_metadata_state_for_test(killing, SandboxState::Killing)
        .await
        .expect("seed a sandbox being killed");
    assert_eq!(
        api.resume(&killing.to_string(), None, None)
            .await
            .expect_err("a sandbox being killed is gone")
            .code(),
        Code::NotFound
    );
}
