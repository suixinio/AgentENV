//! Exercises resume transport metadata and trailers over a real socket.
//!
//! Refusal cases are paired with admitted cases to avoid vacuous tests.

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::oneshot;
use tonic::Code;

use crate::api::impls::{
    PlacedNode, PlacementRefusal, ResumePlacement, ResumePlacementSource, WakeSite,
};
use crate::api::{ApiImpl, PausedSandboxWiring, ResumeWiring};
use crate::cfg::ConfigManager;
use crate::identity::NodeIdentity;
use crate::node_registry::grpc_service::NodeRegistryGrpcService;
use crate::node_registry::registry::{AtomicNodeRegistry, NodeRegistry};
use crate::node_registry::warmup::WarmupGate;
use crate::orchestrator::{
    DisabledPausedSandboxRegistry, DisabledSandboxPersister, InMemoryMetadataStore, Orchestrator,
    ProxyTarget, SandboxState,
};
use crate::proto::apiproxy::{
    self as pb, sandbox_resume_service_client::SandboxResumeServiceClient,
};
use crate::sandbox::mock::MockBackendFactory;
use crate::snapshot::mock::mock_snapshot_manager;
use crate::types::SandboxId;

/// Node on which the process under test wakes sandboxes.
const THIS_NODE: &str = "node-under-test";
/// A different node used by both honourable and unhonourable placements.
const OTHER_NODE: &str = "node-elsewhere";

struct StubPlacement(Result<ResumePlacement, PlacementRefusal>);

#[async_trait]
impl ResumePlacementSource for StubPlacement {
    async fn locate(&self, _sandbox_id: SandboxId) -> Result<ResumePlacement, PlacementRefusal> {
        self.0.clone()
    }
}

fn placed(node_id: &str) -> PlacedNode {
    PlacedNode {
        node_id: node_id.to_string(),
        address: format!("http://{node_id}:8000"),
    }
}

fn wiring(answer: Result<ResumePlacement, PlacementRefusal>) -> ResumeWiring {
    wiring_at(answer, WakeSite::Local(THIS_NODE.to_string()))
}

fn wiring_at(
    answer: Result<ResumePlacement, PlacementRefusal>,
    wake_site: WakeSite,
) -> ResumeWiring {
    ResumeWiring::new(Some(Arc::new(StubPlacement(answer))), wake_site)
}

async fn build_api(resume_wiring: ResumeWiring) -> Arc<ApiImpl> {
    let orchestrator = Orchestrator::new(
        crate::sandbox::AccessTokenSeedPolicy::MayGenerate,
        InMemoryMetadataStore::new(),
        MockBackendFactory::new(),
        DisabledSandboxPersister,
        crate::image::DisabledRuntimeImageRefs::shared(),
    )
    .await
    .expect("an in-memory orchestrator");
    let snapshot_manager = Arc::new(mock_snapshot_manager());

    Arc::new(ApiImpl::new(
        orchestrator,
        Arc::clone(&snapshot_manager),
        None,
        PausedSandboxWiring::new(
            Arc::new(DisabledPausedSandboxRegistry),
            snapshot_manager,
            &NodeIdentity::from_config(&Default::default()),
        ),
        Vec::new(),
        resume_wiring,
    ))
}

struct RunningApi {
    addr: SocketAddr,
    api: Arc<ApiImpl>,
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
    let api = build_api(resume_wiring).await;

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
        _shutdown: tx,
    }
}

fn refusal_reason(status: &tonic::Status) -> Option<&str> {
    status
        .metadata()
        .get(pb::REFUSAL_REASON_TRAILER)
        .and_then(|value| value.to_str().ok())
}

fn refusal_origin(status: &tonic::Status) -> Option<&str> {
    status
        .metadata()
        .get(pb::REFUSAL_ORIGIN_TRAILER)
        .and_then(|value| value.to_str().ok())
}

#[tokio::test]
async fn a_pin_this_node_cannot_honour_is_refused_and_a_preference_for_the_same_node_is_not() {
    let sandbox_id = SandboxId::new().to_string();

    let pinned_elsewhere = serve_api(wiring(Ok(ResumePlacement::Pinned {
        node: placed(OTHER_NODE),
    })))
    .await;
    let refused = pinned_elsewhere
        .resume(&sandbox_id, None, None)
        .await
        .expect_err("a pin this process cannot honour must be refused");

    assert_eq!(
        refused.code(),
        Code::FailedPrecondition,
        "an unhonourable pin is a precondition failure, not an internal error"
    );
    assert_eq!(
        refusal_reason(&refused),
        Some("origin_not_reachable_from_here"),
        "the gateway backs off on the reason trailer; without it every refusal \
         looks the same and it cannot tell 'wait a moment' from 'wait for a machine'"
    );
    assert_eq!(
        refusal_origin(&refused),
        Some(OTHER_NODE),
        "the operator reading the gateway's log needs the node named there too"
    );

    let preferred_elsewhere = serve_api(wiring(Ok(ResumePlacement::Preferred {
        node: placed(OTHER_NODE),
        origin_node_id: OTHER_NODE.to_string(),
    })))
    .await;
    let preferred = preferred_elsewhere
        .resume(&sandbox_id, None, None)
        .await
        .expect_err("the sandbox does not exist, so this still fails — differently");
    assert_eq!(
        preferred.code(),
        Code::NotFound,
        "🔴 a published snapshot may be rebuilt anywhere, so naming another node \
         is a hint and must not refuse. This call reached arbitration and the wake \
         attempt; it failed only because no such sandbox exists"
    );

    let pinned_here = serve_api(wiring(Ok(ResumePlacement::Pinned {
        node: placed(THIS_NODE),
    })))
    .await;
    let honoured = pinned_here
        .resume(&sandbox_id, None, None)
        .await
        .expect_err("the sandbox does not exist, so this still fails — differently");
    assert_eq!(
        honoured.code(),
        Code::NotFound,
        "a pin naming this node is honourable; refusing it would strand every \
         unpublished sandbox on the machine that holds it"
    );
}

#[tokio::test]
async fn a_pin_the_placement_source_refused_carries_its_reason_through() {
    let sandbox_id = SandboxId::new().to_string();

    let refusing = serve_api(wiring(Err(PlacementRefusal::Pinned {
        reason: crate::api::impls::PinRefusalReason::OriginNotAcceptingWork,
        origin_node_id: OTHER_NODE.to_string(),
        detail: "sandbox is local_only on node \"node-elsewhere\", which is not accepting work"
            .to_string(),
    })))
    .await;
    let refused = refusing
        .resume(&sandbox_id, None, None)
        .await
        .expect_err("a drained origin cannot serve the only copy");
    assert_eq!(refused.code(), Code::FailedPrecondition);
    assert_eq!(
        refusal_reason(&refused),
        Some("origin_not_accepting_work"),
        "🔴 'the origin is draining' and 'somebody else is mid-resume' are both \
         FailedPrecondition and back off completely differently"
    );
    assert_eq!(refusal_origin(&refused), Some(OTHER_NODE));

    let unconstrained = serve_api(wiring(Ok(ResumePlacement::Unconstrained))).await;
    let not_refused = unconstrained
        .resume(&sandbox_id, None, None)
        .await
        .expect_err("no such sandbox");
    assert_eq!(
        not_refused.code(),
        Code::NotFound,
        "a single-node deployment has no placement source, and nothing about that \
         constrains where a sandbox may wake"
    );
}

#[tokio::test]
async fn a_pin_is_enforced_here_only_when_this_process_is_the_one_placing_the_wake_up() {
    let sandbox_id = SandboxId::new().to_string();
    let pinned = || {
        Ok(ResumePlacement::Pinned {
            node: placed(OTHER_NODE),
        })
    };

    let local = serve_api(wiring_at(pinned(), WakeSite::Local(THIS_NODE.to_string()))).await;
    assert_eq!(
        local
            .resume(&sandbox_id, None, None)
            .await
            .expect_err("this process wakes sandboxes only on its own machine")
            .code(),
        Code::FailedPrecondition,
        "a process that wakes sandboxes locally cannot honour a pin naming \
         another machine, and waking it here would rewind the sandbox"
    );

    let remote = serve_api(wiring_at(pinned(), WakeSite::Remote)).await;
    assert_eq!(
        remote
            .resume(&sandbox_id, None, None)
            .await
            .expect_err("no such sandbox")
            .code(),
        Code::NotFound,
        "🔴 a process that places the wake-up itself carries the pin with it; \
         refusing here would refuse every unpublished sandbox in the cluster"
    );
}

#[tokio::test]
async fn a_successful_wake_up_names_the_node_it_woke_on_not_the_one_that_was_preferred() {
    let api = serve_api(wiring(Ok(ResumePlacement::Preferred {
        node: placed(OTHER_NODE),
        origin_node_id: OTHER_NODE.to_string(),
    })))
    .await;
    let sandbox_id = SandboxId::new();

    // Already-running sandboxes exercise the idempotent success path.
    api.api
        .orchestrator()
        .set_proxy_target_for_test(
            sandbox_id,
            ProxyTarget::new(Ipv4Addr::LOCALHOST),
            SandboxState::Running,
        )
        .await;

    let woken = api
        .resume(&sandbox_id.to_string(), None, None)
        .await
        .expect("a running sandbox is a successful wake-up, not a conflict");

    assert_eq!(
        woken.node_id, THIS_NODE,
        "🔴 the answer must name the machine the sandbox is on. The placement \
         preferred {OTHER_NODE} and did not get it; reporting the preference \
         would send the request that triggered this straight past the sandbox"
    );
    assert_ne!(
        woken.node_id, OTHER_NODE,
        "and specifically not the preferred node, which is what a naive read of \
         the placement would produce"
    );
    assert!(
        woken.node_address.is_empty(),
        "the placement's address belongs to {OTHER_NODE}, so it must not be \
         handed out as this node's; empty tells the gateway to resolve it itself"
    );
    assert!(
        !woken.execution_id.is_empty(),
        "🔴 the incarnation is what fences the forwarded request. Without one, \
         the request that caused this resume is the single request in a \
         sandbox's life that travels unfenced"
    );
}

#[tokio::test]
async fn a_placement_naming_this_node_hands_back_its_address() {
    let api = serve_api(wiring(Ok(ResumePlacement::Preferred {
        node: placed(THIS_NODE),
        origin_node_id: THIS_NODE.to_string(),
    })))
    .await;
    let sandbox_id = SandboxId::new();

    api.api
        .orchestrator()
        .set_proxy_target_for_test(
            sandbox_id,
            ProxyTarget::new(Ipv4Addr::LOCALHOST),
            SandboxState::Running,
        )
        .await;

    let woken = api
        .resume(&sandbox_id.to_string(), None, None)
        .await
        .expect("a running sandbox is a successful wake-up");

    assert_eq!(woken.node_id, THIS_NODE);
    assert_eq!(
        woken.node_address,
        format!("http://{THIS_NODE}:8000"),
        "🔴 the placement named this machine, so its address is this machine's \
         and the gateway can forward without looking the node up again"
    );
}

#[tokio::test]
async fn an_unreachable_placement_source_is_unavailable_and_a_missing_sandbox_is_not_found() {
    let sandbox_id = SandboxId::new().to_string();

    let unreachable = serve_api(wiring(Err(PlacementRefusal::Unavailable(
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
        "🔴 'nobody could be asked' is not an answer about whether the sandbox \
         exists; answering NotFound here tells the platform to rebuild a sandbox \
         that may be perfectly alive"
    );

    let known_absent = serve_api(wiring(Err(PlacementRefusal::NotFound))).await;
    let absent = known_absent
        .resume(&sandbox_id, None, None)
        .await
        .expect_err("the placement source has never heard of it");
    assert_eq!(
        absent.code(),
        Code::NotFound,
        "and the converse: a source that positively says it has no such sandbox \
         must not be softened into a retry, or a genuinely dead sandbox is \
         retried forever"
    );

    let exhausted = serve_api(wiring(Err(PlacementRefusal::Exhausted(
        "no nodes available".to_string(),
    ))))
    .await;
    assert_eq!(
        exhausted
            .resume(&sandbox_id, None, None)
            .await
            .expect_err("the cluster is full")
            .code(),
        Code::ResourceExhausted
    );
}

#[tokio::test]
async fn a_malformed_sandbox_id_is_invalid_argument_and_a_well_formed_one_reaches_the_decision() {
    let api = serve_api(wiring(Ok(ResumePlacement::Unconstrained))).await;

    let malformed = api
        .resume("not-a-sandbox-id", None, None)
        .await
        .expect_err("a malformed id cannot name a sandbox");
    assert_eq!(
        malformed.code(),
        Code::InvalidArgument,
        "🔴 answering NotFound would tell the gateway a sandbox is gone that it \
         never named — and the gateway's NotFound handling rebuilds from template"
    );

    let well_formed = api
        .resume(&SandboxId::new().to_string(), None, None)
        .await
        .expect_err("no such sandbox");
    assert_eq!(
        well_formed.code(),
        Code::NotFound,
        "a syntactically valid id gets past parsing and reaches the wake-up \
         decision, which is what makes the InvalidArgument above a statement \
         about the id rather than about the surface"
    );
}

#[tokio::test]
async fn the_envd_credential_check_refuses_what_it_must_and_admits_what_it_must() {
    let api = serve_api(wiring(Ok(ResumePlacement::Unconstrained))).await;
    let sandbox_id = SandboxId::new();

    api.api
        .orchestrator()
        .set_metadata_state_for_test(sandbox_id, SandboxState::Paused)
        .await
        .expect("seed a paused sandbox");
    api.api
        .orchestrator()
        .set_secure_for_test(&sandbox_id, true)
        .await
        .expect("make it secure");
    let metadata = api
        .api
        .orchestrator()
        .get_sandbox(&sandbox_id)
        .await
        .expect("read it back")
        .expect("a paused sandbox");
    let valid = api
        .api
        .orchestrator()
        .get_envd_access_token(&metadata)
        .expect("a secure sandbox has a token");
    let envd_port = ConfigManager::global_config()
        .tools
        .control_plane_port
        .to_string();
    let other_port = (ConfigManager::global_config().tools.control_plane_port + 1).to_string();
    let id = sandbox_id.to_string();

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
            "🔴 an unparseable port with no token — the hole a caller would use \
             to skip the check entirely",
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

    for (port, token, what) in [
        (
            Some(envd_port.as_str()),
            Some(valid.expose()),
            "the envd port with the sandbox's own token",
        ),
        (
            Some(other_port.as_str()),
            None,
            "a port that is not envd's, which the check does not cover",
        ),
    ] {
        let status = api
            .resume(&id, port, token)
            .await
            .expect_err(&format!("{what} gets past the check and fails on the wake"));
        assert_ne!(
            status.code(),
            Code::PermissionDenied,
            "{what} must reach the wake-up decision rather than be refused at the door"
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
        "origin_not_reporting",
        "origin_not_accepting_work",
        "origin_not_reachable_from_here",
        "origin_unclassified",
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

    async fn counters(
        placement: Result<ResumePlacement, PlacementRefusal>,
        seed_running: bool,
    ) -> Vec<(String, u64)> {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        // Keep the thread-local recorder and service future on this test thread.
        let guard = metrics::set_default_local_recorder(&recorder);

        let api = build_api(wiring(placement)).await;
        let sandbox_id = SandboxId::new();
        if seed_running {
            api.orchestrator()
                .set_proxy_target_for_test(
                    sandbox_id,
                    ProxyTarget::new(Ipv4Addr::LOCALHOST),
                    SandboxState::Running,
                )
                .await;
        }
        let service = super::resume::SandboxResumeService::new(Arc::clone(&api));
        let _ = ServiceTrait::resume_sandbox(
            &service,
            tonic::Request::new(pb::SandboxResumeRequest {
                sandbox_id: sandbox_id.to_string(),
            }),
        )
        .await;
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
        moved
    }

    let refused = counters(
        Err(PlacementRefusal::Pinned {
            reason: crate::api::impls::PinRefusalReason::OriginNotReporting,
            origin_node_id: OTHER_NODE.to_string(),
            detail: "sandbox is local_only on node \"node-elsewhere\", which is not reporting"
                .to_string(),
        }),
        false,
    )
    .await;
    assert_eq!(
        refused,
        vec![("origin_not_reporting".to_string(), 1)],
        "🔴 the label must be the refusal's own wire spelling — the same string \
         that travelled in the trailer — or the gateway's log and this scrape \
         cannot be joined"
    );

    let woken = counters(Ok(ResumePlacement::Unconstrained), true).await;
    assert_eq!(
        woken,
        vec![("ok".to_string(), 1)],
        "and a wake-up that worked must record exactly one success: without this \
         the assertion above would also hold for a build that recorded a refusal \
         for everything"
    );
}

#[tokio::test]
async fn a_paused_sandbox_with_auto_resume_off_is_refused_and_the_other_two_are_not() {
    let api = serve_api(wiring(Ok(ResumePlacement::Unconstrained))).await;

    let refused_id = SandboxId::new();
    api.api
        .orchestrator()
        .set_metadata_state_for_test(refused_id, SandboxState::Paused)
        .await
        .expect("seed a paused sandbox");
    api.api
        .orchestrator()
        .set_auto_resume_for_test(&refused_id, false)
        .await
        .expect("turn auto-resume off");

    let status = api
        .resume(&refused_id.to_string(), None, None)
        .await
        .expect_err("a paused sandbox with auto-resume off must not wake on traffic");
    assert_eq!(
        status.code(),
        Code::FailedPrecondition,
        "🔴 not NotFound: the sandbox exists and still resumes through the REST \
         route. The gateway turns NotFound into a 404, which the platform reads \
         as 'rebuild it from its template' — that resets the user's workspace"
    );
    assert_eq!(
        status
            .metadata()
            .get(pb::REFUSAL_REASON_TRAILER)
            .map(|value| value.to_str().expect("an ASCII trailer")),
        Some("auto_resume_disabled"),
        "🔴 the reason is what the gateway keys its 410 off — a FailedPrecondition \
         with any other reason is a 503, which advertises 'try again' for a \
         sandbox that is never going to answer"
    );

    let woken_id = SandboxId::new();
    api.api
        .orchestrator()
        .set_metadata_state_for_test(woken_id, SandboxState::Paused)
        .await
        .expect("seed a paused sandbox");
    api.api
        .orchestrator()
        .set_auto_resume_for_test(&woken_id, true)
        .await
        .expect("turn auto-resume on");

    let status = api
        .resume(&woken_id.to_string(), None, None)
        .await
        .expect_err("the mock backend cannot actually bring a sandbox up");
    assert_ne!(
        status.code(),
        Code::FailedPrecondition,
        "🔴 a sandbox with the flag on must get past this check and fail — if at \
         all — on the wake-up itself. Sharing an outcome with the refused case \
         would make the assertion above hold for a build that refuses everything"
    );

    // The flag governs starting a sandbox, not one already running.
    let running_id = SandboxId::new();
    api.api
        .orchestrator()
        .set_proxy_target_for_test(
            running_id,
            ProxyTarget::new(Ipv4Addr::LOCALHOST),
            SandboxState::Running,
        )
        .await;
    api.api
        .orchestrator()
        .set_auto_resume_for_test(&running_id, false)
        .await
        .expect("turn auto-resume off");

    let woken = api
        .resume(&running_id.to_string(), None, None)
        .await
        .expect("an already-running sandbox is a success regardless of the flag");
    assert_eq!(
        woken.node_id, THIS_NODE,
        "and it names the machine it is on, as the idempotent case always did"
    );
}

// Cluster-backed fixture that grants one claim carrying the requested record.
struct GrantingRegistry {
    inner: DisabledPausedSandboxRegistry,
    auto_resume: bool,
    /// Records whether the claim was released.
    released: Arc<std::sync::atomic::AtomicBool>,
}

#[async_trait]
impl crate::orchestrator::PausedSandboxRegistry for GrantingRegistry {
    fn is_cluster_backed(&self) -> bool {
        true
    }

    async fn claim_for_resume(
        &self,
        sandbox_id: &SandboxId,
        node_id: &str,
        execution_id: crate::types::ExecutionId,
    ) -> crate::orchestrator::RegistryResult<crate::orchestrator::ResumeClaim> {
        let metadata = crate::orchestrator::SandboxMetadata {
            auto_resume: self.auto_resume,
            ..Default::default()
        };
        Ok(crate::orchestrator::ResumeClaim::Claimed {
            entry: Box::new(crate::orchestrator::PausedSandboxEntry {
                sandbox_id: *sandbox_id,
                cluster_id: uuid::Uuid::nil(),
                state: crate::orchestrator::PausedRegistryState::Resuming,
                generation: 7,
                origin_node_id: THIS_NODE.to_string(),
                claimed_by_node_id: Some(node_id.to_string()),
                snapshot_id: Some(crate::snapshot::SnapshotId::generate()),
                metadata: Some(metadata),
                execution_id: Some(execution_id),
                paused_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            }),
            previous_state: crate::orchestrator::PausedRegistryState::Paused,
        })
    }

    async fn release_claim(
        &self,
        _sandbox_id: &SandboxId,
        _generation: i64,
    ) -> crate::orchestrator::RegistryResult<bool> {
        self.released
            .store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(true)
    }

    async fn begin_pause(
        &self,
        entry: &crate::orchestrator::PausedSandboxEntry,
    ) -> crate::orchestrator::RegistryResult<crate::orchestrator::BeganPause> {
        self.inner.begin_pause(entry).await
    }
    async fn complete_pause(
        &self,
        sandbox_id: &SandboxId,
        generation: i64,
        snapshot_id: &crate::snapshot::SnapshotId,
    ) -> crate::orchestrator::RegistryResult<()> {
        self.inner
            .complete_pause(sandbox_id, generation, snapshot_id)
            .await
    }
    async fn mark_local_only(
        &self,
        sandbox_id: &SandboxId,
        generation: i64,
    ) -> crate::orchestrator::RegistryResult<()> {
        self.inner.mark_local_only(sandbox_id, generation).await
    }
    async fn get(
        &self,
        sandbox_id: &SandboxId,
    ) -> crate::orchestrator::RegistryResult<Option<crate::orchestrator::PausedSandboxEntry>> {
        self.inner.get(sandbox_id).await
    }
    async fn get_many(
        &self,
        sandbox_ids: &[SandboxId],
    ) -> crate::orchestrator::RegistryResult<crate::orchestrator::PausedRegistryRows> {
        self.inner.get_many(sandbox_ids).await
    }
    async fn renew_lease(
        &self,
        node_id: &str,
        held: &[crate::orchestrator::HeldSandbox],
    ) -> crate::orchestrator::RegistryResult<u64> {
        self.inner.renew_lease(node_id, held).await
    }
    async fn reclaim_expired_holdings(
        &self,
    ) -> crate::orchestrator::RegistryResult<crate::orchestrator::ReclaimedHoldings> {
        self.inner.reclaim_expired_holdings().await
    }
    async fn mark_running(
        &self,
        sandbox_id: &SandboxId,
        node_id: &str,
        holder_node_id: &str,
        execution_id: crate::types::ExecutionId,
        expires_at: Option<std::time::SystemTime>,
    ) -> crate::orchestrator::RegistryResult<crate::orchestrator::MarkRunningOutcome> {
        self.inner
            .mark_running(
                sandbox_id,
                node_id,
                holder_node_id,
                execution_id,
                expires_at,
            )
            .await
    }
    async fn renew_sandbox_deadline(
        &self,
        sandbox_id: &SandboxId,
        execution_id: crate::types::ExecutionId,
        expires_at: Option<std::time::SystemTime>,
    ) -> crate::orchestrator::RegistryResult<crate::orchestrator::DeadlineRenewalOutcome> {
        self.inner
            .renew_sandbox_deadline(sandbox_id, execution_id, expires_at)
            .await
    }
    async fn release_node_holdings(
        &self,
        node_id: &str,
    ) -> crate::orchestrator::RegistryResult<crate::orchestrator::ReleasedHoldings> {
        self.inner.release_node_holdings(node_id).await
    }
    async fn remove(
        &self,
        sandbox_id: &SandboxId,
        generation: i64,
    ) -> crate::orchestrator::RegistryResult<bool> {
        self.inner.remove(sandbox_id, generation).await
    }
    async fn list_all(
        &self,
    ) -> crate::orchestrator::RegistryResult<crate::orchestrator::PausedRegistryListing> {
        self.inner.list_all().await
    }
}

async fn serve_api_with_registry(
    auto_resume: bool,
    released: Arc<std::sync::atomic::AtomicBool>,
) -> RunningApi {
    crate::logging::init_for_tests();
    let orchestrator = Orchestrator::new(
        crate::sandbox::AccessTokenSeedPolicy::MayGenerate,
        InMemoryMetadataStore::new(),
        MockBackendFactory::new(),
        DisabledSandboxPersister,
        crate::image::DisabledRuntimeImageRefs::shared(),
    )
    .await
    .expect("an in-memory orchestrator");
    let snapshot_manager = Arc::new(mock_snapshot_manager());

    let api = Arc::new(ApiImpl::new(
        orchestrator,
        Arc::clone(&snapshot_manager),
        None,
        PausedSandboxWiring::new(
            Arc::new(GrantingRegistry {
                inner: DisabledPausedSandboxRegistry,
                auto_resume,
                released,
            }),
            snapshot_manager,
            &NodeIdentity::from_config(&Default::default()),
        ),
        Vec::new(),
        wiring(Ok(ResumePlacement::Unconstrained)),
    ));

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
    for _ in 0..200 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    RunningApi {
        addr,
        api,
        _shutdown: tx,
    }
}

#[tokio::test]
async fn the_flag_is_read_off_the_cluster_row_when_this_process_has_no_record() {
    let released = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let api = serve_api_with_registry(false, Arc::clone(&released)).await;
    let sandbox_id = SandboxId::new();
    assert!(
        api.api
            .orchestrator()
            .get_sandbox(&sandbox_id)
            .await
            .expect("the store answers")
            .is_none(),
        "🔴 the premise: this process holds no record, so only the cluster row \
         can carry the flag"
    );

    let status = api
        .resume(&sandbox_id.to_string(), None, None)
        .await
        .expect_err("the cluster row says auto-resume is off");
    assert_eq!(status.code(), Code::FailedPrecondition);
    assert_eq!(
        status
            .metadata()
            .get(pb::REFUSAL_REASON_TRAILER)
            .map(|value| value.to_str().expect("an ASCII trailer")),
        Some("auto_resume_disabled"),
    );
    assert!(
        released.load(std::sync::atomic::Ordering::SeqCst),
        "🔴 a refusal after a granted claim must release it, or the row sits in \
         `resuming` until its lease lapses and nothing can wake the sandbox"
    );

    let released_on = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let api_on = serve_api_with_registry(true, released_on).await;
    let status = api_on
        .resume(&SandboxId::new().to_string(), None, None)
        .await
        .expect_err("the mock backend cannot actually restore a sandbox");
    assert_ne!(
        status.code(),
        Code::FailedPrecondition,
        "🔴 with the flag on the same path must get past this check — otherwise \
         the refusal above is just this stub refusing everything"
    );
}
