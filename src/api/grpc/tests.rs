//! The wake-up surface, over a real socket.
//!
//! # Why a socket and not a direct call
//!
//! Half of what this surface promises lives in the transport rather than in the
//! return value: the refusal reason travels in a **trailer** because a status
//! detail would not survive the crossing to the Go gateway
//! (`crate::proto::apiproxy`), and the target port and access token arrive as
//! **metadata** rather than as proto fields. A test that called
//! `ApiImpl::resume_for_data_plane` directly would exercise the decision and
//! none of the contract the only consumer actually reads.
//!
//! # 🔴 What these tests are not
//!
//! They are not evidence that the move works end to end. Nothing in `src/bin/`
//! serves this surface yet and no gateway calls it, so what is verified here is
//! the decision and the wire shape — not that a paused sandbox reached from a
//! real gateway wakes up. See `src/api/grpc/mod.rs`.
//!
//! # The pairing rule
//!
//! Every refusal here is asserted next to a case that is **not** refused, in
//! the same test. A wake path that never fired would pass "this sandbox was not
//! woken" trivially, so no test in this file rests on a refusal alone.

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::oneshot;
use tonic::Code;

use crate::api::impls::{
    PlacedNode, PlacementRefusal, ResumePlacement, ResumePlacementSource, WakeSite,
};
use crate::api::{ApiImpl, PausedSandboxWiring, ResumeWiring};
use crate::cfg::{AppConfig, ConfigManager};
use crate::identity::NodeIdentity;
use crate::image::ImageResolver;
use crate::orchestrator::{
    DisabledPausedSandboxRegistry, DisabledSandboxPersister, InMemoryMetadataStore, Orchestrator,
    ProxyTarget, SandboxState,
};
use crate::proto::apiproxy::{
    self as pb, sandbox_resume_service_client::SandboxResumeServiceClient,
};
use crate::role::ServerRole;
use crate::sandbox::mock::MockBackendFactory;
use crate::snapshot::mock::mock_snapshot_manager;
use crate::template::TemplateBuilder;
use crate::types::SandboxId;

/// The node the process under test wakes sandboxes on.
const THIS_NODE: &str = "node-under-test";
/// A node that is not this one. Both the honourable and the unhonourable
/// placements name it, which is what makes the pair discriminating.
const OTHER_NODE: &str = "node-elsewhere";

// ---------------------------------------------------------------------------
// A placement source that answers whatever the test needs
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// The API half, on a real socket
// ---------------------------------------------------------------------------

async fn build_api(resume_wiring: ResumeWiring) -> Arc<ApiImpl> {
    let orchestrator = Orchestrator::new(
        InMemoryMetadataStore::new(),
        MockBackendFactory::new(),
        DisabledSandboxPersister,
    )
    .await
    .expect("an in-memory orchestrator");
    let snapshot_manager = Arc::new(mock_snapshot_manager());

    Arc::new(ApiImpl::new(
        orchestrator,
        Arc::clone(&snapshot_manager),
        Arc::new(TemplateBuilder::new()),
        Arc::new(ImageResolver::new(&AppConfig::default())),
        None,
        PausedSandboxWiring::new(
            Arc::new(DisabledPausedSandboxRegistry),
            snapshot_manager,
            &NodeIdentity::from_config(&Default::default()),
        ),
        Vec::new(),
        // 🔴 `Api` and not `All`. This surface is the API half's, and building
        // it as `all` would let a test pass that a `--role api` process could
        // not reproduce.
        ServerRole::Api,
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

    /// One `ResumeSandbox` call, with the metadata the gateway would send.
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

async fn serve_api(resume_wiring: ResumeWiring) -> RunningApi {
    crate::logging::init_for_tests();
    let api = build_api(resume_wiring).await;

    // Take a port from the OS and let it go: `serve_with_shutdown` wants an
    // address rather than a listener. Same trick as `src/node_client/tests.rs`.
    let addr: SocketAddr = {
        let probe = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind a port");
        probe.local_addr().expect("the bound address")
    };
    let (tx, rx) = oneshot::channel();

    let served = Arc::clone(&api);
    tokio::spawn(async move {
        let _ = super::serve(addr, served, async {
            let _ = rx.await;
        })
        .await;
    });

    // 🔴 Wait for the bind. A connect that raced it comes back as "connection
    // refused", which is exactly the failure some of these tests are about.
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

/// The reason a refusal named, read off the trailer the gateway reads.
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

// ---------------------------------------------------------------------------
// 🔴 Pin and prefer (§6.6)
// ---------------------------------------------------------------------------

/// 🔴 The test the whole pin/prefer distinction rests on, and the unit-test
/// shape of §12 P6's 對照 A.
///
/// Three calls, one placement source difference between them, and **the node
/// named is the same node in all three**. That is what makes it evidence: an
/// implementation that pinned every paused sandbox to its origin and an
/// implementation that pinned none of them both pass a test that only checks
/// the refusal. Here the refusal is asserted next to two placements that are
/// *not* refused, so neither degenerate implementation survives.
///
/// What each arm means:
/// - `Pinned` on another node — the only copy of the bytes is over there, and
///   waking it here would rebuild from an older snapshot and silently drop the
///   last pause. Refused.
/// - `Pinned` on *this* node — a pin this process can honour. Not refused.
/// - `Preferred` on another node — the snapshot is in shared storage, so origin
///   is a hint and any node may rebuild it. Not refused.
#[tokio::test]
async fn a_pin_this_node_cannot_honour_is_refused_and_a_preference_for_the_same_node_is_not() {
    let sandbox_id = SandboxId::new().to_string();

    // ---- refused: the bytes are on OTHER_NODE and nowhere else ----
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

    // ---- not refused: the same node, but only a preference ----
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

    // ---- not refused: a pin this process *can* honour ----
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

/// A pin the *placement source* refused, rather than one this process cannot
/// honour, arrives with the scheduler's own reason on it.
///
/// Paired with a placement that is not a pin refusal at all, so the assertion
/// is about classification rather than about "everything fails".
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

    // The non-empty half: an unconstrained placement is not refused.
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

/// 🔴 Who enforces the pin depends on who places the wake-up, and this is the
/// branch that decides it.
///
/// `WakeSite::Local` means the orchestration surface behind this `ApiImpl` runs
/// sandboxes on one named machine, so a pin naming any other machine cannot be
/// honoured here and must be refused. `WakeSite::Remote` means the surface
/// places the wake-up itself, so the pin is *its* to honour and refusing here
/// would strand every unpublished sandbox the moment `--role api` lands.
///
/// 🔴 Nothing in `src/bin/` constructs `Remote` yet — it arrives with the
/// remote backend factory, and `WakeSite::Remote`'s own doc says the two must
/// land together because leaving the pin unenforced without it is the silent
/// rewind. This test is the only thing exercising that branch, and it is here
/// so the branch is wrong-in-a-test rather than wrong-in-production on the day
/// the factory is wired.
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

// ---------------------------------------------------------------------------
// 🔴 A wake-up that succeeds
// ---------------------------------------------------------------------------

/// 🔴 The one test in this file where the wake-up **works**, and the reason it
/// has to exist.
///
/// Every other test here ends in a refusal or a `NotFound`. A surface that
/// could only ever fail would pass all of them, so without this one the file
/// proves that the wake path rejects things and never that it wakes anything.
///
/// What it also pins down: the response names **where the sandbox actually
/// woke**, not where the placement would have preferred it. The placement here
/// says `OTHER_NODE`; this process wakes sandboxes on `THIS_NODE`, and for a
/// published snapshot origin is only a hint — so the hint loses and the answer
/// must say `THIS_NODE`. `node_id` is documented on the wire as "the node the
/// sandbox is running on now", and the gateway forwards the triggering request
/// straight at it. Answering `OTHER_NODE` would send that request to a machine
/// without the sandbox, so a successful wake-up would still surface to the user
/// as a failure — the sort of bug that looks like a flaky network.
#[tokio::test]
async fn a_successful_wake_up_names_the_node_it_woke_on_not_the_one_that_was_preferred() {
    let api = serve_api(wiring(Ok(ResumePlacement::Preferred {
        node: placed(OTHER_NODE),
        origin_node_id: OTHER_NODE.to_string(),
    })))
    .await;
    let sandbox_id = SandboxId::new();

    // An already-running sandbox is the idempotent case the proto calls out:
    // "a sandbox that is already running is a success carrying that sandbox's
    // node and incarnation, not a conflict."
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

// ---------------------------------------------------------------------------
// 🔴 Three-state absence: "could not ask" is never "does not exist"
// ---------------------------------------------------------------------------

/// 🔴 The downstream contract for `NotFound` is "rebuild it from its template",
/// which resets a user's workspace. So an unreachable placement source must
/// answer `Unavailable`, and the only way to know the surface can tell them
/// apart is to ask it both questions in one test.
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

    // The third state, so the two above are not merely two spellings of failure.
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

// ---------------------------------------------------------------------------
// Malformed input
// ---------------------------------------------------------------------------

/// A malformed id is the caller's bug, not a missing sandbox.
///
/// Paired with a well-formed id in the same test: `InvalidArgument` on its own
/// would also be what a surface that rejected *everything* returned.
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

// ---------------------------------------------------------------------------
// 🔴 The envd credential check, including the port that does not parse
// ---------------------------------------------------------------------------

/// 🔴 The whole matrix of the envd credential check, because every cell of it
/// is a way through.
///
/// The check only applies to traffic addressed at envd's control-plane port. A
/// caller therefore has two ways to try to skip it — claim a different port, or
/// send a port that is not a number at all — and only one of them is allowed to
/// work. `target_port_of` maps both "absent" and "unparseable" to `None`, and
/// `authorize_envd` treats `None` as *possibly envd*, which is the strict
/// direction.
///
/// The refused half and the admitted half are in the same test on purpose: four
/// `PermissionDenied`s prove nothing on their own, because a surface that
/// refused every caller would produce exactly the same four.
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

    // ---- refused ----
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

    // ---- admitted: the non-empty half ----
    //
    // These get past the credential check and fail later, on the wake-up
    // itself — which is the point. If they came back PermissionDenied the four
    // refusals above would be proving nothing but that this surface refuses.
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

// ---------------------------------------------------------------------------
// 🔴 Metrics exist before anything goes wrong
// ---------------------------------------------------------------------------

/// 🔴 §12 P3 control B compares the gateway's attempt counter against this
/// one "逐条相等", and §12's first methodological rule is that a metric sitting
/// at 0 is not evidence until something has proved the series exists.
///
/// A counter that springs into being on its first increment makes "zero" and
/// "the probe is scraping a series that was never registered" the same reading.
/// This asserts every label value is published at build time, before any
/// request has arrived.
#[test]
fn every_outcome_of_the_wake_up_surface_is_published_before_the_first_request() {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    // Synchronous on purpose: `with_local_recorder` is thread-local, and
    // `describe_metrics` is called from `super::server` on the caller's thread.
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
