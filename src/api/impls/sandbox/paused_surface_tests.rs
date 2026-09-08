use std::sync::Arc;

use axum_extra::extract::CookieJar;
use headers::Host;
use http::Method;

use agentenv_http_server::apis::sandboxes::*;
use agentenv_http_server::models;

use super::ApiImpl;
use crate::api::ResumeWiring;
use crate::orchestrator::{Orchestrator, SandboxState};
use crate::sandbox::mock::MockBackendFactory;
use crate::snapshot::mock::{
    in_memory_snapshot_manager, mock_paused_sandbox_config, paused_sandbox_record,
    InMemorySnapshotCatalog,
};
use crate::snapshot::repository::interfaces::SnapshotCatalog;
use crate::snapshot::PausedSandboxConfig;
use crate::types::SandboxId;

const ORIGIN: &str = "node-origin";

struct Surface {
    api: Arc<ApiImpl>,
    catalog: Arc<InMemorySnapshotCatalog>,
    orchestrator: Arc<Orchestrator<crate::orchestrator::InMemoryMetadataStore, MockBackendFactory>>,
}

/// Answers one fixed verdict about whether a sandbox is still routed to.
struct FixedRouting(bool);

#[async_trait::async_trait]
impl crate::orchestrator::RuntimeRouting for FixedRouting {
    async fn is_routed(&self, _sandbox_id: SandboxId) -> anyhow::Result<bool> {
        Ok(self.0)
    }

    async fn forget(
        &self,
        _sandbox_id: SandboxId,
        _execution_id: crate::types::ExecutionId,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

impl Surface {
    async fn as_half(wiring: ResumeWiring) -> Self {
        Self::as_half_routed(wiring, None).await
    }

    async fn as_half_routed(
        wiring: ResumeWiring,
        routing: Option<Arc<dyn crate::orchestrator::RuntimeRouting>>,
    ) -> Self {
        let orchestrator = Orchestrator::with_in_memory_store(MockBackendFactory::new()).await;
        if let Some(routing) = routing {
            orchestrator.set_runtime_routing(routing);
        }
        let (snapshot_manager, catalog) = in_memory_snapshot_manager();
        let api = Arc::new(ApiImpl::new(
            Arc::clone(&orchestrator) as Arc<dyn crate::orchestrator::SandboxOrchestration>,
            Arc::new(snapshot_manager),
            None,
            Vec::new(),
            wiring,
        ));
        Self {
            api,
            catalog,
            orchestrator,
        }
    }

    async fn api_half() -> Self {
        Self::as_half(ResumeWiring::api_half_for_test()).await
    }

    async fn node_half() -> Self {
        Self::as_half(ResumeWiring::node_local(ORIGIN)).await
    }

    fn paused_at(&self, sandbox_id: SandboxId, paused: PausedSandboxConfig, at_unix_ms: i64) {
        self.catalog.seed(paused_sandbox_record(
            sandbox_id,
            Some(ORIGIN),
            paused,
            at_unix_ms,
        ));
    }

    fn paused(&self, paused: PausedSandboxConfig) -> SandboxId {
        let sandbox_id = SandboxId::new();
        self.paused_at(sandbox_id, paused, 1_700_000_000_000);
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

    async fn get(&self, sandbox_id: SandboxId) -> SandboxesSandboxIdGetResponse {
        self.api
            .sandboxes_sandbox_id_get(
                &Method::GET,
                &host(),
                &CookieJar::new(),
                &super::super::Claims,
                &models::SandboxesSandboxIdGetPathParams {
                    sandbox_id: sandbox_id.to_string(),
                },
            )
            .await
            .expect("the handler answers")
    }

    async fn pause(&self, sandbox_id: SandboxId) -> SandboxesSandboxIdPausePostResponse {
        self.api
            .sandboxes_sandbox_id_pause_post(
                &Method::POST,
                &host(),
                &CookieJar::new(),
                &super::super::Claims,
                &models::SandboxesSandboxIdPausePostPathParams {
                    sandbox_id: sandbox_id.to_string(),
                },
            )
            .await
            .expect("the handler answers")
    }

    async fn resume(&self, sandbox_id: SandboxId) -> SandboxesSandboxIdResumePostResponse {
        self.api
            .sandboxes_sandbox_id_resume_post(
                &Method::POST,
                &host(),
                &CookieJar::new(),
                &super::super::Claims,
                &models::SandboxesSandboxIdResumePostPathParams {
                    sandbox_id: sandbox_id.to_string(),
                },
                &models::ResumedSandbox { timeout: Some(60) },
            )
            .await
            .expect("the handler answers")
    }

    async fn connect(&self, sandbox_id: SandboxId) -> SandboxesSandboxIdConnectPostResponse {
        self.api
            .sandboxes_sandbox_id_connect_post(
                &Method::POST,
                &host(),
                &CookieJar::new(),
                &super::super::Claims,
                &models::SandboxesSandboxIdConnectPostPathParams {
                    sandbox_id: sandbox_id.to_string(),
                },
                &models::ConnectSandbox::new(60),
            )
            .await
            .expect("the handler answers")
    }

    async fn delete(&self, sandbox_id: SandboxId) -> SandboxesSandboxIdDeleteResponse {
        self.api
            .sandboxes_sandbox_id_delete(
                &Method::DELETE,
                &host(),
                &CookieJar::new(),
                &super::super::Claims,
                &models::SandboxesSandboxIdDeletePathParams {
                    sandbox_id: sandbox_id.to_string(),
                },
            )
            .await
            .expect("the handler answers")
    }

    async fn list_v2(&self, state: Vec<models::SandboxState>) -> Vec<models::ListedSandbox> {
        self.list_v2_matching(state, None).await
    }

    async fn list_v2_matching(
        &self,
        state: Vec<models::SandboxState>,
        metadata: Option<&str>,
    ) -> Vec<models::ListedSandbox> {
        let response = self
            .api
            .v2_sandboxes_get(
                &Method::GET,
                &host(),
                &CookieJar::new(),
                &super::super::Claims,
                &models::V2SandboxesGetQueryParams {
                    metadata: metadata.map(ToString::to_string),
                    state,
                    next_token: None,
                    limit: None,
                },
            )
            .await
            .expect("the handler answers");
        match response {
            V2SandboxesGetResponse::Status200_SuccessfullyReturnedAllRunningSandboxes {
                body,
                ..
            } => body,
            other => panic!("expected a listing, got {other:?}"),
        }
    }
}

fn host() -> Host {
    Host::from(http::uri::Authority::from_static("localhost"))
}

fn detail(response: SandboxesSandboxIdGetResponse) -> models::SandboxDetail {
    match response {
        SandboxesSandboxIdGetResponse::Status200_SuccessfullyReturnedTheSandbox(detail) => detail,
        other => panic!("expected the sandbox, got {other:?}"),
    }
}

#[tokio::test]
async fn a_paused_sandbox_is_read_from_its_row_with_state_paused() {
    let surface = Surface::api_half().await;
    let sandbox_id = surface.paused(PausedSandboxConfig {
        template_id: "tpl-from-the-row".to_string(),
        auto_resume: false,
        user_metadata: Some([("owner".to_string(), "row".to_string())].into()),
        ..mock_paused_sandbox_config()
    });

    let detail = detail(surface.get(sandbox_id).await);

    assert_eq!(detail.state, models::SandboxState::Paused);
    assert_eq!(detail.sandbox_id, sandbox_id.to_string());
    assert_eq!(detail.template_id, "tpl-from-the-row");
    assert_eq!(
        detail.metadata,
        Some([("owner".to_string(), "row".to_string())].into())
    );
    assert_eq!(
        detail
            .lifecycle
            .as_ref()
            .map(|lifecycle| lifecycle.auto_resume),
        Some(false),
        "the row's lifecycle flags are what the client reads"
    );
    assert!(
        matches!(
            surface.get(SandboxId::new()).await,
            SandboxesSandboxIdGetResponse::Status404_NotFound(_)
        ),
        "a sandbox with neither a record nor a row is not found"
    );
}

#[tokio::test]
async fn the_node_half_does_not_read_paused_rows() {
    let node = Surface::node_half().await;
    let sandbox_id = node.paused(mock_paused_sandbox_config());

    assert!(
        matches!(
            node.get(sandbox_id).await,
            SandboxesSandboxIdGetResponse::Status404_NotFound(_)
        ),
        "the row sits in this process's catalog and the node half still answers 404"
    );
    assert!(matches!(
        node.resume(sandbox_id).await,
        SandboxesSandboxIdResumePostResponse::Status404_NotFound(_)
    ));
    assert!(matches!(
        node.pause(sandbox_id).await,
        SandboxesSandboxIdPausePostResponse::Status404_NotFound(_)
    ));
    assert!(
        matches!(
            node.delete(sandbox_id).await,
            SandboxesSandboxIdDeleteResponse::Status404_NotFound(_)
        ),
        "and it deletes no rows it does not own"
    );
    assert!(node.list_v2(Vec::new()).await.is_empty());
}

#[tokio::test]
async fn the_newest_pause_of_a_sandbox_is_the_one_read() {
    let surface = Surface::api_half().await;
    let sandbox_id = SandboxId::new();
    surface.paused_at(
        sandbox_id,
        PausedSandboxConfig {
            template_id: "older".to_string(),
            ..mock_paused_sandbox_config()
        },
        1_700_000_001_000,
    );
    surface.paused_at(
        sandbox_id,
        PausedSandboxConfig {
            template_id: "newer".to_string(),
            ..mock_paused_sandbox_config()
        },
        1_700_000_002_000,
    );

    assert_eq!(detail(surface.get(sandbox_id).await).template_id, "newer");
}

#[tokio::test]
async fn pausing_an_already_paused_sandbox_is_a_conflict_and_pausing_nothing_is_not_found() {
    let surface = Surface::api_half().await;
    let sandbox_id = surface.paused(mock_paused_sandbox_config());

    assert!(
        matches!(
            surface.pause(sandbox_id).await,
            SandboxesSandboxIdPausePostResponse::Status409_Conflict(_)
        ),
        "a paused sandbox exists, so pausing it again is a conflict, never absence"
    );
    assert!(matches!(
        surface.pause(SandboxId::new()).await,
        SandboxesSandboxIdPausePostResponse::Status404_NotFound(_)
    ));
}

#[tokio::test]
async fn a_resume_rebuilds_the_sandbox_from_its_row_and_answers_created() {
    let surface = Surface::api_half().await;
    let sandbox_id = surface.paused(PausedSandboxConfig {
        user_metadata: Some([("owner".to_string(), "row".to_string())].into()),
        ..mock_paused_sandbox_config()
    });
    assert_eq!(surface.state_of(sandbox_id).await, None);

    let response = surface.resume(sandbox_id).await;

    let SandboxesSandboxIdResumePostResponse::Status201_TheSandboxWasResumedSuccessfully {
        body,
        x_agentenv_execution_id,
        ..
    } = response
    else {
        panic!("expected the sandbox to be rebuilt, got {response:?}");
    };
    assert_eq!(body.sandbox_id, sandbox_id.to_string());
    let rebuilt = surface
        .api
        .orchestrator()
        .get_sandbox(&sandbox_id)
        .await
        .expect("the store answers")
        .expect("the resume created a record under the sandbox's own id");
    assert_eq!(rebuilt.state, SandboxState::Running);
    assert_eq!(
        x_agentenv_execution_id,
        Some(rebuilt.execution_id.to_string()),
        "the routing headers name the incarnation the resume minted"
    );
    assert_eq!(
        rebuilt.user_metadata,
        Some([("owner".to_string(), "row".to_string())].into()),
        "the rebuilt sandbox carries the configuration its row paused with"
    );

    let detail = detail(surface.get(sandbox_id).await);
    assert_eq!(
        detail.state,
        models::SandboxState::Running,
        "once running, the record answers, not the row"
    );

    assert!(
        matches!(
            surface.resume(sandbox_id).await,
            SandboxesSandboxIdResumePostResponse::Status201_TheSandboxWasResumedSuccessfully { .. }
        ),
        "a resume of a running sandbox is answered as it stands"
    );
    assert!(matches!(
        surface.resume(SandboxId::new()).await,
        SandboxesSandboxIdResumePostResponse::Status404_NotFound(_)
    ));
}

#[tokio::test]
async fn a_connect_rebuilds_a_paused_sandbox_and_answers_a_running_one_as_it_stands() {
    let surface = Surface::api_half().await;
    let sandbox_id = surface.paused(mock_paused_sandbox_config());

    assert!(matches!(
        surface.connect(sandbox_id).await,
        SandboxesSandboxIdConnectPostResponse::Status201_TheSandboxWasResumedSuccessfully { .. }
    ));
    assert_eq!(
        surface.state_of(sandbox_id).await,
        Some(SandboxState::Running)
    );
    assert!(matches!(
        surface.connect(sandbox_id).await,
        SandboxesSandboxIdConnectPostResponse::Status200_TheSandboxWasAlreadyRunning { .. }
    ));
    assert!(matches!(
        surface.connect(SandboxId::new()).await,
        SandboxesSandboxIdConnectPostResponse::Status404_NotFound(_)
    ));
}

#[tokio::test]
async fn a_resume_that_lands_on_no_known_node_leaves_the_rows_origin_alone() {
    let surface = Surface::api_half().await;
    let sandbox_id = SandboxId::new();
    let record = paused_sandbox_record(
        sandbox_id,
        Some(ORIGIN),
        mock_paused_sandbox_config(),
        1_700_000_000_000,
    );
    surface.catalog.seed(record.clone());

    let _ = surface.resume(sandbox_id).await;

    assert_eq!(
        surface
            .api
            .orchestrator()
            .sandbox_holding_node_id(&sandbox_id)
            .await,
        None,
        "the premise: this backend names no node for the sandbox it runs"
    );
    let landed = surface
        .catalog
        .get(&record.id.to_string())
        .await
        .expect("the catalog answers")
        .expect("the row outlives the resume");
    assert_eq!(
        landed.origin_node_id.as_deref(),
        Some(ORIGIN),
        "a landing nobody can name must not erase the node whose cache is warm"
    );
}

#[tokio::test]
async fn deleting_a_paused_sandbox_forgets_every_pause_of_it() {
    let surface = Surface::api_half().await;
    let sandbox_id = SandboxId::new();
    surface.paused_at(sandbox_id, mock_paused_sandbox_config(), 1_700_000_001_000);
    surface.paused_at(sandbox_id, mock_paused_sandbox_config(), 1_700_000_002_000);
    let other = surface.paused(mock_paused_sandbox_config());

    assert!(matches!(
        surface.delete(sandbox_id).await,
        SandboxesSandboxIdDeleteResponse::Status204_TheSandboxWasKilledSuccessfully
    ));

    assert!(
        matches!(
            surface.get(sandbox_id).await,
            SandboxesSandboxIdGetResponse::Status404_NotFound(_)
        ),
        "every pause of the sandbox is gone, not just the newest"
    );
    assert!(
        matches!(
            surface.delete(sandbox_id).await,
            SandboxesSandboxIdDeleteResponse::Status404_NotFound(_)
        ),
        "deleting what is neither running nor paused is not found"
    );
    assert!(
        matches!(
            surface.get(other).await,
            SandboxesSandboxIdGetResponse::Status200_SuccessfullyReturnedTheSandbox(_)
        ),
        "and another sandbox's pause is untouched"
    );
}

#[tokio::test]
async fn deleting_a_running_sandbox_forgets_its_pauses_too() {
    let surface = Surface::api_half().await;
    let sandbox_id = surface.paused(mock_paused_sandbox_config());
    let _ = surface.resume(sandbox_id).await;
    // The resume left the row behind; a delete must not leave a paused ghost.
    assert!(matches!(
        surface.get(sandbox_id).await,
        SandboxesSandboxIdGetResponse::Status200_SuccessfullyReturnedTheSandbox(_)
    ));

    assert!(matches!(
        surface.delete(sandbox_id).await,
        SandboxesSandboxIdDeleteResponse::Status204_TheSandboxWasKilledSuccessfully
    ));

    assert_eq!(surface.state_of(sandbox_id).await, None);
    assert!(matches!(
        surface.get(sandbox_id).await,
        SandboxesSandboxIdGetResponse::Status404_NotFound(_)
    ));
}

#[tokio::test]
async fn the_v2_listing_shows_each_sandbox_once_and_a_running_one_as_running() {
    let surface = Surface::api_half().await;
    let resumed = surface.paused(mock_paused_sandbox_config());
    let still_paused = surface.paused(mock_paused_sandbox_config());
    let _ = surface.resume(resumed).await;

    let all = surface.list_v2(Vec::new()).await;
    let mut states: Vec<(String, models::SandboxState)> = all
        .iter()
        .map(|sandbox| (sandbox.sandbox_id.clone(), sandbox.state))
        .collect();
    states.sort();
    let mut expected = vec![
        (resumed.to_string(), models::SandboxState::Running),
        (still_paused.to_string(), models::SandboxState::Paused),
    ];
    expected.sort();
    assert_eq!(
        states, expected,
        "a resumed sandbox still has its row and must be listed once, as running"
    );

    let running_only = surface.list_v2(vec![models::SandboxState::Running]).await;
    assert_eq!(
        running_only
            .iter()
            .map(|sandbox| sandbox.sandbox_id.clone())
            .collect::<Vec<_>>(),
        vec![resumed.to_string()]
    );
}

#[tokio::test]
async fn the_paused_only_listing_does_not_show_a_sandbox_that_is_running() {
    let surface = Surface::api_half().await;
    let resumed = surface.paused(mock_paused_sandbox_config());
    let still_paused = surface.paused(mock_paused_sandbox_config());
    let _ = surface.resume(resumed).await;

    let paused_only = surface.list_v2(vec![models::SandboxState::Paused]).await;

    assert_eq!(
        paused_only
            .iter()
            .map(|sandbox| sandbox.sandbox_id.clone())
            .collect::<Vec<_>>(),
        vec![still_paused.to_string()],
        "a running sandbox keeps the row it was resumed from; the row is not a \
         second, paused sandbox"
    );
}

/// Puts a record mid-pause under `sandbox_id`, as a pause in flight leaves it.
async fn pause_in_flight(surface: &Surface, sandbox_id: SandboxId) {
    surface
        .api
        .orchestrator()
        .set_metadata_state_for_test(sandbox_id, SandboxState::Pausing)
        .await
        .expect("the store answers");
}

/// Ends the pause in flight shortly, the way a real one ends: the record
/// goes away and the row is all that is left.
fn finish_pause_later(surface: &Surface, sandbox_id: SandboxId) {
    let orchestrator = surface.api.orchestrator();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        orchestrator
            .remove_sandbox_for_test(&sandbox_id)
            .await
            .expect("the store answers");
    });
}

/// Fails the pause in flight shortly: the VM is back and the record is running.
fn fail_pause_later(surface: &Surface, sandbox_id: SandboxId) {
    let orchestrator = surface.api.orchestrator();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        orchestrator
            .set_metadata_state_for_test(sandbox_id, SandboxState::Running)
            .await
            .expect("the store answers");
    });
}

#[tokio::test]
async fn a_resume_during_a_pause_waits_for_it_and_rebuilds_from_the_row() {
    let surface = Surface::api_half().await;
    let sandbox_id = surface.paused(mock_paused_sandbox_config());
    pause_in_flight(&surface, sandbox_id).await;
    finish_pause_later(&surface, sandbox_id);

    let response = surface.resume(sandbox_id).await;

    assert!(
        matches!(
            response,
            SandboxesSandboxIdResumePostResponse::Status201_TheSandboxWasResumedSuccessfully { .. }
        ),
        "the resume waited for the pause to finish and rebuilt from the row, got {response:?}"
    );
    assert_eq!(
        surface.state_of(sandbox_id).await,
        Some(SandboxState::Running)
    );
}

#[tokio::test]
async fn a_connect_during_a_pause_waits_for_it_and_rebuilds_from_the_row() {
    let surface = Surface::api_half().await;
    let sandbox_id = surface.paused(mock_paused_sandbox_config());
    pause_in_flight(&surface, sandbox_id).await;
    finish_pause_later(&surface, sandbox_id);

    let response = surface.connect(sandbox_id).await;

    assert!(
        matches!(
            response,
            SandboxesSandboxIdConnectPostResponse::Status201_TheSandboxWasResumedSuccessfully { .. }
        ),
        "the connect waited for the pause to finish and rebuilt from the row, got {response:?}"
    );
    assert_eq!(
        surface.state_of(sandbox_id).await,
        Some(SandboxState::Running)
    );
}

async fn connect_over_a_running_record(routed: bool) -> SandboxesSandboxIdConnectPostResponse {
    let surface = Surface::as_half_routed(
        ResumeWiring::api_half_for_test(),
        Some(Arc::new(FixedRouting(routed))),
    )
    .await;
    let sandbox_id = surface.paused(mock_paused_sandbox_config());
    surface
        .orchestrator
        .set_metadata_state_for_test(sandbox_id, SandboxState::Running)
        .await
        .expect("a running record should be writable");

    surface.connect(sandbox_id).await
}

#[tokio::test]
async fn a_connect_to_a_runtime_nothing_routes_to_resumes_instead_of_answering_running() {
    let response = connect_over_a_running_record(false).await;

    assert!(
        matches!(
            response,
            SandboxesSandboxIdConnectPostResponse::Status201_TheSandboxWasResumedSuccessfully { .. }
        ),
        "a record whose runtime the cluster cannot reach must not answer as running, \
         got {response:?}"
    );
}

#[tokio::test]
async fn a_connect_to_a_routed_runtime_still_answers_running() {
    let response = connect_over_a_running_record(true).await;

    assert!(
        matches!(
            response,
            SandboxesSandboxIdConnectPostResponse::Status200_TheSandboxWasAlreadyRunning { .. }
        ),
        "a sandbox the cluster still routes to is running, got {response:?}"
    );
}

#[tokio::test]
async fn a_resume_during_a_pause_that_fails_answers_the_sandbox_that_kept_running() {
    let surface = Surface::api_half().await;
    let sandbox_id = surface.paused(mock_paused_sandbox_config());
    pause_in_flight(&surface, sandbox_id).await;
    fail_pause_later(&surface, sandbox_id);

    let response = surface.resume(sandbox_id).await;

    assert!(
        matches!(
            response,
            SandboxesSandboxIdResumePostResponse::Status201_TheSandboxWasResumedSuccessfully { .. }
        ),
        "a pause that failed leaves the sandbox running, and the resume answers it as it \
         stands, got {response:?}"
    );
    assert!(
        matches!(
            surface.connect(sandbox_id).await,
            SandboxesSandboxIdConnectPostResponse::Status200_TheSandboxWasAlreadyRunning { .. }
        ),
        "nothing was rebuilt over the sandbox that kept running"
    );
}

fn paused_owned_by(owner: &str) -> PausedSandboxConfig {
    PausedSandboxConfig {
        user_metadata: Some(
            [("owner".to_string(), owner.to_string())]
                .into_iter()
                .collect(),
        ),
        ..mock_paused_sandbox_config()
    }
}

#[tokio::test]
async fn both_halves_of_one_listing_keep_the_same_sandboxes_for_a_metadata_filter() {
    let surface = Surface::api_half().await;
    let paused_keep = surface.paused(paused_owned_by("keep"));
    let paused_drop = surface.paused(paused_owned_by("drop"));
    let running_keep = surface.paused(paused_owned_by("keep"));
    let running_drop = surface.paused(paused_owned_by("drop"));
    let _ = surface.resume(running_keep).await;
    let _ = surface.resume(running_drop).await;

    let listed = surface
        .list_v2_matching(Vec::new(), Some("owner=keep"))
        .await;
    let mut got: Vec<(String, models::SandboxState)> = listed
        .iter()
        .map(|sandbox| (sandbox.sandbox_id.clone(), sandbox.state))
        .collect();
    got.sort();
    let mut expected = vec![
        (paused_keep.to_string(), models::SandboxState::Paused),
        (running_keep.to_string(), models::SandboxState::Running),
    ];
    expected.sort();
    assert_eq!(
        got, expected,
        "one filter, both halves: {paused_drop} and {running_drop} carry another owner"
    );
}

#[tokio::test]
async fn a_metadata_filter_the_paused_half_answers_alone_still_drops_the_others() {
    let surface = Surface::api_half().await;
    let keep = surface.paused(paused_owned_by("keep"));
    let other = surface.paused(paused_owned_by("drop"));
    let unlabelled = surface.paused(mock_paused_sandbox_config());

    let listed = surface
        .list_v2_matching(vec![models::SandboxState::Paused], Some("owner=keep"))
        .await;

    assert_eq!(
        listed
            .iter()
            .map(|sandbox| sandbox.sandbox_id.clone())
            .collect::<Vec<_>>(),
        vec![keep.to_string()],
        "a sandbox that carries no metadata cannot match a filter that names some, \
         and neither can {other} or {unlabelled}"
    );
}
