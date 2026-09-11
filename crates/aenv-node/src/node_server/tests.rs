//! Node service behavior driven through the generated server trait.

use std::sync::Arc;
use std::time::Duration;

use tonic::{Code, Request, Status};

use crate::orchestrator::{
    ControlPlaneConfig, CreateSandboxRequest, ForkChildAssignment, ForkChildren,
    InMemoryMetadataStore, MetadataStore, NewTimeout, NodeOrchestration, Orchestrator,
    SandboxExpiry, SandboxLaunchSource, SandboxMetadata, SandboxTimeoutAction,
    StagingPausePublisher,
};
use crate::proto::node as pb;
use crate::proto::node::node_sandbox_service_server::NodeSandboxService as _;
use crate::runtime_snapshot::RunnableSnapshot;
use crate::sandbox::mock::{MockAction, MockBackendFactory, MockBehavior, MockOperation};
use crate::sandbox::{
    RuntimeArtifactSet, SandboxBackendFactory, SandboxNetworkPolicy, SandboxRuntimeInfo,
};
use crate::snapshot::mock::{mock_snapshot_manager, recording_snapshot_manager};
use crate::snapshot::SnapshotManager;
use crate::types::{ExecutionId, SandboxId};

use super::service::NodeSandboxService;

const NODE: &str = "node-under-test";

async fn service() -> (Arc<dyn NodeOrchestration>, NodeSandboxService) {
    service_with(MockBackendFactory::new()).await
}

async fn service_with(
    factory: MockBackendFactory,
) -> (Arc<dyn NodeOrchestration>, NodeSandboxService) {
    let manager = Arc::new(mock_snapshot_manager());
    let orchestrator = orchestrator_with(InMemoryMetadataStore::new(), factory, &manager).await;
    serve_node(orchestrator, manager)
}

async fn orchestrator_with<S, F>(
    store: S,
    factory: F,
    manager: &Arc<SnapshotManager>,
) -> Arc<Orchestrator<S, F>>
where
    S: MetadataStore + 'static,
    F: SandboxBackendFactory,
{
    crate::logging::init_for_tests();
    Orchestrator::new(
        crate::sandbox::AccessTokenSeedPolicy::MayGenerate,
        store,
        factory,
        crate::image::DisabledRuntimeImageRefs::shared(),
        // The node's wiring: pauses stage on the same repository the service
        // answers from.
        Arc::new(StagingPausePublisher::new(Arc::clone(manager))),
        crate::orchestrator::GrantsIssuedUpstream::shared(),
    )
    .await
    .expect("an in-memory orchestrator")
}

fn serve_node<S, F>(
    orchestrator: Arc<Orchestrator<S, F>>,
    manager: Arc<SnapshotManager>,
) -> (Arc<dyn NodeOrchestration>, NodeSandboxService)
where
    S: MetadataStore + 'static,
    F: SandboxBackendFactory,
{
    let orchestration: Arc<dyn NodeOrchestration> = orchestrator;
    let service = NodeSandboxService::new(Arc::clone(&orchestration), manager, NODE.to_string());
    (orchestration, service)
}

async fn service_with_catalog() -> (
    Arc<dyn NodeOrchestration>,
    NodeSandboxService,
    Arc<crate::snapshot::mock::MockSnapshotCatalog>,
) {
    let (manager, catalog) = crate::snapshot::mock::mock_snapshot_manager_with_catalog();
    let manager = Arc::new(manager);
    let orchestrator = orchestrator_with(
        InMemoryMetadataStore::new(),
        MockBackendFactory::new(),
        &manager,
    )
    .await;
    let (orchestration, service) = serve_node(orchestrator, manager);
    (orchestration, service, catalog)
}

fn launch(marker: Option<&[u8]>) -> CreateSandboxRequest {
    CreateSandboxRequest {
        traffic_access_token: None,
        source: SandboxLaunchSource::Snapshot(Box::new(RunnableSnapshot::mock())),
        expiry: SandboxExpiry::After(Duration::from_secs(60)),
        timeout_action: SandboxTimeoutAction::Pause,
        user_metadata: None,
        env_vars: None,
        network_policy: SandboxNetworkPolicy::default(),
        custom_extension_params: None,
        control_plane_config: marker
            .and_then(|bytes| ControlPlaneConfig::from_bytes(bytes.to_vec())),
        execution_id: None,
        auto_resume: false,
        secure: false,
        preferred_node_id: None,
    }
}

async fn start(
    orchestration: &Arc<dyn NodeOrchestration>,
    marker: Option<&[u8]>,
) -> SandboxMetadata {
    Arc::clone(orchestration)
        .create_sandbox(launch(marker))
        .await
        .expect("the mock backend starts")
}

async fn listed(service: &NodeSandboxService) -> Vec<pb::NodeSandbox> {
    service
        .list_sandboxes(Request::new(pb::ListSandboxesRequest {}))
        .await
        .expect("a node that looked can answer")
        .into_inner()
        .sandboxes
}

fn resolved_snapshot_source() -> pb::SnapshotSource {
    let record =
        crate::snapshot::SnapshotRecord::mock_ready(crate::snapshot::CommittedSnapshot::mock());
    pb::SnapshotSource {
        snapshot_id: record.id.to_string(),
        resolved_record: Some(pb::encode_value(&record).expect("encode should succeed")),
    }
}

#[tokio::test]
async fn only_the_control_planes_own_sandboxes_are_listed() {
    let (orchestration, service) = service().await;

    let owned = start(&orchestration, Some(b"the-control-planes-record")).await;
    let unowned = start(&orchestration, None).await;

    let sandboxes = listed(&service).await;

    assert_eq!(sandboxes.len(), 1, "reported: {sandboxes:?}");
    assert_eq!(sandboxes[0].sandbox_id, owned.id.to_string());
    assert_eq!(
        sandboxes[0].control_plane_config,
        b"the-control-planes-record"
    );
    assert_eq!(sandboxes[0].node_id, NODE);
    assert!(
        !sandboxes
            .iter()
            .any(|reported| reported.sandbox_id == unowned.id.to_string()),
        "a sandbox nobody claimed was offered to the control plane"
    );
}

#[tokio::test]
async fn a_node_running_nothing_the_control_plane_owns_reports_nothing() {
    let (orchestration, service) = service().await;

    for _ in 0..3 {
        start(&orchestration, None).await;
    }

    assert!(
        listed(&service).await.is_empty(),
        "unmarked sandboxes reached the control plane's listing"
    );

    let owned = start(&orchestration, Some(b"the-fourth-one")).await;
    let sandboxes = listed(&service).await;
    assert_eq!(sandboxes.len(), 1, "reported: {sandboxes:?}");
    assert_eq!(sandboxes[0].sandbox_id, owned.id.to_string());
}

#[tokio::test]
async fn a_node_running_nothing_at_all_answers_with_an_empty_list() {
    let (orchestration, service) = service().await;
    assert!(listed(&service).await.is_empty());

    let owned = start(&orchestration, Some(b"now-there-is-one")).await;
    assert_eq!(
        listed(&service)
            .await
            .into_iter()
            .map(|reported| reported.sandbox_id)
            .collect::<Vec<_>>(),
        vec![owned.id.to_string()]
    );
}

#[tokio::test]
async fn a_sandbox_that_is_gone_leaves_the_listing() {
    let (orchestration, service) = service().await;

    let staying = start(&orchestration, Some(b"staying")).await;
    let going = start(&orchestration, Some(b"going")).await;
    assert_eq!(listed(&service).await.len(), 2);

    Arc::clone(&orchestration)
        .delete_sandbox(going.id)
        .await
        .expect("delete");

    let sandboxes = listed(&service).await;
    assert_eq!(sandboxes.len(), 1);
    assert_eq!(sandboxes[0].sandbox_id, staying.id.to_string());
}

#[tokio::test]
async fn a_partial_record_read_fails_the_listing_instead_of_shortening_it() {
    use crate::orchestrator::{MetadataRows, MetadataStore, MetadataUpdateResult, StoreError};

    struct HalfAnswering(InMemoryMetadataStore);

    #[async_trait::async_trait]
    impl MetadataStore for HalfAnswering {
        async fn add(&self, metadata: SandboxMetadata) -> Result<(), StoreError> {
            self.0.add(metadata).await
        }
        async fn update(&self, metadata: SandboxMetadata) -> Result<(), StoreError> {
            self.0.update(metadata).await
        }
        async fn update_state_if_state(
            &self,
            sandbox_id: &crate::types::SandboxId,
            new_state: crate::orchestrator::SandboxState,
            expected_states: &[crate::orchestrator::SandboxState],
        ) -> Result<crate::orchestrator::SandboxState, StoreError> {
            self.0
                .update_state_if_state(sandbox_id, new_state, expected_states)
                .await
        }
        async fn update_if_state<F>(
            &self,
            sandbox_id: &crate::types::SandboxId,
            expected_states: &[crate::orchestrator::SandboxState],
            update: F,
        ) -> Result<MetadataUpdateResult, StoreError>
        where
            F: FnOnce(&mut SandboxMetadata) + Send,
        {
            self.0
                .update_if_state(sandbox_id, expected_states, update)
                .await
        }
        async fn get(
            &self,
            sandbox_id: &crate::types::SandboxId,
        ) -> Result<Option<SandboxMetadata>, StoreError> {
            self.0.get(sandbox_id).await
        }
        async fn remove(
            &self,
            sandbox_id: &crate::types::SandboxId,
        ) -> Result<Option<SandboxMetadata>, StoreError> {
            self.0.remove(sandbox_id).await
        }
        async fn remove_if_execution(
            &self,
            sandbox_id: &crate::types::SandboxId,
            expected_execution_id: crate::types::ExecutionId,
            expected_states: &[crate::orchestrator::SandboxState],
        ) -> Result<crate::orchestrator::FencedRemoval, StoreError> {
            self.0
                .remove_if_execution(sandbox_id, expected_execution_id, expected_states)
                .await
        }
        async fn list(&self) -> Result<Vec<SandboxMetadata>, StoreError> {
            self.0.list().await
        }
        async fn list_with_callback<F>(&self, callback: F) -> Result<(), StoreError>
        where
            F: FnMut(&SandboxMetadata) + Send,
        {
            self.0.list_with_callback(callback).await
        }
        async fn list_filtered(
            &self,
            filter: crate::orchestrator::SandboxListFilter,
        ) -> Result<Vec<SandboxMetadata>, StoreError> {
            self.0.list_filtered(filter).await
        }
        async fn list_expired(
            &self,
            now: std::time::SystemTime,
        ) -> Result<Vec<SandboxMetadata>, StoreError> {
            self.0.list_expired(now).await
        }
        async fn list_ids(&self) -> Result<Vec<crate::types::SandboxId>, StoreError> {
            self.0.list_ids().await
        }
        async fn wait_while_in_states(
            &self,
            sandbox_id: &crate::types::SandboxId,
            transitional_states: &[crate::orchestrator::SandboxState],
        ) -> Result<Option<SandboxMetadata>, StoreError> {
            self.0
                .wait_while_in_states(sandbox_id, transitional_states)
                .await
        }

        async fn get_many(
            &self,
            ids: &[crate::types::SandboxId],
        ) -> Result<MetadataRows, StoreError> {
            let mut rows = self.0.get_many(ids).await?;
            if let Some(dropped) = ids.first() {
                rows.entries.remove(dropped);
                rows.covered.retain(|covered| covered != dropped);
            }
            Ok(rows)
        }
    }

    let manager = Arc::new(mock_snapshot_manager());
    let orchestrator = orchestrator_with(
        HalfAnswering(InMemoryMetadataStore::new()),
        MockBackendFactory::new(),
        &manager,
    )
    .await;
    let (orchestration, service) = serve_node(orchestrator, manager);

    start(&orchestration, Some(b"owned-a")).await;
    start(&orchestration, Some(b"owned-b")).await;

    let err = service
        .list_sandboxes(Request::new(pb::ListSandboxesRequest {}))
        .await
        .expect_err("a listing built on a partial read must not be served");
    assert_ne!(
        err.code(),
        Code::NotFound,
        "a partial read is not an absence: {err}"
    );
}

#[tokio::test]
async fn a_paused_sandbox_leaves_the_listing_and_keeps_no_record() {
    let harness = staging_service().await;
    let (orchestration, service) = (&harness.orchestration, &harness.service);
    let sandbox = start(orchestration, Some(b"owned")).await;
    let staying = start(orchestration, Some(b"also-owned")).await;
    assert_eq!(listed(service).await.len(), 2);

    Arc::clone(orchestration)
        .pause_sandbox(sandbox.id)
        .await
        .expect("the mock backend pauses");

    assert!(
        Arc::clone(orchestration)
            .get_sandbox(&sandbox.id)
            .await
            .expect("read")
            .is_none(),
        "a paused sandbox exists only as its snapshot"
    );

    let sandboxes = listed(service).await;
    assert_eq!(
        sandboxes.len(),
        1,
        "a paused sandbox was reported as running: {sandboxes:?}"
    );
    assert_eq!(sandboxes[0].sandbox_id, staying.id.to_string());
}

#[tokio::test]
async fn the_listing_names_the_run_that_is_live() {
    let (orchestration, service) = service().await;
    let sandbox = start(&orchestration, Some(b"owned")).await;

    let sandboxes = listed(&service).await;
    assert_eq!(sandboxes.len(), 1);
    assert_eq!(sandboxes[0].execution_id, sandbox.execution_id.to_string());
    assert_ne!(sandboxes[0].execution_id, "");
}

#[tokio::test]
async fn the_listing_reports_the_incarnation_the_handle_is_running() {
    use crate::sandbox::{FreshSandboxBuildSpec, SandboxBackend, SandboxLaunchConfig};

    struct Drifting {
        inner: MockBackendFactory,
        drifted: Arc<std::sync::Mutex<Vec<ExecutionId>>>,
    }

    impl SandboxBackendFactory for Drifting {
        fn build(
            &self,
            build_spec: FreshSandboxBuildSpec,
            launch_config: SandboxLaunchConfig,
            _execution_id: ExecutionId,
        ) -> anyhow::Result<Box<dyn SandboxBackend>> {
            let drifted = ExecutionId::new();
            self.drifted.lock().expect("lock").push(drifted);
            self.inner.build(build_spec, launch_config, drifted)
        }
        fn build_from_snapshot(
            &self,
            snapshot: &RunnableSnapshot,
            launch_config: SandboxLaunchConfig,
            _execution_id: ExecutionId,
        ) -> anyhow::Result<Box<dyn SandboxBackend>> {
            let drifted = ExecutionId::new();
            self.drifted.lock().expect("lock").push(drifted);
            self.inner
                .build_from_snapshot(snapshot, launch_config, drifted)
        }
    }

    let drifted = Arc::new(std::sync::Mutex::new(Vec::new()));
    let manager = Arc::new(mock_snapshot_manager());
    let orchestrator = orchestrator_with(
        InMemoryMetadataStore::new(),
        Drifting {
            inner: MockBackendFactory::new(),
            drifted: Arc::clone(&drifted),
        },
        &manager,
    )
    .await;
    let (orchestration, service) = serve_node(orchestrator, manager);

    let record = start(&orchestration, Some(b"owned")).await;
    let running = *drifted.lock().expect("lock").first().expect("one backend");
    assert_ne!(
        record.execution_id, running,
        "the record and the handle must disagree, or this test proves nothing"
    );

    let sandboxes = listed(&service).await;
    assert_eq!(sandboxes.len(), 1);
    assert_eq!(
        sandboxes[0].execution_id,
        running.to_string(),
        "the listing named the filed run rather than the one that is up"
    );
}

#[tokio::test]
async fn a_sandbox_whose_handle_is_busy_is_still_reported() {
    use crate::sandbox::mock::{MockAction, MockBehavior, MockOperation};

    let behavior = Arc::new(MockBehavior::new());
    let manager = Arc::new(mock_snapshot_manager());
    let orchestrator = orchestrator_with(
        InMemoryMetadataStore::new(),
        MockBackendFactory::with_behavior(Arc::clone(&behavior)),
        &manager,
    )
    .await;
    let (orchestration, service) = serve_node(orchestrator, manager);

    let busy = start(&orchestration, Some(b"busy")).await;
    let idle = start(&orchestration, Some(b"idle")).await;

    behavior.push_action(
        MockOperation::Fork,
        MockAction::SucceedAfter(Duration::from_secs(3)),
    );
    let forking = tokio::spawn({
        let orchestration = Arc::clone(&orchestration);
        async move {
            orchestration
                .fork_sandbox(busy.id, ForkChildren::Fresh(1), NewTimeout::UseExisting)
                .await
        }
    });
    // Fork delay keeps the source handle busy after this short wait.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let sandboxes = listed(&service).await;
    let reported: Vec<&str> = sandboxes
        .iter()
        .map(|sandbox| sandbox.sandbox_id.as_str())
        .collect();
    assert!(
        reported.contains(&busy.id.to_string().as_str()),
        "a sandbox mid-operation was dropped from the listing: {reported:?}"
    );
    assert!(
        reported.contains(&idle.id.to_string().as_str()),
        "the idle sandbox went missing too: {reported:?}"
    );

    let busy_entry = sandboxes
        .iter()
        .find(|sandbox| sandbox.sandbox_id == busy.id.to_string())
        .expect("the busy sandbox");
    assert_eq!(busy_entry.execution_id, busy.execution_id.to_string());

    let _ = forking.await;
}

#[tokio::test]
async fn a_create_carrying_an_empty_marker_produces_an_unowned_sandbox() {
    let (orchestration, service) = service().await;

    let response = service
        .create(Request::new(pb::SandboxCreateRequest {
            sandbox_id: SandboxId::new().to_string(),
            source: Some(pb::sandbox_create_request::Source::Snapshot(
                resolved_snapshot_source(),
            )),
            expiry: Some(pb::sandbox_create_request::Expiry::NodeKeptTimeoutMs(
                60_000,
            )),
            timeout_action: pb::TimeoutAction::Pause as i32,
            control_plane_config: Vec::new(),
            ..Default::default()
        }))
        .await;

    if let Ok(response) = response {
        let created = response.into_inner();
        assert!(!created.sandbox_id.is_empty());
    }

    let sandbox = start(&orchestration, Some(b"")).await;
    assert!(
        sandbox.control_plane_config.is_none(),
        "an empty blob became a marker"
    );
    assert!(listed(&service).await.is_empty());

    let owned = start(&orchestration, Some(b"\0")).await;
    let sandboxes = listed(&service).await;
    assert_eq!(sandboxes.len(), 1, "reported: {sandboxes:?}");
    assert_eq!(sandboxes[0].sandbox_id, owned.id.to_string());
    assert_eq!(sandboxes[0].control_plane_config, b"\0");
}

#[tokio::test]
async fn a_create_that_does_not_say_who_keeps_the_deadline_is_refused_before_the_resolver() {
    let (orchestration, service) = service().await;
    let source = resolved_snapshot_source();
    let request = |expiry| pb::SandboxCreateRequest {
        sandbox_id: SandboxId::new().to_string(),
        source: Some(pb::sandbox_create_request::Source::Snapshot(source.clone())),
        expiry,
        timeout_action: pb::TimeoutAction::Pause as i32,
        control_plane_config: b"owned".to_vec(),
        ..Default::default()
    };

    let silent = service
        .create(Request::new(request(None)))
        .await
        .expect_err("a create that said nothing about expiry");
    assert_eq!(silent.code(), Code::InvalidArgument, "{silent}");
    assert!(silent.message().contains("expiry is required"), "{silent}");

    let answered = service
        .create(Request::new(request(Some(
            pb::sandbox_create_request::Expiry::CallerKept(pb::CallerKeptExpiry {}),
        ))))
        .await
        .expect_err("the mock catalog has no snapshots");
    assert!(
        matches!(answered.code(), Code::NotFound | Code::Internal),
        "{answered}"
    );

    assert!(listed(&service).await.is_empty());
    assert!(Arc::clone(&orchestration)
        .list_sandboxes()
        .await
        .expect("list")
        .is_empty());
}

#[tokio::test]
async fn a_create_uses_the_id_the_caller_chose() {
    let (orchestration, service) = service().await;
    let sandbox_id = SandboxId::new();

    let err = service
        .create(Request::new(pb::SandboxCreateRequest {
            sandbox_id: sandbox_id.to_string(),
            source: Some(pb::sandbox_create_request::Source::Snapshot(
                resolved_snapshot_source(),
            )),
            expiry: Some(pb::sandbox_create_request::Expiry::CallerKept(
                pb::CallerKeptExpiry {},
            )),
            timeout_action: pb::TimeoutAction::Pause as i32,
            control_plane_config: b"owned".to_vec(),
            ..Default::default()
        }))
        .await
        .expect_err("the mock runtime resolver fails every call");
    assert!(
        matches!(err.code(), Code::NotFound | Code::Internal),
        "{err}"
    );
    assert!(
        Arc::clone(&orchestration)
            .get_sandbox(&sandbox_id)
            .await
            .expect("read")
            .is_none(),
        "a refused create left a record behind"
    );
}

#[tokio::test]
async fn a_resolved_snapshot_source_skips_the_nodes_own_catalog_lookup() {
    let (_orchestration, service, catalog) = service_with_catalog().await;
    let record =
        crate::snapshot::SnapshotRecord::mock_ready(crate::snapshot::CommittedSnapshot::mock());
    let resolved_record = pb::encode_value(&record).expect("encode should succeed");

    let err = service
        .create(Request::new(pb::SandboxCreateRequest {
            sandbox_id: SandboxId::new().to_string(),
            source: Some(pb::sandbox_create_request::Source::Snapshot(
                pb::SnapshotSource {
                    snapshot_id: record.id.to_string(),
                    resolved_record: Some(resolved_record),
                },
            )),
            expiry: Some(pb::sandbox_create_request::Expiry::CallerKept(
                pb::CallerKeptExpiry {},
            )),
            timeout_action: pb::TimeoutAction::Pause as i32,
            control_plane_config: b"owned".to_vec(),
            ..Default::default()
        }))
        .await
        .expect_err("the mock runtime resolver fails every call");
    assert_eq!(err.code(), Code::Internal);
    assert!(
        err.message().contains("runtime resolver"),
        "a resolved_record must reach resolve_runnable directly, never the catalog: {err}"
    );
    assert_eq!(
        catalog.get_calls(),
        0,
        "a resolved_record must never touch the node's own catalog"
    );
}

#[tokio::test]
async fn an_absent_resolved_record_is_refused_without_a_catalog_lookup() {
    let (orchestration, mut service, catalog) = service_with_catalog().await;

    let err = service
        .create(Request::new(pb::SandboxCreateRequest {
            sandbox_id: SandboxId::new().to_string(),
            source: Some(pb::sandbox_create_request::Source::Snapshot(
                pb::SnapshotSource {
                    snapshot_id: "no-such-snapshot".to_string(),
                    resolved_record: None,
                },
            )),
            expiry: Some(pb::sandbox_create_request::Expiry::CallerKept(
                pb::CallerKeptExpiry {},
            )),
            timeout_action: pb::TimeoutAction::Pause as i32,
            control_plane_config: b"owned".to_vec(),
            ..Default::default()
        }))
        .await
        .expect_err("an absent resolved_record must be refused");
    assert_eq!(err.code(), Code::InvalidArgument, "{err}");
    assert!(
        err.message().contains("resolved_record is required"),
        "the refusal must name the field the sender owes: {err}"
    );
    assert_eq!(
        catalog.get_calls(),
        0,
        "an absent resolved_record must be refused, never fall back to a catalog lookup"
    );

    assert!(listed(&service).await.is_empty());
    assert!(Arc::clone(&orchestration)
        .list_sandboxes()
        .await
        .expect("list")
        .is_empty());

    service = service.with_template_build(
        Arc::new(crate::image::ImageResolver::new(
            &crate::cfg::AppConfig::default(),
        )),
        Arc::new(crate::template::TemplateBuilder::new()),
    );
    let err = service
        .build_template(Request::new(pb::TemplateBuildRequest {
            build_snapshot_id: crate::snapshot::SnapshotId::generate().to_string(),
            base: Some(pb::template_build_request::Base::BaseSnapshotRef(
                "no-such-alias".to_string(),
            )),
            base_snapshot_resolved: None,
            steps: None,
            resources: Some(pb::SandboxResources {
                cpu_count: 1,
                memory_mib: 512,
                disk_size_mib: 1024,
            }),
            start_cmd: String::new(),
            ready_cmd: String::new(),
        }))
        .await
        .expect_err("an absent base_snapshot_resolved must be refused");
    assert_eq!(err.code(), Code::InvalidArgument, "{err}");
    assert!(
        err.message().contains("base_snapshot_resolved is required"),
        "the refusal must name the field the sender owes: {err}"
    );
    assert_eq!(
        catalog.get_calls(),
        0,
        "an absent base_snapshot_resolved must be refused, never fall back to a catalog lookup"
    );
}

#[tokio::test]
async fn a_resolved_record_naming_a_different_snapshot_is_refused() {
    let (_orchestration, service) = service().await;
    let record =
        crate::snapshot::SnapshotRecord::mock_ready(crate::snapshot::CommittedSnapshot::mock());
    let resolved_record = pb::encode_value(&record).expect("encode should succeed");

    let err = service
        .create(Request::new(pb::SandboxCreateRequest {
            sandbox_id: SandboxId::new().to_string(),
            source: Some(pb::sandbox_create_request::Source::Snapshot(
                pb::SnapshotSource {
                    snapshot_id: "a-different-snapshot-id".to_string(),
                    resolved_record: Some(resolved_record),
                },
            )),
            expiry: Some(pb::sandbox_create_request::Expiry::CallerKept(
                pb::CallerKeptExpiry {},
            )),
            timeout_action: pb::TimeoutAction::Pause as i32,
            control_plane_config: b"owned".to_vec(),
            ..Default::default()
        }))
        .await
        .expect_err("a resolved_record naming a different snapshot must be refused");
    assert_eq!(err.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn a_resolved_base_snapshot_skips_the_nodes_own_catalog_lookup() {
    let (orchestration, mut service, catalog) = service_with_catalog().await;
    service = service.with_template_build(
        Arc::new(crate::image::ImageResolver::new(
            &crate::cfg::AppConfig::default(),
        )),
        Arc::new(crate::template::TemplateBuilder::new()),
    );
    let _ = &orchestration;

    let record =
        crate::snapshot::SnapshotRecord::mock_ready(crate::snapshot::CommittedSnapshot::mock());
    let base_snapshot_resolved = pb::encode_value(&record).expect("encode should succeed");

    let err = service
        .build_template(Request::new(pb::TemplateBuildRequest {
            build_snapshot_id: crate::snapshot::SnapshotId::generate().to_string(),
            base: Some(pb::template_build_request::Base::BaseSnapshotRef(
                record.id.to_string(),
            )),
            base_snapshot_resolved: Some(base_snapshot_resolved),
            steps: None,
            resources: Some(pb::SandboxResources {
                cpu_count: 1,
                memory_mib: 512,
                disk_size_mib: 1024,
            }),
            start_cmd: String::new(),
            ready_cmd: String::new(),
        }))
        .await
        .expect_err("the mock runtime resolver fails every call");
    assert_eq!(err.code(), Code::Internal);
    assert!(
        err.message().contains("runtime resolver"),
        "base_snapshot_resolved must reach resolve_runnable directly, never the catalog: {err}"
    );
    assert_eq!(
        catalog.get_calls(),
        0,
        "base_snapshot_resolved must never touch the node's own catalog"
    );
}

#[tokio::test]
async fn a_base_snapshot_ref_naming_a_different_snapshot_is_refused() {
    let (orchestration, mut service, _catalog) = service_with_catalog().await;
    service = service.with_template_build(
        Arc::new(crate::image::ImageResolver::new(
            &crate::cfg::AppConfig::default(),
        )),
        Arc::new(crate::template::TemplateBuilder::new()),
    );
    let _ = &orchestration;

    let record =
        crate::snapshot::SnapshotRecord::mock_ready(crate::snapshot::CommittedSnapshot::mock());
    let base_snapshot_resolved = pb::encode_value(&record).expect("encode should succeed");

    let err = service
        .build_template(Request::new(pb::TemplateBuildRequest {
            build_snapshot_id: crate::snapshot::SnapshotId::generate().to_string(),
            base: Some(pb::template_build_request::Base::BaseSnapshotRef(
                "a-different-snapshot-id".to_string(),
            )),
            base_snapshot_resolved: Some(base_snapshot_resolved),
            steps: None,
            resources: Some(pb::SandboxResources {
                cpu_count: 1,
                memory_mib: 512,
                disk_size_mib: 1024,
            }),
            start_cmd: String::new(),
            ready_cmd: String::new(),
        }))
        .await
        .expect_err("a base_snapshot_resolved naming a different snapshot must be refused");
    assert_eq!(err.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn a_base_snapshot_ref_matching_the_records_alias_is_accepted() {
    let (orchestration, mut service, catalog) = service_with_catalog().await;
    service = service.with_template_build(
        Arc::new(crate::image::ImageResolver::new(
            &crate::cfg::AppConfig::default(),
        )),
        Arc::new(crate::template::TemplateBuilder::new()),
    );
    let _ = &orchestration;

    let mut record =
        crate::snapshot::SnapshotRecord::mock_ready(crate::snapshot::CommittedSnapshot::mock());
    record.alias = Some(crate::snapshot::SnapshotAlias::parse("my-template").unwrap());
    let base_snapshot_resolved = pb::encode_value(&record).expect("encode should succeed");

    let err = service
        .build_template(Request::new(pb::TemplateBuildRequest {
            build_snapshot_id: crate::snapshot::SnapshotId::generate().to_string(),
            base: Some(pb::template_build_request::Base::BaseSnapshotRef(
                "my-template".to_string(),
            )),
            base_snapshot_resolved: Some(base_snapshot_resolved),
            steps: None,
            resources: Some(pb::SandboxResources {
                cpu_count: 1,
                memory_mib: 512,
                disk_size_mib: 1024,
            }),
            start_cmd: String::new(),
            ready_cmd: String::new(),
        }))
        .await
        .expect_err("the mock runtime resolver fails every call");
    assert_eq!(
        err.code(),
        Code::Internal,
        "an alias match must reach resolve_runnable, not be refused as a mismatch: {err}"
    );
    assert!(
        err.message().contains("runtime resolver"),
        "an alias match must reach resolve_runnable directly, never the catalog: {err}"
    );
    assert_eq!(
        catalog.get_calls(),
        0,
        "an alias match must never touch the node's own catalog"
    );
}

#[tokio::test]
async fn a_create_missing_what_it_needs_is_refused() {
    let (_orchestration, service) = service().await;

    let err = service
        .create(Request::new(pb::SandboxCreateRequest {
            sandbox_id: SandboxId::new().to_string(),
            source: None,
            timeout_action: pb::TimeoutAction::Pause as i32,
            ..Default::default()
        }))
        .await
        .expect_err("a create with no source");
    assert_eq!(err.code(), Code::InvalidArgument);

    let err = service
        .create(Request::new(pb::SandboxCreateRequest {
            sandbox_id: SandboxId::new().to_string(),
            source: Some(pb::sandbox_create_request::Source::Snapshot(
                resolved_snapshot_source(),
            )),
            timeout_action: pb::TimeoutAction::Unspecified as i32,
            ..Default::default()
        }))
        .await
        .expect_err("a create that did not say what its timeout does");
    assert_eq!(err.code(), Code::InvalidArgument);
    assert!(
        err.message().contains("timeout_action"),
        "the refusal must name the timeout action, not something else: {err}"
    );
}

#[tokio::test]
async fn a_command_for_a_superseded_run_is_refused_and_changes_nothing() {
    let (orchestration, service) = service().await;
    let sandbox = start(&orchestration, Some(b"owned")).await;

    let err = service
        .delete(Request::new(pb::SandboxDeleteRequest {
            sandbox_id: sandbox.id.to_string(),
            execution_id: ExecutionId::new().to_string(),
        }))
        .await
        .expect_err("a delete naming another run");
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert_eq!(listed(&service).await.len(), 1, "the sandbox was torn down");

    service
        .delete(Request::new(pb::SandboxDeleteRequest {
            sandbox_id: sandbox.id.to_string(),
            execution_id: sandbox.execution_id.to_string(),
        }))
        .await
        .expect("a delete naming the live run");
    assert!(listed(&service).await.is_empty());
}

#[tokio::test]
async fn a_command_for_an_unknown_sandbox_is_not_found() {
    let (_orchestration, service) = service().await;

    let err = service
        .delete(Request::new(pb::SandboxDeleteRequest {
            sandbox_id: SandboxId::new().to_string(),
            execution_id: ExecutionId::new().to_string(),
        }))
        .await
        .expect_err("a delete for a sandbox that is not here");
    assert_eq!(err.code(), Code::NotFound);
}

#[tokio::test]
async fn a_fork_pairs_children_with_the_markers_they_were_given() {
    let (orchestration, service) = service().await;
    let source = start(&orchestration, Some(b"the-source")).await;

    let children = (0..3)
        .map(|index| pb::ForkChildSpec {
            sandbox_id: SandboxId::new().to_string(),
            execution_id: String::new(),
            control_plane_config: format!("child-{index}").into_bytes(),
        })
        .collect::<Vec<_>>();

    let response = service
        .fork(Request::new(pb::SandboxForkRequest {
            source_sandbox_id: source.id.to_string(),
            source_execution_id: source.execution_id.to_string(),
            children: children.clone(),
            timeout_ms: 0,
        }))
        .await
        .expect("fork")
        .into_inner();

    assert_eq!(response.children.len(), children.len());
    for (result, requested) in response.children.iter().zip(&children) {
        assert_eq!(result.sandbox_id, requested.sandbox_id);
        assert!(matches!(
            result.outcome,
            Some(pb::fork_child_result::Outcome::Started(_))
        ));
    }

    let sandboxes = listed(&service).await;
    assert_eq!(sandboxes.len(), 4, "the source and its three children");
    for requested in &children {
        let reported = sandboxes
            .iter()
            .find(|reported| reported.sandbox_id == requested.sandbox_id)
            .expect("a forked child that never reached the listing");
        assert_eq!(
            reported.control_plane_config, requested.control_plane_config,
            "a child came back under another child's record"
        );
    }
}

#[tokio::test]
async fn a_forked_child_with_no_marker_is_not_reported() {
    let (orchestration, service) = service().await;
    let source = start(&orchestration, Some(b"the-source")).await;

    let child_id = SandboxId::new();
    service
        .fork(Request::new(pb::SandboxForkRequest {
            source_sandbox_id: source.id.to_string(),
            source_execution_id: source.execution_id.to_string(),
            children: vec![pb::ForkChildSpec {
                sandbox_id: child_id.to_string(),
                execution_id: String::new(),
                control_plane_config: Vec::new(),
            }],
            timeout_ms: 0,
        }))
        .await
        .expect("fork")
        .into_inner();

    let sandboxes = listed(&service).await;
    assert_eq!(sandboxes.len(), 1, "reported: {sandboxes:?}");
    assert_eq!(sandboxes[0].sandbox_id, source.id.to_string());

    assert!(Arc::clone(&orchestration)
        .get_sandbox(&child_id)
        .await
        .expect("read")
        .is_some());
}

#[tokio::test]
async fn the_unserved_calls_say_so_rather_than_answering() {
    let (_orchestration, service) = service().await;

    let err = service
        .create(Request::new(pb::SandboxCreateRequest {
            sandbox_id: SandboxId::new().to_string(),
            source: Some(pb::sandbox_create_request::Source::Image(pb::ImageSource {
                image_ref: "ubuntu:24.04".to_string(),
                ..Default::default()
            })),
            timeout_action: pb::TimeoutAction::Pause as i32,
            expiry: Some(pb::sandbox_create_request::Expiry::CallerKept(
                pb::CallerKeptExpiry {},
            )),
            ..Default::default()
        }))
        .await
        .expect_err("a cold create is not served");
    assert_eq!(err.code(), Code::Unimplemented);
}

#[tokio::test]
async fn update_network_reaches_the_sandbox_and_is_fenced() {
    let (orchestration, service) = service().await;
    let sandbox = start(&orchestration, Some(b"owned")).await;
    let policy = crate::orchestrator::SandboxMetadata::default().network_policy;

    service
        .update_network(Request::new(pb::SandboxNetworkRequest {
            sandbox_id: sandbox.id.to_string(),
            execution_id: sandbox.execution_id.to_string(),
            network_policy: Some(crate::proto::node::encode_value(&policy).expect("encode")),
        }))
        .await
        .expect("update_network");

    let err = service
        .update_network(Request::new(pb::SandboxNetworkRequest {
            sandbox_id: sandbox.id.to_string(),
            execution_id: ExecutionId::new().to_string(),
            network_policy: None,
        }))
        .await
        .expect_err("a policy change naming another run");
    assert_eq!(err.code(), Code::FailedPrecondition);
}

#[tokio::test]
async fn update_params_reaches_the_sandbox_and_is_fenced() {
    let behavior = Arc::new(MockBehavior::new());
    let (orchestration, service) =
        service_with(MockBackendFactory::with_behavior(Arc::clone(&behavior))).await;
    let sandbox = start(&orchestration, Some(b"owned")).await;
    assert_eq!(
        behavior.last_custom_extension_params(),
        None,
        "nothing has been applied to the runtime yet"
    );

    let mut first = serde_json::Map::new();
    first.insert("mode".to_string(), serde_json::json!("fast"));

    service
        .update_params(Request::new(pb::SandboxParamsRequest {
            sandbox_id: sandbox.id.to_string(),
            execution_id: sandbox.execution_id.to_string(),
            custom_extension_params: Some(
                crate::proto::node::encode_value(&first).expect("encode"),
            ),
        }))
        .await
        .expect("update_params");
    assert_eq!(
        behavior.last_custom_extension_params(),
        Some(Some(first.clone())),
        "the runtime must hold exactly the value that was sent"
    );
    let stored = orchestration
        .get_sandbox(&sandbox.id)
        .await
        .expect("read")
        .expect("sandbox metadata should exist")
        .custom_extension_params;
    assert_eq!(stored, Some(first.clone()));

    let mut wrong_run = serde_json::Map::new();
    wrong_run.insert("mode".to_string(), serde_json::json!("wrong-run"));
    let err = service
        .update_params(Request::new(pb::SandboxParamsRequest {
            sandbox_id: sandbox.id.to_string(),
            execution_id: ExecutionId::new().to_string(),
            custom_extension_params: Some(
                crate::proto::node::encode_value(&wrong_run).expect("encode"),
            ),
        }))
        .await
        .expect_err("a params change naming another run");
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert_eq!(
        behavior.last_custom_extension_params(),
        Some(Some(first.clone())),
        "a fenced-out call must not reach the runtime"
    );

    behavior.push_action(
        MockOperation::UpdateCustomExtensionParams,
        MockAction::Fail {
            message: "extension runtime unreachable".to_string(),
        },
    );
    let mut second = serde_json::Map::new();
    second.insert("mode".to_string(), serde_json::json!("slow"));
    let err = service
        .update_params(Request::new(pb::SandboxParamsRequest {
            sandbox_id: sandbox.id.to_string(),
            execution_id: sandbox.execution_id.to_string(),
            custom_extension_params: Some(
                crate::proto::node::encode_value(&second).expect("encode"),
            ),
        }))
        .await
        .expect_err("the injected backend failure must surface");
    assert_ne!(err.code(), Code::Unimplemented);
    assert_eq!(
        behavior.last_custom_extension_params(),
        Some(Some(first.clone())),
        "a failed assignment must not leave the runtime holding a value it never accepted"
    );
    let stored = orchestration
        .get_sandbox(&sandbox.id)
        .await
        .expect("read")
        .expect("sandbox metadata should exist")
        .custom_extension_params;
    assert_eq!(
        stored,
        Some(first.clone()),
        "the metadata store must not adopt a value the runtime never received"
    );
}

#[tokio::test]
async fn a_fork_with_no_children_is_refused() {
    let (orchestration, service) = service().await;
    let source = start(&orchestration, Some(b"the-source")).await;

    let err = service
        .fork(Request::new(pb::SandboxForkRequest {
            source_sandbox_id: source.id.to_string(),
            source_execution_id: source.execution_id.to_string(),
            children: Vec::new(),
            timeout_ms: 0,
        }))
        .await
        .expect_err("a fork with nothing to fork into");
    assert_eq!(err.code(), Code::InvalidArgument);
}

const ROOTFS_WHEN_THE_SOURCE_STARTED: u64 = 4 * 1024 * 1024;
const ROOTFS_BY_THE_TIME_IT_FORKED: u64 = 9 * 1024 * 1024;

#[tokio::test]
async fn a_fork_answers_each_child_with_the_facts_of_its_own_vm() {
    let behavior = Arc::new(MockBehavior::new());
    behavior.set_runtime_info(SandboxRuntimeInfo {
        rootfs_virtual_size: Some(ROOTFS_WHEN_THE_SOURCE_STARTED),
        runtime_artifacts: RuntimeArtifactSet::empty(),
        ..Default::default()
    });
    let (orchestration, service) =
        service_with(MockBackendFactory::with_behavior(Arc::clone(&behavior))).await;
    let source = start(&orchestration, Some(b"owned")).await;

    let created = listed(&service).await;
    assert_eq!(created.len(), 1);
    let source_address = created[0].host_interaction_ip.clone();
    assert!(
        !source_address.is_empty(),
        "a sandbox this node created was reported with no address, so this test \
         is not comparing against anything"
    );
    assert_eq!(
        created[0].rootfs_virtual_size,
        ROOTFS_WHEN_THE_SOURCE_STARTED
    );

    behavior.set_runtime_info(SandboxRuntimeInfo {
        rootfs_virtual_size: Some(ROOTFS_BY_THE_TIME_IT_FORKED),
        runtime_artifacts: RuntimeArtifactSet::empty(),
        ..Default::default()
    });

    let answered = service
        .fork(Request::new(pb::SandboxForkRequest {
            source_sandbox_id: source.id.to_string(),
            source_execution_id: source.execution_id.to_string(),
            children: (0..2)
                .map(|_| pb::ForkChildSpec {
                    sandbox_id: SandboxId::new().to_string(),
                    execution_id: ExecutionId::new().to_string(),
                    control_plane_config: b"child".to_vec(),
                })
                .collect(),
            timeout_ms: 0,
        }))
        .await
        .expect("the fork ran")
        .into_inner()
        .children;

    let started = answered
        .iter()
        .map(|child| match &child.outcome {
            Some(pb::fork_child_result::Outcome::Started(ack)) => ack,
            other => panic!("a child whose VM is running was answered with {other:?}"),
        })
        .collect::<Vec<_>>();
    assert_eq!(started.len(), 2);

    for ack in &started {
        assert!(
            !ack.host_interaction_ip.is_empty(),
            "a running fork child was answered with no address, which is what the \
             API half reads to publish its proxy route"
        );
        assert_eq!(
            ack.rootfs_virtual_size, ROOTFS_BY_THE_TIME_IT_FORKED,
            "a fork child's rootfs size was not read from its own handle"
        );
    }
    assert_ne!(
        started[0].host_interaction_ip, started[1].host_interaction_ip,
        "two fork children were answered with one address"
    );
    for ack in &started {
        assert_ne!(
            ack.host_interaction_ip, source_address,
            "a fork child was answered with the address of the sandbox it was forked from"
        );
    }
}

#[tokio::test]
async fn assigned_children_reach_the_orchestrator_unchanged() {
    let (orchestration, _service) = service().await;
    let source = start(&orchestration, Some(b"the-source")).await;

    let assigned = vec![ForkChildAssignment {
        sandbox_id: SandboxId::new(),
        execution_id: None,
        control_plane_config: ControlPlaneConfig::from_bytes(b"child".to_vec()),
    }];
    let outcomes = Arc::clone(&orchestration)
        .fork_sandbox(
            source.id,
            ForkChildren::Assigned(assigned.clone()),
            NewTimeout::UseExisting,
        )
        .await
        .expect("fork");

    let child = outcomes.into_iter().next().expect("one child").expect("ok");
    assert_eq!(child.id, assigned[0].sandbox_id);
    assert_eq!(
        child.control_plane_config.as_ref().map(|c| c.as_bytes()),
        Some(&b"child"[..])
    );
}

const ROOTFS_WHEN_IT_STARTED: u64 = 3 * 1024 * 1024;
const ROOTFS_BY_THE_TIME_IT_WAS_ASKED: u64 = 11 * 1024 * 1024;

async fn describe(
    service: &NodeSandboxService,
    sandbox_id: SandboxId,
) -> Result<pb::SandboxDescribeResponse, Status> {
    service
        .describe(Request::new(pb::SandboxDescribeRequest {
            sandbox_id: sandbox_id.to_string(),
        }))
        .await
        .map(|response| response.into_inner())
}

#[tokio::test]
async fn describe_answers_for_a_sandbox_the_listing_leaves_out() {
    let behavior = Arc::new(MockBehavior::new());
    behavior.set_runtime_info(SandboxRuntimeInfo {
        rootfs_virtual_size: Some(ROOTFS_WHEN_IT_STARTED),
        runtime_artifacts: RuntimeArtifactSet::empty(),
        ..Default::default()
    });
    let (orchestration, service) =
        service_with(MockBackendFactory::with_behavior(Arc::clone(&behavior))).await;

    let marked = start(&orchestration, Some(b"owned")).await;

    let unmarked = SandboxId::new();
    service
        .fork(Request::new(pb::SandboxForkRequest {
            source_sandbox_id: marked.id.to_string(),
            source_execution_id: marked.execution_id.to_string(),
            children: vec![pb::ForkChildSpec {
                sandbox_id: unmarked.to_string(),
                execution_id: ExecutionId::new().to_string(),
                control_plane_config: Vec::new(),
            }],
            timeout_ms: 0,
        }))
        .await
        .expect("the fork ran");

    let listing = listed(&service).await;
    assert_eq!(
        listing
            .iter()
            .map(|sandbox| sandbox.sandbox_id.as_str())
            .collect::<Vec<_>>(),
        vec![marked.id.to_string().as_str()],
        "the listing is not the surface this test thinks it is"
    );

    behavior.set_runtime_info(SandboxRuntimeInfo {
        rootfs_virtual_size: Some(ROOTFS_BY_THE_TIME_IT_WAS_ASKED),
        runtime_artifacts: RuntimeArtifactSet::empty(),
        ..Default::default()
    });

    let described_marked = describe(&service, marked.id)
        .await
        .expect("a node running this sandbox can say so");
    let described_unmarked = describe(&service, unmarked)
        .await
        .expect("🔴 a sandbox nobody claims is still a sandbox this node is running");

    for (what, described) in [
        ("the marked sandbox", &described_marked),
        ("the unmarked sandbox", &described_unmarked),
    ] {
        assert!(
            described.facts_from_handle,
            "{what} was described from a record rather than from its handle"
        );
        assert!(
            !described.host_interaction_ip.is_empty(),
            "{what} is running and was described with no address"
        );
        assert_eq!(
            described.rootfs_virtual_size, ROOTFS_BY_THE_TIME_IT_WAS_ASKED,
            "{what}'s rootfs size was not read from its handle at the time of asking"
        );
    }

    assert_ne!(
        described_marked.host_interaction_ip, described_unmarked.host_interaction_ip,
        "two sandboxes on one node were described with one address"
    );
    assert_eq!(
        described_marked.execution_id,
        marked.execution_id.to_string(),
        "the run reported for a sandbox is not the run it is running"
    );
}

#[tokio::test]
async fn a_record_kept_without_a_handle_is_described_and_refuses_a_create() {
    let manager = Arc::new(mock_snapshot_manager());
    let orchestrator = orchestrator_with(
        InMemoryMetadataStore::new(),
        MockBackendFactory::new(),
        &manager,
    )
    .await;
    let (orchestration, service) = serve_node(Arc::clone(&orchestrator), manager);
    let started = start(&orchestration, Some(b"owned")).await;
    assert!(
        orchestrator
            .forget_sandbox_handle_for_test(&started.id)
            .await,
        "this node held no handle to drop, so the two faces were never asked to disagree"
    );

    let described = describe(&service, started.id)
        .await
        .expect("a node whose record still names this sandbox holds it");
    assert_eq!(
        described.execution_id,
        started.execution_id.to_string(),
        "the run the node's record names is what a describe has to report"
    );
    assert!(
        !described.facts_from_handle,
        "there is no handle these facts could have come from"
    );

    let status = service
        .create(Request::new(pb::SandboxCreateRequest {
            sandbox_id: started.id.to_string(),
            source: Some(pb::sandbox_create_request::Source::Snapshot(
                resolved_snapshot_source(),
            )),
            expiry: Some(pb::sandbox_create_request::Expiry::NodeKeptTimeoutMs(
                60_000,
            )),
            timeout_action: pb::TimeoutAction::Pause as i32,
            control_plane_config: b"owned".to_vec(),
            ..Default::default()
        }))
        .await
        .expect_err("a node that describes a sandbox may not create it a second time");
    assert_eq!(status.code(), Code::AlreadyExists, "{status:?}");
}

#[tokio::test]
async fn a_create_for_a_sandbox_this_node_is_still_starting_is_already_exists() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    // start_nowait is the window with no handle and no record: what refuses a
    // second create there can only be the claim.
    let started = Arc::new(AtomicUsize::new(0));
    let behavior = Arc::new(MockBehavior::new());
    behavior.set_on_operation(MockOperation::StartNowait, {
        let started = Arc::clone(&started);
        Arc::new(move || {
            started.fetch_add(1, Ordering::SeqCst);
        })
    });
    behavior.push_action(
        MockOperation::StartNowait,
        MockAction::SucceedAfter(std::time::Duration::from_millis(400)),
    );
    let manager = Arc::new(mock_snapshot_manager());
    let orchestrator = orchestrator_with(
        InMemoryMetadataStore::new(),
        MockBackendFactory::with_behavior(Arc::clone(&behavior)),
        &manager,
    )
    .await;
    let (orchestration, service) = serve_node(Arc::clone(&orchestrator), manager);

    let sandbox_id = SandboxId::new();
    let starting = tokio::spawn({
        let orchestration = Arc::clone(&orchestration);
        async move {
            orchestration
                .restore_sandbox(sandbox_id, launch(Some(b"owned")))
                .await
        }
    });
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    while started.load(Ordering::SeqCst) == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for the first launch to reach its start"
        );
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }

    // The window this test is about: the launch holds the id and nothing else
    // on this node names it yet.
    assert!(
        orchestration
            .get_sandbox(&sandbox_id)
            .await
            .expect("read the node's records")
            .is_none(),
        "the launch already wrote its record, so the claim is not what the create meets"
    );

    let status = service
        .create(Request::new(pb::SandboxCreateRequest {
            sandbox_id: sandbox_id.to_string(),
            source: Some(pb::sandbox_create_request::Source::Snapshot(
                resolved_snapshot_source(),
            )),
            expiry: Some(pb::sandbox_create_request::Expiry::NodeKeptTimeoutMs(
                60_000,
            )),
            timeout_action: pb::TimeoutAction::Pause as i32,
            control_plane_config: b"owned".to_vec(),
            ..Default::default()
        }))
        .await
        .expect_err("a node already starting this sandbox may not start it a second time");
    assert_eq!(status.code(), Code::AlreadyExists, "{status:?}");
    assert!(
        status.message().contains("is already on this node"),
        "the caller has to read this the same way it reads a create that found the sandbox \
         already here: {status:?}"
    );
    assert_eq!(
        behavior.stop_calls(),
        0,
        "the refused create must not have torn the running launch down"
    );
    starting
        .await
        .expect("the first launch task")
        .expect("the launch the refused create was racing");
}

#[tokio::test]
async fn describe_says_not_found_for_a_sandbox_this_node_is_not_running() {
    let (orchestration, service) = service().await;
    let running = start(&orchestration, Some(b"owned")).await;

    describe(&service, running.id)
        .await
        .expect("the control face: a sandbox this node is running");

    let status = describe(&service, SandboxId::new())
        .await
        .expect_err("a node running nothing under that id has an answer, not facts");
    assert_eq!(status.code(), Code::NotFound, "{status:?}");
}

#[tokio::test]
async fn a_busy_handle_is_described_as_read_from_no_handle() {
    use crate::sandbox::mock::{MockAction, MockOperation};

    crate::logging::init_for_tests();
    let behavior = Arc::new(MockBehavior::new());
    let (orchestration, service) =
        service_with(MockBackendFactory::with_behavior(Arc::clone(&behavior))).await;

    let busy = start(&orchestration, Some(b"busy")).await;
    let idle = start(&orchestration, Some(b"idle")).await;

    behavior.push_action(
        MockOperation::Fork,
        MockAction::SucceedAfter(Duration::from_secs(3)),
    );
    let forking = tokio::spawn({
        let orchestration = Arc::clone(&orchestration);
        async move {
            orchestration
                .fork_sandbox(busy.id, ForkChildren::Fresh(1), NewTimeout::UseExisting)
                .await
        }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;

    let described_busy = describe(&service, busy.id)
        .await
        .expect("a sandbox mid-operation is still a sandbox this node is running");
    let described_idle = describe(&service, idle.id).await.expect("the control face");

    assert!(
        !described_busy.facts_from_handle,
        "a sandbox whose handle could not be read was described as if it had been"
    );
    assert!(
        described_busy.host_interaction_ip.is_empty(),
        "the node invented an address for a handle it could not read"
    );

    assert!(
        described_idle.facts_from_handle,
        "an idle sandbox's handle was not read, so this test compares nothing"
    );
    assert!(
        !described_idle.host_interaction_ip.is_empty(),
        "an idle sandbox was described with no address"
    );

    let _ = forking.await;
}

fn classification(status: &Status) -> Option<bool> {
    use prost::Message as _;

    if status.details().is_empty() {
        return None;
    }
    pb::SandboxCaptureFailure::decode(status.details())
        .ok()
        .map(|failure| failure.terminal)
}

struct StagingHarness {
    orchestration: Arc<dyn NodeOrchestration>,
    service: NodeSandboxService,
    repository: Arc<crate::snapshot::mock::RecordingSnapshotRepository>,
    behavior: Arc<MockBehavior>,
}

async fn staging_service() -> StagingHarness {
    let behavior = Arc::new(MockBehavior::new());
    behavior.make_captures_stageable();
    let (manager, repository) = recording_snapshot_manager();
    let manager = Arc::new(manager);
    let orchestrator = orchestrator_with(
        InMemoryMetadataStore::new(),
        MockBackendFactory::with_behavior(Arc::clone(&behavior)),
        &manager,
    )
    .await;
    let (orchestration, service) = serve_node(orchestrator, manager);
    StagingHarness {
        orchestration,
        service,
        repository,
        behavior,
    }
}

fn decode_staged(
    staged: Option<pb::StagedSnapshot>,
) -> crate::snapshot::repository::StagedSnapshot {
    let value = staged
        .expect("the reply carried no staged snapshot")
        .value
        .expect("the staged snapshot carried no value");
    assert_eq!(
        value.schema_version,
        pb::SERIALIZED_VALUE_VERSION,
        "a staged row went out stamped with a schema this build does not write"
    );
    serde_json::from_slice(&value.json).expect("the staged row should decode")
}

fn pause_request(sandbox: &SandboxMetadata) -> pb::SandboxPauseRequest {
    pb::SandboxPauseRequest {
        sandbox_id: sandbox.id.to_string(),
        execution_id: sandbox.execution_id.to_string(),
    }
}

#[tokio::test]
async fn a_pause_stages_the_bytes_here_and_leaves_the_row_to_the_caller() {
    let harness = staging_service().await;
    let published = start(&harness.orchestration, Some(b"owned")).await;

    let reply = harness
        .service
        .pause(Request::new(pause_request(&published)))
        .await
        .expect("a pause")
        .into_inner();

    let staged = decode_staged(reply.staged);
    assert_eq!(
        harness.repository.staged(),
        vec![staged.commit.id.clone()],
        "the bytes went somewhere other than the id the row names"
    );
    assert!(
        harness.repository.committed().is_empty(),
        "the node announced the row itself; it is the caller's to commit"
    );
    assert_eq!(
        staged.origin_node_id,
        crate::identity::local_node_id(),
        "the row must name the machine holding the bytes"
    );
    assert_ne!(
        staged.origin_node_id, "",
        "a row that names no machine cannot be resumed anywhere"
    );
    assert!(
        matches!(
            &staged.commit.source,
            crate::snapshot::SnapshotPublishSource::Sandbox { source_sandbox_id }
                if source_sandbox_id == &published.id.to_string()
        ),
        "the row must name the sandbox it came from, and names {:?}",
        staged.commit.source
    );
    assert!(
        staged.commit.alias.is_none(),
        "staging is never told a name; binding one is the committer's business"
    );
    assert!(
        staged.commit.committed.paused_sandbox.is_some(),
        "the row must carry what a resume needs to bring the sandbox back"
    );

    assert!(
        listed(&harness.service).await.is_empty(),
        "a sandbox that was paused is still running"
    );
    assert!(
        harness
            .orchestration
            .get_sandbox(&published.id)
            .await
            .expect("read")
            .is_none(),
        "a paused sandbox kept a record on the node"
    );
}

#[tokio::test]
async fn a_pause_of_a_sandbox_already_paused_finds_nothing_to_pause() {
    let harness = staging_service().await;
    let sandbox = start(&harness.orchestration, Some(b"owned")).await;

    let first = harness
        .service
        .pause(Request::new(pause_request(&sandbox)))
        .await
        .expect("the pause that does the work")
        .into_inner();
    assert!(
        first.staged.is_some(),
        "the pause that actually paused produced no row"
    );

    let err = harness
        .service
        .pause(Request::new(pause_request(&sandbox)))
        .await
        .expect_err("the sandbox is gone from this node");
    assert_eq!(err.code(), Code::NotFound, "{err}");
    assert_eq!(
        harness.repository.staged().len(),
        1,
        "one capture was staged twice"
    );
}

#[tokio::test]
async fn a_pause_that_joins_another_callers_pause_is_refused_the_staged_value() {
    let harness = staging_service().await;
    let sandbox = start(&harness.orchestration, Some(b"owned")).await;
    let service = Arc::new(harness.service);
    harness.behavior.push_action(
        MockOperation::Pause,
        MockAction::SucceedAfter(Duration::from_secs(1)),
    );

    let first = tokio::spawn({
        let service = Arc::clone(&service);
        let request = pause_request(&sandbox);
        async move { service.pause(Request::new(request)).await }
    });
    // The capture delay keeps the first pause in flight past this wait.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let joined = service
        .pause(Request::new(pause_request(&sandbox)))
        .await
        .expect_err("a pause somebody else is performing has no row for this caller");
    assert_eq!(joined.code(), Code::Internal, "{joined}");
    // What the caller decodes is the detail, so assert on the detail itself:
    // an unclassified failure is read as terminal on the other side.
    let classification = prost::Message::decode(joined.details())
        .map(|failure: pb::SandboxCaptureFailure| failure.terminal)
        .expect("the refusal carries a capture classification");
    assert!(
        !classification,
        "a joiner that got nothing did not touch the runtime"
    );

    let first = first
        .await
        .expect("the first pause finishes")
        .expect("the pause that did the work")
        .into_inner();
    assert!(
        first.staged.is_some(),
        "the caller that made the pause was not handed its row"
    );
    assert_eq!(
        harness.repository.staged().len(),
        1,
        "one capture was staged twice"
    );
    assert!(listed(&service).await.is_empty());
}

#[tokio::test]
async fn a_pause_whose_staging_failed_leaves_the_sandbox_running() {
    let harness = staging_service().await;
    let sandbox = start(&harness.orchestration, Some(b"owned")).await;

    harness.repository.fail_staging();
    let err = harness
        .service
        .pause(Request::new(pause_request(&sandbox)))
        .await
        .expect_err("a pause that could not be staged");
    assert!(
        err.message().contains("could not be published"),
        "the failure did not say what went wrong: {err}"
    );
    assert!(
        harness.repository.staged().is_empty(),
        "a staging that failed left a row behind"
    );

    let live = listed(&harness.service).await;
    assert_eq!(
        live.len(),
        1,
        "a pause whose staging failed stopped the sandbox: {live:?}"
    );
    assert_eq!(live[0].execution_id, sandbox.execution_id.to_string());
    let record = harness
        .orchestration
        .get_sandbox(&sandbox.id)
        .await
        .expect("read")
        .expect("a sandbox that was not paused keeps its record");
    assert_eq!(record.state, crate::orchestrator::SandboxState::Running);
}

#[tokio::test]
async fn a_checkpoint_answers_with_a_row_nobody_has_announced() {
    let harness = staging_service().await;
    let sandbox = start(&harness.orchestration, Some(b"owned")).await;

    let reply = harness
        .service
        .checkpoint(Request::new(pb::SandboxCheckpointRequest {
            sandbox_id: sandbox.id.to_string(),
            execution_id: sandbox.execution_id.to_string(),
        }))
        .await
        .expect("a checkpoint")
        .into_inner();

    let staged = decode_staged(reply.staged);
    assert_eq!(
        harness.repository.staged(),
        vec![staged.commit.id.clone()],
        "the bytes went somewhere other than the id the row names"
    );
    assert!(
        harness.repository.committed().is_empty(),
        "the node announced the row itself; it is the caller's to commit"
    );
    assert_eq!(staged.origin_node_id, crate::identity::local_node_id());
    assert_ne!(staged.origin_node_id, "");
    assert!(
        matches!(
            &staged.commit.source,
            crate::snapshot::SnapshotPublishSource::Sandbox { source_sandbox_id }
                if source_sandbox_id == &sandbox.id.to_string()
        ),
        "the row must name the sandbox it came from, and names {:?}",
        staged.commit.source
    );

    assert_eq!(
        listed(&harness.service).await.len(),
        1,
        "a checkpoint stopped the sandbox it was supposed to leave running"
    );

    let again = harness
        .service
        .checkpoint(Request::new(pb::SandboxCheckpointRequest {
            sandbox_id: sandbox.id.to_string(),
            execution_id: sandbox.execution_id.to_string(),
        }))
        .await
        .expect("a second checkpoint")
        .into_inner();
    let again = decode_staged(again.staged);
    assert_ne!(again.commit.id, staged.commit.id);
    assert_eq!(harness.repository.staged().len(), 2);
}

#[tokio::test]
async fn a_checkpoint_says_whether_the_sandbox_survived_its_failure() {
    let harness = staging_service().await;
    let sandbox = start(&harness.orchestration, Some(b"owned")).await;

    harness.repository.fail_staging();
    let err = harness
        .service
        .checkpoint(Request::new(pb::SandboxCheckpointRequest {
            sandbox_id: sandbox.id.to_string(),
            execution_id: sandbox.execution_id.to_string(),
        }))
        .await
        .expect_err("staging was made to fail");
    assert_eq!(
        classification(&err),
        Some(false),
        "a staging failure that never touched the runtime was called terminal: {err}"
    );
    assert_eq!(
        listed(&harness.service).await.len(),
        1,
        "the sandbox the failure did not touch is gone"
    );

    harness.behavior.push_action(
        crate::sandbox::mock::MockOperation::Snapshot,
        crate::sandbox::mock::MockAction::FailTerminal {
            message: "the VM was left paused".to_string(),
        },
    );
    let err = harness
        .service
        .checkpoint(Request::new(pb::SandboxCheckpointRequest {
            sandbox_id: sandbox.id.to_string(),
            execution_id: sandbox.execution_id.to_string(),
        }))
        .await
        .expect_err("the capture was made to fail terminally");
    assert_eq!(
        classification(&err),
        Some(true),
        "a capture that mutated the runtime was called recoverable: {err}"
    );
}

#[tokio::test]
async fn a_failed_pause_says_whether_the_sandbox_survived_it() {
    for (action, expected_terminal) in [
        (
            crate::sandbox::mock::MockAction::Fail {
                message: "the capture did not take".to_string(),
            },
            false,
        ),
        (
            crate::sandbox::mock::MockAction::FailTerminal {
                message: "the runtime was mutated".to_string(),
            },
            true,
        ),
    ] {
        let behavior = Arc::new(MockBehavior::new());
        let manager = Arc::new(mock_snapshot_manager());
        let orchestrator = orchestrator_with(
            InMemoryMetadataStore::new(),
            MockBackendFactory::with_behavior(Arc::clone(&behavior)),
            &manager,
        )
        .await;
        let (orchestration, service) = serve_node(orchestrator, manager);

        let sandbox = start(&orchestration, Some(b"owned")).await;
        behavior.push_action(MockOperation::Pause, action);

        let err = service
            .pause(Request::new(pause_request(&sandbox)))
            .await
            .expect_err("a pause the backend refused");
        assert_eq!(
            classification(&err),
            Some(expected_terminal),
            "a {} pause was reported as {:?}: {err}",
            if expected_terminal {
                "terminal"
            } else {
                "recoverable"
            },
            classification(&err)
        );
    }
}

#[tokio::test]
async fn a_pause_that_does_not_parse_is_refused_without_condemning_the_sandbox() {
    let harness = staging_service().await;
    let (orchestration, service) = (&harness.orchestration, &harness.service);
    let sandbox = start(orchestration, Some(b"owned")).await;

    let err = service
        .pause(Request::new(pb::SandboxPauseRequest {
            sandbox_id: sandbox.id.to_string(),
            execution_id: String::new(),
        }))
        .await
        .expect_err("a pause naming no run");
    assert_eq!(err.code(), Code::InvalidArgument, "{err}");
    assert_eq!(
        classification(&err),
        Some(false),
        "a request that never reached the runtime condemned the sandbox: {err}"
    );
    assert_eq!(
        listed(service).await.len(),
        1,
        "a refused pause stopped the sandbox anyway"
    );

    service
        .pause(Request::new(pause_request(&sandbox)))
        .await
        .expect("a pause that names the run it means");
    assert!(listed(service).await.is_empty());
}

#[tokio::test]
async fn a_pause_for_a_superseded_run_is_refused_without_condemning_the_sandbox() {
    let harness = staging_service().await;
    let (orchestration, service) = (&harness.orchestration, &harness.service);
    let sandbox = start(orchestration, Some(b"owned")).await;

    let err = service
        .pause(Request::new(pb::SandboxPauseRequest {
            sandbox_id: sandbox.id.to_string(),
            execution_id: ExecutionId::new().to_string(),
        }))
        .await
        .expect_err("a pause naming a run this node is not running");
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    assert_eq!(
        classification(&err),
        Some(false),
        "a fence check that never reached the runtime condemned the sandbox: {err}"
    );
    assert_eq!(
        listed(service).await.len(),
        1,
        "a refused pause stopped the sandbox anyway"
    );

    service
        .pause(Request::new(pause_request(&sandbox)))
        .await
        .expect("a pause naming the run this node is running");
    assert!(listed(service).await.is_empty());
}

#[tokio::test]
async fn a_sandbox_paused_through_the_rpc_is_created_again_under_its_own_id() {
    let harness = staging_service().await;
    let (orchestration, service) = (&harness.orchestration, &harness.service);
    let sandbox = start(orchestration, Some(b"owned")).await;

    service
        .pause(Request::new(pause_request(&sandbox)))
        .await
        .expect("pause through the RPC");
    assert!(
        listed(service).await.is_empty(),
        "the sandbox the RPC paused is still running"
    );

    let back = Arc::clone(orchestration)
        .restore_sandbox(sandbox.id, launch(Some(b"owned")))
        .await
        .expect("the id a pause freed can be created under again");
    assert_eq!(back.id, sandbox.id);
    assert_ne!(
        back.execution_id, sandbox.execution_id,
        "the sandbox came back as the run it was paused under"
    );

    let live = listed(service).await;
    assert_eq!(live.len(), 1, "the sandbox did not come back: {live:?}");
    assert_eq!(live[0].sandbox_id, sandbox.id.to_string());
    assert_eq!(live[0].execution_id, back.execution_id.to_string());
    assert_eq!(live[0].control_plane_config, b"owned");
}

#[tokio::test]
async fn a_built_templates_metadata_survives_stage_encode_decode_and_commit() {
    use crate::sandbox::FirecrackerSnapshotManifest;
    use crate::snapshot::mock::write_mock_built_artifacts;
    use crate::snapshot::repository::backends::storage::{PosixFsBackend, PosixFsBackendConfig};
    use crate::snapshot::repository::StagedSnapshot;
    use crate::snapshot::{CommandContext, SnapshotId, SnapshotRuntimeVersions};
    use crate::template::TemplateBuildExecution;
    use crate::types::{ImageConfigs, SandboxResources};
    use crate::virtualization::VirtualizationMode;

    let repo_root = tempfile::tempdir().expect("tempdir");
    let backend = PosixFsBackend::new(PosixFsBackendConfig {
        root: repo_root.path().join("repository"),
        cache_root: Some(repo_root.path().join("runtime-cache")),
        runtime_cache_root: Some(repo_root.path().join("runtime-cache").join("runtime")),
    })
    .expect("posix backend");
    let (repository, runtime_resolver) = backend.into_parts();
    let snapshot_manager = crate::snapshot::SnapshotManager::from_parts(
        crate::snapshot::mock::InMemorySnapshotCatalog::in_front_of(&repository),
        Some(runtime_resolver),
        None,
    );

    let artifacts_workspace = tempfile::tempdir().expect("tempdir");
    let (_, _, manifest): (_, _, FirecrackerSnapshotManifest) =
        write_mock_built_artifacts(artifacts_workspace.path()).expect("mock built artifacts");
    let mut manifest = manifest;
    manifest.rootfs.virtual_size = 5 * (1 << 20) + 1;

    let build_snapshot_id = SnapshotId::generate();
    let resources = SandboxResources {
        cpu_count: 2,
        memory_mib: 256,
        // Pre-build zero proves publish metadata derives the final disk size.
        disk_size_mib: 0,
    };
    let build_execution = TemplateBuildExecution {
        runtime_versions: SnapshotRuntimeVersions {
            kernel_version: "test-kernel".to_string(),
            firecracker_version: "test-firecracker".to_string(),
            envd_version: "test-envd".to_string(),
            tools_drive_version: "test-tools".to_string(),
        },
        manifest,
        build_context: CommandContext::default(),
        startup: None,
        image_configs: ImageConfigs::new(),
    };

    let (metadata, manifest) = super::service::template_build_publish_metadata(
        build_snapshot_id.clone(),
        resources,
        VirtualizationMode::Kvm,
        false,
        build_execution,
    );
    assert!(
        metadata.alias.is_none(),
        "a node must never apply an alias when staging a template build — that is the \
         committer's to do at `adopt_staged` time"
    );
    assert_eq!(
        metadata.resources.disk_size_mib, 6,
        "5 MiB + 1 byte must round up to 6 MiB, matching execute_and_publish's own div_ceil"
    );

    let staged = snapshot_manager
        .stage(metadata, manifest)
        .await
        .expect("a freshly built template's artifacts must stage")
        .into_staged();

    let encoded = crate::proto::node::encode_value(&staged).expect("a staged value must encode");
    let decoded: StagedSnapshot = super::convert::serialized(Some(&encoded), "test")
        .expect("decoding must not be refused")
        .expect("an encoded value must decode to something");

    let record = snapshot_manager
        .commit_staged(decoded)
        .await
        .expect("a node-staged template build must be acceptable to commit_staged");

    assert_eq!(record.id, build_snapshot_id);
    assert!(
        record.alias.is_none(),
        "nothing supplied an alias on this path, so the committed row must have none either"
    );
    assert_eq!(record.resources.disk_size_mib, 6);
    let committed = record
        .committed
        .expect("a committed build has committed state");
    assert_eq!(committed.virtualization_mode, VirtualizationMode::Kvm);
    assert_eq!(committed.runtime_versions.kernel_version, "test-kernel");
}
