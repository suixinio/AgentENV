//! Node service behavior driven through the generated server trait.

use std::sync::Arc;
use std::time::Duration;

use tonic::{Code, Request, Status};

use crate::orchestrator::{
    ControlPlaneConfig, CreateSandboxRequest, DisabledSandboxPersister, FileBackedSandboxPersister,
    ForkChildAssignment, ForkChildren, InMemoryMetadataStore, NewTimeout, Orchestrator,
    RecordingCall, RecordingPersister, SandboxExpiry, SandboxLaunchSource, SandboxMetadata,
    SandboxOrchestration, SandboxTimeoutAction,
};
use crate::proto::node as pb;
use crate::proto::node::node_sandbox_service_server::NodeSandboxService as _;
use crate::runtime_snapshot::RunnableSnapshot;
use crate::sandbox::mock::{MockAction, MockBackendFactory, MockBehavior, MockOperation};
use crate::sandbox::{
    PausedSandboxState, RuntimeArtifactSet, SandboxNetworkPolicy, SandboxRuntimeInfo,
};
use crate::snapshot::mock::mock_snapshot_manager;
use crate::types::{ExecutionId, SandboxId};

use super::service::NodeSandboxService;

const NODE: &str = "node-under-test";

async fn service() -> (Arc<dyn SandboxOrchestration>, NodeSandboxService) {
    service_with(MockBackendFactory::new()).await
}

async fn service_with(
    factory: MockBackendFactory,
) -> (Arc<dyn SandboxOrchestration>, NodeSandboxService) {
    crate::logging::init_for_tests();
    let orchestrator = Orchestrator::new(
        crate::sandbox::AccessTokenSeedPolicy::MayGenerate,
        InMemoryMetadataStore::new(),
        factory,
        DisabledSandboxPersister,
        crate::image::DisabledRuntimeImageRefs::shared(),
    )
    .await
    .expect("an in-memory orchestrator");
    let orchestration: Arc<dyn SandboxOrchestration> = orchestrator;
    let service = NodeSandboxService::new(
        Arc::clone(&orchestration),
        Arc::new(mock_snapshot_manager()),
        NODE.to_string(),
    );
    (orchestration, service)
}

async fn service_with_catalog() -> (
    Arc<dyn SandboxOrchestration>,
    NodeSandboxService,
    Arc<crate::snapshot::mock::MockSnapshotCatalog>,
) {
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
    let orchestration: Arc<dyn SandboxOrchestration> = orchestrator;
    let (manager, catalog) = crate::snapshot::mock::mock_snapshot_manager_with_catalog();
    let service = NodeSandboxService::new(
        Arc::clone(&orchestration),
        Arc::new(manager),
        NODE.to_string(),
    );
    (orchestration, service, catalog)
}

fn launch(marker: Option<&[u8]>) -> CreateSandboxRequest {
    CreateSandboxRequest {
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
    }
}

async fn start(
    orchestration: &Arc<dyn SandboxOrchestration>,
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

struct FlakyRecords {
    inner: InMemoryMetadataStore,
    reads_fail: Arc<std::sync::atomic::AtomicBool>,
}

impl FlakyRecords {
    fn new() -> Self {
        Self {
            inner: InMemoryMetadataStore::new(),
            reads_fail: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    fn unreachable<T>(&self) -> Result<T, crate::orchestrator::StoreError> {
        Err(crate::orchestrator::StoreError::Backend {
            source: anyhow::anyhow!("the records could not be reached"),
        })
    }
}

#[async_trait::async_trait]
impl crate::orchestrator::MetadataStore for FlakyRecords {
    async fn get(
        &self,
        sandbox_id: &SandboxId,
    ) -> Result<Option<SandboxMetadata>, crate::orchestrator::StoreError> {
        if self.reads_fail.load(std::sync::atomic::Ordering::SeqCst) {
            return self.unreachable();
        }
        self.inner.get(sandbox_id).await
    }

    async fn add(&self, metadata: SandboxMetadata) -> Result<(), crate::orchestrator::StoreError> {
        self.inner.add(metadata).await
    }
    async fn update(
        &self,
        metadata: SandboxMetadata,
    ) -> Result<(), crate::orchestrator::StoreError> {
        self.inner.update(metadata).await
    }
    async fn update_state_if_state(
        &self,
        sandbox_id: &SandboxId,
        new_state: crate::orchestrator::SandboxState,
        expected_states: &[crate::orchestrator::SandboxState],
    ) -> Result<crate::orchestrator::SandboxState, crate::orchestrator::StoreError> {
        self.inner
            .update_state_if_state(sandbox_id, new_state, expected_states)
            .await
    }
    async fn update_if_state<F>(
        &self,
        sandbox_id: &SandboxId,
        expected_states: &[crate::orchestrator::SandboxState],
        update: F,
    ) -> Result<crate::orchestrator::MetadataUpdateResult, crate::orchestrator::StoreError>
    where
        F: FnOnce(&mut SandboxMetadata) + Send,
    {
        self.inner
            .update_if_state(sandbox_id, expected_states, update)
            .await
    }
    async fn remove(
        &self,
        sandbox_id: &SandboxId,
    ) -> Result<Option<SandboxMetadata>, crate::orchestrator::StoreError> {
        self.inner.remove(sandbox_id).await
    }
    async fn remove_if_execution(
        &self,
        sandbox_id: &SandboxId,
        expected_execution_id: crate::types::ExecutionId,
        expected_states: &[crate::orchestrator::SandboxState],
    ) -> Result<crate::orchestrator::FencedRemoval, crate::orchestrator::StoreError> {
        self.inner
            .remove_if_execution(sandbox_id, expected_execution_id, expected_states)
            .await
    }
    async fn list(&self) -> Result<Vec<SandboxMetadata>, crate::orchestrator::StoreError> {
        self.inner.list().await
    }
    async fn list_with_callback<F>(
        &self,
        callback: F,
    ) -> Result<(), crate::orchestrator::StoreError>
    where
        F: FnMut(&SandboxMetadata) + Send,
    {
        self.inner.list_with_callback(callback).await
    }
    async fn list_filtered(
        &self,
        filter: crate::orchestrator::SandboxListFilter,
    ) -> Result<Vec<SandboxMetadata>, crate::orchestrator::StoreError> {
        self.inner.list_filtered(filter).await
    }
    async fn list_expired(
        &self,
        now: std::time::SystemTime,
    ) -> Result<Vec<SandboxMetadata>, crate::orchestrator::StoreError> {
        self.inner.list_expired(now).await
    }
    async fn list_ids(&self) -> Result<Vec<SandboxId>, crate::orchestrator::StoreError> {
        self.inner.list_ids().await
    }
    async fn get_many(
        &self,
        ids: &[SandboxId],
    ) -> Result<crate::orchestrator::MetadataRows, crate::orchestrator::StoreError> {
        self.inner.get_many(ids).await
    }
    async fn wait_while_in_states(
        &self,
        sandbox_id: &SandboxId,
        transitional_states: &[crate::orchestrator::SandboxState],
    ) -> Result<Option<SandboxMetadata>, crate::orchestrator::StoreError> {
        self.inner
            .wait_while_in_states(sandbox_id, transitional_states)
            .await
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

    crate::logging::init_for_tests();
    let orchestrator = Orchestrator::new(
        crate::sandbox::AccessTokenSeedPolicy::MayGenerate,
        HalfAnswering(InMemoryMetadataStore::new()),
        MockBackendFactory::new(),
        DisabledSandboxPersister,
        crate::image::DisabledRuntimeImageRefs::shared(),
    )
    .await
    .expect("an in-memory orchestrator");
    let orchestration: Arc<dyn SandboxOrchestration> = orchestrator;
    let service = NodeSandboxService::new(
        Arc::clone(&orchestration),
        Arc::new(mock_snapshot_manager()),
        NODE.to_string(),
    );

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
async fn a_paused_sandbox_keeps_its_record_and_leaves_the_listing() {
    let (orchestration, service) = service().await;
    let sandbox = start(&orchestration, Some(b"owned")).await;
    let staying = start(&orchestration, Some(b"also-owned")).await;
    assert_eq!(listed(&service).await.len(), 2);

    Arc::clone(&orchestration)
        .pause_sandbox(sandbox.id)
        .await
        .expect("the mock backend pauses");

    let record = Arc::clone(&orchestration)
        .get_sandbox(&sandbox.id)
        .await
        .expect("read")
        .expect("a paused sandbox keeps its record");
    assert!(
        record.control_plane_config.is_some(),
        "the marker did not survive the pause, so this test proves nothing"
    );

    let sandboxes = listed(&service).await;
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
    use crate::sandbox::{
        EnvdAccessToken, FreshSandboxBuildSpec, PausedSandboxState, SandboxBackend,
        SandboxBackendFactory, SandboxLaunchConfig,
    };

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
        fn decode_paused_state(
            &self,
            artifact_root: std::path::PathBuf,
            state: serde_json::Value,
        ) -> anyhow::Result<Arc<dyn PausedSandboxState>> {
            self.inner.decode_paused_state(artifact_root, state)
        }
        fn build_from_paused_state(
            &self,
            sandbox_id: SandboxId,
            execution_id: ExecutionId,
            state: &dyn PausedSandboxState,
            envd_access_token: Option<EnvdAccessToken>,
        ) -> anyhow::Result<Box<dyn SandboxBackend>> {
            self.inner
                .build_from_paused_state(sandbox_id, execution_id, state, envd_access_token)
        }
    }

    crate::logging::init_for_tests();
    let drifted = Arc::new(std::sync::Mutex::new(Vec::new()));
    let orchestrator = Orchestrator::new(
        crate::sandbox::AccessTokenSeedPolicy::MayGenerate,
        InMemoryMetadataStore::new(),
        Drifting {
            inner: MockBackendFactory::new(),
            drifted: Arc::clone(&drifted),
        },
        DisabledSandboxPersister,
        crate::image::DisabledRuntimeImageRefs::shared(),
    )
    .await
    .expect("an in-memory orchestrator");
    let orchestration: Arc<dyn SandboxOrchestration> = orchestrator;
    let service = NodeSandboxService::new(
        Arc::clone(&orchestration),
        Arc::new(mock_snapshot_manager()),
        NODE.to_string(),
    );

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

    crate::logging::init_for_tests();
    let behavior = Arc::new(MockBehavior::new());
    let orchestrator = Orchestrator::new(
        crate::sandbox::AccessTokenSeedPolicy::MayGenerate,
        InMemoryMetadataStore::new(),
        MockBackendFactory::with_behavior(Arc::clone(&behavior)),
        DisabledSandboxPersister,
        crate::image::DisabledRuntimeImageRefs::shared(),
    )
    .await
    .expect("an in-memory orchestrator");
    let orchestration: Arc<dyn SandboxOrchestration> = orchestrator;
    let service = NodeSandboxService::new(
        Arc::clone(&orchestration),
        Arc::new(mock_snapshot_manager()),
        NODE.to_string(),
    );

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

async fn pause(orchestration: &Arc<dyn SandboxOrchestration>, marker: &[u8]) -> SandboxMetadata {
    let sandbox = start(orchestration, Some(marker)).await;
    let paused = Arc::clone(orchestration)
        .pause_sandbox(sandbox.id)
        .await
        .expect("the mock backend pauses");
    assert_eq!(
        paused.execution_id, sandbox.execution_id,
        "a pause changed the run the sandbox had been running"
    );
    paused
}

fn resume_request(
    sandbox_id: SandboxId,
    execution_id: ExecutionId,
    resumed_execution_id: ExecutionId,
    timeout_ms: u64,
) -> pb::SandboxResumeRequest {
    pb::SandboxResumeRequest {
        sandbox_id: sandbox_id.to_string(),
        execution_id: execution_id.to_string(),
        resumed_execution_id: resumed_execution_id.to_string(),
        timeout_ms,
    }
}

#[tokio::test]
async fn a_resume_reopens_the_capture_under_the_run_the_caller_claimed() {
    let (orchestration, service) = service().await;
    let paused = pause(&orchestration, b"owned").await;
    let claimed = ExecutionId::new();
    assert_ne!(claimed, paused.execution_id);
    assert!(
        listed(&service).await.is_empty(),
        "a paused sandbox is live"
    );

    let err = service
        .resume(Request::new(resume_request(
            paused.id,
            claimed,
            paused.execution_id,
            0,
        )))
        .await
        .expect_err("a resume whose two incarnations were exchanged");
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    assert!(
        listed(&service).await.is_empty(),
        "a refused resume brought the sandbox back anyway"
    );

    let started = service
        .resume(Request::new(resume_request(
            paused.id,
            paused.execution_id,
            claimed,
            0,
        )))
        .await
        .expect("resume")
        .into_inner()
        .started
        .expect("a resume that succeeded says what it started");

    assert_eq!(started.sandbox_id, paused.id.to_string());
    assert_eq!(
        started.execution_id,
        claimed.to_string(),
        "the node brought the sandbox back under a run nobody claimed"
    );

    let sandboxes = listed(&service).await;
    assert_eq!(sandboxes.len(), 1, "reported: {sandboxes:?}");
    assert_eq!(sandboxes[0].sandbox_id, paused.id.to_string());
    assert_eq!(sandboxes[0].execution_id, claimed.to_string());
    assert_eq!(
        sandboxes[0].control_plane_config, b"owned",
        "the sandbox came back without the record that says whose it is"
    );
}

#[tokio::test]
async fn a_resume_whose_capture_record_is_gone_answers_not_found() {
    crate::logging::init_for_tests();
    let root = tempfile::tempdir().expect("a temp dir");
    let orchestrator = Orchestrator::new(
        crate::sandbox::AccessTokenSeedPolicy::MayGenerate,
        InMemoryMetadataStore::new(),
        MockBackendFactory::new(),
        FileBackedSandboxPersister::new_for_test(root.path().to_path_buf()),
        crate::image::DisabledRuntimeImageRefs::shared(),
    )
    .await
    .expect("an orchestrator that keeps records");
    let orchestration: Arc<dyn SandboxOrchestration> = orchestrator;
    let service = NodeSandboxService::new(
        Arc::clone(&orchestration),
        Arc::new(mock_snapshot_manager()),
        NODE.to_string(),
    );

    let paused = pause(&orchestration, b"owned").await;
    std::fs::remove_file(
        root.path()
            .join("records")
            .join(format!("{}.json", paused.id)),
    )
    .expect("pausing wrote the record this deletes");

    let err = service
        .resume(Request::new(resume_request(
            paused.id,
            paused.execution_id,
            ExecutionId::new(),
            0,
        )))
        .await
        .expect_err("the capture this resume needs is gone");

    assert_eq!(
        err.code(),
        Code::NotFound,
        "the api half rebuilds only an absence it can classify, and every other code reaches \
         the caller as a 500 instead: {err}"
    );
    assert!(
        matches!(
            crate::node_client::wire::RemoteResumeFailure::from_status(NODE, paused.id, err),
            crate::node_client::wire::RemoteResumeFailure::CaptureAbsent { .. }
        ),
        "the two halves have to agree: this status is what the api half classifies"
    );
}

#[tokio::test]
async fn a_resume_for_a_capture_this_node_does_not_hold_is_not_found() {
    let (orchestration, service) = service().await;
    let paused = pause(&orchestration, b"owned").await;

    let err = service
        .resume(Request::new(resume_request(
            SandboxId::new(),
            paused.execution_id,
            ExecutionId::new(),
            0,
        )))
        .await
        .expect_err("a resume for a sandbox that was never here");
    assert_eq!(err.code(), Code::NotFound, "{err}");

    service
        .resume(Request::new(resume_request(
            paused.id,
            paused.execution_id,
            ExecutionId::new(),
            0,
        )))
        .await
        .expect("a resume for the capture this node holds");
}

#[tokio::test]
async fn a_resume_whose_records_could_not_be_read_is_not_an_absence() {
    crate::logging::init_for_tests();
    let store = FlakyRecords::new();
    let breaker = store.reads_fail.clone();
    let orchestrator = Orchestrator::new(
        crate::sandbox::AccessTokenSeedPolicy::MayGenerate,
        store,
        MockBackendFactory::new(),
        DisabledSandboxPersister,
        crate::image::DisabledRuntimeImageRefs::shared(),
    )
    .await
    .expect("an in-memory orchestrator");
    let orchestration: Arc<dyn SandboxOrchestration> = orchestrator;
    let service = NodeSandboxService::new(
        Arc::clone(&orchestration),
        Arc::new(mock_snapshot_manager()),
        NODE.to_string(),
    );

    let request = || resume_request(SandboxId::new(), ExecutionId::new(), ExecutionId::new(), 0);

    let err = service
        .resume(Request::new(request()))
        .await
        .expect_err("a resume for a sandbox that was never here");
    assert_eq!(err.code(), Code::NotFound, "{err}");

    breaker.store(true, std::sync::atomic::Ordering::SeqCst);
    let err = service
        .resume(Request::new(request()))
        .await
        .expect_err("a resume this node could not answer");
    assert_ne!(
        err.code(),
        Code::NotFound,
        "a store this node could not read was reported as a sandbox that is not here: {err}"
    );
    assert_eq!(err.code(), Code::Internal, "{err}");
}

#[tokio::test]
async fn a_resume_into_the_run_it_is_replacing_is_refused() {
    let (orchestration, service) = service().await;
    let paused = pause(&orchestration, b"owned").await;

    let err = service
        .resume(Request::new(resume_request(
            paused.id,
            paused.execution_id,
            paused.execution_id,
            0,
        )))
        .await
        .expect_err("a resume into the run it is replacing");
    assert_eq!(err.code(), Code::InvalidArgument, "{err}");
    assert!(listed(&service).await.is_empty());

    let err = service
        .resume(Request::new(pb::SandboxResumeRequest {
            resumed_execution_id: String::new(),
            ..resume_request(paused.id, paused.execution_id, ExecutionId::new(), 0)
        }))
        .await
        .expect_err("a resume that named no run to start");
    assert_eq!(err.code(), Code::InvalidArgument, "{err}");
    assert!(listed(&service).await.is_empty());

    service
        .resume(Request::new(resume_request(
            paused.id,
            paused.execution_id,
            ExecutionId::new(),
            0,
        )))
        .await
        .expect("a resume under a run of its own");
    assert_eq!(listed(&service).await.len(), 1);
}

#[tokio::test]
async fn a_zero_timeout_keeps_the_deadline_the_sandbox_was_paused_with() {
    const AN_HOUR_MS: u64 = 60 * 60 * 1_000;
    let (orchestration, service) = service().await;

    let kept = pause(&orchestration, b"kept").await;
    let replaced = pause(&orchestration, b"replaced").await;

    let resume = |sandbox: &SandboxMetadata, timeout_ms| {
        let request = resume_request(
            sandbox.id,
            sandbox.execution_id,
            ExecutionId::new(),
            timeout_ms,
        );
        async { service.resume(Request::new(request)).await }
    };

    let kept = resume(&kept, 0)
        .await
        .expect("resume keeping the paused deadline")
        .into_inner()
        .started
        .expect("started");
    let replaced = resume(&replaced, AN_HOUR_MS)
        .await
        .expect("resume with a new deadline")
        .into_inner()
        .started
        .expect("started");

    assert!(kept.expires_at_ms > 0, "{kept:?}");
    assert!(replaced.expires_at_ms > 0, "{replaced:?}");
    assert!(
        replaced.expires_at_ms - kept.expires_at_ms > 3_000_000,
        "the timeout on the request did not reach the sandbox: kept {}, replaced {}",
        kept.expires_at_ms,
        replaced.expires_at_ms
    );
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

async fn service_with_persister() -> (
    Arc<dyn SandboxOrchestration>,
    NodeSandboxService,
    RecordingPersister,
) {
    crate::logging::init_for_tests();
    let persister = RecordingPersister::default();
    let orchestrator = Orchestrator::new(
        crate::sandbox::AccessTokenSeedPolicy::MayGenerate,
        InMemoryMetadataStore::new(),
        MockBackendFactory::new(),
        persister.clone(),
        crate::image::DisabledRuntimeImageRefs::shared(),
    )
    .await
    .expect("an in-memory orchestrator");
    let orchestration: Arc<dyn SandboxOrchestration> = orchestrator;
    let service = NodeSandboxService::new(
        Arc::clone(&orchestration),
        Arc::new(mock_snapshot_manager()),
        NODE.to_string(),
    );
    (orchestration, service, persister)
}

struct StagingHarness {
    orchestration: Arc<dyn SandboxOrchestration>,
    service: NodeSandboxService,
    repository: Arc<crate::snapshot::mock::RecordingSnapshotRepository>,
    behavior: Arc<MockBehavior>,
    _artifacts: tempfile::TempDir,
}

async fn staging_service() -> StagingHarness {
    crate::logging::init_for_tests();
    let artifacts = tempfile::tempdir().expect("tempdir");
    let persister = RecordingPersister::default();
    persister.allocates_artifact_root_at(artifacts.path().join("capture"));
    let behavior = Arc::new(MockBehavior::new());
    behavior.make_captures_stageable();
    let orchestrator = Orchestrator::new(
        crate::sandbox::AccessTokenSeedPolicy::MayGenerate,
        InMemoryMetadataStore::new(),
        MockBackendFactory::with_behavior(Arc::clone(&behavior)),
        persister,
        crate::image::DisabledRuntimeImageRefs::shared(),
    )
    .await
    .expect("an in-memory orchestrator");
    let orchestration: Arc<dyn SandboxOrchestration> = orchestrator;
    let (manager, repository) = crate::snapshot::mock::recording_snapshot_manager();
    let service = NodeSandboxService::new(
        Arc::clone(&orchestration),
        Arc::new(manager),
        NODE.to_string(),
    );
    StagingHarness {
        orchestration,
        service,
        repository,
        behavior,
        _artifacts: artifacts,
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
        value.schema_version, 1,
        "a staged row went out stamped with a schema this build does not write"
    );
    serde_json::from_slice(&value.json).expect("the staged row should decode")
}

fn pause_request(sandbox: &SandboxMetadata, publish: bool) -> pb::SandboxPauseRequest {
    pb::SandboxPauseRequest {
        sandbox_id: sandbox.id.to_string(),
        execution_id: sandbox.execution_id.to_string(),
        publish,
    }
}

#[tokio::test]
async fn a_pause_says_where_the_capture_went() {
    let (orchestration, service, persister) = service_with_persister().await;

    let nowhere = start(&orchestration, Some(b"owned")).await;
    let reply = service
        .pause(Request::new(pause_request(&nowhere, false)))
        .await
        .expect("pause")
        .into_inner()
        .paused_state
        .expect("a pause that succeeded says how to reopen the sandbox");
    assert_eq!(
        reply.artifact_root, "",
        "a node that allocated no directory named one anyway"
    );

    persister.holds_capture_at("/var/lib/agentenv/paused/abc/7");
    let somewhere = start(&orchestration, Some(b"owned")).await;
    let reply = service
        .pause(Request::new(pause_request(&somewhere, false)))
        .await
        .expect("pause")
        .into_inner()
        .paused_state
        .expect("a pause that succeeded says how to reopen the sandbox");
    assert_eq!(reply.artifact_root, "/var/lib/agentenv/paused/abc/7");

    let state = reply.state.expect("the backend's encoding of its capture");
    assert_eq!(
        state.schema_version,
        crate::proto::node::SERIALIZED_VALUE_VERSION
    );
    let decoded: serde_json::Value =
        serde_json::from_slice(&state.json).expect("the encoding is the value the backend wrote");
    assert_eq!(
        decoded,
        crate::sandbox::mock::MockSnapshot
            .encode()
            .expect("the mock backend encodes its capture"),
        "the reply carried something other than the capture the backend made"
    );

    for sandbox in [&nowhere, &somewhere] {
        let record = orchestration
            .get_sandbox(&sandbox.id)
            .await
            .expect("read")
            .expect("the sandbox still has a record");
        assert_eq!(record.state, crate::orchestrator::SandboxState::Paused);
    }
    assert!(
        listed(&service).await.is_empty(),
        "a paused sandbox is still running"
    );
}

#[tokio::test]
async fn a_pause_whose_record_could_not_be_read_is_not_a_pause_without_a_directory() {
    let (orchestration, service, persister) = service_with_persister().await;
    persister.holds_capture_at("/var/lib/agentenv/paused/abc/7");

    let unreadable = start(&orchestration, Some(b"owned")).await;
    persister.fail_next(RecordingCall::PausedArtifactRoot);
    let err = service
        .pause(Request::new(pause_request(&unreadable, false)))
        .await
        .expect_err("a pause whose record could not be read");
    assert_ne!(
        err.code(),
        Code::NotFound,
        "a disk that could not be read was reported as an absence: {err}"
    );

    let readable = start(&orchestration, Some(b"owned")).await;
    let reply = service
        .pause(Request::new(pause_request(&readable, false)))
        .await
        .expect("pause")
        .into_inner()
        .paused_state
        .expect("paused state");
    assert_eq!(reply.artifact_root, "/var/lib/agentenv/paused/abc/7");
}

#[tokio::test]
async fn a_published_pause_stages_the_bytes_here_and_leaves_the_row_to_the_caller() {
    let harness = staging_service().await;
    let published = start(&harness.orchestration, Some(b"owned")).await;
    let unpublished = start(&harness.orchestration, Some(b"owned")).await;

    let reply = harness
        .service
        .pause(Request::new(pause_request(&published, true)))
        .await
        .expect("a published pause")
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
    assert!(reply.paused_state.is_some());

    let reply = harness
        .service
        .pause(Request::new(pause_request(&unpublished, false)))
        .await
        .expect("an unpublished pause")
        .into_inner();
    assert!(
        reply.staged.is_none(),
        "a pause that did not ask to publish came back with a row"
    );
    assert!(reply.paused_state.is_some());
    assert_eq!(
        harness.repository.staged().len(),
        1,
        "a pause that did not ask to publish staged bytes anyway"
    );

    assert!(
        listed(&harness.service).await.is_empty(),
        "a sandbox that was paused is still running"
    );
}

#[tokio::test]
async fn a_pause_that_had_already_happened_answers_with_no_row() {
    let harness = staging_service().await;
    let sandbox = start(&harness.orchestration, Some(b"owned")).await;

    let first = harness
        .service
        .pause(Request::new(pause_request(&sandbox, true)))
        .await
        .expect("the pause that does the work")
        .into_inner();
    assert!(
        first.staged.is_some(),
        "the pause that actually paused produced no row"
    );

    let again = harness
        .service
        .pause(Request::new(pause_request(&sandbox, true)))
        .await
        .expect("a repeated pause is idempotent, not an error")
        .into_inner();
    assert!(
        again.staged.is_none(),
        "a pause that did nothing produced a second row for one capture"
    );
    assert_eq!(
        harness.repository.staged().len(),
        1,
        "one capture was staged twice"
    );
    assert!(again.paused_state.is_some());
}

#[tokio::test]
async fn a_pause_whose_staging_failed_still_pauses_and_says_what_was_lost() {
    let harness = staging_service().await;
    let works = start(&harness.orchestration, Some(b"owned")).await;
    let breaks = start(&harness.orchestration, Some(b"owned")).await;

    let ok = harness
        .service
        .pause(Request::new(pause_request(&works, true)))
        .await
        .expect("a published pause")
        .into_inner();
    assert!(ok.staged.is_some());
    assert_eq!(
        ok.staging_error, "",
        "a staging that worked reported an error"
    );

    harness.repository.fail_staging();
    let broken = harness
        .service
        .pause(Request::new(pause_request(&breaks, true)))
        .await
        .expect("a staging failure must not fail the pause")
        .into_inner();
    assert!(
        broken.staged.is_none(),
        "a staging that failed produced a row anyway"
    );
    assert!(
        broken.staging_error.contains("no room on the device"),
        "the reply did not say what failed: {:?}",
        broken.staging_error
    );
    assert!(
        broken.paused_state.is_some(),
        "a pause that could not be staged came back with no way to reopen it"
    );
    assert!(
        listed(&harness.service).await.is_empty(),
        "a pause whose staging failed left the sandbox running"
    );

    let again = harness
        .service
        .pause(Request::new(pause_request(&breaks, true)))
        .await
        .expect("a repeated pause")
        .into_inner();
    assert!(again.staged.is_none());
    assert_eq!(
        again.staging_error, "",
        "a pause that had nothing to stage reported a staging failure"
    );
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
        crate::logging::init_for_tests();
        let behavior = Arc::new(crate::sandbox::mock::MockBehavior::new());
        let orchestrator = Orchestrator::new(
            crate::sandbox::AccessTokenSeedPolicy::MayGenerate,
            InMemoryMetadataStore::new(),
            MockBackendFactory::with_behavior(Arc::clone(&behavior)),
            DisabledSandboxPersister,
            crate::image::DisabledRuntimeImageRefs::shared(),
        )
        .await
        .expect("an in-memory orchestrator");
        let orchestration: Arc<dyn SandboxOrchestration> = orchestrator;
        let service = NodeSandboxService::new(
            Arc::clone(&orchestration),
            Arc::new(mock_snapshot_manager()),
            NODE.to_string(),
        );

        let sandbox = start(&orchestration, Some(b"owned")).await;
        behavior.push_action(crate::sandbox::mock::MockOperation::Pause, action);

        let err = service
            .pause(Request::new(pause_request(&sandbox, false)))
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
    let (orchestration, service) = service().await;
    let sandbox = start(&orchestration, Some(b"owned")).await;

    let err = service
        .pause(Request::new(pb::SandboxPauseRequest {
            sandbox_id: sandbox.id.to_string(),
            execution_id: String::new(),
            publish: false,
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
        listed(&service).await.len(),
        1,
        "a refused pause stopped the sandbox anyway"
    );

    service
        .pause(Request::new(pause_request(&sandbox, false)))
        .await
        .expect("a pause that names the run it means");
    assert!(listed(&service).await.is_empty());
}

#[tokio::test]
async fn a_pause_for_a_superseded_run_is_refused_without_condemning_the_sandbox() {
    let (orchestration, service) = service().await;
    let sandbox = start(&orchestration, Some(b"owned")).await;

    let err = service
        .pause(Request::new(pb::SandboxPauseRequest {
            sandbox_id: sandbox.id.to_string(),
            execution_id: ExecutionId::new().to_string(),
            publish: false,
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
        listed(&service).await.len(),
        1,
        "a refused pause stopped the sandbox anyway"
    );

    service
        .pause(Request::new(pause_request(&sandbox, false)))
        .await
        .expect("a pause naming the run this node is running");
    assert!(listed(&service).await.is_empty());
}

#[tokio::test]
async fn a_sandbox_paused_through_the_rpc_is_reopened_through_the_rpc() {
    let (orchestration, service) = service().await;
    let sandbox = start(&orchestration, Some(b"owned")).await;
    let claimed = ExecutionId::new();
    assert_ne!(claimed, sandbox.execution_id);

    service
        .pause(Request::new(pause_request(&sandbox, false)))
        .await
        .expect("pause through the RPC");
    assert!(
        listed(&service).await.is_empty(),
        "the sandbox the RPC paused is still running"
    );

    let stale = service
        .resume(Request::new(resume_request(
            sandbox.id,
            ExecutionId::new(),
            claimed,
            0,
        )))
        .await
        .expect_err("a resume fenced on a run this node never paused");
    assert_eq!(stale.code(), Code::FailedPrecondition, "{stale}");

    let started = service
        .resume(Request::new(resume_request(
            sandbox.id,
            sandbox.execution_id,
            claimed,
            0,
        )))
        .await
        .expect("resume through the RPC")
        .into_inner()
        .started
        .expect("a resume that succeeded says what it started");
    assert_eq!(started.execution_id, claimed.to_string());

    let live = listed(&service).await;
    assert_eq!(live.len(), 1, "the sandbox did not come back: {live:?}");
    assert_eq!(live[0].execution_id, claimed.to_string());
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
