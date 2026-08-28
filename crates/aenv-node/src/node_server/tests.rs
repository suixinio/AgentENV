//! What the node service answers, driven through the generated server trait.
//!
//! 🔴 These go through `pb::node_sandbox_service_server::NodeSandboxService`
//! rather than through the inherent methods, because the trait is what a
//! request actually arrives on and it is the layer where a method could be
//! wired to the wrong RPC without anything looking wrong.

use std::sync::Arc;
use std::time::Duration;

use tonic::{Code, Request, Status};

use crate::orchestrator::{
    ControlPlaneConfig, CreateSandboxRequest, DisabledSandboxPersister, ForkChildAssignment,
    ForkChildren, InMemoryMetadataStore, NewTimeout, Orchestrator, RecordingCall,
    RecordingPersister, SandboxExpiry, SandboxLaunchSource, SandboxMetadata, SandboxOrchestration,
    SandboxTimeoutAction,
};
use crate::proto::node as pb;
use crate::proto::node::node_sandbox_service_server::NodeSandboxService as _;
use crate::sandbox::mock::{MockAction, MockBackendFactory, MockBehavior, MockOperation};
use crate::sandbox::{
    PausedSandboxState, RuntimeArtifactSet, SandboxNetworkPolicy, SandboxRuntimeInfo,
};
use crate::snapshot::{mock::mock_snapshot_manager, RunnableSnapshot};
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

/// [`service`], but also hands back the concrete [`MockSnapshotCatalog`]
/// instance wired into the service's snapshot manager — for the
/// fallback/skip pairs below, which assert on
/// [`crate::snapshot::mock::MockSnapshotCatalog::get_calls`] rather than on
/// the wording of whatever the mock catalog and mock runtime resolver
/// refused with. See those tests' own doc comments for why.
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

/// The in-memory store, with a switch that makes single-record reads fail.
///
/// 🔴 It fails rather than answers empty, which is the whole point: `get`
/// returning `Ok(None)` and `get` returning an error are the two things this
/// service must never flatten, and nothing else in this file can produce the
/// second.
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
    /// The one override.
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

/// 🔴 The load-bearing one. Only sandboxes carrying an ownership marker are
/// reported, and the marker comes back byte for byte.
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

/// 🔴 The control probe for the test above.
///
/// A node running only unmarked sandboxes reports none. Without this, a filter
/// that had been inverted, dropped, or replaced by "everything with a record"
/// would still satisfy the first test's assertion that the owned sandbox is in
/// the list — because it would be.
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

    // 🔴 And the resolution, in this run rather than in the test above it.
    // An emptiness that is only ever compared against another emptiness is
    // satisfied by a listing that has stopped working altogether — which is a
    // node reporting a clean "I hold nothing" while it holds three sandboxes,
    // and is the reading that makes a control plane retire live bindings.
    let owned = start(&orchestration, Some(b"the-fourth-one")).await;
    let sandboxes = listed(&service).await;
    assert_eq!(sandboxes.len(), 1, "reported: {sandboxes:?}");
    assert_eq!(sandboxes[0].sandbox_id, owned.id.to_string());
}

/// The empty answer a node with nothing running gives is an answer, not a
/// failure, and it is the one a caller may act on.
#[tokio::test]
async fn a_node_running_nothing_at_all_answers_with_an_empty_list() {
    let (orchestration, service) = service().await;
    assert!(listed(&service).await.is_empty());

    // 🔴 The same probe, on the same service, with something to find. Without
    // it "empty" here is indistinguishable from a listing that could not answer
    // at all — and those two must never read the same, because one of them says
    // the fleet holds nothing.
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

/// 🔴 Membership comes from the live handles, not from the record store.
///
/// A sandbox that has been deleted is gone from both, so the way to tell the
/// two sources apart is to delete one and check that the listing loses it —
/// while another, still running, stays. A listing built from a stale read of
/// either source would fail one half of this.
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

/// 🔴 A batch read that only partly worked fails the listing rather than
/// shortening it.
///
/// A store that answers for some of the ids it was given and quietly skips the
/// rest produces exactly the same shape as a store that answered fully about a
/// node running fewer sandboxes — and every skipped id then looks like a
/// sandbox with no record, which means no ownership marker, which means it
/// drops out of the answer the control plane reconciles against. The cluster
/// would conclude those sandboxes are gone while their VMs are up.
#[tokio::test]
async fn a_partial_record_read_fails_the_listing_instead_of_shortening_it() {
    use crate::orchestrator::{MetadataRows, MetadataStore, MetadataUpdateResult, StoreError};

    /// Wraps the in-memory store and answers batch reads about all but one id.
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

        /// The one override: answer about everything except the first id.
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

/// 🔴 The discriminator between "what this node holds" and "what it claims to
/// hold": a paused sandbox keeps its record — marker and all — and loses its
/// handle. It must not be listed.
///
/// Nothing else in this file can tell the two sources apart, because in every
/// other state they agree. An implementation that answered from the record
/// store would pass every other test here and would report a sandbox with no
/// VM behind it as running, which is the exact shape of answer a reconcile
/// turns into a decision.
#[tokio::test]
async fn a_paused_sandbox_keeps_its_record_and_leaves_the_listing() {
    let (orchestration, service) = service().await;
    let sandbox = start(&orchestration, Some(b"owned")).await;
    // 🔴 A second sandbox that stays up, and it is load-bearing. With only one
    // sandbox, pausing it empties the handle table *and* the running set at
    // once, so a listing built from the records would come back empty for the
    // wrong reason and look correct. Mutation testing found exactly that: an
    // implementation that answered from `list_ids()` passed this test until
    // there was something left running for it to get wrong.
    let staying = start(&orchestration, Some(b"also-owned")).await;
    assert_eq!(listed(&service).await.len(), 2);

    Arc::clone(&orchestration)
        .pause_sandbox(sandbox.id)
        .await
        .expect("the mock backend pauses");

    // The record is still there, and it still carries the marker...
    let record = Arc::clone(&orchestration)
        .get_sandbox(&sandbox.id)
        .await
        .expect("read")
        .expect("a paused sandbox keeps its record");
    assert!(
        record.control_plane_config.is_some(),
        "the marker did not survive the pause, so this test proves nothing"
    );

    // ...and the listing reports only the sandbox that is still running.
    let sandboxes = listed(&service).await;
    assert_eq!(
        sandboxes.len(),
        1,
        "a paused sandbox was reported as running: {sandboxes:?}"
    );
    assert_eq!(sandboxes[0].sandbox_id, staying.id.to_string());
}

/// The incarnation in the listing is the one that is running.
///
/// 🔴 This is what an orphan check compares, so an entry that carried the id of
/// some other run would have the control plane tear down a live sandbox on the
/// grounds that the one it knew about is gone.
#[tokio::test]
async fn the_listing_names_the_run_that_is_live() {
    let (orchestration, service) = service().await;
    let sandbox = start(&orchestration, Some(b"owned")).await;

    let sandboxes = listed(&service).await;
    assert_eq!(sandboxes.len(), 1);
    assert_eq!(sandboxes[0].execution_id, sandbox.execution_id.to_string());
    assert_ne!(sandboxes[0].execution_id, "");
}

/// 🔴 The listing reports the run that is *up*, not the run that is *filed*.
///
/// Everywhere else in this file the two agree, so nothing here can tell which
/// one an entry came from — mutation testing put an implementation that
/// preferred the record into the tree and every test still passed. This one
/// makes them disagree on purpose, with a factory that builds a backend under
/// an incarnation other than the one it was asked for.
///
/// The property matters because an orphan check compares incarnations. Told
/// the filed one, a control plane comparing against what it believes would
/// find agreement and conclude everything is fine — about a machine running a
/// different run than the one it thinks it is.
#[tokio::test]
async fn the_listing_reports_the_incarnation_the_handle_is_running() {
    use crate::sandbox::{
        EnvdAccessToken, FreshSandboxBuildSpec, PausedSandboxState, SandboxBackend,
        SandboxBackendFactory, SandboxLaunchConfig,
    };

    /// Builds every backend under a fresh incarnation, ignoring the one it was
    /// given. Stands in for a node whose handle and record have come apart.
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

/// 🔴 A sandbox whose handle is busy is still reported.
///
/// Reading the live facts needs the handle's lock, and a long operation holds
/// it. Waiting would make the listing as slow as the slowest thing on the node;
/// *skipping* would tell the control plane a running sandbox is not there, and
/// "not there" is the answer that gets a live VM torn down. So it is reported
/// from its record, and only the facts only the handle knows go missing.
///
/// Mutation testing is why this exists: an implementation that dropped busy
/// handles passed every other test here, because nothing else in this file ever
/// makes one busy.
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

    // A fork holds the source's handle for the whole operation and leaves it in
    // the table while it runs, which is exactly the shape being tested: the
    // sandbox is there and cannot be read.
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
    // Long enough for the fork to have taken the lock, short enough that it is
    // still holding it. The fork's own delay is what makes this reliable rather
    // than the sleep length.
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

    // And the record still supplied what it could: the incarnation is there
    // even though the handle could not be read.
    let busy_entry = sandboxes
        .iter()
        .find(|sandbox| sandbox.sandbox_id == busy.id.to_string())
        .expect("the busy sandbox");
    assert_eq!(busy_entry.execution_id, busy.execution_id.to_string());

    let _ = forking.await;
}

/// A marker sent as zero bytes is no marker, and the sandbox is left out of the
/// listing rather than killed.
///
/// 🔴 This is the "blank a node's marker" probe: the failure it rules out is a
/// node treating an empty blob as an owned sandbox with an empty record, which
/// would put a sandbox into the reconciliation with nothing to reconcile it
/// against.
#[tokio::test]
async fn a_create_carrying_an_empty_marker_produces_an_unowned_sandbox() {
    let (orchestration, service) = service().await;

    let response = service
        .create(Request::new(pb::SandboxCreateRequest {
            sandbox_id: SandboxId::new().to_string(),
            source: Some(pb::sandbox_create_request::Source::Snapshot(
                pb::SnapshotSource {
                    snapshot_id: "mock".to_string(),
                    resolved_record: None,
                },
            )),
            expiry: Some(pb::sandbox_create_request::Expiry::NodeKeptTimeoutMs(
                60_000,
            )),
            timeout_action: pb::TimeoutAction::Pause as i32,
            control_plane_config: Vec::new(),
            ..Default::default()
        }))
        .await;

    // The mock snapshot manager has no snapshot to resolve, so the create is
    // refused before it starts anything — which is fine for what this asserts:
    // whatever happens, an empty marker never produces a listed sandbox.
    if let Ok(response) = response {
        let created = response.into_inner();
        assert!(!created.sandbox_id.is_empty());
    }

    // And directly: a sandbox created through the orchestrator with an empty
    // marker is not the control plane's.
    let sandbox = start(&orchestration, Some(b"")).await;
    assert!(
        sandbox.control_plane_config.is_none(),
        "an empty blob became a marker"
    );
    assert!(listed(&service).await.is_empty());

    // 🔴 Resolution: one byte is a marker, and the same listing finds it. The
    // assertion above is about the *empty* blob, and on its own it would pass
    // against a node whose listing admitted nothing at all.
    let owned = start(&orchestration, Some(b"\0")).await;
    let sandboxes = listed(&service).await;
    assert_eq!(sandboxes.len(), 1, "reported: {sandboxes:?}");
    assert_eq!(sandboxes[0].sandbox_id, owned.id.to_string());
    assert_eq!(sandboxes[0].control_plane_config, b"\0");
}

/// 🔴 A create that does not say who keeps the sandbox's deadline is refused,
/// and refused before the node goes looking for a snapshot.
///
/// One request shape, one field different, opposite answers — and both halves
/// are non-empty, which is what makes the refusal mean anything. Without the
/// control face below, "the create was refused" would be satisfied by a node
/// that refuses every create, including for the reason the snapshot does not
/// exist.
///
/// The *codes* are what the two halves are told apart by, and they say where
/// the request got to: `InvalidArgument` is this node reading the message,
/// `NotFound` is this node having gone to its snapshot resolver and come back.
/// The silent create the split shipped went all the way to a running VM.
#[tokio::test]
async fn a_create_that_does_not_say_who_keeps_the_deadline_is_refused_before_the_resolver() {
    let (orchestration, service) = service().await;
    let request = |expiry| pb::SandboxCreateRequest {
        sandbox_id: SandboxId::new().to_string(),
        source: Some(pb::sandbox_create_request::Source::Snapshot(
            pb::SnapshotSource {
                snapshot_id: "no-such-snapshot".to_string(),
                resolved_record: None,
            },
        )),
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

    // 🔴 The control face. Same request, expiry named, and now the refusal
    // comes from the snapshot resolver — which is to say the message was
    // understood and the call got further than the one above.
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

    // Neither left anything behind on the node.
    assert!(listed(&service).await.is_empty());
    assert!(Arc::clone(&orchestration)
        .list_sandboxes()
        .await
        .expect("list")
        .is_empty());
}

/// A create the caller identified is created under that id.
#[tokio::test]
async fn a_create_uses_the_id_the_caller_chose() {
    let (orchestration, service) = service().await;
    let sandbox_id = SandboxId::new();

    // The mock snapshot manager cannot resolve a snapshot, so this exercises
    // the refusal rather than the happy path — but it must refuse for the right
    // reason, and never by inventing an id.
    let err = service
        .create(Request::new(pb::SandboxCreateRequest {
            sandbox_id: sandbox_id.to_string(),
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
        .expect_err("the mock catalog has no snapshots");
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

/// Q3's whole point, proven rather than asserted in prose: a `SnapshotSource`
/// carrying `resolved_record` never reaches this node's own catalog.
///
/// The service's mock catalog (`MockSnapshotCatalog`) and runtime resolver
/// (`MockSnapshotRuntimeResolver`) both fail every call. `load_runnable` (the
/// pre-Stage-B path) would fail at the *catalog* step, before ever reaching
/// the resolver; `resolve_runnable` (the path this test exercises) skips the
/// catalog and fails at the *resolver* step instead.
///
/// 🔴 Proven with `MockSnapshotCatalog::get_calls()`, not by matching on
/// which of the two refusal messages came back. A string match cannot tell
/// "the catalog was consulted and refused" from "the catalog was never asked
/// in the first place" once the wording changes — and it will: Phase 4's own
/// direction is `aenv-node` ending up with no catalog access at all, at
/// which point this fallback is deleted outright and the refusal becomes
/// something like "no catalog access on `aenv-node`", which still contains
/// the substring "catalog" and would keep a string-matching assertion green
/// over behaviour that no longer exists. A call count does not have that
/// blind spot: it reads zero the moment nothing calls `get` any more,
/// whatever the message says.
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

/// The rolling-upgrade fallback `SnapshotSource.resolved_record`'s doc
/// promises: absent, the node resolves `snapshot_id` itself, exactly the
/// pre-Stage-B path — same shape of proof as the test above, mirrored. This
/// is `a_create_uses_the_id_the_caller_chose` in miniature, named for what it
/// specifically guards: an API replica built before this field existed (or a
/// caller that simply has nothing resolved yet) must not be refused for
/// omitting it.
///
/// 🔴 Proven with `MockSnapshotCatalog::get_calls()` — see the skip test
/// above's doc for why a message match on "catalog" cannot be trusted to
/// keep detecting this once `aenv-node` stops holding a catalog at all.
#[tokio::test]
async fn a_snapshot_source_with_no_resolved_record_falls_back_to_the_nodes_own_catalog_lookup() {
    let (_orchestration, service, catalog) = service_with_catalog().await;

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
        .expect_err("the mock catalog fails every call");
    assert_eq!(err.code(), Code::Internal);
    assert!(
        catalog.get_calls() >= 1,
        "an absent resolved_record must fall back to this node's own catalog lookup"
    );
}

/// A `resolved_record` naming a different snapshot than `snapshot_id` is a
/// malformed request, refused loudly rather than silently launching whichever
/// one the record actually names.
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

/// [`a_resolved_snapshot_source_skips_the_nodes_own_catalog_lookup`]'s pair,
/// for `BuildTemplate`'s `base_snapshot_resolved` instead of `Create`'s
/// `resolved_record`. Same proof, same mock manager — see that test's own
/// doc for why `MockSnapshotCatalog::get_calls()` is what settles this,
/// rather than matching on which of the catalog's and the runtime
/// resolver's differently-worded refusals came back.
///
/// The request never reaches `TemplateBuildRunner::execute` (which would
/// need a real Firecracker VM): resolving the base fails first, inside
/// `build_template_impl`'s `Base::BaseSnapshotRef` arm, before the build ever
/// starts.
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
            // 🔴 Must name the resolved record (its id, here) now that the
            // template arm cross-checks `base_snapshot_ref` against
            // `base_snapshot_resolved` — see
            // `a_base_snapshot_ref_naming_a_different_snapshot_is_refused`
            // for the case where it does not.
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

/// P4: the template arm's counterpart to
/// `a_resolved_record_naming_a_different_snapshot_is_refused` — a
/// `base_snapshot_resolved` naming a different snapshot than
/// `base_snapshot_ref` is a malformed request, refused loudly rather than
/// silently building on top of whichever snapshot the record actually names.
///
/// Before this test (and the check it guards) existed, nothing compared the
/// two at all: a misrouted or buggy API replica could send a
/// `base_snapshot_resolved` for one snapshot alongside a `base_snapshot_ref`
/// naming a different one, and the node would build on top of the resolved
/// record without ever noticing the mismatch.
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

/// The other half of the same guard: `base_snapshot_ref` may name the
/// resolved record either by id (proven above and by the skip test) or by
/// its alias — `base_snapshot_ref`'s own doc in node.proto says either is
/// accepted, unlike `Create`'s `snapshot_id`, which is always an id. A record
/// matched only by alias must not be refused as a mismatch.
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
            // Names the record by its alias, not its id — the cross-check
            // must accept this, not just an id match.
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

/// The fallback half of the pair above: no `base_snapshot_resolved`, so the
/// node resolves `base_snapshot_ref` itself, exactly the pre-Stage-B path.
///
/// 🔴 Proven with `MockSnapshotCatalog::get_calls()` — see
/// `a_resolved_snapshot_source_skips_the_nodes_own_catalog_lookup`'s doc for
/// why a message match on "catalog" cannot be trusted to keep detecting this
/// once `aenv-node` stops holding a catalog at all.
#[tokio::test]
async fn a_base_snapshot_ref_with_no_resolved_record_falls_back_to_the_nodes_own_catalog_lookup() {
    let (orchestration, mut service, catalog) = service_with_catalog().await;
    service = service.with_template_build(
        Arc::new(crate::image::ImageResolver::new(
            &crate::cfg::AppConfig::default(),
        )),
        Arc::new(crate::template::TemplateBuilder::new()),
    );
    let _ = &orchestration;

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
        .expect_err("the mock catalog fails every call");
    assert_eq!(err.code(), Code::Internal);
    assert!(
        catalog.get_calls() >= 1,
        "an absent base_snapshot_resolved must fall back to this node's own catalog lookup"
    );
}

/// A create with no source, or an unset timeout action, is refused rather than
/// given a default.
///
/// 🔴 The second half is the one worth having: proto3 hands every enum a zero
/// value whether the sender set it or not, and the two timeout actions differ
/// by whether the user's sandbox is destroyed.
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
                pb::SnapshotSource {
                    snapshot_id: "mock".to_string(),
                    resolved_record: None,
                },
            )),
            timeout_action: pb::TimeoutAction::Unspecified as i32,
            ..Default::default()
        }))
        .await
        .expect_err("a create that did not say what its timeout does");
    assert_eq!(err.code(), Code::InvalidArgument);
}

/// A command naming a run this node is not running is refused, and the sandbox
/// is left alone.
///
/// 🔴 The whole point of carrying the incarnation on every request. A delete
/// written for a run that has since been resumed somewhere else would
/// otherwise tear down a sandbox its author has never seen.
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

    // The control probe: the same call with the right incarnation does delete.
    service
        .delete(Request::new(pb::SandboxDeleteRequest {
            sandbox_id: sandbox.id.to_string(),
            execution_id: sandbox.execution_id.to_string(),
        }))
        .await
        .expect("a delete naming the live run");
    assert!(listed(&service).await.is_empty());
}

/// A command for a sandbox this node has never heard of is `NotFound`, which is
/// a different answer from "your run is stale".
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

/// A fork gives each child the marker the caller assigned to it, and the
/// children come back in the order they were asked for.
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

/// 🔴 The control probe for the fork: a child asked for with no marker is not
/// the control plane's, even though its parent is.
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

    // And it really did start: the orchestrator knows about it, the control
    // plane's listing does not.
    assert!(Arc::clone(&orchestration)
        .get_sandbox(&child_id)
        .await
        .expect("read")
        .is_some());
}

/// The RPCs this build declares and does not serve refuse loudly.
///
/// 🔴 A test rather than a comment. `Unimplemented` is the one status a caller
/// must not retry through, and a method that silently started answering with
/// something plausible would be exactly the "shipped early, wired later" shape
/// that has already cost this project three defects.
///
/// 🔴 `pause`, `checkpoint` and `update_params` are all deliberately absent
/// from this list now. Each is served, and each is pinned by a test that
/// would fail if it went back to refusing — `a_pause_leaves_the_capture_where_the_node_put_it`,
/// `a_checkpoint_answers_with_a_row_nobody_has_announced`, and
/// `update_params_reaches_the_sandbox_and_is_fenced` — so a method
/// reappearing here would break those rather than this.
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
            // 🔴 Filled in so the refusal below is about the *source*. Anything
            // a request can be refused for without touching the machine is
            // refused before the source is looked at, so a request that also
            // said nothing about expiry would be turned away for that instead
            // and this assertion would stop being about a cold create.
            expiry: Some(pb::sandbox_create_request::Expiry::CallerKept(
                pb::CallerKeptExpiry {},
            )),
            ..Default::default()
        }))
        .await
        .expect_err("a cold create is not served");
    assert_eq!(err.code(), Code::Unimplemented);
}

/// Replacing a sandbox's egress policy goes through, and is fenced like
/// everything else.
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

/// A custom extension params update reaches the running sandbox, is fenced
/// like every other property update, and a backend failure never reaches the
/// metadata store.
///
/// # 🔴 Why this test exists
///
/// This RPC used to answer `Unimplemented` unconditionally, which turned
/// `PATCH /sandboxes/{id}/custom-extension-params` into a call that told its
/// caller "done" and updated the metadata store while the running sandbox
/// kept its old value — a control plane lying to a `GET` that followed. Three
/// faces here, all in one round, each differing in exactly one value:
///
/// * **success changes what the mock backend actually holds**, read back
///   through `MockBehavior::last_custom_extension_params` — not just that
///   `update_params` returned `Ok`. `GET`-equivalent evidence (the metadata
///   store's row) is checked too, but never alone.
/// * **fencing on the wrong execution id is refused before anything is
///   touched**: both the runtime and the store keep the value the successful
///   call above applied.
/// * **a backend failure is reported as an error**, and both the runtime and
///   the store keep the last value that actually applied rather than
///   adopting the value the failed call carried. This is the property the
///   old `Unimplemented` handler broke: a failure must not let the store and
///   the runtime disagree.
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

    // Fencing: naming a different run than the one currently live is refused
    // before the runtime or the store are touched.
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

    // A backend failure is reported, and the runtime and the store both keep
    // the last value that actually applied.
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

/// A fork asked for with no children is refused rather than treated as a
/// no-op.
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

/// The size the backend reports for a rootfs before the source was created.
const ROOTFS_WHEN_THE_SOURCE_STARTED: u64 = 4 * 1024 * 1024;
/// And a different one by the time the fork runs.
const ROOTFS_BY_THE_TIME_IT_FORKED: u64 = 9 * 1024 * 1024;

/// A fork answers each child with the facts of the VM that child got — the way
/// a create does, and not with a blank.
///
/// # 🔴 Why both fields are pinned here by name
///
/// `ForkChildResult::Started` carries a `SandboxCreateResponse`, and two of its
/// fields are knowable only from the live handle: the address the sandbox
/// reaches the host on, and the size its rootfs turned out to be. This RPC used
/// to fill both with the wire's zero value while the children's VMs were up and
/// addressable. Nothing in this project's mutation testing covers a `.proto`,
/// and an empty address and a zero size are both *legal* values on those
/// fields, so the only thing that can say the fork read them is a test that
/// says so.
///
/// The control faces, all in one round and each differing in exactly one value:
///
/// * **create against fork.** The source is a sandbox this node created, and
///   the node already answers for it with an address and a size. The children
///   have to answer the same way. A build that fills one field and blanks the
///   other fails here.
/// * **child against child, and child against source.** The two children answer
///   with addresses that differ from each other *and* from the source's. That
///   is what separates "read from each child's own handle" from "filled in with
///   something": a build that copied the source's address, or handed every
///   child one constant, passes "non-empty" and fails this.
/// * **the size moves.** The backend reports a different rootfs size by the
///   time the fork runs than it did when the source started, and the children
///   answer with the new one. A build that carried the source's size across, or
///   left the field at zero, fails on that value.
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

    // What this node answers about a sandbox it *created*: the face the fork
    // has to match.
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

    // 🔴 One value moved before the fork. It is what tells a size read from the
    // child's own handle from one carried over out of the source's record.
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

/// The orchestrator's own fork still works through the facade with assigned
/// children, which is what the RPC above is built on.
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

// ---------------------------------------------------------------------------
// Describe
// ---------------------------------------------------------------------------

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

/// `Describe` answers for a sandbox the listing leaves out, with that
/// sandbox's own live facts.
///
/// # 🔴 The faces, one node and one round apart by a single value each
///
/// * **marked against unmarked.** Both sandboxes are running on this node and
///   they differ in one thing: whether the create that started them carried an
///   ownership marker. `ListSandboxes` reports one of the two — that is its
///   job — and `Describe` answers for both. A build that answered this call out
///   of the listing would be `NOT_FOUND` on the unmarked half, which is exactly
///   the half an API-driven fork child lands in.
/// * **one address against the other.** The two answers carry different
///   addresses, and each one is non-empty. A constant, or a value copied from
///   the other sandbox, fails both halves of that at once.
/// * **the size moves.** The backend reports a different rootfs size by the
///   time it is asked than it did when the sandbox started. The answer has to
///   be the new one: a size taken from the node's record — which does not hold
///   one — or carried over from the create would fail on that value.
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

    // A fork child driven by the API half reaches a node with no marker on it,
    // which is what keeps it out of the listing below.
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

    // 🔴 Moved after both sandboxes started and before either is described.
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

/// A sandbox this node is not running is `NOT_FOUND`, and one it is running is
/// not.
///
/// 🔴 One node, one round, one value apart: which id was asked about. The
/// `NOT_FOUND` half is what a caller reads as *the machine looked and there is
/// nothing there*, and it has to be reachable — a build that answered every
/// `Describe` with facts would pass the other test in this section and this
/// one's control face, and fail here.
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

/// A sandbox whose handle is busy is described as one nobody could read, not as
/// one with no address.
///
/// # 🔴 Why the bit exists at all
///
/// The node answers a busy handle from its record, and its record holds neither
/// the address nor the rootfs size. Both fields then come back as the blanks
/// proto3 cannot tell from *unset* — and the caller for this call is attaching
/// to a sandbox precisely because it has no other way to learn either. Reading
/// those blanks as facts would put "this running sandbox has no address" into a
/// stub, which is the one value that makes the orchestrator above tear a
/// healthy sandbox down.
///
/// The faces are the two sandboxes in this one round: one held by a fork that
/// is still running, one idle.
#[tokio::test]
async fn a_busy_handle_is_described_as_read_from_no_handle() {
    use crate::sandbox::mock::{MockAction, MockOperation};

    crate::logging::init_for_tests();
    let behavior = Arc::new(MockBehavior::new());
    let (orchestration, service) =
        service_with(MockBackendFactory::with_behavior(Arc::clone(&behavior))).await;

    let busy = start(&orchestration, Some(b"busy")).await;
    let idle = start(&orchestration, Some(b"idle")).await;

    // A fork holds the source's handle for the whole operation and leaves it in
    // the table while it runs.
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

// ---------------------------------------------------------------------------
// Resume
// ---------------------------------------------------------------------------

/// Pauses a sandbox this node is running and hands back the run it was paused
/// under.
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

/// A resume reopens the capture this node is holding, under the run the caller
/// claimed and fenced on the run it was paused under.
///
/// 🔴 The control face is the swap. `execution_id` and `resumed_execution_id`
/// are two strings in one message, and nothing in this project's mutation
/// testing covers a `.proto`: exchanging them compiles, resumes the sandbox
/// under the identity it had just been paused with, and fences against a run
/// that has not happened. So the same call is made twice with the two values
/// exchanged, and the exchanged one has to be refused.
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

    // 🔴 Swapped: the fence names the run that has not happened, and the run to
    // start is the one the capture is of.
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

    // The same call, the two values the right way round.
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

/// A resume for a capture this node is not holding is `NotFound`, and a resume
/// for one it is holding is not.
///
/// 🔴 The caller acts on this answer by concluding the only copy of the sandbox
/// is gone — rebuilding it from a published snapshot, or giving it up. So the
/// answer has to be produced by a node that looked and found nothing, which is
/// what the second half of this test establishes the node can tell apart.
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

    // 🔴 The control face: the same call about the sandbox this node *is*
    // holding a capture for succeeds, so `NotFound` is an answer rather than
    // this method's only outcome.
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

/// 🔴 A node that could not read its own records does not answer `NotFound`.
///
/// The two are one `Option` apart in the code and opposite in consequence: the
/// caller reads `NotFound` as "the only copy of this sandbox is gone" and acts
/// on it, and a store that was merely unreachable would have it discard a
/// sandbox whose capture is intact on this disk. One store, one flag, and the
/// same call on both sides of it.
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

    // Reads work: a sandbox that is not here is not here, and that is a fact.
    let err = service
        .resume(Request::new(request()))
        .await
        .expect_err("a resume for a sandbox that was never here");
    assert_eq!(err.code(), Code::NotFound, "{err}");

    // 🔴 The same call, one flag different.
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

/// A resume that would run under the incarnation the sandbox was paused under
/// is refused, and one that starts a new run is not.
///
/// 🔴 A resume starts a new run. Reusing the paused run's identity would leave
/// commands written before the pause indistinguishable from commands written
/// after it — which is exactly what the incarnation on every call here exists
/// to tell apart — and it would do it without any call failing.
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

    // 🔴 And an empty one is refused rather than read as "you choose". A create
    // with no incarnation is a caller that keeps no record; a resume with none
    // is a resume nobody licensed.
    let err = service
        .resume(Request::new(pb::SandboxResumeRequest {
            resumed_execution_id: String::new(),
            ..resume_request(paused.id, paused.execution_id, ExecutionId::new(), 0)
        }))
        .await
        .expect_err("a resume that named no run to start");
    assert_eq!(err.code(), Code::InvalidArgument, "{err}");
    assert!(listed(&service).await.is_empty());

    // The control face: a run of its own is accepted.
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

/// 🔴 A zero timeout keeps the deadline the sandbox was paused with; a non-zero
/// one replaces it.
///
/// The two are told apart by an hour, not by whatever the clock did between two
/// statements: a difference the length of one call would be satisfied by an
/// implementation that ignored the field entirely.
#[tokio::test]
async fn a_zero_timeout_keeps_the_deadline_the_sandbox_was_paused_with() {
    const AN_HOUR_MS: u64 = 60 * 60 * 1_000;
    let (orchestration, service) = service().await;

    // `launch()` creates both of these with a sixty-second timeout.
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

    // 🔴 Neither is zero: zero is what this field carries for a sandbox with no
    // expiry at all, and two zeroes would satisfy the comparison below by
    // saying nothing.
    assert!(kept.expires_at_ms > 0, "{kept:?}");
    assert!(replaced.expires_at_ms > 0, "{replaced:?}");
    assert!(
        replaced.expires_at_ms - kept.expires_at_ms > 3_000_000,
        "the timeout on the request did not reach the sandbox: kept {}, replaced {}",
        kept.expires_at_ms,
        replaced.expires_at_ms
    );
}

// ---------------------------------------------------------------------------
// Pause
// ---------------------------------------------------------------------------

/// Whether a status carries a capture classification, and which one.
///
/// 🔴 `None` and `Some(false)` are not the same answer and the tests below tell
/// them apart: the caller reads a missing classification as *terminal*, so a
/// refusal that meant to say "the sandbox is untouched" and forgot to say it
/// reads as "tear the sandbox down".
fn classification(status: &Status) -> Option<bool> {
    use prost::Message as _;

    if status.details().is_empty() {
        return None;
    }
    pb::SandboxCaptureFailure::decode(status.details())
        .ok()
        .map(|failure| failure.terminal)
}

/// A node service whose persister answers for where captures went.
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

/// A node service that can really stage a capture.
///
/// 🔴 Three things have to be true at once for staging to happen at all, and
/// each of them is off in the default harness: the persister has to hand the
/// pause a directory that outlives the runtime (otherwise there is no
/// publishable capture), the backend's capture has to be something a repository
/// recognises (otherwise staging refuses it), and the repository has to accept
/// writes (the default one refuses, so a test that reaches it fails loudly).
/// Turning all three on here rather than in each test keeps "this test staged
/// nothing" from ever being an accident of the harness.
struct StagingHarness {
    orchestration: Arc<dyn SandboxOrchestration>,
    service: NodeSandboxService,
    repository: Arc<crate::snapshot::mock::RecordingSnapshotRepository>,
    behavior: Arc<MockBehavior>,
    /// Held: the artifact root lives as long as this does.
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

/// The staged row a reply carries, decoded.
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

/// A pause answers with the directory the capture went into and the node's own
/// encoding of it — and the directory is the one the record names, not a blank.
///
/// 🔴 The control face is the same call against a persister that allocated no
/// directory. Without it, "the reply carries the artifact root" is satisfied by
/// an implementation that writes an empty string every time — which is exactly
/// what this reply carried before it was served, and what the caller would then
/// store as the permanent record of where the user's sandbox lives.
#[tokio::test]
async fn a_pause_says_where_the_capture_went() {
    let (orchestration, service, persister) = service_with_persister().await;

    // The half where the node kept nothing: no artifact root, so no directory
    // to name.
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

    // The same call, one value different: a persister that did allocate one.
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

    // And the backend's own encoding travels beside it, versioned, rather than
    // being dropped because the directory was the interesting half.
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

    // 🔴 And both sandboxes are actually paused, so the two replies above are
    // about pauses that happened rather than about a method that answers
    // without doing anything.
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

/// A read of the paused record that failed is a failed pause, not a pause with
/// no directory.
///
/// 🔴 The two are one `Option` apart and opposite in consequence: the caller
/// stores what this reply says and never asks again, so a blank written because
/// a disk hiccuped is a permanent lie about where the user's sandbox lives.
/// The control face is the same persister with the failure spent.
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

    // The same call with nothing forced to fail comes back with the directory,
    // so the refusal above is about the read and not about this node.
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

/// A pause asked to publish stages the capture here and hands the row back
/// unannounced.
///
/// 🔴 `publish` is the only difference between the two halves of this test, and
/// each half is the other's control. Without the `publish: false` half, "a row
/// came back" is satisfied by a node that stages every pause — which is exactly
/// the leak the flag exists to prevent, since nothing commits a row nobody
/// asked for. Without the `publish: true` half, "nothing was staged" is
/// satisfied by a node that stages nothing ever.
///
/// 🔴 The empty `committed()` is paired with a non-empty `staged()` in the same
/// round. On its own it would pass on a build that had stopped doing anything
/// at all; next to a staging that did happen it says the one thing it is here
/// to say — the bytes are durable and the row is still the caller's to
/// announce.
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
    // 🔴 The repository's own identity and not the service's `node_id`. Both
    // resolve to `NodeIdentity::from_config` on a real node, so they agree
    // there; here they deliberately do not, which is what makes this assertion
    // about the value `stage` actually wrote rather than about a string this
    // test handed the service two lines earlier.
    assert_eq!(
        staged.origin_node_id,
        crate::identity::local_node_id(),
        "the row must name the machine holding the bytes"
    );
    assert_ne!(
        staged.origin_node_id, "",
        "a row that names no machine cannot be resumed anywhere"
    );
    assert_eq!(
        staged.execution_id,
        Some(published.execution_id),
        "the row must name the run it is a snapshot of"
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
    // 🔴 The reply is still a pause: the paused state has to be there too, or a
    // published pause is a sandbox the caller cannot reopen anywhere.
    assert!(reply.paused_state.is_some());

    // The control face: the same call with the flag off.
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

/// A pause of a sandbox that is already paused answers with no row rather than
/// failing.
///
/// 🔴 The first pause in the same round is what makes the absent row mean "this
/// call produced no capture" rather than "this node never produces one". The
/// distinction is the whole reason an absent `staged` is allowed to be an
/// answer: collapsing it into a refusal would fail every retried pause, and
/// collapsing the other way would hide a node that had silently stopped
/// staging.
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

/// A pause whose staging failed still pauses, and says what was lost.
///
/// 🔴 This is the one place on this service where a failure is reported inside
/// a success, and the test exists because the alternative is not a worse error
/// message — it is a wrong one. A failed `Pause` carries a classification with
/// two possible readings and both are false here: *recoverable* has the caller
/// put back a sandbox this node has stopped, and *terminal* has it destroy a
/// pause that worked.
///
/// 🔴 Three faces in one round, because `staging_error` exists to tell them
/// apart and any two of them alone would let it be a constant: staging worked
/// (a row, no error), staging broke (no row, an error), and the pause was
/// idempotent (no row, no error).
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
    // 🔴 The pause itself stands: the state to reopen it is there, and the
    // sandbox is off the listing. Without these two the assertions above are
    // satisfied by a call that refused everything.
    assert!(
        broken.paused_state.is_some(),
        "a pause that could not be staged came back with no way to reopen it"
    );
    assert!(
        listed(&harness.service).await.is_empty(),
        "a pause whose staging failed left the sandbox running"
    );

    // The third face: a pause with nothing to stage says nothing failed.
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

/// A checkpoint answers with a row nobody has announced, and leaves the sandbox
/// running.
///
/// 🔴 The sandbox still being listed afterwards is not incidental: a checkpoint
/// that stopped the VM would satisfy every other assertion here, and the whole
/// point of the call is that it does not.
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
    assert_eq!(staged.execution_id, Some(sandbox.execution_id));
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

    // A second checkpoint of the same sandbox is a second snapshot, under a
    // second id — the control that stops the assertion above passing on a node
    // that answers with one cached row forever.
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

/// A checkpoint whose staging fails says the sandbox survived; one whose
/// capture failed terminally says it did not.
///
/// 🔴 The two arms are in one test because the classification is a single bit
/// and "always false" passes any test that only ever asks for one of them. The
/// caller tears sandboxes down on the strength of this bit: read as terminal, a
/// full disk deletes a sandbox that is running and serving requests; read as
/// recoverable, a runtime that was mutated past safe resume is left marked
/// running.
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

    // The other arm: a capture that mutated the runtime past safe resume.
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

/// A pause that failed says whether the sandbox survived it, and says the same
/// thing the node's own orchestrator acted on.
///
/// 🔴 The pair is the point. "Terminal" tells the caller to tear the sandbox
/// down and "recoverable" tells it to put the sandbox back; a classification
/// that was constant would be right for one of these and destructive for the
/// other. Both faces are driven through the same call, one backend action
/// apart.
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

/// A pause whose request does not parse is refused, and the refusal says the
/// sandbox was not touched.
///
/// 🔴 The caller reads a capture failure with no classification as *terminal*,
/// meaning "tear the sandbox down". A malformed request never reaches the
/// runtime, so leaving this one bare would have a caller destroy a running
/// sandbox because of a string it got wrong. The control face is the same call
/// with the field it needs.
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

    // The same call with the run it left out.
    service
        .pause(Request::new(pause_request(&sandbox, false)))
        .await
        .expect("a pause that names the run it means");
    assert!(listed(&service).await.is_empty());
}

/// A pause naming a run this node is not running is refused, and the refusal
/// says the sandbox was not touched.
///
/// 🔴 The classification is the half worth having. The caller treats an
/// unclassified capture failure as terminal — meaning "tear the sandbox down" —
/// so a fence check that refused without saying so would delete a running
/// sandbox because a pause arrived a moment after somebody else's resume. The
/// control face is the same call under the run that *is* live.
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

    // The same call under the run that is live.
    service
        .pause(Request::new(pause_request(&sandbox, false)))
        .await
        .expect("a pause naming the run this node is running");
    assert!(listed(&service).await.is_empty());
}

/// The round trip this half exists for: a sandbox paused through the `Pause`
/// RPC is reopened through the `Resume` RPC, under a run the caller claimed.
///
/// 🔴 Both halves driven through the wire types rather than through the
/// orchestrator, because that is the pair `aenv-api` uses and each was
/// served in a different change. Nothing else in this file pauses through the
/// RPC, so nothing else would notice a `Pause` that answered plausibly and left
/// no record for a `Resume` to find.
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

    // 🔴 The control face for the resume below: a run this node never paused is
    // refused, so "the resume succeeded" is about the record the pause wrote
    // and not about a node that reopens whatever it is asked for.
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

/// 🔴 `NodeSandboxService::build_template`'s whole reason to exist, covered
/// without booting a real Firecracker VM.
///
/// `TemplateBuildRunner::execute` — the one piece of `build_template_impl`
/// this test does not drive — boots a real build sandbox and cannot run
/// inside `cargo test --lib`; that is exactly why
/// `template_build_publish_metadata` exists as a function of its own (see its
/// doc in `service.rs`). This test drives everything *around* it: a
/// hand-built `TemplateBuildExecution` (standing in for what `execute` would
/// have returned) goes through `template_build_publish_metadata`, then
/// through a real `SnapshotManager::stage` against a `PosixFsBackend` — the
/// same call `build_template_impl` makes — then through the exact
/// `encode_value`/`convert::serialized` pair that carries a `StagedSnapshot`
/// across the wire in production, and finally through `commit_staged`, which
/// is the one call on the other side of this RPC that decides whether any of
/// it was worth doing.
///
/// # 🔴 What breaking each of the things this test touches looks like
///
/// - Forget to compute `resources.disk_size_mib` from the manifest (or divide
///   it wrong): `committed.resources.disk_size_mib` below is wrong.
/// - Thread the wrong `virtualization_mode`, `image_configs`, or `context`
///   through: the corresponding assertion below is wrong.
/// - Stop passing `alias: None` (see the doc on `TemplateBuildRequest` in
///   `node.proto` for why that has to stay `None`): the committed row's alias
///   assertion is wrong — it must be *unset*, because nothing on this side of
///   the wire is the alias's owner.
/// - Break `encode_value` or `convert::serialized`'s pairing (a schema
///   version mismatch, a field that stops round-tripping): the `unwrap()` on
///   the decode fails outright.
/// - Stage a manifest `commit_staged` cannot read back (wrong artifact
///   layout, a path that does not survive staging): `commit_staged` fails
///   outright.
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
    // 🔴 The catalog is the test's, standing in for `aenv-api`. This test spans
    // both halves on purpose — a node stages and encodes, the committer decodes
    // and commits — and a node's own repository refuses every catalog call.
    let snapshot_manager = crate::snapshot::SnapshotManager::from_parts(
        crate::snapshot::mock::InMemorySnapshotCatalog::in_front_of(&repository),
        Some(runtime_resolver),
        None,
    );

    let artifacts_workspace = tempfile::tempdir().expect("tempdir");
    let (_, _, manifest): (_, _, FirecrackerSnapshotManifest) =
        write_mock_built_artifacts(artifacts_workspace.path()).expect("mock built artifacts");
    // 🔴 A distinctive, non-round-number virtual size, so a disk-size
    // calculation that silently used a different field (or the wrong shift)
    // would not coincidentally still pass.
    let mut manifest = manifest;
    manifest.rootfs.virtual_size = 5 * (1 << 20) + 1;

    let build_snapshot_id = SnapshotId::generate();
    let resources = SandboxResources {
        cpu_count: 2,
        memory_mib: 256,
        // Deliberately wrong on the way in: this is what `TemplateBuildContext`
        // carries *before* a build runs, and `execute_and_publish`'s own
        // comment names 0 as "disk size is determined after build" — the
        // point of `template_build_publish_metadata` is that it corrects
        // this, so seeding it with the pre-build value here is what proves
        // the correction happened rather than merely surviving.
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
        .stage(metadata, manifest, None)
        .await
        .expect("a freshly built template's artifacts must stage")
        .into_staged();

    // The exact pair `NodeSandboxService::build_template` and the caller that
    // decodes its response use — not a generic `serde_json` round trip.
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
