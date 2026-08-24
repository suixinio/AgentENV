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
use crate::role::ServerRole;
use crate::sandbox::mock::{MockBackendFactory, MockBehavior};
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
        ServerRole::All,
        InMemoryMetadataStore::new(),
        factory,
        DisabledSandboxPersister,
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
        ServerRole::All,
        HalfAnswering(InMemoryMetadataStore::new()),
        MockBackendFactory::new(),
        DisabledSandboxPersister,
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
        ServerRole::All,
        InMemoryMetadataStore::new(),
        Drifting {
            inner: MockBackendFactory::new(),
            drifted: Arc::clone(&drifted),
        },
        DisabledSandboxPersister,
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
        ServerRole::All,
        InMemoryMetadataStore::new(),
        MockBackendFactory::with_behavior(Arc::clone(&behavior)),
        DisabledSandboxPersister,
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
/// 🔴 `pause` is deliberately absent from this list now, and its presence in
/// the one below — where the sandbox has to survive the refusal — is what keeps
/// the two apart: a `pause` that answered `Unimplemented` again would fail
/// `a_pause_leaves_the_capture_where_the_node_put_it`, and a `checkpoint` that
/// started answering would fail this one.
#[tokio::test]
async fn the_unserved_calls_say_so_rather_than_answering() {
    let (orchestration, service) = service().await;
    let sandbox = start(&orchestration, Some(b"owned")).await;

    let err = service
        .checkpoint(Request::new(pb::SandboxCheckpointRequest {
            sandbox_id: sandbox.id.to_string(),
            execution_id: sandbox.execution_id.to_string(),
        }))
        .await
        .expect_err("checkpoint is not served");
    assert_eq!(err.code(), Code::Unimplemented);

    let err = service
        .update_params(Request::new(pb::SandboxParamsRequest {
            sandbox_id: sandbox.id.to_string(),
            execution_id: sandbox.execution_id.to_string(),
            custom_extension_params: None,
        }))
        .await
        .expect_err("update_params is not served");
    assert_eq!(err.code(), Code::Unimplemented);

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
        ServerRole::All,
        store,
        MockBackendFactory::new(),
        DisabledSandboxPersister,
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
        ServerRole::All,
        InMemoryMetadataStore::new(),
        MockBackendFactory::new(),
        persister.clone(),
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

/// Asking for a pause to be published is refused, and refused before the
/// sandbox is touched.
///
/// 🔴 Two assertions and both are load-bearing. The refusal itself keeps
/// `staged: None` meaning "the backend had nothing publishable" rather than
/// "this build cannot stage" — collapsing those is how a sandbox comes to be
/// paused with nothing to rebuild it from anywhere, silently. And the sandbox
/// still running afterwards is what makes it a refusal rather than a pause that
/// threw its own result away.
#[tokio::test]
async fn asking_for_a_published_pause_is_refused_before_the_sandbox_is_touched() {
    let (orchestration, service) = service().await;
    let sandbox = start(&orchestration, Some(b"owned")).await;

    let err = service
        .pause(Request::new(pause_request(&sandbox, true)))
        .await
        .expect_err("publishing a pause is not served");
    assert_eq!(err.code(), Code::Unimplemented, "{err}");
    assert_eq!(
        classification(&err),
        Some(false),
        "a refusal that touched nothing was not classified as such: {err}"
    );
    assert_eq!(
        listed(&service).await.len(),
        1,
        "a refused pause stopped the sandbox anyway"
    );

    // 🔴 The control face: the same call, one flag different, and this one
    // actually pauses. Without it the assertion above is satisfied by a `pause`
    // that refuses everything.
    service
        .pause(Request::new(pause_request(&sandbox, false)))
        .await
        .expect("a pause that asks for nothing this build cannot do");
    assert!(
        listed(&service).await.is_empty(),
        "the sandbox that was paused is still running"
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
            ServerRole::All,
            InMemoryMetadataStore::new(),
            MockBackendFactory::with_behavior(Arc::clone(&behavior)),
            DisabledSandboxPersister,
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
/// orchestrator, because that is the pair `--role api` uses and each was
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
