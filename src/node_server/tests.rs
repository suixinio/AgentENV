//! What the node service answers, driven through the generated server trait.
//!
//! 🔴 These go through `pb::node_sandbox_service_server::NodeSandboxService`
//! rather than through the inherent methods, because the trait is what a
//! request actually arrives on and it is the layer where a method could be
//! wired to the wrong RPC without anything looking wrong.

use std::sync::Arc;
use std::time::Duration;

use tonic::{Code, Request};

use crate::orchestrator::{
    ControlPlaneConfig, CreateSandboxRequest, DisabledSandboxPersister, ForkChildAssignment,
    ForkChildren, InMemoryMetadataStore, NewTimeout, Orchestrator, SandboxLaunchSource,
    SandboxMetadata, SandboxOrchestration, SandboxTimeoutAction,
};
use crate::proto::node as pb;
use crate::proto::node::node_sandbox_service_server::NodeSandboxService as _;
use crate::sandbox::mock::MockBackendFactory;
use crate::sandbox::SandboxNetworkPolicy;
use crate::snapshot::{mock::mock_snapshot_manager, RunnableSnapshot};
use crate::types::{ExecutionId, SandboxId};

use super::service::NodeSandboxService;

const NODE: &str = "node-under-test";

async fn service() -> (Arc<dyn SandboxOrchestration>, NodeSandboxService) {
    crate::logging::init_for_tests();
    let orchestrator = Orchestrator::new(
        InMemoryMetadataStore::new(),
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
    (orchestration, service)
}

fn launch(marker: Option<&[u8]>) -> CreateSandboxRequest {
    CreateSandboxRequest {
        source: SandboxLaunchSource::Snapshot(Box::new(RunnableSnapshot::mock())),
        timeout: Some(Duration::from_secs(60)),
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
}

/// The empty answer a node with nothing running gives is an answer, not a
/// failure, and it is the one a caller may act on.
#[tokio::test]
async fn a_node_running_nothing_at_all_answers_with_an_empty_list() {
    let (_orchestration, service) = service().await;
    assert!(listed(&service).await.is_empty());
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
            timeout_ms: 60_000,
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
            timeout_ms: 0,
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

/// The three RPCs this build declares and does not serve refuse loudly.
///
/// 🔴 A test rather than a comment. `Unimplemented` is the one status a caller
/// must not retry through, and a method that silently started answering with
/// something plausible would be exactly the "shipped early, wired later" shape
/// that has already cost this project three defects.
#[tokio::test]
async fn the_unserved_calls_say_so_rather_than_answering() {
    let (orchestration, service) = service().await;
    let sandbox = start(&orchestration, Some(b"owned")).await;

    let err = service
        .pause(Request::new(pb::SandboxPauseRequest {
            sandbox_id: sandbox.id.to_string(),
            execution_id: sandbox.execution_id.to_string(),
            publish: false,
        }))
        .await
        .expect_err("pause is not served");
    assert_eq!(err.code(), Code::Unimplemented);

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
