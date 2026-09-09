//! The deciding half's sandbox control path.
//!
//! Every operation is one straight line: choose a node, write the record, call
//! the node, settle the record — plus the rollback arm that puts back whatever
//! the line had already changed when a step fails. Nothing here holds a VM, a
//! handle table or a proxy route: a sandbox this process decided about runs on
//! a machine that answers for it, and the routing binding is where the cluster
//! keeps that answer.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::Context as _;
use async_trait::async_trait;
use tokio::sync::{broadcast, oneshot, watch};
use tokio::time::MissedTickBehavior;
use tracing::{debug, error, info, warn};

use crate::orchestrator::RuntimeRouting;
use aenv_core::cfg::ConfigManager;
use aenv_core::orchestrator::launch_parts::{
    configured_runtime_versions, resources_with_runtime_info, snapshot_create_parts,
    SnapshotCreateInputs, SnapshotCreateParts,
};
use aenv_core::orchestrator::metrics::{aggregate_resource_metrics, SandboxContribution};
use aenv_core::orchestrator::store::{
    configured_max_sandbox_lifetime, FencedRemoval, MetadataStore, NewTimeout, SandboxListFilter,
    SandboxMetadata, StoreError, TransitionOutcome, TransitionRequest, ALL_SANDBOX_STATES,
};
use aenv_core::orchestrator::{
    ControlPlaneConfig, CreateSandboxRequest, ForkChildAssignment, ForkChildren, GrantIssuer,
    LaunchHeldElsewhere, OrchestratorError, OrchestratorMetrics, PauseOutcome, PausePublisher,
    PublishedPause, RestoredSandbox, Result, SandboxExpiry, SandboxForkOutcome,
    SandboxLaunchSource, SandboxLifecycleEvent, SandboxLifecycleEventType, SandboxOperation,
    SandboxOrchestration, SandboxRosterEntry, SandboxState, SandboxTimeoutAction,
    SnapshotCaptureResult,
};
use aenv_core::sandbox::{
    AccessTokenSeedPolicy, CapturedSandboxSnapshot, CustomExtensionClient, CustomExtensionParams,
    EnvdAccessToken, RuntimeArtifactSet, RuntimeConfirmedGone, SandboxAccessTokenGenerator,
    SandboxForkSpec, SandboxLaunchConfig, SandboxNetworkPolicy, SandboxRuntimeInfo,
    UnresolvedImageBuildSpec,
};
use aenv_core::snapshot::SnapshotRecord;
use aenv_core::types::{ExecutionId, SandboxId, SandboxResources};

use crate::node_client::{NodeLaunchBuilder, NodePlacement, RemoteSandboxStub, SandboxRecordOwner};

mod counters;

pub use counters::ControlCounters;

/// How long a caller waits for a sandbox to leave a transitional state.
const WAIT_TRANSITION_TIMEOUT: Duration = Duration::from_secs(60);
/// How often a caller waiting out another replica's launch re-reads the shared
/// record. The launch it waits on writes that record once, from another
/// process, so there is nothing local to be woken by.
const LAUNCH_ELSEWHERE_POLL: Duration = Duration::from_millis(250);
const SANDBOX_EVENT_CHANNEL_CAPACITY: usize = 1024;
/// Maximum expired sandboxes processed per eviction round.
const AUTO_EVICT_BATCH_LIMIT: usize = 256;
/// A grant younger than this is left alone by the reaper: a create grants
/// before it writes the record, and a resume carries the record across nodes,
/// so a fresh grant with no record yet is not an orphan.
const GRANT_REAP_MIN_AGE: Duration = Duration::from_secs(10 * 60);
const GRANT_REAP_BATCH_LIMIT: usize = 256;

/// Commit attempts for a pause whose VM can no longer be resumed. The bytes
/// are the only copy of the sandbox from the first attempt on, so the commit
/// is retried rather than abandoned.
const PAUSE_PUBLICATION_ATTEMPTS: u32 = 5;
/// Delay before the second attempt; each further wait doubles it.
const PAUSE_PUBLICATION_RETRY_BACKOFF: Duration = Duration::from_secs(2);
/// Wall-clock budget for the retries, measured from the first failed commit.
const PAUSE_PUBLICATION_RETRY_DEADLINE: Duration = Duration::from_secs(45);
/// Slack over that budget for the capture, the commit that failed first and
/// the attempt still running when the budget ends.
const PAUSE_JOIN_MARGIN: Duration = Duration::from_secs(30);
/// How long a caller waits out a pause somebody else is performing.
const CONCURRENT_PAUSE_WAIT: Duration =
    Duration::from_secs(PAUSE_PUBLICATION_RETRY_DEADLINE.as_secs() + PAUSE_JOIN_MARGIN.as_secs());

/// The collaborators the control path is assembled from. Everything it can
/// ever need is here: nothing is installed after construction, so no operation
/// has to ask at run time which half it is running in.
pub struct SandboxControlParts<S: MetadataStore> {
    pub access_token_seed_policy: AccessTokenSeedPolicy,
    pub store: S,
    pub placement: Arc<dyn NodePlacement>,
    pub record_owner: Arc<dyn SandboxRecordOwner>,
    pub routing: Arc<dyn RuntimeRouting>,
    pub pause_publisher: Arc<dyn PausePublisher>,
    pub grants: Arc<dyn GrantIssuer>,
}

/// What a launch has decided before any node is asked for a runtime.
struct ApiLaunch {
    sandbox_id: SandboxId,
    execution_id: ExecutionId,
    launch_config: SandboxLaunchConfig,
    metadata: SandboxMetadata,
    timeout: NewTimeout,
    source: ApiLaunchSource,
}

enum ApiLaunchSource {
    SnapshotRecord(Box<SnapshotRecord>),
    UnresolvedImage(Box<UnresolvedImageBuildSpec>),
}

/// Where a launch failed, which decides what its rollback has to take back.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FailedLaunchStage {
    /// The store refused the record, so this launch has none to take back and
    /// the node's own teardown is what retires its routing record.
    RecordRefused,
    /// A record may stand under this launch's incarnation.
    Recorded,
}

/// The node running one sandbox, or the placement source's verdict that none is.
enum NodeSession {
    Live(Box<RemoteSandboxStub>),
    /// A complete lookup found nothing: no machine is running this sandbox.
    Gone,
}

pub struct SandboxControl<S: MetadataStore> {
    store: S,
    launches: NodeLaunchBuilder,
    placement: Arc<dyn NodePlacement>,
    routing: Arc<dyn RuntimeRouting>,
    pause_publisher: Arc<dyn PausePublisher>,
    grants: Arc<dyn GrantIssuer>,
    access_tokens: SandboxAccessTokenGenerator,
    counters: ControlCounters,
    sandbox_event_tx: broadcast::Sender<SandboxLifecycleEvent>,
    default_sandbox_timeout: Duration,
    is_shutting_down: AtomicBool,
    /// Isolation refuses new placements while preserving existing sandboxes.
    scheduling_disabled: AtomicBool,
    /// Unix milliseconds of the last isolation change; zero means never.
    scheduling_disabled_changed_at_ms: AtomicI64,
    shutdown_tx: watch::Sender<bool>,
}

impl<S: MetadataStore + 'static> SandboxControl<S> {
    pub async fn new(parts: SandboxControlParts<S>) -> Result<Arc<Self>> {
        let SandboxControlParts {
            access_token_seed_policy,
            store,
            placement,
            record_owner,
            routing,
            pause_publisher,
            grants,
        } = parts;

        let app_config = ConfigManager::global_config();
        let config = &app_config.orchestrator;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (sandbox_event_tx, _sandbox_event_rx) =
            broadcast::channel(SANDBOX_EVENT_CHANNEL_CAPACITY);

        // No record this process keeps survives its restart, so no restored
        // record can demand a pre-existing seed.
        let managed_seed_must_exist = false;
        let access_tokens = tokio::task::spawn_blocking(move || {
            SandboxAccessTokenGenerator::load_or_create(
                app_config,
                access_token_seed_policy,
                managed_seed_must_exist,
            )
        })
        .await
        .context("join envd access-token seed loader")??;

        let control = Arc::new(Self {
            store,
            launches: NodeLaunchBuilder::new(Arc::clone(&placement), record_owner),
            placement,
            routing,
            pause_publisher,
            grants,
            access_tokens,
            counters: ControlCounters::default(),
            sandbox_event_tx,
            default_sandbox_timeout: Duration::from_secs(config.default_sandbox_timeout_secs),
            is_shutting_down: AtomicBool::new(false),
            scheduling_disabled: AtomicBool::new(false),
            scheduling_disabled_changed_at_ms: AtomicI64::new(0),
            shutdown_tx,
        });

        let evict_interval = Duration::from_millis(config.auto_evict_interval_ms);
        Self::start_auto_evict_task(Arc::clone(&control), evict_interval, shutdown_rx);

        Ok(control)
    }

    // ---- plumbing -------------------------------------------------------

    /// Runs an operation on its own task so a caller that stops waiting cannot
    /// leave a half-applied change behind.
    async fn run_cancellation_safe<T>(
        self: &Arc<Self>,
        operation: &'static str,
        sandbox_id: SandboxId,
        future: impl std::future::Future<Output = Result<T>> + Send + 'static,
    ) -> Result<T>
    where
        T: Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            let result = future.await;
            if tx.send(result).is_err() {
                debug!(
                    %sandbox_id,
                    operation,
                    "operation completed after caller stopped waiting"
                );
            }
        });

        rx.await.map_err(|err| {
            OrchestratorError::InternalError(format!(
                "operation task ended before reporting result: {err}"
            ))
        })?
    }

    fn is_shutting_down(&self) -> bool {
        self.is_shutting_down.load(Ordering::Acquire)
    }

    fn ensure_accepting_lifecycle_operations(&self) -> Result<()> {
        if self.is_shutting_down() {
            info!("rejecting lifecycle operation because this process is shutting down");
            return Err(OrchestratorError::ShuttingDown);
        }
        Ok(())
    }

    fn ensure_accepting_new_work(&self) -> Result<()> {
        self.ensure_accepting_lifecycle_operations()?;
        if self.scheduling_disabled.load(Ordering::Acquire) {
            info!("rejecting new work because this replica is isolated");
            return Err(OrchestratorError::NotAcceptingNewWork);
        }
        Ok(())
    }

    fn publish_sandbox_event(
        &self,
        event_type: SandboxLifecycleEventType,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
        resources: SandboxResources,
    ) {
        let _ = self.sandbox_event_tx.send(SandboxLifecycleEvent {
            event_type,
            sandbox_id,
            execution_id,
            resources,
        });
    }

    fn new_timeout_for(&self, expiry: SandboxExpiry) -> NewTimeout {
        match expiry {
            SandboxExpiry::After(timeout) => NewTimeout::Set(timeout),
            SandboxExpiry::AfterConfiguredDefault => NewTimeout::Set(self.default_sandbox_timeout),
            SandboxExpiry::NotKeptHere => NewTimeout::None,
        }
    }

    /// The node running a sandbox, or the verdict that nothing is.
    ///
    /// The routing binding answers this, not this replica's memory: another
    /// replica placed most of the sandboxes this one is asked about.
    async fn node_session(
        &self,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
        resources: SandboxResources,
    ) -> Result<NodeSession> {
        let mut session = self
            .launches
            .attach_to_running(sandbox_id, execution_id, resources);
        match session.start().await {
            Ok(()) => Ok(NodeSession::Live(Box::new(session))),
            Err(error) if error.downcast_ref::<RuntimeConfirmedGone>().is_some() => {
                warn!(
                    %sandbox_id,
                    error = %format_args!("{error:#}"),
                    "the machine running this sandbox is gone; treating its runtime as gone"
                );
                Ok(NodeSession::Gone)
            }
            Err(error) => Err(OrchestratorError::InternalError(format!(
                "sandbox {sandbox_id} could not be reached on the machine running it: {error:#}"
            ))),
        }
    }

    /// Whether the cluster has stopped routing to this sandbox's runtime.
    ///
    /// Only a complete answer counts: a lookup that fails leaves the record
    /// alone.
    async fn runtime_confirmed_gone(&self, sandbox_id: SandboxId) -> bool {
        match self.routing.is_routed(sandbox_id).await {
            Ok(routed) => !routed,
            Err(error) => {
                warn!(
                    %sandbox_id,
                    error = %format_args!("{error:#}"),
                    "could not tell whether anything still routes to this sandbox"
                );
                false
            }
        }
    }

    /// Drops the record of a runtime nothing routes to any more.
    async fn forget_unrouted_runtime(&self, sandbox_id: SandboxId, execution_id: ExecutionId) {
        warn!(
            %sandbox_id,
            %execution_id,
            "nothing routes to this sandbox's runtime any more; dropping its record"
        );
        if let Err(error) = self.forget_sandbox(sandbox_id, execution_id).await {
            warn!(
                %sandbox_id,
                error = ?error,
                "failed to remove the record of a sandbox nothing routes to"
            );
        }
    }

    /// Retires the cluster's routing answer for one incarnation.
    ///
    /// Failure is not fatal to the teardown: the node's own event and the next
    /// heartbeat's reconcile still remove the binding, one interval later.
    async fn forget_runtime_routing(&self, sandbox_id: SandboxId, execution_id: ExecutionId) {
        if let Err(error) = self.routing.forget(sandbox_id, execution_id).await {
            warn!(
                %sandbox_id,
                %execution_id,
                error = %format_args!("{error:#}"),
                "could not retire this sandbox's routing binding; traffic may reach the node \
                 that no longer runs it until a heartbeat reconciles it"
            );
        }
    }

    /// Drops a sandbox's record, its routing binding and its incarnation's
    /// grant together. The routing binding goes first, so no request is routed
    /// at a runtime this teardown has already stopped; the grant goes even when
    /// the record removal fails.
    ///
    /// The removal is fenced on `execution_id`: a record another incarnation
    /// wrote under this id belongs to that incarnation.
    async fn forget_sandbox(
        &self,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
    ) -> Result<Option<SandboxMetadata>> {
        // Read first so the caller still learns what the removal took.
        let current = self.store.get(&sandbox_id).await;
        self.forget_runtime_routing(sandbox_id, execution_id).await;
        let removal = self
            .store
            .remove_if_execution(&sandbox_id, execution_id, &ALL_SANDBOX_STATES)
            .await;
        self.revoke_secrets(sandbox_id, execution_id).await;
        match removal? {
            FencedRemoval::Removed => Ok(current?),
            FencedRemoval::Absent => Ok(None),
            FencedRemoval::Superseded {
                state,
                execution_id: actual,
            } => {
                info!(
                    %sandbox_id,
                    %execution_id,
                    actual_execution_id = %actual,
                    actual_state = ?state,
                    "left this sandbox's record alone: a newer incarnation owns it"
                );
                Ok(None)
            }
        }
    }

    // ---- grants ---------------------------------------------------------

    /// Grants the names the policy references before the sandbox can open a
    /// brokered connection. An empty set is a no-op for every issuer.
    async fn grant_secrets(
        &self,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
        policy: &SandboxNetworkPolicy,
    ) -> anyhow::Result<()> {
        let wanted = policy.egress.referenced_secrets();
        if wanted.is_empty() {
            return Ok(());
        }
        // A wake carries a policy accepted long ago and must not be refused
        // now, so a name deleted or reshaped meanwhile is reported here and the
        // sandbox still starts.
        if let Some(unusable) = self.grants.unusable_names(&wanted).await {
            if !unusable.is_empty() {
                warn!(
                    %sandbox_id,
                    %execution_id,
                    unusable = %unusable.join(", "),
                    "starting a sandbox whose policy names secrets the store cannot serve as \
                     used; brokered connections using them will be refused"
                );
            }
        }
        let names: BTreeSet<String> = wanted.into_keys().collect();
        self.grants
            .grant(sandbox_id, execution_id, &names)
            .await
            .with_context(|| {
                format!(
                    "grant {} secret name(s) to sandbox {sandbox_id}",
                    names.len()
                )
            })
    }

    /// Best effort: a grant that outlives its incarnation is unusable because
    /// the execution id it names is gone, and the reaper picks the row up on a
    /// later round.
    async fn revoke_secrets(&self, sandbox_id: SandboxId, execution_id: ExecutionId) {
        if let Err(err) = self.grants.revoke(sandbox_id, execution_id).await {
            warn!(
                %sandbox_id,
                %execution_id,
                error = %format_args!("{err:#}"),
                "failed to revoke secret grants"
            );
        }
    }

    /// One write supersedes the whole previous name set; an empty set revokes.
    async fn regrant_secrets(
        &self,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
        names: &BTreeSet<String>,
    ) -> anyhow::Result<()> {
        if names.is_empty() {
            return self
                .grants
                .revoke(sandbox_id, execution_id)
                .await
                .with_context(|| format!("revoke the secret grants of sandbox {sandbox_id}"));
        }
        self.grants
            .grant(sandbox_id, execution_id, names)
            .await
            .with_context(|| {
                format!(
                    "grant {} secret name(s) to sandbox {sandbox_id}",
                    names.len()
                )
            })
    }

    /// Revokes aged grants no record backs. A grant whose sandbox record names
    /// the same incarnation is live and kept; a store that cannot answer ends
    /// the round, since nothing can then be compared against.
    pub async fn reap_orphaned_grants(&self) -> Result<Vec<(SandboxId, ExecutionId)>> {
        let candidates = self
            .grants
            .stale_grant_candidates(GRANT_REAP_MIN_AGE, GRANT_REAP_BATCH_LIMIT)
            .await
            .map_err(|err| OrchestratorError::InternalError(format!("{err:#}")))?;
        let mut reaped = Vec::new();
        for (sandbox_id, execution_id) in candidates {
            let record = match self.store.get(&sandbox_id).await {
                Ok(record) => record,
                Err(err) => {
                    warn!(
                        %sandbox_id,
                        error = ?err,
                        "grant reaper stopped: the store could not say whether the sandbox exists"
                    );
                    break;
                }
            };
            if record.is_some_and(|record| record.execution_id == execution_id) {
                continue;
            }
            info!(%sandbox_id, %execution_id, "revoking a secret grant no sandbox record backs");
            self.revoke_secrets(sandbox_id, execution_id).await;
            reaped.push((sandbox_id, execution_id));
        }
        Ok(reaped)
    }
}

// ---- create ------------------------------------------------------------

impl<S: MetadataStore + 'static> SandboxControl<S> {
    /// Creates and starts a sandbox under a fresh identity.
    pub async fn create_sandbox(
        self: &Arc<Self>,
        request: CreateSandboxRequest,
    ) -> Result<SandboxMetadata> {
        let sandbox_id = SandboxId::new();
        let this = Arc::clone(self);
        self.run_cancellation_safe("create", sandbox_id, async move {
            this.create_sandbox_inner(sandbox_id, request).await
        })
        .await
    }

    /// Starts a sandbox under an id the caller already records: a resume of a
    /// paused sandbox from its snapshot row. Nothing here arbitrates; the
    /// caller's row is the licence.
    pub async fn restore_sandbox(
        self: &Arc<Self>,
        sandbox_id: SandboxId,
        request: CreateSandboxRequest,
    ) -> Result<SandboxMetadata> {
        let this = Arc::clone(self);
        self.run_cancellation_safe("restore", sandbox_id, async move {
            this.create_sandbox_inner(sandbox_id, request).await
        })
        .await
    }

    /// Starts a sandbox under an id the caller records, waiting out a launch of
    /// the same id another replica holds rather than starting a second one.
    pub async fn restore_or_join_launch(
        self: &Arc<Self>,
        sandbox_id: SandboxId,
        request: CreateSandboxRequest,
    ) -> Result<RestoredSandbox> {
        match self.restore_sandbox(sandbox_id, request).await {
            Err(error) if LaunchHeldElsewhere::refused(&error) => self
                .await_launch_elsewhere(sandbox_id, error)
                .await
                .map(|metadata| RestoredSandbox {
                    metadata,
                    joined: true,
                }),
            other => other.map(|metadata| RestoredSandbox {
                metadata,
                joined: false,
            }),
        }
    }

    /// Waits for the launch another replica holds to leave a running record.
    ///
    /// The reservation says who is launching; the record it writes is what this
    /// caller is waiting for, and is the same record a resume and a connect
    /// already read.
    pub(crate) async fn await_launch_elsewhere(
        &self,
        sandbox_id: SandboxId,
        refusal: OrchestratorError,
    ) -> Result<SandboxMetadata> {
        let deadline = tokio::time::Instant::now() + WAIT_TRANSITION_TIMEOUT;
        info!(
            %sandbox_id,
            "another replica is launching this sandbox; waiting for the record it will write"
        );
        loop {
            match self.store.get(&sandbox_id).await? {
                Some(metadata) if metadata.state == SandboxState::Running => return Ok(metadata),
                Some(metadata) if metadata.state == SandboxState::Killing => {
                    return Err(OrchestratorError::SandboxNotFound(sandbox_id))
                }
                _ => {}
            }
            let now = tokio::time::Instant::now();
            if now >= deadline {
                warn!(%sandbox_id, "timed out waiting for another replica to finish this launch");
                // The refusal is what this caller actually met; a launch that
                // never produced a record has nothing better to report.
                return Err(refusal);
            }
            tokio::time::sleep(LAUNCH_ELSEWHERE_POLL.min(deadline - now)).await;
        }
    }

    #[tracing::instrument(
        name = "create_sandbox",
        skip(self, request),
        fields(sandbox_id = %sandbox_id)
    )]
    async fn create_sandbox_inner(
        self: Arc<Self>,
        sandbox_id: SandboxId,
        request: CreateSandboxRequest,
    ) -> Result<SandboxMetadata> {
        if let Err(err) = self.ensure_accepting_new_work() {
            self.counters.record_create_fail(1);
            return Err(err);
        }

        let plan = match self.plan_launch(sandbox_id, request) {
            Ok(plan) => plan,
            Err(err) => {
                self.counters.record_create_fail(1);
                return Err(err);
            }
        };

        match self.launch(plan).await {
            Ok(metadata) => {
                self.counters.record_create_success(1);
                self.publish_sandbox_event(
                    SandboxLifecycleEventType::Create,
                    metadata.id,
                    metadata.execution_id,
                    metadata.resources,
                );
                Ok(metadata)
            }
            Err(err) => {
                self.counters.record_create_fail(1);
                Err(err)
            }
        }
    }

    /// Turns a request into the record the sandbox will have and the create the
    /// node will be sent.
    fn plan_launch(
        &self,
        sandbox_id: SandboxId,
        request: CreateSandboxRequest,
    ) -> Result<ApiLaunch> {
        let CreateSandboxRequest {
            source,
            expiry,
            timeout_action,
            user_metadata,
            env_vars,
            auto_resume,
            network_policy,
            custom_extension_params,
            secure,
            traffic_access_token,
            control_plane_config,
            execution_id,
            preferred_node_id,
        } = request;
        let envd_access_token = secure.then(|| self.access_tokens.generate(sandbox_id));
        // Log the three-state expiry decision without collapsing its meanings.
        info!(?expiry, "creating sandbox");
        let timeout = self.new_timeout_for(expiry);

        let (launch_config, metadata, launch_source) = match source {
            SandboxLaunchSource::Snapshot(snapshot) => {
                let parts = snapshot_create_parts(
                    snapshot.record(),
                    SnapshotCreateInputs {
                        sandbox_id,
                        envd_access_token,
                        env_vars,
                        user_metadata,
                        network_policy,
                        custom_extension_params,
                        timeout_action,
                        auto_resume,
                        secure,
                        traffic_access_token,
                        control_plane_config,
                        preferred_node_id,
                    },
                )?;
                let SnapshotCreateParts {
                    launch_config,
                    transitional_metadata,
                } = parts;
                (
                    launch_config,
                    transitional_metadata,
                    ApiLaunchSource::SnapshotRecord(Box::new(snapshot.record().clone())),
                )
            }
            SandboxLaunchSource::SnapshotRecord(record) => {
                let SnapshotCreateParts {
                    launch_config,
                    transitional_metadata,
                } = snapshot_create_parts(
                    &record,
                    SnapshotCreateInputs {
                        sandbox_id,
                        envd_access_token,
                        env_vars,
                        user_metadata,
                        network_policy,
                        custom_extension_params,
                        timeout_action,
                        auto_resume,
                        secure,
                        traffic_access_token,
                        control_plane_config,
                        preferred_node_id,
                    },
                )?;
                (
                    launch_config,
                    transitional_metadata,
                    ApiLaunchSource::SnapshotRecord(record),
                )
            }
            SandboxLaunchSource::UnresolvedImage {
                image_ref,
                resources,
                attached_drives,
                extra_boot_args,
            } => {
                let launch_config = SandboxLaunchConfig {
                    sandbox_id,
                    snapshot_id: image_ref.clone(),
                    env_vars,
                    network: network_policy.runtime_policy(),
                    // The node builds its own launch config after resolution.
                    extra_mmds: serde_json::Map::new(),
                    custom_extension_params: custom_extension_params.clone(),
                    envd_access_token,
                    traffic_access_token: traffic_access_token.clone(),
                    control_plane_config: None,
                    preferred_node_id: preferred_node_id.clone(),
                };
                let metadata = SandboxMetadata {
                    id: sandbox_id,
                    snapshot_id: image_ref.clone(),
                    snapshot_alias: None,
                    virtualization_mode: ConfigManager::global_config().virtualization_mode,
                    runtime_versions: configured_runtime_versions(),
                    resources,
                    // Resolution facts are filled from the node's start response.
                    timeout_action,
                    auto_resume,
                    user_metadata,
                    network_policy,
                    custom_extension_params,
                    secure,
                    traffic_access_token,
                    control_plane_config,
                    max_lifetime: configured_max_sandbox_lifetime(),
                    ..Default::default()
                };
                (
                    launch_config,
                    metadata,
                    ApiLaunchSource::UnresolvedImage(Box::new(UnresolvedImageBuildSpec {
                        image_ref,
                        resources,
                        attached_drives,
                        extra_boot_args,
                    })),
                )
            }
            // A build spec resolved into local paths names files no other
            // machine has, and the image reference a node would need is no
            // longer in it.
            SandboxLaunchSource::Image { image_ref, .. } => {
                return Err(OrchestratorError::InvalidRequest(format!(
                    "a cold sandbox cannot be started on another node from {image_ref}: the \
                     build spec has already been resolved into local paths, and the image \
                     reference a node would need is no longer in it"
                )));
            }
        };

        let execution_id = execution_id.unwrap_or_else(ExecutionId::new);
        let mut metadata = metadata;
        metadata.execution_id = execution_id;
        // The marker travels with the complete record, so an unstamped create
        // stays unclaimed rather than being reconciled away.
        if metadata.control_plane_config.is_none() {
            metadata.control_plane_config = ControlPlaneConfig::for_record(&metadata);
        }
        let mut launch_config = launch_config;
        launch_config.control_plane_config = metadata
            .control_plane_config
            .as_ref()
            .map(|marker| marker.as_bytes().to_vec());

        Ok(ApiLaunch {
            sandbox_id,
            execution_id,
            launch_config,
            metadata,
            timeout,
            source: launch_source,
        })
    }

    /// Takes the sandbox id for this launch, runs it, and gives the id back.
    ///
    /// The reservation brackets the whole launch: it is held before the record
    /// under this id is read and given back only once that record is written,
    /// so a concurrent launch of the same id cannot read "no record" through
    /// the window this one needs to write one.
    async fn launch(self: &Arc<Self>, plan: ApiLaunch) -> Result<SandboxMetadata> {
        self.ensure_accepting_lifecycle_operations()?;

        let sandbox_id = plan.sandbox_id;
        let execution_id = plan.execution_id;

        if let Err(source) = self
            .placement
            .reserve_launch(sandbox_id, execution_id)
            .await
        {
            return Err(OrchestratorError::SandboxOperationFailed {
                sandbox_id,
                operation: SandboxOperation::Start,
                source: source.context(format!("reserve the launch of sandbox {sandbox_id}")),
            });
        }
        let launched = self.launch_holding_the_id(plan).await;
        if let Err(error) = self
            .placement
            .release_launch(sandbox_id, execution_id)
            .await
        {
            warn!(
                %sandbox_id,
                %execution_id,
                error = %format_args!("{error:#}"),
                "could not give back the launch reservation of this sandbox; it expires on its own"
            );
        }
        launched
    }

    async fn launch_holding_the_id(self: &Arc<Self>, plan: ApiLaunch) -> Result<SandboxMetadata> {
        let sandbox_id = plan.sandbox_id;
        let execution_id = plan.execution_id;

        // Refuse an id a record already names. The routing binding is the only
        // thing that can say the id is free: a record left behind by a runtime
        // nobody routes to any more must not refuse this sandbox's resume.
        if let Some(held) = self.store.get(&sandbox_id).await? {
            if self.runtime_confirmed_gone(sandbox_id).await {
                self.forget_unrouted_runtime(sandbox_id, held.execution_id)
                    .await;
            } else {
                warn!(
                    %sandbox_id,
                    held_execution_id = %held.execution_id,
                    %execution_id,
                    "refusing a launch under a sandbox id this control plane already records"
                );
                return Err(OrchestratorError::StoreOperationFailed(
                    StoreError::SandboxAlreadyExists { sandbox_id },
                ));
            }
        }

        let mut node = match self.build_launch(&plan) {
            Ok(node) => node,
            Err(err) => {
                self.rollback_launch(&plan).await;
                return Err(err);
            }
        };
        node.set_projection_budget(plan.metadata.projection_ttl_secs(SystemTime::now()));

        if let Err(source) = self
            .grant_secrets(sandbox_id, execution_id, &plan.metadata.network_policy)
            .await
        {
            warn!(error = %format_args!("{source:#}"), "failed to grant secrets before start");
            self.rollback_launch(&plan).await;
            return Err(OrchestratorError::SandboxOperationFailed {
                sandbox_id,
                operation: SandboxOperation::Start,
                source,
            });
        }

        // One call: the reservation, the placement decision, the node's create
        // and the routing record it produces.
        if let Err(source) = node.start().await {
            warn!(error = %format_args!("{source:#}"), "failed to start sandbox");
            self.revoke_secrets(sandbox_id, execution_id).await;
            if let Err(stop_err) = node.stop().await {
                warn!(error = %format_args!("{stop_err:#}"), "failed to stop sandbox after start failure");
            }
            self.rollback_launch(&plan).await;
            return Err(OrchestratorError::SandboxOperationFailed {
                sandbox_id,
                operation: SandboxOperation::Start,
                source,
            });
        }

        if self.is_shutting_down() {
            info!("this process started shutting down just after starting the sandbox");
            self.cleanup_failed_launch(&plan, &mut node, FailedLaunchStage::Recorded)
                .await;
            return Err(OrchestratorError::ShuttingDown);
        }

        let runtime_info = SandboxRuntimeInfo {
            rootfs_virtual_size: node.rootfs_virtual_size(),
            runtime_artifacts: RuntimeArtifactSet::empty(),
            resolved_image_facts: node.resolved_image_facts(),
        };
        let runtime_resources =
            resources_with_runtime_info(plan.metadata.resources, runtime_info.clone());
        let transitional_metadata = {
            let mut metadata = plan.metadata.clone();
            metadata.resources = runtime_resources;
            // The node fills resolution facts after start.
            if let Some(facts) = runtime_info.resolved_image_facts.clone() {
                metadata.context = facts.context;
                metadata.image_configs = facts.image_configs;
            }
            metadata
        };

        if let Err(err) = self.store.add(transitional_metadata).await {
            warn!(error = %format_args!("{err:#}"), "failed to persist sandbox metadata; cleaning up");
            self.cleanup_failed_launch(&plan, &mut node, FailedLaunchStage::RecordRefused)
                .await;
            return Err(OrchestratorError::from(err));
        }

        if self.is_shutting_down() {
            info!("this process started shutting down before the sandbox was recorded running");
            self.cleanup_failed_launch(&plan, &mut node, FailedLaunchStage::Recorded)
                .await;
            return Err(OrchestratorError::ShuttingDown);
        }

        let launch_timeout = plan.timeout;
        let final_metadata = match self
            .store
            .update_if_state(&sandbox_id, &[SandboxState::Creating], move |metadata| {
                metadata.resources = runtime_resources;
                metadata.execution_id = execution_id;
                metadata.state = SandboxState::Running;
                metadata.update_timeout(launch_timeout);
            })
            .await
        {
            Ok(update) => update.current,
            Err(err) => {
                warn!(error = %format_args!("{err:#}"), "failed to persist final sandbox metadata after launch");
                self.cleanup_failed_launch(&plan, &mut node, FailedLaunchStage::Recorded)
                    .await;
                return Err(OrchestratorError::from(err));
            }
        };

        // A sandbox with no interaction address has nowhere for the proxy to
        // send traffic, so the launch is rolled back rather than recorded.
        if node.host_interaction_ip().is_none() {
            warn!("sandbox started without an interaction IP; rolling back launch");
            self.cleanup_failed_launch(&plan, &mut node, FailedLaunchStage::Recorded)
                .await;
            return Err(OrchestratorError::InternalError(
                "sandbox missing host interaction IP after start".to_string(),
            ));
        }

        info!("sandbox launch completed");
        Ok(final_metadata)
    }

    fn build_launch(&self, plan: &ApiLaunch) -> Result<RemoteSandboxStub> {
        let built = match &plan.source {
            ApiLaunchSource::SnapshotRecord(record) => self.launches.build_from_snapshot_record(
                record,
                plan.launch_config.clone(),
                plan.execution_id,
            ),
            ApiLaunchSource::UnresolvedImage(spec) => self.launches.build_from_image_ref(
                (**spec).clone(),
                plan.launch_config.clone(),
                plan.execution_id,
            ),
        };
        built.map_err(|source| {
            warn!(error = %format_args!("{source:#}"), "failed to build the node's create request");
            OrchestratorError::SandboxOperationFailed {
                sandbox_id: plan.sandbox_id,
                operation: SandboxOperation::Build,
                source,
            }
        })
    }

    /// Takes back whatever a launch had written before it failed. Nothing was
    /// started, so this only has the record and the grant to give back.
    async fn rollback_launch(&self, plan: &ApiLaunch) {
        if let Err(err) = self
            .forget_sandbox(plan.sandbox_id, plan.execution_id)
            .await
        {
            warn!(error = %format_args!("{err:#}"), "failed to remove sandbox metadata during launch rollback");
        }
    }

    async fn cleanup_failed_launch(
        &self,
        plan: &ApiLaunch,
        node: &mut RemoteSandboxStub,
        stage: FailedLaunchStage,
    ) {
        self.revoke_secrets(plan.sandbox_id, plan.execution_id)
            .await;
        if let Err(err) = node.stop().await {
            warn!(error = %format_args!("{err:#}"), "failed to stop sandbox while rolling back launch");
        }
        if stage == FailedLaunchStage::Recorded {
            self.rollback_launch(plan).await;
        } else {
            // No record to take back, but the binding this launch wrote is
            // still its own: a runtime nothing records must not stay routable.
            self.forget_runtime_routing(plan.sandbox_id, plan.execution_id)
                .await;
        }
    }
}

// ---- delete ------------------------------------------------------------

impl<S: MetadataStore + 'static> SandboxControl<S> {
    /// Stops a sandbox on the machine running it and forgets it.
    pub async fn delete_sandbox(self: &Arc<Self>, sandbox_id: SandboxId) -> Result<()> {
        let this = Arc::clone(self);
        self.run_cancellation_safe("delete", sandbox_id, async move {
            this.delete_sandbox_inner(sandbox_id).await
        })
        .await
    }

    #[tracing::instrument(name = "delete_sandbox", skip(self), fields(sandbox_id = %sandbox_id))]
    async fn delete_sandbox_inner(self: &Arc<Self>, sandbox_id: SandboxId) -> Result<()> {
        info!("deleting sandbox");

        // Enter `Killing`, waiting out whichever transitional state the record
        // is in rather than racing the operation that holds it.
        let previous_state = loop {
            match self
                .store
                .update_state_if_state(&sandbox_id, SandboxState::Killing, &[SandboxState::Running])
                .await
            {
                Ok(previous_state) => break previous_state,
                Err(StoreError::StateConflict { actual_state, .. }) => match actual_state {
                    SandboxState::Killing => {
                        debug!("sandbox already in killing state, waiting for delete to finish");
                        match self
                            .wait_for_transition(sandbox_id, SandboxState::Killing)
                            .await
                        {
                            Ok(_) => continue,
                            Err(OrchestratorError::SandboxNotFound(_)) => {
                                info!("sandbox was deleted by a concurrent delete");
                                return Ok(());
                            }
                            Err(e) => return Err(e),
                        }
                    }
                    SandboxState::Creating
                    | SandboxState::Snapshotting
                    | SandboxState::Forking
                    | SandboxState::Pausing => {
                        debug!(
                            state = ?actual_state,
                            "sandbox in transitional state, waiting before deletion"
                        );
                        match self.wait_for_transition(sandbox_id, actual_state).await {
                            Ok(_) => continue,
                            Err(OrchestratorError::SandboxNotFound(_)) => {
                                info!("sandbox was deleted while waiting for transitional state");
                                return Ok(());
                            }
                            Err(e) => return Err(e),
                        }
                    }
                    // A rollback put the sandbox back to running between the
                    // read and this CAS; take it from the top.
                    SandboxState::Running => continue,
                },
                Err(err) => return Err(OrchestratorError::from(err)),
            }
        };

        self.delete_entered_sandbox(sandbox_id, previous_state)
            .await
    }

    /// Deletes a sandbox whose record this caller already moved into `Killing`.
    ///
    /// `previous_state` is where a rollback returns the record.
    #[tracing::instrument(
        name = "delete_entered_sandbox",
        skip(self),
        fields(sandbox_id = %sandbox_id)
    )]
    async fn delete_entered_sandbox(
        self: &Arc<Self>,
        sandbox_id: SandboxId,
        previous_state: SandboxState,
    ) -> Result<()> {
        let metadata = match self.store.get(&sandbox_id).await {
            Ok(Some(metadata)) => metadata,
            Ok(None) => {
                // This caller just won the Killing transition; the record
                // vanishing immediately after reads as "already deleted".
                warn!("sandbox record disappeared while deleting");
                return Ok(());
            }
            Err(err) => {
                warn!(error = ?err, "could not read sandbox record while deleting; leaving its record alone");
                self.store
                    .update_state_if_state(&sandbox_id, previous_state, &[SandboxState::Killing])
                    .await?;
                return Err(OrchestratorError::from(err));
            }
        };
        let expected_execution_id = metadata.execution_id;

        if self.runtime_confirmed_gone(sandbox_id).await {
            self.forget_unrouted_runtime(sandbox_id, expected_execution_id)
                .await;
            return Ok(());
        }

        let session = match self
            .node_session(sandbox_id, expected_execution_id, metadata.resources)
            .await
        {
            Ok(session) => session,
            // Uncertainty about the runtime must not authorize deleting the record.
            Err(error) => {
                warn!(error = %error, "could not reach the sandbox while deleting; leaving its record alone");
                self.store
                    .update_state_if_state(&sandbox_id, previous_state, &[SandboxState::Killing])
                    .await?;
                return Err(error);
            }
        };

        if let NodeSession::Live(mut node) = session {
            if let Err(err) = node.stop().await {
                warn!(error = ?err, "failed to stop sandbox during delete");
                self.store
                    .update_state_if_state(&sandbox_id, previous_state, &[SandboxState::Killing])
                    .await?;
                return Err(OrchestratorError::SandboxOperationFailed {
                    sandbox_id,
                    operation: SandboxOperation::Stop,
                    source: err,
                });
            }
        }

        let removed = self
            .forget_sandbox(sandbox_id, expected_execution_id)
            .await?;
        if let Some(metadata) = removed {
            self.publish_sandbox_event(
                SandboxLifecycleEventType::Delete,
                metadata.id,
                metadata.execution_id,
                metadata.resources,
            );
        }
        info!("sandbox deleted");

        Ok(())
    }
}

// ---- pause -------------------------------------------------------------

impl<S: MetadataStore + 'static> SandboxControl<S> {
    /// Pauses a running sandbox: the node captures it, this half commits the
    /// capture, and the record goes with it. Afterwards the sandbox exists only
    /// as the snapshot row it published.
    pub async fn pause_sandbox(self: &Arc<Self>, sandbox_id: SandboxId) -> Result<PauseOutcome> {
        let this = Arc::clone(self);
        self.run_cancellation_safe("pause", sandbox_id, async move {
            this.pause_sandbox_inner(sandbox_id).await
        })
        .await
    }

    #[tracing::instrument(name = "pause_sandbox", skip(self), fields(sandbox_id = %sandbox_id))]
    async fn pause_sandbox_inner(self: &Arc<Self>, sandbox_id: SandboxId) -> Result<PauseOutcome> {
        info!("pausing sandbox");
        match self
            .store
            .update_state_if_state(&sandbox_id, SandboxState::Pausing, &[SandboxState::Running])
            .await
        {
            Ok(_) => {}
            Err(StoreError::StateConflict { actual_state, .. }) => {
                return match actual_state {
                    SandboxState::Pausing => self.join_concurrent_pause(sandbox_id).await,
                    SandboxState::Killing => {
                        info!("sandbox is being deleted while pausing");
                        Err(OrchestratorError::SandboxNotFound(sandbox_id))
                    }
                    _ => {
                        info!(state = ?actual_state, "cannot pause sandbox in current state");
                        Err(OrchestratorError::InvalidSandboxState {
                            sandbox_id,
                            state: actual_state,
                        })
                    }
                };
            }
            Err(err) => return Err(OrchestratorError::from(err)),
        }

        self.pause_entered_sandbox(sandbox_id).await
    }

    /// Pauses a sandbox whose record this caller already moved into `Pausing`.
    #[tracing::instrument(
        name = "pause_entered_sandbox",
        skip(self),
        fields(sandbox_id = %sandbox_id)
    )]
    async fn pause_entered_sandbox(
        self: &Arc<Self>,
        sandbox_id: SandboxId,
    ) -> Result<PauseOutcome> {
        let metadata = match self.store.get(&sandbox_id).await {
            Ok(Some(metadata)) => metadata,
            Ok(None) => {
                warn!("sandbox record disappeared while pausing");
                return Err(OrchestratorError::SandboxNotFound(sandbox_id));
            }
            Err(err) => {
                warn!(error = ?err, "failed to read sandbox record while pausing");
                self.rollback_pause_to_running(sandbox_id).await;
                return Err(OrchestratorError::from(err));
            }
        };
        let expected_execution_id = metadata.execution_id;

        if self.runtime_confirmed_gone(sandbox_id).await {
            self.forget_unrouted_runtime(sandbox_id, expected_execution_id)
                .await;
            return Err(OrchestratorError::SandboxNotFound(sandbox_id));
        }

        let mut node = match self
            .node_session(sandbox_id, expected_execution_id, metadata.resources)
            .await
        {
            Ok(NodeSession::Live(node)) => node,
            // Confirmed runtime absence permits record cleanup.
            Ok(NodeSession::Gone) => {
                warn!("nothing is running this sandbox any more; removing its record");
                self.forget_sandbox(sandbox_id, expected_execution_id)
                    .await?;
                return Err(OrchestratorError::SandboxNotFound(sandbox_id));
            }
            // Uncertainty rolls the record back without deletion.
            Err(error) => {
                warn!(error = %error, "could not reach the sandbox while pausing; leaving its record alone");
                self.rollback_pause_to_running(sandbox_id).await;
                return Err(error);
            }
        };

        // The node captures with the VM paused in place and forgets it; the
        // capture stays uncommitted until it is durable here.
        let capture = match node.pause().await {
            Ok(capture) => capture,
            Err(err) => {
                warn!(error = ?err, "failed to pause sandbox");
                if err.is_terminal() {
                    if let Err(stop_err) = node.stop().await {
                        warn!(error = ?stop_err, "failed to stop sandbox after terminal pause failure");
                    }
                    self.forget_sandbox(sandbox_id, expected_execution_id)
                        .await?;
                } else if self.runtime_confirmed_gone(sandbox_id).await {
                    // A node that says it never had this sandbox and a cluster
                    // that routes nowhere for it agree: there is nothing to put
                    // back and nothing to retry next tick.
                    self.forget_unrouted_runtime(sandbox_id, expected_execution_id)
                        .await;
                } else {
                    self.rollback_pause_to_running(sandbox_id).await;
                }
                return Err(OrchestratorError::SandboxOperationFailed {
                    sandbox_id,
                    operation: SandboxOperation::Pause,
                    source: err.into(),
                });
            }
        };

        // A capture the node staged can be committed again; publishing consumes
        // a local one, and this half never makes those.
        let restageable = match &capture {
            CapturedSandboxSnapshot::Staged(staged) => Some((**staged).clone()),
            CapturedSandboxSnapshot::Local(_) => None,
        };

        let published = match self.pause_publisher.publish(&metadata, capture).await {
            Ok(published) => published,
            Err(source) => {
                warn!(
                    error = %format_args!("{source:#}"),
                    "the pause capture could not be published; putting the sandbox back"
                );
                // A pause on this half stops the VM on the node, so the ask
                // yields the reason it cannot come back and never the sandbox:
                // `RemoteSandboxStub::resume` refuses by construction.
                let resume_err = node.resume().await.expect_err(
                    "a node that paused a sandbox has already stopped it, so resuming it in \
                     place cannot succeed",
                );
                // The VM cannot come back, so the staged capture is the only
                // copy of this sandbox. Commit it again before giving up on it.
                warn!(error = ?resume_err, "failed to resume sandbox after a failed publication");
                let recovered = match restageable.as_ref() {
                    Some(staged) => self.retry_pause_publication(&metadata, staged).await,
                    None => Err(source),
                };
                match recovered {
                    Ok(published) => {
                        info!(
                            published = ?published,
                            "a retried commit published the pause capture of a sandbox \
                             that could not be resumed"
                        );
                        published
                    }
                    Err(retry_error) => {
                        let snapshot_id = restageable
                            .as_ref()
                            .map(|staged| staged.commit.id.to_string())
                            .unwrap_or_default();
                        let staged_commit = restageable
                            .as_ref()
                            .and_then(|staged| serde_json::to_string(staged).ok())
                            .unwrap_or_default();
                        // The bytes stay where the node put them; this line is
                        // what an operator re-commits from.
                        error!(
                            %sandbox_id,
                            %snapshot_id,
                            %staged_commit,
                            error = %format_args!("{retry_error:#}"),
                            "the pause capture of this sandbox could not be committed \
                             and its sandbox cannot be resumed; the capture bytes were \
                             kept and only a commit of the staged snapshot recovers it"
                        );
                        if let Err(stop_err) = node.stop().await {
                            warn!(error = ?stop_err, "failed to stop sandbox after a failed publication");
                        }
                        if let Err(error) =
                            self.forget_sandbox(sandbox_id, metadata.execution_id).await
                        {
                            warn!(error = ?error, "failed to remove sandbox after pause failure");
                        }
                        return Err(OrchestratorError::SandboxOperationFailed {
                            sandbox_id,
                            operation: SandboxOperation::Pause,
                            source: aenv_core::sandbox::SandboxCaptureError::terminal(
                                retry_error.context(format!(
                                    "the capture could not be committed and the sandbox \
                                     could not be resumed ({resume_err:#}); its capture \
                                     bytes were retained under snapshot {snapshot_id}"
                                )),
                            )
                            .into(),
                        });
                    }
                }
            }
        };

        // The capture is durable: stop the VM and let the record go with it.
        if let Err(err) = node.stop().await {
            warn!(error = ?err, "failed to stop sandbox after pausing");
        }
        if let Err(err) = self.forget_sandbox(sandbox_id, metadata.execution_id).await {
            warn!(error = ?err, "failed to remove the record of a paused sandbox");
        }
        self.publish_sandbox_event(
            SandboxLifecycleEventType::Pause,
            sandbox_id,
            metadata.execution_id,
            metadata.resources,
        );
        info!(published = ?published, "sandbox paused");

        Ok(PauseOutcome {
            metadata,
            published,
        })
    }

    /// Commits an already-staged pause capture again, with bounded backoff.
    ///
    /// Only reached when the VM is gone: the alternative to a retry is losing
    /// the sandbox, so this waits rather than failing fast.
    async fn retry_pause_publication(
        &self,
        metadata: &SandboxMetadata,
        staged: &aenv_core::snapshot::repository::StagedSnapshot,
    ) -> anyhow::Result<PublishedPause> {
        let deadline = tokio::time::Instant::now() + PAUSE_PUBLICATION_RETRY_DEADLINE;
        let mut delay = PAUSE_PUBLICATION_RETRY_BACKOFF;
        let mut last = anyhow::anyhow!("no commit was attempted");
        for attempt in 2..=PAUSE_PUBLICATION_ATTEMPTS {
            let wakes_at = tokio::time::Instant::now() + delay;
            if wakes_at > deadline {
                warn!(
                    sandbox_id = %metadata.id,
                    attempt,
                    "the commit of this pause capture ran out of its retry budget"
                );
                break;
            }
            tokio::time::sleep_until(wakes_at).await;
            delay = delay.saturating_mul(2);
            let capture = CapturedSandboxSnapshot::staged(staged.clone());
            match self.pause_publisher.publish(metadata, capture).await {
                Ok(published) => return Ok(published),
                Err(error) => {
                    warn!(
                        sandbox_id = %metadata.id,
                        attempt,
                        error = %format_args!("{error:#}"),
                        "retrying the commit of a pause capture whose sandbox cannot be resumed"
                    );
                    last = error;
                }
            }
        }
        Err(last)
    }

    async fn rollback_pause_to_running(&self, sandbox_id: SandboxId) {
        let _ = self
            .store
            .update_state_if_state(&sandbox_id, SandboxState::Running, &[SandboxState::Pausing])
            .await;
    }

    /// Joins a pause another caller is performing on the same sandbox.
    ///
    /// The record disappearing is not on its own the pause finishing: the owner
    /// removes the record on several failure paths too, so a joiner reports
    /// success only against a snapshot row a pause of this sandbox left behind.
    async fn join_concurrent_pause(&self, sandbox_id: SandboxId) -> Result<PauseOutcome> {
        debug!("concurrent pause in progress, waiting for completion");
        let joined_at = SystemTime::now();
        let before = self
            .store
            .get(&sandbox_id)
            .await?
            .ok_or(OrchestratorError::SandboxNotFound(sandbox_id))?;
        let wait = self
            .store
            .wait_while_in_states(&sandbox_id, &[SandboxState::Pausing]);
        match tokio::time::timeout(CONCURRENT_PAUSE_WAIT, wait).await {
            Ok(Ok(None)) => {
                self.joined_pause_publication(sandbox_id, before, joined_at)
                    .await
            }
            Ok(Ok(Some(after))) => match after.state {
                SandboxState::Running => {
                    info!("concurrent pause failed; sandbox returned to running state");
                    Err(OrchestratorError::InvalidSandboxState {
                        sandbox_id,
                        state: SandboxState::Running,
                    })
                }
                SandboxState::Killing => {
                    info!("sandbox is being deleted after concurrent pause attempt");
                    Err(OrchestratorError::SandboxNotFound(sandbox_id))
                }
                other => {
                    info!(state = ?other, "unexpected state after waiting for concurrent pause");
                    Err(OrchestratorError::InvalidSandboxState {
                        sandbox_id,
                        state: other,
                    })
                }
            },
            Ok(Err(err)) => Err(OrchestratorError::from(err)),
            Err(_elapsed) => {
                warn!("timed out waiting for a concurrent pause to finish");
                Err(OrchestratorError::InvalidSandboxState {
                    sandbox_id,
                    state: SandboxState::Pausing,
                })
            }
        }
    }

    /// Finds the snapshot the pause this caller joined published.
    ///
    /// The window starts one [`CONCURRENT_PAUSE_WAIT`] before the joiner began
    /// waiting, which is as far back as a pause still in flight can have
    /// started.
    async fn joined_pause_publication(
        &self,
        sandbox_id: SandboxId,
        before: SandboxMetadata,
        joined_at: SystemTime,
    ) -> Result<PauseOutcome> {
        let not_before_unix_ms = joined_at
            .checked_sub(CONCURRENT_PAUSE_WAIT)
            .and_then(|start| start.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|since| i64::try_from(since.as_millis()).unwrap_or(i64::MAX))
            .unwrap_or(0);
        match self
            .pause_publisher
            .published_since(sandbox_id, not_before_unix_ms)
            .await
        {
            Ok(Some(snapshot_id)) => {
                debug!(%snapshot_id, "concurrent pause succeeded");
                Ok(PauseOutcome {
                    metadata: before,
                    published: PublishedPause::Committed(snapshot_id),
                })
            }
            Ok(None) => {
                warn!(
                    "the record of a sandbox this caller was waiting on disappeared without a \
                     snapshot to show for it"
                );
                Err(OrchestratorError::PausePublicationFailed {
                    sandbox_id,
                    source: anyhow::anyhow!(
                        "the pause this call joined removed the sandbox's record without \
                         publishing a snapshot for it"
                    ),
                })
            }
            Err(error) => {
                warn!(
                    error = %format_args!("{error:#}"),
                    "could not tell whether the pause this caller joined published anything"
                );
                Err(OrchestratorError::PausePublicationFailed {
                    sandbox_id,
                    source: error.context(
                        "the pause this call joined cannot be confirmed to have published \
                         a snapshot",
                    ),
                })
            }
        }
    }
}

// ---- snapshot, fork and in-place updates -------------------------------

impl<S: MetadataStore + 'static> SandboxControl<S> {
    /// Captures a snapshot of a running sandbox and leaves it running.
    pub async fn capture_snapshot(
        self: &Arc<Self>,
        sandbox_id: SandboxId,
    ) -> Result<SnapshotCaptureResult> {
        let this = Arc::clone(self);
        self.run_cancellation_safe("snapshot", sandbox_id, async move {
            this.capture_snapshot_inner(sandbox_id).await
        })
        .await
    }

    #[tracing::instrument(name = "capture_snapshot", skip(self), fields(sandbox_id = %sandbox_id))]
    async fn capture_snapshot_inner(
        self: Arc<Self>,
        sandbox_id: SandboxId,
    ) -> Result<SnapshotCaptureResult> {
        self.ensure_accepting_lifecycle_operations()?;

        info!("capturing sandbox snapshot");
        match self
            .store
            .update_state_if_state(
                &sandbox_id,
                SandboxState::Snapshotting,
                &[SandboxState::Running],
            )
            .await
        {
            Ok(_) => {}
            Err(StoreError::StateConflict { actual_state, .. }) => {
                return match actual_state {
                    SandboxState::Killing => Err(OrchestratorError::SandboxNotFound(sandbox_id)),
                    _ => Err(OrchestratorError::InvalidSandboxState {
                        sandbox_id,
                        state: actual_state,
                    }),
                };
            }
            Err(err) => return Err(OrchestratorError::from(err)),
        }

        let metadata = match self.store.get(&sandbox_id).await {
            Ok(Some(metadata)) => metadata,
            Ok(None) => {
                warn!("sandbox record disappeared while snapshotting");
                return Err(OrchestratorError::SandboxNotFound(sandbox_id));
            }
            Err(err) => {
                warn!(error = ?err, "could not read sandbox record while snapshotting; leaving its record alone");
                self.rollback_to_running(sandbox_id, SandboxState::Snapshotting)
                    .await;
                return Err(OrchestratorError::from(err));
            }
        };
        let expected_execution_id = metadata.execution_id;

        let mut node = match self
            .node_session(sandbox_id, expected_execution_id, metadata.resources)
            .await
        {
            Ok(NodeSession::Live(node)) => node,
            // Only confirmed runtime absence authorizes record cleanup.
            Ok(NodeSession::Gone) => {
                warn!("nothing is running this sandbox any more; removing its record");
                self.forget_sandbox(sandbox_id, expected_execution_id)
                    .await?;
                return Err(OrchestratorError::SandboxNotFound(sandbox_id));
            }
            Err(error) => {
                warn!(error = %error, "could not reach the sandbox while snapshotting; leaving its record alone");
                self.rollback_to_running(sandbox_id, SandboxState::Snapshotting)
                    .await;
                return Err(error);
            }
        };

        let captured_snapshot = match node.snapshot().await {
            Ok(captured_snapshot) => captured_snapshot,
            Err(err) => {
                warn!(error = ?err, "failed to capture sandbox snapshot");
                if err.is_unknown() {
                    // Nobody said what the node did, so ask it. A running
                    // sandbox is put back untouched; only a node that says the
                    // sandbox is gone authorizes forgetting the record.
                    self.settle_unknown_capture(
                        sandbox_id,
                        expected_execution_id,
                        &mut node,
                        SandboxState::Snapshotting,
                    )
                    .await;
                } else if err.is_terminal() {
                    if let Err(stop_err) = node.stop().await {
                        warn!(error = ?stop_err, "failed to stop sandbox after terminal snapshot failure");
                    }
                    self.forget_sandbox(sandbox_id, expected_execution_id)
                        .await?;
                } else {
                    self.rollback_to_running(sandbox_id, SandboxState::Snapshotting)
                        .await;
                }
                return Err(OrchestratorError::SandboxOperationFailed {
                    sandbox_id,
                    operation: SandboxOperation::Snapshot,
                    source: err.into(),
                });
            }
        };

        self.store
            .update_state_if_state(
                &sandbox_id,
                SandboxState::Running,
                &[SandboxState::Snapshotting],
            )
            .await?;
        let metadata = match self.store.get(&sandbox_id).await? {
            Some(metadata) => metadata,
            None => {
                warn!("sandbox disappeared after snapshotting");
                return Err(OrchestratorError::SandboxNotFound(sandbox_id));
            }
        };

        info!("snapshot captured");
        Ok(SnapshotCaptureResult {
            metadata,
            captured_snapshot,
        })
    }

    /// Settles a capture whose outcome the node did not classify.
    ///
    /// The runtime is asked directly: still running puts the record back,
    /// confirmed absent forgets it, and an unanswerable probe puts the record
    /// back so a later operation asks again. Nothing is stopped on an unknown
    /// outcome.
    async fn settle_unknown_capture(
        &self,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
        node: &mut RemoteSandboxStub,
        transitional_state: SandboxState,
    ) {
        match node.is_still_running().await {
            Ok(false) => {
                warn!(
                    %sandbox_id,
                    "the node holding this sandbox says it is no longer running it; \
                     dropping its record"
                );
                if let Err(error) = self.forget_sandbox(sandbox_id, execution_id).await {
                    warn!(%sandbox_id, error = ?error, "failed to remove the record of a sandbox its node no longer has");
                }
            }
            Ok(true) => {
                info!(
                    %sandbox_id,
                    "an unclassified capture failure left the sandbox running on its node; \
                     putting its record back"
                );
                self.rollback_to_running(sandbox_id, transitional_state)
                    .await;
            }
            Err(error) => {
                warn!(
                    %sandbox_id,
                    error = %format_args!("{error:#}"),
                    "could not ask the node what became of this sandbox; putting its record \
                     back for a later operation to settle"
                );
                self.rollback_to_running(sandbox_id, transitional_state)
                    .await;
            }
        }
    }

    async fn rollback_to_running(&self, sandbox_id: SandboxId, from: SandboxState) {
        let _ = self
            .store
            .update_state_if_state(&sandbox_id, SandboxState::Running, &[from])
            .await;
    }

    /// Forks a running sandbox into one child per request, in request order.
    pub async fn fork_sandbox(
        self: &Arc<Self>,
        source_sandbox_id: SandboxId,
        children: ForkChildren,
        new_timeout: NewTimeout,
    ) -> Result<Vec<SandboxForkOutcome>> {
        let this = Arc::clone(self);
        self.run_cancellation_safe("fork", source_sandbox_id, async move {
            this.fork_sandbox_inner(source_sandbox_id, children, new_timeout)
                .await
        })
        .await
    }

    #[tracing::instrument(
        name = "fork_sandbox",
        skip(self),
        fields(source_sandbox_id = %source_sandbox_id)
    )]
    async fn fork_sandbox_inner(
        self: Arc<Self>,
        source_sandbox_id: SandboxId,
        children: ForkChildren,
        new_timeout: NewTimeout,
    ) -> Result<Vec<SandboxForkOutcome>> {
        self.ensure_accepting_new_work()?;

        let count = children.count();
        info!("forking sandboxes");

        let source_metadata = self
            .store
            .update_if_state(&source_sandbox_id, &[SandboxState::Running], |metadata| {
                metadata.state = SandboxState::Forking
            })
            .await
            .map_err(|err| match err {
                StoreError::StateConflict { actual_state, .. } => match actual_state {
                    SandboxState::Killing => OrchestratorError::SandboxNotFound(source_sandbox_id),
                    _ => OrchestratorError::InvalidSandboxState {
                        sandbox_id: source_sandbox_id,
                        state: actual_state,
                    },
                },
                err => OrchestratorError::from(err),
            })?
            .previous;

        let mut source = match self
            .node_session(
                source_sandbox_id,
                source_metadata.execution_id,
                source_metadata.resources,
            )
            .await
        {
            Ok(NodeSession::Live(node)) => node,
            Ok(NodeSession::Gone) => {
                self.rollback_to_running(source_sandbox_id, SandboxState::Forking)
                    .await;
                return Err(OrchestratorError::SandboxNotFound(source_sandbox_id));
            }
            Err(error) => {
                warn!(error = %error, "could not reach the sandbox while forking; leaving its record alone");
                self.rollback_to_running(source_sandbox_id, SandboxState::Forking)
                    .await;
                return Err(error);
            }
        };
        source.set_projection_budget(source_metadata.projection_ttl_secs(SystemTime::now()));

        // Caller-assigned child identity and ownership travel together.
        let children: Vec<ForkChildAssignment> = match children {
            ForkChildren::Fresh(count) => (0..count)
                .map(|_| ForkChildAssignment {
                    sandbox_id: SandboxId::new(),
                    execution_id: None,
                    control_plane_config: None,
                })
                .collect(),
            ForkChildren::Assigned(children) => children,
        };
        let children_spec = children
            .iter()
            .map(|child| SandboxForkSpec {
                sandbox_id: child.sandbox_id,
                execution_id: child.execution_id.unwrap_or_else(ExecutionId::new),
                envd_access_token: source_metadata
                    .secure
                    .then(|| self.access_tokens.generate(child.sandbox_id)),
            })
            .collect::<Vec<_>>();

        // Children inherit the parent's policy, so each child's incarnation
        // needs its own grant before it can open a brokered connection.
        for spec in &children_spec {
            if let Err(source_err) = self
                .grant_secrets(
                    spec.sandbox_id,
                    spec.execution_id,
                    &source_metadata.network_policy,
                )
                .await
            {
                for granted in &children_spec {
                    self.revoke_secrets(granted.sandbox_id, granted.execution_id)
                        .await;
                }
                self.counters.record_create_fail(u64::from(count));
                self.rollback_to_running(source_sandbox_id, SandboxState::Forking)
                    .await;
                return Err(OrchestratorError::SandboxOperationFailed {
                    sandbox_id: source_sandbox_id,
                    operation: SandboxOperation::Fork,
                    source: source_err,
                });
            }
        }

        let forked = match source.fork(&children_spec).await {
            Ok(forked) => forked,
            Err(err) => {
                warn!(error = ?err, "failed to fork sandbox");
                self.counters.record_create_fail(u64::from(count));
                // No child started, so every child grant issued above is dead.
                for granted in &children_spec {
                    self.revoke_secrets(granted.sandbox_id, granted.execution_id)
                        .await;
                }
                if err.is_terminal() {
                    if let Err(stop_err) = source.stop().await {
                        warn!(error = ?stop_err, "failed to stop sandbox after terminal fork failure");
                    }
                    self.forget_sandbox(source_sandbox_id, source_metadata.execution_id)
                        .await?;
                } else {
                    self.rollback_to_running(source_sandbox_id, SandboxState::Forking)
                        .await;
                }
                return Err(OrchestratorError::SandboxOperationFailed {
                    sandbox_id: source_sandbox_id,
                    operation: SandboxOperation::Fork,
                    source: err.into(),
                });
            }
        };

        if let Err(err) = self
            .store
            .update_state_if_state(
                &source_sandbox_id,
                SandboxState::Running,
                &[SandboxState::Forking],
            )
            .await
        {
            warn!(error = ?err, "failed to restore source sandbox metadata after fork");
        }

        let mut outcomes = Vec::with_capacity(children_spec.len());
        let mut successes = 0u64;
        let now = SystemTime::now();
        for ((child, spec), started) in children.into_iter().zip(children_spec).zip(forked) {
            let sandbox_id = child.sandbox_id;
            let mut started = match started {
                Ok(started) => started,
                Err(err) => {
                    warn!(%sandbox_id, error = ?err, "failed to start forked sandbox");
                    self.revoke_secrets(sandbox_id, spec.execution_id).await;
                    outcomes.push(Err(fork_child_error(sandbox_id, err)));
                    continue;
                }
            };

            let mut metadata = source_metadata.clone();
            metadata.id = sandbox_id;
            metadata.execution_id = spec.execution_id;
            metadata.control_plane_config = child.control_plane_config;
            metadata.state = SandboxState::Running;
            metadata.created_at = now;
            // Fork children start a fresh lifetime budget.
            metadata.restart_lifetime_clock(now);
            metadata.update_timeout(new_timeout);

            if started.host_interaction_ip().is_none() {
                warn!(%sandbox_id, "fork child started without an interaction IP");
                stop_failed_fork(&mut started, sandbox_id).await;
                self.forget_runtime_routing(sandbox_id, spec.execution_id)
                    .await;
                self.revoke_secrets(sandbox_id, spec.execution_id).await;
                outcomes.push(Err(fork_child_error(
                    sandbox_id,
                    anyhow::anyhow!("sandbox missing host interaction IP after start"),
                )));
                continue;
            }
            if let Err(err) = self.store.add(metadata.clone()).await {
                warn!(%sandbox_id, error = ?err, "failed to register forked sandbox");
                stop_failed_fork(&mut started, sandbox_id).await;
                self.forget_runtime_routing(sandbox_id, spec.execution_id)
                    .await;
                self.revoke_secrets(sandbox_id, spec.execution_id).await;
                outcomes.push(Err(fork_child_error(sandbox_id, anyhow::Error::new(err))));
                continue;
            }
            self.publish_sandbox_event(
                SandboxLifecycleEventType::Fork,
                metadata.id,
                metadata.execution_id,
                metadata.resources,
            );
            successes += 1;
            outcomes.push(Ok(metadata));
        }

        self.counters.record_create_success(successes);
        self.counters
            .record_create_fail(u64::from(count) - successes);
        Ok(outcomes)
    }

    /// Replaces a running sandbox's egress policy on the node and in its record.
    pub async fn replace_sandbox_network_policy(
        self: &Arc<Self>,
        sandbox_id: SandboxId,
        network_policy: SandboxNetworkPolicy,
    ) -> Result<()> {
        let this = Arc::clone(self);
        self.run_cancellation_safe("update_network", sandbox_id, async move {
            this.replace_sandbox_network_policy_inner(sandbox_id, network_policy)
                .await
        })
        .await
    }

    #[tracing::instrument(
        name = "replace_sandbox_network_policy",
        skip(self, network_policy),
        fields(sandbox_id = %sandbox_id)
    )]
    async fn replace_sandbox_network_policy_inner(
        &self,
        sandbox_id: SandboxId,
        network_policy: SandboxNetworkPolicy,
    ) -> Result<()> {
        let metadata = self.running_record(sandbox_id).await?;
        let mut node = self
            .running_node(sandbox_id, &metadata, SandboxOperation::UpdateNetwork)
            .await?;

        // The grant is what the broker checks, so it has to name exactly the
        // new policy's secrets before the new rules can serve a request.
        let previous_names = metadata.network_policy.egress.referenced_secret_names();
        let next_names = network_policy.egress.referenced_secret_names();
        let grant_changed = next_names != previous_names;
        if grant_changed {
            self.regrant_secrets(sandbox_id, metadata.execution_id, &next_names)
                .await
                .map_err(|source| OrchestratorError::SandboxOperationFailed {
                    sandbox_id,
                    operation: SandboxOperation::UpdateNetwork,
                    source,
                })?;
        }

        let runtime_policy = network_policy.runtime_policy();
        if let Err(source) = node.update_network_policy(runtime_policy).await {
            if grant_changed {
                // The runtime kept the previous rules, so its grant goes back too.
                if let Err(err) = self
                    .regrant_secrets(sandbox_id, metadata.execution_id, &previous_names)
                    .await
                {
                    warn!(
                        error = %format_args!("{err:#}"),
                        "failed to put the previous secret grant back after a refused network update"
                    );
                }
            }
            return Err(OrchestratorError::SandboxOperationFailed {
                sandbox_id,
                operation: SandboxOperation::UpdateNetwork,
                source,
            });
        }

        self.store
            .update_if_state(&sandbox_id, &[SandboxState::Running], |metadata| {
                metadata.network_policy = network_policy;
            })
            .await?;

        Ok(())
    }

    /// Applies an extension-defined patch to a running sandbox's parameters.
    ///
    /// The patch is passed verbatim to the extension's patch-params hook, which
    /// returns the updated full parameters. On hook failure the sandbox keeps
    /// what it had and the record is untouched.
    pub async fn patch_sandbox_custom_extension_params(
        self: &Arc<Self>,
        sandbox_id: SandboxId,
        patch: serde_json::Map<String, serde_json::Value>,
    ) -> Result<Option<CustomExtensionParams>> {
        let this = Arc::clone(self);
        self.run_cancellation_safe("patch_custom_extension_params", sandbox_id, async move {
            this.patch_sandbox_custom_extension_params_inner(sandbox_id, patch)
                .await
        })
        .await
    }

    #[tracing::instrument(
        name = "patch_sandbox_custom_extension_params",
        skip(self, patch),
        fields(sandbox_id = %sandbox_id)
    )]
    async fn patch_sandbox_custom_extension_params_inner(
        &self,
        sandbox_id: SandboxId,
        patch: serde_json::Map<String, serde_json::Value>,
    ) -> Result<Option<CustomExtensionParams>> {
        let metadata = self.running_record(sandbox_id).await?;
        let mut node = self
            .running_node(
                sandbox_id,
                &metadata,
                SandboxOperation::PatchCustomExtensionParams,
            )
            .await?;

        let client = CustomExtensionClient::global().ok_or_else(|| {
            OrchestratorError::SandboxOperationFailed {
                sandbox_id,
                operation: SandboxOperation::PatchCustomExtensionParams,
                source: anyhow::anyhow!(
                    "custom extension is not configured ([custom_extension].url is unset)"
                ),
            }
        })?;
        let new_params = client
            .hook_patch_params(sandbox_id, patch)
            .await
            .map_err(|source| OrchestratorError::SandboxOperationFailed {
                sandbox_id,
                operation: SandboxOperation::PatchCustomExtensionParams,
                source,
            })?;

        self.apply_custom_extension_params(sandbox_id, &mut node, new_params.clone())
            .await?;

        Ok(new_params)
    }

    /// Applies extension parameters on the node before persisting them.
    async fn apply_custom_extension_params(
        &self,
        sandbox_id: SandboxId,
        node: &mut RemoteSandboxStub,
        params: Option<CustomExtensionParams>,
    ) -> Result<()> {
        node.update_custom_extension_params(params.clone())
            .await
            .map_err(|source| OrchestratorError::SandboxOperationFailed {
                sandbox_id,
                operation: SandboxOperation::PatchCustomExtensionParams,
                source,
            })?;

        // A concurrent pause may have moved the sandbox since the entry check,
        // so this write may lose the race; that is a conflict, not a failure.
        self.store
            .update_if_state(&sandbox_id, &[SandboxState::Running], |metadata| {
                metadata.custom_extension_params = params.clone();
            })
            .await
            .map_err(|err| match err {
                StoreError::StateConflict {
                    sandbox_id,
                    actual_state,
                    ..
                } => OrchestratorError::InvalidSandboxState {
                    sandbox_id,
                    state: actual_state,
                },
                other => OrchestratorError::from(other),
            })?;

        Ok(())
    }

    /// The record of a sandbox an in-place update may act on.
    async fn running_record(&self, sandbox_id: SandboxId) -> Result<SandboxMetadata> {
        let metadata = self
            .store
            .get(&sandbox_id)
            .await?
            .ok_or(OrchestratorError::SandboxNotFound(sandbox_id))?;
        if metadata.state != SandboxState::Running {
            return Err(OrchestratorError::InvalidSandboxState {
                sandbox_id,
                state: metadata.state,
            });
        }
        Ok(metadata)
    }

    /// The node running a sandbox an in-place update may act on.
    async fn running_node(
        &self,
        sandbox_id: SandboxId,
        metadata: &SandboxMetadata,
        operation: SandboxOperation,
    ) -> Result<Box<RemoteSandboxStub>> {
        match self
            .node_session(sandbox_id, metadata.execution_id, metadata.resources)
            .await?
        {
            NodeSession::Live(node) => Ok(node),
            NodeSession::Gone => Err(OrchestratorError::SandboxOperationConflict {
                sandbox_id,
                operation,
            }),
        }
    }
}

fn fork_child_error(sandbox_id: SandboxId, source: anyhow::Error) -> OrchestratorError {
    OrchestratorError::SandboxOperationFailed {
        sandbox_id,
        operation: SandboxOperation::Fork,
        source,
    }
}

async fn stop_failed_fork(child: &mut RemoteSandboxStub, sandbox_id: SandboxId) {
    if let Err(err) = child.stop().await {
        warn!(%sandbox_id, error = ?err, "failed to stop unsuccessful fork");
    }
}

// ---- reads, expiry and shutdown ----------------------------------------

impl<S: MetadataStore + 'static> SandboxControl<S> {
    pub async fn get_sandbox(&self, sandbox_id: &SandboxId) -> Result<Option<SandboxMetadata>> {
        Ok(self.store.get(sandbox_id).await?)
    }

    pub async fn list_sandboxes(&self) -> Result<Vec<SandboxMetadata>> {
        Ok(self.store.list().await?)
    }

    pub async fn list_sandboxes_filtered(
        &self,
        filter: SandboxListFilter,
    ) -> Result<Vec<SandboxMetadata>> {
        Ok(self.store.list_filtered(filter).await?)
    }

    /// The roster of what this control plane records, with each sandbox's
    /// remaining routing budget.
    pub async fn list_sandbox_roster(&self) -> Result<Vec<SandboxRosterEntry>> {
        let now = SystemTime::now();
        Ok(self
            .store
            .list()
            .await?
            .into_iter()
            .map(|metadata| SandboxRosterEntry {
                sandbox_id: metadata.id,
                execution_id: metadata.execution_id,
                projection_ttl_secs: metadata.projection_ttl_secs(now),
            })
            .collect())
    }

    /// The machine the routing binding names for a sandbox, if any.
    pub async fn sandbox_holding_node_id(&self, sandbox_id: &SandboxId) -> Option<String> {
        match self.placement.place_existing(*sandbox_id).await {
            Ok(node) => node.map(|node| node.node_id),
            Err(error) => {
                warn!(
                    %sandbox_id,
                    error = %format_args!("{error:#}"),
                    "could not tell which machine is running this sandbox"
                );
                None
            }
        }
    }

    pub fn get_envd_access_token(&self, metadata: &SandboxMetadata) -> Option<EnvdAccessToken> {
        metadata
            .secure
            .then(|| self.access_tokens.generate(metadata.id))
    }

    pub fn validate_envd_access_token(&self, sandbox_id: SandboxId, candidate: &str) -> bool {
        self.access_tokens.matches(sandbox_id, candidate)
    }

    pub fn subscribe_sandbox_events(&self) -> broadcast::Receiver<SandboxLifecycleEvent> {
        self.sandbox_event_tx.subscribe()
    }

    /// Runtime counters and derived resource totals, sampled now.
    pub async fn metrics_snapshot(&self) -> Result<OrchestratorMetrics> {
        let mut metrics = OrchestratorMetrics::default();
        self.store
            .list_with_callback(|metadata| {
                aggregate_resource_metrics(
                    &mut metrics,
                    SandboxContribution::new(metadata.state, metadata.resources),
                );
            })
            .await?;
        metrics.create_successes = self.counters.create_successes();
        metrics.create_fails = self.counters.create_fails();
        Ok(metrics)
    }

    pub fn scheduling_disabled(&self) -> bool {
        self.scheduling_disabled.load(Ordering::Acquire)
    }

    /// Changes isolation and reports whether the value changed.
    pub fn set_scheduling_disabled(&self, disabled: bool) -> bool {
        if self.scheduling_disabled.swap(disabled, Ordering::AcqRel) == disabled {
            return false;
        }
        let now_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|since| since.as_millis() as i64)
            .unwrap_or_default();
        self.scheduling_disabled_changed_at_ms
            .store(now_ms, Ordering::Release);
        info!(
            scheduling_disabled = disabled,
            "scheduling availability changed"
        );
        true
    }

    pub fn scheduling_disabled_changed_at_ms(&self) -> Option<i64> {
        match self
            .scheduling_disabled_changed_at_ms
            .load(Ordering::Acquire)
        {
            0 => None,
            ms => Some(ms),
        }
    }

    /// Stops accepting work. The sandboxes this half decided about run on
    /// machines that outlive it, so none of them is stopped here.
    pub async fn shutdown(self: &Arc<Self>) -> Result<()> {
        let was_already_shutting_down = self.is_shutting_down.swap(true, Ordering::AcqRel);
        let _ = self.shutdown_tx.send_replace(true);
        if !was_already_shutting_down {
            info!(
                "this process runs no sandboxes of its own; leaving the recorded sandboxes to \
                 the machines running them"
            );
        }
        Ok(())
    }

    /// Extends a sandbox's lifetime.
    #[tracing::instrument(skip(self), fields(sandbox_id = %sandbox_id, allow_shorter = allow_shorter))]
    pub async fn keep_alive_for(
        &self,
        sandbox_id: SandboxId,
        timeout: Option<Duration>,
        allow_shorter: bool,
    ) -> Result<Option<SandboxMetadata>> {
        self.ensure_accepting_lifecycle_operations()?;

        let valid_timeout = timeout.unwrap_or(self.default_sandbox_timeout);

        let mut metadata = match self.store.get(&sandbox_id).await? {
            Some(metadata) => metadata,
            None => return Err(OrchestratorError::SandboxNotFound(sandbox_id)),
        };

        // A transitional state that may resolve to `Running` is waited out
        // before the keep-alive is judged against it.
        if matches!(
            metadata.state,
            SandboxState::Creating | SandboxState::Snapshotting | SandboxState::Forking
        ) {
            debug!(state = ?metadata.state, "sandbox in transitional state, waiting before applying keep-alive");
            metadata = self.wait_for_transition(sandbox_id, metadata.state).await?;
        }

        if metadata.state != SandboxState::Running {
            info!(state = ?metadata.state, "cannot update keep-alive timeout in non-running state");
            return Err(OrchestratorError::InvalidSandboxState {
                sandbox_id,
                state: metadata.state,
            });
        }

        // A record that says running is this replica's memory; the binding is
        // the cluster's. Extending the life of a runtime nobody can reach would
        // keep the record out of the evictor's way forever.
        if self.runtime_confirmed_gone(sandbox_id).await {
            self.forget_unrouted_runtime(sandbox_id, metadata.execution_id)
                .await;
            return Err(OrchestratorError::SandboxNotFound(sandbox_id));
        }

        // Only an already-exhausted lifetime ceiling refuses renewal.
        let now = SystemTime::now();
        if let Some(deadline) = metadata.lifetime_deadline(now) {
            if now >= deadline {
                info!(?deadline, "sandbox has exceeded its maximum lifetime");
                return Err(OrchestratorError::SandboxLifetimeExceeded {
                    sandbox_id,
                    deadline,
                });
            }
        }

        let mut timeout_updated = false;
        let update_result = self
            .store
            .update_if_state(&sandbox_id, &[SandboxState::Running], |metadata| {
                let new_expire_time = SystemTime::now().checked_add(valid_timeout);
                if !allow_shorter {
                    if let (Some(current_expire), Some(new_expire)) =
                        (metadata.expires_at, new_expire_time)
                    {
                        if new_expire <= current_expire {
                            info!(
                                current_expire = ?current_expire,
                                new_expire = ?new_expire,
                                "new timeout is not longer than current timeout, skipping update",
                            );
                            return;
                        }
                    }
                }
                metadata.set_timeout(Some(valid_timeout));
                timeout_updated = true;
            })
            .await
            .map_err(|err| match err {
                StoreError::StateConflict { actual_state, .. } => {
                    info!(state = ?actual_state, "keep-alive update failed due to state conflict");
                    OrchestratorError::InvalidSandboxState {
                        sandbox_id,
                        state: actual_state,
                    }
                }
                other => OrchestratorError::from(other),
            })?;
        if timeout_updated {
            info!(?valid_timeout, "sandbox keep-alive timeout updated");
        }

        Ok(Some(update_result.current))
    }

    /// Waits for `sandbox_id` to leave `transitional_state`, then returns the
    /// resulting metadata.
    async fn wait_for_transition(
        &self,
        sandbox_id: SandboxId,
        transitional_state: SandboxState,
    ) -> Result<SandboxMetadata> {
        let states = [transitional_state];
        let wait = self.store.wait_while_in_states(&sandbox_id, &states);
        match tokio::time::timeout(WAIT_TRANSITION_TIMEOUT, wait).await {
            Ok(Ok(Some(metadata))) => Ok(metadata),
            Ok(Ok(None)) => Err(OrchestratorError::SandboxNotFound(sandbox_id)),
            Ok(Err(err)) => Err(OrchestratorError::from(err)),
            Err(_elapsed) => {
                warn!(
                    %sandbox_id,
                    state = ?transitional_state,
                    "timed out waiting for sandbox to leave transitional state"
                );
                Err(OrchestratorError::InvalidSandboxState {
                    sandbox_id,
                    state: transitional_state,
                })
            }
        }
    }

    /// Waits for a pause in flight to settle.
    ///
    /// `None` is the pause having finished: the record is gone and the
    /// sandbox's newest catalog row speaks for it from here.
    pub async fn wait_for_pause_to_settle(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<SandboxMetadata>> {
        let wait = self
            .store
            .wait_while_in_states(&sandbox_id, &[SandboxState::Pausing]);
        match tokio::time::timeout(CONCURRENT_PAUSE_WAIT, wait).await {
            Ok(settled) => settled.map_err(OrchestratorError::from),
            Err(_elapsed) => {
                warn!(%sandbox_id, "timed out waiting for a pause to settle");
                Err(OrchestratorError::InvalidSandboxState {
                    sandbox_id,
                    state: SandboxState::Pausing,
                })
            }
        }
    }

    /// Pauses or deletes sandboxes whose timeout has expired.
    pub(crate) async fn evict_expired_sandboxes(self: &Arc<Self>) -> Result<Vec<SandboxId>> {
        if self.is_shutting_down() {
            debug!("skipping auto-evict because this process is shutting down");
            return Ok(Vec::new());
        }

        let expired = self
            .store
            .expired_batch(SystemTime::now(), AUTO_EVICT_BATCH_LIMIT)
            .await?;
        let mut evicted_ids = Vec::new();

        for metadata in expired {
            if metadata.state != SandboxState::Running {
                continue;
            }
            let target_state = match metadata.timeout_action {
                SandboxTimeoutAction::Pause => SandboxState::Pausing,
                SandboxTimeoutAction::Delete => SandboxState::Killing,
            };
            // The batch is a stale read; the claim re-checks expiry and the
            // incarnation against the record as it stands now.
            if !self.claim_eviction(&metadata, target_state).await {
                continue;
            }
            if let Err(err) = match metadata.timeout_action {
                SandboxTimeoutAction::Pause => {
                    self.pause_entered_sandbox(metadata.id).await.map(|_| ())
                }
                SandboxTimeoutAction::Delete => {
                    self.delete_entered_sandbox(metadata.id, SandboxState::Running)
                        .await
                }
            } {
                warn!(
                    sandbox_id = %metadata.id,
                    action = ?metadata.timeout_action,
                    error = ?err,
                    "failed to auto-evict expired sandbox"
                );
                continue;
            }
            evicted_ids.push(metadata.id);
        }

        Ok(evicted_ids)
    }

    /// Enters `target_state` for an eviction, atomically with a re-check that
    /// the sandbox is still the same run and still expired.
    async fn claim_eviction(&self, metadata: &SandboxMetadata, target_state: SandboxState) -> bool {
        let request = TransitionRequest::new(target_state, vec![SandboxState::Running])
            .with_execution(metadata.execution_id)
            .as_eviction();
        match self.store.start_transition(&metadata.id, request).await {
            Ok(TransitionOutcome::Started(guard)) => {
                // The operation settles the record itself, so the claim has
                // nothing left to do once the state is entered.
                if let Err(error) = guard.release().await {
                    warn!(
                        sandbox_id = %metadata.id,
                        error = %error,
                        "could not release an eviction claim; it expires on its own"
                    );
                }
                true
            }
            Ok(TransitionOutcome::NotExpired) => {
                debug!(
                    sandbox_id = %metadata.id,
                    "a keep-alive landed after this sandbox was picked for eviction"
                );
                false
            }
            Ok(TransitionOutcome::InFlight { transition_id }) => {
                debug!(
                    sandbox_id = %metadata.id,
                    %transition_id,
                    "another caller is already moving this sandbox out of running"
                );
                false
            }
            Err(error) => {
                debug!(
                    sandbox_id = %metadata.id,
                    error = %error,
                    "the sandbox picked for eviction is no longer the one that was picked"
                );
                false
            }
        }
    }

    fn start_auto_evict_task(
        this: Arc<Self>,
        evict_interval: Duration,
        mut shutdown_rx: watch::Receiver<bool>,
    ) {
        let Ok(runtime_handle) = tokio::runtime::Handle::try_current() else {
            warn!("auto-evict task not started: no Tokio runtime available");
            return;
        };

        let this = Arc::downgrade(&this);
        runtime_handle.spawn(async move {
            let mut ticker = tokio::time::interval(evict_interval);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            debug!("auto-evict task started with interval {:?}", evict_interval);

            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            debug!("auto-evict task stopping because this process is shutting down");
                            break;
                        }
                    }
                    _ = ticker.tick() => {
                        let Some(this) = this.upgrade() else {
                            debug!("auto-evict task stopping because the control path was dropped");
                            break;
                        };
                        if let Err(err) = this.evict_expired_sandboxes().await {
                            warn!("auto-evict task failed: {err}");
                        }
                        if let Err(err) = this.reap_orphaned_grants().await {
                            warn!("grant reaper failed: {err}");
                        }
                    }
                }
            }
        });
    }
}

#[cfg(any(test, feature = "test-support"))]
impl<S: MetadataStore + 'static> SandboxControl<S> {
    pub async fn set_metadata_state_for_test(
        &self,
        sandbox_id: SandboxId,
        state: SandboxState,
    ) -> Result<()> {
        match self.store.get(&sandbox_id).await? {
            Some(mut metadata) => {
                metadata.state = state;
                self.store.update(metadata).await?;
            }
            None => {
                self.store
                    .add(SandboxMetadata {
                        id: sandbox_id,
                        state,
                        ..Default::default()
                    })
                    .await?;
            }
        }
        Ok(())
    }

    pub async fn remove_sandbox_for_test(&self, sandbox_id: &SandboxId) -> Result<()> {
        self.store.remove(sandbox_id).await?;
        Ok(())
    }

    pub async fn set_auto_resume_for_test(
        &self,
        sandbox_id: &SandboxId,
        auto_resume_enabled: bool,
    ) -> Result<()> {
        let Some(mut metadata) = self.store.get(sandbox_id).await? else {
            return Err(OrchestratorError::SandboxNotFound(*sandbox_id));
        };
        metadata.auto_resume = auto_resume_enabled;
        self.store.update(metadata).await?;
        Ok(())
    }
}

#[async_trait]
impl<S: MetadataStore + 'static> SandboxOrchestration for SandboxControl<S> {
    async fn create_sandbox(
        self: Arc<Self>,
        request: CreateSandboxRequest,
    ) -> Result<SandboxMetadata> {
        SandboxControl::create_sandbox(&self, request).await
    }

    async fn restore_or_join_launch(
        self: Arc<Self>,
        sandbox_id: SandboxId,
        request: CreateSandboxRequest,
    ) -> Result<RestoredSandbox> {
        SandboxControl::restore_or_join_launch(&self, sandbox_id, request).await
    }

    async fn fork_sandbox(
        self: Arc<Self>,
        source_sandbox_id: SandboxId,
        children: ForkChildren,
        new_timeout: NewTimeout,
    ) -> Result<Vec<SandboxForkOutcome>> {
        SandboxControl::fork_sandbox(&self, source_sandbox_id, children, new_timeout).await
    }

    async fn delete_sandbox(self: Arc<Self>, sandbox_id: SandboxId) -> Result<()> {
        SandboxControl::delete_sandbox(&self, sandbox_id).await
    }

    async fn shutdown(self: Arc<Self>) -> Result<()> {
        SandboxControl::shutdown(&self).await
    }

    async fn pause_sandbox(self: Arc<Self>, sandbox_id: SandboxId) -> Result<PauseOutcome> {
        SandboxControl::pause_sandbox(&self, sandbox_id).await
    }

    async fn capture_snapshot(
        self: Arc<Self>,
        sandbox_id: SandboxId,
    ) -> Result<SnapshotCaptureResult> {
        SandboxControl::capture_snapshot(&self, sandbox_id).await
    }

    async fn replace_sandbox_network_policy(
        self: Arc<Self>,
        sandbox_id: SandboxId,
        network_policy: SandboxNetworkPolicy,
    ) -> Result<()> {
        SandboxControl::replace_sandbox_network_policy(&self, sandbox_id, network_policy).await
    }

    async fn patch_sandbox_custom_extension_params(
        self: Arc<Self>,
        sandbox_id: SandboxId,
        patch: serde_json::Map<String, serde_json::Value>,
    ) -> Result<Option<CustomExtensionParams>> {
        SandboxControl::patch_sandbox_custom_extension_params(&self, sandbox_id, patch).await
    }

    async fn get_sandbox(&self, sandbox_id: &SandboxId) -> Result<Option<SandboxMetadata>> {
        SandboxControl::get_sandbox(self, sandbox_id).await
    }

    async fn wait_for_pause_to_settle(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<SandboxMetadata>> {
        SandboxControl::wait_for_pause_to_settle(self, sandbox_id).await
    }

    async fn list_sandboxes_filtered(
        &self,
        filter: SandboxListFilter,
    ) -> Result<Vec<SandboxMetadata>> {
        SandboxControl::list_sandboxes_filtered(self, filter).await
    }

    async fn list_sandbox_roster(&self) -> Result<Vec<SandboxRosterEntry>> {
        SandboxControl::list_sandbox_roster(self).await
    }

    async fn keep_alive_for(
        &self,
        sandbox_id: SandboxId,
        timeout: Option<Duration>,
        allow_shorter: bool,
    ) -> Result<Option<SandboxMetadata>> {
        SandboxControl::keep_alive_for(self, sandbox_id, timeout, allow_shorter).await
    }

    async fn sandbox_holding_node_id(&self, sandbox_id: &SandboxId) -> Option<String> {
        SandboxControl::sandbox_holding_node_id(self, sandbox_id).await
    }

    async fn metrics_snapshot(&self) -> Result<OrchestratorMetrics> {
        SandboxControl::metrics_snapshot(self).await
    }

    #[cfg(any(test, feature = "test-support"))]
    async fn set_metadata_state_for_test(
        &self,
        sandbox_id: SandboxId,
        state: SandboxState,
    ) -> Result<()> {
        SandboxControl::set_metadata_state_for_test(self, sandbox_id, state).await
    }

    #[cfg(any(test, feature = "test-support"))]
    async fn remove_sandbox_for_test(&self, sandbox_id: &SandboxId) -> Result<()> {
        SandboxControl::remove_sandbox_for_test(self, sandbox_id).await
    }

    #[cfg(any(test, feature = "test-support"))]
    async fn set_auto_resume_for_test(
        &self,
        sandbox_id: &SandboxId,
        auto_resume_enabled: bool,
    ) -> Result<()> {
        SandboxControl::set_auto_resume_for_test(self, sandbox_id, auto_resume_enabled).await
    }

    fn get_envd_access_token(&self, metadata: &SandboxMetadata) -> Option<EnvdAccessToken> {
        SandboxControl::get_envd_access_token(self, metadata)
    }

    fn validate_envd_access_token(&self, sandbox_id: SandboxId, candidate: &str) -> bool {
        SandboxControl::validate_envd_access_token(self, sandbox_id, candidate)
    }

    fn subscribe_sandbox_events(&self) -> broadcast::Receiver<SandboxLifecycleEvent> {
        SandboxControl::subscribe_sandbox_events(self)
    }

    fn scheduling_disabled(&self) -> bool {
        SandboxControl::scheduling_disabled(self)
    }

    fn set_scheduling_disabled(&self, disabled: bool) -> bool {
        SandboxControl::set_scheduling_disabled(self, disabled)
    }

    fn scheduling_disabled_changed_at_ms(&self) -> Option<i64> {
        SandboxControl::scheduling_disabled_changed_at_ms(self)
    }
}
