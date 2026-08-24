//! The node service: what one node accepts from the API half of the split.
//!
//! # 🔴 What is not here
//!
//! `SandboxBackend` is the node's own interface to one sandbox and it does not
//! appear on this surface. Three of its methods return values that own live
//! local state — a paused capture, a captured snapshot, and the set of local
//! image artifacts a running sandbox has open — and two of those keep a
//! temporary directory alive for as long as the value does. None of them can
//! cross a process boundary, so what crosses instead is *where the bytes are*.
//!
//! # 🔴 What "not found" means here
//!
//! Nothing on this service ever answers a question it could not look at. A
//! store read that failed, a handle that could not be reached, a resolver that
//! could not reach a registry: each of those is an error, never an empty
//! answer. The caller reconciles a cluster against these replies and tears
//! sandboxes down on the strength of them, and "I could not look" and "there is
//! nothing there" have to stay two different answers all the way up.

use std::sync::Arc;

use tonic::{Request, Response, Status};
use tracing::{debug, warn};

use crate::orchestrator::{
    ClaimedExecution, CreateSandboxRequest, ForkChildAssignment, ForkChildren, NewTimeout,
    OrchestratorError, SandboxLaunchSource, SandboxMetadata, SandboxOperation,
    SandboxOrchestration,
};
use crate::proto::node as pb;
use crate::sandbox::{CustomExtensionParams, SandboxCaptureError, SandboxNetworkPolicy};
use crate::snapshot::SnapshotManager;
use crate::types::{ExecutionId, SandboxId};

use super::convert;
use super::ownership::owned_by_control_plane;

/// Serves [`pb::node_sandbox_service_server::NodeSandboxService`] out of one
/// node's orchestrator.
pub struct NodeSandboxService {
    orchestration: Arc<dyn SandboxOrchestration>,
    /// Used to turn a snapshot id from the wire into something this machine can
    /// boot.
    ///
    /// 🔴 The API half sends an id, not a resolved snapshot. Resolving one
    /// opens local artifacts and yields local paths, and both belong to the
    /// machine that will run the VM — so the API decides *which* snapshot and
    /// the node decides what that means on its disk.
    snapshots: Arc<SnapshotManager>,
    node_id: String,
}

impl NodeSandboxService {
    pub fn new(
        orchestration: Arc<dyn SandboxOrchestration>,
        snapshots: Arc<SnapshotManager>,
        node_id: String,
    ) -> Self {
        Self {
            orchestration,
            snapshots,
            node_id,
        }
    }

    /// Refuses a call addressed to a run this node is not the one running.
    ///
    /// # 🔴 Three answers, and only one of them is a refusal
    ///
    /// - the incarnations match: proceed;
    /// - they differ: the caller is acting on a run that has been superseded —
    ///   most likely resumed on another machine — and applying its command
    ///   would tear down or mutate a run it has never seen;
    /// - this node has no record of the sandbox at all: `NotFound`, which the
    ///   caller resolves by looking again rather than by concluding anything.
    ///
    /// The incarnation is read from the live handle when there is one and from
    /// the record otherwise, because a paused sandbox has a record and no
    /// handle and is still perfectly deletable.
    async fn fenced(&self, sandbox_id: SandboxId, claimed: ExecutionId) -> Result<(), Status> {
        if let Some(live) = self.orchestration.live_execution_id(&sandbox_id).await {
            return if live == claimed {
                Ok(())
            } else {
                Err(superseded(sandbox_id, claimed, live))
            };
        }

        // 🔴 An error from the record store is propagated, not read as absence.
        // "I could not reach the records" and "this node has never heard of
        // that sandbox" lead the caller to opposite conclusions.
        let record = self
            .orchestration
            .get_sandbox(&sandbox_id)
            .await
            .map_err(|err| orchestrator_status(&err))?;
        match record {
            Some(record) if record.execution_id == claimed => Ok(()),
            Some(record) => Err(superseded(sandbox_id, claimed, record.execution_id)),
            None => Err(Status::not_found(format!(
                "sandbox {sandbox_id} is not on this node"
            ))),
        }
    }

    /// What a caller learns about a sandbox this node has just brought up.
    ///
    /// Shared by `create` and `resume` because the two answer the same
    /// question — *what is running now, and under which run* — and a second
    /// copy of it is a second place for a field to be forgotten.
    ///
    /// The two facts a record cannot supply — the address the sandbox reaches
    /// the host on, and the size the rootfs turned out to be — are only
    /// knowable from the live handle, and this is the one call that reaches
    /// one.
    ///
    /// It costs a scan of everything running on this node. That is a real cost
    /// and it is the right trade here: the call it is inside has just booted a
    /// virtual machine, and the alternative is a second accessor on the
    /// orchestration surface that exists to serve one field.
    ///
    /// 🔴 `.ok()` and not `?`: an operation that *succeeded* must not be
    /// reported as a failure because the follow-up read did not work. The
    /// sandbox is running either way, and a caller told it failed would leak
    /// it.
    async fn running_sandbox(&self, metadata: &SandboxMetadata) -> pb::SandboxCreateResponse {
        let sandbox_id = metadata.id;
        let live = self.orchestration.list_live_sandboxes().await.ok();
        let facts = live.as_ref().and_then(|live| {
            live.iter()
                .find(|candidate| candidate.sandbox_id == sandbox_id)
        });

        pb::SandboxCreateResponse {
            sandbox_id: sandbox_id.to_string(),
            execution_id: metadata.execution_id.to_string(),
            host_interaction_ip: facts
                .and_then(|facts| facts.host_interaction_ip)
                .map(|ip| ip.to_string())
                .unwrap_or_default(),
            rootfs_virtual_size: facts
                .and_then(|facts| facts.rootfs_virtual_size)
                .unwrap_or_default(),
            resources: Some(pb::SandboxResources {
                cpu_count: metadata.resources.cpu_count,
                memory_mib: metadata.resources.memory_mib,
                disk_size_mib: metadata.resources.disk_size_mib,
            }),
            started_at_ms: convert::unix_millis(Some(metadata.created_at)),
            expires_at_ms: convert::unix_millis(metadata.expires_at),
        }
    }
}

/// Re-stamps a status raised before the backend was reached as one that did not
/// touch the sandbox.
///
/// 🔴 The code and message are kept exactly as they were — a `NotFound` stays a
/// `NotFound` — and only the classification is added. Without it the caller,
/// which treats an unclassified capture failure as terminal, tears down a
/// sandbox over a fence check that never reached the runtime.
fn untouched(status: Status) -> Status {
    crate::proto::node::capture_failure_status(
        status.code(),
        status.message().to_string(),
        false,
        "the node did not touch the sandbox",
    )
}

/// What a caller must do about a pause that did not produce a capture.
///
/// # 🔴 Terminal is the default and each exception is named
///
/// "Terminal" tells the caller the live runtime was mutated past safe resume
/// and has to be torn down; "recoverable" tells it the sandbox is still there
/// and should go back to running. Guessing either way is expensive, so the only
/// answers that claim the sandbox survived are the ones this node can prove:
/// the orchestrator's own capture classification, and the errors raised before
/// the backend was ever asked to pause.
fn pause_failure_status(sandbox_id: SandboxId, err: &OrchestratorError) -> Status {
    let status = orchestrator_status(err);
    let terminal = match err {
        // The backend answered, and its answer already says which of the two
        // this is — the orchestrator acted on the same flag when it decided
        // whether to put the sandbox back or tear it down.
        //
        // 🔴 `unwrap_or(true)` and not `false`: a source that is not a capture
        // error is a failure this node cannot account for, and the fail-closed
        // reading of "I do not know what happened to the runtime" is that it
        // did not survive.
        OrchestratorError::SandboxOperationFailed {
            operation: SandboxOperation::Pause,
            source,
            ..
        } => source
            .downcast_ref::<SandboxCaptureError>()
            .map(SandboxCaptureError::is_terminal)
            .unwrap_or(true),
        // Refused before the runtime was touched: the wrong state, another
        // operation holding the sandbox, a node that is draining, a request
        // that does not parse.
        OrchestratorError::InvalidSandboxState { .. }
        | OrchestratorError::SandboxOperationConflict { .. }
        | OrchestratorError::SandboxLifetimeExceeded { .. }
        | OrchestratorError::InvalidRequest(_)
        | OrchestratorError::ShuttingDown
        | OrchestratorError::NotAcceptingNewWork
        | OrchestratorError::VirtualizationModeMismatch { .. } => false,
        // 🔴 Allocating the artifact directory happens before the backend is
        // asked for anything, and its failure puts the sandbox straight back to
        // running.
        OrchestratorError::SandboxPersistenceFailed(_) => false,
        // 🔴 Including `SandboxNotFound`, which on this path means the pause
        // found no sandbox to pause and removed the record — so the caller's
        // record of it is the last one standing and must go too.
        _ => true,
    };
    crate::proto::node::capture_failure_status(
        status.code(),
        format!("sandbox {sandbox_id}: {}", status.message()),
        terminal,
        err.to_string(),
    )
}

fn superseded(sandbox_id: SandboxId, claimed: ExecutionId, actual: ExecutionId) -> Status {
    Status::failed_precondition(format!(
        "sandbox {sandbox_id} is running execution {actual}, and this call names {claimed}"
    ))
}

/// Maps an orchestrator failure onto a gRPC status.
///
/// 🔴 `SandboxNotFound` becomes `NotFound` and everything else stays a failure.
/// The temptation is to flatten "it was not there" and "something went wrong"
/// into one unhappy path, and the caller's next move differs between them:
/// after `NotFound` it may decide the sandbox is gone, and after anything else
/// it may not.
fn orchestrator_status(err: &OrchestratorError) -> Status {
    match err {
        OrchestratorError::SandboxNotFound(sandbox_id) => {
            Status::not_found(format!("sandbox {sandbox_id} is not on this node"))
        }
        OrchestratorError::InvalidRequest(message) => Status::invalid_argument(message.clone()),
        OrchestratorError::InvalidSandboxState { .. }
        | OrchestratorError::SandboxOperationConflict { .. }
        | OrchestratorError::SandboxLifetimeExceeded { .. } => {
            Status::failed_precondition(err.to_string())
        }
        OrchestratorError::ShuttingDown | OrchestratorError::NotAcceptingNewWork => {
            Status::unavailable(err.to_string())
        }
        other => Status::internal(other.to_string()),
    }
}

#[tonic::async_trait]
impl pb::node_sandbox_service_server::NodeSandboxService for NodeSandboxService {
    async fn create(
        &self,
        request: Request<pb::SandboxCreateRequest>,
    ) -> Result<Response<pb::SandboxCreateResponse>, Status> {
        let request = request.into_inner();
        let sandbox_id = convert::sandbox_id(&request.sandbox_id)?;
        // 🔴 Everything that can be refused without touching the machine is
        // refused first. Resolving a snapshot reaches a registry and opens
        // local artifacts, and doing that before noticing the request was
        // malformed spends a network round trip to arrive at the same answer.
        let timeout_action = convert::timeout_action(request.timeout_action)?;
        // 🔴 Read here, with the rest of the "can be refused without touching
        // the machine" group, and not down at the launch. A create whose sender
        // did not say who keeps the deadline is refused before a snapshot is
        // resolved, so the refusal costs a registry round trip less than the
        // silent mis-reading it replaced.
        let expiry = convert::create_expiry(request.expiry)?;
        let network_policy: SandboxNetworkPolicy =
            convert::serialized(request.network_policy.as_ref(), "network_policy")?
                .unwrap_or_default();
        let custom_extension_params: Option<CustomExtensionParams> = convert::serialized(
            request.custom_extension_params.as_ref(),
            "custom_extension_params",
        )?;

        let source = match request.source {
            Some(pb::sandbox_create_request::Source::Snapshot(snapshot)) => {
                if snapshot.snapshot_id.is_empty() {
                    return Err(Status::invalid_argument("snapshot.snapshot_id is required"));
                }
                let runnable = self
                    .snapshots
                    .load_runnable(&snapshot.snapshot_id)
                    .await
                    // 🔴 A resolver that could not answer is an error. Reading
                    // it as "no such snapshot" would turn a registry outage
                    // into a permanent-looking refusal.
                    .map_err(|err| {
                        Status::internal(format!(
                            "resolve snapshot {}: {err:#}",
                            snapshot.snapshot_id
                        ))
                    })?
                    .ok_or_else(|| {
                        Status::not_found(format!("snapshot {} not found", snapshot.snapshot_id))
                    })?;
                SandboxLaunchSource::Snapshot(Box::new(runnable))
            }
            Some(pb::sandbox_create_request::Source::Image(_)) => {
                // 🔴 Declared in the proto and refused here, rather than
                // half-implemented. A cold create needs the node to resolve an
                // image reference and its attached drives into local overlaybd,
                // and the piece that is genuinely missing is on the *calling*
                // side: `SandboxBackendFactory::build` hands a factory a
                // `FreshSandboxBuildSpec` whose paths are already resolved and
                // local, so a remote factory has no reference left to send. See
                // the note on `RemoteSandboxBackendFactory::build`.
                return Err(Status::unimplemented(
                    "creating a sandbox from an image reference is not served yet: the remote \
                     factory is handed an already-resolved local build spec and has no reference \
                     left to send. Use a snapshot source.",
                ));
            }
            None => return Err(Status::invalid_argument("source is required")),
        };

        let create = CreateSandboxRequest {
            source,
            expiry,
            timeout_action,
            auto_resume: request.auto_resume,
            user_metadata: (!request.user_metadata.is_empty()).then_some(request.user_metadata),
            env_vars: (!request.env_vars.is_empty()).then_some(request.env_vars),
            network_policy,
            secure: request.secure,
            custom_extension_params,
            // 🔴 The one place a marker is ever set. Everything else that can
            // create a sandbox on this node leaves it `None`.
            control_plane_config: convert::control_plane_config(&request.control_plane_config),
            // 🔴 Empty means "you choose". A caller that sent one is an
            // orchestrator that already recorded it, and running the sandbox
            // under a different one would leave its record naming a run that
            // is not the one that is up.
            execution_id: convert::optional_execution_id(&request.execution_id)?,
        };

        // Under the id the caller chose, because the caller's record of this
        // sandbox already names it.
        let metadata = Arc::clone(&self.orchestration)
            .restore_sandbox(sandbox_id, create)
            .await
            .map_err(|err| orchestrator_status(&err))?;

        Ok(Response::new(self.running_sandbox(&metadata).await))
    }

    async fn delete(
        &self,
        request: Request<pb::SandboxDeleteRequest>,
    ) -> Result<Response<pb::SandboxDeleteResponse>, Status> {
        let request = request.into_inner();
        let sandbox_id = convert::sandbox_id(&request.sandbox_id)?;
        let execution_id = convert::execution_id(&request.execution_id)?;
        self.fenced(sandbox_id, execution_id).await?;

        Arc::clone(&self.orchestration)
            .delete_sandbox(sandbox_id)
            .await
            .map_err(|err| orchestrator_status(&err))?;
        Ok(Response::new(pb::SandboxDeleteResponse {}))
    }

    /// Pauses the sandbox and says where the capture it produced now sits.
    ///
    /// # 🔴 What travels back is a location, not a capture
    ///
    /// The bytes are written to this node's disk and stay there. What the
    /// caller receives is the pair its own factory will hand back the day it
    /// wants the sandbox reopened — the directory they went into and this
    /// node's own encoding of its backend state — plus, from the reply's
    /// envelope, which machine said it. Nothing here owns anything the caller
    /// has to release.
    ///
    /// # 🔴 Every failure carries a classification, including the ones that
    /// are not capture failures
    ///
    /// The caller reads an unclassified failure as *terminal* by design, and
    /// terminal on this path means "tear the sandbox down". So a refusal that
    /// left the sandbox untouched — the wrong state, a node that is draining, a
    /// request that never reached the backend — must say so explicitly, or a
    /// caller doing exactly what it was told to do will delete a running
    /// sandbox because its pause arrived at an awkward moment. See
    /// [`pause_failure_status`].
    ///
    /// # 🔴 `publish` is refused rather than answered with nothing
    ///
    /// Staging a publishable capture on the node is not wired up: the seam
    /// exists (`SnapshotManager::stage_captured` / `commit_staged`) and the
    /// orchestrator does not hand the publishable capture back out of
    /// `pause_sandbox`, so there is nothing here to stage. Answering
    /// `staged: None` would be indistinguishable from a backend that had no
    /// publishable capture to offer — which is exactly how a sandbox comes to
    /// be paused with nothing to rebuild it from anywhere, silently. A caller
    /// that needs the published arm is told it cannot have it.
    async fn pause(
        &self,
        request: Request<pb::SandboxPauseRequest>,
    ) -> Result<Response<pb::SandboxPauseResponse>, Status> {
        let request = request.into_inner();
        // 🔴 Classified, like every other refusal on this call. A request that
        // does not parse never reached the runtime, and the caller reads an
        // unclassified capture failure as terminal — so leaving these two bare
        // would have a malformed request tear down the sandbox it named.
        let sandbox_id = convert::sandbox_id(&request.sandbox_id).map_err(untouched)?;
        let execution_id = convert::execution_id(&request.execution_id).map_err(untouched)?;
        if request.publish {
            // 🔴 Before the fence and before the pause. A refusal that arrived
            // after the VM was stopped would have destroyed the thing the
            // caller was refused.
            return Err(crate::proto::node::capture_failure_status(
                tonic::Code::Unimplemented,
                "publishing a pause is not served yet: staging a captured snapshot on the node is \
                 not wired up, and answering with no staged row would be indistinguishable from a \
                 backend that had nothing publishable to offer",
                false,
                "the node did not touch the sandbox",
            ));
        }
        self.fenced(sandbox_id, execution_id)
            .await
            .map_err(untouched)?;

        let metadata = Arc::clone(&self.orchestration)
            .pause_sandbox(sandbox_id)
            .await
            .map_err(|err| pause_failure_status(sandbox_id, &err))?;

        // 🔴 The handle the pause produced, from the record the pause wrote. A
        // reply without it is not a pause with nothing to say: it is a stopped
        // VM the caller has no way of reopening, and the caller has to be told
        // that rather than handed a `paused_state` it will store and later find
        // empty.
        let paused_state = metadata.paused_state.as_ref().ok_or_else(|| {
            // 🔴 Not terminal. The sandbox *is* paused on this node and a later
            // `Resume` addressed here will reopen it from this node's own
            // record; what failed is this reply's ability to describe it. Of
            // the two wrong readings available, "put it back to running" costs
            // a reconciliation and "tear it down" costs the user's only copy.
            crate::proto::node::capture_failure_status(
                tonic::Code::Internal,
                format!(
                    "sandbox {sandbox_id} was paused on this node, but its record carries no \
                     capture to describe"
                ),
                false,
                "the sandbox is paused on this node and can still be reopened here",
            )
        })?;
        let state = paused_state
            .encode()
            .map_err(|err| {
                crate::proto::node::capture_failure_status(
                    tonic::Code::Internal,
                    format!("encode sandbox {sandbox_id}'s paused state: {err:#}"),
                    false,
                    "the sandbox is paused on this node and can still be reopened here",
                )
            })
            .and_then(|state| {
                crate::proto::node::encode_value(&state).map_err(|err| {
                    crate::proto::node::capture_failure_status(
                        tonic::Code::Internal,
                        format!("encode sandbox {sandbox_id}'s paused state: {err}"),
                        false,
                        "the sandbox is paused on this node and can still be reopened here",
                    )
                })
            })?;

        // 🔴 A failed read fails the call rather than answering with an empty
        // path. The caller stores this and never asks again, so a blank written
        // into its record because a disk hiccuped is a permanent lie about
        // where the user's sandbox lives.
        let artifact_root = self
            .orchestration
            .paused_artifact_root(&sandbox_id)
            .await
            .map_err(|err| pause_failure_status(sandbox_id, &err))?;
        if artifact_root.is_none() {
            // Not a failure: a node whose persister allocates nothing captured
            // into temporaries it manages itself, and there is no directory to
            // name. Worth saying out loud because the caller's record of this
            // sandbox will carry no location.
            warn!(
                %sandbox_id,
                "paused a sandbox this node kept no artifact directory for"
            );
        }

        Ok(Response::new(pb::SandboxPauseResponse {
            paused_state: Some(pb::PausedState {
                artifact_root: artifact_root
                    .map(|root| root.to_string_lossy().into_owned())
                    .unwrap_or_default(),
                state: Some(state),
            }),
            // 🔴 Always absent, and only reachable because `publish` was not
            // asked for: the arm that would fill this is refused above.
            staged: None,
        }))
    }

    async fn checkpoint(
        &self,
        _request: Request<pb::SandboxCheckpointRequest>,
    ) -> Result<Response<pb::SandboxCheckpointResponse>, Status> {
        // The same missing piece as `pause`, minus the artifact root: a
        // checkpoint's whole product is the staged snapshot, and staging on the
        // node is not wired up.
        Err(crate::proto::node::capture_failure_status(
            tonic::Code::Unimplemented,
            "checkpoint is not served yet: staging a captured snapshot on the node is not wired \
             up, and there is nothing else this call could return",
            false,
            "the node did not touch the sandbox",
        ))
    }

    /// Reopens the capture this node is holding.
    ///
    /// # 🔴 Why nothing about the capture arrives with the request
    ///
    /// The bytes never left. `Pause` handed the caller a path on this node's
    /// disk and this node's own encoding of its backend state, and the caller
    /// stored those so it could tell *which machine* to come back to — not so
    /// it could hand them back as an input. What reopens the sandbox is the
    /// record this node already has: the paused metadata, the persisted
    /// artifacts, and the paused-state handle its own factory decoded. So this
    /// call carries an identity, a fence, and the run to start, and everything
    /// else would be a second source of truth for a question that already has
    /// one.
    ///
    /// # 🔴 The three answers, and why they may not be flattened
    ///
    /// - the record is here and names the run being resumed: proceed;
    /// - **there is no record**: `NotFound`, which tells the caller the only
    ///   copy of this sandbox is not on this machine — a conclusion it acts on
    ///   by rebuilding from a published snapshot, or by giving the sandbox up;
    /// - **the records could not be read**, or the node is not taking work:
    ///   anything but `NotFound`. A caller that read those as absence would
    ///   discard a sandbox whose bytes are sitting intact on this disk.
    ///
    /// The first two come out of [`fenced`](Self::fenced), which a paused
    /// sandbox reaches through its record because it has no live handle.
    async fn resume(
        &self,
        request: Request<pb::SandboxResumeRequest>,
    ) -> Result<Response<pb::SandboxResumeResponse>, Status> {
        let request = request.into_inner();
        let sandbox_id = convert::sandbox_id(&request.sandbox_id)?;
        let paused_execution_id = convert::execution_id(&request.execution_id)?;
        // 🔴 Required, and not `optional_execution_id`. An empty value on a
        // create means "you choose", because a caller that keeps no record of
        // its own has nothing to impose; a resume has no such caller. The only
        // way to obtain the incarnation a resume runs under is the arbitration
        // that decided the sandbox may come back, so a request that carries
        // none is a resume nobody licensed.
        let resumed_execution_id = convert::execution_id(&request.resumed_execution_id)?;
        if resumed_execution_id == paused_execution_id {
            // 🔴 Refused rather than treated as a no-op. A resume starts a new
            // run; one that reused the paused run's identity would leave every
            // command written before the pause indistinguishable from one
            // written after it, which is precisely what the incarnation on
            // every other call here exists to tell apart.
            return Err(Status::invalid_argument(format!(
                "sandbox {sandbox_id} cannot be resumed as the same run it was paused under \
                 ({paused_execution_id}): a resume starts a new one"
            )));
        }

        self.fenced(sandbox_id, paused_execution_id).await?;

        let timeout = match convert::optional_timeout(request.timeout_ms) {
            Some(timeout) => NewTimeout::Set(timeout),
            // 🔴 The sandbox keeps what it was paused with, rather than picking
            // up this node's configured default. The deadline belongs to the
            // record the caller holds, and a node that substituted its own
            // would move a deadline nobody agreed to move.
            None => NewTimeout::UseExisting,
        };

        let metadata = Arc::clone(&self.orchestration)
            .resume_sandbox(
                sandbox_id,
                timeout,
                // 🔴 Adopted, not minted. The decision was taken by the
                // orchestrator that owns this sandbox and is already in its
                // record; a node that minted here would start a run the
                // cluster's record does not name.
                ClaimedExecution::adopted_from_remote_claim(resumed_execution_id),
            )
            .await
            .map_err(|err| orchestrator_status(&err))?;

        // 🔴 Reported rather than asserted. A resume that arrived for a sandbox
        // this node had already brought back is answered by
        // `resume_sandbox` with the run that is *up*, which is not the one that
        // was asked for — and the caller, which holds the record, is the one
        // that decides what to do about the disagreement. Refusing here would
        // turn a node's honest answer into a failure of an operation that did
        // not happen.
        Ok(Response::new(pb::SandboxResumeResponse {
            started: Some(self.running_sandbox(&metadata).await),
        }))
    }

    async fn fork(
        &self,
        request: Request<pb::SandboxForkRequest>,
    ) -> Result<Response<pb::SandboxForkResponse>, Status> {
        let request = request.into_inner();
        let source_sandbox_id = convert::sandbox_id(&request.source_sandbox_id)?;
        let source_execution_id = convert::execution_id(&request.source_execution_id)?;
        self.fenced(source_sandbox_id, source_execution_id).await?;

        if request.children.is_empty() {
            return Err(Status::invalid_argument("children is required"));
        }
        let children = request
            .children
            .iter()
            .map(|child| {
                Ok(ForkChildAssignment {
                    sandbox_id: convert::sandbox_id(&child.sandbox_id)?,
                    execution_id: convert::optional_execution_id(&child.execution_id)?,
                    control_plane_config: convert::control_plane_config(
                        &child.control_plane_config,
                    ),
                })
            })
            .collect::<Result<Vec<_>, Status>>()?;

        let timeout = match convert::optional_timeout(request.timeout_ms) {
            Some(timeout) => NewTimeout::Set(timeout),
            None => NewTimeout::UseExisting,
        };

        let outcomes = Arc::clone(&self.orchestration)
            .fork_sandbox(
                source_sandbox_id,
                ForkChildren::Assigned(children.clone()),
                timeout,
            )
            .await
            .map_err(|err| orchestrator_status(&err))?;

        // 🔴 One result per requested child, in request order, paired by
        // position. That is the contract the orchestrator's fork states and the
        // one the caller's markers were assigned under; zipping is what keeps
        // the two statements of it in step.
        let results = outcomes
            .into_iter()
            .zip(&children)
            .map(|(outcome, requested)| match outcome {
                Ok(metadata) => pb::ForkChildResult {
                    sandbox_id: metadata.id.to_string(),
                    execution_id: metadata.execution_id.to_string(),
                    outcome: Some(pb::fork_child_result::Outcome::Started(
                        pb::SandboxCreateResponse {
                            sandbox_id: metadata.id.to_string(),
                            execution_id: metadata.execution_id.to_string(),
                            host_interaction_ip: String::new(),
                            rootfs_virtual_size: 0,
                            resources: Some(pb::SandboxResources {
                                cpu_count: metadata.resources.cpu_count,
                                memory_mib: metadata.resources.memory_mib,
                                disk_size_mib: metadata.resources.disk_size_mib,
                            }),
                            started_at_ms: convert::unix_millis(Some(metadata.created_at)),
                            expires_at_ms: convert::unix_millis(metadata.expires_at),
                        },
                    )),
                },
                Err(err) => pb::ForkChildResult {
                    // 🔴 The id comes from the request, not from the failure:
                    // a child that never started has no metadata to read it
                    // out of, and a result with an empty id cannot be paired
                    // with the child it belongs to.
                    sandbox_id: requested.sandbox_id.to_string(),
                    execution_id: String::new(),
                    outcome: Some(pb::fork_child_result::Outcome::Error(err.to_string())),
                },
            })
            .collect();

        Ok(Response::new(pb::SandboxForkResponse { children: results }))
    }

    async fn update_network(
        &self,
        request: Request<pb::SandboxNetworkRequest>,
    ) -> Result<Response<pb::SandboxNetworkResponse>, Status> {
        let request = request.into_inner();
        let sandbox_id = convert::sandbox_id(&request.sandbox_id)?;
        let execution_id = convert::execution_id(&request.execution_id)?;
        self.fenced(sandbox_id, execution_id).await?;

        let policy: SandboxNetworkPolicy =
            convert::serialized(request.network_policy.as_ref(), "network_policy")?
                .unwrap_or_default();

        Arc::clone(&self.orchestration)
            .replace_sandbox_network_policy(sandbox_id, policy)
            .await
            .map_err(|err| orchestrator_status(&err))?;
        Ok(Response::new(pb::SandboxNetworkResponse {}))
    }

    async fn update_params(
        &self,
        request: Request<pb::SandboxParamsRequest>,
    ) -> Result<Response<pb::SandboxParamsResponse>, Status> {
        let request = request.into_inner();
        let sandbox_id = convert::sandbox_id(&request.sandbox_id)?;
        let execution_id = convert::execution_id(&request.execution_id)?;
        self.fenced(sandbox_id, execution_id).await?;

        // 🔴 The hook has already run on the caller's side and this value is
        // its answer, so what happens here is assignment. Running the hook
        // again would be a second chance for the extension to change its mind
        // about a value the caller has already recorded.
        let _params: Option<CustomExtensionParams> = convert::serialized(
            request.custom_extension_params.as_ref(),
            "custom_extension_params",
        )?;

        Err(Status::unimplemented(
            "update_params is not served yet: the orchestrator's only entry point runs the \
             custom extension hook itself, and this call carries a value the hook has already \
             approved",
        ))
    }

    async fn list_sandboxes(
        &self,
        _request: Request<pb::ListSandboxesRequest>,
    ) -> Result<Response<pb::SandboxListResponse>, Status> {
        let live = self
            .orchestration
            .list_live_sandboxes()
            .await
            // 🔴 The whole call fails. There is no partial answer: a caller
            // that received "half the sandboxes, and something went wrong" and
            // treated it as a listing would conclude the other half are gone.
            .map_err(|err| orchestrator_status(&err))?;

        let owned = owned_by_control_plane(&live);
        if owned.len() != live.len() {
            // Not a warning: a node that also serves user-facing creates is
            // *expected* to be running sandboxes the control plane does not
            // own, and so is a node running a template build.
            debug!(
                running = live.len(),
                owned = owned.len(),
                "reporting the sandboxes the control plane owns"
            );
        }
        for sandbox in &live {
            if !sandbox.facts_from_handle {
                // Worth saying out loud: this sandbox's live facts came from
                // its record because its handle was mid-operation, so anything
                // reading `host_interaction_ip` here is reading a blank rather
                // than an address.
                warn!(
                    sandbox_id = %sandbox.sandbox_id,
                    "sandbox handle was busy; reported from its record"
                );
            }
        }

        let sandboxes = owned
            .iter()
            .map(|owned| {
                convert::node_sandbox(owned.sandbox, owned.control_plane_config, &self.node_id)
            })
            .collect();

        Ok(Response::new(pb::SandboxListResponse { sandboxes }))
    }
}
