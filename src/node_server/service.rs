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
    OrchestratorError, SandboxLaunchSource, SandboxMetadata, SandboxOrchestration,
};
use crate::proto::node as pb;
use crate::sandbox::{CustomExtensionParams, SandboxNetworkPolicy};
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
            timeout: convert::timeout(request.timeout_ms),
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

    async fn pause(
        &self,
        _request: Request<pb::SandboxPauseRequest>,
    ) -> Result<Response<pb::SandboxPauseResponse>, Status> {
        // 🔴 Refused rather than approximated, and the reason is worth stating
        // in full because a plausible-looking implementation is available and
        // is worse than nothing.
        //
        // A pause produces two things this reply needs and the orchestrator
        // does not currently hand back: the artifact directory the capture was
        // written into — which is allocated inside `pause_sandbox` and never
        // leaves it — and, when publishing was asked for, a staged snapshot,
        // which needs the node side of the `stage`/`commit_staged` seam wired
        // into the pause path. Answering with the paused state alone would look
        // right and hand the caller a resume that cannot find its bytes;
        // answering with `staged: None` would be indistinguishable from a
        // backend that had no publishable capture to offer, which is how a
        // sandbox comes to be paused with nothing to resume it from anywhere.
        //
        // 🔴 Classified as *not* terminal, and the classification matters even
        // for a refusal: nothing was done to the sandbox, so the caller should
        // put it back to running rather than tear it down. An unclassified
        // failure is read as terminal by design, which for this one would be
        // the wrong answer.
        Err(crate::proto::node::capture_failure_status(
            tonic::Code::Unimplemented,
            "pause is not served yet: the orchestrator does not return the artifact root a \
             capture was written into, and staging a publishable capture on the node is not \
             wired up",
            false,
            "the node did not touch the sandbox",
        ))
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

        let timeout = match convert::timeout(request.timeout_ms) {
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

        let timeout = match convert::timeout(request.timeout_ms) {
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
