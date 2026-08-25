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

use std::path::PathBuf;
use std::sync::Arc;

use tonic::{Request, Response, Status};
use tracing::{debug, info, warn};

use crate::image::ImageResolver;
use crate::orchestrator::{
    ClaimedExecution, CreateSandboxRequest, ForkChildAssignment, ForkChildren, LiveSandbox,
    NewTimeout, OrchestratorError, SandboxLaunchSource, SandboxMetadata, SandboxOperation,
    SandboxOrchestration,
};
use crate::proto::node as pb;
use crate::sandbox::{
    CustomExtensionParams, ExtraDrive, SandboxCaptureError, SandboxNetworkPolicy,
};
use crate::snapshot::{
    CommandContext, SnapshotId, SnapshotManager, SnapshotPublishMetadata, SnapshotPublishSource,
    SnapshotRecord,
};
use crate::template::{TemplateBuildRunner, TemplateBuildSpec, TemplateBuildStep, TemplateBuilder};
use crate::types::{ExecutionId, ImageConfigs, SandboxId, SandboxResources};

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
    /// What `build_template` needs beyond the above, plus the `ImageResolver`
    /// `create` reads for a `Source::Image` request — or `None` on a service
    /// nobody wired for either.
    ///
    /// 🔴 A builder step (`with_template_build`) rather than two more
    /// constructor parameters, and deliberately so: every test in this module
    /// but the ones that specifically exercise a build or an image-source
    /// create constructs a service through `new(...)` alone, and required
    /// extra arguments would be pure churn for all of them. Production always
    /// calls `with_template_build` — see `node_server::server`.
    ///
    /// 🔴 `create`'s `Source::Image` arm reuses `image_resolver` rather than
    /// getting its own copy. Resolving an OCI image reference into local
    /// overlaybd is one capability regardless of which RPC needed it, and a
    /// node wired for one has always been wired for the other —
    /// `assemble_node_core` builds this `ImageResolver` unconditionally, the
    /// same as `template_builder`.
    template_build: Option<TemplateBuildWiring>,
}

/// What [`NodeSandboxService::build_template`] needs beyond the orchestrator
/// and the snapshot manager every other call already has — also read by
/// `create`'s `Source::Image` arm for `image_resolver` alone. See the note on
/// `template_build`.
struct TemplateBuildWiring {
    /// Resolves an image reference into local overlaybd, node-side — the
    /// piece `--role api` cannot do for itself: see the note on
    /// `TemplateBuildImageBase` and `ImageSource` in `node.proto`.
    image_resolver: Arc<ImageResolver>,
    /// Drives `TemplateBuildRunner` and validates the build's `TemplateBuildContext`.
    template_builder: Arc<TemplateBuilder>,
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
            template_build: None,
        }
    }

    /// Wires this service to serve `BuildTemplate`.
    ///
    /// 🔴 Not called by most of this file's own tests, and that is the
    /// point — see the note on `template_build`'s field. A service nobody
    /// called this on answers `build_template` with `Unimplemented` rather
    /// than panicking on a `None`, which is why that path is a refusal and
    /// not an `.unwrap()`.
    pub fn with_template_build(
        mut self,
        image_resolver: Arc<ImageResolver>,
        template_builder: Arc<TemplateBuilder>,
    ) -> Self {
        self.template_build = Some(TemplateBuildWiring {
            image_resolver,
            template_builder,
        });
        self
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

    /// Writes one capture's bytes into the snapshot repository and encodes the
    /// row the caller will announce.
    ///
    /// # 🔴 It returns a message, not a `Status`, because its two callers do
    /// opposite things with the failure
    ///
    /// Staging runs *after* the operation that produced the capture has already
    /// settled — a checkpoint has put the sandbox back to `Running`, a pause has
    /// persisted it and stopped it — so nothing that fails here has touched the
    /// runtime. But the two callers are not in the same position afterwards.
    ///
    /// A checkpoint's whole product is the staged row: with nothing to return
    /// it fails the call, non-terminally, and the sandbox goes on running. A
    /// pause has already produced the thing the user asked for, and failing the
    /// call would hand the caller a classification that is false either way —
    /// "recoverable" tells it to put back a sandbox this node has stopped, and
    /// "terminal" tells it to destroy a pause that worked. So the pause
    /// succeeds and reports the loss in `staging_error`.
    ///
    /// Returning the message leaves that choice with the caller instead of
    /// making it here twice.
    ///
    /// 🔴 The publish metadata is built from *this node's* record and not from
    /// anything on the wire. Every field of it but the alias is a fact about
    /// this machine — the kernel it booted, the Firecracker it ran, the images
    /// it resolved — and this node's record is the one that saw the capture
    /// happen. The alias is the single field staging never reads, so it stays
    /// the committer's to apply.
    async fn stage_for_caller(
        &self,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
        metadata: &SandboxMetadata,
        captured: crate::sandbox::CapturedSandboxSnapshot,
    ) -> Result<pb::StagedSnapshot, String> {
        let staged = self
            .snapshots
            .stage_captured(
                crate::orchestrator::capture_publish_metadata(metadata, None),
                captured,
                // 🔴 The incarnation this call was fenced on, so the row records
                // which run of the sandbox it is a snapshot of. Fencing already
                // refused a caller naming a superseded run; this is what a later
                // reader consults once the caller is long gone.
                Some(execution_id),
            )
            .await
            .map_err(|err| format!("stage sandbox {sandbox_id}'s capture: {err}"))?
            // 🔴 Drops the local half here, which releases the capture's
            // temporary directory. Nothing below reads a file: the local half's
            // one consumer is the P2P advertisement, and that belongs to the
            // process that commits, which is not this one.
            .into_staged();

        // 🔴 The bytes are staged by now and this reply was the only thing that
        // would have told anyone about them. Reported like any other staging
        // failure: what is lost is the snapshot, never the sandbox.
        let value = crate::proto::node::encode_value(&staged).map_err(|err| {
            format!(
                "encode the staged snapshot for sandbox {sandbox_id}: {err} (its bytes are staged \
                 on this node and nothing will announce them)"
            )
        })?;

        // 🔴 An `info!` and not a `debug!`, and it names the id. These bytes
        // are durable from this instant and nothing on this node will ever
        // mention them again: a caller that dies before committing leaves them
        // with no row, and no read path resolves an unannounced snapshot, so
        // nothing finds them. This line is the only record that they exist, and
        // an operator reconciling `snapshots/` against the catalog has nothing
        // else to reconcile it *from*.
        info!(
            %sandbox_id,
            snapshot_id = %staged.commit.id,
            "staged a snapshot for its caller to announce"
        );

        Ok(pb::StagedSnapshot { value: Some(value) })
    }

    /// [`NodeSandboxService::build_template`]'s body, once wiring has been
    /// confirmed present.
    ///
    /// # 🔴 What this mirrors, and what it does not
    ///
    /// The "resolve the base, execute, stage" shape is exactly
    /// `run_the_build` -> `TemplateBuilder::execute_and_publish`'s local path
    /// in `src/api/impls/template.rs`, moved here because resolving an image
    /// needs `regctl` and `--role api` has none. It stops one step short of
    /// that path, at `SnapshotManager::stage` rather than `publish`: a build
    /// run for `--role api` has no catalog row of its own to write into —
    /// only the caller's `try_start_build` row does, and only the caller can
    /// write it. What crosses back is therefore a `StagedSnapshot` for the
    /// caller to commit, exactly like `stage_for_caller` above.
    async fn build_template_impl(
        &self,
        wiring: &TemplateBuildWiring,
        request: pb::TemplateBuildRequest,
    ) -> Result<pb::StagedSnapshot, Status> {
        let build_snapshot_id = SnapshotId::parse(&request.build_snapshot_id).map_err(|err| {
            Status::invalid_argument(format!(
                "build_snapshot_id {:?}: {err}",
                request.build_snapshot_id
            ))
        })?;

        let resources = request
            .resources
            .ok_or_else(|| Status::invalid_argument("resources is required"))?;
        if resources.cpu_count == 0 || resources.memory_mib == 0 {
            return Err(Status::invalid_argument(
                "resources.cpu_count and resources.memory_mib must be greater than 0",
            ));
        }

        let steps: Vec<TemplateBuildStep> =
            convert::serialized(request.steps.as_ref(), "steps")?.unwrap_or_default();

        let mut spec = TemplateBuildSpec::new()
            .resources(resources.cpu_count, resources.memory_mib)
            .with_steps(steps);
        if !request.start_cmd.is_empty() {
            spec = spec.start_cmd(request.start_cmd.clone());
        }
        if !request.ready_cmd.is_empty() {
            spec = spec.ready_cmd(request.ready_cmd.clone());
        }

        let base_snapshot = match request.base {
            Some(pb::template_build_request::Base::Image(image)) => {
                let image_ref = (!image.image_ref.is_empty()).then_some(image.image_ref.as_str());
                let resolved = resolve_build_image(&wiring.image_resolver, image_ref).await?;
                spec = apply_image_base(spec, resolved);
                None
            }
            Some(pb::template_build_request::Base::BaseSnapshotRef(base_ref)) => {
                if base_ref.is_empty() {
                    return Err(Status::invalid_argument("base_snapshot_ref is required"));
                }
                // 🔴 `base_snapshot_resolved`, when the API half sent one, in
                // place of this node's own catalog lookup — same contract as
                // `SnapshotSource.resolved_record` in `create` above; see
                // that field's doc in node.proto.
                let resolved: Option<SnapshotRecord> = convert::serialized(
                    request.base_snapshot_resolved.as_ref(),
                    "base_snapshot_resolved",
                )?;
                let runnable = match resolved {
                    Some(record) => {
                        self.snapshots
                            .resolve_runnable(record)
                            .await
                            .map_err(|err| {
                                Status::internal(format!(
                                    "resolve base template {base_ref}: {err:#}"
                                ))
                            })?
                    }
                    None => self
                        .snapshots
                        .load_runnable(&base_ref)
                        .await
                        .map_err(|err| {
                            Status::internal(format!("resolve base template {base_ref}: {err:#}"))
                        })?
                        .ok_or_else(|| {
                            Status::not_found(format!("base template {base_ref} not found"))
                        })?,
                };
                Some(runnable)
            }
            None => return Err(Status::invalid_argument("base is required")),
        };

        let context = wiring
            .template_builder
            .prepare_remote_context(&spec, build_snapshot_id.clone(), base_snapshot.as_ref())
            .map_err(prepare_context_status)?;

        // 🔴 Synchronous and blocking, matching `execute_and_publish`'s own
        // call to it exactly — see that function in `src/template/builder.rs`.
        // `execute` spawns its own OS thread and joins it, so this does hold
        // the async task (and the worker thread under it) for as long as the
        // build sandbox runs; nothing about the split changes that trade-off,
        // and `TemplateBuildRunner::execute`'s own code is untouched here.
        let build_execution = TemplateBuildRunner::new()
            .execute(&context)
            .map_err(|err| execute_status(&err))?;

        let (metadata, manifest) = template_build_publish_metadata(
            context.build_snapshot_id.clone(),
            context.resources,
            context.virtualization_mode,
            build_execution,
        );

        // 🔴 `stage`, not `stage_captured`: this build produced a manifest
        // directly, not a `CapturedSandboxSnapshot` — see `SnapshotManager::stage`'s
        // own doc, "for artifacts that were built rather than captured".
        // `execution_id: None`, matching the local publish path
        // (`TemplateBuilder::execute_and_publish` calls `publish` ->
        // `stage(metadata, manifest, None)`): a template build fences on
        // nothing, because there is no sandbox run for a later caller to name.
        let staged = self
            .snapshots
            .stage(metadata, manifest, None)
            .await
            .map_err(|err| {
                Status::internal(format!("stage template build {build_snapshot_id}: {err:#}"))
            })?
            // 🔴 Drops the local half here, same as `stage_for_caller` above:
            // by the time `stage` returns, `import_built_artifacts` has
            // already copied the build's artifacts into this node's durable
            // repository storage, so `context`'s temporary workspace (which
            // goes out of scope at the end of this function) has nothing left
            // that matters.
            .into_staged();

        let value = crate::proto::node::encode_value(&staged).map_err(|err| {
            Status::internal(format!(
                "encode the staged template build {build_snapshot_id}: {err} (its bytes are \
                 staged on this node and nothing will announce them)"
            ))
        })?;

        info!(
            snapshot_id = %staged.commit.id,
            "staged a template build for its caller to announce"
        );

        Ok(pb::StagedSnapshot { value: Some(value) })
    }

    /// The live facts for everything this node is running, or `None` when the
    /// read did not work.
    ///
    /// The two facts a record cannot supply — the address the sandbox reaches
    /// the host on, and the size the rootfs turned out to be — are only
    /// knowable from the live handle, and this is the one call that reaches
    /// one.
    ///
    /// It costs a scan of everything running on this node. That is a real cost
    /// and it is the right trade here: the calls it is inside have just booted
    /// virtual machines, and the alternative is a second accessor on the
    /// orchestration surface that exists to serve two fields.
    ///
    /// 🔴 `.ok()` and not `?`: an operation that *succeeded* must not be
    /// reported as a failure because the follow-up read did not work. The
    /// sandbox is running either way, and a caller told it failed would leak
    /// it.
    async fn live_facts(&self) -> Option<Vec<LiveSandbox>> {
        self.orchestration.list_live_sandboxes().await.ok()
    }

    /// One sandbox's live facts out of a scan.
    fn facts_for(live: Option<&Vec<LiveSandbox>>, sandbox_id: SandboxId) -> Option<&LiveSandbox> {
        live?
            .iter()
            .find(|candidate| candidate.sandbox_id == sandbox_id)
    }

    /// What a caller learns about a sandbox this node has just brought up.
    ///
    /// 🔴 One renderer for `create`, `resume` **and** each of `fork`'s
    /// children, because the three answer the same question — *what is running
    /// now, and under which run* — and a second copy of it is a second place
    /// for a field to be forgotten. `fork` had that second copy, and it had
    /// forgotten both of the fields only the live handle can supply: it sent
    /// an empty address and a zero rootfs size for children whose VMs were up
    /// and addressable. The API half reads the address to publish the child's
    /// proxy route, so every fork it drove failed with *missing host
    /// interaction IP after start* — deterministically, on a child that was
    /// running fine.
    fn started_sandbox(
        metadata: &SandboxMetadata,
        facts: Option<&LiveSandbox>,
    ) -> pb::SandboxCreateResponse {
        pb::SandboxCreateResponse {
            sandbox_id: metadata.id.to_string(),
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
            // 🔴 Left unset here rather than encoded: `started_sandbox` is
            // also `fork`'s renderer, and a fork child's context and image
            // configs are already known to the caller from the parent it
            // forked — nothing here needs patching. `create` is the one
            // caller that sets these two fields itself, after this call
            // returns; see there.
            context: None,
            image_configs: None,
        }
    }

    /// [`started_sandbox`](Self::started_sandbox) for a single sandbox, with
    /// the scan it needs.
    async fn running_sandbox(&self, metadata: &SandboxMetadata) -> pb::SandboxCreateResponse {
        let live = self.live_facts().await;
        Self::started_sandbox(metadata, Self::facts_for(live.as_ref(), metadata.id))
    }

    /// `create`'s `Source::Image` arm: resolves the reference and its
    /// attached drives node-side, into the same `SandboxLaunchSource::Image`
    /// a local cold create already builds from a `regctl`-resolved path. See
    /// the note on `ImageSource` in `node.proto` for why the reference
    /// crosses this call unresolved rather than already turned into one.
    async fn resolve_image_source(
        &self,
        image: pb::ImageSource,
    ) -> Result<SandboxLaunchSource, Status> {
        let Some(wiring) = self.template_build.as_ref() else {
            // 🔴 Mirrors `build_template`'s own refusal when nobody called
            // `with_template_build`: production always does (see
            // `node_server::server`), so this is a test-fixture-only path in
            // practice, not a production one.
            return Err(Status::unimplemented(
                "this node was not wired to resolve images \
                 (NodeSandboxService::with_template_build was not called), so a cold create from \
                 an image reference cannot be served here",
            ));
        };
        if image.image_ref.is_empty() {
            return Err(Status::invalid_argument("image.image_ref is required"));
        }
        let requested = image
            .resources
            .ok_or_else(|| Status::invalid_argument("image.resources is required"))?;
        if requested.cpu_count == 0 || requested.memory_mib == 0 {
            return Err(Status::invalid_argument(
                "image.resources.cpu_count and image.resources.memory_mib must be greater than 0",
            ));
        }
        let resources = SandboxResources {
            cpu_count: requested.cpu_count,
            memory_mib: requested.memory_mib,
            disk_size_mib: requested.disk_size_mib,
        };

        let resolved_rootfs =
            resolve_build_image(&wiring.image_resolver, Some(image.image_ref.as_str())).await?;

        let mut image_configs = ImageConfigs::new();
        if let Some(config) = &resolved_rootfs.raw_config {
            image_configs.add(None::<String>, "/", config.clone());
        }

        let mut extra_drives = Vec::with_capacity(image.attached_drives.len());
        for drive in &image.attached_drives {
            if drive.image_ref.is_empty() {
                return Err(Status::invalid_argument(format!(
                    "attached drive '{}' has no image_ref",
                    drive.drive_id
                )));
            }
            let resolved_drive =
                resolve_build_image(&wiring.image_resolver, Some(drive.image_ref.as_str())).await?;
            let sub_path = (!drive.sub_path.is_empty()).then(|| PathBuf::from(&drive.sub_path));
            let mut extra_drive = ExtraDrive::try_new_overlaybd_with_mount_path(
                drive.drive_id.clone(),
                resolved_drive.overlaybd_config_path,
                drive.read_only,
                PathBuf::from(&drive.mount_path),
                sub_path,
            )
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
            if drive.virtual_size_bytes > 0 {
                extra_drive = extra_drive
                    .try_with_virtual_size(drive.virtual_size_bytes)
                    .map_err(|err| Status::invalid_argument(err.to_string()))?;
            }
            if let Some(config) = &resolved_drive.raw_config {
                image_configs.add(
                    Some(extra_drive.drive_id().to_string()),
                    extra_drive.mount_path().display().to_string(),
                    config.clone(),
                );
            }
            extra_drives.push(extra_drive);
        }

        let base = resolved_rootfs.base_context;
        let context = CommandContext::from_env_and_workdir(base.env_vars, base.workdir)
            .with_user(base.user)
            .with_exposed_ports(base.exposed_ports)
            .with_entrypoint(base.entrypoint)
            .with_cmd(base.cmd)
            .with_volumes(base.volumes)
            .with_labels(base.labels);

        Ok(SandboxLaunchSource::Image {
            image_ref: resolved_rootfs.image_ref,
            overlaybd_config_path: resolved_rootfs.overlaybd_config_path,
            context: Box::new(context),
            resources: Some(resources),
            extra_drives,
            extra_boot_args: (!image.extra_boot_args.is_empty()).then_some(image.extra_boot_args),
            image_configs: Box::new(image_configs),
        })
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
fn capture_op_failure_status(
    sandbox_id: SandboxId,
    operation: SandboxOperation,
    err: &OrchestratorError,
) -> Status {
    let status = orchestrator_status(err);
    let terminal = match err {
        // The backend answered, and its answer already says which of the two
        // this is — the orchestrator acted on the same flag when it decided
        // whether to put the sandbox back or tear it down.
        //
        // 🔴 The operation is matched, not ignored. A sandbox can only be
        // inside one lifecycle operation at a time, so a `SandboxOperationFailed`
        // naming a *different* one than the call being served is not this
        // call's capture answering: it is some other failure arriving through
        // the same shape, and reading its classification would be reading a
        // verdict about a runtime this call never touched. That falls through
        // to the fail-closed default below.
        //
        // 🔴 `unwrap_or(true)` and not `false`: a source that is not a capture
        // error is a failure this node cannot account for, and the fail-closed
        // reading of "I do not know what happened to the runtime" is that it
        // did not survive.
        OrchestratorError::SandboxOperationFailed {
            operation: failed,
            source,
            ..
        } if *failed == operation => source
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
        // running. A capture never reaches the persister at all.
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

/// Resolves a template build's image base, node-side.
///
/// 🔴 Mirrors `resolve_template_rootfs_image` in
/// `src/api/impls/template.rs` rather than sharing code with it: that
/// function speaks `models::Error`, an HTTP type this gRPC surface has no
/// business depending on, and the part that is not the four-line translation
/// of an `ImageResolutionError` into a status — `ImageResolver::resolve`
/// itself — is already shared, being the one and only implementation either
/// caller drives.
async fn resolve_build_image(
    image_resolver: &ImageResolver,
    image_ref: Option<&str>,
) -> Result<crate::image::ResolvedBlockImage, Status> {
    let image_ref = image_ref.unwrap_or_else(|| image_resolver.default_image());
    image_resolver.resolve(image_ref).await.map_err(|err| {
        let code = if err.is_user_error() {
            tonic::Code::InvalidArgument
        } else {
            tonic::Code::Internal
        };
        Status::new(code, format!("resolve image {image_ref}: {err}"))
    })
}

/// Applies a resolved image base to a `TemplateBuildSpec`, matching the
/// `raw_config` -> `ImageConfigs` and `base_context` -> `CommandContext`
/// translation `resolve_template_rootfs_image`'s caller performs today in
/// `src/api/impls/template.rs`'s `run_the_build`.
fn apply_image_base(
    spec: TemplateBuildSpec,
    resolved: crate::image::ResolvedBlockImage,
) -> TemplateBuildSpec {
    let mut image_configs = ImageConfigs::new();
    if let Some(config) = &resolved.raw_config {
        image_configs.add(None::<String>, "/", config.clone());
    }
    let base = resolved.base_context;
    let base_context = CommandContext::from_env_and_workdir(base.env_vars, base.workdir)
        .with_user(base.user)
        .with_exposed_ports(base.exposed_ports)
        .with_entrypoint(base.entrypoint)
        .with_cmd(base.cmd)
        .with_volumes(base.volumes)
        .with_labels(base.labels);
    spec.with_resolved_overlaybd_image(resolved.overlaybd_config_path, image_configs)
        .with_base_context(base_context)
}

/// Splits a finished build into the two things `SnapshotManager::stage` wants
/// — the metadata describing it, and the manifest naming its artifacts — and
/// derives `resources.disk_size_mib` from the manifest the way
/// `TemplateBuilder::execute_and_publish` does for the local build path.
///
/// 🔴 Pure and free of `self` on purpose: `TemplateBuildRunner::execute`
/// (which produces the `TemplateBuildExecution` this consumes) boots a real
/// Firecracker VM and cannot run inside `cargo test --lib`, but everything
/// downstream of it — this function, `SnapshotManager::stage`, encoding for
/// the wire, and `commit_staged` on the other end — is ordinary Rust that
/// can. See `a_built_templates_metadata_survives_stage_encode_decode_and_commit`
/// in `tests.rs`, which drives exactly that: a hand-built
/// `TemplateBuildExecution` through this function and a real
/// (PosixFs-backed) `SnapshotManager`.
///
/// 🔴 `alias: None`, deliberately — see `TemplateBuildRequest`'s doc in
/// `node.proto`. Staging never reads it: the caller's `adopt_staged`
/// overwrites `staged.commit.alias` unconditionally once this row is
/// committed, so a value written here would never be read back.
pub(super) fn template_build_publish_metadata(
    build_snapshot_id: SnapshotId,
    resources: crate::types::SandboxResources,
    virtualization_mode: crate::virtualization::VirtualizationMode,
    build_execution: crate::template::TemplateBuildExecution,
) -> (
    SnapshotPublishMetadata,
    crate::sandbox::FirecrackerSnapshotManifest,
) {
    let mut resources = resources;
    resources.disk_size_mib = build_execution
        .manifest
        .rootfs
        .virtual_size
        .div_ceil(1 << 20)
        .try_into()
        .unwrap_or(u32::MAX);

    let metadata = SnapshotPublishMetadata {
        id: build_snapshot_id,
        alias: None,
        source: SnapshotPublishSource::Template,
        context: build_execution.build_context,
        startup: build_execution.startup,
        resources,
        runtime_versions: build_execution.runtime_versions,
        virtualization_mode,
        image_configs: build_execution.image_configs,
        custom_extension_params: None,
    };
    (metadata, build_execution.manifest)
}

/// Maps a `TemplateBuildContext` preparation failure — the id colliding with
/// its base, a virtualization mode mismatch, resources changing under a
/// snapshot base, or a local I/O failure creating the build's workspace — onto
/// a status. Never step-scoped: `TemplateBuildFailure`'s `step` field is only
/// ever attached by `step_executor.rs`, once a build sandbox is actually
/// running, which none of these failures reach.
fn prepare_context_status(err: crate::template::TemplateBuildError) -> Status {
    match err {
        crate::template::TemplateBuildError::InvalidInput { reason } => {
            crate::proto::node::build_failure_status(tonic::Code::InvalidArgument, reason, None)
        }
        crate::template::TemplateBuildError::System { reason, .. } => {
            crate::proto::node::build_failure_status(
                tonic::Code::Internal,
                reason.message,
                reason.step.as_deref(),
            )
        }
    }
}

/// Maps a `TemplateBuildRunner::execute` failure onto a status, preserving
/// which build step it failed on the same way `TemplateBuilder::execute_and_publish`
/// preserves it locally — by unwrapping the `TemplateBuildFailure` `execute`'s
/// error chain carries, when there is one.
fn execute_status(err: &anyhow::Error) -> Status {
    let reason = TemplateBuilder::build_failure_reason(err);
    crate::proto::node::build_failure_status(
        tonic::Code::Internal,
        reason.message,
        reason.step.as_deref(),
    )
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
                // 🔴 `resolved_record`, when the API half sent one, in place
                // of this node's own catalog lookup — see
                // `SnapshotSource.resolved_record`'s own doc in node.proto
                // and Q3 in `_sd-phase4-open-questions-resolved.md`. Absent
                // (an API replica built before this field existed) falls
                // back to `load_runnable`, the pre-existing path, unchanged.
                let resolved: Option<SnapshotRecord> =
                    convert::serialized(snapshot.resolved_record.as_ref(), "resolved_record")?;
                let runnable = match resolved {
                    Some(record) => {
                        if record.id.to_string() != snapshot.snapshot_id {
                            return Err(Status::invalid_argument(format!(
                                "resolved_record names snapshot {}, not snapshot_id {}",
                                record.id, snapshot.snapshot_id
                            )));
                        }
                        self.snapshots
                            .resolve_runnable(record)
                            .await
                            .map_err(|err| {
                                Status::internal(format!(
                                    "resolve snapshot {}: {err:#}",
                                    snapshot.snapshot_id
                                ))
                            })?
                    }
                    None => self
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
                            Status::not_found(format!(
                                "snapshot {} not found",
                                snapshot.snapshot_id
                            ))
                        })?,
                };
                SandboxLaunchSource::Snapshot(Box::new(runnable))
            }
            Some(pb::sandbox_create_request::Source::Image(image)) => {
                self.resolve_image_source(image).await?
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

        let mut response = self.running_sandbox(&metadata).await;
        // 🔴 Sent unconditionally rather than only for a `Source::Image`
        // create: this process's own record of the sandbox is the same
        // `metadata` either way, and a `Source::Snapshot` caller that already
        // knew both values reads back exactly what it sent. See the note on
        // these two fields in `node.proto`.
        response.context = Some(
            crate::proto::node::encode_value(&metadata.context)
                .map_err(|err| Status::internal(format!("encode resolved context: {err}")))?,
        );
        response.image_configs = Some(
            crate::proto::node::encode_value(&metadata.image_configs)
                .map_err(|err| Status::internal(format!("encode resolved image configs: {err}")))?,
        );
        Ok(Response::new(response))
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
    /// [`capture_op_failure_status`].
    ///
    /// # 🔴 What `publish` changes, and what it does not
    ///
    /// Without it the pause is this node's business: the bytes stay here, the
    /// caller gets a path and a handle, and the sandbox is reopenable on this
    /// machine and nowhere else. With it the capture is *also* staged into the
    /// snapshot repository and the row comes back for the caller to commit, so
    /// the sandbox survives losing this machine.
    ///
    /// It does not change the pause. Staging happens after the sandbox is
    /// paused, persisted and stopped, and a staging that fails leaves all three
    /// of those standing — so the reply says so and the sandbox stays paused
    /// here rather than being torn down for a failure that never touched it.
    ///
    /// 🔴 An absent `staged` in a reply that was *asked* to publish is not a
    /// silent gap and never becomes one: the pause path only reaches this call
    /// without a capture when the pause did not happen on this call — the
    /// sandbox was already paused, or this call joined one in flight — and in
    /// both the capture belongs to the pause that produced it and is gone. Any
    /// other reason fails the call.
    ///
    /// 🔴 This node's own publisher is not consulted on this arm. On `--role
    /// node` it is `DisabledPausedSandboxRegistry` and would drop the capture;
    /// on any role it would be a second process writing a row for a pause the
    /// caller already owns.
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
        self.fenced(sandbox_id, execution_id)
            .await
            .map_err(untouched)?;

        // 🔴 Two entry points and not one with a flag, because they differ in
        // who the capture is offered to and not merely in what comes back.
        // The unpublished arm is left byte-for-byte the call it has always
        // been: `pause_sandbox`, which offers the capture to this process's own
        // publisher exactly as a machine-local role does.
        let (metadata, staged, staging_error) = if request.publish {
            let outcome = Arc::clone(&self.orchestration)
                .pause_sandbox_for_publication(sandbox_id)
                .await
                .map_err(|err| {
                    capture_op_failure_status(sandbox_id, SandboxOperation::Pause, &err)
                })?;
            let (staged, staging_error) = match outcome.publishable {
                // 🔴 A staging that fails does not fail the pause, and this is
                // the only place on this service where a failure is reported
                // inside a success. The sandbox is paused, persisted and
                // stopped by now; the two answers a failed pause can carry are
                // "put it back to running" and "tear it down", and both are
                // lies about a sandbox that is sitting here paused and
                // perfectly reopenable. What is lost is the cluster's copy, so
                // that is what the reply says was lost. See
                // `SandboxPauseResponse.staging_error`.
                Some(publishable) => match self
                    .stage_for_caller(sandbox_id, execution_id, &outcome.metadata, publishable)
                    .await
                {
                    Ok(staged) => (Some(staged), String::new()),
                    Err(err) => {
                        warn!(
                            %sandbox_id,
                            error = %err,
                            "paused a sandbox and could not stage its capture; it is resumable on \
                             this node only"
                        );
                        (None, err)
                    }
                },
                // The pause was idempotent: nothing happened on this call, so
                // there is no capture of it. See the note on the flag above.
                None => {
                    debug!(
                        %sandbox_id,
                        "pause produced no publishable capture; the sandbox was already paused here"
                    );
                    (None, String::new())
                }
            };
            (outcome.metadata, staged, staging_error)
        } else {
            let metadata = Arc::clone(&self.orchestration)
                .pause_sandbox(sandbox_id)
                .await
                .map_err(|err| {
                    capture_op_failure_status(sandbox_id, SandboxOperation::Pause, &err)
                })?;
            (metadata, None, String::new())
        };

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
            .map_err(|err| capture_op_failure_status(sandbox_id, SandboxOperation::Pause, &err))?;
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
            // 🔴 Absent unless `publish` was asked for, and then absent for
            // exactly two reasons — the idempotent pause above, and a staging
            // that failed. `staging_error` is what tells them apart.
            staged,
            staging_error,
        }))
    }

    /// Captures a snapshot of a running sandbox and writes its bytes, leaving
    /// the row for the caller to announce.
    ///
    /// # 🔴 Why the reply is a staged row and not a capture
    ///
    /// A `CapturedSandboxSnapshot` owns a temporary directory on this machine
    /// and cannot cross a process boundary, so what crosses is what is left
    /// once the bytes are durable: a [`StagedSnapshot`], which is a pure value
    /// carrying everything the catalog row needs and nothing that points at
    /// this disk. The caller commits it. Until it does, no reader anywhere can
    /// resolve this snapshot.
    ///
    /// # 🔴 Why this node picks the snapshot id, and the caller picks the name
    ///
    /// Staging writes the bytes into the directory the id names. The id is
    /// therefore a decision only the machine writing them can take — it is the
    /// one that finds out whether the write worked. Everything else on the row
    /// except the alias is a fact about *this* machine (its kernel, its
    /// Firecracker, the images it resolved) and is read off this node's own
    /// record of the sandbox, which is the record that saw the capture happen.
    /// The alias is the single field staging never looks at, so it stays the
    /// caller's, applied when the row is committed.
    ///
    /// # 🔴 What is left behind when the caller never commits
    ///
    /// Staged bytes. They are durable, they are reachable by id, and no row
    /// points at them — the same residue a `publish` whose commit failed leaves
    /// behind, and this build reclaims neither. The window is one round trip
    /// wide and each loss costs one capture's worth of storage. Nothing else
    /// breaks: an unannounced snapshot is invisible to every read path, so it
    /// cannot be resolved, launched, or mistaken for a snapshot that works.
    async fn checkpoint(
        &self,
        request: Request<pb::SandboxCheckpointRequest>,
    ) -> Result<Response<pb::SandboxCheckpointResponse>, Status> {
        let request = request.into_inner();
        // 🔴 Classified, for the reason spelled out on `pause`: the caller
        // reads an unclassified capture failure as terminal, so a malformed
        // request that never reached the runtime must say it never reached it.
        let sandbox_id = convert::sandbox_id(&request.sandbox_id).map_err(untouched)?;
        let execution_id = convert::execution_id(&request.execution_id).map_err(untouched)?;
        self.fenced(sandbox_id, execution_id)
            .await
            .map_err(untouched)?;

        let capture = Arc::clone(&self.orchestration)
            .capture_snapshot(sandbox_id)
            .await
            .map_err(|err| {
                capture_op_failure_status(sandbox_id, SandboxOperation::Snapshot, &err)
            })?;

        // 🔴 Non-terminal, and `capture_snapshot` is what makes that true rather
        // than optimism: it has already put the sandbox back to `Running` by
        // the time it hands over a capture. A terminal answer here would have
        // the caller tear down a sandbox that is running and serving requests
        // because a disk filled up.
        let staged = self
            .stage_for_caller(
                sandbox_id,
                execution_id,
                &capture.metadata,
                capture.captured_snapshot,
            )
            .await
            .map_err(|err| {
                crate::proto::node::capture_failure_status(
                    tonic::Code::Internal,
                    err,
                    false,
                    "the sandbox is still running on this node",
                )
            })?;

        Ok(Response::new(pb::SandboxCheckpointResponse {
            staged: Some(staged),
        }))
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

        // 🔴 One scan for the whole fork, not one per child. Every child that
        // started is already registered and unlocked by the time `fork_sandbox`
        // returns, so a single pass sees all of them — and asking once per
        // child would re-read every sandbox on this node once per child.
        let live = self.live_facts().await;

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
                    // 🔴 The same renderer `create` and `resume` answer with.
                    // A child's address and rootfs size are *its own* — it was
                    // given its own network slot — so they are read from that
                    // child's live handle rather than blanked or copied from
                    // the source.
                    outcome: Some(pb::fork_child_result::Outcome::Started(
                        Self::started_sandbox(
                            &metadata,
                            Self::facts_for(live.as_ref(), metadata.id),
                        ),
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
        let params: Option<CustomExtensionParams> = convert::serialized(
            request.custom_extension_params.as_ref(),
            "custom_extension_params",
        )?;

        Arc::clone(&self.orchestration)
            .replace_sandbox_custom_extension_params(sandbox_id, params)
            .await
            .map_err(|err| orchestrator_status(&err))?;

        Ok(Response::new(pb::SandboxParamsResponse {}))
    }

    /// What this node is running under one sandbox id.
    ///
    /// # 🔴 Why this is not `list_sandboxes` with a filter
    ///
    /// The listing exists to be reconciled against, so it reports only the
    /// sandboxes carrying the control plane's ownership marker — an unmarked
    /// sandbox is nobody's and is left out entirely. A fork child started
    /// through the API half is unmarked (`ForkChildSpec::control_plane_config`
    /// arrives empty), so filtering the listing by id would answer `NOT_FOUND`
    /// for precisely the sandboxes a caller most needs this for.
    ///
    /// This call answers a different question, and the difference is who is
    /// asking: a caller that already holds the record for an id, wanting to
    /// know what this machine is running under it. Ownership is not part of
    /// that question.
    ///
    /// # 🔴 A read that fails must fail
    ///
    /// [`live_facts`](Self::live_facts) swallows the error with `.ok()`,
    /// because there the read is a follow-up to a VM that has *already*
    /// booted, and reporting a successful create as a failure would leak it.
    /// Here the read is the whole call: an answer that could not look must not
    /// come back shaped like one that looked and found nothing.
    async fn describe(
        &self,
        request: Request<pb::SandboxDescribeRequest>,
    ) -> Result<Response<pb::SandboxDescribeResponse>, Status> {
        let request = request.into_inner();
        let sandbox_id = convert::sandbox_id(&request.sandbox_id)?;

        let live = self
            .orchestration
            .list_live_sandboxes()
            .await
            .map_err(|err| orchestrator_status(&err))?;

        // 🔴 `NOT_FOUND` is an answer and it is this one: the node looked at
        // what it is running and there is nothing under that id. It is not the
        // answer for a node that could not look — that left above.
        let sandbox = live
            .iter()
            .find(|candidate| candidate.sandbox_id == sandbox_id)
            .ok_or_else(|| {
                Status::not_found(format!("sandbox {sandbox_id} is not running on this node"))
            })?;

        Ok(Response::new(pb::SandboxDescribeResponse {
            execution_id: sandbox
                .execution_id
                .map(|execution_id| execution_id.to_string())
                .unwrap_or_default(),
            host_interaction_ip: sandbox
                .host_interaction_ip
                .map(|ip| ip.to_string())
                .unwrap_or_default(),
            rootfs_virtual_size: sandbox.rootfs_virtual_size.unwrap_or_default(),
            // 🔴 Reported rather than turned into a `NOT_FOUND`. The sandbox is
            // running; what the node could not do is read its live facts right
            // now. Those are different, and the caller acts on the difference.
            facts_from_handle: sandbox.facts_from_handle,
        }))
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

    async fn build_template(
        &self,
        request: Request<pb::TemplateBuildRequest>,
    ) -> Result<Response<pb::TemplateBuildResponse>, Status> {
        let Some(wiring) = self.template_build.as_ref() else {
            return Err(Status::unimplemented(
                "this node was not wired to build templates \
                 (NodeSandboxService::with_template_build was not called)",
            ));
        };
        let staged = self
            .build_template_impl(wiring, request.into_inner())
            .await?;
        Ok(Response::new(pb::TemplateBuildResponse {
            staged: Some(staged),
        }))
    }
}
