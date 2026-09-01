//! Node-side implementation of the control-plane gRPC contract.
//!
//! Only identifiers and durable facts cross the boundary. Lookup failures are
//! errors, never absence, because reconciliation may delete on `NotFound`.

use std::path::PathBuf;
use std::sync::Arc;

use tonic::{Request, Response, Status};
use tracing::{debug, info, warn};

use crate::image::ImageResolver;
use crate::orchestrator::{
    ClaimedExecution, CreateSandboxRequest, ForkChildAssignment, ForkChildren, LiveSandbox,
    NewTimeout, OrchestratorError, SandboxLaunchSource, SandboxMetadata, SandboxOperation,
    SandboxOrchestration, SandboxPersistenceError,
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

/// Serves node sandbox RPCs from one orchestrator.
pub struct NodeSandboxService {
    orchestration: Arc<dyn SandboxOrchestration>,
    // Snapshot resolution remains node-local because it opens local artifacts.
    snapshots: Arc<SnapshotManager>,
    node_id: String,
    // Optional only for narrow tests; production wires template builds and images.
    template_build: Option<TemplateBuildWiring>,
}

// Image and template-build capabilities shared by their RPC handlers.
struct TemplateBuildWiring {
    image_resolver: Arc<ImageResolver>,
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

    /// Enables template-build and image-source RPCs.
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

    // Refuse a superseded execution ID; preserve not-found as a distinct result.
    async fn fenced(&self, sandbox_id: SandboxId, claimed: ExecutionId) -> Result<(), Status> {
        if let Some(live) = self.orchestration.live_execution_id(&sandbox_id).await {
            return if live == claimed {
                Ok(())
            } else {
                Err(superseded(sandbox_id, claimed, live))
            };
        }

        // Store errors are not absence and must propagate.
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

    // Stages durable bytes and returns the row the caller must announce.
    // Checkpoint propagates staging failure; pause records it after runtime state
    // has already settled. Publish metadata always comes from this node's record.
    async fn stage_for_caller(
        &self,
        sandbox_id: SandboxId,
        metadata: &SandboxMetadata,
        captured: crate::sandbox::CapturedSandboxSnapshot,
    ) -> Result<pb::StagedSnapshot, String> {
        let staged = self
            .snapshots
            .stage_captured(
                crate::orchestrator::capture_publish_metadata(metadata, None),
                captured,
            )
            .await
            .map_err(|err| format!("stage sandbox {sandbox_id}'s capture: {err}"))?
            // Drop only the temporary local half after durable staging.
            .into_staged();

        // Encoding failure loses the announcement, never the sandbox.
        let value = crate::proto::node::encode_value(&staged).map_err(|err| {
            format!(
                "encode the staged snapshot for sandbox {sandbox_id}: {err} (its bytes are staged \
                 on this node and nothing will announce them)"
            )
        })?;

        // This is the only durable log linking unannounced bytes to their ID.
        info!(
            %sandbox_id,
            snapshot_id = %staged.commit.id,
            "staged a snapshot for its caller to announce"
        );

        Ok(pb::StagedSnapshot { value: Some(value) })
    }

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
                // The node has no catalog; callers must supply the resolved record.
                let record: SnapshotRecord = convert::serialized(
                    request.base_snapshot_resolved.as_ref(),
                    "base_snapshot_resolved",
                )?
                .ok_or_else(|| {
                    Status::invalid_argument(
                        "base_snapshot_resolved is required: this node holds no snapshot \
                         catalog to resolve base_snapshot_ref against",
                    )
                })?;
                if !record_names(&record, &base_ref) {
                    return Err(Status::invalid_argument(format!(
                        "base_snapshot_resolved names snapshot {}{}, not \
                         base_snapshot_ref {base_ref}",
                        record.id,
                        record
                            .alias
                            .as_ref()
                            .map(|alias| format!(" (alias {alias})"))
                            .unwrap_or_default(),
                    )));
                }
                let runnable = self
                    .snapshots
                    .resolve_runnable(record)
                    .await
                    .map_err(|err| {
                        Status::internal(format!("resolve base template {base_ref}: {err:#}"))
                    })?;
                Some(runnable)
            }
            None => return Err(Status::invalid_argument("base is required")),
        };

        let context = wiring
            .template_builder
            .prepare_remote_context(&spec, build_snapshot_id.clone(), base_snapshot.as_ref())
            .map_err(prepare_context_status)?;

        // Execution is synchronous and internally joins its worker thread.
        let build_execution = TemplateBuildRunner::new()
            .execute(&context)
            .map_err(|err| execute_status(&err))?;

        let (metadata, manifest) = template_build_publish_metadata(
            context.build_snapshot_id.clone(),
            context.resources,
            context.virtualization_mode,
            build_execution,
        );

        // Built artifacts use `stage`; there is no sandbox execution to fence.
        let staged = self
            .snapshots
            .stage(metadata, manifest)
            .await
            .map_err(|err| {
                Status::internal(format!("stage template build {build_snapshot_id}: {err:#}"))
            })?
            // Durable import completed before the temporary build context is dropped.
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

    // Best-effort follow-up facts must not turn a successful start into failure.
    async fn live_facts(&self) -> Option<Vec<LiveSandbox>> {
        self.orchestration.list_live_sandboxes().await.ok()
    }

    fn facts_for(live: Option<&Vec<LiveSandbox>>, sandbox_id: SandboxId) -> Option<&LiveSandbox> {
        live?
            .iter()
            .find(|candidate| candidate.sandbox_id == sandbox_id)
    }

    // Shared renderer for create, resume, and fork start responses.
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
            // Create fills these fields; fork already knows them from its parent.
            context: None,
            image_configs: None,
        }
    }

    async fn running_sandbox(&self, metadata: &SandboxMetadata) -> pb::SandboxCreateResponse {
        let live = self.live_facts().await;
        Self::started_sandbox(metadata, Self::facts_for(live.as_ref(), metadata.id))
    }

    // Resolves image and attached-drive references on this node.
    async fn resolve_image_source(
        &self,
        image: pb::ImageSource,
    ) -> Result<SandboxLaunchSource, Status> {
        let Some(wiring) = self.template_build.as_ref() else {
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

// Preserves status code/message while marking that runtime state was untouched.
fn untouched(status: Status) -> Status {
    crate::proto::node::capture_failure_status(
        status.code(),
        status.message().to_string(),
        false,
        "the node did not touch the sandbox",
    )
}

// Capture failures default terminal; only proven pre-runtime or recoverable
// failures may tell the caller the sandbox survived.
fn capture_op_failure_status(
    sandbox_id: SandboxId,
    operation: SandboxOperation,
    err: &OrchestratorError,
) -> Status {
    let status = orchestrator_status(err);
    let terminal = match err {
        // Use a matching capture operation's explicit classification; unknown
        // sources and mismatched operations remain terminal.
        OrchestratorError::SandboxOperationFailed {
            operation: failed,
            source,
            ..
        } if *failed == operation => source
            .downcast_ref::<SandboxCaptureError>()
            .map(SandboxCaptureError::is_terminal)
            .unwrap_or(true),
        // Refused before touching runtime state.
        OrchestratorError::InvalidSandboxState { .. }
        | OrchestratorError::SandboxOperationConflict { .. }
        | OrchestratorError::SandboxLifetimeExceeded { .. }
        | OrchestratorError::InvalidRequest(_)
        | OrchestratorError::ShuttingDown
        | OrchestratorError::NotAcceptingNewWork
        | OrchestratorError::VirtualizationModeMismatch { .. } => false,
        // Artifact allocation failure occurs before backend capture.
        OrchestratorError::SandboxPersistenceFailed(_) => false,
        // NotFound here means no sandbox remained to pause.
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

// Preserves NotFound because callers distinguish absence from node failure.
fn orchestrator_status(err: &OrchestratorError) -> Status {
    match err {
        OrchestratorError::SandboxNotFound(sandbox_id) => {
            Status::not_found(format!("sandbox {sandbox_id} is not on this node"))
        }
        // Absence, not node failure: a claim holder can rebuild a published row.
        OrchestratorError::SandboxPersistenceFailed(SandboxPersistenceError::RecordAbsent {
            sandbox_id,
        }) => Status::not_found(format!(
            "sandbox {sandbox_id} is not holding a paused capture on this node"
        )),
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

// Accept either the snapshot ID or its current alias.
fn record_names(record: &SnapshotRecord, id_or_alias: &str) -> bool {
    record.id.to_string() == id_or_alias
        || record
            .alias
            .as_ref()
            .is_some_and(|alias| alias.to_string() == id_or_alias)
}

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

/// Splits completed template-build output into publish metadata and a manifest.
///
/// Alias remains caller-owned and is applied when the staged row is committed.
pub fn template_build_publish_metadata(
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
        // Reject malformed scalar fields before any registry or local-artifact work.
        let timeout_action = convert::timeout_action(request.timeout_action)?;
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
                // Nodes have no catalog; callers must supply the resolved record.
                let record: SnapshotRecord =
                    convert::serialized(snapshot.resolved_record.as_ref(), "resolved_record")?
                        .ok_or_else(|| {
                            Status::invalid_argument(
                                "resolved_record is required: this node holds no snapshot \
                                 catalog to resolve snapshot_id against",
                            )
                        })?;
                if record.id.to_string() != snapshot.snapshot_id {
                    return Err(Status::invalid_argument(format!(
                        "resolved_record names snapshot {}, not snapshot_id {}",
                        record.id, snapshot.snapshot_id
                    )));
                }
                let runnable = self
                    .snapshots
                    .resolve_runnable(record)
                    .await
                    .map_err(|err| {
                        Status::internal(format!(
                            "resolve snapshot {}: {err:#}",
                            snapshot.snapshot_id
                        ))
                    })?;
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
            // Ownership markers are set only at create.
            control_plane_config: convert::control_plane_config(&request.control_plane_config),
            // Empty lets the orchestrator mint; nonempty adopts the caller's claim.
            execution_id: convert::optional_execution_id(&request.execution_id)?,
        };

        // Under the id the caller chose, because the caller's record of this
        // sandbox already names it.
        let metadata = Arc::clone(&self.orchestration)
            .restore_sandbox(sandbox_id, create)
            .await
            .map_err(|err| orchestrator_status(&err))?;

        let mut response = self.running_sandbox(&metadata).await;
        // Always return the node's resolved context and image configuration.
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

    /// Pauses locally and returns the node-owned capture location.
    ///
    /// Optional publication stages a row for the caller without changing pause success.
    async fn pause(
        &self,
        request: Request<pb::SandboxPauseRequest>,
    ) -> Result<Response<pb::SandboxPauseResponse>, Status> {
        let request = request.into_inner();
        // Parse and fence failures are classified as runtime-untouched.
        let sandbox_id = convert::sandbox_id(&request.sandbox_id).map_err(untouched)?;
        let execution_id = convert::execution_id(&request.execution_id).map_err(untouched)?;
        self.fenced(sandbox_id, execution_id)
            .await
            .map_err(untouched)?;

        // Published and local pauses intentionally use distinct orchestration entry points.
        let (metadata, staged, staging_error) = if request.publish {
            let outcome = Arc::clone(&self.orchestration)
                .pause_sandbox_for_publication(sandbox_id)
                .await
                .map_err(|err| {
                    capture_op_failure_status(sandbox_id, SandboxOperation::Pause, &err)
                })?;
            let (staged, staging_error) = match outcome.publishable {
                // Staging failure is reported inside a successful, locally resumable pause.
                Some(publishable) => match self
                    .stage_for_caller(sandbox_id, &outcome.metadata, publishable)
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

        // A successful pause must return the handle needed to reopen it.
        let paused_state = metadata.paused_state.as_ref().ok_or_else(|| {
            // Missing reply state is nonterminal because the node record remains resumable.
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

        // Never persist a fabricated empty artifact location after a failed read.
        let artifact_root = self
            .orchestration
            .paused_artifact_root(&sandbox_id)
            .await
            .map_err(|err| capture_op_failure_status(sandbox_id, SandboxOperation::Pause, &err))?;
        if artifact_root.is_none() {
            // Some persisters manage temporary capture storage without a named root.
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
            staged,
            staging_error,
        }))
    }

    /// Captures and stages a snapshot row for the caller to announce.
    ///
    /// Uncommitted staged bytes remain durable but invisible to read paths.
    async fn checkpoint(
        &self,
        request: Request<pb::SandboxCheckpointRequest>,
    ) -> Result<Response<pb::SandboxCheckpointResponse>, Status> {
        let request = request.into_inner();
        // Parse and fence failures never reached the runtime.
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

        // Staging failure is nonterminal because capture restored the sandbox to running.
        let staged = self
            .stage_for_caller(sandbox_id, &capture.metadata, capture.captured_snapshot)
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

    /// Resumes from this node's persisted record.
    ///
    /// NotFound remains distinct from record-read or availability failures.
    async fn resume(
        &self,
        request: Request<pb::SandboxResumeRequest>,
    ) -> Result<Response<pb::SandboxResumeResponse>, Status> {
        let request = request.into_inner();
        let sandbox_id = convert::sandbox_id(&request.sandbox_id)?;
        let paused_execution_id = convert::execution_id(&request.execution_id)?;
        // A resume must carry the execution ID granted by arbitration.
        let resumed_execution_id = convert::execution_id(&request.resumed_execution_id)?;
        if resumed_execution_id == paused_execution_id {
            // Resume always creates a new incarnation.
            return Err(Status::invalid_argument(format!(
                "sandbox {sandbox_id} cannot be resumed as the same run it was paused under \
                 ({paused_execution_id}): a resume starts a new one"
            )));
        }

        self.fenced(sandbox_id, paused_execution_id).await?;

        let timeout = match convert::optional_timeout(request.timeout_ms) {
            Some(timeout) => NewTimeout::Set(timeout),
            // Preserve the deadline stored with the paused sandbox.
            None => NewTimeout::UseExisting,
        };

        let metadata = Arc::clone(&self.orchestration)
            .resume_sandbox(
                sandbox_id,
                timeout,
                // Adopt the execution claim already recorded by the caller.
                ClaimedExecution::adopted_from_remote_claim(resumed_execution_id),
            )
            .await
            .map_err(|err| orchestrator_status(&err))?;

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

        // One live-state scan covers every completed child.
        let live = self.live_facts().await;

        // Preserve request order by zipping outcomes with assignments.
        let results = outcomes
            .into_iter()
            .zip(&children)
            .map(|(outcome, requested)| match outcome {
                Ok(metadata) => pb::ForkChildResult {
                    sandbox_id: metadata.id.to_string(),
                    execution_id: metadata.execution_id.to_string(),
                    // Render each child's own live address and rootfs facts.
                    outcome: Some(pb::fork_child_result::Outcome::Started(
                        Self::started_sandbox(
                            &metadata,
                            Self::facts_for(live.as_ref(), metadata.id),
                        ),
                    )),
                },
                Err(err) => pb::ForkChildResult {
                    // A failed child has no metadata; identify it from the request.
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

        // The caller already ran the extension hook; assign its recorded answer.
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

    // Describes any running sandbox by ID, regardless of reconciliation ownership.
    // Read failure remains distinct from a successful not-found result.
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

        // NotFound is returned only after a successful live-state read.
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
            // Report handle-read quality separately from sandbox presence.
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
            // Fail the whole listing rather than return a dangerous partial view.
            .map_err(|err| orchestrator_status(&err))?;

        let owned = owned_by_control_plane(&live);
        if owned.len() != live.len() {
            // Unowned sandboxes are expected on mixed or template-building nodes.
            debug!(
                running = live.len(),
                owned = owned.len(),
                "reporting the sandboxes the control plane owns"
            );
        }
        for sandbox in &live {
            if !sandbox.facts_from_handle {
                // Busy handles contribute record-only facts with no live address.
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

    async fn override_status(
        &self,
        request: Request<pb::NodeStatusOverrideRequest>,
    ) -> Result<Response<pb::NodeStatusOverrideResponse>, Status> {
        let disabled = request.into_inner().scheduling_disabled;
        if self.orchestration.set_scheduling_disabled(disabled) {
            info!(
                node_id = %self.node_id,
                scheduling_disabled = disabled,
                "node scheduling status overridden by the control plane"
            );
        }
        Ok(Response::new(pb::NodeStatusOverrideResponse {}))
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
