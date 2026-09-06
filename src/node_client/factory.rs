//! Building sandboxes that run somewhere else.

use std::sync::Arc;

use anyhow::{bail, Result};

use crate::proto::node as pb;
use crate::runtime_snapshot::RunnableSnapshot;
use crate::sandbox::SandboxNetworkPolicy;
use crate::sandbox::{
    FreshSandboxBuildSpec, SandboxBackend, SandboxBackendFactory, SandboxLaunchConfig,
    UnresolvedImageBuildSpec,
};
use crate::snapshot::SnapshotRecord;
use crate::types::{ExecutionId, SandboxId, SandboxResources};

use super::placement::{NodePlacement, PlacementNeeds};
use super::stub::{PendingLaunch, RemoteSandboxStub};
use super::wire;

/// Builds remote sandbox stubs without network I/O.
///
/// Unresolved image and snapshot inputs are forwarded for node-side resolution.
pub struct RemoteSandboxBackendFactory {
    placement: Arc<dyn NodePlacement>,
}

impl RemoteSandboxBackendFactory {
    pub fn new(placement: Arc<dyn NodePlacement>) -> Self {
        Self { placement }
    }
}

impl SandboxBackendFactory for RemoteSandboxBackendFactory {
    /// Marks remote sandboxes so node reconciliation can attribute ownership.
    fn stamps_control_plane_ownership(&self) -> bool {
        true
    }

    /// Refuses already-resolved local image inputs.
    fn build(
        &self,
        _build_spec: FreshSandboxBuildSpec,
        _launch_config: SandboxLaunchConfig,
        _execution_id: ExecutionId,
    ) -> Result<Box<dyn SandboxBackend>> {
        bail!(
            "a cold sandbox cannot be started on another node from here: the build spec this \
             factory is handed has already been resolved into local paths, and the image \
             reference a node would need is no longer in it"
        )
    }

    fn build_from_snapshot(
        &self,
        snapshot: &RunnableSnapshot,
        launch_config: SandboxLaunchConfig,
        execution_id: ExecutionId,
    ) -> Result<Box<dyn SandboxBackend>> {
        // Delegate so resolved and unresolved snapshot paths build the same request.
        self.build_from_snapshot_record(snapshot.record(), launch_config, execution_id)
    }

    /// Builds a create request from a snapshot record without materializing its bytes locally.
    fn build_from_snapshot_record(
        &self,
        record: &SnapshotRecord,
        launch_config: SandboxLaunchConfig,
        execution_id: ExecutionId,
    ) -> Result<Box<dyn SandboxBackend>> {
        let resources = record.resources;

        let request = pb::SandboxCreateRequest {
            sandbox_id: launch_config.sandbox_id.to_string(),
            execution_id: execution_id.to_string(),
            source: Some(pb::sandbox_create_request::Source::Snapshot(
                pb::SnapshotSource {
                    snapshot_id: record.id.to_string(),
                    // Forward the already-resolved catalog row to avoid a second lookup.
                    resolved_record: Some(wire::serialize(record, "resolved snapshot record")?),
                },
            )),
            // The orchestrator owns expiry; the node must not invent another deadline.
            expiry: Some(pb::sandbox_create_request::Expiry::CallerKept(
                pb::CallerKeptExpiry {},
            )),
            timeout_action: pb::TimeoutAction::Pause as i32,
            auto_resume: false,
            secure: launch_config.envd_access_token.is_some(),
            traffic_access_token: launch_config
                .traffic_access_token
                .clone()
                .unwrap_or_default(),
            user_metadata: Default::default(),
            env_vars: launch_config.env_vars.clone().unwrap_or_default(),
            network_policy: launch_config
                .network
                .as_ref()
                .map(|policy| wire::serialize(policy, "network policy"))
                .transpose()?,
            custom_extension_params: launch_config
                .custom_extension_params
                .as_ref()
                .map(|params| wire::serialize(params, "custom extension params"))
                .transpose()?,
            // Ownership is stamped on the complete launch record by the orchestrator.
            // An unstamped create remains unclaimed rather than being reconciled away.
            control_plane_config: launch_config.control_plane_config.unwrap_or_default(),
        };

        Ok(Box::new(RemoteSandboxStub::pending(
            launch_config.sandbox_id,
            execution_id,
            resources,
            Arc::clone(&self.placement),
            PendingLaunch::Launch {
                request: Box::new(request),
                preferred_node_id: launch_config.preferred_node_id,
                needs: placement_needs(&launch_config.network),
            },
        )))
    }

    /// Builds a create request from an unresolved image specification.
    fn build_from_image_ref(
        &self,
        spec: UnresolvedImageBuildSpec,
        launch_config: SandboxLaunchConfig,
        execution_id: ExecutionId,
    ) -> Result<Box<dyn SandboxBackend>> {
        let resources = spec.resources;
        let attached_drives = spec
            .attached_drives
            .iter()
            .map(|drive| pb::AttachedDrive {
                image_ref: drive.image_ref.clone(),
                mount_path: drive.mount_path.display().to_string(),
                drive_id: drive.drive_id.clone(),
                read_only: drive.read_only,
                sub_path: drive
                    .sub_path
                    .as_ref()
                    .map(|p| p.display().to_string())
                    .unwrap_or_default(),
                virtual_size_bytes: drive.virtual_size.unwrap_or_default(),
            })
            .collect();

        let request = pb::SandboxCreateRequest {
            sandbox_id: launch_config.sandbox_id.to_string(),
            execution_id: execution_id.to_string(),
            source: Some(pb::sandbox_create_request::Source::Image(pb::ImageSource {
                image_ref: spec.image_ref,
                resources: Some(pb::SandboxResources {
                    cpu_count: resources.cpu_count,
                    memory_mib: resources.memory_mib,
                    disk_size_mib: resources.disk_size_mib,
                }),
                attached_drives,
                extra_boot_args: spec.extra_boot_args.unwrap_or_default(),
            })),
            // The orchestrator, not the node, owns expiry.
            expiry: Some(pb::sandbox_create_request::Expiry::CallerKept(
                pb::CallerKeptExpiry {},
            )),
            timeout_action: pb::TimeoutAction::Pause as i32,
            auto_resume: false,
            secure: launch_config.envd_access_token.is_some(),
            traffic_access_token: launch_config
                .traffic_access_token
                .clone()
                .unwrap_or_default(),
            user_metadata: Default::default(),
            env_vars: launch_config.env_vars.clone().unwrap_or_default(),
            network_policy: launch_config
                .network
                .as_ref()
                .map(|policy| wire::serialize(policy, "network policy"))
                .transpose()?,
            custom_extension_params: launch_config
                .custom_extension_params
                .as_ref()
                .map(|params| wire::serialize(params, "custom extension params"))
                .transpose()?,
            // Ownership is stamped by the orchestrator.
            control_plane_config: launch_config.control_plane_config.unwrap_or_default(),
        };

        Ok(Box::new(RemoteSandboxStub::pending(
            launch_config.sandbox_id,
            execution_id,
            resources,
            Arc::clone(&self.placement),
            PendingLaunch::Launch {
                request: Box::new(request),
                preferred_node_id: launch_config.preferred_node_id,
                needs: placement_needs(&launch_config.network),
            },
        )))
    }

    /// Remote sandboxes continue running when this deciding process exits.
    fn sandboxes_outlive_this_process(&self) -> bool {
        true
    }

    /// Adopts a running sandbox through deferred placement resolution.
    ///
    /// Construction stays synchronous; `start` locates the existing runtime.
    fn adopt_running(
        &self,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
        resources: SandboxResources,
    ) -> Result<Option<Box<dyn SandboxBackend>>> {
        Ok(Some(Box::new(RemoteSandboxStub::attaching(
            sandbox_id,
            execution_id,
            resources,
            Arc::clone(&self.placement),
        ))))
    }
}

/// What a launch needs from its node, read off the policy it will run under.
fn placement_needs(network: &Option<SandboxNetworkPolicy>) -> PlacementNeeds {
    PlacementNeeds {
        egress_broker: network.as_ref().is_some_and(|policy| policy.has_brokers()),
    }
}
