//! What a create builds before it reaches a runtime: the launch config the
//! sandbox starts under and the record that stands for it while it starts.
//! Both halves build these from the same committed row.

use std::collections::HashMap;

use crate::cfg::ConfigManager;
use crate::orchestrator::store::{configured_max_sandbox_lifetime, SandboxMetadata};
use crate::orchestrator::{ControlPlaneConfig, OrchestratorError, Result, SandboxTimeoutAction};
use crate::sandbox::{
    CustomExtensionParams, EnvdAccessToken, SandboxLaunchConfig, SandboxNetworkPolicy,
    SandboxRuntimeInfo,
};
use crate::snapshot::{SnapshotRuntimeVersions, SnapshotSource};
use crate::types::{bytes_to_mib_ceil, SandboxId, SandboxResources};

/// Shared inputs for resolved and unresolved committed-snapshot creates.
pub struct SnapshotCreateInputs {
    pub sandbox_id: SandboxId,
    pub envd_access_token: Option<EnvdAccessToken>,
    pub env_vars: Option<HashMap<String, String>>,
    pub user_metadata: Option<HashMap<String, String>>,
    pub network_policy: SandboxNetworkPolicy,
    pub custom_extension_params: Option<CustomExtensionParams>,
    pub timeout_action: SandboxTimeoutAction,
    pub auto_resume: bool,
    pub secure: bool,
    pub traffic_access_token: Option<String>,
    pub control_plane_config: Option<ControlPlaneConfig>,
    pub preferred_node_id: Option<String>,
}

pub struct SnapshotCreateParts {
    pub launch_config: SandboxLaunchConfig,
    pub transitional_metadata: SandboxMetadata,
}

/// Builds launch config and transitional metadata directly from a committed row.
pub fn snapshot_create_parts(
    record: &crate::snapshot::SnapshotRecord,
    inputs: SnapshotCreateInputs,
) -> Result<SnapshotCreateParts> {
    let SnapshotCreateInputs {
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
    } = inputs;

    // Uncommitted catalog rows are invalid requests, not panics.
    let Some(committed) = record.committed.as_ref() else {
        return Err(OrchestratorError::InvalidRequest(format!(
            "snapshot {} is not ready to launch from: it has no committed artifacts",
            record.id
        )));
    };

    let configured_mode = ConfigManager::global_config().virtualization_mode;
    if committed.virtualization_mode != configured_mode {
        return Err(OrchestratorError::VirtualizationModeMismatch {
            resource: format!("snapshot {}", record.id),
            resource_mode: committed.virtualization_mode,
            node_mode: configured_mode,
        });
    }
    let launch_image_configs = committed.image_configs.clone();
    let mut extra_mmds = serde_json::Map::new();
    if !launch_image_configs.is_empty() {
        extra_mmds.insert("imageConfigs".to_string(), launch_image_configs.to_value());
    };
    // Launch params override snapshot params; otherwise inherit them.
    let effective_custom_extension_params =
        custom_extension_params.or_else(|| committed.custom_extension_params.clone());
    let launch_config = SandboxLaunchConfig {
        sandbox_id,
        snapshot_id: record.id.to_string(),
        env_vars,
        network: network_policy.runtime_policy(),
        extra_mmds,
        custom_extension_params: effective_custom_extension_params.clone(),
        envd_access_token,
        traffic_access_token: traffic_access_token.clone(),
        // Stamped after the encoded record is complete.
        control_plane_config: None,
        preferred_node_id,
    };

    let mut transitional_metadata = SandboxMetadata {
        id: sandbox_id,
        snapshot_id: record.id.to_string(),
        snapshot_alias: record.alias.as_ref().map(ToString::to_string),
        virtualization_mode: committed.virtualization_mode,
        huge_pages: committed.huge_pages,
        runtime_versions: committed.runtime_versions.clone(),
        resources: record.resources,
        context: committed.context.clone(),
        startup: committed.startup.clone(),
        image_configs: launch_image_configs,
        timeout_action,
        auto_resume,
        user_metadata,
        network_policy,
        custom_extension_params: effective_custom_extension_params,
        secure,
        traffic_access_token,
        control_plane_config,
        max_lifetime: configured_max_sandbox_lifetime(),
        ..Default::default()
    };

    // A resume continues the sandbox the row was paused from: same identity,
    // same lifetime budget, and the time already charged against it. A create
    // that merely starts from a sandbox's snapshot is a new sandbox.
    let resumes_this_sandbox = matches!(
        &record.source,
        SnapshotSource::Sandbox { source_sandbox_id } if *source_sandbox_id == sandbox_id.to_string()
    );
    if let Some(paused) = record.paused_sandbox().filter(|_| resumes_this_sandbox) {
        transitional_metadata.snapshot_id = paused.template_id.clone();
        transitional_metadata.snapshot_alias = paused.template_alias.clone();
        transitional_metadata.created_at = paused.created_at();
        transitional_metadata.max_lifetime = paused.max_lifetime();
        transitional_metadata.running_elapsed = paused.running_elapsed();
    }

    Ok(SnapshotCreateParts {
        launch_config,
        transitional_metadata,
    })
}

pub fn default_fresh_sandbox_resources() -> SandboxResources {
    let config = ConfigManager::global_config();
    SandboxResources {
        cpu_count: config.machine.vcpu_count,
        memory_mib: config.machine.mem_size_mib,
        // Filled from backend runtime info after the rootfs device is created.
        disk_size_mib: 0,
    }
}

pub fn resources_with_runtime_info(
    mut resources: SandboxResources,
    runtime_info: SandboxRuntimeInfo,
) -> SandboxResources {
    // This API resource field tracks the rootfs block device size. Attached
    // drives are separately configured storage and are not folded into it.
    if let Some(size) = runtime_info.rootfs_virtual_size {
        resources.disk_size_mib = bytes_to_mib_ceil(size);
    }
    resources
}

pub fn configured_runtime_versions() -> SnapshotRuntimeVersions {
    let config = ConfigManager::global_config();
    SnapshotRuntimeVersions::new(
        config
            .kernel
            .version
            .clone()
            .unwrap_or_else(|| "unknown".to_string()),
        config
            .firecracker
            .version
            .clone()
            .unwrap_or_else(|| "unknown".to_string()),
        config.envd.version.clone(),
        config.resolved_tools_version().to_string(),
    )
}
