mod access;
mod backend;
pub(crate) mod custom_extension;
mod envd;
mod extra_drive;
mod firecracker;
#[cfg(test)]
pub(crate) mod mock;
mod network;
mod process;
mod ublk;

use std::{collections::HashMap, path::PathBuf};

pub(crate) use custom_extension::{
    custom_extension_params_is_empty, CustomExtensionClient, CustomExtensionParams,
};

use crate::types::{ImageConfigs, SandboxId};

pub use ::envd::process::Signal;
pub use access::{EnvdAccessToken, SandboxAccessTokenGenerator};
pub use backend::{
    CapturedSandboxSnapshot, PausedSandboxCapture, PausedSandboxState, RuntimeArtifactSet,
    SandboxBackend, SandboxBackendFactory, SandboxCaptureError, SandboxCaptureResult,
    SandboxExecutor, SandboxForkResult, SandboxForkSpec, SandboxRuntimeInfo,
};
pub use extra_drive::{
    normalize_mount_path_for_drive, validate_drive_id, validate_mount_path, validate_sub_path,
    ExtraDrive,
};
pub use firecracker::{
    FirecrackerCapturedSnapshot, FirecrackerCommonConfig, FirecrackerPausedState, FirecrackerPool,
    FirecrackerRuntimePolicy, FirecrackerSandbox, FirecrackerSandboxConfig,
    FirecrackerSandboxFactory, FirecrackerSnapshotConfig, FirecrackerSnapshotManifest,
};
pub(crate) use network::{prepare_runtime as prepare_network_runtime, NetworkManager};
pub use network::{BaseSandboxNetworkPolicy, SandboxNetworkEgressPolicy, SandboxNetworkPolicy};
pub use process::{Executor, ProcessHandle, ProcessOpts, ProcessOutput};
pub use ublk::{OverlaybdConfig, UblkBackend, UblkConfig, UblkDaemonConfig, UblkDeviceManager};

#[derive(Clone, Debug)]
pub struct FreshSandboxBuildSpec {
    pub image_config_path: PathBuf,
    pub context: crate::snapshot::CommandContext,
    pub resources: crate::types::SandboxResources,
    pub extra_drives: Vec<ExtraDrive>,
    pub extra_boot_args: Option<String>,
}

/// High-level launch request consumed by sandbox backend factories.
///
/// Carries launch-time inputs from upper layers (for example orchestrator)
/// into backend construction.
#[derive(Clone, Debug, Default)]
pub struct SandboxLaunchConfig {
    /// Stable sandbox identity
    pub sandbox_id: SandboxId,
    /// Snapshot/template identity
    pub snapshot_id: String,
    /// One-off environment variable overrides to apply on top of snapshot defaults.
    pub env_vars: Option<HashMap<String, String>>,
    /// Per-sandbox egress policy.
    pub network: Option<SandboxNetworkPolicy>,
    /// Opaque extra key-value pairs merged into the MMDS metadata JSON.
    /// The sandbox layer does not interpret these; they are passed through
    /// as-is to the VM via the Firecracker MMDS interface.
    pub extra_mmds: serde_json::Map<String, serde_json::Value>,
    /// Opaque user-provided JSON passed through to the custom extension hooks.
    /// Takes precedence over any value persisted in the source snapshot.
    pub custom_extension_params: Option<CustomExtensionParams>,
    /// Runtime-only credential used by envd. The token is never serialized and
    /// its Debug representation is redacted.
    pub envd_access_token: Option<EnvdAccessToken>,
    /// The control plane's ownership marker for this sandbox, when the
    /// orchestrator above is one that stamps them.
    ///
    /// 🔴 Opaque here, and opaque on the machine it lands on. This layer moves
    /// the bytes and never reads them — the same contract the node service
    /// keeps (`crate::orchestrator::ControlPlaneConfig`), which is why the
    /// field is bytes rather than that type: the sandbox layer has no business
    /// knowing what a control plane's record looks like, and does not depend
    /// on the orchestrator for anything else.
    ///
    /// 🔴 `None` and `Some(vec![])` are not the same thing anywhere else in
    /// this system — an empty marker means *not owned*, which is the direction
    /// that leaves a sandbox alone — so nothing may put an empty vector here.
    /// The only producer is `ControlPlaneConfig::as_bytes`, which cannot be
    /// empty by construction, and `Orchestrator::stamp_control_plane_ownership`
    /// is the only writer.
    pub control_plane_config: Option<Vec<u8>>,
}

impl SandboxLaunchConfig {
    pub(crate) fn new(sandbox_id: SandboxId, snapshot_id: impl Into<String>) -> Self {
        Self {
            sandbox_id,
            snapshot_id: snapshot_id.into(),
            env_vars: None,
            network: None,
            extra_mmds: serde_json::Map::new(),
            custom_extension_params: None,
            envd_access_token: None,
            control_plane_config: None,
        }
    }

    pub(crate) fn with_image_configs(mut self, image_configs: &ImageConfigs) -> Self {
        if !image_configs.is_empty() {
            self.extra_mmds
                .insert("imageConfigs".to_string(), image_configs.to_value());
        }
        self
    }
}
