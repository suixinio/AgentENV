pub mod access;
pub mod backend;
pub mod custom_extension;
pub mod envd;
#[doc(hidden)]
pub mod mock;
pub mod network;
pub mod process;

use std::{collections::HashMap, path::PathBuf};

pub use custom_extension::{
    custom_extension_params_is_empty, CustomExtensionClient, CustomExtensionParams,
};

use crate::types::{ImageConfigs, SandboxId};

pub use crate::types::{
    normalize_mount_path_for_drive, validate_drive_id, validate_mount_path, validate_sub_path,
    ExtraDrive,
};
pub use ::envd::process::Signal;
pub use access::{AccessTokenSeedPolicy, EnvdAccessToken, SandboxAccessTokenGenerator};
pub use backend::{
    CapturedSandboxSnapshot, InvalidSandboxRequest, ResolvedImageFacts, RuntimeArtifactSet,
    RuntimeConfirmedGone, SandboxBackend, SandboxBackendFactory, SandboxCaptureError,
    SandboxCaptureResult, SandboxExecutor, SandboxForkResult, SandboxForkSpec, SandboxRuntimeInfo,
};
pub use network::{BaseSandboxNetworkPolicy, SandboxNetworkEgressPolicy, SandboxNetworkPolicy};
pub use process::{Executor, ProcessHandle, ProcessOpts, ProcessOutput};

#[derive(Clone, Debug)]
pub struct FreshSandboxBuildSpec {
    pub image_config_path: PathBuf,
    pub context: crate::snapshot::CommandContext,
    pub resources: crate::types::SandboxResources,
    pub extra_drives: Vec<ExtraDrive>,
    pub extra_boot_args: Option<String>,
}

/// An attached drive whose OCI reference must be resolved on the target node.
#[derive(Clone, Debug)]
pub struct UnresolvedAttachedDrive {
    pub image_ref: String,
    pub drive_id: String,
    pub mount_path: PathBuf,
    pub sub_path: Option<PathBuf>,
    pub read_only: bool,
    /// Requested virtual size in bytes.
    ///
    /// `None` lets the target node use the source image size.
    pub virtual_size: Option<u64>,
}

/// A sandbox whose OCI image references must be resolved on the target node.
#[derive(Clone, Debug)]
pub struct UnresolvedImageBuildSpec {
    pub image_ref: String,
    pub resources: crate::types::SandboxResources,
    pub attached_drives: Vec<UnresolvedAttachedDrive>,
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
    /// Opaque control-plane ownership marker.
    ///
    /// `None` means unowned; producers must never emit an empty value.
    pub control_plane_config: Option<Vec<u8>>,
    /// Node a remote placement should favour; ignored by backends that run
    /// the sandbox on this machine.
    pub preferred_node_id: Option<String>,
}

impl SandboxLaunchConfig {
    pub fn new(sandbox_id: SandboxId, snapshot_id: impl Into<String>) -> Self {
        Self {
            sandbox_id,
            snapshot_id: snapshot_id.into(),
            env_vars: None,
            network: None,
            extra_mmds: serde_json::Map::new(),
            custom_extension_params: None,
            envd_access_token: None,
            control_plane_config: None,
            preferred_node_id: None,
        }
    }

    pub fn with_image_configs(mut self, image_configs: &ImageConfigs) -> Self {
        if !image_configs.is_empty() {
            self.extra_mmds
                .insert("imageConfigs".to_string(), image_configs.to_value());
        }
        self
    }
}
