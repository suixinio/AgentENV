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
pub use access::{EnvdAccessToken, SandboxAccessTokenGenerator};
pub use backend::{
    CapturedSandboxSnapshot, InvalidSandboxRequest, PausedSandboxCapture, PausedSandboxState,
    ResolvedImageFacts, RuntimeArtifactSet, RuntimeConfirmedGone, SandboxBackend,
    SandboxBackendFactory, SandboxCaptureError, SandboxCaptureResult, SandboxExecutor,
    SandboxForkResult, SandboxForkSpec, SandboxRuntimeInfo,
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

/// One attached drive named by an OCI image reference nobody has resolved
/// yet.
///
/// # 🔴 The unresolved counterpart to [`ExtraDrive::Overlaybd`]
///
/// `ExtraDrive` carries an `image_config_path` — a path on the local disk an
/// `ImageResolver` already wrote. This carries the reference that path would
/// have come from, because the machine building the request
/// (`--role api`, which has no `regctl`) is not the machine that gets to
/// resolve it. See [`UnresolvedImageBuildSpec`] and
/// `SandboxLaunchSource::UnresolvedImage`.
#[derive(Clone, Debug)]
pub struct UnresolvedAttachedDrive {
    pub image_ref: String,
    pub drive_id: String,
    pub mount_path: PathBuf,
    pub sub_path: Option<PathBuf>,
    pub read_only: bool,
    /// Already converted to bytes from the REST API's `diskSizeMB`, exactly
    /// as [`ExtraDrive::Overlaybd::virtual_size`] carries it. `None` when the
    /// caller named no size, in which case the node that resolves this drive
    /// falls back to the source image's own size, same as a local cold create
    /// does today.
    pub virtual_size: Option<u64>,
}

/// A sandbox to build from an OCI image reference nobody has resolved yet.
///
/// # 🔴 The unresolved counterpart to [`FreshSandboxBuildSpec`]
///
/// `FreshSandboxBuildSpec` is handed to a factory that resolves images
/// locally and can turn a reference into a local overlaybd path itself. A
/// factory whose sandboxes run on another machine (`RemoteSandboxBackendFactory`)
/// cannot do that — resolving an image needs `regctl`, and `--role api` has
/// none — so it needs the reference, not a path, to hand to the machine that
/// will. `resources` is unaffected: computing the sandbox's CPU/memory/disk
/// request needs no image, so it is resolved locally either way and travels
/// here already concrete, exactly like `FreshSandboxBuildSpec::resources`.
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
