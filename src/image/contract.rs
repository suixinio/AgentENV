//! What the rest of the system asks of the image layer.
//!
//! # 🔴 The half that has no `regctl`, no cache and no overlaybd
//!
//! Resolving an image reference into local bytes means fetching a manifest,
//! pulling blobs and converting them into a local overlaybd image. A process
//! that boots no microVMs does none of that — and must not link the code that
//! could. What it *does* still handle is the same request shapes: it validates
//! them, names them, and hands them to a machine that can.
//!
//! So the two halves are stated apart. This module holds the request and answer
//! types plus the traits the callers spell; the implementations that touch
//! registries, the layer cache and overlaybd live beside them in the half that
//! owns those things.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use super::{ImageError, ImageResult};
use crate::sandbox::RuntimeArtifactSet;
use crate::types::SandboxId;

/// The parts of an OCI image config a sandbox launch actually reads.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImageBaseContext {
    #[serde(default)]
    pub env_vars: HashMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workdir: Option<String>,
    // OCI image config `User` field forwarded as-is to envd's InitPostRequest.
    // Accepted formats: "username", "uid", "user:group", "uid:gid", "" (root).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub exposed_ports: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entrypoint: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cmd: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub volumes: Vec<String>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub labels: HashMap<String, String>,
}

impl ImageBaseContext {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        env_vars: HashMap<String, String>,
        workdir: Option<String>,
        user: Option<String>,
        exposed_ports: Vec<String>,
        entrypoint: Option<Vec<String>>,
        cmd: Option<Vec<String>>,
        volumes: Vec<String>,
        labels: HashMap<String, String>,
    ) -> Self {
        Self {
            env_vars,
            workdir: normalize_optional_string(workdir),
            user: normalize_optional_string(user),
            exposed_ports,
            entrypoint,
            cmd,
            volumes,
            labels,
        }
    }
}

fn normalize_optional_string(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

/// One image reference turned into a local overlaybd image.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedBlockImage {
    pub image_ref: String,
    pub overlaybd_config_path: PathBuf,
    pub base_context: ImageBaseContext,
    /// Raw source image config JSON, `None` when the image source has no config
    /// (e.g. bare overlaybd config path) or when loaded from a legacy cache entry.
    pub raw_config: Option<serde_json::Value>,
}

/// Turns a user-facing image reference into local overlaybd bytes.
#[async_trait]
pub trait RootfsImageResolver: Send + Sync + std::fmt::Debug {
    /// The image a request that named none is asking for.
    fn default_image(&self) -> &str;

    async fn resolve(&self, image_ref: &str) -> ImageResult<ResolvedBlockImage>;
}

/// A resolver for a process that resolves nothing.
///
/// 🔴 Refuses rather than panicking, and carries the configured default image
/// so the request shapes that only need the *name* — a template build that
/// named no image, and is about to be dispatched to a machine that will
/// resolve it — still work here. Only `resolve` is the thing this half cannot
/// do.
#[derive(Debug)]
pub struct RefusingImageResolver {
    default_image: String,
}

impl RefusingImageResolver {
    pub fn new(default_image: impl Into<String>) -> Self {
        Self {
            default_image: default_image.into(),
        }
    }
}

#[async_trait]
impl RootfsImageResolver for RefusingImageResolver {
    fn default_image(&self) -> &str {
        &self.default_image
    }

    async fn resolve(&self, image_ref: &str) -> ImageResult<ResolvedBlockImage> {
        Err(ImageError::Other(anyhow::anyhow!(
            "this process resolves no images: '{image_ref}' has to be resolved on a machine that \
             runs sandboxes"
        )))
    }
}

/// Who is keeping a set of runtime artifacts from being reclaimed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeImageOwner {
    StartingSandbox(SandboxId),
    PausedSandbox(SandboxId),
}

/// The node-local layer cache's view of what is still in use.
///
/// 🔴 The orchestrator holds one of these on both halves, and only one half has
/// a layer cache to protect. See [`DisabledRuntimeImageRefs`].
#[async_trait]
pub trait RuntimeImageRefs: Send + Sync + std::fmt::Debug {
    async fn pin(&self, owner: RuntimeImageOwner, artifacts: RuntimeArtifactSet) -> Result<()>;

    async fn unpin_best_effort(&self, owner: RuntimeImageOwner);

    async fn reconcile_paused(&self, live_paused: &[SandboxId]) -> Result<()>;

    async fn maintain_running(&self, running: Vec<(SandboxId, RuntimeArtifactSet)>) -> Result<()>;
}

/// Pins nothing, because there is no local layer cache on this machine.
///
/// 🔴 Not an `Option<Arc<dyn RuntimeImageRefs>>` on the orchestrator. Every
/// pin has a matching release on some path, and an `Option` would make each of
/// those paths a place where somebody has to remember that "no cache" is not
/// the same as "release failed". A no-op implementation makes the two halves
/// the same shape and leaves the release paths unconditional.
#[derive(Debug, Default)]
pub struct DisabledRuntimeImageRefs;

#[async_trait]
impl RuntimeImageRefs for DisabledRuntimeImageRefs {
    async fn pin(&self, _owner: RuntimeImageOwner, _artifacts: RuntimeArtifactSet) -> Result<()> {
        Ok(())
    }

    async fn unpin_best_effort(&self, _owner: RuntimeImageOwner) {}

    async fn reconcile_paused(&self, _live_paused: &[SandboxId]) -> Result<()> {
        Ok(())
    }

    async fn maintain_running(&self, _running: Vec<(SandboxId, RuntimeArtifactSet)>) -> Result<()> {
        Ok(())
    }
}

impl DisabledRuntimeImageRefs {
    pub fn shared() -> Arc<dyn RuntimeImageRefs> {
        Arc::new(Self)
    }
}
