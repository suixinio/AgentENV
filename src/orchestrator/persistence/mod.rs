mod file_backed;
#[cfg(test)]
mod mock;

use async_trait::async_trait;
use std::path::{Path, PathBuf};

use crate::orchestrator::store::SandboxMetadata;
use crate::sandbox::{PausedSandboxState, SandboxBackendFactory};
use crate::types::SandboxId;

pub use file_backed::FileBackedSandboxPersister;
#[cfg(test)]
pub(crate) use mock::{RecordingCall, RecordingPersister};

pub type PersistenceResult<T> = std::result::Result<T, SandboxPersistenceError>;

#[derive(Debug, thiserror::Error)]
pub enum SandboxPersistenceError {
    #[error("failed to {operation} {path}: {source}")]
    Io {
        operation: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid sandbox record: {reason}")]
    InvalidRecord {
        reason: String,
        #[source]
        source: Option<anyhow::Error>,
    },
    #[error("invalid paused sandbox runtime state: {reason}")]
    RuntimeState { reason: &'static str },
    #[error("paused sandbox store operation failed: {operation}: {source}")]
    Store {
        operation: &'static str,
        #[source]
        source: anyhow::Error,
    },
}

impl SandboxPersistenceError {
    pub(super) fn io(
        operation: &'static str,
        path: impl Into<PathBuf>,
        source: std::io::Error,
    ) -> Self {
        Self::Io {
            operation,
            path: path.into(),
            source,
        }
    }

    pub(super) fn store(operation: &'static str, source: anyhow::Error) -> Self {
        Self::Store { operation, source }
    }
}

/// Whether a paused record was ever announced to a cluster registry, and under
/// which node identity.
///
/// The identity matters because it, not the node's *current* identity, is what
/// a registry row must be compared against. A node's ID is only as stable as
/// whatever supplies it — under Kubernetes it is commonly the pod name, which
/// changes every time the pod is recreated — and a node that mistook its own
/// rows for another node's would discard every paused sandbox it holds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClusterRegistration {
    /// Never announced. The local copy is the only copy, so nothing the
    /// registry says (including saying nothing) may be acted on.
    Never,
    /// Announced by a build that did not record the identity it used. Only
    /// facts that hold regardless of identity may be acted on.
    Anonymous,
    /// Announced under this identity.
    As(String),
}

#[async_trait]
/// Persistence interface for sandbox records and artifacts.
pub trait SandboxPersister: Send + Sync {
    /// Load all persisted sandbox metadata.
    async fn load_all<F>(&self, factory: &F) -> PersistenceResult<Vec<SandboxMetadata>>
    where
        F: SandboxBackendFactory;

    /// Allocate an UNIQUE directory for sandbox artifacts.
    ///
    /// `None` means persistence is disabled and the sandbox backend should manage
    /// the lifecycle of its temporary artifacts.
    async fn allocate_artifact_root(
        &self,
        sandbox_id: &SandboxId,
    ) -> PersistenceResult<Option<PathBuf>>;

    /// Persist metadata and runtime state for a paused sandbox.
    async fn persist_paused(
        &self,
        metadata: &SandboxMetadata,
        artifact_root: Option<&Path>,
        paused_state: &dyn PausedSandboxState,
    ) -> PersistenceResult<()>;

    /// The directory this persister wrote a paused sandbox's capture into.
    ///
    /// # 🔴 Three answers, and the two that are not a path mean opposite
    /// things
    ///
    /// - `Ok(Some(path))` — the capture is there;
    /// - `Ok(None)` — there is no paused record here. For
    ///   [`DisabledSandboxPersister`] that is the permanent answer: it writes
    ///   nothing, so the backend kept the capture in temporaries it manages
    ///   itself and there is no directory to name.
    /// - `Err(..)` — the records could not be read. A caller that took this for
    ///   `None` would report a sandbox as having no capture on the strength of
    ///   a disk it could not reach.
    ///
    /// Read from the record rather than returned by `pause_sandbox`, because
    /// the question is *where did this sandbox's capture go* and not *what did
    /// this call do*: a second pause of an already-paused sandbox does no work
    /// and must still be able to say where the bytes are.
    async fn paused_artifact_root(
        &self,
        sandbox_id: &SandboxId,
    ) -> PersistenceResult<Option<PathBuf>>;

    /// Mark a paused sandbox as resuming.
    async fn mark_resuming(&self, sandbox_id: &SandboxId) -> PersistenceResult<()>;

    /// Record that this paused sandbox has been announced to a cluster registry
    /// under `node_id`.
    async fn mark_cluster_registered(
        &self,
        sandbox_id: &SandboxId,
        node_id: &str,
    ) -> PersistenceResult<()>;

    /// Whether this paused sandbox was ever announced to a cluster registry.
    ///
    /// Records that never were must be left alone by reconciliation: for them
    /// the local copy is the only copy, so "absent from the registry" carries
    /// no information at all.
    async fn cluster_registration(
        &self,
        sandbox_id: &SandboxId,
    ) -> PersistenceResult<ClusterRegistration>;

    /// Roll back a resuming mark after a failed resume attempt.
    async fn rollback_resuming(&self, sandbox_id: &SandboxId) -> PersistenceResult<()>;

    /// Delete the persistence record for a sandbox.
    async fn delete_record(&self, sandbox_id: &SandboxId) -> PersistenceResult<()>;

    /// Delete the persistence record and all associated artifacts.
    async fn delete_record_and_artifacts(&self, sandbox_id: &SandboxId) -> PersistenceResult<()>;
}

#[derive(Default)]
pub struct DisabledSandboxPersister;

#[async_trait]
impl SandboxPersister for DisabledSandboxPersister {
    async fn load_all<F>(&self, _factory: &F) -> PersistenceResult<Vec<SandboxMetadata>>
    where
        F: SandboxBackendFactory,
    {
        Ok(Vec::new())
    }

    async fn allocate_artifact_root(
        &self,
        _sandbox_id: &SandboxId,
    ) -> PersistenceResult<Option<PathBuf>> {
        Ok(None)
    }

    async fn persist_paused(
        &self,
        _metadata: &SandboxMetadata,
        _artifact_root: Option<&Path>,
        _paused_state: &dyn PausedSandboxState,
    ) -> PersistenceResult<()> {
        Ok(())
    }

    /// 🔴 `None`, and it is a fact rather than a shrug: this persister
    /// allocates no artifact root, so no capture it was told about was written
    /// into one.
    async fn paused_artifact_root(
        &self,
        _sandbox_id: &SandboxId,
    ) -> PersistenceResult<Option<PathBuf>> {
        Ok(None)
    }

    async fn mark_cluster_registered(
        &self,
        _sandbox_id: &SandboxId,
        _node_id: &str,
    ) -> PersistenceResult<()> {
        Ok(())
    }

    async fn cluster_registration(
        &self,
        _sandbox_id: &SandboxId,
    ) -> PersistenceResult<ClusterRegistration> {
        Ok(ClusterRegistration::Never)
    }

    async fn mark_resuming(&self, _sandbox_id: &SandboxId) -> PersistenceResult<()> {
        Ok(())
    }

    async fn rollback_resuming(&self, _sandbox_id: &SandboxId) -> PersistenceResult<()> {
        Ok(())
    }

    async fn delete_record(&self, _sandbox_id: &SandboxId) -> PersistenceResult<()> {
        Ok(())
    }

    async fn delete_record_and_artifacts(&self, _sandbox_id: &SandboxId) -> PersistenceResult<()> {
        Ok(())
    }
}
