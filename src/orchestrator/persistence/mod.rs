mod file_backed;
#[cfg(any(test, feature = "test-support"))]
mod mock;

use async_trait::async_trait;
use std::path::{Path, PathBuf};

use crate::orchestrator::store::SandboxMetadata;
use crate::sandbox::{PausedSandboxState, SandboxBackendFactory};
use crate::types::SandboxId;

pub use file_backed::FileBackedSandboxPersister;
#[cfg(any(test, feature = "test-support"))]
pub use mock::{RecordingCall, RecordingPersister};

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
    pub fn io(operation: &'static str, path: impl Into<PathBuf>, source: std::io::Error) -> Self {
        Self::Io {
            operation,
            path: path.into(),
            source,
        }
    }

    pub fn store(operation: &'static str, source: anyhow::Error) -> Self {
        Self::Store { operation, source }
    }
}

/// Whether and under which identity a paused record reached a cluster registry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClusterRegistration {
    /// Never announced; registry absence cannot be acted on.
    Never,
    /// Announced without a recorded node identity.
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

    /// Returns a persisted capture path, confirmed absence, or a read error.
    async fn paused_artifact_root(
        &self,
        sandbox_id: &SandboxId,
    ) -> PersistenceResult<Option<PathBuf>>;

    /// Mark a paused sandbox as resuming.
    async fn mark_resuming(&self, sandbox_id: &SandboxId) -> PersistenceResult<()>;

    /// Records the node identity used for cluster registration.
    async fn mark_cluster_registered(
        &self,
        sandbox_id: &SandboxId,
        node_id: &str,
    ) -> PersistenceResult<()>;

    /// Returns this record's cluster-registration state.
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

    /// Always absent because this persister allocates no artifact root.
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
