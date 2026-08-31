use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use tonic::async_trait;

use super::super::store::SandboxMetadata;
use super::{ClusterRegistration, PersistenceResult, SandboxPersistenceError, SandboxPersister};
use crate::sandbox::PausedSandboxState;
use crate::types::SandboxId;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum RecordingCall {
    LoadAll,
    AllocateArtifactRoot,
    PersistPaused,
    MarkResuming,
    MarkClusterRegistered,
    PausedArtifactRoot,
    RollbackResuming,
    DeleteRecord,
    DeleteRecordAndArtifacts,
}

impl RecordingCall {
    const fn as_str(self) -> &'static str {
        match self {
            Self::LoadAll => "load_all",
            Self::AllocateArtifactRoot => "allocate_artifact_root",
            Self::PersistPaused => "persist_paused",
            Self::MarkResuming => "mark_resuming",
            Self::MarkClusterRegistered => "mark_cluster_registered",
            Self::PausedArtifactRoot => "paused_artifact_root",
            Self::RollbackResuming => "rollback_resuming",
            Self::DeleteRecord => "delete_record",
            Self::DeleteRecordAndArtifacts => "delete_record_and_artifacts",
        }
    }
}

impl fmt::Display for RecordingCall {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Default)]
pub struct RecordingPersister {
    pub calls: Arc<Mutex<Vec<RecordingCall>>>,
    loaded: Arc<Mutex<Vec<SandboxMetadata>>>,
    persisted: Arc<Mutex<Vec<SandboxMetadata>>>,
    failures: Arc<Mutex<HashMap<RecordingCall, usize>>>,
    artifact_root: Arc<Mutex<Option<PathBuf>>>,
    allocated_root: Arc<Mutex<Option<PathBuf>>>,
}

impl RecordingPersister {
    pub fn with_loaded(loaded: Vec<SandboxMetadata>) -> Self {
        Self {
            loaded: Arc::new(Mutex::new(loaded)),
            ..Default::default()
        }
    }

    /// Configures the paused capture path returned by this test persister.
    pub fn holds_capture_at(&self, artifact_root: impl Into<PathBuf>) {
        *self.artifact_root.lock().unwrap() = Some(artifact_root.into());
    }

    /// Configures the distinct path allocated before a pause capture.
    pub fn allocates_artifact_root_at(&self, root: impl Into<PathBuf>) {
        *self.allocated_root.lock().unwrap() = Some(root.into());
    }

    pub fn calls(&self) -> Vec<RecordingCall> {
        self.calls.lock().unwrap().clone()
    }

    /// Returns records passed to `persist_paused`, in order.
    pub fn persisted(&self) -> Vec<SandboxMetadata> {
        self.persisted.lock().unwrap().clone()
    }

    pub fn clear_calls(&self) {
        self.calls.lock().unwrap().clear();
    }

    pub fn record(&self, call: RecordingCall) {
        self.calls.lock().unwrap().push(call);
    }

    pub fn fail_next(&self, call: RecordingCall) {
        let mut failures = self.failures.lock().unwrap();
        *failures.entry(call).or_default() += 1;
    }

    fn maybe_fail(&self, call: RecordingCall) -> PersistenceResult<()> {
        let mut failures = self.failures.lock().unwrap();
        let Some(remaining) = failures.get_mut(&call) else {
            return Ok(());
        };
        if *remaining == 0 {
            return Ok(());
        }
        *remaining -= 1;
        Err(SandboxPersistenceError::InvalidRecord {
            reason: format!("forced {call} failure"),
            source: None,
        })
    }
}

#[async_trait]
impl SandboxPersister for RecordingPersister {
    async fn load_all<F>(&self, _factory: &F) -> PersistenceResult<Vec<SandboxMetadata>>
    where
        F: crate::sandbox::SandboxBackendFactory,
    {
        self.record(RecordingCall::LoadAll);
        self.maybe_fail(RecordingCall::LoadAll)?;
        Ok(self.loaded.lock().unwrap().clone())
    }

    async fn allocate_artifact_root(
        &self,
        _sandbox_id: &SandboxId,
    ) -> PersistenceResult<Option<PathBuf>> {
        self.record(RecordingCall::AllocateArtifactRoot);
        self.maybe_fail(RecordingCall::AllocateArtifactRoot)?;
        Ok(self.allocated_root.lock().unwrap().clone())
    }

    async fn persist_paused(
        &self,
        metadata: &SandboxMetadata,
        _artifact_root: Option<&Path>,
        _paused_state: &dyn PausedSandboxState,
    ) -> PersistenceResult<()> {
        self.persisted.lock().unwrap().push(metadata.clone());
        self.record(RecordingCall::PersistPaused);
        self.maybe_fail(RecordingCall::PersistPaused)?;
        Ok(())
    }

    /// Returns the configured capture path, if any.
    async fn paused_artifact_root(
        &self,
        _sandbox_id: &SandboxId,
    ) -> PersistenceResult<Option<PathBuf>> {
        self.record(RecordingCall::PausedArtifactRoot);
        self.maybe_fail(RecordingCall::PausedArtifactRoot)?;
        Ok(self.artifact_root.lock().unwrap().clone())
    }

    async fn mark_cluster_registered(
        &self,
        _sandbox_id: &SandboxId,
        _node_id: &str,
    ) -> PersistenceResult<()> {
        self.record(RecordingCall::MarkClusterRegistered);

        Ok(())
    }

    async fn cluster_registration(
        &self,
        _sandbox_id: &SandboxId,
    ) -> PersistenceResult<ClusterRegistration> {
        Ok(ClusterRegistration::Never)
    }

    async fn mark_resuming(&self, _sandbox_id: &SandboxId) -> PersistenceResult<()> {
        self.record(RecordingCall::MarkResuming);
        self.maybe_fail(RecordingCall::MarkResuming)?;
        Ok(())
    }

    async fn rollback_resuming(&self, _sandbox_id: &SandboxId) -> PersistenceResult<()> {
        self.record(RecordingCall::RollbackResuming);
        self.maybe_fail(RecordingCall::RollbackResuming)?;
        Ok(())
    }

    async fn delete_record(&self, _sandbox_id: &SandboxId) -> PersistenceResult<()> {
        self.record(RecordingCall::DeleteRecord);
        self.maybe_fail(RecordingCall::DeleteRecord)?;
        Ok(())
    }

    async fn delete_record_and_artifacts(&self, _sandbox_id: &SandboxId) -> PersistenceResult<()> {
        self.record(RecordingCall::DeleteRecordAndArtifacts);
        self.maybe_fail(RecordingCall::DeleteRecordAndArtifacts)?;
        Ok(())
    }
}
