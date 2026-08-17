use async_trait::async_trait;

use super::{BeganPause, PausedSandboxEntry, PausedSandboxRegistry, RegistryResult, ResumeClaim};
use crate::snapshot::SnapshotId;
use crate::types::SandboxId;

/// Default registry: records nothing, claims nothing.
///
/// With this backend a paused sandbox stays exactly as resumable as it was
/// before the registry existed — on its own node, through the node-local
/// persister — and `claim_for_resume` always reports `NotFound`, so the resume
/// path never leaves the local fast path.
#[derive(Default)]
pub struct DisabledPausedSandboxRegistry;

#[async_trait]
impl PausedSandboxRegistry for DisabledPausedSandboxRegistry {
    async fn begin_pause(&self, _entry: &PausedSandboxEntry) -> RegistryResult<BeganPause> {
        Ok(BeganPause {
            generation: 0,
            previous_snapshot_id: None,
        })
    }

    async fn complete_pause(
        &self,
        _sandbox_id: &SandboxId,
        _generation: i64,
        _snapshot_id: &SnapshotId,
    ) -> RegistryResult<()> {
        Ok(())
    }

    async fn mark_local_only(
        &self,
        _sandbox_id: &SandboxId,
        _generation: i64,
    ) -> RegistryResult<()> {
        Ok(())
    }

    async fn get(&self, _sandbox_id: &SandboxId) -> RegistryResult<Option<PausedSandboxEntry>> {
        Ok(None)
    }

    async fn claim_for_resume(
        &self,
        _sandbox_id: &SandboxId,
        _node_id: &str,
    ) -> RegistryResult<ResumeClaim> {
        Ok(ResumeClaim::NotFound)
    }

    async fn release_claim(&self, _sandbox_id: &SandboxId, _generation: i64) -> RegistryResult<()> {
        Ok(())
    }

    async fn mark_running(&self, _sandbox_id: &SandboxId, _node_id: &str) -> RegistryResult<()> {
        Ok(())
    }

    async fn remove(&self, _sandbox_id: &SandboxId) -> RegistryResult<()> {
        Ok(())
    }
}
