use async_trait::async_trait;

use super::{
    BeganPause, DeadlineRenewalOutcome, HeldSandbox, MarkRunningOutcome, PausedRegistryListing,
    PausedRegistryRows, PausedSandboxEntry, PausedSandboxRegistry, ReclaimedHoldings,
    RegistryResult, ReleasedHoldings, ResumeClaim,
};
use crate::snapshot::SnapshotId;
use crate::types::{ExecutionId, SandboxId};

/// No-op registry that keeps pause and resume node-local.
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

    /// Covers no ids, so callers cannot treat its empty result as absence.
    async fn get_many(&self, _sandbox_ids: &[SandboxId]) -> RegistryResult<PausedRegistryRows> {
        Ok(PausedRegistryRows::default())
    }

    async fn claim_for_resume(
        &self,
        _sandbox_id: &SandboxId,
        _node_id: &str,
        _execution_id: ExecutionId,
    ) -> RegistryResult<ResumeClaim> {
        Ok(ResumeClaim::NotFound)
    }

    async fn release_claim(
        &self,
        _sandbox_id: &SandboxId,
        _generation: i64,
    ) -> RegistryResult<bool> {
        Ok(false)
    }

    async fn renew_lease(&self, _node_id: &str, _held: &[HeldSandbox]) -> RegistryResult<u64> {
        Ok(0)
    }

    async fn reclaim_expired_holdings(&self) -> RegistryResult<ReclaimedHoldings> {
        Ok(ReclaimedHoldings::default())
    }

    async fn mark_running(
        &self,
        _sandbox_id: &SandboxId,
        _node_id: &str,
        _holder_node_id: &str,
        _execution_id: ExecutionId,
        _expires_at: Option<std::time::SystemTime>,
    ) -> RegistryResult<MarkRunningOutcome> {
        Ok(MarkRunningOutcome::Untracked)
    }

    async fn renew_sandbox_deadline(
        &self,
        _sandbox_id: &SandboxId,
        _execution_id: ExecutionId,
        _expires_at: Option<std::time::SystemTime>,
    ) -> RegistryResult<DeadlineRenewalOutcome> {
        Ok(DeadlineRenewalOutcome::NotTracked)
    }

    async fn release_node_holdings(&self, _node_id: &str) -> RegistryResult<ReleasedHoldings> {
        Ok(ReleasedHoldings::default())
    }

    async fn remove(&self, _sandbox_id: &SandboxId, _generation: i64) -> RegistryResult<bool> {
        Ok(false)
    }

    async fn list_all(&self) -> RegistryResult<PausedRegistryListing> {
        Ok(PausedRegistryListing {
            sandboxes: Vec::new(),
            now: chrono::Utc::now(),
        })
    }
}
