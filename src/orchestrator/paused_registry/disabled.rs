use std::collections::HashMap;

use async_trait::async_trait;

use super::{
    BeganPause, HeldSandbox, MarkRunningOutcome, PausedSandboxEntry, PausedSandboxRegistry,
    ReclaimedHoldings, RegistryResult, ReleasedHoldings, ResumeClaim,
};
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

    async fn get_many(
        &self,
        _sandbox_ids: &[SandboxId],
    ) -> RegistryResult<HashMap<SandboxId, PausedSandboxEntry>> {
        Ok(HashMap::new())
    }

    async fn claim_for_resume(
        &self,
        _sandbox_id: &SandboxId,
        _node_id: &str,
    ) -> RegistryResult<ResumeClaim> {
        Ok(ResumeClaim::NotFound)
    }

    async fn release_claim(
        &self,
        _sandbox_id: &SandboxId,
        _generation: i64,
    ) -> RegistryResult<bool> {
        // Nothing was ever recorded, so nothing matched. `false` here is the
        // truth rather than a stub: there is no row this release could have
        // returned to the cluster.
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
        _expires_at: Option<std::time::SystemTime>,
    ) -> RegistryResult<MarkRunningOutcome> {
        // Untracked, not held-elsewhere: this backend has no cluster to hold a
        // sandbox anywhere else.
        Ok(MarkRunningOutcome::Untracked)
    }

    async fn release_node_holdings(&self, _node_id: &str) -> RegistryResult<ReleasedHoldings> {
        Ok(ReleasedHoldings::default())
    }

    async fn remove(&self, _sandbox_id: &SandboxId, _generation: i64) -> RegistryResult<bool> {
        Ok(false)
    }
}
