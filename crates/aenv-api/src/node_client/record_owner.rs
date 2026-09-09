//! Who the deciding half's own record says owns a sandbox id right now.

use std::sync::Arc;

use async_trait::async_trait;

use crate::orchestrator::store::MetadataStore;
use crate::types::{ExecutionId, SandboxId};

/// Reads the incarnation the control plane recorded for a sandbox.
#[async_trait]
pub trait SandboxRecordOwner: Send + Sync + 'static {
    /// `Ok(None)` is a complete answer that no record exists. `Err` is
    /// unknown and must never be read as absence.
    async fn recorded_execution(
        &self,
        sandbox_id: SandboxId,
    ) -> anyhow::Result<Option<ExecutionId>>;
}

/// The metadata store as the answer to who owns a sandbox id.
pub struct StoreRecordOwner<S>(S);

impl<S: MetadataStore + 'static> StoreRecordOwner<S> {
    pub fn shared(store: S) -> Arc<dyn SandboxRecordOwner> {
        Arc::new(Self(store))
    }
}

#[async_trait]
impl<S: MetadataStore + 'static> SandboxRecordOwner for StoreRecordOwner<S> {
    async fn recorded_execution(
        &self,
        sandbox_id: SandboxId,
    ) -> anyhow::Result<Option<ExecutionId>> {
        Ok(self
            .0
            .get(&sandbox_id)
            .await?
            .map(|metadata| metadata.execution_id))
    }
}
