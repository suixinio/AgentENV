//! Whether the cluster still routes traffic to a sandbox's runtime.
//!
//! A record saying `Running` and a handle held in this process are both this
//! replica's memory. The routing binding is the cluster's answer, and it is the
//! only one that survives the node that held the sandbox going away.

use async_trait::async_trait;

use crate::types::{ExecutionId, SandboxId};

#[async_trait]
pub trait RuntimeRouting: Send + Sync {
    /// `Ok(false)` is a verdict that nothing routes to this sandbox any more.
    /// `Err` is unknown and must never be read as absence.
    async fn is_routed(&self, sandbox_id: SandboxId) -> anyhow::Result<bool>;

    /// Retires the routing answer for one incarnation, so that no request is
    /// routed at a runtime this teardown is about to remove.
    ///
    /// Fenced on `execution_id`: an answer naming another incarnation belongs
    /// to that incarnation and is left alone.
    async fn forget(&self, sandbox_id: SandboxId, execution_id: ExecutionId) -> anyhow::Result<()>;
}
