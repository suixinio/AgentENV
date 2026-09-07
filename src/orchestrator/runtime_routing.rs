//! Whether the cluster still routes traffic to a sandbox's runtime.
//!
//! A record saying `Running` and a handle held in this process are both this
//! replica's memory. The routing binding is the cluster's answer, and it is the
//! only one that survives the node that held the sandbox going away.

use async_trait::async_trait;

use crate::types::SandboxId;

#[async_trait]
pub trait RuntimeRouting: Send + Sync {
    /// `Ok(false)` is a verdict that nothing routes to this sandbox any more.
    /// `Err` is unknown and must never be read as absence.
    async fn is_routed(&self, sandbox_id: SandboxId) -> anyhow::Result<bool>;
}
