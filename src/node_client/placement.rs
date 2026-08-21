//! Deciding which node a sandbox goes to.
//!
//! # 🔴 Two questions, not one
//!
//! Starting a fresh sandbox asks *which machine has room*. Bringing a paused
//! one back asks *where this sandbox may go*, and those are different questions
//! with different answers: a paused sandbox whose bytes are only on the disk of
//! the node that paused it can go to exactly one machine, and asking the first
//! question about it would place it somewhere its bytes are not.
//!
//! Both are answered by the cluster scheduler in the assembled system. The
//! trait is here so that the piece which drives sandboxes over the wire does
//! not also have to know how placement is decided.

use async_trait::async_trait;

use crate::types::{SandboxId, SandboxResources};

/// One node this client can talk to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeEndpoint {
    pub node_id: String,
    /// The gRPC address of the node service, as a URI.
    pub endpoint: String,
}

#[async_trait]
pub trait NodePlacement: Send + Sync + 'static {
    /// A node with room for a new sandbox.
    async fn place_new(
        &self,
        sandbox_id: SandboxId,
        resources: SandboxResources,
    ) -> anyhow::Result<NodeEndpoint>;

    /// The node a sandbox that already exists must be driven on.
    ///
    /// 🔴 Separate from [`place_new`](Self::place_new) because the answer may
    /// be pinned rather than preferred. A caller that used the other one here
    /// would send a resume to a machine that does not have the bytes.
    async fn place_existing(&self, sandbox_id: SandboxId) -> anyhow::Result<NodeEndpoint>;
}

/// Sends everything to one node.
///
/// For tests, and for a single-node deployment where the question has one
/// answer.
pub struct FixedNodePlacement {
    node: NodeEndpoint,
}

impl FixedNodePlacement {
    pub fn new(node: NodeEndpoint) -> Self {
        Self { node }
    }
}

#[async_trait]
impl NodePlacement for FixedNodePlacement {
    async fn place_new(
        &self,
        _sandbox_id: SandboxId,
        _resources: SandboxResources,
    ) -> anyhow::Result<NodeEndpoint> {
        Ok(self.node.clone())
    }

    async fn place_existing(&self, _sandbox_id: SandboxId) -> anyhow::Result<NodeEndpoint> {
        Ok(self.node.clone())
    }
}
