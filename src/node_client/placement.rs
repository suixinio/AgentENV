//! Selects nodes for new sandboxes, resolves existing sandbox holders, and records
//! completed placement. Existing placement is distinct because local captures may be
//! pinned to exactly one node.

use async_trait::async_trait;

use crate::types::{ExecutionId, SandboxId, SandboxResources};

/// Node identity plus advertised and sandbox-service addresses.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeEndpoint {
    pub node_id: String,
    /// Sandbox-service URI dialed by this client.
    pub endpoint: String,
    /// Original discovery address used for placement records and cluster routing.
    pub advertised_endpoint: String,
}

impl NodeEndpoint {
    /// Constructs a node whose dial and advertised addresses are identical.
    pub fn same_address(node_id: impl Into<String>, endpoint: impl Into<String>) -> Self {
        let endpoint = endpoint.into();
        Self {
            node_id: node_id.into(),
            advertised_endpoint: endpoint.clone(),
            endpoint,
        }
    }
}

/// Discovery-backed membership for a node identity already held by the caller.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeMembership {
    /// Discovery still lists the node.
    Present,
    /// Discovery no longer lists the node.
    Gone,
}

#[async_trait]
pub trait NodePlacement: Send + Sync + 'static {
    /// Selects a node for a new sandbox.
    async fn place_new(
        &self,
        sandbox_id: SandboxId,
        resources: SandboxResources,
    ) -> anyhow::Result<NodeEndpoint>;

    /// Locates the node holding an existing sandbox.
    ///
    /// `Ok(None)` means a complete lookup found no record; `Err` means lookup failed.
    async fn place_existing(&self, sandbox_id: SandboxId) -> anyhow::Result<Option<NodeEndpoint>>;

    /// Resolves the current address of an already-selected node identity.
    async fn resolve_node(&self, node_id: &str) -> anyhow::Result<NodeEndpoint>;

    /// Reports whether discovery still lists an already-selected node.
    ///
    /// Callers must treat errors as unknown, never as [`NodeMembership::Gone`].
    async fn node_membership(&self, node_id: &str) -> anyhow::Result<NodeMembership>;

    /// Best-effort records a completed placement; heartbeats repair failed writes.
    async fn record_placement(
        &self,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
        node: &NodeEndpoint,
    ) -> anyhow::Result<()>;
}

/// Sends every operation to one fixed node.
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

    /// Always returns the fixed node.
    async fn place_existing(&self, _sandbox_id: SandboxId) -> anyhow::Result<Option<NodeEndpoint>> {
        Ok(Some(self.node.clone()))
    }

    /// Resolves any identity to the fixed node; callers enforce identity constraints.
    async fn resolve_node(&self, _node_id: &str) -> anyhow::Result<NodeEndpoint> {
        Ok(self.node.clone())
    }

    /// Always reports the fixed node present.
    async fn node_membership(&self, _node_id: &str) -> anyhow::Result<NodeMembership> {
        Ok(NodeMembership::Present)
    }

    /// Does nothing because fixed placement has no mutable index.
    async fn record_placement(
        &self,
        _sandbox_id: SandboxId,
        _execution_id: ExecutionId,
        _node: &NodeEndpoint,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}
