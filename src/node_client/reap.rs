//! Deletes one sandbox on a node, for the api half's orphan reaper.

use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use tonic::transport::Endpoint;

use super::native_placement::rewrite_port;
use crate::node_registry::orphan_reaper::NodeSandboxDeleter;
use crate::node_registry::types::Node;
use crate::proto::node as pb;
use crate::types::{ExecutionId, SandboxId};

/// Bounds the dial: an advertised address can be black-holed, and a heartbeat
/// is waiting on this call.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Bounds the delete itself; a node that cannot answer in time keeps the
/// sandbox until the next heartbeat asks again.
const CALL_TIMEOUT: Duration = Duration::from_secs(10);

/// Reaps over the node service, dialing the registry's address on
/// `node_service_port` the way every other node-service caller does.
pub struct NodeServiceSandboxDeleter {
    node_service_port: u16,
}

impl NodeServiceSandboxDeleter {
    pub fn new(node_service_port: u16) -> Self {
        Self { node_service_port }
    }
}

#[async_trait]
impl NodeSandboxDeleter for NodeServiceSandboxDeleter {
    async fn delete(
        &self,
        node: &Node,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
    ) -> Result<()> {
        if node.endpoint.is_empty() {
            anyhow::bail!("node {} has no address to reap through", node.id);
        }
        let endpoint = rewrite_port(&node.endpoint, self.node_service_port)?;
        let channel = Endpoint::from_shared(endpoint.clone())
            .with_context(|| format!("node endpoint {endpoint:?} is not a URI"))?
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(CALL_TIMEOUT)
            .connect()
            .await
            .with_context(|| format!("connect to node service at {endpoint}"))?;
        super::client(channel)
            .delete(pb::SandboxDeleteRequest {
                sandbox_id: sandbox_id.to_string(),
                execution_id: execution_id.to_string(),
            })
            .await
            .map(|_| ())
            .with_context(|| {
                format!(
                    "delete sandbox {sandbox_id} run {execution_id} on node {}",
                    node.id
                )
            })
    }
}
