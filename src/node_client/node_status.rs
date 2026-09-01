//! Sets a cluster peer's scheduling status over its node service.

use anyhow::{Context, Result};
use tonic::transport::Endpoint;

use crate::proto::node::{self as pb, node_sandbox_service_client::NodeSandboxServiceClient};

/// Overrides whether the node at `endpoint` accepts new placements.
///
/// The observed status catches up on the node's next heartbeat, not here.
pub async fn override_node_status(endpoint: &str, scheduling_disabled: bool) -> Result<()> {
    let channel = Endpoint::from_shared(endpoint.to_string())
        .with_context(|| format!("node endpoint {endpoint:?} is not a URI"))?
        .connect()
        .await
        .with_context(|| format!("connect to node service at {endpoint}"))?;
    let mut client = NodeSandboxServiceClient::new(channel);
    client
        .override_status(pb::NodeStatusOverrideRequest {
            scheduling_disabled,
        })
        .await
        .with_context(|| format!("override node status at {endpoint}"))?;
    Ok(())
}
