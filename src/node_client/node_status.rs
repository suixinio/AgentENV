//! Sets a cluster peer's scheduling status over its node service.

use anyhow::{Context, Result};
use tonic::transport::Endpoint;

use super::native_placement::rewrite_port;
use crate::proto::node::{self as pb, node_sandbox_service_client::NodeSandboxServiceClient};

/// Overrides whether the node advertised at `advertised_endpoint` accepts new
/// placements, dialing its node service on `node_service_port`.
///
/// The advertised address is the node's user-facing HTTP one — the registry
/// hands out no other — so the service port is substituted here, the same
/// rewrite every other node-service caller performs.
///
/// The observed status catches up on the node's next heartbeat, not here.
pub async fn override_node_status(
    advertised_endpoint: &str,
    node_service_port: u16,
    scheduling_disabled: bool,
) -> Result<()> {
    let endpoint = rewrite_port(advertised_endpoint, node_service_port)?;
    let channel = Endpoint::from_shared(endpoint.clone())
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
