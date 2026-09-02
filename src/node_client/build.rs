//! Dispatches a template build to a selected node and returns its staged snapshot row.

use std::time::Duration;

use prost::Message as _;
use tonic::transport::Endpoint;
use tracing::info;

use crate::proto::node::{self as pb, node_sandbox_service_client::NodeSandboxServiceClient};
use crate::snapshot::repository::StagedSnapshot;
use crate::snapshot::TemplateBuildErrorReason;
use crate::types::{SandboxId, SandboxResources};

use super::placement::NodePlacement;
use super::wire;

/// Exceeds the build runner's own readiness timeout.
const BUILD_CALL_TIMEOUT: Duration = Duration::from_secs(20 * 60);

/// Fails unreachable-node dialing well before the build-call budget.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Keeps the long-lived build RPC active across idle connection reapers.
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);
const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(10);

/// Runs a template build and returns the node-staged, uncommitted row.
///
/// Structured failure details preserve the failing build step.
pub async fn build_template_on_a_node(
    placement: &dyn NodePlacement,
    resources: SandboxResources,
    request: pb::TemplateBuildRequest,
) -> Result<StagedSnapshot, TemplateBuildErrorReason> {
    // Builds have no sandbox ID; placement implementations ignore this synthetic one.
    let sandbox_id = SandboxId::new();
    let node = placement
        .place_new(sandbox_id, resources, None)
        .await
        .map_err(|err| {
            TemplateBuildErrorReason::new(format!("choose a node for a template build: {err:#}"))
        })?;

    // Preserve the executor node in logs; the build lease records the administering replica.
    let build_id = request.build_snapshot_id.clone();
    info!(
        %build_id,
        node_id = %node.node_id,
        endpoint = %node.endpoint,
        "dispatching template build to node"
    );

    let channel = Endpoint::from_shared(node.endpoint.clone())
        .map_err(|err| {
            TemplateBuildErrorReason::new(format!(
                "node endpoint {:?} is not a URI: {err}",
                node.endpoint
            ))
        })?
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(BUILD_CALL_TIMEOUT)
        .http2_keep_alive_interval(KEEPALIVE_INTERVAL)
        .keep_alive_timeout(KEEPALIVE_TIMEOUT)
        .keep_alive_while_idle(true)
        .connect()
        .await
        .map_err(|err| {
            TemplateBuildErrorReason::new(format!(
                "connect to node {} at {}: {err}",
                node.node_id, node.endpoint
            ))
        })?;

    let mut client = NodeSandboxServiceClient::new(channel);
    let response = client
        .build_template(request)
        .await
        .map_err(|status| build_failure_reason(&status, &node.node_id))?
        .into_inner();

    let staged = response.staged.and_then(|staged| staged.value);
    let staged = wire::serialized(staged.as_ref(), "staged template build")
        .map_err(|err| {
            TemplateBuildErrorReason::new(format!(
                "decode the staged template build node {} returned: {err:#}",
                node.node_id
            ))
        })?
        .ok_or_else(|| {
            TemplateBuildErrorReason::new(format!(
                "node {} answered a template build with no staged snapshot",
                node.node_id
            ))
        })?;

    info!(
        %build_id,
        node_id = %node.node_id,
        "node returned the staged template build"
    );
    Ok(staged)
}

/// Converts a failed build status, including any structured step detail.
fn build_failure_reason(status: &tonic::Status, node_id: &str) -> TemplateBuildErrorReason {
    let message = format!("node {node_id} refused the template build: {status}");
    let step = (!status.details().is_empty())
        .then(|| pb::TemplateBuildFailureDetail::decode(status.details()).ok())
        .flatten()
        .map(|detail| detail.step)
        .filter(|step| !step.is_empty());
    match step {
        Some(step) => TemplateBuildErrorReason::with_step(message, step),
        None => TemplateBuildErrorReason::new(message),
    }
}
