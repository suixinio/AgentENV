//! Node gRPC surface used by the API half to execute sandbox operations.
//!
//! Only identifiers and durable facts cross this boundary; VM handles,
//! temporary-directory guards, and node-local paths remain on the node.
//! `aenv-node` serves this surface on its dedicated node-service listener.

mod convert;
mod ownership;
mod service;

#[cfg(test)]
mod tests;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use tracing::info;

use crate::image::ImageResolver;
use crate::orchestrator::SandboxOrchestration;
use crate::proto::node::node_sandbox_service_server::NodeSandboxServiceServer;
use crate::snapshot::SnapshotManager;
use crate::template::TemplateBuilder;

pub use service::NodeSandboxService;

// Keep quiet long-running template-build RPCs alive; clients use matching bounds.
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);
const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(10);

/// Test-only ownership predicate shared with node-client wire tests.
#[cfg(test)]
pub use ownership::owned_by_control_plane;

/// Builds the node gRPC server with template-building support.
pub fn server(
    orchestration: Arc<dyn SandboxOrchestration>,
    snapshots: Arc<SnapshotManager>,
    node_id: String,
    image_resolver: Arc<ImageResolver>,
    template_builder: Arc<TemplateBuilder>,
) -> NodeSandboxServiceServer<NodeSandboxService> {
    NodeSandboxServiceServer::new(
        NodeSandboxService::new(orchestration, snapshots, node_id)
            .with_template_build(image_resolver, template_builder),
    )
}

/// Serves on a caller-bound listener until shutdown.
///
/// Binding remains with process assembly so a port conflict prevents startup.
#[allow(clippy::too_many_arguments)]
pub async fn serve_on(
    listener: tokio::net::TcpListener,
    orchestration: Arc<dyn SandboxOrchestration>,
    snapshots: Arc<SnapshotManager>,
    node_id: String,
    image_resolver: Arc<ImageResolver>,
    template_builder: Arc<TemplateBuilder>,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    let addr = listener.local_addr().ok();
    info!(target: "agentenv", ?addr, "serving the node sandbox service");
    tonic::transport::Server::builder()
        .http2_keepalive_interval(Some(KEEPALIVE_INTERVAL))
        .http2_keepalive_timeout(Some(KEEPALIVE_TIMEOUT))
        .add_service(server(
            orchestration,
            snapshots,
            node_id,
            image_resolver,
            template_builder,
        ))
        .serve_with_incoming_shutdown(
            tonic::transport::server::TcpIncoming::from(listener),
            shutdown,
        )
        .await
        .with_context(|| format!("serve the node sandbox service on {addr:?}"))
}
