//! Node gRPC surface used by the API half to execute sandbox operations.
//!
//! Only identifiers and durable facts cross this boundary; VM handles,
//! temporary-directory guards, and node-local paths remain on the node.
//! `aenv-node` serves this surface on its dedicated node-service listener.

mod convert;
mod gate;
mod ownership;
mod service;

#[cfg(test)]
mod tests;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use tracing::info;

use crate::image::ImageResolver;
use crate::orchestrator::NodeOrchestration;
use crate::proto::node::node_sandbox_service_server::NodeSandboxServiceServer;
use crate::snapshot::SnapshotManager;
use crate::template::TemplateBuilder;

pub use gate::NodeGrpcGate;

/// The node service as it is served: every RPC passes the credential gate.
pub type GatedNodeService = tonic::service::interceptor::InterceptedService<
    NodeSandboxServiceServer<NodeSandboxService>,
    NodeGrpcGate,
>;

pub use service::NodeSandboxService;

// Keep quiet long-running template-build RPCs alive; clients use matching bounds.
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);
const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(10);

/// The reconciliation predicate `service.rs` filters a roster with.
pub use ownership::owned_by_control_plane;

/// Builds the node gRPC server with template-building support, behind the
/// credential gate: the surface is reachable from inside a sandbox namespace.
pub fn server(
    orchestration: Arc<dyn NodeOrchestration>,
    snapshots: Arc<SnapshotManager>,
    node_id: String,
    image_resolver: Arc<ImageResolver>,
    template_builder: Arc<TemplateBuilder>,
) -> GatedNodeService {
    server_with_gate(
        orchestration,
        snapshots,
        node_id,
        image_resolver,
        template_builder,
        NodeGrpcGate::from_global_config(),
    )
}

/// The same server behind credentials the caller resolved itself.
pub fn server_with_gate(
    orchestration: Arc<dyn NodeOrchestration>,
    snapshots: Arc<SnapshotManager>,
    node_id: String,
    image_resolver: Arc<ImageResolver>,
    template_builder: Arc<TemplateBuilder>,
    gate: NodeGrpcGate,
) -> GatedNodeService {
    NodeSandboxServiceServer::with_interceptor(
        NodeSandboxService::new(orchestration, snapshots, node_id)
            .with_template_build(image_resolver, template_builder),
        gate,
    )
}

/// Serves on a caller-bound listener until shutdown.
///
/// Binding remains with process assembly so a port conflict prevents startup.
#[allow(clippy::too_many_arguments)]
pub async fn serve_on(
    listener: tokio::net::TcpListener,
    orchestration: Arc<dyn NodeOrchestration>,
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
