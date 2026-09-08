//! API-half gRPC services.
//!
//! One listener serves data-plane resume requests and node-registry heartbeats;
//! only `aenv-api` exposes it.

mod resume;

#[cfg(test)]
mod tests;

use anyhow::Context;
use tracing::info;

use crate::api::ApiImpl;
use crate::node_registry::grpc_service::NodeRegistryGrpcService;
use crate::proto::apiproxy::sandbox_resume_service_server::SandboxResumeServiceServer;
use crate::proto::scheduler::scheduler_server::SchedulerServer;

pub use resume::SandboxResumeService;

/// Builds the tonic server for one API half's wake-up surface.
pub fn server<I>(api_impl: I) -> SandboxResumeServiceServer<SandboxResumeService<I>>
where
    I: AsRef<ApiImpl> + Send + Sync + 'static,
{
    resume::describe_metrics();
    SandboxResumeServiceServer::new(SandboxResumeService::new(api_impl))
}

/// Serves wake-up and node-registry heartbeat services on a caller-bound
/// listener until `shutdown` resolves.
pub async fn serve_on<I>(
    listener: tokio::net::TcpListener,
    api_impl: I,
    node_registry: NodeRegistryGrpcService,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()>
where
    I: AsRef<ApiImpl> + Send + Sync + 'static,
{
    let addr = listener.local_addr().ok();
    info!(
        target: "agentenv", ?addr,
        "serving the sandbox resume service"
    );
    NodeRegistryGrpcService::describe_metrics();
    tonic::transport::Server::builder()
        .add_service(server(api_impl))
        .add_service(SchedulerServer::new(node_registry))
        .serve_with_incoming_shutdown(
            tonic::transport::server::TcpIncoming::from(listener),
            shutdown,
        )
        .await
        .with_context(|| format!("serve the sandbox resume service on {addr:?}"))
}
