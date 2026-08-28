//! The gRPC surface the API half serves.
//!
//! Two services share this one listener, always: the data plane's cold path
//! (see [`resume`] and `services/api/proto/apiproxy/apiproxy.proto` for the
//! contract) and api's own node-registry heartbeat-receiving plane (task's
//! own "D5" — see `crate::node_registry::grpc_service`). One TCP listener,
//! one port to open in a deployment, two independent-audience gRPC services
//! multiplexed by HTTP/2 path — the same way any two tonic services share a
//! `Server::builder()`.
//!
//! # 🔴 Served by `aenv-api`, and by nothing else
//!
//! `assemble_api` binds [`serve_on`] on `[cluster].api_grpc_addr`.
//! the pre-split single process deliberately does not open a second listener: it is the
//! rollback target and is defined as the process that ran before the split.
//! Under the pre-split single process the wake-up decision is still taken where it always was —
//! `try_auto_resume` on the local reverse proxy's request path — which is what
//! makes rolling back to it a ConfigMap change rather than a code change.

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

/// Serves the wake-up surface and api's node-registry heartbeat plane
/// together, on a listener somebody else bound, until `shutdown` resolves.
///
/// 🔴 A separate listener from the HTTP one, for the same reason the node
/// service is: the two have different audiences. This one is spoken to only
/// by the gateway's cold path and by nodes' dual heartbeat, and a deployment
/// has to be able to expose them differently — the HTTP port carries user
/// traffic, this one carries a decision.
///
/// 🔴 The listener is bound by the caller; see `crate::node_server::serve_on`
/// for why the bind belongs to the assembly and not to a spawned task. It
/// matters more here than there: this surface is the *only* way the gateway's
/// cold path can wake a paused sandbox, and a replica that came up without it
/// answers every wake-up with a connection refused that the gateway reads as a
/// control plane that is merely slow.
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
