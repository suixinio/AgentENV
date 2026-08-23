//! The gRPC surface the API half serves.
//!
//! One service, one method: the data plane's cold path. See
//! [`resume`] for what it decides and `services/api/proto/apiproxy/apiproxy.proto`
//! for the contract.
//!
//! # 🔴 Served by `--role api`, and by nothing else
//!
//! `assemble_api` binds [`serve_on`] on `[cluster].api_grpc_addr`.
//! `--role all` deliberately does not open a second listener: it is the
//! rollback target and is defined as the process that ran before the split.
//! Under `--role all` the wake-up decision is still taken where it always was —
//! `try_auto_resume` on the local reverse proxy's request path — which is what
//! makes rolling back to it a ConfigMap change rather than a code change.

mod resume;

#[cfg(test)]
mod tests;

use std::net::SocketAddr;

use anyhow::Context;
use tracing::info;

use crate::api::ApiImpl;
use crate::proto::apiproxy::sandbox_resume_service_server::SandboxResumeServiceServer;

pub use resume::SandboxResumeService;

/// Builds the tonic server for one API half's wake-up surface.
pub fn server<I>(api_impl: I) -> SandboxResumeServiceServer<SandboxResumeService<I>>
where
    I: AsRef<ApiImpl> + Send + Sync + 'static,
{
    resume::describe_metrics();
    SandboxResumeServiceServer::new(SandboxResumeService::new(api_impl))
}

/// Serves the wake-up surface on `addr` until `shutdown` resolves.
///
/// 🔴 A separate listener from the HTTP one, for the same reason the node
/// service is: the two have different audiences. This one is spoken to only by
/// the gateway's cold path, and a deployment has to be able to expose them
/// differently — the HTTP port carries user traffic, this one carries a
/// decision.
pub async fn serve<I>(
    addr: SocketAddr,
    api_impl: I,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()>
where
    I: AsRef<ApiImpl> + Send + Sync + 'static,
{
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("bind the sandbox resume service to {addr}"))?;
    serve_on(listener, api_impl, shutdown).await
}

/// Serves the wake-up surface on a listener somebody else bound.
///
/// 🔴 The variant a binary uses; see `crate::node_server::serve_on` for why the
/// bind belongs to the assembly and not to a spawned task. It matters more here
/// than there: this surface is the *only* way the gateway's cold path can wake
/// a paused sandbox, and a replica that came up without it answers every
/// wake-up with a connection refused that the gateway reads as a control plane
/// that is merely slow.
pub async fn serve_on<I>(
    listener: tokio::net::TcpListener,
    api_impl: I,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()>
where
    I: AsRef<ApiImpl> + Send + Sync + 'static,
{
    let addr = listener.local_addr().ok();
    info!(target: "agentenv", ?addr, "serving the sandbox resume service");
    tonic::transport::Server::builder()
        .add_service(server(api_impl))
        .serve_with_incoming_shutdown(
            tonic::transport::server::TcpIncoming::from(listener),
            shutdown,
        )
        .await
        .with_context(|| format!("serve the sandbox resume service on {addr:?}"))
}
