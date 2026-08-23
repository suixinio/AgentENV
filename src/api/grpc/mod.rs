//! The gRPC surface the API half serves.
//!
//! One service, one method: the data plane's cold path. See
//! [`resume`] for what it decides and `services/api/proto/apiproxy/apiproxy.proto`
//! for the contract.
//!
//! # 🔴 Not served by any binary yet
//!
//! `--role api` cannot be assembled — it needs a cluster metadata store and the
//! remote backend factory, and neither has landed (`src/bin/server.rs`,
//! `assemble_api`) — and `--role all` deliberately does not open a second
//! listener, because it is the rollback target and is defined as today's
//! behaviour verbatim. So [`serve`] exists and is exercised by tests over a
//! real socket; nothing in `src/bin/` calls it.
//!
//! That is the same position `src/node_server/` was landed in, and for the same
//! reason: the transport is the last thing to be wired, not the first.

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
    info!(target: "agentenv", %addr, "serving the sandbox resume service");
    tonic::transport::Server::builder()
        .add_service(server(api_impl))
        .serve_with_shutdown(addr, shutdown)
        .await
        .with_context(|| format!("serve the sandbox resume service on {addr}"))
}
