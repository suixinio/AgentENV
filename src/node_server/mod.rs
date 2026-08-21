//! The gRPC surface one node serves to the API half of the split control plane.
//!
//! # Transport
//!
//! tonic, over the proto in `services/api/proto/node.proto`. Nothing new was
//! built for it: the crate already generates a gRPC client and server from
//! `scheduler.proto` through the same `build.rs`, and the calls this service
//! carries are the same shape as that one's — a command, a wait, a result.
//!
//! # 🔴 What this service is for, and what it is not for
//!
//! It exists so that a process with no `/dev/kvm` can run sandboxes on a
//! machine that has one. It is *not* a remote form of
//! [`SandboxBackend`][crate::sandbox::SandboxBackend]: three of that trait's
//! return types own live local state, two of them keep a temporary directory
//! alive, and a fourth is a list of paths on one particular disk. What crosses
//! this wire is identifiers and facts, and where the local trait hands back
//! something holding bytes, this one hands back where the bytes already are.
//!
//! # 🔴 Not wired into any binary yet
//!
//! `--role node` does not serve this. The role gate that stops a node's HTTP
//! port answering user-facing REST, and the startup reclaim of host leftovers,
//! land before a node is deployable at all — so a listener here would be a way
//! to turn the split on early. [`serve`] exists and is exercised by tests;
//! nothing in `src/bin/` calls it.

mod convert;
mod ownership;
mod service;

#[cfg(test)]
mod tests;

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Context;
use tracing::info;

use crate::orchestrator::SandboxOrchestration;
use crate::proto::node::node_sandbox_service_server::NodeSandboxServiceServer;
use crate::snapshot::SnapshotManager;

pub use service::NodeSandboxService;

/// Builds the tonic server for one node's orchestrator.
pub(crate) fn server(
    orchestration: Arc<dyn SandboxOrchestration>,
    snapshots: Arc<SnapshotManager>,
    node_id: String,
) -> NodeSandboxServiceServer<NodeSandboxService> {
    NodeSandboxServiceServer::new(NodeSandboxService::new(orchestration, snapshots, node_id))
}

/// Serves the node service on `addr` until `shutdown` resolves.
///
/// 🔴 A separate listener from the HTTP one, rather than a route on it. The two
/// have different audiences — this one is spoken to only by the API half, and
/// the HTTP port is spoken to by users and by the gateway — and a deployment
/// has to be able to expose them differently.
pub async fn serve(
    addr: SocketAddr,
    orchestration: Arc<dyn SandboxOrchestration>,
    snapshots: Arc<SnapshotManager>,
    node_id: String,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    info!(target: "agentenv", %addr, "serving the node sandbox service");
    tonic::transport::Server::builder()
        .add_service(server(orchestration, snapshots, node_id))
        .serve_with_shutdown(addr, shutdown)
        .await
        .with_context(|| format!("serve the node sandbox service on {addr}"))
}
