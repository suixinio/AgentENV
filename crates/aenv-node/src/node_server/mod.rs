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
//! # 🔴 Served by `--role node`, and by nothing else
//!
//! `assemble_node` binds [`serve_on`] on `[cluster].node_service_addr`.
//! `--role all` deliberately does not: it is the rollback target and is defined
//! as the process that ran before the split, which listened on one port.
//!
//! 🔴 That has a consequence for the shadow phase, and it is not a small one.
//! §11.2's 3a keeps the DaemonSet on `--role all` while the API half drives it
//! through this service — and a `--role all` node does not serve this service.
//! So the API half can decide, and can serve the wake-up surface, but has no
//! machine it can drive until the DaemonSet moves to `--role node`.

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

/// How often this server pings an otherwise-quiet HTTP/2 connection, and how
/// long it waits for the reply before dropping it.
///
/// 🔴 Exists for one RPC on this service — `BuildTemplate` — which can sit
/// with nothing on the wire for most of ten minutes while a build sandbox
/// runs. Every other call here is a handful of round trips and would never
/// notice this setting either way, so applying it to the whole server rather
/// than one route costs those calls nothing: see
/// `src/node_client/build.rs`'s matching client-side constants for the other
/// half of the argument.
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);
const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(10);

/// Reachable from `crate::node_client`'s tests, which drive a create through
/// the wire and then ask this side whether the sandbox came out owned.
///
/// 🔴 Test-only, because the production consumer is `service.rs` next door and
/// a crate-wide export would invite a second one — and "who counts as the
/// control plane's" having two callers is how the answer comes to differ
/// between them.
#[cfg(test)]
pub use ownership::owned_by_control_plane;

/// Builds the tonic server for one node's orchestrator.
///
/// 🔴 Always wired for `BuildTemplate`: every `--role node` process has its
/// own `ImageResolver` and `TemplateBuilder` regardless of whether a template
/// build ever reaches it (`assemble_node_core` builds both unconditionally,
/// the same as `--role all` always has), so there is no configuration under
/// which this server should answer `Unimplemented` for it. See
/// `NodeSandboxService::with_template_build`'s doc for why the *type* still
/// allows a service with neither wired — that is for this function's own
/// tests, not for production.
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

/// Serves the node service on a listener somebody else bound, until `shutdown`
/// resolves.
///
/// 🔴 A separate listener from the HTTP one, rather than a route on it. The two
/// have different audiences — this one is spoken to only by the API half, and
/// the HTTP port is spoken to by users and by the gateway — and a deployment
/// has to be able to expose them differently.
///
/// 🔴 The listener is bound by the caller, and there is deliberately no
/// variant that takes an address and binds here. Binding inside this future
/// means an assembly that spawns it learns nothing: the port being taken shows
/// up as a task that ended, and the process goes on running with a surface the
/// API half cannot reach — which from the outside is indistinguishable from an
/// API half that has nothing to say. Bound by the assembly, that is a process
/// that does not start.
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
