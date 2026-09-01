//! Sets a cluster peer's scheduling status over its node service.

use std::time::Duration;

use anyhow::{Context, Result};
use tonic::transport::Endpoint;

use super::native_placement::rewrite_port;
use crate::proto::node::{self as pb, node_sandbox_service_client::NodeSandboxServiceClient};

/// Bounds the dial: an advertised address can be black-holed, and nothing
/// above this call bounds the request.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Bounds the override call itself; the node answers it from memory.
const CALL_TIMEOUT: Duration = Duration::from_secs(10);

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
    override_node_status_with_timeouts(
        advertised_endpoint,
        node_service_port,
        scheduling_disabled,
        CONNECT_TIMEOUT,
        CALL_TIMEOUT,
    )
    .await
}

async fn override_node_status_with_timeouts(
    advertised_endpoint: &str,
    node_service_port: u16,
    scheduling_disabled: bool,
    connect_timeout: Duration,
    call_timeout: Duration,
) -> Result<()> {
    let endpoint = rewrite_port(advertised_endpoint, node_service_port)?;
    let channel = Endpoint::from_shared(endpoint.clone())
        .with_context(|| format!("node endpoint {endpoint:?} is not a URI"))?
        .connect_timeout(connect_timeout)
        .timeout(call_timeout)
        .connect()
        .await
        .with_context(|| {
            format!("connect to node service at {endpoint} (connect timeout {connect_timeout:?})")
        })?;
    let mut client = NodeSandboxServiceClient::new(channel);
    client
        .override_status(pb::NodeStatusOverrideRequest {
            scheduling_disabled,
        })
        .await
        .with_context(|| {
            format!("override node status at {endpoint} (call timeout {call_timeout:?})")
        })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::proto::node as pb;
    use pb::node_sandbox_service_server::{NodeSandboxService, NodeSandboxServiceServer};

    /// A node service whose override never answers.
    struct SilentNode;

    #[tonic::async_trait]
    impl NodeSandboxService for SilentNode {
        async fn override_status(
            &self,
            _request: tonic::Request<pb::NodeStatusOverrideRequest>,
        ) -> Result<tonic::Response<pb::NodeStatusOverrideResponse>, tonic::Status> {
            std::future::pending().await
        }

        async fn create(
            &self,
            _request: tonic::Request<pb::SandboxCreateRequest>,
        ) -> Result<tonic::Response<pb::SandboxCreateResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("override only"))
        }
        async fn delete(
            &self,
            _request: tonic::Request<pb::SandboxDeleteRequest>,
        ) -> Result<tonic::Response<pb::SandboxDeleteResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("override only"))
        }
        async fn pause(
            &self,
            _request: tonic::Request<pb::SandboxPauseRequest>,
        ) -> Result<tonic::Response<pb::SandboxPauseResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("override only"))
        }
        async fn checkpoint(
            &self,
            _request: tonic::Request<pb::SandboxCheckpointRequest>,
        ) -> Result<tonic::Response<pb::SandboxCheckpointResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("override only"))
        }
        async fn resume(
            &self,
            _request: tonic::Request<pb::SandboxResumeRequest>,
        ) -> Result<tonic::Response<pb::SandboxResumeResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("override only"))
        }
        async fn fork(
            &self,
            _request: tonic::Request<pb::SandboxForkRequest>,
        ) -> Result<tonic::Response<pb::SandboxForkResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("override only"))
        }
        async fn update_network(
            &self,
            _request: tonic::Request<pb::SandboxNetworkRequest>,
        ) -> Result<tonic::Response<pb::SandboxNetworkResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("override only"))
        }
        async fn update_params(
            &self,
            _request: tonic::Request<pb::SandboxParamsRequest>,
        ) -> Result<tonic::Response<pb::SandboxParamsResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("override only"))
        }
        async fn describe(
            &self,
            _request: tonic::Request<pb::SandboxDescribeRequest>,
        ) -> Result<tonic::Response<pb::SandboxDescribeResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("override only"))
        }
        async fn list_sandboxes(
            &self,
            _request: tonic::Request<pb::ListSandboxesRequest>,
        ) -> Result<tonic::Response<pb::SandboxListResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("override only"))
        }
        async fn build_template(
            &self,
            _request: tonic::Request<pb::TemplateBuildRequest>,
        ) -> Result<tonic::Response<pb::TemplateBuildResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("override only"))
        }
    }

    #[tokio::test]
    async fn an_override_the_node_never_answers_fails_within_the_call_timeout() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind a port");
        let node_service_port = listener.local_addr().expect("the bound address").port();
        let (shutdown, rx) = tokio::sync::oneshot::channel::<()>();
        tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(NodeSandboxServiceServer::new(SilentNode))
                .serve_with_incoming_shutdown(
                    tonic::transport::server::TcpIncoming::from(listener),
                    async {
                        let _ = rx.await;
                    },
                )
                .await;
        });

        let outcome = tokio::time::timeout(
            Duration::from_secs(2),
            super::override_node_status_with_timeouts(
                "http://127.0.0.1:1",
                node_service_port,
                true,
                Duration::from_secs(1),
                Duration::from_millis(200),
            ),
        )
        .await;
        let _ = shutdown.send(());

        let result = outcome.expect("the call timeout must fire before the 2 s wrapper does");
        let err = result.expect_err("a node that never answers is an error, not a hang");
        assert!(
            format!("{err:#}").contains("call timeout"),
            "the context must name the timeout, got: {err:#}"
        );
    }
}
