//! Asking the cluster scheduler where a sandbox goes.
//!
//! # 🔴 Two questions, two RPCs, and they are not interchangeable
//!
//! `Schedule` answers *which machine has room*; `LookupNode` answers *where
//! this sandbox may go*. Using the first where the second belongs places a
//! paused sandbox on a machine its bytes are not on — which does not fail, it
//! rebuilds the sandbox from an older snapshot and answers 200. See
//! [`NodePlacement`] for the general form of the argument and
//! `crate::api::impls::resume_surface` for the pin/prefer decision the resume
//! path takes on top of the same RPC.

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use tonic::transport::{Channel, Endpoint};

use crate::proto::scheduler::{self, scheduler_client::SchedulerClient};
use crate::types::{SandboxId, SandboxResources};

use super::placement::{NodeEndpoint, NodePlacement};

/// Placement backed by the cluster scheduler.
pub struct SchedulerNodePlacement {
    channel: Channel,
    /// The port the node sandbox service listens on.
    ///
    /// 🔴 Substituted into the address the scheduler answers with, rather than
    /// configured as a second endpoint list. The scheduler names a node by its
    /// user-facing HTTP address — that is what the gateway proxies to and what
    /// node discovery produces — and the node service is a different port of
    /// the same machine. A second list would be a second thing to keep in step
    /// with the first, and the failure of letting them drift is a create sent
    /// to a machine that is not the one the scheduler chose.
    node_service_port: u16,
}

impl SchedulerNodePlacement {
    /// Connects lazily: `connect_lazy` opens no socket, so a scheduler that is
    /// down at startup delays a create rather than a process.
    pub fn connect_lazy(endpoint: &str, node_service_port: u16) -> Result<Self> {
        let channel = Endpoint::from_shared(qualified(endpoint))
            .with_context(|| format!("scheduler endpoint {endpoint:?} is not a valid URI"))?
            .connect_lazy();
        Ok(Self {
            channel,
            node_service_port,
        })
    }

    fn client(&self) -> SchedulerClient<Channel> {
        SchedulerClient::new(self.channel.clone())
    }

    /// Turns the scheduler's answer into the node service's address.
    fn node_service_endpoint(&self, node: Option<scheduler::Node>) -> Result<NodeEndpoint> {
        let node = node.ok_or_else(|| anyhow!("the scheduler named no node"))?;
        if node.node_id.is_empty() {
            bail!("the scheduler named a node with no id");
        }
        // 🔴 Refused rather than defaulted to the node id as a hostname. A
        // scheduler that knows a node's identity but not its address is
        // answering half a question, and half an answer is not somewhere to
        // send a create.
        if node.endpoint.is_empty() {
            bail!("the scheduler named node {} with no address", node.node_id);
        }
        Ok(NodeEndpoint {
            endpoint: rewrite_port(&node.endpoint, self.node_service_port)?,
            node_id: node.node_id,
        })
    }
}

#[async_trait]
impl NodePlacement for SchedulerNodePlacement {
    async fn place_new(
        &self,
        _sandbox_id: SandboxId,
        _resources: SandboxResources,
    ) -> Result<NodeEndpoint> {
        // 🔴 `NewSandboxHint` and not `NewColdSandboxHint`, even though the
        // resources are in hand. The two hints name the two request shapes the
        // scheduler knows — `POST /sandboxes` and `POST /sandboxes-cold` — and
        // this factory only ever builds from a snapshot, because its cold arm
        // refuses. Sending the cold hint would tell the scheduler to weigh
        // image locality for images this create is not going to pull.
        //
        // The consequence is worth stating: the resources this create needs do
        // not reach the scheduler, so placement is taken without them. That is
        // what `POST /sandboxes` has always done — the hint carries only
        // metadata — and it is the scheduler's shape to widen, not this
        // caller's to work around.
        let response = self
            .client()
            .schedule(scheduler::ScheduleRequest {
                hint: Some(scheduler::ScheduleRequestHint {
                    kind: Some(scheduler::schedule_request_hint::Kind::NewSandbox(
                        scheduler::NewSandboxHint {
                            metadata: Default::default(),
                        },
                    )),
                }),
            })
            .await
            .map_err(|status| anyhow!("the scheduler refused to place a sandbox: {status}"))?
            .into_inner();
        self.node_service_endpoint(response.node)
    }

    async fn place_existing(&self, sandbox_id: SandboxId) -> Result<NodeEndpoint> {
        let response = self
            .client()
            .lookup_node(scheduler::LookupNodeRequest {
                sandbox_id: sandbox_id.to_string(),
            })
            .await
            .map_err(|status| anyhow!("the scheduler could not locate {sandbox_id}: {status}"))?
            .into_inner();
        // 🔴 The `SandboxLocation` is read by the resume surface, which decides
        // whether a pin may be honoured, and *not* here: this trait's job is to
        // name a machine, and re-deciding pin-versus-prefer in a second place
        // is how the two answers come to disagree. What this refuses is only
        // an answer with no machine in it.
        self.node_service_endpoint(response.node)
    }
}

fn qualified(endpoint: &str) -> String {
    if endpoint.contains("://") {
        endpoint.to_string()
    } else {
        format!("http://{endpoint}")
    }
}

/// Replaces the port in an `scheme://host:port` address, keeping everything
/// else the scheduler said.
///
/// 🔴 Parsed rather than string-spliced, because an IPv6 literal is written
/// `http://[::1]:8000` and the last colon in it is not the one before the
/// port on any naive reading that also has to cope with `http://[::1]`.
fn rewrite_port(endpoint: &str, port: u16) -> Result<String> {
    let mut url = url::Url::parse(&qualified(endpoint))
        .with_context(|| format!("node address {endpoint:?} is not a valid URI"))?;
    url.set_port(Some(port))
        .map_err(|()| anyhow!("node address {endpoint:?} has no host to put a port on"))?;
    Ok(url.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_scheduler_names_a_http_port_and_the_node_service_is_on_another() {
        assert_eq!(
            rewrite_port("http://10.0.0.7:8000", 8001).unwrap(),
            "http://10.0.0.7:8001/"
        );
        // A bare host:port is what a static discovery list carries.
        assert_eq!(
            rewrite_port("10.0.0.7:8000", 8001).unwrap(),
            "http://10.0.0.7:8001/"
        );
        // 🔴 The control for the two above: an IPv6 literal, where the last
        // colon in the string is inside the address rather than before the
        // port. A splice on the last colon passes both cases above and turns
        // this one into an address that does not resolve.
        assert_eq!(
            rewrite_port("http://[fd00::7]:8000", 8001).unwrap(),
            "http://[fd00::7]:8001/"
        );
    }

    #[test]
    fn an_address_with_no_host_is_refused_rather_than_carrying_a_port() {
        let err = rewrite_port("unix:///var/run/agentenv.sock", 8001)
            .expect_err("a socket path is not somewhere to put a port");
        assert!(err.to_string().contains("no host"), "{err}");
    }

    /// 🔴 Both halves of the answer are required, and the test asserts on each
    /// one separately: a placement that named a node with no address, or an
    /// address with no node, would satisfy any assertion that only counted
    /// that an answer came back.
    #[tokio::test]
    async fn half_an_answer_is_not_a_placement() {
        let placement = SchedulerNodePlacement::connect_lazy("http://127.0.0.1:1", 8001).unwrap();

        let err = placement
            .node_service_endpoint(None)
            .expect_err("no node at all");
        assert!(err.to_string().contains("named no node"), "{err}");

        let err = placement
            .node_service_endpoint(Some(scheduler::Node {
                node_id: "node-a".to_string(),
                endpoint: String::new(),
            }))
            .expect_err("a node with no address");
        assert!(err.to_string().contains("no address"), "{err}");

        let err = placement
            .node_service_endpoint(Some(scheduler::Node {
                node_id: String::new(),
                endpoint: "http://10.0.0.7:8000".to_string(),
            }))
            .expect_err("an address with no node");
        assert!(err.to_string().contains("no id"), "{err}");

        // And the control: a whole answer resolves, so the three refusals
        // above are about what was missing rather than about the function
        // refusing everything.
        let node = placement
            .node_service_endpoint(Some(scheduler::Node {
                node_id: "node-a".to_string(),
                endpoint: "http://10.0.0.7:8000".to_string(),
            }))
            .expect("a whole answer");
        assert_eq!(node.node_id, "node-a");
        assert_eq!(node.endpoint, "http://10.0.0.7:8001/");
    }
}
