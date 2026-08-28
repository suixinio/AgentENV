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
//!
//! # 🔴 And one write
//!
//! `RecordAssignment` is the only call here that tells the scheduler something
//! rather than asking it. Nothing in this process made it until now: the
//! gateway was the sole caller in the whole system, and it stopped being able
//! to make it the day user-facing REST began going to the API half — it no
//! longer routes the create, so it no longer knows the node. Its own comment
//! names this half as the owner of the write it gave up
//! (`services/gateway/internal/rest_upstream.go`).

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use tonic::transport::Channel;

use crate::proto::scheduler::{self, scheduler_client::SchedulerClient};
use crate::scheduler_endpoint::{qualified, SchedulerEndpointSource};
use crate::types::{ExecutionId, SandboxId, SandboxResources};

use super::placement::{NodeEndpoint, NodeMembership, NodePlacement};

/// Placement backed by the cluster scheduler.
pub struct SchedulerNodePlacement {
    endpoint_source: SchedulerEndpointSource,
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
    /// Connects lazily: opens no socket, so a scheduler that is down at
    /// startup delays a create rather than a process. No file-driven
    /// hot-reload — the static endpoint for the rest of this process's
    /// lifetime. Production assembly uses
    /// [`connect_hot_reloadable`](Self::connect_hot_reloadable) instead;
    /// this constructor stays simple for tests and any caller with no
    /// reason to reach for the other one.
    pub fn connect_lazy(endpoint: &str, node_service_port: u16) -> Result<Self> {
        let endpoint_source =
            SchedulerEndpointSource::spawn(qualified(endpoint), None, "create_placement")
                .with_context(|| format!("scheduler endpoint {endpoint:?} is not a valid URI"))?;
        Ok(Self {
            endpoint_source,
            node_service_port,
        })
    }

    /// [`connect_lazy`](Self::connect_lazy), but the endpoint can be
    /// hot-reloaded from `[cluster].scheduler_endpoint_file` while the
    /// process runs — see [`SchedulerEndpointSource::spawn_from_config`].
    /// This is what `cluster_placement` in `src/bin/aenv-api.rs` uses.
    pub fn connect_hot_reloadable(
        endpoint: &str,
        cluster: &crate::cfg::ClusterConfig,
        scheduler_report: &crate::cfg::ObservabilitySchedulerReportConfig,
        node_service_port: u16,
    ) -> Result<Self> {
        let endpoint_source = SchedulerEndpointSource::spawn_from_config(
            qualified(endpoint),
            cluster,
            scheduler_report,
            "create_placement",
        )
        .with_context(|| format!("scheduler endpoint {endpoint:?} is not a valid URI"))?;
        Ok(Self {
            endpoint_source,
            node_service_port,
        })
    }

    fn client(&self) -> SchedulerClient<Channel> {
        SchedulerClient::new(self.endpoint_source.channel())
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
            // 🔴 Kept alongside the rewritten one rather than recomputed later.
            // `rewrite_port` parses and re-renders, so it is not reversible —
            // and this exact string is what a `RecordAssignment` has to carry
            // back for the scheduler to recognise the node at all.
            advertised_endpoint: node.endpoint,
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

    async fn place_existing(&self, sandbox_id: SandboxId) -> Result<Option<NodeEndpoint>> {
        let response = match self
            .client()
            .lookup_node(scheduler::LookupNodeRequest {
                sandbox_id: sandbox_id.to_string(),
            })
            .await
        {
            Ok(response) => response.into_inner(),
            // 🔴 The one status that is an answer rather than a failure to
            // answer, and it is kept apart from the rest for the reason written
            // on `NodePlacement::place_existing`. The scheduler produces it in
            // exactly one place — `lookupAbsent` — and only after the bindings,
            // the heartbeat rosters and the paused registry have each been read
            // and each held no row. Every outcome where something could not be
            // consulted is `Unavailable`, and stays an error here.
            Err(status) if status.code() == tonic::Code::NotFound => return Ok(None),
            Err(status) => bail!("the scheduler could not locate {sandbox_id}: {status}"),
        };
        // 🔴 The `SandboxLocation` is read by the resume surface, which decides
        // whether a pin may be honoured, and *not* here: this trait's job is to
        // name a machine, and re-deciding pin-versus-prefer in a second place
        // is how the two answers come to disagree. What this refuses is only
        // an answer with no machine in it.
        self.node_service_endpoint(response.node).map(Some)
    }

    /// Asks discovery, through `GetNode`, and never `LookupNode`.
    ///
    /// 🔴 The distinction is the whole reason this method exists: the caller
    /// already knows which machine it is entitled to talk to and is only
    /// missing that machine's current address. `LookupNode` answers a different
    /// question — *which* machine — and the one case this method is called in
    /// is precisely the one where `LookupNode` has no answer at all.
    async fn resolve_node(&self, node_id: &str) -> Result<NodeEndpoint> {
        let response = self
            .client()
            .get_node(scheduler::GetNodeRequest {
                node_id: node_id.to_string(),
                // 🔴 Left empty on purpose: the scheduler reads a blank cluster
                // id as "do not filter". This half is configured with one
                // scheduler for one cluster, and sending this process's own
                // cluster id would make the resolution fail on any deployment
                // where the two are spelled differently — a configuration
                // mismatch this call is in no position to adjudicate, and one
                // whose only symptom here would be a resume that cannot find
                // its node.
                cluster_id: String::new(),
            })
            .await
            .map_err(|status| {
                anyhow!("the scheduler could not say where node {node_id} is: {status}")
            })?
            .into_inner();
        let observed = response
            .node
            .ok_or_else(|| anyhow!("the scheduler answered about node {node_id} with no node"))?;
        let resolved = self.node_service_endpoint(Some(scheduler::Node {
            node_id: observed.node_id,
            endpoint: observed.endpoint,
        }))?;
        // 🔴 The answer has to be about the node that was asked for. `GetNode`
        // looks its record up under the id it is given, so a mismatch is not
        // something a healthy deployment produces — which is exactly why it is
        // checked rather than assumed: the caller is about to reopen a capture
        // that exists on one machine, and an address belonging to another is
        // the one input that turns that into a resume on the wrong host.
        if resolved.node_id != node_id {
            bail!(
                "asked the scheduler where node {node_id} is and it answered about node {}",
                resolved.node_id
            );
        }
        Ok(resolved)
    }

    /// Also `GetNode`, and for the same reason `resolve_node` above uses it
    /// rather than `LookupNode`: this asks about the node's own identity, not
    /// about which node holds a sandbox.
    ///
    /// 🔴 `NotFound` is kept apart from every other failure, the same shape as
    /// `place_existing`'s `NOT_FOUND` handling above. The scheduler's
    /// `AtomicNodeRegistry.GetObserved` produces it in exactly one place: the
    /// node's own record has been dropped from the registry entirely — an
    /// explicit `UnregisterNode`, or the registry's own discovery no longer
    /// listing the node at all. A node it merely rates unhealthy — a stale
    /// heartbeat that has not (yet) aged out of discovery — still answers
    /// `Ok`, carrying that status in the snapshot this method does not even
    /// read. Folding every other failure into the same answer `NotFound` gets
    /// here is what would let a scheduler hiccup be read as a node leaving the
    /// cluster.
    async fn node_membership(&self, node_id: &str) -> Result<NodeMembership> {
        match self
            .client()
            .get_node(scheduler::GetNodeRequest {
                node_id: node_id.to_string(),
                // See `resolve_node`'s note on the same field, verbatim: blank
                // means "do not filter by cluster".
                cluster_id: String::new(),
            })
            .await
        {
            Ok(_) => Ok(NodeMembership::Present),
            Err(status) if status.code() == tonic::Code::NotFound => Ok(NodeMembership::Gone),
            Err(status) => bail!(
                "the scheduler could not say whether node {node_id} is still in the cluster: \
                 {status}"
            ),
        }
    }

    async fn record_placement(
        &self,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
        node: &NodeEndpoint,
    ) -> Result<()> {
        let mut request = tonic::Request::new(scheduler::RecordAssignmentRequest {
            sandbox_id: sandbox_id.to_string(),
            node: Some(scheduler::Node {
                node_id: node.node_id.clone(),
                // 🔴 The address the scheduler named, not the one this process
                // dials. `AtomicNodeRegistry.Contains` compares it byte-for-byte
                // against discovery's own entry and refuses an assignment naming
                // anything else — so sending the node-service address here would
                // have every one of these writes rejected as *an unknown node*.
                endpoint: node.advertised_endpoint.clone(),
            }),
            // 🔴 The run the node acknowledged, which `start` and `reopen` have
            // each already checked against the one this process allocated. It is
            // optional on the wire, and sending it is what makes the binding
            // authoritative for the window before the node's first heartbeat —
            // the same window this whole call exists to cover.
            execution_id: execution_id.to_string(),
            // 🔴 Zero, which the scheduler reads as "use your own `binding_ttl`".
            // The projection budget is the *node's* to compute from the
            // sandbox's lifetime ceiling and it travels on the node's heartbeat
            // roster; a number invented here would be this process overriding
            // the only party that knows it. Zero is also exactly what the
            // gateway sends with its projection switch off, so the write this
            // makes is the write that already shipped.
            projection_ttl_secs: 0,
        });
        // 🔴 Bounded, at the same five seconds the gateway bounds its copy of
        // this call at. The caller cannot fail over this — the sandbox is
        // already up on the node — so an unbounded call would hold a create
        // open for as long as a wedged scheduler cared to, to write an entry
        // the next heartbeat rewrites anyway.
        request.set_timeout(RECORD_PLACEMENT_TIMEOUT);
        self.client()
            .record_assignment(request)
            .await
            .map_err(|status| {
                anyhow!("the scheduler refused an assignment for {sandbox_id}: {status}")
            })?;
        Ok(())
    }
}

/// How long a `RecordAssignment` may take before it is given up on.
///
/// The same ceiling the gateway applies to its own copy of this call
/// (`maxRecordAssignmentTimeout`).
const RECORD_PLACEMENT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Replaces the port in an `scheme://host:port` address, keeping everything
/// else the scheduler said.
///
/// 🔴 Parsed rather than string-spliced, because an IPv6 literal is written
/// `http://[::1]:8000` and the last colon in it is not the one before the
/// port on any naive reading that also has to cope with `http://[::1]`.
pub fn rewrite_port(endpoint: &str, port: u16) -> Result<String> {
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

    /// 🔴 The two addresses on a placement are different strings, and the one a
    /// `RecordAssignment` carries is the scheduler's own spelling.
    ///
    /// The control is in the same assertion pair: the dialled address is the
    /// rewritten one, so a `node_service_endpoint` that stored the same value
    /// in both fields fails whichever of the two it chose. And the advertised
    /// half is compared to the exact input string rather than to a normalised
    /// form of it, because normalising is what makes the scheduler reject the
    /// write — `Contains` is a byte comparison against discovery's entry.
    #[tokio::test]
    async fn the_address_a_binding_carries_is_the_one_the_scheduler_said() {
        let placement = SchedulerNodePlacement::connect_lazy("http://127.0.0.1:1", 8001).unwrap();

        // A bare `host:port`, which is what a static discovery list holds and
        // therefore what `Contains` will be comparing against.
        let node = placement
            .node_service_endpoint(Some(scheduler::Node {
                node_id: "node-a".to_string(),
                endpoint: "10.0.0.7:8000".to_string(),
            }))
            .expect("a whole answer");
        assert_eq!(
            node.advertised_endpoint, "10.0.0.7:8000",
            "the address the scheduler named was not kept verbatim"
        );
        assert_eq!(
            node.endpoint, "http://10.0.0.7:8001/",
            "the dialled address is the node service's"
        );
        assert_ne!(
            node.advertised_endpoint, node.endpoint,
            "the two addresses came out identical, so one of them is the wrong one"
        );

        // 🔴 The second face: an address that survives the rewrite unchanged
        // except for its port. Without it, "kept verbatim" could be satisfied
        // by a function that simply never rewrote anything.
        let node = placement
            .node_service_endpoint(Some(scheduler::Node {
                node_id: "node-a".to_string(),
                endpoint: "http://10.0.0.7:8000/".to_string(),
            }))
            .expect("a whole answer");
        assert_eq!(node.advertised_endpoint, "http://10.0.0.7:8000/");
        assert_eq!(node.endpoint, "http://10.0.0.7:8001/");
    }
}

/// The three calls this file makes, driven against a scheduler on a real
/// socket.
///
/// 🔴 Over a socket rather than against a fake trait, because everything these
/// tests are about lives in the gRPC layer: which status is an answer and which
/// is a failure to answer, and which of a node's two addresses ends up in a
/// message. Neither exists if the two halves are the same object.
#[cfg(test)]
mod against_a_scheduler {
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex};

    use tokio::sync::oneshot;
    use tonic::{Request, Response, Status};

    use super::*;
    use crate::proto::scheduler::scheduler_server::{Scheduler, SchedulerServer};

    /// A scheduler that answers whatever a test told it to.
    #[derive(Default)]
    struct ScriptedScheduler {
        lookup: Mutex<Option<Result<scheduler::LookupNodeResponse, Status>>>,
        get_node: Mutex<Option<Result<scheduler::GetNodeResponse, Status>>>,
        assignments: Mutex<Vec<scheduler::RecordAssignmentRequest>>,
    }

    impl ScriptedScheduler {
        fn answers_lookup(&self, answer: Result<scheduler::LookupNodeResponse, Status>) {
            *self.lookup.lock().expect("lock") = Some(answer);
        }

        fn answers_get_node(&self, answer: Result<scheduler::GetNodeResponse, Status>) {
            *self.get_node.lock().expect("lock") = Some(answer);
        }

        fn assignments(&self) -> Vec<scheduler::RecordAssignmentRequest> {
            self.assignments.lock().expect("lock").clone()
        }
    }

    #[tonic::async_trait]
    impl Scheduler for Arc<ScriptedScheduler> {
        async fn record_assignment(
            &self,
            request: Request<scheduler::RecordAssignmentRequest>,
        ) -> Result<Response<scheduler::RecordAssignmentResponse>, Status> {
            self.assignments
                .lock()
                .expect("lock")
                .push(request.into_inner());
            Ok(Response::new(scheduler::RecordAssignmentResponse {}))
        }

        async fn lookup_node(
            &self,
            _request: Request<scheduler::LookupNodeRequest>,
        ) -> Result<Response<scheduler::LookupNodeResponse>, Status> {
            self.lookup
                .lock()
                .expect("lock")
                .clone()
                .expect("the test scripted no lookup answer")
                .map(Response::new)
        }

        async fn get_node(
            &self,
            _request: Request<scheduler::GetNodeRequest>,
        ) -> Result<Response<scheduler::GetNodeResponse>, Status> {
            self.get_node
                .lock()
                .expect("lock")
                .clone()
                .expect("the test scripted no get_node answer")
                .map(Response::new)
        }

        async fn schedule(
            &self,
            _request: Request<scheduler::ScheduleRequest>,
        ) -> Result<Response<scheduler::ScheduleResponse>, Status> {
            Err(Status::unimplemented("not used by these tests"))
        }
        async fn list_nodes(
            &self,
            _request: Request<scheduler::ListNodesRequest>,
        ) -> Result<Response<scheduler::ListNodesResponse>, Status> {
            Err(Status::unimplemented("not used by these tests"))
        }
        async fn heartbeat(
            &self,
            _request: Request<scheduler::HeartbeatRequest>,
        ) -> Result<Response<scheduler::HeartbeatResponse>, Status> {
            Err(Status::unimplemented("not used by these tests"))
        }
        async fn report_sandbox_event(
            &self,
            _request: Request<scheduler::ReportSandboxEventRequest>,
        ) -> Result<Response<scheduler::ReportSandboxEventResponse>, Status> {
            Err(Status::unimplemented("not used by these tests"))
        }
        async fn list_observed_nodes(
            &self,
            _request: Request<scheduler::ListObservedNodesRequest>,
        ) -> Result<Response<scheduler::ListObservedNodesResponse>, Status> {
            Err(Status::unimplemented("not used by these tests"))
        }
        async fn list_p2p_peers(
            &self,
            _request: Request<scheduler::ListP2pPeersRequest>,
        ) -> Result<Response<scheduler::ListP2pPeersResponse>, Status> {
            Err(Status::unimplemented("not used by these tests"))
        }
        async fn record_p2p_artifact(
            &self,
            _request: Request<scheduler::RecordP2pArtifactRequest>,
        ) -> Result<Response<scheduler::RecordP2pArtifactResponse>, Status> {
            Err(Status::unimplemented("not used by these tests"))
        }
        async fn forget_p2p_artifact(
            &self,
            _request: Request<scheduler::ForgetP2pArtifactRequest>,
        ) -> Result<Response<scheduler::ForgetP2pArtifactResponse>, Status> {
            Err(Status::unimplemented("not used by these tests"))
        }
        async fn lookup_p2p_artifact(
            &self,
            _request: Request<scheduler::LookupP2pArtifactRequest>,
        ) -> Result<Response<scheduler::LookupP2pArtifactResponse>, Status> {
            Err(Status::unimplemented("not used by these tests"))
        }
        async fn unregister_node(
            &self,
            _request: Request<scheduler::UnregisterNodeRequest>,
        ) -> Result<Response<scheduler::UnregisterNodeResponse>, Status> {
            Err(Status::unimplemented("not used by these tests"))
        }
        async fn list_registry_sandboxes(
            &self,
            _request: Request<scheduler::ListRegistrySandboxesRequest>,
        ) -> Result<Response<scheduler::ListRegistrySandboxesResponse>, Status> {
            Err(Status::unimplemented("not used by these tests"))
        }
    }

    /// A scripted scheduler on a real port, and a placement pointed at it.
    async fn scheduler_on_a_socket() -> (
        Arc<ScriptedScheduler>,
        SchedulerNodePlacement,
        oneshot::Sender<()>,
    ) {
        let scripted = Arc::new(ScriptedScheduler::default());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind a port");
        let addr: SocketAddr = listener.local_addr().expect("the bound address");
        let (tx, rx) = oneshot::channel();

        let served = Arc::clone(&scripted);
        tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(SchedulerServer::new(served))
                .serve_with_incoming_shutdown(
                    tonic::transport::server::TcpIncoming::from(listener),
                    async {
                        let _ = rx.await;
                    },
                )
                .await;
        });
        for _ in 0..200 {
            if tokio::net::TcpStream::connect(addr).await.is_ok() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        let placement = SchedulerNodePlacement::connect_lazy(&format!("http://{addr}"), 8001)
            .expect("a placement pointed at the scripted scheduler");
        (scripted, placement, tx)
    }

    fn node(node_id: &str, endpoint: &str) -> Option<scheduler::Node> {
        Some(scheduler::Node {
            node_id: node_id.to_string(),
            endpoint: endpoint.to_string(),
        })
    }

    /// An assignment carries the address the scheduler named the node by.
    ///
    /// 🔴 This is the one line in this file whose failure mode is invisible from
    /// here: `AtomicNodeRegistry.Contains` compares the address on the wire
    /// byte-for-byte against discovery's entry and answers `InvalidArgument:
    /// node is not in scheduler node list` for anything else. A write that sent
    /// the dialled address would therefore be rejected on every create, and
    /// rejected with a message about discovery.
    ///
    /// The controls are in the same round: the dialled address is asserted to be
    /// a *different string*, so a build that sent it fails rather than agreeing
    /// with itself; and the two assignments carry two different sandboxes and
    /// two different runs, so nothing here can be satisfied by a constant.
    #[tokio::test]
    async fn an_assignment_carries_the_address_the_scheduler_named() {
        let (scripted, placement, _stop) = scheduler_on_a_socket().await;
        scripted.answers_lookup(Ok(scheduler::LookupNodeResponse {
            node: node("node-a", "http://10.0.0.7:8000"),
            ..Default::default()
        }));

        let placed = placement
            .place_existing(SandboxId::new())
            .await
            .expect("the scheduler answered")
            .expect("with a node");
        assert_eq!(placed.endpoint, "http://10.0.0.7:8001/");
        assert_eq!(placed.advertised_endpoint, "http://10.0.0.7:8000");

        let first = (SandboxId::new(), ExecutionId::new());
        let second = (SandboxId::new(), ExecutionId::new());
        assert_ne!(first.1, second.1, "two mints produced one incarnation");
        for (sandbox_id, execution_id) in [first, second] {
            placement
                .record_placement(sandbox_id, execution_id, &placed)
                .await
                .expect("the scheduler accepted the assignment");
        }

        let assignments = scripted.assignments();
        assert_eq!(assignments.len(), 2, "the scheduler was told nothing");
        for (index, (sandbox_id, execution_id)) in [first, second].into_iter().enumerate() {
            let request = &assignments[index];
            assert_eq!(request.sandbox_id, sandbox_id.to_string());
            assert_eq!(
                request.execution_id,
                execution_id.to_string(),
                "the assignment named a run other than the one it was given"
            );
            let named = request.node.as_ref().expect("an assignment names a node");
            assert_eq!(named.node_id, "node-a");
            assert_eq!(
                named.endpoint, "http://10.0.0.7:8000",
                "the assignment carried an address discovery would not recognise"
            );
            assert_ne!(
                named.endpoint, placed.endpoint,
                "the assignment carried the node-service address"
            );
            // 🔴 Zero, meaning "use your own binding_ttl" — and it is only
            // evidence because the fields around it in the same message are
            // populated and vary. A message that arrived empty would satisfy a
            // lone `== 0` and fail the two assertions above.
            assert_eq!(request.projection_ttl_secs, 0);
        }
    }

    /// A sandbox the scheduler has never heard of is an answer; a scheduler that
    /// could not look is not.
    ///
    /// 🔴 Three faces over one value — the status the scheduler returned — and
    /// the whole point is that two of them used to be the same thing here. A
    /// caller cannot tell "there is no such sandbox" from "I could not check"
    /// once they have both become `Err`, and a resume that took the second for
    /// the first would reopen a capture the cluster had already moved.
    #[tokio::test]
    async fn a_sandbox_nobody_has_heard_of_is_not_a_scheduler_that_could_not_look() {
        let (scripted, placement, _stop) = scheduler_on_a_socket().await;
        let sandbox_id = SandboxId::new();

        scripted.answers_lookup(Err(Status::not_found("sandbox assignment not found")));
        assert_eq!(
            placement
                .place_existing(sandbox_id)
                .await
                .expect("an absent record is an answer, not a failure"),
            None
        );

        scripted.answers_lookup(Err(Status::unavailable(
            "scheduler is still seeding sandbox assignments",
        )));
        let err = placement
            .place_existing(sandbox_id)
            .await
            .expect_err("a scheduler that could not look was read as an absent record");
        assert!(format!("{err}").contains("could not locate"), "{err}");

        scripted.answers_lookup(Ok(scheduler::LookupNodeResponse {
            node: node("node-a", "http://10.0.0.7:8000"),
            ..Default::default()
        }));
        let placed = placement
            .place_existing(sandbox_id)
            .await
            .expect("the scheduler answered")
            .expect("with a node");
        assert_eq!(placed.node_id, "node-a");
    }

    /// Resolving a node asks discovery, and refuses an answer about a different
    /// machine.
    ///
    /// 🔴 The middle face is the safety property: this method's one caller is
    /// about to reopen a capture that exists on exactly one machine, and an
    /// address belonging to another is the single input that turns that into a
    /// resume on the wrong host. The first and third faces bracket it, so the
    /// refusal is about the id rather than about the method refusing everything.
    #[tokio::test]
    async fn resolving_a_node_refuses_an_answer_about_another_one() {
        let (scripted, placement, _stop) = scheduler_on_a_socket().await;

        scripted.answers_get_node(Ok(scheduler::GetNodeResponse {
            node: Some(scheduler::ObservedNode {
                node_id: "node-a".to_string(),
                endpoint: "http://10.0.0.7:8000".to_string(),
                ..Default::default()
            }),
        }));
        let resolved = placement
            .resolve_node("node-a")
            .await
            .expect("the scheduler knows where node-a is");
        assert_eq!(resolved.node_id, "node-a");
        assert_eq!(resolved.endpoint, "http://10.0.0.7:8001/");
        assert_eq!(resolved.advertised_endpoint, "http://10.0.0.7:8000");

        // The same call, one value different: the machine the answer is about.
        scripted.answers_get_node(Ok(scheduler::GetNodeResponse {
            node: Some(scheduler::ObservedNode {
                node_id: "node-b".to_string(),
                endpoint: "http://10.0.0.9:8000".to_string(),
                ..Default::default()
            }),
        }));
        let err = placement
            .resolve_node("node-a")
            .await
            .expect_err("an answer about another machine was followed");
        assert!(
            format!("{err}").contains("answered about node node-b"),
            "{err}"
        );

        scripted.answers_get_node(Err(Status::not_found("observed node not found")));
        let err = placement
            .resolve_node("node-a")
            .await
            .expect_err("a node the scheduler cannot place was resolved anyway");
        assert!(format!("{err}").contains("could not say where"), "{err}");
    }

    /// A node's cluster membership is a different answer from "is it healthy
    /// right now" — `NotFound` alone means gone, not merely unhealthy.
    ///
    /// 🔴 Three faces over what `GetNode` answers, mirroring the shape
    /// `place_existing`'s own `NOT_FOUND` handling gets: an ordinary answer, an
    /// authoritative absence (`NotFound`, produced only once the registry has
    /// actually dropped the node), and everything else, which must never be
    /// read as the second. Folding the third into the second is exactly the
    /// mistake that would let a scheduler hiccup evict a node that never left.
    #[tokio::test]
    async fn node_membership_tells_gone_from_merely_unreachable() {
        let (scripted, placement, _stop) = scheduler_on_a_socket().await;

        scripted.answers_get_node(Ok(scheduler::GetNodeResponse {
            node: Some(scheduler::ObservedNode {
                node_id: "node-a".to_string(),
                endpoint: "http://10.0.0.7:8000".to_string(),
                ..Default::default()
            }),
        }));
        assert_eq!(
            placement
                .node_membership("node-a")
                .await
                .expect("the registry answered"),
            NodeMembership::Present,
            "a node the registry still lists was read as gone"
        );

        scripted.answers_get_node(Err(Status::not_found("observed node not found")));
        assert_eq!(
            placement
                .node_membership("node-a")
                .await
                .expect("an absent node is an answer, not a failure"),
            NodeMembership::Gone,
            "a node the registry has stopped listing was read as still present"
        );

        scripted.answers_get_node(Err(Status::unavailable("scheduler store unavailable")));
        let err = placement
            .node_membership("node-a")
            .await
            .expect_err("a scheduler that could not answer was read as a verdict");
        assert!(format!("{err}").contains("could not say"), "{err}");
    }
}
