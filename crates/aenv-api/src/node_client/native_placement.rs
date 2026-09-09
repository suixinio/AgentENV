//! In-process [`NodePlacement`] backed by the local node registry and scheduler surface.
//!
//! New placement, existing lookup, and assignment recording reuse
//! [`NodeRegistryGrpcService`] directly. Node resolution and membership read the
//! heartbeat-observed registry view.
//!
//! Registry misses become confirmed absence only after [`WarmupGate`] opens; before
//! then they remain errors so a restarting replica cannot declare live runtimes gone.

use std::sync::Arc;
use std::time::SystemTime;

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use tonic::Code;

use crate::binding_store::BindingDecision;
use crate::node_registry::grpc_service::NodeRegistryGrpcService;
use crate::node_registry::registry::{AtomicNodeRegistry, NodeRegistry};
use crate::node_registry::warmup::WarmupGate;
use crate::orchestrator::LaunchHeldElsewhere;
use crate::proto::scheduler::scheduler_server::Scheduler;
use crate::proto::scheduler::{self, ObservedNode};
use crate::scheduler_endpoint::qualified;
use crate::types::{ExecutionId, SandboxId, SandboxResources};

use super::placement::{
    NodeEndpoint, NodeMembership, NodePlacement, PlacementNeeds, PlacementRefused,
    PLACEMENT_RESERVATION_TTL,
};

/// Replaces the port in an HTTP node address, including bracketed IPv6 literals.
pub fn rewrite_port(endpoint: &str, port: u16) -> Result<String> {
    let mut url = url::Url::parse(&qualified(endpoint))
        .with_context(|| format!("node address {endpoint:?} is not a valid URI"))?;
    url.set_port(Some(port))
        .map_err(|()| anyhow!("node address {endpoint:?} has no host to put a port on"))?;
    Ok(url.to_string())
}

#[cfg(test)]
mod rewrite_port_tests {
    use super::rewrite_port;

    #[test]
    fn the_scheduler_names_a_http_port_and_the_node_service_is_on_another() {
        assert_eq!(
            rewrite_port("http://10.0.0.7:8000", 8001).unwrap(),
            "http://10.0.0.7:8001/"
        );
        assert_eq!(
            rewrite_port("10.0.0.7:8000", 8001).unwrap(),
            "http://10.0.0.7:8001/"
        );
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
}

/// Placement backed entirely by the local API process.
pub struct NativeNodePlacement {
    registry: Arc<AtomicNodeRegistry>,
    /// Node sandbox-service port substituted into advertised addresses.
    node_service_port: u16,
    /// Prevents cold registry misses from becoming confirmed absence.
    warmup: Arc<WarmupGate>,
    /// Shared local service used by in-process and gRPC placement callers.
    local: NodeRegistryGrpcService,
}

impl NativeNodePlacement {
    pub fn new(
        registry: Arc<AtomicNodeRegistry>,
        node_service_port: u16,
        warmup: Arc<WarmupGate>,
        local: NodeRegistryGrpcService,
    ) -> Self {
        Self {
            registry,
            node_service_port,
            warmup,
            local,
        }
    }

    /// Converts an observed registry node into its sandbox-service endpoint.
    fn node_service_endpoint(&self, observed: ObservedNode) -> Result<NodeEndpoint> {
        if observed.node_id.is_empty() {
            bail!("the node registry named a node with no id");
        }
        if observed.endpoint.is_empty() {
            bail!(
                "the node registry named node {} with no address",
                observed.node_id
            );
        }
        Ok(NodeEndpoint {
            endpoint: rewrite_port(&observed.endpoint, self.node_service_port)?,
            advertised_endpoint: observed.endpoint,
            node_id: observed.node_id,
        })
    }

    /// Converts a scheduler wire node into its sandbox-service endpoint.
    fn node_service_endpoint_from_wire(
        &self,
        node: Option<scheduler::Node>,
    ) -> Result<NodeEndpoint> {
        let node = node.ok_or_else(|| anyhow!("the local scheduler named no node"))?;
        self.node_service_endpoint_for(node.node_id, node.endpoint)
    }

    /// Converts a node the local surface named into its sandbox-service endpoint.
    fn node_service_endpoint_for(&self, node_id: String, endpoint: String) -> Result<NodeEndpoint> {
        if node_id.is_empty() {
            bail!("the local scheduler named a node with no id");
        }
        if endpoint.is_empty() {
            bail!("the local scheduler named node {node_id} with no address");
        }
        Ok(NodeEndpoint {
            endpoint: rewrite_port(&endpoint, self.node_service_port)?,
            advertised_endpoint: endpoint,
            node_id,
        })
    }
}

#[async_trait]
impl NodePlacement for NativeNodePlacement {
    /// Places a new sandbox through the local scheduler surface.
    async fn place_new(
        &self,
        sandbox_id: SandboxId,
        resources: SandboxResources,
        preferred_node_id: Option<&str>,
        excluded_node_ids: &[String],
    ) -> Result<NodeEndpoint> {
        self.place_new_with(
            sandbox_id,
            resources,
            preferred_node_id,
            excluded_node_ids,
            PlacementNeeds::default(),
        )
        .await
    }

    async fn place_new_with(
        &self,
        _sandbox_id: SandboxId,
        resources: SandboxResources,
        preferred_node_id: Option<&str>,
        excluded_node_ids: &[String],
        needs: PlacementNeeds,
    ) -> Result<NodeEndpoint> {
        let response = self
            .local
            .schedule(tonic::Request::new(scheduler::ScheduleRequest {
                hint: Some(scheduler::ScheduleRequestHint {
                    kind: Some(scheduler::schedule_request_hint::Kind::NewSandbox(
                        scheduler::NewSandboxHint {
                            metadata: Default::default(),
                            // Presence is explicit even at zero so scoring sees known resources.
                            cpu_count: Some(resources.cpu_count),
                            memory_mib: Some(u64::from(resources.memory_mib)),
                            preferred_node_id: preferred_node_id.unwrap_or_default().to_string(),
                            excluded_node_ids: excluded_node_ids.to_vec(),
                            requires_egress_broker: needs.egress_broker,
                        },
                    )),
                }),
            }))
            .await
            .map_err(|status| match status.code() {
                // The scheduler had nodes, none with the capability asked for.
                Code::FailedPrecondition if needs.egress_broker => {
                    anyhow::Error::new(PlacementRefused::NoEgressBrokerNode)
                }
                _ => anyhow!("the local scheduler refused to place a sandbox: {status}"),
            })?
            .into_inner();
        self.node_service_endpoint_from_wire(response.node)
    }

    /// Locates an existing sandbox; only `NotFound` becomes `Ok(None)`.
    async fn place_existing(&self, sandbox_id: SandboxId) -> Result<Option<NodeEndpoint>> {
        let answer = match self.local.lookup_sandbox(&sandbox_id.to_string()).await {
            Ok(answer) => answer,
            Err(status) if status.code() == Code::NotFound => return Ok(None),
            // Keep the status in the chain: its code is what separates a verdict
            // about this sandbox from a scheduler that could not answer.
            Err(status) => {
                return Err(anyhow::Error::new(status)
                    .context(format!("the local scheduler could not locate {sandbox_id}")))
            }
        };
        self.node_service_endpoint_for(answer.node.id, answer.node.endpoint)
            .map(Some)
    }

    /// Resolves a node from the heartbeat-observed registry view.
    async fn resolve_node(&self, node_id: &str) -> Result<NodeEndpoint> {
        let Some(observed) = self.registry.get_observed(node_id, "", SystemTime::now()) else {
            if self.warmup.warmed_up(SystemTime::now()) {
                bail!("the node registry has no observed record for node {node_id}");
            }
            bail!(
                "the node registry has not finished warming up (not every node discovery \
                 already knows about has heartbeated yet) and cannot yet say whether node \
                 {node_id} has an observed record"
            );
        };
        let resolved = self.node_service_endpoint(observed)?;
        // The returned identity must match the requested node.
        if resolved.node_id != node_id {
            bail!(
                "asked the node registry where node {node_id} is and it answered about node {}",
                resolved.node_id
            );
        }
        Ok(resolved)
    }

    /// Reports membership from the observed view, withholding `Gone` while cold.
    async fn node_membership(&self, node_id: &str) -> Result<NodeMembership> {
        match self.registry.get_observed(node_id, "", SystemTime::now()) {
            Some(_) => Ok(NodeMembership::Present),
            None if self.warmup.warmed_up(SystemTime::now()) => Ok(NodeMembership::Gone),
            None => bail!(
                "the node registry has not finished warming up (not every node discovery \
                 already knows about has heartbeated yet) and cannot yet say whether node \
                 {node_id} is still part of the cluster"
            ),
        }
    }

    /// Records the advertised node address through the local assignment surface.
    async fn record_placement(
        &self,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
        node: &NodeEndpoint,
        projection_ttl_secs: u32,
    ) -> Result<()> {
        self.local
            .record_assignment(
                &sandbox_id.to_string(),
                &node.node_id,
                &execution_id.to_string(),
                projection_ttl_secs,
            )
            .await
            .map_err(|status| {
                anyhow!("the local scheduler refused an assignment for {sandbox_id}: {status}")
            })?;
        Ok(())
    }

    /// Reserves through the local assignment surface, in process.
    async fn reserve_placement(
        &self,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
        node: &NodeEndpoint,
    ) -> Result<()> {
        let decision = self
            .local
            .reserve_assignment(
                &sandbox_id.to_string(),
                &node.node_id,
                &execution_id.to_string(),
                PLACEMENT_RESERVATION_TTL,
            )
            .await
            .map_err(|status| {
                anyhow!("the local scheduler refused to reserve {sandbox_id}: {status}")
            })?;
        if decision == BindingDecision::RejectedInflight {
            // A launch of this sandbox is still running somewhere; the caller
            // waits for it instead of starting a second one.
            return Err(
                anyhow::Error::new(LaunchHeldElsewhere { sandbox_id }).context(format!(
                "the local scheduler refused to reserve {sandbox_id} on node {}: a launch of it \
                 is still in flight",
                node.node_id
            )),
            );
        }
        if !decision.accepted() {
            bail!(
                "the local scheduler refused to reserve {sandbox_id} on node {}: another \
                 incarnation ({}) already holds it",
                node.node_id,
                decision.as_str()
            );
        }
        Ok(())
    }

    async fn release_placement_reservation(
        &self,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
    ) -> Result<()> {
        self.local
            .release_assignment_reservation(&sandbox_id.to_string(), &execution_id.to_string())
            .await
            .map(|_| ())
            .map_err(|status| {
                anyhow!("the local scheduler could not release the reservation for {sandbox_id}: {status}")
            })
    }

    async fn forget_placement(
        &self,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
    ) -> Result<()> {
        self.local
            .forget_assignment(&sandbox_id.to_string(), &execution_id.to_string())
            .await
            .map(|_| ())
            .map_err(|status| {
                anyhow!("the local scheduler could not retire the routing record for {sandbox_id}: {status}")
            })
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use crate::binding_store::{Binding, BindingStore, BindingStoreSettings, InMemoryBindingStore};
    use crate::node_registry::registry::AtomicNodeRegistry;
    use crate::node_registry::types::Node;
    use crate::proto::scheduler::HeartbeatRequest;

    use super::*;

    fn placement(registry: Arc<AtomicNodeRegistry>) -> NativeNodePlacement {
        placement_with_warmup(registry, warm_gate())
    }

    fn placement_with_warmup(
        registry: Arc<AtomicNodeRegistry>,
        warmup: Arc<WarmupGate>,
    ) -> NativeNodePlacement {
        let local = NodeRegistryGrpcService::new(Arc::clone(&registry), Arc::clone(&warmup));
        NativeNodePlacement::new(registry, 8001, warmup, local)
    }

    fn placement_with_binding_store(
        registry: Arc<AtomicNodeRegistry>,
        binding_store: Arc<dyn BindingStore>,
    ) -> NativeNodePlacement {
        let warmup = warm_gate();
        let local = NodeRegistryGrpcService::new(Arc::clone(&registry), Arc::clone(&warmup))
            .with_binding_store(binding_store, false, Duration::ZERO);
        NativeNodePlacement::new(registry, 8001, warmup, local)
    }

    /// Returns an already-warm test gate.
    fn warm_gate() -> Arc<WarmupGate> {
        let registry = Arc::new(AtomicNodeRegistry::new(Vec::new(), Duration::from_secs(30)))
            as Arc<dyn crate::node_registry::registry::NodeRegistry>;
        let gate = Arc::new(WarmupGate::new(
            registry,
            Duration::from_secs(1),
            SystemTime::UNIX_EPOCH,
        ));
        gate.reported_in(SystemTime::now());
        gate
    }

    /// Returns a test gate that stays cold until its deadline.
    fn cold_gate(registry: Arc<AtomicNodeRegistry>, now: SystemTime) -> Arc<WarmupGate> {
        Arc::new(WarmupGate::new(
            registry as Arc<dyn crate::node_registry::registry::NodeRegistry>,
            Duration::from_secs(60),
            now,
        ))
    }

    fn heartbeat(node_id: &str) -> HeartbeatRequest {
        HeartbeatRequest {
            node_id: node_id.to_string(),
            cluster_id: "cluster-a".to_string(),
            service_instance_id: "instance-a".to_string(),
            ..Default::default()
        }
    }

    fn node(id: &str, endpoint: &str) -> Node {
        Node {
            id: id.to_string(),
            endpoint: endpoint.to_string(),
            pod_name: String::new(),
        }
    }

    #[tokio::test]
    async fn place_new_answers_locally_and_round_robins() {
        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![
                node("node-a", "http://10.0.0.1:8000"),
                node("node-b", "http://10.0.0.2:8000"),
            ],
            Duration::from_secs(30),
        ));
        let placement = placement(Arc::clone(&registry));

        let mut seen = Vec::new();
        for _ in 0..2 {
            let endpoint = placement
                .place_new(SandboxId::new(), SandboxResources::default(), None, &[])
                .await
                .expect("two discovered nodes, nothing else wired");
            seen.push(endpoint.node_id);
        }
        seen.sort();
        assert_eq!(
            seen,
            vec!["node-a".to_string(), "node-b".to_string()],
            "round-robin over two calls must visit both nodes exactly once"
        );
    }

    #[tokio::test]
    async fn place_new_refuses_when_no_nodes_are_discovered() {
        let registry = Arc::new(AtomicNodeRegistry::new(Vec::new(), Duration::from_secs(30)));
        let placement = placement(registry);

        let err = placement
            .place_new(SandboxId::new(), SandboxResources::default(), None, &[])
            .await
            .expect_err("nothing discovered");
        assert!(err.to_string().contains("no nodes available"), "{err}");
    }

    #[tokio::test]
    async fn record_placement_then_place_existing_round_trips_through_the_local_binding_store() {
        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![node("node-a", "http://10.0.0.7:8000")],
            Duration::from_secs(30),
        ));
        let binding_store: Arc<dyn BindingStore> =
            Arc::new(InMemoryBindingStore::new(BindingStoreSettings::default()));
        let placement =
            placement_with_binding_store(Arc::clone(&registry), Arc::clone(&binding_store));
        let sandbox_id = SandboxId::new();
        let execution_id = ExecutionId::new();

        let node_endpoint = NodeEndpoint {
            node_id: "node-a".to_string(),
            endpoint: "http://10.0.0.7:8001/".to_string(),
            advertised_endpoint: "http://10.0.0.7:8000".to_string(),
        };
        placement
            .record_placement(sandbox_id, execution_id, &node_endpoint, 0)
            .await
            .expect("node-a is a known node");

        let resolved = placement
            .place_existing(sandbox_id)
            .await
            .expect("no transport error")
            .expect("the binding this test just recorded");
        assert_eq!(resolved.node_id, "node-a");
        assert_eq!(resolved.endpoint, "http://10.0.0.7:8001/");

        let absent = placement
            .place_existing(SandboxId::new())
            .await
            .expect("a clean miss is not a transport error");
        assert!(absent.is_none(), "nothing was ever recorded for this id");
    }

    #[tokio::test]
    async fn forget_placement_retires_its_own_incarnation_and_leaves_a_newer_one_bound() {
        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![node("node-a", "http://10.0.0.7:8000")],
            Duration::from_secs(30),
        ));
        let binding_store: Arc<dyn BindingStore> =
            Arc::new(InMemoryBindingStore::new(BindingStoreSettings::default()));
        let placement =
            placement_with_binding_store(Arc::clone(&registry), Arc::clone(&binding_store));
        let node_endpoint = NodeEndpoint::same_address("node-a", "http://10.0.0.7:8000");
        let older = ExecutionId::parse_str("00000000-0000-7000-8000-000000000001").expect("uuid");
        let newer = ExecutionId::parse_str("00000000-0000-7000-8000-000000000002").expect("uuid");

        let own = SandboxId::new();
        placement
            .record_placement(own, older, &node_endpoint, 0)
            .await
            .expect("node-a is a known node");
        placement
            .forget_placement(own, older)
            .await
            .expect("retiring one's own placement is not an error");
        assert!(
            placement
                .place_existing(own)
                .await
                .expect("no transport error")
                .is_none(),
            "the teardown's own incarnation must leave no routing answer behind"
        );

        let reused = SandboxId::new();
        placement
            .record_placement(reused, newer, &node_endpoint, 0)
            .await
            .expect("node-a is a known node");
        placement
            .forget_placement(reused, older)
            .await
            .expect("a refused delete is not an error");
        assert!(
            placement
                .place_existing(reused)
                .await
                .expect("no transport error")
                .is_some(),
            "a binding a newer incarnation wrote belongs to that incarnation"
        );
    }

    // An incarnation older than the one already holding the id names a launch
    // that lost its race outright; it is refused, never joined.
    #[tokio::test]
    async fn a_colliding_reservation_is_refused_rather_than_joined() {
        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![Node {
                id: "node-a".to_string(),
                endpoint: "http://10.0.0.1:8000".to_string(),
                pod_name: String::new(),
            }],
            Duration::from_secs(30),
        ));
        let placement = placement_with_binding_store(
            Arc::clone(&registry),
            Arc::new(InMemoryBindingStore::new(BindingStoreSettings::default())),
        );
        let node = NodeEndpoint::same_address("node-a", "http://10.0.0.1:8000");
        let sandbox_id = SandboxId::new();

        let newer = ExecutionId::parse_str("00000000-0000-7000-8000-000000000002").expect("uuid");
        let older = ExecutionId::parse_str("00000000-0000-7000-8000-000000000001").expect("uuid");
        placement
            .reserve_placement(sandbox_id, newer, &node)
            .await
            .expect("the first launch reserves");

        let err = placement
            .reserve_placement(sandbox_id, older, &node)
            .await
            .expect_err("an older incarnation must not silently start a second runtime");
        assert!(
            err.to_string().contains("already holds it"),
            "the refusal did not say another incarnation holds the id: {err}"
        );
    }

    #[tokio::test]
    async fn a_newer_launch_meeting_a_reservation_in_flight_is_told_to_wait_for_it() {
        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![Node {
                id: "node-a".to_string(),
                endpoint: "http://10.0.0.1:8000".to_string(),
                pod_name: String::new(),
            }],
            Duration::from_secs(30),
        ));
        let placement = placement_with_binding_store(
            Arc::clone(&registry),
            Arc::new(InMemoryBindingStore::new(BindingStoreSettings::default())),
        );
        let node = NodeEndpoint::same_address("node-a", "http://10.0.0.1:8000");
        let sandbox_id = SandboxId::new();

        let holder = ExecutionId::parse_str("00000000-0000-7000-8000-000000000001").expect("uuid");
        let newer = ExecutionId::parse_str("00000000-0000-7000-8000-000000000002").expect("uuid");
        placement
            .reserve_placement(sandbox_id, holder, &node)
            .await
            .expect("the first launch reserves");

        let err = placement
            .reserve_placement(sandbox_id, newer, &node)
            .await
            .expect_err("a newer launch must not supersede one that is still running");
        assert!(
            err.chain().any(|cause| cause.is::<LaunchHeldElsewhere>()),
            "the refusal must be the one a caller waits on rather than retries: {err:#}"
        );
        assert!(
            err.to_string().contains("still in flight"),
            "the refusal did not say why it is not a plain collision: {err}"
        );
    }

    #[tokio::test]
    async fn place_existing_reports_a_binding_store_failure_as_an_error_not_a_clean_miss() {
        use crate::binding_store::{BindingDeleteOutcome, BindingStoreError};
        use crate::node_registry::types::RosterEntry;

        struct FailingBindingStore;
        #[async_trait]
        impl BindingStore for FailingBindingStore {
            async fn get(
                &self,
                _sandbox_id: &str,
                _now: SystemTime,
            ) -> Result<Option<Binding>, BindingStoreError> {
                Err(BindingStoreError::new("binding store is down"))
            }
            async fn record(
                &self,
                _sandbox_id: &str,
                _binding: Binding,
                _now: SystemTime,
            ) -> Result<crate::binding_store::BindingDecision, BindingStoreError> {
                unreachable!("this test never records")
            }
            async fn reconcile_node(
                &self,
                _node: Node,
                _roster: Vec<RosterEntry>,
                _now: SystemTime,
            ) -> Result<Vec<(String, crate::binding_store::BindingDecision)>, BindingStoreError>
            {
                unreachable!("this test never reconciles")
            }
            async fn delete(
                &self,
                _sandbox_id: &str,
                _execution_id: &str,
                _now: SystemTime,
            ) -> Result<BindingDeleteOutcome, BindingStoreError> {
                unreachable!("this test never deletes")
            }
            async fn release_reservation(
                &self,
                _sandbox_id: &str,
                _execution_id: &str,
                _now: SystemTime,
            ) -> Result<BindingDeleteOutcome, BindingStoreError> {
                unreachable!("this test never releases")
            }
        }

        let registry = Arc::new(AtomicNodeRegistry::new(Vec::new(), Duration::from_secs(30)));
        let placement =
            placement_with_binding_store(Arc::clone(&registry), Arc::new(FailingBindingStore));

        let err = placement
            .place_existing(SandboxId::new())
            .await
            .expect_err("a binding store failure must not read as a clean miss");
        assert!(err.to_string().contains("could not locate"), "{err}");
    }

    #[tokio::test]
    async fn resolve_node_answers_from_the_local_registry_and_refuses_the_unheard_from() {
        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![Node {
                id: "node-a".to_string(),
                endpoint: "http://10.0.0.7:8000".to_string(),
                pod_name: String::new(),
            }],
            Duration::from_secs(30),
        ));
        registry
            .heartbeat(&heartbeat("node-a"), SystemTime::now())
            .expect("node-a is in discovery");

        let placement = placement(Arc::clone(&registry));

        let resolved = placement
            .resolve_node("node-a")
            .await
            .expect("node-a heartbeated");
        assert_eq!(resolved.node_id, "node-a");
        assert_eq!(resolved.endpoint, "http://10.0.0.7:8001/");
        assert_eq!(resolved.advertised_endpoint, "http://10.0.0.7:8000");

        let err = placement
            .resolve_node("node-b")
            .await
            .expect_err("node-b never heartbeated");
        assert!(err.to_string().contains("no observed record"), "{err}");
    }

    #[tokio::test]
    async fn node_membership_is_present_only_once_heartbeated() {
        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![
                Node {
                    id: "node-a".to_string(),
                    endpoint: "http://10.0.0.7:8000".to_string(),
                    pod_name: String::new(),
                },
                Node {
                    id: "node-b".to_string(),
                    endpoint: "http://10.0.0.9:8000".to_string(),
                    pod_name: String::new(),
                },
            ],
            Duration::from_secs(30),
        ));
        registry
            .heartbeat(&heartbeat("node-a"), SystemTime::now())
            .expect("node-a is in discovery");

        let placement = placement(registry);

        assert_eq!(
            placement.node_membership("node-a").await.unwrap(),
            NodeMembership::Present,
            "node-a heartbeated and should read as present"
        );
        assert_eq!(
            placement.node_membership("node-b").await.unwrap(),
            NodeMembership::Gone,
            "node-b is discovered but has never heartbeated, and should not read as present"
        );
        assert_eq!(
            placement.node_membership("node-c").await.unwrap(),
            NodeMembership::Gone,
            "node-c is not discovered at all"
        );
    }

    #[tokio::test]
    async fn node_membership_withholds_gone_while_the_registry_is_cold() {
        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![
                Node {
                    id: "node-a".to_string(),
                    endpoint: "http://10.0.0.7:8000".to_string(),
                    pod_name: String::new(),
                },
                Node {
                    id: "node-b".to_string(),
                    endpoint: "http://10.0.0.9:8000".to_string(),
                    pod_name: String::new(),
                },
            ],
            Duration::from_secs(30),
        ));
        let now = SystemTime::now();
        let warmup = cold_gate(Arc::clone(&registry), now);
        let placement = placement_with_warmup(Arc::clone(&registry), Arc::clone(&warmup));

        let err = placement
            .node_membership("node-b")
            .await
            .expect_err("a cold registry must not assert a node is gone");
        assert!(
            err.to_string().contains("has not finished warming up"),
            "{err}"
        );

        registry
            .heartbeat(&heartbeat("node-a"), now)
            .expect("node-a is in discovery");
        warmup.reported_in(now);
        placement
            .node_membership("node-b")
            .await
            .expect_err("node-b still has not reported; the gate is still cold");

        registry
            .heartbeat(&heartbeat("node-b"), now)
            .expect("node-b is in discovery");
        warmup.reported_in(now);
        assert_eq!(
            placement.node_membership("node-b").await.unwrap(),
            NodeMembership::Present,
            "node-b just heartbeated and the gate is now warm"
        );

        assert_eq!(
            placement.node_membership("node-c").await.unwrap(),
            NodeMembership::Gone,
            "node-c is not discovered at all, and the gate is warm"
        );
    }

    #[tokio::test]
    async fn node_membership_opens_at_the_warmup_deadline_for_a_silent_straggler() {
        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![
                Node {
                    id: "node-a".to_string(),
                    endpoint: "http://10.0.0.7:8000".to_string(),
                    pod_name: String::new(),
                },
                Node {
                    id: "node-b".to_string(),
                    endpoint: "http://10.0.0.9:8000".to_string(),
                    pod_name: String::new(),
                },
            ],
            Duration::from_secs(30),
        ));
        let start = SystemTime::now();
        let warmup = Arc::new(WarmupGate::new(
            Arc::clone(&registry) as Arc<dyn crate::node_registry::registry::NodeRegistry>,
            Duration::from_secs(15),
            start,
        ));
        let placement = placement_with_warmup(Arc::clone(&registry), Arc::clone(&warmup));

        registry
            .heartbeat(&heartbeat("node-a"), start)
            .expect("node-a is in discovery");
        warmup.reported_in(start);

        placement
            .node_membership("node-b")
            .await
            .expect_err("node-b has never heartbeated and the deadline has not passed");

        // Check the synthetic deadline directly instead of waiting on wall clock.
        assert!(
            !warmup.warmed_up(start + Duration::from_secs(14)),
            "the gate must still be shut one second before its deadline, even with node-a \
             already reported"
        );
        assert!(
            warmup.warmed_up(start + Duration::from_secs(15)),
            "the gate must open at its deadline even with node-b still silent, now that \
             node-a has reported at least once"
        );
        // The direct deadline check latches the gate for subsequent real-time calls.
        assert_eq!(
            placement.node_membership("node-b").await.unwrap(),
            NodeMembership::Gone,
            "the gate is now latched warm, so node-b -- still silent -- must read as Gone"
        );
    }

    #[tokio::test]
    async fn node_membership_reports_gone_once_a_silent_node_stops_holding_its_veto() {
        // Report TTL under the veto window, so node-a is past reporting while its
        // veto is still inside its window: liveness is the only leg that can open
        // the gate here, and the node stays in discovery throughout.
        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![Node {
                id: "node-a".to_string(),
                endpoint: "http://10.0.0.7:8000".to_string(),
                pod_name: String::new(),
            }],
            Duration::from_secs(5),
        ));
        let start = SystemTime::now() - Duration::from_secs(60);
        let warmup = Arc::new(WarmupGate::new(
            Arc::clone(&registry) as Arc<dyn crate::node_registry::registry::NodeRegistry>,
            Duration::from_secs(300),
            start,
        ));
        let placement = placement_with_warmup(Arc::clone(&registry), Arc::clone(&warmup));

        registry
            .heartbeat(&heartbeat("node-a"), start)
            .expect("node-a is in discovery");
        warmup.roster_not_absorbed("node-a", start);
        assert!(
            !warmup.warmed_up(start),
            "sanity: an unabsorbed roster shuts the gate when it is recorded"
        );

        assert_eq!(
            placement.node_membership("node-c").await.unwrap(),
            NodeMembership::Gone,
            "node-a stopped reporting, so its unabsorbed roster withholds Gone no longer even \
             though the window it was stamped in has not run out -- withholding it forever is \
             how records on a dead node become unreclaimable"
        );
    }

    #[tokio::test]
    async fn node_membership_stays_cold_past_the_deadline_when_nothing_has_ever_reported() {
        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![Node {
                id: "node-a".to_string(),
                endpoint: "http://10.0.0.7:8000".to_string(),
                pod_name: String::new(),
            }],
            Duration::from_secs(30),
        ));
        // Elapsed deadline with zero reports must remain cold.
        let warmup = Arc::new(WarmupGate::new(
            Arc::clone(&registry) as Arc<dyn crate::node_registry::registry::NodeRegistry>,
            Duration::from_secs(1),
            SystemTime::UNIX_EPOCH,
        ));
        let placement = placement_with_warmup(Arc::clone(&registry), Arc::clone(&warmup));

        placement.node_membership("node-a").await.expect_err(
            "the deadline (anchored at the Unix epoch) has long since passed, but nothing has \
             ever reported -- this must still refuse rather than answer Gone",
        );
    }

    #[tokio::test]
    async fn resolve_node_names_warm_up_rather_than_absence_while_cold() {
        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![Node {
                id: "node-a".to_string(),
                endpoint: "http://10.0.0.7:8000".to_string(),
                pod_name: String::new(),
            }],
            Duration::from_secs(30),
        ));
        let now = SystemTime::now();
        let warmup = cold_gate(Arc::clone(&registry), now);
        let placement = placement_with_warmup(Arc::clone(&registry), warmup);

        let err = placement
            .resolve_node("node-a")
            .await
            .expect_err("node-a has never heartbeated and the gate is cold");
        assert!(
            err.to_string().contains("has not finished warming up"),
            "{err}"
        );
        assert!(
            !err.to_string().contains("no observed record"),
            "a cold-registry refusal must not be worded like a confirmed absence: {err}"
        );
    }
}
