use std::time::SystemTime;

use crate::node_registry::fleet::{FleetNode, FleetNodes};
use crate::observability::{DiskMetric, MachineInfo, NodeMetricsSnapshot, NodeSnapshot};
use crate::proto::scheduler as scheduler_proto;
use agentenv_http_server::{apis::admin::*, models};

use super::ApiImpl;

impl From<MachineInfo> for models::MachineInfo {
    fn from(machine_info: MachineInfo) -> Self {
        models::MachineInfo::new(
            machine_info.cpu_family,
            machine_info.cpu_model,
            machine_info.cpu_model_name,
            machine_info.cpu_architecture,
        )
    }
}

impl From<DiskMetric> for models::DiskMetrics {
    fn from(disk: DiskMetric) -> Self {
        models::DiskMetrics::new(
            disk.mount_point,
            disk.device,
            disk.filesystem_type,
            disk.used_bytes,
            disk.total_bytes,
        )
    }
}

impl From<NodeMetricsSnapshot> for models::NodeMetrics {
    fn from(metrics: NodeMetricsSnapshot) -> Self {
        models::NodeMetrics::new(
            metrics.allocated_cpu,
            metrics.cpu_percent,
            metrics.cpu_count,
            metrics.allocated_memory_bytes,
            metrics.memory_used_bytes,
            metrics.memory_total_bytes,
            metrics
                .disks
                .into_iter()
                .map(models::DiskMetrics::from)
                .collect(),
            0,
            0,
        )
    }
}

/// Maps node-observable state to its public status.
fn node_status(node: &NodeSnapshot) -> models::NodeStatus {
    if node.draining {
        models::NodeStatus::NodeStatusDraining
    } else {
        models::NodeStatus::NodeStatusReady
    }
}

impl From<NodeSnapshot> for models::Node {
    fn from(node: NodeSnapshot) -> Self {
        let status = node_status(&node);
        let egress_broker = node.egress_broker.as_str().to_string();
        let mut model = models::Node::new(
            node.version,
            node.commit,
            node.node_id,
            node.service_instance_id,
            node.cluster_id.to_string(),
            node.machine_info.into(),
            status,
            node.sandbox_count,
            node.metrics.into(),
            node.create_successes,
            node.create_fails,
            node.sandbox_starting_count,
            0,
        );
        model.egress_broker = Some(egress_broker);
        model
    }
}

/// The broker state as `/nodes` spells it. A wire value this build does not
/// know is left absent rather than rendered as a state that does not exist.
fn observed_egress_broker(snapshot: &scheduler_proto::NodeSnapshot) -> Option<String> {
    use scheduler_proto::EgressBrokerState as Wire;
    let label = match snapshot.egress_broker() {
        Wire::Disabled => "disabled",
        Wire::Embedded => "embedded",
        Wire::LocalOk => "local_ok",
        Wire::LocalUnreachable => "local_unreachable",
        Wire::Unspecified => return None,
    };
    Some(label.to_string())
}

/// Renders the optional `clusterID` filter the way the registry reads it.
///
/// An empty filter spans every cluster, which is what an absent parameter means.
fn cluster_filter(cluster_id: Option<uuid::Uuid>) -> String {
    cluster_id.map(|id| id.to_string()).unwrap_or_default()
}

/// Maps a requested node status to the scheduling override it names.
///
/// Only `ready` and `draining` are operator-settable; every other status is
/// derived by the scheduler and refused.
fn scheduling_override(status: models::NodeStatus) -> Result<bool, NodesNodeIdPostResponse> {
    match status {
        models::NodeStatus::NodeStatusDraining => Ok(true),
        models::NodeStatus::NodeStatusReady => Ok(false),
        status => Err(NodesNodeIdPostResponse::Status409_Conflict(ApiImpl::error(
            409,
            format!("node status {status} is derived by the scheduler and cannot be set"),
        ))),
    }
}

/// Maps a scheduler-observed status to its REST spelling.
///
/// The gateway spells the same statuses out of the same RPC
/// (`services/gateway/internal/node_list.go`); one node must not read
/// differently by which address asked.
fn observed_status(status: scheduler_proto::NodeStatus) -> models::NodeStatus {
    match status {
        scheduler_proto::NodeStatus::Ready => models::NodeStatus::NodeStatusReady,
        scheduler_proto::NodeStatus::Draining => models::NodeStatus::NodeStatusDraining,
        scheduler_proto::NodeStatus::Unhealthy => models::NodeStatus::NodeStatusUnhealthy,
        scheduler_proto::NodeStatus::Lingering => models::NodeStatus::NodeStatusLingering,
        // A derived observed view never leaves the status unspecified.
        scheduler_proto::NodeStatus::Connecting | scheduler_proto::NodeStatus::Unspecified => {
            models::NodeStatus::NodeStatusConnecting
        }
    }
}

fn observed_machine_info(
    machine_info: Option<scheduler_proto::MachineInfo>,
) -> models::MachineInfo {
    let machine_info = machine_info.unwrap_or_default();
    models::MachineInfo::new(
        machine_info.cpu_family,
        machine_info.cpu_model,
        machine_info.cpu_model_name,
        machine_info.cpu_architecture,
    )
}

fn observed_metrics(snapshot: &scheduler_proto::NodeSnapshot) -> models::NodeMetrics {
    models::NodeMetrics::new(
        snapshot.allocated_cpu,
        snapshot.cpu_percent,
        snapshot.cpu_count,
        snapshot.allocated_memory_bytes,
        snapshot.memory_used_bytes,
        snapshot.memory_total_bytes,
        snapshot
            .disks
            .iter()
            .map(|disk| {
                models::DiskMetrics::new(
                    disk.mount_point.clone(),
                    disk.device.clone(),
                    disk.filesystem_type.clone(),
                    disk.used_bytes,
                    disk.total_bytes,
                )
            })
            .collect(),
        0,
        0,
    )
}

/// Renders one cluster-observed node as a collection entry.
fn observed_node_model(observed: scheduler_proto::ObservedNode) -> models::Node {
    let snapshot = observed.snapshot.unwrap_or_default();
    let egress_broker = observed_egress_broker(&snapshot);
    let mut model = models::Node::new(
        observed.version,
        observed.commit,
        observed.node_id,
        observed.service_instance_id,
        observed.cluster_id,
        observed_machine_info(observed.machine_info),
        observed_status(snapshot.status()),
        snapshot.sandbox_count,
        observed_metrics(&snapshot),
        snapshot.create_successes,
        snapshot.create_fails,
        snapshot.sandbox_starting_count,
        0,
    );
    model.egress_broker = egress_broker;
    model
}

/// Renders one cluster-observed node as its detail view.
///
/// Cached builds are a node-local inventory the heartbeat does not carry.
fn observed_node_detail(observed: scheduler_proto::ObservedNode) -> models::NodeDetail {
    let snapshot = observed.snapshot.unwrap_or_default();
    models::NodeDetail::new(
        observed.cluster_id,
        observed.version,
        observed.commit,
        observed.node_id,
        observed.service_instance_id,
        observed_machine_info(observed.machine_info),
        observed_status(snapshot.status()),
        snapshot.sandbox_count,
        observed_metrics(&snapshot),
        vec![],
        snapshot.create_successes,
        snapshot.create_fails,
        0,
    )
}

impl ApiImpl {
    /// Lists the cluster's nodes, or this process's own report when it keeps no
    /// cluster view — a node's `/nodes` is its self-report surface.
    pub(super) async fn list_nodes(
        &self,
        query_params: &models::NodesGetQueryParams,
    ) -> Result<NodesGetResponse, ()> {
        let cluster_id = cluster_filter(query_params.cluster_id);
        if let FleetNodes::Cluster(nodes) = self.node_fleet().list(&cluster_id, SystemTime::now()) {
            return Ok(NodesGetResponse::Status200_SuccessfullyReturnedAllNodes(
                nodes.into_iter().map(observed_node_model).collect(),
            ));
        }

        let Some(observability) = self.observability() else {
            // When observability is disabled, the collection endpoint exposes
            // no nodes rather than returning a partial or synthetic record.
            return Ok(NodesGetResponse::Status200_SuccessfullyReturnedAllNodes(
                vec![],
            ));
        };
        if query_params
            .cluster_id
            .is_some_and(|cluster_id| cluster_id != observability.cluster_id())
        {
            return Ok(NodesGetResponse::Status200_SuccessfullyReturnedAllNodes(
                vec![],
            ));
        }
        let node = match observability.node_snapshot().await {
            Ok(node) => node,
            Err(err) => {
                return Ok(NodesGetResponse::Status500_ServerError(Self::error(
                    500,
                    err.to_string(),
                )));
            }
        };
        Ok(NodesGetResponse::Status200_SuccessfullyReturnedAllNodes(
            vec![models::Node::from(node)],
        ))
    }

    /// Reads one cluster node, or this process's own report when it keeps no
    /// cluster view.
    pub(super) async fn get_node(
        &self,
        path_params: &models::NodesNodeIdGetPathParams,
        query_params: &models::NodesNodeIdGetQueryParams,
    ) -> Result<NodesNodeIdGetResponse, ()> {
        let cluster_id = cluster_filter(query_params.cluster_id);
        match self
            .node_fleet()
            .get(&path_params.node_id, &cluster_id, SystemTime::now())
        {
            FleetNode::Observed { node, .. } => {
                return Ok(
                    NodesNodeIdGetResponse::Status200_SuccessfullyReturnedTheNode(
                        observed_node_detail(*node),
                    ),
                );
            }
            FleetNode::Absent => {
                return Ok(NodesNodeIdGetResponse::Status404_NotFound(Self::error(
                    404,
                    format!("node {} not found", path_params.node_id),
                )));
            }
            FleetNode::SelfReport => {}
        }

        let Some(observability) = self.observability() else {
            // A disabled observability service behaves like node details are
            // unavailable on this process.
            return Ok(NodesNodeIdGetResponse::Status404_NotFound(Self::error(
                404,
                "observability is disabled on this node",
            )));
        };
        let cluster_mismatch = query_params
            .cluster_id
            .map(|cluster_id| cluster_id != observability.cluster_id())
            .unwrap_or(false);
        if path_params.node_id != observability.node_id() || cluster_mismatch {
            return Ok(NodesNodeIdGetResponse::Status404_NotFound(Self::error(
                404,
                format!("node {} not found", path_params.node_id),
            )));
        }

        let node = match observability.node_snapshot().await {
            Ok(node) => node,
            Err(err) => {
                return Ok(NodesNodeIdGetResponse::Status500_ServerError(Self::error(
                    500,
                    err.to_string(),
                )));
            }
        };

        let status = node_status(&node);
        let detail = models::NodeDetail::new(
            node.cluster_id.to_string(),
            node.version,
            node.commit,
            node.node_id,
            node.service_instance_id,
            node.machine_info.into(),
            status,
            node.sandbox_count,
            node.metrics.into(),
            vec![],
            node.create_successes,
            node.create_fails,
            0,
        );
        Ok(NodesNodeIdGetResponse::Status200_SuccessfullyReturnedTheNode(detail))
    }

    /// Sets a node to `ready` or `draining`; scheduler-derived statuses return 409.
    ///
    /// A cluster-observed node is set over its node service. The reply carries
    /// no status: the observed one catches up on that node's next heartbeat,
    /// so a read immediately after this call may still see the old value.
    pub(super) async fn set_node_status(
        &self,
        path_params: &models::NodesNodeIdPostPathParams,
        query_params: &models::NodesNodeIdPostQueryParams,
        body: &models::NodeStatusChange,
    ) -> Result<NodesNodeIdPostResponse, ()> {
        let cluster_id = cluster_filter(query_params.cluster_id.or(body.cluster_id));
        match self
            .node_fleet()
            .get(&path_params.node_id, &cluster_id, SystemTime::now())
        {
            FleetNode::Observed {
                node,
                node_service_port,
            } => {
                let disabled = match scheduling_override(body.status) {
                    Ok(disabled) => disabled,
                    Err(refused) => return Ok(refused),
                };
                if let Err(err) = crate::node_client::override_node_status(
                    &node.endpoint,
                    node_service_port,
                    disabled,
                )
                .await
                {
                    return Ok(NodesNodeIdPostResponse::Status500_ServerError(Self::error(
                        500,
                        format!("{err:#}"),
                    )));
                }
                return Ok(NodesNodeIdPostResponse::Status204_TheNodeStatusWasChangedSuccessfully);
            }
            FleetNode::Absent => {
                return Ok(NodesNodeIdPostResponse::Status404_NotFound(Self::error(
                    404,
                    format!("node {} not found", path_params.node_id),
                )));
            }
            FleetNode::SelfReport => {}
        }

        let Some(observability) = self.observability() else {
            return Ok(NodesNodeIdPostResponse::Status404_NotFound(Self::error(
                404,
                "observability is disabled on this node",
            )));
        };

        // Both route and optional cluster identities must address this node.
        let cluster_mismatch = query_params
            .cluster_id
            .or(body.cluster_id)
            .map(|cluster_id| cluster_id != observability.cluster_id())
            .unwrap_or(false);
        if path_params.node_id != observability.node_id() || cluster_mismatch {
            return Ok(NodesNodeIdPostResponse::Status404_NotFound(Self::error(
                404,
                format!("node {} not found", path_params.node_id),
            )));
        }

        let disabled = match scheduling_override(body.status) {
            Ok(disabled) => disabled,
            Err(refused) => return Ok(refused),
        };

        self.orchestrator().set_scheduling_disabled(disabled);

        Ok(NodesNodeIdPostResponse::Status204_TheNodeStatusWasChangedSuccessfully)
    }
}

#[cfg(test)]
mod fleet_node_tests {
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

    use axum_extra::extract::CookieJar;
    use headers::Host;
    use http::Method;
    use serde_json::{json, Value};

    use agentenv_http_server::apis::admin::*;
    use agentenv_http_server::models;

    use super::{observed_node_detail, observed_node_model, observed_status, ApiImpl};
    use crate::identity::NodeIdentity;
    use crate::node_registry::registry::{AtomicNodeRegistry, NodeRegistry};
    use crate::node_registry::types::Node as DiscoveredNode;
    use crate::orchestrator::Orchestrator;
    use crate::proto::scheduler as scheduler_proto;
    use crate::sandbox::mock::MockBackendFactory;
    use crate::snapshot::mock::mock_snapshot_manager;

    /// An observed node whose every field carries a value distinct from the rest.
    fn sentinel_observed() -> scheduler_proto::ObservedNode {
        scheduler_proto::ObservedNode {
            node_id: "node-a".to_string(),
            endpoint: "http://10.0.0.1:8080".to_string(),
            cluster_id: "cluster-1".to_string(),
            service_instance_id: "svc-a".to_string(),
            version: "0.1.2".to_string(),
            commit: "abcdef0".to_string(),
            machine_info: Some(scheduler_proto::MachineInfo {
                cpu_family: "6".to_string(),
                cpu_model: "143".to_string(),
                cpu_model_name: "Xeon Platinum".to_string(),
                cpu_architecture: "x86_64".to_string(),
                cpu_config_json: "{}".to_string(),
            }),
            snapshot: Some(scheduler_proto::NodeSnapshot {
                egress_broker: 0,
                status: scheduler_proto::NodeStatus::Ready as i32,
                allocated_cpu: 11,
                allocated_memory_bytes: 12,
                cpu_percent: 13,
                cpu_count: 14,
                memory_used_bytes: 15,
                memory_total_bytes: 16,
                disks: vec![scheduler_proto::DiskMetric {
                    mount_point: "/".to_string(),
                    device: "/dev/sda1".to_string(),
                    filesystem_type: "ext4".to_string(),
                    used_bytes: 17,
                    total_bytes: 18,
                }],
                sandbox_count: 19,
                sandbox_starting_count: 20,
                create_successes: 21,
                create_fails: 22,
                reported_at_unix_ms: 1_700_000_000_000,
            }),
            last_seen_unix_ms: 1_700_000_000_001,
        }
    }

    fn value(model: impl serde::Serialize) -> Value {
        serde_json::to_value(model).expect("the model serializes")
    }

    #[test]
    fn a_listed_node_carries_every_field_the_gateway_renders() {
        // 🔴 Mirrors `nodeListItem`'s JSON tags in
        // services/gateway/internal/node_list.go. Two addresses, one shape.
        assert_eq!(
            value(observed_node_model(sentinel_observed())),
            json!({
                "version": "0.1.2",
                "commit": "abcdef0",
                "id": "node-a",
                "serviceInstanceID": "svc-a",
                "clusterID": "cluster-1",
                "machineInfo": {
                    "cpuFamily": "6",
                    "cpuModel": "143",
                    "cpuModelName": "Xeon Platinum",
                    "cpuArchitecture": "x86_64",
                },
                "status": "ready",
                "sandboxCount": 19,
                "metrics": {
                    "allocatedCPU": 11,
                    "cpuPercent": 13,
                    "cpuCount": 14,
                    "allocatedMemoryBytes": 12,
                    "memoryUsedBytes": 15,
                    "memoryTotalBytes": 16,
                    "disks": [{
                        "mountPoint": "/",
                        "device": "/dev/sda1",
                        "filesystemType": "ext4",
                        "usedBytes": 17,
                        "totalBytes": 18,
                    }],
                    "pausedAllocatedCPU": 0,
                    "pausedAllocatedMemoryBytes": 0,
                },
                "createSuccesses": 21,
                "createFails": 22,
                "sandboxStartingCount": 20,
                "sandboxPausedCount": 0,
            })
        );
    }

    #[test]
    fn a_node_detail_carries_the_same_reading_without_the_starting_count() {
        let detail = value(observed_node_detail(sentinel_observed()));
        let listed = value(observed_node_model(sentinel_observed()));

        let models::NodeDetail {
            cluster_id: _,
            version: _,
            commit: _,
            id: _,
            service_instance_id: _,
            machine_info: _,
            status: _,
            sandbox_count: _,
            metrics: _,
            cached_builds,
            create_successes: _,
            create_fails: _,
            sandbox_paused_count: _,
        } = observed_node_detail(sentinel_observed());
        assert!(
            cached_builds.is_empty(),
            "the heartbeat carries no cached-build inventory"
        );

        for (key, value) in listed.as_object().expect("an object") {
            if key == "sandboxStartingCount" {
                assert!(
                    !detail.as_object().expect("an object").contains_key(key),
                    "the detail schema has no starting count"
                );
                continue;
            }
            assert_eq!(detail.get(key), Some(value), "{key} disagrees");
        }
    }

    #[test]
    fn every_observed_status_keeps_the_gateway_spelling() {
        // 🔴 The right column is `nodeStatusToString` in node_list.go.
        for (observed, spelling) in [
            (scheduler_proto::NodeStatus::Ready, "ready"),
            (scheduler_proto::NodeStatus::Connecting, "connecting"),
            (scheduler_proto::NodeStatus::Unhealthy, "unhealthy"),
            (scheduler_proto::NodeStatus::Lingering, "lingering"),
            (scheduler_proto::NodeStatus::Draining, "draining"),
        ] {
            assert_eq!(
                observed_status(observed).to_string(),
                spelling,
                "{observed:?} must read the same through either address"
            );
        }
    }

    fn observing(nodes: &[&str]) -> Arc<AtomicNodeRegistry> {
        let registry = Arc::new(AtomicNodeRegistry::new(
            nodes
                .iter()
                .map(|id| DiscoveredNode {
                    id: (*id).to_string(),
                    endpoint: format!("http://{id}:8080"),
                    pod_name: (*id).to_string(),
                })
                .collect(),
            Duration::from_secs(30),
        ));
        for id in nodes {
            registry
                .heartbeat(
                    &scheduler_proto::HeartbeatRequest {
                        node_id: (*id).to_string(),
                        cluster_id: String::new(),
                        service_instance_id: format!("svc-{id}"),
                        snapshot: Some(scheduler_proto::NodeSnapshot {
                            status: scheduler_proto::NodeStatus::Ready as i32,
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                    SystemTime::now(),
                )
                .expect("the heartbeat lands");
        }
        registry
    }

    async fn api(fleet: Option<Arc<AtomicNodeRegistry>>) -> ApiImpl {
        // Port 1 answers nothing, so any test that reaches a dial fails fast.
        api_with_node_service_port(fleet, 1).await
    }

    async fn api_with_node_service_port(
        fleet: Option<Arc<AtomicNodeRegistry>>,
        node_service_port: u16,
    ) -> ApiImpl {
        let orchestrator = Orchestrator::with_in_memory_store(MockBackendFactory::new()).await;
        let identity = NodeIdentity::from_config(&Default::default());

        let api = ApiImpl::new(
            orchestrator,
            Arc::new(mock_snapshot_manager()),
            None,
            Vec::new(),
            crate::api::ResumeWiring::node_local(identity.id.clone()),
        );
        match fleet {
            Some(registry) => {
                api.with_node_fleet(registry as Arc<dyn NodeRegistry>, node_service_port)
            }
            None => api,
        }
    }

    async fn list(api: &ApiImpl) -> Vec<models::Node> {
        let response = api
            .nodes_get(
                &Method::GET,
                &Host::from(http::uri::Authority::from_static("localhost")),
                &CookieJar::new(),
                &crate::api::impls::Claims,
                &models::NodesGetQueryParams { cluster_id: None },
            )
            .await
            .expect("the handler answers");
        match response {
            NodesGetResponse::Status200_SuccessfullyReturnedAllNodes(nodes) => nodes,
            other => panic!("expected a listing, got {other:?}"),
        }
    }

    async fn detail(api: &ApiImpl, node_id: &str) -> NodesNodeIdGetResponse {
        api.nodes_node_id_get(
            &Method::GET,
            &Host::from(http::uri::Authority::from_static("localhost")),
            &CookieJar::new(),
            &crate::api::impls::Claims,
            &models::NodesNodeIdGetPathParams {
                node_id: node_id.to_string(),
            },
            &models::NodesNodeIdGetQueryParams { cluster_id: None },
        )
        .await
        .expect("the handler answers")
    }

    #[tokio::test]
    async fn a_fleet_view_lists_every_observed_node() {
        let api = api(Some(observing(&["node-a", "node-b"]))).await;

        let mut ids: Vec<String> = list(&api).await.into_iter().map(|node| node.id).collect();
        ids.sort();

        assert_eq!(ids, vec!["node-a".to_string(), "node-b".to_string()]);
    }

    #[tokio::test]
    async fn without_a_fleet_view_the_local_report_still_answers() {
        // Observability is off here, so the self-report surface has nothing to
        // show — but it is still the surface being asked.
        let api = api(None).await;

        assert!(list(&api).await.is_empty());
    }

    #[tokio::test]
    async fn a_fleet_view_reads_one_node_from_the_cluster() {
        let api = api(Some(observing(&["node-a", "node-b"]))).await;

        match detail(&api, "node-b").await {
            NodesNodeIdGetResponse::Status200_SuccessfullyReturnedTheNode(node) => {
                assert_eq!(node.id, "node-b");
                assert_eq!(node.status, models::NodeStatus::NodeStatusReady);
            }
            other => panic!("expected the node, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_fleet_view_that_does_not_observe_a_node_is_a_404() {
        let api = api(Some(observing(&["node-a"]))).await;

        assert!(matches!(
            detail(&api, "node-z").await,
            NodesNodeIdGetResponse::Status404_NotFound(_)
        ));
    }

    async fn set_status(
        api: &ApiImpl,
        node_id: &str,
        status: models::NodeStatus,
    ) -> NodesNodeIdPostResponse {
        api.nodes_node_id_post(
            &Method::POST,
            &Host::from(http::uri::Authority::from_static("localhost")),
            &CookieJar::new(),
            &crate::api::impls::Claims,
            &models::NodesNodeIdPostPathParams {
                node_id: node_id.to_string(),
            },
            &models::NodesNodeIdPostQueryParams { cluster_id: None },
            &models::NodeStatusChange {
                cluster_id: None,
                status,
            },
        )
        .await
        .expect("the handler answers")
    }

    #[tokio::test]
    async fn a_status_change_for_an_unobserved_node_is_a_404() {
        let api = api(Some(observing(&["node-a"]))).await;

        assert!(matches!(
            set_status(&api, "node-z", models::NodeStatus::NodeStatusDraining).await,
            NodesNodeIdPostResponse::Status404_NotFound(_)
        ));
    }

    #[tokio::test]
    async fn a_scheduler_derived_status_is_refused_before_the_node_is_dialed() {
        // The fixture endpoints resolve nowhere; answering 409 rather than a
        // dial failure proves the refusal happens without a dial.
        let api = api(Some(observing(&["node-a"]))).await;

        assert!(matches!(
            set_status(&api, "node-a", models::NodeStatus::NodeStatusUnhealthy).await,
            NodesNodeIdPostResponse::Status409_Conflict(_)
        ));
    }

    #[tokio::test]
    async fn an_unreachable_observed_node_surfaces_as_a_500_not_a_local_flip() {
        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![DiscoveredNode {
                id: "node-a".to_string(),
                // Nothing listens on port 1: the dial must fail, and the
                // failure must surface instead of flipping this process.
                endpoint: "http://127.0.0.1:1".to_string(),
                pod_name: "node-a".to_string(),
            }],
            Duration::from_secs(30),
        ));
        registry
            .heartbeat(
                &scheduler_proto::HeartbeatRequest {
                    node_id: "node-a".to_string(),
                    service_instance_id: "svc-node-a".to_string(),
                    snapshot: Some(scheduler_proto::NodeSnapshot {
                        status: scheduler_proto::NodeStatus::Ready as i32,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                SystemTime::now(),
            )
            .expect("the heartbeat lands");
        let api = api(Some(registry)).await;

        assert!(matches!(
            set_status(&api, "node-a", models::NodeStatus::NodeStatusDraining).await,
            NodesNodeIdPostResponse::Status500_ServerError(_)
        ));
    }

    /// A node service that answers only status overrides, recording each one.
    struct OverrideOnlyNode {
        seen: Arc<std::sync::Mutex<Vec<bool>>>,
    }

    #[tonic::async_trait]
    impl crate::proto::node::node_sandbox_service_server::NodeSandboxService for OverrideOnlyNode {
        async fn override_status(
            &self,
            request: tonic::Request<crate::proto::node::NodeStatusOverrideRequest>,
        ) -> Result<tonic::Response<crate::proto::node::NodeStatusOverrideResponse>, tonic::Status>
        {
            self.seen
                .lock()
                .expect("seen")
                .push(request.into_inner().scheduling_disabled);
            Ok(tonic::Response::new(
                crate::proto::node::NodeStatusOverrideResponse {},
            ))
        }

        async fn create(
            &self,
            _request: tonic::Request<crate::proto::node::SandboxCreateRequest>,
        ) -> Result<tonic::Response<crate::proto::node::SandboxCreateResponse>, tonic::Status>
        {
            Err(tonic::Status::unimplemented("override only"))
        }
        async fn delete(
            &self,
            _request: tonic::Request<crate::proto::node::SandboxDeleteRequest>,
        ) -> Result<tonic::Response<crate::proto::node::SandboxDeleteResponse>, tonic::Status>
        {
            Err(tonic::Status::unimplemented("override only"))
        }
        async fn pause(
            &self,
            _request: tonic::Request<crate::proto::node::SandboxPauseRequest>,
        ) -> Result<tonic::Response<crate::proto::node::SandboxPauseResponse>, tonic::Status>
        {
            Err(tonic::Status::unimplemented("override only"))
        }
        async fn checkpoint(
            &self,
            _request: tonic::Request<crate::proto::node::SandboxCheckpointRequest>,
        ) -> Result<tonic::Response<crate::proto::node::SandboxCheckpointResponse>, tonic::Status>
        {
            Err(tonic::Status::unimplemented("override only"))
        }
        async fn fork(
            &self,
            _request: tonic::Request<crate::proto::node::SandboxForkRequest>,
        ) -> Result<tonic::Response<crate::proto::node::SandboxForkResponse>, tonic::Status>
        {
            Err(tonic::Status::unimplemented("override only"))
        }
        async fn update_network(
            &self,
            _request: tonic::Request<crate::proto::node::SandboxNetworkRequest>,
        ) -> Result<tonic::Response<crate::proto::node::SandboxNetworkResponse>, tonic::Status>
        {
            Err(tonic::Status::unimplemented("override only"))
        }
        async fn update_params(
            &self,
            _request: tonic::Request<crate::proto::node::SandboxParamsRequest>,
        ) -> Result<tonic::Response<crate::proto::node::SandboxParamsResponse>, tonic::Status>
        {
            Err(tonic::Status::unimplemented("override only"))
        }
        async fn describe(
            &self,
            _request: tonic::Request<crate::proto::node::SandboxDescribeRequest>,
        ) -> Result<tonic::Response<crate::proto::node::SandboxDescribeResponse>, tonic::Status>
        {
            Err(tonic::Status::unimplemented("override only"))
        }
        async fn list_sandboxes(
            &self,
            _request: tonic::Request<crate::proto::node::ListSandboxesRequest>,
        ) -> Result<tonic::Response<crate::proto::node::SandboxListResponse>, tonic::Status>
        {
            Err(tonic::Status::unimplemented("override only"))
        }
        async fn build_template(
            &self,
            _request: tonic::Request<crate::proto::node::TemplateBuildRequest>,
        ) -> Result<tonic::Response<crate::proto::node::TemplateBuildResponse>, tonic::Status>
        {
            Err(tonic::Status::unimplemented("override only"))
        }
    }

    #[tokio::test]
    async fn a_drain_dials_the_node_service_port_not_the_advertised_address() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind a port");
        let node_service_port = listener.local_addr().expect("the bound address").port();
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let (shutdown, rx) = tokio::sync::oneshot::channel::<()>();
        let service = OverrideOnlyNode {
            seen: Arc::clone(&seen),
        };
        tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(
                    crate::proto::node::node_sandbox_service_server::NodeSandboxServiceServer::new(
                        service,
                    ),
                )
                .serve_with_incoming_shutdown(
                    tonic::transport::server::TcpIncoming::from(listener),
                    async {
                        let _ = rx.await;
                    },
                )
                .await;
        });

        // The registry advertises the node by its user-facing address, whose
        // port answers nothing here — the request can only succeed through the
        // node-service port rewrite. This is the axis the cluster caught when
        // the fixtures all handed the client a directly dialable address.
        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![DiscoveredNode {
                id: "node-a".to_string(),
                endpoint: "http://127.0.0.1:1".to_string(),
                pod_name: "node-a".to_string(),
            }],
            Duration::from_secs(30),
        ));
        registry
            .heartbeat(
                &scheduler_proto::HeartbeatRequest {
                    node_id: "node-a".to_string(),
                    service_instance_id: "svc-node-a".to_string(),
                    snapshot: Some(scheduler_proto::NodeSnapshot {
                        status: scheduler_proto::NodeStatus::Ready as i32,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                SystemTime::now(),
            )
            .expect("the heartbeat lands");
        let api = api_with_node_service_port(Some(registry), node_service_port).await;

        assert!(matches!(
            set_status(&api, "node-a", models::NodeStatus::NodeStatusDraining).await,
            NodesNodeIdPostResponse::Status204_TheNodeStatusWasChangedSuccessfully
        ));
        assert_eq!(
            seen.lock().expect("seen").as_slice(),
            &[true],
            "the drain must land on the node service, once"
        );
        drop(shutdown);
    }

    #[test]
    fn only_ready_and_draining_are_operator_settable() {
        use super::scheduling_override;

        assert!(!scheduling_override(models::NodeStatus::NodeStatusReady).expect("settable"));
        assert!(scheduling_override(models::NodeStatus::NodeStatusDraining).expect("settable"));
        assert!(scheduling_override(models::NodeStatus::NodeStatusUnhealthy).is_err());
        assert!(scheduling_override(models::NodeStatus::NodeStatusConnecting).is_err());
    }
}
