//! A node's self-report as `/nodes` and `/nodes/{id}` render it.
//!
//! The conversions live here rather than beside either half's handlers because
//! both halves render the same snapshot and neither owns `models`.

use agentenv_http_server::models;

use super::{DiskMetric, MachineInfo, NodeMetricsSnapshot, NodeSnapshot};

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
pub fn node_status(node: &NodeSnapshot) -> models::NodeStatus {
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

/// The same snapshot as `/nodes/{id}` renders it.
///
/// The sandbox list is empty: a detail read reports the node, and the sandboxes
/// on it are the cluster registry's answer.
pub fn node_detail(node: NodeSnapshot) -> models::NodeDetail {
    let status = node_status(&node);
    models::NodeDetail::new(
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
    )
}
