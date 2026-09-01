//! The one registry read path behind node reads, whichever surface asks.
//!
//! `scheduler.v1`'s `ListObservedNodes`/`GetNode` — which the gateway aggregates
//! `GET /nodes` from — and the api half's own REST `/nodes` both read through
//! here, so a directly addressed REST answer and a gateway-aggregated one cannot
//! diverge in what they saw.

use std::sync::Arc;
use std::time::SystemTime;

use super::registry::NodeRegistry;
use crate::proto::scheduler::ObservedNode;

/// Reads every observed node in a cluster; an empty `cluster_id` spans all of them.
pub fn observed_nodes(
    registry: &dyn NodeRegistry,
    cluster_id: &str,
    now: SystemTime,
) -> Vec<ObservedNode> {
    registry.list_observed(cluster_id, now)
}

/// Reads one observed node by its canonical id.
///
/// An empty id addresses nothing, and an id outside `cluster_id` is not observed.
pub fn observed_node(
    registry: &dyn NodeRegistry,
    node_id: &str,
    cluster_id: &str,
    now: SystemTime,
) -> Option<ObservedNode> {
    let node_id = node_id.trim();
    if node_id.is_empty() {
        return None;
    }
    registry.get_observed(node_id, cluster_id, now)
}

/// Whether this process can answer node reads for the whole cluster.
///
/// The verdict travels with the value: a caller cannot take a node list out of
/// this handle without also being told whose list it is.
#[derive(Clone)]
pub struct NodeFleetView {
    registry: Option<Arc<dyn NodeRegistry>>,
}

/// The answer to a fleet-wide node list.
pub enum FleetNodes {
    /// This process observes the cluster; these are its nodes.
    Cluster(Vec<ObservedNode>),
    /// This process observes no cluster, so only its own report exists.
    SelfReport,
}

/// The answer to a single-node read.
pub enum FleetNode {
    /// The cluster view holds this node.
    Observed(Box<ObservedNode>),
    /// The cluster view is authoritative and does not hold this node.
    Absent,
    /// This process observes no cluster, so only its own report exists.
    SelfReport,
}

impl NodeFleetView {
    /// A process that observes no cluster, such as a node reporting itself.
    pub fn self_report() -> Self {
        Self { registry: None }
    }

    /// A process holding the cluster's node registry.
    pub fn cluster(registry: Arc<dyn NodeRegistry>) -> Self {
        Self {
            registry: Some(registry),
        }
    }

    /// Lists the cluster's nodes, or says this process has no such list.
    pub fn list(&self, cluster_id: &str, now: SystemTime) -> FleetNodes {
        match &self.registry {
            Some(registry) => {
                FleetNodes::Cluster(observed_nodes(registry.as_ref(), cluster_id, now))
            }
            None => FleetNodes::SelfReport,
        }
    }

    /// Reads one of the cluster's nodes, or says this process has no such view.
    pub fn get(&self, node_id: &str, cluster_id: &str, now: SystemTime) -> FleetNode {
        let Some(registry) = &self.registry else {
            return FleetNode::SelfReport;
        };
        match observed_node(registry.as_ref(), node_id, cluster_id, now) {
            Some(node) => FleetNode::Observed(Box::new(node)),
            None => FleetNode::Absent,
        }
    }
}
