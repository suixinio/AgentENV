//! Port of `services/scheduler/internal/filter.go` — narrowing a candidate
//! node list down to what may actually be scheduled onto.
//!
//! One pure filter lives here: [`filter_unschedulable`] drops nodes that
//! self-reported they are not taking new work (`DRAINING`). It runs on
//! every placement, from [`crate::binding_store::lookup::select_node`].
//!
//! Go's second filter — an operator-configured per-node resource ceiling
//! (`services/shared/config.NodeResourceLimit`) — had a stand-in here that
//! was never wired to anything: no `AppConfig` field, no builder on
//! [`super::grpc_service::NodeRegistryGrpcService`], and therefore no
//! caller that could pass it anything but `None`. It has been deleted
//! rather than kept as a permanently-disabled branch; reinstating it means
//! porting `filter.go` again, not un-commenting this file.

use crate::node_registry::types::RichNode;
use crate::proto::scheduler::NodeStatus;

/// Removes nodes whose own last heartbeat says they are not taking new
/// work — today that means a node isolated through its admin API, which
/// reports `DRAINING`.
///
/// Only a status the node actually reported is acted on. A node with no
/// snapshot yet, or one reporting `UNSPECIFIED`, is kept: it has just
/// registered and has not had a chance to say anything about itself, and
/// dropping it would leave a freshly started cluster with nothing to
/// schedule onto until the first heartbeat lands. Fail open on what we do
/// not know, fail closed on what a node told us.
///
/// Statuses the scheduler derives rather than receives — `LINGERING` from
/// pod termination, `UNHEALTHY` from a lost heartbeat — are not handled
/// here. Those come from discovery and heartbeat expiry, and are filtered
/// upstream of this call.
pub fn filter_unschedulable(nodes: Vec<RichNode>) -> Vec<RichNode> {
    nodes
        .into_iter()
        .filter(|n| {
            let status = n
                .snapshot
                .as_ref()
                .map(|s| s.status())
                .unwrap_or(NodeStatus::Unspecified);
            !(status != NodeStatus::Unspecified && !status.can_accept_new_requests())
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node_registry::types::Node;
    use crate::proto::scheduler::NodeSnapshot;

    fn node(id: &str) -> Node {
        Node {
            id: id.to_string(),
            endpoint: String::new(),
            pod_name: String::new(),
        }
    }

    fn with_snapshot(id: &str, snapshot: NodeSnapshot) -> RichNode {
        RichNode::with_snapshot(node(id), snapshot)
    }

    fn no_snapshot(id: &str) -> RichNode {
        RichNode::new(node(id))
    }

    fn ids(nodes: &[RichNode]) -> Vec<&str> {
        nodes.iter().map(|n| n.node.id.as_str()).collect()
    }

    #[test]
    fn unschedulable_drops_self_reported_draining() {
        let nodes = vec![
            with_snapshot(
                "ready",
                NodeSnapshot {
                    status: NodeStatus::Ready as i32,
                    ..Default::default()
                },
            ),
            with_snapshot(
                "isolated",
                NodeSnapshot {
                    status: NodeStatus::Draining as i32,
                    ..Default::default()
                },
            ),
        ];
        let result = filter_unschedulable(nodes);
        assert_eq!(ids(&result), vec!["ready"]);
    }

    /// A node that has registered but not yet reported must stay
    /// schedulable — otherwise a freshly started cluster has nothing to
    /// place sandboxes on until the first heartbeat lands.
    #[test]
    fn unschedulable_keeps_nodes_that_have_not_reported() {
        let nodes = vec![
            no_snapshot("no-snapshot"),
            with_snapshot("unspecified", NodeSnapshot::default()),
        ];
        let result = filter_unschedulable(nodes);
        assert_eq!(ids(&result), vec!["no-snapshot", "unspecified"]);
    }

    #[test]
    fn node_status_can_accept_new_requests() {
        assert!(NodeStatus::Ready.can_accept_new_requests());
        assert!(!NodeStatus::Draining.can_accept_new_requests());
        assert!(!NodeStatus::Lingering.can_accept_new_requests());
        assert!(!NodeStatus::Unhealthy.can_accept_new_requests());
        assert!(!NodeStatus::Connecting.can_accept_new_requests());
        assert!(!NodeStatus::Unspecified.can_accept_new_requests());
    }
}
