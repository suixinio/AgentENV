//! Filters real placement candidates using node-reported schedulability.

use crate::node_registry::types::RichNode;
use crate::proto::scheduler::NodeStatus;

/// Removes nodes whose latest heartbeat says they cannot accept new requests.
///
/// Nodes without a snapshot or explicit status remain eligible until they report.
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
    fn unschedulable_keeps_and_drops_the_whole_status_set() {
        let keep = [NodeStatus::Unspecified, NodeStatus::Ready];
        let drop = [
            NodeStatus::Connecting,
            NodeStatus::Unhealthy,
            NodeStatus::Lingering,
            NodeStatus::Draining,
        ];

        // Exhaustively account for the generated status enum.
        let mut all: Vec<i32> = keep
            .iter()
            .chain(drop.iter())
            .map(|status| *status as i32)
            .collect();
        all.sort_unstable();
        all.dedup();
        assert_eq!(
            all,
            vec![0, 1, 2, 3, 4, 5],
            "NodeStatus gained or lost a variant; decide here whether it is schedulable"
        );

        for status in keep {
            let kept = filter_unschedulable(vec![with_snapshot(
                "n",
                NodeSnapshot {
                    status: status as i32,
                    ..Default::default()
                },
            )]);
            assert_eq!(ids(&kept), vec!["n"], "{status:?} must stay schedulable");
        }
        for status in drop {
            let kept = filter_unschedulable(vec![with_snapshot(
                "n",
                NodeSnapshot {
                    status: status as i32,
                    ..Default::default()
                },
            )]);
            assert!(kept.is_empty(), "{status:?} must be dropped");
        }

        assert_eq!(
            ids(&filter_unschedulable(vec![no_snapshot("never-reported")])),
            vec!["never-reported"],
        );
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
