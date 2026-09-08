//! Concrete round-robin node placement.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::node_registry::types::RichNode;
use crate::proto::scheduler::ScheduleRequestHint;

/// Empty-candidate error whose text is returned over the scheduler wire contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("no nodes available")]
pub struct NoNodesAvailable;

/// Stable strategy metric-label value.
pub const ROUND_ROBIN_NAME: &str = "round_robin";

/// Advances one shared cursor through candidates in order.
///
/// Schedule and paused lookup must share an instance to share rotation.
#[derive(Debug, Default)]
pub struct RoundRobinStrategy {
    next: AtomicU64,
}

impl RoundRobinStrategy {
    pub fn new() -> Self {
        Self::default()
    }

    /// Selects one candidate; round-robin ignores the request hint.
    pub fn select(
        &self,
        nodes: &[RichNode],
        _hint: Option<&ScheduleRequestHint>,
    ) -> Result<RichNode, NoNodesAvailable> {
        if nodes.is_empty() {
            return Err(NoNodesAvailable);
        }
        // Wrapping is harmless because selection uses modulo candidate count.
        let idx = self.next.fetch_add(1, Ordering::Relaxed);
        Ok(nodes[(idx % nodes.len() as u64) as usize].clone())
    }

    pub fn name(&self) -> &'static str {
        ROUND_ROBIN_NAME
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node_registry::types::Node;

    fn rich(id: &str) -> RichNode {
        RichNode::new(Node {
            id: id.to_string(),
            endpoint: String::new(),
            pod_name: String::new(),
        })
    }

    #[test]
    fn round_robin_cycles_in_order() {
        let s = RoundRobinStrategy::new();
        let nodes = vec![rich("a"), rich("b"), rich("c")];

        let got1 = s.select(&nodes, None).unwrap();
        let got2 = s.select(&nodes, None).unwrap();
        let got3 = s.select(&nodes, None).unwrap();
        let got4 = s.select(&nodes, None).unwrap();

        assert_eq!(
            [got1.node.id, got2.node.id, got3.node.id, got4.node.id],
            ["a", "b", "c", "a"]
        );
    }

    #[test]
    fn round_robin_no_nodes_errors() {
        let s = RoundRobinStrategy::new();
        assert!(s.select(&[], None).is_err());
    }

    #[test]
    fn the_strategy_metric_label_value_is_round_robin() {
        assert_eq!(ROUND_ROBIN_NAME, "round_robin");
        assert_eq!(RoundRobinStrategy::new().name(), "round_robin");
    }
}
