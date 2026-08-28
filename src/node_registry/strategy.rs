//! Port of `services/scheduler/internal/strategy.go` (60 lines) — the
//! round-robin placement strategy `Schedule` uses to pick a node.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::node_registry::types::RichNode;
use crate::proto::scheduler::ScheduleRequestHint;

/// Mirrors Go's `ErrNoNodes` — the empty-candidate-list outcome every
/// [`Strategy`] implementation reports the same way.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("no nodes available")]
pub struct NoNodesAvailable;

pub trait Strategy: Send + Sync {
    /// Picks one node from `nodes`. `hint` carries the same
    /// `ScheduleRequestHint` `Schedule` received, for a strategy that wants
    /// to weigh it — the strategy ported here does not read it, matching Go.
    fn select(
        &self,
        nodes: &[RichNode],
        hint: Option<&ScheduleRequestHint>,
    ) -> Result<RichNode, NoNodesAvailable>;

    fn name(&self) -> &'static str;
}

/// Cycles through `nodes` in order, advancing one slot per call regardless of
/// which strategy instance is asked — state lives in `next`, not in the
/// argument list.
#[derive(Debug, Default)]
pub struct RoundRobinStrategy {
    next: AtomicU64,
}

impl RoundRobinStrategy {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Strategy for RoundRobinStrategy {
    fn select(
        &self,
        nodes: &[RichNode],
        _hint: Option<&ScheduleRequestHint>,
    ) -> Result<RichNode, NoNodesAvailable> {
        if nodes.is_empty() {
            return Err(NoNodesAvailable);
        }
        // fetch_add wraps on overflow the same way Go's atomic.AddUint64
        // does; the modulo below is what actually matters for the index.
        let idx = self.next.fetch_add(1, Ordering::Relaxed);
        Ok(nodes[(idx % nodes.len() as u64) as usize].clone())
    }

    fn name(&self) -> &'static str {
        "round_robin"
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
}
