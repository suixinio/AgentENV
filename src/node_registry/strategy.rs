//! Port of `services/scheduler/internal/strategy.go` (60 lines) — the
//! round-robin placement strategy `Schedule` uses to pick a node.
//!
//! Go had a `Strategy` interface with a `NewStrategy` factory choosing
//! between implementations. This port only ever grew one implementation,
//! and nothing ever selected between them: no config knob, no factory, no
//! second type. So the trait was a vtable in front of exactly one type and
//! is gone; [`RoundRobinStrategy`] is used concretely. Adding a second
//! strategy means reintroducing the trait *and* the knob that picks — the
//! metric label below is already shaped for that day.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::node_registry::types::RichNode;
use crate::proto::scheduler::ScheduleRequestHint;

/// Mirrors Go's `ErrNoNodes` — the empty-candidate-list outcome
/// [`RoundRobinStrategy::select`] reports.
///
/// 🔴 The `Display` text is a wire contract, not a log line:
/// `NodeRegistryGrpcService::schedule` turns it into
/// `Status::unavailable("no nodes available")`, and `lookup_node` into
/// `LookupOutcome::Unavailable(LookupResultLabel::UnavailableNoNodes,
/// "no nodes available")`. Both strings are what a caller matches on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("no nodes available")]
pub struct NoNodesAvailable;

/// The `strategy` label value on `agentenv_api_schedule_duration_seconds`
/// and `agentenv_api_schedule_assignments_total`.
///
/// 🔴 Still emitted, and still label-valued, even though round-robin is
/// the only strategy this build has: dropping the label — or changing the
/// value — silently rewrites the identity of a series dashboards and
/// recording rules already group by. Pinned by
/// `grpc_service.rs`'s `schedule_metrics_are_named_and_labelled_exactly`.
pub const ROUND_ROBIN_NAME: &str = "round_robin";

/// Cycles through `nodes` in order, advancing one slot per call regardless of
/// which strategy instance is asked — state lives in `next`, not in the
/// argument list.
///
/// 🔴 That makes the *instance* the scheduling state. `Schedule` and
/// `LookupNode`'s `Paused` branch must share one, or each call path gets
/// its own cursor and placement stops rotating across them while every
/// round-robin test still passes. `NodeRegistryGrpcService` holds a single
/// `Arc<RoundRobinStrategy>` for exactly that reason.
#[derive(Debug, Default)]
pub struct RoundRobinStrategy {
    next: AtomicU64,
}

impl RoundRobinStrategy {
    pub fn new() -> Self {
        Self::default()
    }

    /// Picks one node from `nodes`. `hint` carries the same
    /// `ScheduleRequestHint` `Schedule` received, for a strategy that wants
    /// to weigh it — round-robin does not read it, matching Go.
    pub fn select(
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

    /// The `strategy` metric label value — see [`ROUND_ROBIN_NAME`].
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

    /// 🔴 The metric label value is a series identity, so it is pinned
    /// here as well as at the emission site — a rename here alone would
    /// otherwise be invisible until a dashboard went blank.
    #[test]
    fn the_strategy_metric_label_value_is_round_robin() {
        assert_eq!(ROUND_ROBIN_NAME, "round_robin");
        assert_eq!(RoundRobinStrategy::new().name(), "round_robin");
    }
}
