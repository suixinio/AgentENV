//! Port of `services/scheduler/internal/filter.go` (118 lines) — narrowing a
//! candidate node list down to what may actually be scheduled onto.
//!
//! Two independent filters, both pure: [`filter_unschedulable`] drops nodes
//! that self-reported they are not taking new work (`DRAINING`), and
//! [`filter_by_resource_limit`] drops nodes that exceed an operator-configured
//! resource ceiling. Neither one is wired into a scheduling path yet — see
//! `src/node_registry/mod.rs`.
//!
//! [`NodeResourceLimit`] is a Stage A-local stand-in for
//! `services/shared/config.NodeResourceLimit`. It is not registered on
//! [`crate::cfg::AppConfig`] — that wiring is deliberately deferred to the
//! step that actually gives a Rust consumer a reason to read it, so this
//! module can be tested in isolation first (per the construction plan's
//! step 1-2 "pure functions first, no consumers yet").

use crate::node_registry::types::RichNode;
use crate::proto::scheduler::NodeStatus;

/// Per-node resource thresholds for scheduling eligibility. A node exceeding
/// any configured (`Some`) limit is excluded from scheduling candidates.
/// `None` fields impose no limit.
///
/// Allocated-percent limits (CPU and memory) can legitimately exceed 100%
/// because allocated resources reflect the sum of all sandbox reservations,
/// which may overcommit the physical capacity of the node.
#[derive(Debug, Clone, Default)]
pub struct NodeResourceLimit {
    pub max_sandbox_count: Option<u32>,
    pub max_sandbox_starting_count: Option<u32>,
    pub max_cpu_used_percent: Option<u32>,
    /// Can exceed 100 (overcommit).
    pub max_cpu_allocated_percent: Option<u32>,
    pub max_memory_used_percent: Option<u32>,
    /// Can exceed 100 (overcommit).
    pub max_memory_allocated_percent: Option<u32>,

    /// Limits that apply to the sum of the active running set plus paused
    /// sandboxes. Paused sandboxes have released their VM-side CPU / memory
    /// but still occupy persisted state on the node, so operators may want a
    /// separate ceiling on total node footprint (including paused) on top of
    /// the active-only ceilings above.
    pub max_sandbox_count_including_paused: Option<u32>,
    pub max_allocated_cpu_including_paused: Option<u32>,
    pub max_allocated_memory_bytes_including_paused: Option<u64>,
}

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

/// Removes nodes that exceed any configured resource threshold. Nodes
/// without a heartbeat snapshot are always kept (they have no metrics to
/// evaluate). `None` disables all filtering.
pub fn filter_by_resource_limit(
    nodes: Vec<RichNode>,
    limit: Option<&NodeResourceLimit>,
) -> Vec<RichNode> {
    let Some(limit) = limit else {
        return nodes;
    };

    nodes
        .into_iter()
        .filter(|n| match &n.snapshot {
            // No heartbeat yet — cannot evaluate limits; keep the node.
            None => true,
            Some(snapshot) => within_limit(snapshot, limit),
        })
        .collect()
}

fn within_limit(s: &crate::proto::scheduler::NodeSnapshot, limit: &NodeResourceLimit) -> bool {
    if let Some(max) = limit.max_sandbox_count {
        if s.sandbox_count > max {
            return false;
        }
    }
    if let Some(max) = limit.max_sandbox_starting_count {
        if s.sandbox_starting_count > max {
            return false;
        }
    }
    if let Some(max) = limit.max_cpu_used_percent {
        if s.cpu_percent > max {
            return false;
        }
    }
    if let Some(max) = limit.max_cpu_allocated_percent {
        if let Some(allocated_percent) = (s.allocated_cpu * 100).checked_div(s.cpu_count) {
            if allocated_percent > max {
                return false;
            }
        }
    }
    if let Some(max) = limit.max_memory_used_percent {
        if let Some(used_percent) = (s.memory_used_bytes * 100).checked_div(s.memory_total_bytes) {
            if used_percent as u32 > max {
                return false;
            }
        }
    }
    if let Some(max) = limit.max_memory_allocated_percent {
        if let Some(allocated_percent) =
            (s.allocated_memory_bytes * 100).checked_div(s.memory_total_bytes)
        {
            if allocated_percent as u32 > max {
                return false;
            }
        }
    }

    // "Including paused" ceilings sum the active running set with the paused
    // reservations reported in the snapshot. A node exceeding any of these is
    // dropped from scheduling candidates regardless of whether the
    // active-only counters are within limits.
    if let Some(max) = limit.max_sandbox_count_including_paused {
        let total = s.sandbox_count + s.paused_sandbox_count;
        if total > max {
            return false;
        }
    }
    if let Some(max) = limit.max_allocated_cpu_including_paused {
        let total = s.allocated_cpu + s.paused_allocated_cpu;
        if total > max {
            return false;
        }
    }
    if let Some(max) = limit.max_allocated_memory_bytes_including_paused {
        let total = s.allocated_memory_bytes + s.paused_allocated_memory_bytes;
        if total > max {
            return false;
        }
    }
    true
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
    fn nil_limit_keeps_all() {
        let nodes = vec![no_snapshot("a"), no_snapshot("b")];
        let result = filter_by_resource_limit(nodes, None);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn keeps_nodes_without_snapshot() {
        let limit = NodeResourceLimit {
            max_sandbox_count: Some(5),
            ..Default::default()
        };
        let nodes = vec![no_snapshot("no-heartbeat")];
        let result = filter_by_resource_limit(nodes, Some(&limit));
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn max_sandbox_count() {
        let limit = NodeResourceLimit {
            max_sandbox_count: Some(10),
            ..Default::default()
        };
        let nodes = vec![
            with_snapshot(
                "ok",
                NodeSnapshot {
                    sandbox_count: 5,
                    ..Default::default()
                },
            ),
            with_snapshot(
                "at-limit",
                NodeSnapshot {
                    sandbox_count: 10,
                    ..Default::default()
                },
            ),
            with_snapshot(
                "over",
                NodeSnapshot {
                    sandbox_count: 11,
                    ..Default::default()
                },
            ),
        ];
        let result = filter_by_resource_limit(nodes, Some(&limit));
        assert_eq!(ids(&result), vec!["ok", "at-limit"]);
    }

    #[test]
    fn max_sandbox_starting_count() {
        let limit = NodeResourceLimit {
            max_sandbox_starting_count: Some(2),
            ..Default::default()
        };
        let nodes = vec![
            with_snapshot(
                "ok",
                NodeSnapshot {
                    sandbox_starting_count: 1,
                    ..Default::default()
                },
            ),
            with_snapshot(
                "over",
                NodeSnapshot {
                    sandbox_starting_count: 3,
                    ..Default::default()
                },
            ),
        ];
        let result = filter_by_resource_limit(nodes, Some(&limit));
        assert_eq!(ids(&result), vec!["ok"]);
    }

    #[test]
    fn max_cpu_used_percent() {
        let limit = NodeResourceLimit {
            max_cpu_used_percent: Some(80),
            ..Default::default()
        };
        let nodes = vec![
            with_snapshot(
                "ok",
                NodeSnapshot {
                    cpu_percent: 50,
                    ..Default::default()
                },
            ),
            with_snapshot(
                "over",
                NodeSnapshot {
                    cpu_percent: 95,
                    ..Default::default()
                },
            ),
        ];
        let result = filter_by_resource_limit(nodes, Some(&limit));
        assert_eq!(ids(&result), vec!["ok"]);
    }

    #[test]
    fn max_cpu_allocated_percent() {
        let limit = NodeResourceLimit {
            max_cpu_allocated_percent: Some(50),
            ..Default::default()
        };
        let nodes = vec![
            with_snapshot(
                "ok",
                NodeSnapshot {
                    allocated_cpu: 2,
                    cpu_count: 8,
                    ..Default::default()
                },
            ), // 25%
            with_snapshot(
                "over",
                NodeSnapshot {
                    allocated_cpu: 6,
                    cpu_count: 8,
                    ..Default::default()
                },
            ), // 75%
            with_snapshot(
                "zero-cpu",
                NodeSnapshot {
                    allocated_cpu: 5,
                    cpu_count: 0,
                    ..Default::default()
                },
            ), // skip check
        ];
        let result = filter_by_resource_limit(nodes, Some(&limit));
        assert_eq!(ids(&result), vec!["ok", "zero-cpu"]);
    }

    #[test]
    fn max_memory_used_percent() {
        let limit = NodeResourceLimit {
            max_memory_used_percent: Some(70),
            ..Default::default()
        };
        let nodes = vec![
            with_snapshot(
                "ok",
                NodeSnapshot {
                    memory_used_bytes: 60,
                    memory_total_bytes: 100,
                    ..Default::default()
                },
            ), // 60%
            with_snapshot(
                "over",
                NodeSnapshot {
                    memory_used_bytes: 80,
                    memory_total_bytes: 100,
                    ..Default::default()
                },
            ), // 80%
        ];
        let result = filter_by_resource_limit(nodes, Some(&limit));
        assert_eq!(ids(&result), vec!["ok"]);
    }

    #[test]
    fn max_memory_allocated_percent() {
        let limit = NodeResourceLimit {
            max_memory_allocated_percent: Some(50),
            ..Default::default()
        };
        let nodes = vec![
            with_snapshot(
                "ok",
                NodeSnapshot {
                    allocated_memory_bytes: 40,
                    memory_total_bytes: 100,
                    ..Default::default()
                },
            ), // 40%
            with_snapshot(
                "over",
                NodeSnapshot {
                    allocated_memory_bytes: 60,
                    memory_total_bytes: 100,
                    ..Default::default()
                },
            ), // 60%
        ];
        let result = filter_by_resource_limit(nodes, Some(&limit));
        assert_eq!(ids(&result), vec!["ok"]);
    }

    #[test]
    fn multiple_limits() {
        let limit = NodeResourceLimit {
            max_sandbox_count: Some(10),
            max_cpu_used_percent: Some(80),
            ..Default::default()
        };
        let nodes = vec![
            with_snapshot(
                "both-ok",
                NodeSnapshot {
                    sandbox_count: 5,
                    cpu_percent: 50,
                    ..Default::default()
                },
            ),
            with_snapshot(
                "sandbox-over",
                NodeSnapshot {
                    sandbox_count: 15,
                    cpu_percent: 50,
                    ..Default::default()
                },
            ),
            with_snapshot(
                "cpu-over",
                NodeSnapshot {
                    sandbox_count: 5,
                    cpu_percent: 90,
                    ..Default::default()
                },
            ),
            with_snapshot(
                "both-over",
                NodeSnapshot {
                    sandbox_count: 15,
                    cpu_percent: 90,
                    ..Default::default()
                },
            ),
        ];
        let result = filter_by_resource_limit(nodes, Some(&limit));
        assert_eq!(ids(&result), vec!["both-ok"]);
    }

    #[test]
    fn all_excluded_returns_empty() {
        let limit = NodeResourceLimit {
            max_sandbox_count: Some(0),
            ..Default::default()
        };
        let nodes = vec![with_snapshot(
            "a",
            NodeSnapshot {
                sandbox_count: 1,
                ..Default::default()
            },
        )];
        let result = filter_by_resource_limit(nodes, Some(&limit));
        assert!(result.is_empty());
    }

    #[test]
    fn max_sandbox_count_including_paused() {
        let limit = NodeResourceLimit {
            max_sandbox_count_including_paused: Some(5),
            ..Default::default()
        };
        let nodes = vec![
            // 2 running + 3 paused = 5, at limit, kept.
            with_snapshot(
                "at-limit",
                NodeSnapshot {
                    sandbox_count: 2,
                    paused_sandbox_count: 3,
                    ..Default::default()
                },
            ),
            // 3 running + 3 paused = 6, over.
            with_snapshot(
                "over",
                NodeSnapshot {
                    sandbox_count: 3,
                    paused_sandbox_count: 3,
                    ..Default::default()
                },
            ),
            // 0 running + 0 paused, kept.
            with_snapshot("empty", NodeSnapshot::default()),
        ];
        let result = filter_by_resource_limit(nodes, Some(&limit));
        assert_eq!(ids(&result), vec!["at-limit", "empty"]);
    }

    #[test]
    fn max_allocated_cpu_including_paused() {
        let limit = NodeResourceLimit {
            max_allocated_cpu_including_paused: Some(8),
            ..Default::default()
        };
        let nodes = vec![
            // 4 active + 4 paused = 8, at limit.
            with_snapshot(
                "at-limit",
                NodeSnapshot {
                    allocated_cpu: 4,
                    paused_allocated_cpu: 4,
                    ..Default::default()
                },
            ),
            // 4 active + 5 paused = 9, over.
            with_snapshot(
                "over",
                NodeSnapshot {
                    allocated_cpu: 4,
                    paused_allocated_cpu: 5,
                    ..Default::default()
                },
            ),
        ];
        let result = filter_by_resource_limit(nodes, Some(&limit));
        assert_eq!(ids(&result), vec!["at-limit"]);
    }

    #[test]
    fn max_allocated_memory_bytes_including_paused() {
        let limit = NodeResourceLimit {
            max_allocated_memory_bytes_including_paused: Some(1000),
            ..Default::default()
        };
        let nodes = vec![
            // 400 + 600 = 1000, at limit.
            with_snapshot(
                "at-limit",
                NodeSnapshot {
                    allocated_memory_bytes: 400,
                    paused_allocated_memory_bytes: 600,
                    ..Default::default()
                },
            ),
            // 500 + 600 = 1100, over.
            with_snapshot(
                "over",
                NodeSnapshot {
                    allocated_memory_bytes: 500,
                    paused_allocated_memory_bytes: 600,
                    ..Default::default()
                },
            ),
            // 0 + 0 = 0, kept.
            with_snapshot("empty", NodeSnapshot::default()),
        ];
        let result = filter_by_resource_limit(nodes, Some(&limit));
        assert_eq!(ids(&result), vec!["at-limit", "empty"]);
    }

    /// A node that fits active limits but exceeds an "including paused"
    /// limit must still be excluded.
    #[test]
    fn including_paused_excludes_node_within_active_limits() {
        let limit = NodeResourceLimit {
            max_sandbox_count: Some(10),
            max_sandbox_count_including_paused: Some(5),
            ..Default::default()
        };
        let nodes = vec![
            // Within active ceiling (2 <= 10) but over including-paused
            // (2 + 4 = 6 > 5).
            with_snapshot(
                "over-paused",
                NodeSnapshot {
                    sandbox_count: 2,
                    paused_sandbox_count: 4,
                    ..Default::default()
                },
            ),
            // Within both.
            with_snapshot(
                "ok",
                NodeSnapshot {
                    sandbox_count: 2,
                    paused_sandbox_count: 2,
                    ..Default::default()
                },
            ),
        ];
        let result = filter_by_resource_limit(nodes, Some(&limit));
        assert_eq!(ids(&result), vec!["ok"]);
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

    // 🔴 Regression guard: an "including paused" ceiling must be evaluated
    // independently of — not merged into — its active-only counterpart. A
    // simplification that summed max_sandbox_count and
    // max_sandbox_count_including_paused into one comparison, or dropped one
    // of the two checks because "the paused one is stricter", would pass
    // every test above by coincidence of fixture values but silently stop
    // enforcing whichever ceiling got dropped.
    #[test]
    fn active_and_including_paused_ceilings_are_independent() {
        let limit = NodeResourceLimit {
            // Deliberately looser than the "including paused" ceiling below,
            // so a node can violate max_sandbox_count alone while satisfying
            // max_sandbox_count_including_paused, and vice versa.
            max_sandbox_count: Some(3),
            max_sandbox_count_including_paused: Some(100),
            ..Default::default()
        };
        // Violates the active-only ceiling (5 > 3) while comfortably inside
        // the including-paused one (5 + 0 = 5 <= 100).
        let nodes = vec![with_snapshot(
            "active-over",
            NodeSnapshot {
                sandbox_count: 5,
                paused_sandbox_count: 0,
                ..Default::default()
            },
        )];
        let result = filter_by_resource_limit(nodes, Some(&limit));
        assert!(
            result.is_empty(),
            "the active-only ceiling must still be enforced even though the \
             including-paused ceiling is satisfied"
        );
    }
}
