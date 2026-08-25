//! Port of `services/scheduler/internal/warmup.go` (100 lines) — the gate
//! that decides whether a binding-store miss may honestly be answered as
//! "not found."
//!
//! 🔴 The consumer of this gate is Stage D, not Stage A. Go's `warmupGate` is
//! read by exactly one caller, `lookupNode` (`service.go`), which also reads
//! the `BindingStore` — a Stage D type this codebase has not ported yet. So
//! [`WarmupGate`] here is data-structure-complete and unit-tested against the
//! [`super::registry::NodeRegistry`] trait alone, but — matching the
//! construction plan's explicit call-out for this file — it has no consumer
//! in this build and will not until Stage D exists to read it. That is
//! expected, not a gap: `warmup_test.go`'s `TestLookupWithholdsNotFoundWhileCold`,
//! `TestLookupReportsNotFoundOnceWarm`, and `TestQueryOnlyLookupIsNotGated`
//! all drive a `Service`/`LookupNode` this build has no equivalent of yet, and
//! are not ported here for the same reason — only the four tests that
//! exercise `warmupGate` directly are.
//!
//! Bindings are a cache: held in memory, expired on a TTL, and re-seeded
//! entirely from node heartbeats. A registry that has just started therefore
//! knows nothing — and answering "not found" from that state is not a cache
//! miss, it is the cache asserting something it cannot know. This gate
//! withholds that assertion until either every node discovery currently
//! knows about has reported at least one heartbeat, or a deadline has
//! passed (a node that is genuinely down never reports, and waiting for it
//! forever would turn every legitimate "not found" into a permanent
//! "unavailable").

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use super::registry::NodeRegistry;

/// Mirrors Go's `defaultWarmupTimeout`.
pub const DEFAULT_WARMUP_TIMEOUT: Duration = Duration::from_secs(15);

pub struct WarmupGate {
    nodes: Arc<dyn NodeRegistry>,
    deadline: SystemTime,
    /// Latched once warm: nodes come and go afterwards, and a node joining
    /// an hour later must not put the gate back into warm-up.
    warm: AtomicBool,
    /// Whether any node has reported at all. Without it, "every known node
    /// has reported" passes vacuously whenever the node list is empty —
    /// which is true both before discovery has run (the coldest state there
    /// is) and after the last node unregisters (long since warm).
    reported: AtomicBool,
}

impl WarmupGate {
    pub fn new(nodes: Arc<dyn NodeRegistry>, timeout: Duration, now: SystemTime) -> Self {
        let timeout = if timeout > Duration::ZERO {
            timeout
        } else {
            DEFAULT_WARMUP_TIMEOUT
        };
        Self {
            nodes,
            deadline: now + timeout,
            warm: AtomicBool::new(false),
            reported: AtomicBool::new(false),
        }
    }

    /// Records that a node has delivered a heartbeat, and with it the
    /// roster of sandboxes that node holds.
    ///
    /// Re-evaluates the gate rather than only setting a flag, because the
    /// moment the last node reports is the moment warm-up is over — waiting
    /// for a lookup to notice would leave the gate shut through a node
    /// unregistering or dropping out of discovery, either of which puts a
    /// never-reported node back in the list and reads as cold all over
    /// again.
    pub fn reported_in(&self, now: SystemTime) {
        self.reported.store(true, Ordering::SeqCst);
        self.warmed_up(now);
    }

    /// Reports whether a binding miss may be answered as "not found."
    pub fn warmed_up(&self, now: SystemTime) -> bool {
        if self.warm.load(Ordering::SeqCst) {
            return true;
        }
        if now >= self.deadline {
            self.warm.store(true, Ordering::SeqCst);
            return true;
        }
        if !self.reported.load(Ordering::SeqCst) {
            return false;
        }

        // Lingering nodes are excluded deliberately: they are on their way
        // out of discovery, and waiting for a heartbeat that may never come
        // would hold the gate shut for the whole deadline.
        for node in self.nodes.snapshot(/* allow_lingering */ false) {
            if self.nodes.peek_observed(&node.id).is_none() {
                return false;
            }
        }

        self.warm.store(true, Ordering::SeqCst);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node_registry::registry::AtomicNodeRegistry;
    use crate::node_registry::types::Node;
    use crate::proto::scheduler::{HeartbeatRequest, NodeSnapshot, NodeStatus};

    fn unix(secs: u64) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(secs)
    }

    fn node(id: &str) -> Node {
        Node {
            id: id.to_string(),
            endpoint: format!("http://{id}"),
            pod_name: String::new(),
        }
    }

    fn heartbeat(registry: &AtomicNodeRegistry, gate: &WarmupGate, node_id: &str, now: SystemTime) {
        registry
            .heartbeat(
                &HeartbeatRequest {
                    node_id: node_id.to_string(),
                    cluster_id: "cluster-a".to_string(),
                    service_instance_id: format!("svc-{node_id}"),
                    snapshot: Some(NodeSnapshot {
                        status: NodeStatus::Ready as i32,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                now,
            )
            .expect("heartbeat");
        gate.reported_in(now);
    }

    // Discovery has not produced a node list yet, so "every known node
    // reported" would be vacuously true. That is exactly the coldest moment
    // there is.
    #[test]
    fn cold_before_discovery() {
        let registry: Arc<dyn NodeRegistry> =
            Arc::new(AtomicNodeRegistry::new(Vec::new(), Duration::from_secs(30)));
        let now = unix(100);
        let gate = WarmupGate::new(registry, Duration::from_secs(15), now);

        assert!(
            !gate.warmed_up(now),
            "a registry that has discovered no nodes must not be warm"
        );
    }

    #[test]
    fn stays_cold_until_every_known_node_reports() {
        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![node("node-a"), node("node-b")],
            Duration::from_secs(30),
        ));
        let now = unix(100);
        let gate = WarmupGate::new(registry.clone(), Duration::from_secs(15), now);

        heartbeat(&registry, &gate, "node-a", now);
        assert!(!gate.warmed_up(now), "node-b has not reported yet");

        heartbeat(&registry, &gate, "node-b", now);
        assert!(
            gate.warmed_up(now),
            "every known node has reported; the gate must open"
        );
    }

    // A node that is genuinely down never reports. Waiting for it forever
    // would turn every legitimate "not found" into "unavailable" for the
    // life of the process.
    #[test]
    fn opens_at_the_deadline() {
        let registry: Arc<dyn NodeRegistry> = Arc::new(AtomicNodeRegistry::new(
            vec![node("node-a")],
            Duration::from_secs(30),
        ));
        let start = unix(100);
        let gate = WarmupGate::new(registry, Duration::from_secs(15), start);

        assert!(
            !gate.warmed_up(start + Duration::from_secs(14)),
            "the gate must stay shut until the deadline"
        );
        assert!(
            gate.warmed_up(start + Duration::from_secs(15)),
            "the gate must open at the deadline even with a silent node"
        );
    }

    // Nodes join and leave for the life of the cluster. A node discovered an
    // hour in must not put the registry back into warm-up and start
    // withholding answers about sandboxes it already knows.
    #[test]
    fn stays_open_when_a_new_node_appears() {
        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![node("node-a")],
            Duration::from_secs(30),
        ));
        let now = unix(100);
        let gate = WarmupGate::new(registry.clone(), Duration::from_secs(15), now);
        heartbeat(&registry, &gate, "node-a", now);
        assert!(gate.warmed_up(now), "expected the gate to open");

        registry.set(vec![node("node-a"), node("node-b")], Vec::new());

        assert!(
            gate.warmed_up(now),
            "a newly discovered node must not reopen warm-up"
        );
    }
}
