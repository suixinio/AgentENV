//! Gates whether a binding-store miss may be reported as not found.
//!
//! Heartbeats repopulate bindings, so a new registry cannot assert absence until every
//! schedulable discovered node reports or the warm-up deadline passes. The deadline may
//! open the gate only after at least one report, preventing an unstarted registry from
//! declaring every runtime gone.
//!
//! A roster reaches routing only by being absorbed into the binding store, so a node
//! whose roster this replica failed to absorb shuts the gate for as long as it is
//! discovered: its sandboxes have no binding to find, and a miss is not absence.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

use super::registry::NodeRegistry;

pub const DEFAULT_WARMUP_TIMEOUT: Duration = Duration::from_secs(15);

pub struct WarmupGate {
    nodes: Arc<dyn NodeRegistry>,
    /// Rebased when the heartbeat listener becomes reachable.
    deadline: RwLock<SystemTime>,
    /// One-way warm latch; later node joins do not close the gate.
    warm: AtomicBool,
    /// Prevents empty discovery from satisfying readiness vacuously.
    reported: AtomicBool,
    /// Nodes whose latest roster did not reach the binding store. Never latched:
    /// absorption is a live condition, and the next one that lands clears it.
    unabsorbed: RwLock<HashSet<String>>,
}

impl WarmupGate {
    pub fn new(nodes: Arc<dyn NodeRegistry>, timeout: Duration, now: SystemTime) -> Self {
        Self {
            nodes,
            deadline: RwLock::new(Self::effective_deadline(now, timeout)),
            warm: AtomicBool::new(false),
            reported: AtomicBool::new(false),
            unabsorbed: RwLock::new(HashSet::new()),
        }
    }

    fn effective_deadline(now: SystemTime, timeout: Duration) -> SystemTime {
        let timeout = if timeout > Duration::ZERO {
            timeout
        } else {
            DEFAULT_WARMUP_TIMEOUT
        };
        now + timeout
    }

    /// Rebases the deadline from the time the heartbeat listener becomes reachable.
    ///
    /// No effect after the gate has latched warm.
    pub fn rebase_deadline(&self, now: SystemTime, timeout: Duration) {
        let mut deadline = self
            .deadline
            .write()
            .expect("warmup deadline lock poisoned");
        *deadline = Self::effective_deadline(now, timeout);
    }

    /// Records a heartbeat that nothing absorbs a roster from, and immediately
    /// re-evaluates readiness.
    pub fn reported_in(&self, now: SystemTime) {
        self.reported.store(true, Ordering::SeqCst);
        self.warmed_up(now);
    }

    /// Records that `node_id`'s roster reached the binding store.
    pub fn roster_absorbed(&self, node_id: &str, now: SystemTime) {
        self.unabsorbed
            .write()
            .expect("warmup absorption lock poisoned")
            .remove(node_id);
        self.reported_in(now);
    }

    /// Records that `node_id`'s roster did not reach the binding store.
    ///
    /// Its sandboxes have no binding to find, so the gate shuts until a later
    /// roster for that node lands or the node stops being discovered.
    pub fn roster_not_absorbed(&self, node_id: &str) {
        self.unabsorbed
            .write()
            .expect("warmup absorption lock poisoned")
            .insert(node_id.to_string());
    }

    fn holds_an_unabsorbed_roster(&self) -> bool {
        let unabsorbed = self
            .unabsorbed
            .read()
            .expect("warmup absorption lock poisoned");
        if unabsorbed.is_empty() {
            return false;
        }
        self.nodes
            .snapshot(/* allow_lingering */ false)
            .iter()
            .any(|node| unabsorbed.contains(&node.id))
    }

    /// Returns whether a binding miss may be reported as absent.
    ///
    /// A deadline opens only after at least one heartbeat has arrived.
    pub fn warmed_up(&self, now: SystemTime) -> bool {
        if self.holds_an_unabsorbed_roster() {
            return false;
        }

        if self.warm.load(Ordering::SeqCst) {
            return true;
        }

        // Observation, not this process's own heartbeat traffic. Heartbeats pin to
        // one replica, so a gate keyed on having received one leaves every other
        // replica permanently cold, reporting each miss as "still seeding".
        // Lingering nodes may never heartbeat again and do not block warm-up.
        let discovered = self.nodes.snapshot(/* allow_lingering */ false);
        if !discovered.is_empty()
            && discovered
                .iter()
                .all(|node| self.nodes.peek_observed(&node.id).is_some())
        {
            self.warm.store(true, Ordering::SeqCst);
            return true;
        }

        // Only the deadline still needs a report: it must not let a registry that
        // has heard nothing declare every runtime gone.
        if !self.reported.load(Ordering::SeqCst) {
            return false;
        }
        let deadline = *self.deadline.read().expect("warmup deadline lock poisoned");
        if now >= deadline {
            self.warm.store(true, Ordering::SeqCst);
            return true;
        }

        false
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

    /// Records the heartbeat in the registry without telling any gate about it.
    fn observe(registry: &AtomicNodeRegistry, node_id: &str, now: SystemTime) {
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
    }

    fn heartbeat(registry: &AtomicNodeRegistry, gate: &WarmupGate, node_id: &str, now: SystemTime) {
        observe(registry, node_id, now);
        gate.reported_in(now);
    }

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

    #[test]
    fn opens_at_the_deadline_for_a_silent_straggler_once_something_has_reported() {
        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![node("node-a"), node("node-b")],
            Duration::from_secs(30),
        ));
        let start = unix(100);
        let gate = WarmupGate::new(
            registry.clone() as Arc<dyn NodeRegistry>,
            Duration::from_secs(15),
            start,
        );
        heartbeat(&registry, &gate, "node-a", start);

        assert!(
            !gate.warmed_up(start + Duration::from_secs(14)),
            "the gate must stay shut until the deadline, even with node-a already reported"
        );
        assert!(
            gate.warmed_up(start + Duration::from_secs(15)),
            "the gate must open at the deadline even with node-b still silent, now that \
             node-a has reported at least once"
        );
    }

    #[test]
    fn stays_cold_past_the_deadline_when_nothing_has_ever_reported() {
        let registry: Arc<dyn NodeRegistry> = Arc::new(AtomicNodeRegistry::new(
            vec![node("node-a")],
            Duration::from_secs(30),
        ));
        let start = unix(100);
        let gate = WarmupGate::new(registry, Duration::from_secs(15), start);

        assert!(
            !gate.warmed_up(start + Duration::from_secs(14)),
            "the gate must stay shut before the deadline"
        );
        assert!(
            !gate.warmed_up(start + Duration::from_secs(15)),
            "the gate must stay shut at and after the deadline too -- nothing has ever \
             reported, so this is a registry that has not finished starting, not a \
             genuinely-down node"
        );
        assert!(
            !gate.warmed_up(start + Duration::from_secs(1_000_000)),
            "and it must stay shut arbitrarily far past the deadline, for the same reason"
        );
    }

    #[test]
    fn a_replica_the_heartbeats_never_reached_warms_on_what_its_registry_observed() {
        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![node("node-a")],
            Duration::from_secs(30),
        ));
        let now = unix(100);
        let gate = WarmupGate::new(
            registry.clone() as Arc<dyn NodeRegistry>,
            Duration::from_secs(15),
            now,
        );

        // Every discovered node is observed, but no heartbeat RPC landed on this
        // process: the state of every replica the heartbeats did not pin to.
        observe(&registry, "node-a", now);

        assert!(
            gate.warmed_up(now),
            "🔴 a gate that counts only heartbeats delivered to its own process leaves every \
             other replica cold forever, answering each lookup miss with 'still seeding' and \
             disguising a permanent refusal as something worth retrying"
        );
    }

    #[test]
    fn an_unabsorbed_roster_shuts_the_gate_observation_alone_would_have_opened() {
        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![node("node-a")],
            Duration::from_secs(30),
        ));
        let now = unix(100);
        let gate = WarmupGate::new(
            registry.clone() as Arc<dyn NodeRegistry>,
            Duration::from_secs(15),
            now,
        );

        observe(&registry, "node-a", now);
        gate.roster_not_absorbed("node-a");
        assert!(
            !gate.warmed_up(now + Duration::from_secs(1_000_000)),
            "the registry observed node-a, but its sandboxes never reached the binding \
             store, so a miss is not absence -- not now and not past the deadline"
        );

        gate.roster_absorbed("node-a", now);
        assert!(
            gate.warmed_up(now),
            "the roster landed; the gate has nothing left to wait for"
        );
    }

    #[test]
    fn a_node_that_left_discovery_stops_holding_the_gate_shut() {
        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![node("node-a"), node("node-b")],
            Duration::from_secs(30),
        ));
        let now = unix(100);
        let gate = WarmupGate::new(
            registry.clone() as Arc<dyn NodeRegistry>,
            Duration::from_secs(15),
            now,
        );
        observe(&registry, "node-a", now);
        observe(&registry, "node-b", now);
        gate.roster_absorbed("node-a", now);
        gate.roster_not_absorbed("node-b");
        assert!(!gate.warmed_up(now), "node-b's roster is unabsorbed");

        registry.set(vec![node("node-a")], Vec::new(), now);

        assert!(
            gate.warmed_up(now),
            "an undiscovered node has no sandboxes this cluster can be asked about"
        );
    }

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

        registry.set(vec![node("node-a"), node("node-b")], Vec::new(), now);

        assert!(
            gate.warmed_up(now),
            "a newly discovered node must not reopen warm-up"
        );
    }

    #[test]
    fn rebase_deadline_moves_when_the_deadline_falls() {
        // One node reports while another remains silent so only the deadline can open.
        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![node("node-a"), node("node-b")],
            Duration::from_secs(30),
        ));
        // Start with a deadline already consumed before listener bind.
        let constructed_at = unix(0);
        let gate = WarmupGate::new(
            registry.clone() as Arc<dyn NodeRegistry>,
            Duration::from_secs(15),
            constructed_at,
        );
        heartbeat(&registry, &gate, "node-a", constructed_at);
        assert!(
            gate.warmed_up(constructed_at + Duration::from_secs(15)),
            "sanity: without a rebase the original deadline would already have passed"
        );

        let bound_at = unix(1_000);
        let (registry, gate) = {
            let registry = Arc::new(AtomicNodeRegistry::new(
                vec![node("node-a"), node("node-b")],
                Duration::from_secs(30),
            ));
            let gate = WarmupGate::new(
                registry.clone() as Arc<dyn NodeRegistry>,
                Duration::from_secs(15),
                constructed_at,
            );
            (registry, gate)
        };
        heartbeat(&registry, &gate, "node-a", constructed_at);
        gate.rebase_deadline(bound_at, Duration::from_secs(15));

        assert!(
            !gate.warmed_up(bound_at + Duration::from_secs(14)),
            "the rebased deadline must still hold the gate shut this close to it"
        );
        assert!(
            gate.warmed_up(bound_at + Duration::from_secs(15)),
            "the rebased deadline, not the original construction-time one, must be what opens \
             the gate"
        );
    }
}
