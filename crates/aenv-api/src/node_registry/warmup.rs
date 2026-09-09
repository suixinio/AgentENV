//! Gates whether a binding-store miss may be reported as not found.
//!
//! Heartbeats repopulate bindings, so a new registry cannot assert absence until every
//! schedulable discovered node reports or the warm-up deadline passes. The deadline may
//! open the gate only after at least one report, preventing an unstarted registry from
//! declaring every runtime gone.
//!
//! A roster reaches routing only by being absorbed into the binding store, so a node
//! whose roster this replica failed to absorb shuts the gate while that node is still
//! discovered and still reporting: its sandboxes have no binding to find, and a miss
//! is not absence. The veto expires with the node's own liveness, so a node that stops
//! heartbeating withholds neither absence nor a placement verdict of `NodeMembership::Gone`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

use super::placement::score::SnapshotFreshness;
use super::registry::NodeRegistry;

pub const DEFAULT_WARMUP_TIMEOUT: Duration = Duration::from_secs(15);

/// Decides whether a binding-store miss may be reported as absence.
///
/// The unabsorbed-roster veto is process-local while observation is shared through the
/// node-registry store: a replica that never receives node N's heartbeats records no
/// veto for N and answers a miss on N's sandboxes as absence.
pub struct WarmupGate {
    nodes: Arc<dyn NodeRegistry>,
    /// Rebased when the heartbeat listener becomes reachable.
    deadline: RwLock<SystemTime>,
    /// The warm-up window, which is also how long one absorption failure vetoes for.
    window: RwLock<Duration>,
    /// One-way warm latch; later node joins do not close the gate.
    warm: AtomicBool,
    /// Prevents empty discovery from satisfying readiness vacuously.
    reported: AtomicBool,
    /// Absorption failures by node, stamped with the heartbeat that recorded them.
    /// Never latched: absorption is a live condition, and the next roster that lands,
    /// the window, or the node's own silence ends it.
    unabsorbed: RwLock<HashMap<String, SystemTime>>,
}

impl WarmupGate {
    pub fn new(nodes: Arc<dyn NodeRegistry>, timeout: Duration, now: SystemTime) -> Self {
        let window = Self::effective_window(timeout);
        Self {
            nodes,
            deadline: RwLock::new(now + window),
            window: RwLock::new(window),
            warm: AtomicBool::new(false),
            reported: AtomicBool::new(false),
            unabsorbed: RwLock::new(HashMap::new()),
        }
    }

    fn effective_window(timeout: Duration) -> Duration {
        if timeout > Duration::ZERO {
            timeout
        } else {
            DEFAULT_WARMUP_TIMEOUT
        }
    }

    /// Rebases the deadline, and the veto window with it, from listener readiness.
    pub fn rebase_deadline(&self, now: SystemTime, timeout: Duration) {
        let window = Self::effective_window(timeout);
        *self
            .deadline
            .write()
            .expect("warmup deadline lock poisoned") = now + window;
        *self.window.write().expect("warmup window lock poisoned") = window;
    }

    /// Records a heartbeat that nothing absorbs a roster from, and immediately
    /// re-evaluates readiness.
    pub fn reported_in(&self, now: SystemTime) {
        self.reported.store(true, Ordering::SeqCst);
        self.warmed_up(now);
    }

    /// Records that `node_id`'s roster reached the binding store.
    pub fn roster_absorbed(&self, node_id: &str, now: SystemTime) {
        let (deadline, window) = self.veto_bounds();
        {
            let mut unabsorbed = self
                .unabsorbed
                .write()
                .expect("warmup absorption lock poisoned");
            unabsorbed.remove(node_id);
            unabsorbed.retain(|_, at| Self::veto_in_force(*at, deadline, window, now));
        }
        self.reported_in(now);
    }

    /// Records that `node_id`'s roster did not reach the binding store, stamped with
    /// when the absorption failed rather than when its heartbeat arrived.
    ///
    /// Its sandboxes have no binding to find, so the gate shuts until a later roster
    /// for that node lands, the node stops being discovered or reporting, or this
    /// stamp falls outside the veto window.
    pub fn roster_not_absorbed(&self, node_id: &str, failed_at: SystemTime) {
        let (deadline, window) = self.veto_bounds();
        let mut unabsorbed = self
            .unabsorbed
            .write()
            .expect("warmup absorption lock poisoned");
        unabsorbed.retain(|_, at| Self::veto_in_force(*at, deadline, window, failed_at));
        unabsorbed.insert(node_id.to_string(), failed_at);
    }

    fn veto_bounds(&self) -> (SystemTime, Duration) {
        let deadline = *self.deadline.read().expect("warmup deadline lock poisoned");
        let window = *self.window.read().expect("warmup window lock poisoned");
        (deadline, window)
    }

    /// A stamp holds for one window, renewed by every failing heartbeat, and never
    /// expires before warm-up itself could have opened the gate.
    fn veto_in_force(
        recorded_at: SystemTime,
        deadline: SystemTime,
        window: Duration,
        now: SystemTime,
    ) -> bool {
        now < recorded_at.max(deadline) + window
    }

    /// A node past its own report TTL holds nothing: this replica can no longer tell
    /// its sandboxes from those of any other node it cannot reach.
    fn node_still_reporting(&self, node_id: &str, now: SystemTime) -> bool {
        match self.nodes.peek_observed_with_freshness(node_id, now) {
            // A receive time ahead of `now` is a newer report, not a missing one.
            Some((_, SnapshotFreshness::Fresh | SnapshotFreshness::ClockSkew)) => true,
            Some((_, SnapshotFreshness::Stale)) | None => false,
        }
    }

    fn holds_an_unabsorbed_roster(&self, now: SystemTime) -> bool {
        let (deadline, window) = self.veto_bounds();
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
            .any(|node| {
                unabsorbed
                    .get(&node.id)
                    .is_some_and(|at| Self::veto_in_force(*at, deadline, window, now))
                    && self.node_still_reporting(&node.id, now)
            })
    }

    /// Returns whether a binding miss may be reported as absent.
    ///
    /// A deadline opens only after at least one heartbeat has arrived.
    pub fn warmed_up(&self, now: SystemTime) -> bool {
        if self.holds_an_unabsorbed_roster(now) {
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
        gate.roster_not_absorbed("node-a", now);
        assert!(
            !gate.warmed_up(now),
            "the registry observed node-a, but its sandboxes never reached the binding \
             store, so a miss is not absence"
        );
        assert!(
            !gate.warmed_up(now + Duration::from_secs(20)),
            "and not past the warm-up deadline either, while node-a is still reporting"
        );

        gate.roster_absorbed("node-a", now);
        assert!(
            gate.warmed_up(now),
            "the roster landed; the gate has nothing left to wait for"
        );
    }

    #[test]
    fn a_veto_stops_holding_the_gate_once_the_node_stops_reporting() {
        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![node("node-a")],
            Duration::from_secs(30),
        ));
        let now = unix(100);
        // A window far longer than the report TTL, so only liveness can end this veto.
        let gate = WarmupGate::new(
            registry.clone() as Arc<dyn NodeRegistry>,
            Duration::from_secs(300),
            now,
        );

        observe(&registry, "node-a", now);
        gate.roster_not_absorbed("node-a", now);
        assert!(!gate.warmed_up(now), "node-a's roster is unabsorbed");

        assert!(
            gate.warmed_up(now + Duration::from_secs(31)),
            "node-a has not heartbeated within its report TTL: this replica can no longer \
             claim its sandboxes are real, and a permanent refusal is worse than absence"
        );
    }

    #[test]
    fn a_veto_expires_on_the_warmup_window_even_while_a_shared_observation_stays_fresh() {
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
        gate.roster_not_absorbed("node-a", now);

        // A heartbeat this process never saw, arriving through the shared store.
        let later = now + Duration::from_secs(31);
        observe(&registry, "node-a", later);

        assert!(
            gate.warmed_up(later),
            "node-a looks fresh, but no failure has renewed the veto for a window: a veto \
             this replica cannot refresh must not outlive one"
        );
    }

    #[test]
    fn a_fresh_failure_renews_the_veto_past_the_deadline() {
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
        gate.roster_not_absorbed("node-a", now);

        let renewed_at = now + Duration::from_secs(29);
        observe(&registry, "node-a", renewed_at);
        gate.roster_not_absorbed("node-a", renewed_at);

        assert!(
            !gate.warmed_up(renewed_at + Duration::from_secs(14)),
            "node-a is still reporting and its roster still does not land, so the gate stays \
             shut a full window past the renewal"
        );
        assert!(
            gate.warmed_up(renewed_at + Duration::from_secs(15)),
            "and opens one window after the last failure, not one after the first"
        );
    }

    #[test]
    fn an_unabsorbed_roster_on_one_node_withholds_absence_for_sandboxes_on_every_node() {
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
        gate.roster_not_absorbed("node-b", now);

        assert!(
            !gate.warmed_up(now),
            "the gate is one answer for the whole replica: node-a's roster landed, but a miss \
             on one of its sandboxes is still withheld while node-b's roster is missing"
        );
        assert!(
            gate.warmed_up(now + Duration::from_secs(31)),
            "once node-b stops reporting, node-a's misses are answerable again"
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
        gate.roster_not_absorbed("node-b", now);
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
