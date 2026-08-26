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
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

use super::registry::NodeRegistry;

/// Mirrors Go's `defaultWarmupTimeout`.
pub const DEFAULT_WARMUP_TIMEOUT: Duration = Duration::from_secs(15);

pub struct WarmupGate {
    nodes: Arc<dyn NodeRegistry>,
    /// `RwLock`, not a plain field: [`Self::rebase_deadline`] needs to move
    /// this after construction — see its own doc comment for why a gate
    /// constructed once and armed later needs that at all. Reads (every
    /// [`Self::warmed_up`] call) are far more frequent than the single
    /// rebase a caller is expected to perform, so an uncontended `RwLock`
    /// read is the right trade rather than an atomic epoch encoding.
    deadline: RwLock<SystemTime>,
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
        Self {
            nodes,
            deadline: RwLock::new(Self::effective_deadline(now, timeout)),
            warm: AtomicBool::new(false),
            reported: AtomicBool::new(false),
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

    /// Moves the deadline to `now + timeout`, superseding whatever
    /// [`Self::new`] computed it as.
    ///
    /// 🔴 Exists because `start_native_node_registry`
    /// (`src/bin/aenv-api.rs`) has to construct this gate (and hand it to
    /// `NodeRegistryGrpcService`, which the gRPC listener is built around)
    /// *before* that listener binds — but the clock this timeout should be
    /// measured from is when the listener actually starts being able to
    /// receive a `Heartbeat` RPC, not when the gate happened to be
    /// constructed earlier in `assemble_api`'s sequence. See
    /// `AENV_CLUSTER_NATIVE_WARMUP_TIMEOUT_SECS`'s own doc comment
    /// (`cfg.rs`) for the measured cost of getting this wrong: assembly
    /// between the two points has run over 15s end to end, which is enough
    /// to expire the whole default timeout before a heartbeat could
    /// possibly have arrived, latching the gate "warm" on the very first
    /// `warmed_up` call with zero heartbeats received.
    ///
    /// Harmless to call after the gate has already gone warm (every known
    /// node reported before the listener even finished binding, the
    /// fast/healthy case): [`Self::warmed_up`] short-circuits on `warm`
    /// before it ever reads the deadline this moves, so a rebase at that
    /// point changes a value nothing looks at again.
    pub fn rebase_deadline(&self, now: SystemTime, timeout: Duration) {
        let mut deadline = self
            .deadline
            .write()
            .expect("warmup deadline lock poisoned");
        *deadline = Self::effective_deadline(now, timeout);
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
    ///
    /// 🔴 P1 (task's own "phase4-close" P3): `reported` is checked *before*
    /// the deadline, not after. The deadline exists to stop waiting forever
    /// on a straggler that is genuinely down while *other* nodes have
    /// already reported (the loop below) — it must never, on its own,
    /// latch `warm` on a registry that has received zero heartbeats at
    /// all. A registry in that state is not "waited long enough and heard
    /// nothing back from a dead node" — it is a process that has not
    /// finished starting, and every replica of a rolling DaemonSet restart
    /// is in exactly that state simultaneously for this gate's whole
    /// `timeout`. Before this check existed here, a cold registry still
    /// latched `warm = true` the moment its deadline passed, and
    /// [`crate::node_client::NativeNodePlacement::node_membership`] turned
    /// that straight into a confident [`crate::node_client::placement::NodeMembership::Gone`]
    /// for every node it was asked about — which `src/node_client/stub.rs`
    /// and `src/orchestrator/service.rs` treat as proof a sandbox's runtime
    /// is gone for good, and delete its record. See this module's own
    /// tests, and `native_placement.rs`'s
    /// `node_membership_stays_cold_past_the_deadline_when_nothing_has_ever_reported`,
    /// for the two-sided proof: the gate must stay shut with zero reports
    /// even past the deadline, and must still open at the deadline once at
    /// least one node (not necessarily every node) has reported in.
    pub fn warmed_up(&self, now: SystemTime) -> bool {
        if self.warm.load(Ordering::SeqCst) {
            return true;
        }
        if !self.reported.load(Ordering::SeqCst) {
            return false;
        }
        let deadline = *self.deadline.read().expect("warmup deadline lock poisoned");
        if now >= deadline {
            self.warm.store(true, Ordering::SeqCst);
            return true;
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
    // life of the process -- but only once *something else* has reported,
    // so this registry is not merely a process that has not finished
    // starting yet. See `warmed_up`'s own doc comment for the two-sided
    // argument and the P1 bug this pair of tests replaced a single,
    // now-wrong test for.
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
        // node-a reports; node-b never does for the rest of this test --
        // deliberately not both, or the roster-complete path a few lines
        // below `warmed_up`'s deadline check would open the gate on its
        // own and this test would no longer be exercising the deadline at
        // all.
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

    /// 🔴 P1: the fixed half of the bug `NativeNodePlacement::node_membership`
    /// used to have -- a registry that has received *zero* heartbeats at
    /// all (not "one straggler among several", but every known node still
    /// silent) must not open past its deadline. Before the fix, this and
    /// the test above were one test, asserting the gate opens at the
    /// deadline "even with a silent node" with no heartbeat fed to it at
    /// all -- which was proving the exact bug that let a freshly
    /// (re)started replica read every node as `Gone` the moment its
    /// deadline passed, heartbeats or not.
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

        registry.set(vec![node("node-a"), node("node-b")], Vec::new(), now);

        assert!(
            gate.warmed_up(now),
            "a newly discovered node must not reopen warm-up"
        );
    }

    // ---- rebase_deadline: the gRPC-bind-not-construction-time fix ----

    #[test]
    fn rebase_deadline_moves_when_the_deadline_falls() {
        // Two known nodes, and only node-a ever heartbeats, for the same
        // reason `opens_at_the_deadline_for_a_silent_straggler_once_something_has_reported`
        // above uses two: since P1, the deadline can only open the gate
        // once *something* has reported (`warmed_up`'s own doc comment), so
        // this test needs at least one heartbeat to exercise the deadline
        // boundary at all -- but heartbeating every known node would open
        // the gate through the roster-complete path instead, before either
        // `warmed_up` call below ever consulted a deadline, rebased or not.
        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![node("node-a"), node("node-b")],
            Duration::from_secs(30),
        ));
        // Constructed as if the assembly sequence before the gRPC listener
        // bound had already burned the whole timeout — exactly the bug
        // this exists to fix.
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

        // The listener actually binds much later — rebase from there.
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
