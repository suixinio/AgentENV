//! Task's own "D4": ports `services/scheduler/internal/sweep.go` (383
//! lines) — the heartbeat-timeout binding sweep. A node that stops
//! heartbeating has its rosters *evicted from discovery* within roughly
//! `KubernetesDiscovery`'s own re-list cadence (`AtomicNodeRegistry::set`'s
//! stale-cleanup pass), but the binding store does not react to that on its
//! own — a binding is a Redis (or in-memory) key with its own TTL, wholly
//! independent of node-registry state. Left alone, gateway keeps routing to
//! a dead node's sandboxes until each binding's own TTL finally lapses
//! (`binding_ttl`, ordinarily tens of seconds — short, but not zero). This
//! sweep exists to shorten that window explicitly, the same way Go's does.
//!
//! # Why this keeps its own shadow copy of the roster, not just the live one
//!
//! `AtomicNodeRegistry::set`'s stale-cleanup drops a node's roster from the
//! registry itself well before any silence threshold this sweep would
//! apply — a `KubernetesDiscovery` re-list happens on a much shorter cadence
//! than `binding_sweep_silence` (minutes, not the discovery loop's usual
//! seconds). By the time this sweep would notice a node missing from
//! [`crate::node_registry::registry::NodeRegistry::rosters_in_cluster`], the
//! registry's own copy of that node's roster is already gone — there would
//! be nothing left to retire. So [`BindingSweeper`] keeps its own
//! `last_known` copy, refreshed every round from whatever
//! `rosters_in_cluster` currently reports, and reacts when a node it once
//! shadowed stops showing up there at all.

use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use crate::binding_store::{BindingDeleteOutcome, BindingStore};
use crate::node_registry::registry::NodeRegistry;
use crate::node_registry::types::Roster;

/// Mirrors Go's `defaultBindingSweepSilence`.
pub const DEFAULT_SWEEP_SILENCE: Duration = Duration::from_secs(5 * 60);
/// Mirrors Go's `defaultBindingSweepInterval`.
pub const DEFAULT_SWEEP_INTERVAL: Duration = Duration::from_secs(30);

/// One sweep round's tally, for logging/metrics at the call site.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SweepOutcome {
    pub retired: u64,
    pub refused_stale: u64,
    pub store_errors: u64,
    pub ignored_unknown_execution: u64,
    pub nodes_swept: u64,
    pub nodes_suppressed_all_silent: u64,
}

struct Inner {
    /// This sweeper's own copy of the last roster it saw for a node, kept
    /// independently of the live registry -- see the module doc.
    last_known: HashMap<String, Roster>,
    /// Which `last_seen` timestamp a node has already been swept for, so a
    /// node that stays silent is retired exactly once rather than every
    /// round, while a node that comes back and goes silent again *is*
    /// swept again (a fresh `last_seen` no longer matches the recorded
    /// one).
    swept: HashMap<String, SystemTime>,
}

/// Ports Go's `bindingSweeper`.
pub struct BindingSweeper {
    cluster_id: String,
    silence: Duration,
    inner: std::sync::Mutex<Inner>,
}

impl BindingSweeper {
    pub fn new(cluster_id: String, silence: Duration) -> Self {
        let silence = if silence.is_zero() {
            DEFAULT_SWEEP_SILENCE
        } else {
            silence
        };
        Self {
            cluster_id,
            silence,
            inner: std::sync::Mutex::new(Inner {
                last_known: HashMap::new(),
                swept: HashMap::new(),
            }),
        }
    }

    /// One round: refresh the shadow copy, find nodes that dropped out of
    /// discovery, retire the silent ones' bindings (unless every candidate
    /// this round is silent -- see [`SweepOutcome::nodes_suppressed_all_silent`]'s
    /// doc), and prune bookkeeping for nodes that are both gone and already
    /// swept.
    pub async fn sweep_once(
        &self,
        registry: &dyn NodeRegistry,
        store: &dyn BindingStore,
        now: SystemTime,
    ) -> SweepOutcome {
        let live: HashMap<String, Roster> = registry
            .rosters_in_cluster(&self.cluster_id)
            .into_iter()
            .map(|r| (r.node_id.clone(), r))
            .collect();

        // Candidates: shadowed nodes no longer present in the live roster
        // list at all. A node still listed there (even with an empty
        // roster) is not a sweep candidate -- it is still known to
        // discovery and merely idle.
        let mut candidates: Vec<(String, Roster)> = Vec::new();
        {
            let mut inner = self.inner.lock().expect("sweep lock poisoned");

            // Refresh the shadow with every currently-live, ever-reported
            // roster -- a roster whose `last_seen` is `None` (known to
            // discovery, never heartbeated) is never shadowed, matching
            // Go's own rule.
            for (node_id, roster) in &live {
                if roster.last_seen.is_some() {
                    inner.last_known.insert(node_id.clone(), roster.clone());
                }
            }

            let shadowed_ids: Vec<String> = inner.last_known.keys().cloned().collect();
            for node_id in shadowed_ids {
                if live.contains_key(&node_id) {
                    continue;
                }
                // A fleet-upgrade rename (the same machine now reports
                // under a new id) is not a death -- forget the stale shadow
                // and move on without sweeping it.
                if let Some(resolved) = registry.resolve(&node_id) {
                    if resolved.id != node_id {
                        inner.last_known.remove(&node_id);
                        inner.swept.remove(&node_id);
                        continue;
                    }
                }
                let roster = inner
                    .last_known
                    .get(&node_id)
                    .cloned()
                    .expect("just checked");
                let last_seen = roster.last_seen.expect("shadowed rosters always have one");
                if inner.swept.get(&node_id) == Some(&last_seen) {
                    continue;
                }
                candidates.push((node_id, roster));
            }
        }

        if candidates.is_empty() {
            return SweepOutcome::default();
        }

        let silent: Vec<&(String, Roster)> = candidates
            .iter()
            .filter(|(_, roster)| {
                let last_seen = roster.last_seen.expect("candidates always have one");
                now.duration_since(last_seen).unwrap_or(Duration::ZERO) > self.silence
            })
            .collect();

        // Whole-fleet guard: with more than one candidate, "every one of
        // them is silent" is much more likely to be a partition or a
        // control-plane outage than every one of them actually dying at
        // once -- suppress rather than retire the whole fleet's bindings.
        // A single-node deployment is exempt: there, "the only candidate is
        // silent" and "the one node died" are the same sentence.
        if candidates.len() > 1 && silent.len() == candidates.len() {
            return SweepOutcome {
                nodes_suppressed_all_silent: candidates.len() as u64,
                ..Default::default()
            };
        }

        let mut outcome = SweepOutcome::default();
        for (node_id, roster) in silent {
            let last_seen = roster.last_seen.expect("candidates always have one");
            if now.duration_since(last_seen).unwrap_or(Duration::ZERO) <= self.silence {
                continue;
            }
            let mut complete = true;
            for entry in &roster.entries {
                if entry.execution_id.is_empty() {
                    // Unguarded delete is refused on purpose -- such
                    // records already carry the short binding_ttl and
                    // expire on their own.
                    outcome.ignored_unknown_execution += 1;
                    continue;
                }
                match store
                    .delete(&entry.sandbox_id, &entry.execution_id, now)
                    .await
                {
                    Ok(
                        BindingDeleteOutcome::Deleted
                        | BindingDeleteOutcome::DeletedUnknownIncumbent,
                    ) => {
                        outcome.retired += 1;
                    }
                    Ok(BindingDeleteOutcome::RejectedStale) => {
                        // The guard doing its job: the sandbox was rescued
                        // elsewhere under a newer incarnation.
                        outcome.refused_stale += 1;
                    }
                    Ok(BindingDeleteOutcome::Absent) => {}
                    Err(_) => {
                        outcome.store_errors += 1;
                        complete = false;
                    }
                }
            }
            if complete {
                let mut inner = self.inner.lock().expect("sweep lock poisoned");
                inner.swept.insert(node_id.clone(), last_seen);
                outcome.nodes_swept += 1;
            }
            // else: retried next round, matching Go's own "not marked swept
            // on failure" rule.
        }

        // Prune: a node both gone from discovery and already fully swept
        // for its current timestamp no longer needs shadow bookkeeping.
        {
            let mut inner = self.inner.lock().expect("sweep lock poisoned");
            let stale: Vec<String> = inner
                .last_known
                .keys()
                .filter(|id| {
                    !live.contains_key(*id)
                        && inner.swept.get(*id).is_some_and(|swept_at| {
                            Some(*swept_at) == inner.last_known[*id].last_seen
                        })
                })
                .cloned()
                .collect();
            for id in stale {
                inner.last_known.remove(&id);
                inner.swept.remove(&id);
            }
        }

        outcome
    }

    /// Runs [`BindingSweeper::sweep_once`] on `interval`, forever. No
    /// initial pass on start -- a fresh replica must not conclude a fleet
    /// it has never spoken to is dead (mirrors Go's own
    /// `RunBindingSweep`).
    pub async fn run(
        self: std::sync::Arc<Self>,
        registry: std::sync::Arc<dyn NodeRegistry>,
        store: std::sync::Arc<dyn BindingStore>,
        interval: Duration,
    ) {
        let interval = if interval.is_zero() {
            DEFAULT_SWEEP_INTERVAL
        } else {
            interval
        };
        let mut ticker = tokio::time::interval(interval);
        ticker.tick().await; // consume the immediate first tick
        loop {
            ticker.tick().await;
            let outcome = self
                .sweep_once(registry.as_ref(), store.as_ref(), SystemTime::now())
                .await;
            if outcome.retired > 0
                || outcome.refused_stale > 0
                || outcome.store_errors > 0
                || outcome.nodes_suppressed_all_silent > 0
            {
                tracing::info!(
                    retired = outcome.retired,
                    refused_stale = outcome.refused_stale,
                    store_errors = outcome.store_errors,
                    nodes_swept = outcome.nodes_swept,
                    nodes_suppressed_all_silent = outcome.nodes_suppressed_all_silent,
                    "binding sweep round completed"
                );
            }
            metrics::counter!(SWEEP_RETIRED_METRIC).increment(outcome.retired);
            metrics::counter!(SWEEP_REFUSED_STALE_METRIC).increment(outcome.refused_stale);
            metrics::counter!(SWEEP_STORE_ERROR_METRIC).increment(outcome.store_errors);
            metrics::counter!(SWEEP_NODES_SWEPT_METRIC).increment(outcome.nodes_swept);
            metrics::counter!(SWEEP_NODES_SUPPRESSED_METRIC)
                .increment(outcome.nodes_suppressed_all_silent);
        }
    }
}

/// Ports `agentenv_scheduler_binding_sweep_total{outcome="deleted"}`-shaped
/// counters, split one metric per outcome rather than a label set, since
/// `run`'s own increments are already outcome-specific sums.
const SWEEP_RETIRED_METRIC: &str = "agentenv_api_binding_sweep_retired_total";
const SWEEP_REFUSED_STALE_METRIC: &str = "agentenv_api_binding_sweep_refused_stale_total";
const SWEEP_STORE_ERROR_METRIC: &str = "agentenv_api_binding_sweep_store_error_total";
const SWEEP_NODES_SWEPT_METRIC: &str = "agentenv_api_binding_sweep_nodes_swept_total";
const SWEEP_NODES_SUPPRESSED_METRIC: &str = "agentenv_api_binding_sweep_nodes_suppressed_total";

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binding_store::in_memory::InMemoryBindingStore;
    use crate::binding_store::{Binding, BindingStoreSettings};
    use crate::node_registry::registry::AtomicNodeRegistry;
    use crate::node_registry::types::Node;

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

    async fn registry_with_roster(
        node_id: &str,
        sandbox_id: &str,
        execution_id: &str,
        last_seen: SystemTime,
    ) -> AtomicNodeRegistry {
        let registry = AtomicNodeRegistry::new(vec![node(node_id)], Duration::from_secs(30));
        registry
            .heartbeat(
                &crate::proto::scheduler::HeartbeatRequest {
                    node_id: node_id.to_string(),
                    cluster_id: "cluster-a".to_string(),
                    service_instance_id: format!("svc-{node_id}"),
                    roster: vec![crate::proto::scheduler::SandboxRosterEntry {
                        sandbox_id: sandbox_id.to_string(),
                        execution_id: execution_id.to_string(),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                last_seen,
            )
            .unwrap();
        registry
    }

    #[tokio::test]
    async fn a_node_still_in_discovery_is_never_a_candidate_regardless_of_silence() {
        let registry = registry_with_roster(
            "node-a",
            "sbx-1",
            "00000000-0000-7000-8000-000000000001",
            unix(0),
        )
        .await;
        let store = InMemoryBindingStore::new(BindingStoreSettings::default());
        store
            .record(
                "sbx-1",
                Binding {
                    node: node("node-a"),
                    execution_id: "00000000-0000-7000-8000-000000000001".to_string(),
                    projection_ttl: Duration::from_secs(200_000),
                },
                unix(0),
            )
            .await
            .unwrap();
        let sweeper = BindingSweeper::new("cluster-a".to_string(), Duration::from_secs(60));

        // First round shadows node-a; a long time later, node-a is *still*
        // discoverable (its EndpointSlice entry is unchanged) even though
        // it never heartbeated again.
        sweeper.sweep_once(&registry, &store, unix(0)).await;
        let outcome = sweeper.sweep_once(&registry, &store, unix(100_000)).await;

        assert_eq!(outcome, SweepOutcome::default());
        assert!(store.get("sbx-1", unix(100_000)).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn a_node_that_disappears_and_stays_silent_gets_its_bindings_retired() {
        let registry = registry_with_roster(
            "node-a",
            "sbx-1",
            "00000000-0000-7000-8000-000000000001",
            unix(0),
        )
        .await;
        let store = InMemoryBindingStore::new(BindingStoreSettings::default());
        store
            .record(
                "sbx-1",
                Binding {
                    node: node("node-a"),
                    execution_id: "00000000-0000-7000-8000-000000000001".to_string(),
                    projection_ttl: Duration::from_secs(3600),
                },
                unix(0),
            )
            .await
            .unwrap();
        let sweeper = BindingSweeper::new("cluster-a".to_string(), Duration::from_secs(60));

        // Round 1, while node-a is still discoverable: shadows it.
        sweeper.sweep_once(&registry, &store, unix(0)).await;

        // node-a's EndpointSlice entry is gone entirely (never renamed,
        // just gone) -- an empty registry.
        let empty_registry = AtomicNodeRegistry::new(vec![], Duration::from_secs(30));

        let outcome = sweeper
            .sweep_once(&empty_registry, &store, unix(1_000))
            .await;
        assert_eq!(outcome.retired, 1);
        assert_eq!(outcome.nodes_swept, 1);
        assert!(
            store.get("sbx-1", unix(1_000)).await.unwrap().is_none(),
            "the dead node's binding must have been retired"
        );
    }

    #[tokio::test]
    async fn a_binding_rescued_under_a_newer_incarnation_survives_the_sweep() {
        let registry = registry_with_roster(
            "node-a",
            "sbx-1",
            "00000000-0000-7000-8000-000000000001",
            unix(0),
        )
        .await;
        let store = InMemoryBindingStore::new(BindingStoreSettings::default());
        // The sandbox has since been resumed elsewhere under a newer
        // incarnation -- the sweep's own delete call must be refused by
        // the same guard applyProjectionDelete relies on.
        store
            .record(
                "sbx-1",
                Binding {
                    node: node("node-b"),
                    execution_id: "00000000-0000-7000-8000-000000000002".to_string(),
                    projection_ttl: Duration::from_secs(3600),
                },
                unix(0),
            )
            .await
            .unwrap();
        let sweeper = BindingSweeper::new("cluster-a".to_string(), Duration::from_secs(60));
        sweeper.sweep_once(&registry, &store, unix(0)).await;

        let empty_registry = AtomicNodeRegistry::new(vec![], Duration::from_secs(30));
        let outcome = sweeper
            .sweep_once(&empty_registry, &store, unix(1_000))
            .await;

        assert_eq!(outcome.refused_stale, 1);
        assert_eq!(outcome.retired, 0);
        let binding = store
            .get("sbx-1", unix(1_000))
            .await
            .unwrap()
            .expect("must survive");
        assert_eq!(binding.node.id, "node-b");
    }

    #[tokio::test]
    async fn a_roster_entry_with_no_execution_id_is_never_unguarded_deleted() {
        let registry = AtomicNodeRegistry::new(vec![node("node-a")], Duration::from_secs(30));
        registry
            .heartbeat(
                &crate::proto::scheduler::HeartbeatRequest {
                    node_id: "node-a".to_string(),
                    cluster_id: "cluster-a".to_string(),
                    service_instance_id: "svc-node-a".to_string(),
                    roster: vec![crate::proto::scheduler::SandboxRosterEntry {
                        sandbox_id: "sbx-1".to_string(),
                        execution_id: String::new(),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                unix(0),
            )
            .unwrap();
        let store = InMemoryBindingStore::new(BindingStoreSettings::default());
        store
            .record(
                "sbx-1",
                Binding {
                    node: node("node-a"),
                    ..Default::default()
                },
                unix(0),
            )
            .await
            .unwrap();
        let sweeper = BindingSweeper::new("cluster-a".to_string(), Duration::from_secs(60));
        sweeper.sweep_once(&registry, &store, unix(0)).await;

        let empty_registry = AtomicNodeRegistry::new(vec![], Duration::from_secs(30));
        let outcome = sweeper
            .sweep_once(&empty_registry, &store, unix(1_000))
            .await;

        assert_eq!(outcome.ignored_unknown_execution, 1);
        assert_eq!(outcome.retired, 0);
        // The record still exists -- it will expire on its own short TTL,
        // not via an unguarded sweep delete.
        assert!(store.get("sbx-1", unix(0)).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn a_single_node_cluster_still_sweeps_even_though_all_candidates_are_silent() {
        let registry = registry_with_roster(
            "node-a",
            "sbx-1",
            "00000000-0000-7000-8000-000000000001",
            unix(0),
        )
        .await;
        let store = InMemoryBindingStore::new(BindingStoreSettings::default());
        store
            .record(
                "sbx-1",
                Binding {
                    node: node("node-a"),
                    execution_id: "00000000-0000-7000-8000-000000000001".to_string(),
                    projection_ttl: Duration::from_secs(3600),
                },
                unix(0),
            )
            .await
            .unwrap();
        let sweeper = BindingSweeper::new("cluster-a".to_string(), Duration::from_secs(60));
        sweeper.sweep_once(&registry, &store, unix(0)).await;

        let empty_registry = AtomicNodeRegistry::new(vec![], Duration::from_secs(30));
        let outcome = sweeper
            .sweep_once(&empty_registry, &store, unix(1_000))
            .await;

        assert_eq!(
            outcome.retired, 1,
            "a single candidate being silent is 'the one node died', not 'suppress everything'"
        );
    }

    #[tokio::test]
    async fn a_renamed_node_is_forgotten_not_swept() {
        let registry = AtomicNodeRegistry::new(vec![node("node-a-old")], Duration::from_secs(30));
        registry
            .heartbeat(
                &crate::proto::scheduler::HeartbeatRequest {
                    node_id: "node-a-old".to_string(),
                    cluster_id: "cluster-a".to_string(),
                    service_instance_id: "svc-node-a-old".to_string(),
                    roster: vec![crate::proto::scheduler::SandboxRosterEntry {
                        sandbox_id: "sbx-1".to_string(),
                        execution_id: "00000000-0000-7000-8000-000000000001".to_string(),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                unix(0),
            )
            .unwrap();
        let store = InMemoryBindingStore::new(BindingStoreSettings::default());
        store
            .record(
                "sbx-1",
                Binding {
                    node: node("node-a-old"),
                    execution_id: "00000000-0000-7000-8000-000000000001".to_string(),
                    projection_ttl: Duration::from_secs(3600),
                },
                unix(0),
            )
            .await
            .unwrap();

        let sweeper = BindingSweeper::new("cluster-a".to_string(), Duration::from_secs(60));
        // Round 1: shadow node-a-old while it is still the live identity.
        sweeper.sweep_once(&registry, &store, unix(0)).await;

        // A fleet upgrade renames the same machine: node-a-old is now an
        // alias for the new canonical id node-a-new, not a departed node.
        registry.set(
            vec![Node {
                id: "node-a-new".to_string(),
                endpoint: "http://node-a-new".to_string(),
                pod_name: "node-a-old".to_string(),
            }],
            Vec::new(),
            unix(1),
        );

        let outcome = sweeper.sweep_once(&registry, &store, unix(1_000)).await;

        assert_eq!(
            outcome,
            SweepOutcome::default(),
            "a rename must never be swept as a death"
        );
        assert!(
            store.get("sbx-1", unix(1_000)).await.unwrap().is_some(),
            "the binding recorded under the old identity must survive a rename"
        );
    }

    #[tokio::test]
    async fn a_whole_fleet_going_silent_at_once_is_suppressed_not_retired() {
        let registry = AtomicNodeRegistry::new(
            vec![node("node-a"), node("node-b")],
            Duration::from_secs(30),
        );
        for (node_id, sandbox_id, exec) in [
            ("node-a", "sbx-1", "00000000-0000-7000-8000-000000000001"),
            ("node-b", "sbx-2", "00000000-0000-7000-8000-000000000002"),
        ] {
            registry
                .heartbeat(
                    &crate::proto::scheduler::HeartbeatRequest {
                        node_id: node_id.to_string(),
                        cluster_id: "cluster-a".to_string(),
                        service_instance_id: format!("svc-{node_id}"),
                        roster: vec![crate::proto::scheduler::SandboxRosterEntry {
                            sandbox_id: sandbox_id.to_string(),
                            execution_id: exec.to_string(),
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                    unix(0),
                )
                .unwrap();
        }

        let store = InMemoryBindingStore::new(BindingStoreSettings::default());
        for (sandbox_id, node_id, exec) in [
            ("sbx-1", "node-a", "00000000-0000-7000-8000-000000000001"),
            ("sbx-2", "node-b", "00000000-0000-7000-8000-000000000002"),
        ] {
            store
                .record(
                    sandbox_id,
                    Binding {
                        node: node(node_id),
                        execution_id: exec.to_string(),
                        projection_ttl: Duration::from_secs(3600),
                    },
                    unix(0),
                )
                .await
                .unwrap();
        }

        let sweeper = BindingSweeper::new("cluster-a".to_string(), Duration::from_secs(60));
        sweeper.sweep_once(&registry, &store, unix(0)).await;

        let empty_registry = AtomicNodeRegistry::new(vec![], Duration::from_secs(30));
        let outcome = sweeper
            .sweep_once(&empty_registry, &store, unix(1_000))
            .await;

        assert_eq!(outcome.nodes_suppressed_all_silent, 2);
        assert_eq!(outcome.retired, 0);
        assert!(store.get("sbx-1", unix(1_000)).await.unwrap().is_some());
        assert!(store.get("sbx-2", unix(1_000)).await.unwrap().is_some());
    }
}
