//! Retires bindings for nodes that disappear and remain silent.
//! The sweeper retains its own last-known roster because discovery drops stale
//! rosters before the binding-silence threshold.

use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use crate::binding_store::{BindingDeleteOutcome, BindingStore};
use crate::node_registry::registry::NodeRegistry;
use crate::node_registry::types::Roster;

/// Default silence threshold before a missing node is swept.
pub const DEFAULT_SWEEP_SILENCE: Duration = Duration::from_secs(5 * 60);
/// Default interval between sweep rounds.
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
    /// Last roster observed independently of the live registry.
    last_known: HashMap<String, Roster>,
    /// Last `last_seen` value already swept per node.
    swept: HashMap<String, SystemTime>,
}

/// Heartbeat-timeout binding sweeper.
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

    /// Refreshes rosters, retires silent missing nodes, and prunes bookkeeping.
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

        // Only nodes absent from discovery are candidates.
        let mut candidates: Vec<(String, Roster)> = Vec::new();
        {
            let mut inner = self.inner.lock().expect("sweep lock poisoned");

            // Never shadow nodes that have not heartbeated.
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
                // Renamed nodes are not dead nodes.
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

        // Suppress whole-fleet retirement, except for a single-node deployment.
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
                    // Unknown incarnations expire by TTL instead of an unguarded delete.
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
                        // A newer incarnation has already taken over.
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
            // Incomplete sweeps retry next round.
        }

        // Prune fully swept nodes that remain absent.
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

    /// Runs sweep rounds forever, delaying the first pass by one interval.
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

// Per-outcome counters avoid a free-form label set.
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

        sweeper.sweep_once(&registry, &store, unix(0)).await;

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
        sweeper.sweep_once(&registry, &store, unix(0)).await;

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
