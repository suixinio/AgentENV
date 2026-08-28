//! Task 4's equivalence-dump debug endpoint: a read-only view of what api's
//! own node registry currently believes.
//!
//! # 🔴 One source now, not two
//!
//! This used to run under either of a now-deleted `[cluster].node_placement_source`
//! switch's two values — reading api's own [`AtomicNodeRegistry`] directly
//! under `Native`, or making a live `ListNodes` + `ListObservedNodes` RPC
//! pair against a real Go scheduler process under `Scheduler` (the former
//! default) — and
//! reshaping both into the identical [`NodeRegistryDump`] JSON shape so a
//! caller could diff this endpoint's output against the scheduler's own
//! `ListNodes`/`ListObservedNodes` and see the two agree. That scheduler
//! process is deleted from the tree (see "Distributed Control Plane" in the
//! repo's top-level `CLAUDE.md`), so there is nothing left to proxy to or to
//! diff against: [`dump`] now only ever reads the local registry, and
//! `source` on [`NodeRegistryDump`] is always `"native"`.
//!
//! # 🔴 D6: the CPU intersection is *recomputed here*, not read from a cache
//!
//! [`compute_intersections`] runs
//! [`crate::node_registry::cpu_template::intersect_cpu_configs`] — the same
//! algorithm [`AtomicNodeRegistry::heartbeat`] uses internally — fresh, over
//! whatever `machine_info.cpu_config_json` values the dump just fetched from
//! the local registry, rather than reading a cached value out of it. This
//! makes the dump a second, independent invocation of the same algorithm on
//! live data, on top of `cpu_template`'s own byte-for-byte golden-output test
//! against the real Go implementation and `grpc_service`'s own end-to-end
//! wire test — three different angles on the same "must keep working" chain
//! CLAUDE.md names.
//!
//! 🔴 P4 correction to the paragraph above: that recompute
//! (`cpu_intersection_recomputed_by_cluster` below) does **not** check
//! `all_configs_ready` the way production does (`AtomicNodeRegistry::heartbeat`,
//! `registry.rs`), so it can show a non-empty intersection while the cluster
//! is still only partially reported — any node discovery knows about that has
//! not yet sent even one `cpu_config_json` is invisible to it, because
//! [`compute_intersections`] only ever sees nodes that already have a
//! non-empty config to contribute. A `D6` acceptance comparison that reads
//! this field as "the value the cluster is running on" can therefore be
//! fooled. `cpu_intersection_applied_by_cluster` is the other half of the
//! fix: the *gated* value [`NodeRegistry::applied_cpu_intersection`] actually
//! cached and would hand a node on its next heartbeat. Both are emitted side
//! by side so a diff shows the gap instead of hiding it.
//!
//! # `sandbox_ids`
//!
//! The heartbeat roster (which sandboxes a node reported holding) never
//! travels on `ObservedNode` — neither `ListObservedNodes` nor any other RPC
//! exposes it — so [`dump`] enriches its own output from
//! [`NodeRegistry::roster_of`], the same call `AtomicNodeRegistry` itself
//! serves scheduling from. This matters more than it looks: two dumps can
//! report identical `sandbox_count`s while disagreeing about *which*
//! sandboxes those are, which a count alone cannot show.
//!
//! # Determinism
//!
//! `nodes` and `observed` are sorted by `node_id` — the underlying source is
//! a `HashMap` (`AtomicNodeRegistry`'s own internal storage), which is not
//! stable across two calls, and an acceptance diff comparing this endpoint's
//! output against itself needs identical input to read as identical, not a
//! reorder read as a difference.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::SystemTime;

use serde::Serialize;

use crate::node_registry::cpu_template::intersect_cpu_configs;
use crate::node_registry::registry::{AtomicNodeRegistry, NodeRegistry};
use crate::proto::scheduler;

#[derive(Debug, Serialize)]
pub struct NodeRegistryDump {
    /// Always `"native"` now — kept as a field (rather than dropped) because
    /// it is a stable part of this debug endpoint's JSON shape, from the
    /// window when a `"scheduler-proxy"` source also existed. See the
    /// module doc.
    pub source: &'static str,
    /// Discovery-only view — mirrors `ListNodes`. Sorted by `node_id`.
    pub nodes: Vec<DumpNode>,
    /// Heartbeat-derived view — mirrors `ListObservedNodes`. Sorted by
    /// `node_id`.
    pub observed: Vec<DumpObservedNode>,
    /// One entry per cluster id that had at least one node reporting a
    /// non-empty `cpu_config_json`, keyed by cluster id. 🔴 Recomputed fresh
    /// on every request and **not gated** on every known node having
    /// reported — see the module doc's "D6" section. Not the value a node
    /// was actually sent; [`cpu_intersection_applied_by_cluster`] is.
    ///
    /// [`cpu_intersection_applied_by_cluster`]: NodeRegistryDump::cpu_intersection_applied_by_cluster
    pub cpu_intersection_recomputed_by_cluster: BTreeMap<String, String>,
    /// The gated value production actually cached and would hand a node on
    /// its next heartbeat — see the module doc's "D6" section.
    pub cpu_intersection_applied_by_cluster: BTreeMap<String, String>,
    /// Always `None` now that [`dump`] has one source that cannot itself
    /// fail (a registry read is infallible). Kept for the same JSON-shape
    /// reason as `source` above, from the window when a failed
    /// scheduler-proxy RPC used this to say so.
    pub error: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct DumpNode {
    pub node_id: String,
    pub endpoint: String,
}

#[derive(Debug, Serialize)]
pub struct DumpObservedNode {
    pub node_id: String,
    pub endpoint: String,
    pub cluster_id: String,
    /// Which process instance this is — see the module doc's D6 section.
    /// Two dumps agreeing on `node_id` but not this is two different
    /// processes wearing the same name, e.g. a roll the reader believed had
    /// not happened yet.
    pub service_instance_id: String,
    pub version: String,
    pub commit: String,
    pub status: &'static str,
    pub sandbox_count: u32,
    /// The heartbeat roster itself, sorted — not just its count. Two nodes
    /// can report the same `sandbox_count` while holding entirely different
    /// sandboxes, which a count alone cannot show and a binding reconcile
    /// would act on regardless.
    pub sandbox_ids: Vec<String>,
    pub allocated_cpu: u32,
    pub allocated_memory_bytes: u64,
    pub last_seen_unix_ms: i64,
}

impl From<scheduler::ObservedNode> for DumpObservedNode {
    /// `sandbox_ids` is always empty here — `ObservedNode` carries no
    /// roster on the wire (see the module doc) — so [`dump`] fills it in
    /// separately, from [`NodeRegistry::roster_of`], after this conversion
    /// runs.
    fn from(node: scheduler::ObservedNode) -> Self {
        let status = node
            .snapshot
            .as_ref()
            .map(|s| s.status())
            .unwrap_or(scheduler::NodeStatus::Unspecified);
        let (sandbox_count, allocated_cpu, allocated_memory_bytes) = node
            .snapshot
            .as_ref()
            .map(|s| (s.sandbox_count, s.allocated_cpu, s.allocated_memory_bytes))
            .unwrap_or_default();
        Self {
            node_id: node.node_id,
            endpoint: node.endpoint,
            cluster_id: node.cluster_id,
            service_instance_id: node.service_instance_id,
            version: node.version,
            commit: node.commit,
            status: status_label(status),
            sandbox_count,
            sandbox_ids: Vec::new(),
            allocated_cpu,
            allocated_memory_bytes,
            last_seen_unix_ms: node.last_seen_unix_ms,
        }
    }
}

fn status_label(status: scheduler::NodeStatus) -> &'static str {
    match status {
        scheduler::NodeStatus::Ready => "ready",
        scheduler::NodeStatus::Connecting => "connecting",
        scheduler::NodeStatus::Unhealthy => "unhealthy",
        scheduler::NodeStatus::Lingering => "lingering",
        scheduler::NodeStatus::Draining => "draining",
        scheduler::NodeStatus::Unspecified => "unspecified",
    }
}

/// Reads a snapshot of `registry`'s current state — see the module doc.
/// Infallible: a registry read cannot itself fail, so `error` on the result
/// is always `None`.
pub fn dump(registry: &Arc<AtomicNodeRegistry>) -> NodeRegistryDump {
    let now = SystemTime::now();
    let mut nodes: Vec<DumpNode> = registry
        .snapshot(true)
        .into_iter()
        .map(|n| DumpNode {
            node_id: n.id,
            endpoint: n.endpoint,
        })
        .collect();
    nodes.sort_by(|a, b| a.node_id.cmp(&b.node_id));

    let observed_raw = registry.list_observed("", now);
    let cpu_intersection_recomputed_by_cluster = compute_intersections(&observed_raw);
    let cpu_intersection_applied_by_cluster = applied_intersections(registry, &observed_raw);
    let mut observed: Vec<DumpObservedNode> = observed_raw
        .into_iter()
        .map(|node| {
            // 🔴 P4: `ObservedNode` carries no roster on the wire (see the
            // module doc), so this is filled in from the registry directly
            // — the same call `AtomicNodeRegistry` itself would make.
            // `node_id` is read before the field move below consumes it.
            let mut sandbox_ids: Vec<String> = registry
                .roster_of(&node.node_id)
                .map(|(entries, _last_seen)| {
                    entries.into_iter().map(|entry| entry.sandbox_id).collect()
                })
                .unwrap_or_default();
            sandbox_ids.sort();
            DumpObservedNode {
                sandbox_ids,
                ..DumpObservedNode::from(node)
            }
        })
        .collect();
    observed.sort_by(|a, b| a.node_id.cmp(&b.node_id));

    NodeRegistryDump {
        source: "native",
        nodes,
        observed,
        cpu_intersection_recomputed_by_cluster,
        cpu_intersection_applied_by_cluster,
        error: None,
    }
}

/// See the module doc's "🔴 D6" section: recomputed fresh from whatever
/// `machine_info.cpu_config_json` values are currently visible, rather than
/// read from a cache, and **not gated** on every known node having reported
/// — this can disagree with [`applied_intersections`] below.
fn compute_intersections(observed: &[scheduler::ObservedNode]) -> BTreeMap<String, String> {
    let mut by_cluster: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for node in observed {
        let Some(config) = node
            .machine_info
            .as_ref()
            .map(|m| m.cpu_config_json.clone())
            .filter(|c| !c.is_empty())
        else {
            continue;
        };
        by_cluster
            .entry(node.cluster_id.clone())
            .or_default()
            .push(config);
    }
    by_cluster
        .into_iter()
        .filter_map(|(cluster_id, configs)| {
            intersect_cpu_configs(&configs)
                .ok()
                .filter(|intersection| !intersection.is_empty())
                .map(|intersection| (cluster_id, intersection))
        })
        .collect()
}

/// Native-only counterpart to [`compute_intersections`]: for every cluster
/// id represented in `observed`, reads back
/// [`NodeRegistry::applied_cpu_intersection`] — the gated value production
/// actually cached, `None` for as long as `all_configs_ready` is still
/// withholding it — rather than recomputing anything itself. See the module
/// doc's "D6" section for why the two are expected to disagree while a
/// cluster is only partially reported.
fn applied_intersections(
    registry: &Arc<AtomicNodeRegistry>,
    observed: &[scheduler::ObservedNode],
) -> BTreeMap<String, String> {
    let cluster_ids: std::collections::BTreeSet<String> = observed
        .iter()
        .map(|node| node.cluster_id.clone())
        .collect();
    // Matches `compute_intersections`' own grouping: whatever `cluster_id` a
    // node actually sent, blank included, so the two maps' key sets stay
    // directly comparable rather than one silently dropping a case the
    // other keeps.
    cluster_ids
        .into_iter()
        .filter_map(|cluster_id| {
            registry
                .applied_cpu_intersection(&cluster_id)
                .map(|value| (cluster_id, value))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::node_registry::types::Node;
    use crate::proto::scheduler::HeartbeatRequest;

    use super::*;

    fn node(id: &str, endpoint: &str) -> Node {
        Node {
            id: id.to_string(),
            endpoint: endpoint.to_string(),
            pod_name: String::new(),
        }
    }

    /// 🔴 Native mode: the dump reflects both discovery (`nodes`) and
    /// heartbeat state (`observed`), and the recomputed intersection matches
    /// `cpu_template::intersect_cpu_configs` run on the same two configs
    /// directly — proving the dump's own recompute step is not silently
    /// doing something different from the algorithm it claims to run. Also
    /// covers P4's other additions: `service_instance_id`/`version`/
    /// `commit`, and — since this single node's config is immediately
    /// complete — `cpu_intersection_applied_by_cluster` agreeing with the
    /// recomputed value (the divergent case is its own test below).
    #[tokio::test]
    async fn native_dump_reflects_discovery_and_recomputes_the_intersection() {
        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![node("node-a", "http://10.0.0.7:8000")],
            Duration::from_secs(30),
        ));
        let cfg_a =
            r#"{"kvm_capabilities":["cap.a","cap.b"],"cpuid_modifiers":[],"msr_modifiers":[]}"#;
        registry
            .heartbeat(
                &HeartbeatRequest {
                    node_id: "node-a".to_string(),
                    cluster_id: "cluster-a".to_string(),
                    service_instance_id: "instance-a".to_string(),
                    version: "v1.2.3".to_string(),
                    commit: "abc123".to_string(),
                    machine_info: Some(scheduler::MachineInfo {
                        cpu_config_json: cfg_a.to_string(),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                SystemTime::now(),
            )
            .expect("node-a is in discovery");

        let result = dump(&registry);

        assert_eq!(result.source, "native");
        assert_eq!(result.nodes.len(), 1);
        assert_eq!(result.nodes[0].node_id, "node-a");
        assert_eq!(result.observed.len(), 1);
        assert_eq!(result.observed[0].node_id, "node-a");
        assert_eq!(result.observed[0].service_instance_id, "instance-a");
        assert_eq!(result.observed[0].version, "v1.2.3");
        assert_eq!(result.observed[0].commit, "abc123");
        assert!(result.error.is_none());

        // The single-config "intersection of one thing with itself" case —
        // a real cross-node intersection is `grpc_service`'s test, this one
        // is about the dump's own recompute wiring, not the algorithm.
        let expected = intersect_cpu_configs(&[cfg_a.to_string()]).expect("golden algorithm");
        assert_eq!(
            result
                .cpu_intersection_recomputed_by_cluster
                .get("cluster-a"),
            Some(&expected)
        );
        assert_eq!(
            result.cpu_intersection_applied_by_cluster.get("cluster-a"),
            Some(&expected),
            "one node is the whole cluster here, so the gate opens on the same heartbeat"
        );
    }

    /// 🔴 P4: `sandbox_ids`, Native mode — not just `sandbox_count`. Two
    /// nodes hold the same *number* of sandboxes but different ones, which
    /// only the roster itself can show.
    #[tokio::test]
    async fn native_dump_reports_the_heartbeat_roster_not_just_its_count() {
        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![
                node("node-a", "http://10.0.0.7:8000"),
                node("node-b", "http://10.0.0.8:8000"),
            ],
            Duration::from_secs(30),
        ));
        registry
            .heartbeat(
                &HeartbeatRequest {
                    node_id: "node-a".to_string(),
                    cluster_id: "cluster-a".to_string(),
                    service_instance_id: "instance-a".to_string(),
                    roster: vec![
                        scheduler::SandboxRosterEntry {
                            sandbox_id: "sandbox-2".to_string(),
                            ..Default::default()
                        },
                        scheduler::SandboxRosterEntry {
                            sandbox_id: "sandbox-1".to_string(),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                },
                SystemTime::now(),
            )
            .expect("node-a is in discovery");
        registry
            .heartbeat(
                &HeartbeatRequest {
                    node_id: "node-b".to_string(),
                    cluster_id: "cluster-a".to_string(),
                    service_instance_id: "instance-b".to_string(),
                    roster: vec![
                        scheduler::SandboxRosterEntry {
                            sandbox_id: "sandbox-3".to_string(),
                            ..Default::default()
                        },
                        scheduler::SandboxRosterEntry {
                            sandbox_id: "sandbox-4".to_string(),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                },
                SystemTime::now(),
            )
            .expect("node-b is in discovery");

        let result = dump(&registry);

        // Sorted output (P4's determinism guarantee) makes the indices
        // below meaningful without a lookup helper.
        assert_eq!(result.observed[0].node_id, "node-a");
        assert_eq!(result.observed[1].node_id, "node-b");
        // Sorted roster too — insertion order above was deliberately
        // reversed for node-a to prove it.
        assert_eq!(
            result.observed[0].sandbox_ids,
            vec!["sandbox-1", "sandbox-2"]
        );
        assert_eq!(
            result.observed[1].sandbox_ids,
            vec!["sandbox-3", "sandbox-4"]
        );
        assert_eq!(
            result.observed[0].sandbox_count,
            result.observed[1].sandbox_count
        );
        assert_ne!(
            result.observed[0].sandbox_ids, result.observed[1].sandbox_ids,
            "equal counts must not be mistaken for equal rosters"
        );
    }

    /// 🔴 P4: the D6 correction itself — the recomputed intersection can be
    /// non-empty while the applied (gated) one is still withheld, because
    /// `compute_intersections` only ever sees nodes that already have a
    /// non-empty config, while `all_configs_ready` waits for every node
    /// that has ever heartbeated for the cluster. node-b here has
    /// heartbeated (so it counts toward "the cluster") but not yet with a
    /// config, which is exactly the gap.
    #[tokio::test]
    async fn native_dump_shows_the_recomputed_and_applied_intersections_diverging() {
        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![
                node("node-a", "http://10.0.0.7:8000"),
                node("node-b", "http://10.0.0.8:8000"),
            ],
            Duration::from_secs(30),
        ));
        let cfg_a =
            r#"{"kvm_capabilities":["cap.a","cap.b"],"cpuid_modifiers":[],"msr_modifiers":[]}"#;
        registry
            .heartbeat(
                &HeartbeatRequest {
                    node_id: "node-a".to_string(),
                    cluster_id: "cluster-a".to_string(),
                    service_instance_id: "instance-a".to_string(),
                    machine_info: Some(scheduler::MachineInfo {
                        cpu_config_json: cfg_a.to_string(),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                SystemTime::now(),
            )
            .expect("node-a is in discovery");
        // node-b heartbeats — so it counts toward the cluster's size — but
        // reports no cpu_config_json yet.
        registry
            .heartbeat(
                &HeartbeatRequest {
                    node_id: "node-b".to_string(),
                    cluster_id: "cluster-a".to_string(),
                    service_instance_id: "instance-b".to_string(),
                    ..Default::default()
                },
                SystemTime::now(),
            )
            .expect("node-b is in discovery");

        let result = dump(&registry);

        let expected_recompute =
            intersect_cpu_configs(&[cfg_a.to_string()]).expect("golden algorithm");
        assert_eq!(
            result
                .cpu_intersection_recomputed_by_cluster
                .get("cluster-a"),
            Some(&expected_recompute),
            "the ungated recompute only ever sees node-a, which has a config"
        );
        assert_eq!(
            result.cpu_intersection_applied_by_cluster.get("cluster-a"),
            None,
            "the gated, actually-applied value must stay withheld until node-b reports too"
        );
    }
}
