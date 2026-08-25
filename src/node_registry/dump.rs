//! Task 4's equivalence-dump debug endpoint: a read-only view of what api's
//! node registry currently believes, in a shape directly comparable to the
//! real scheduler's `ListNodes`/`ListObservedNodes` answers — the
//! cluster-verification tool the Stage A placement switch (task's own "D7")
//! needs to be checked against something.
//!
//! # Works under both `[cluster].node_placement_source` values, on purpose
//!
//! - `Native`: reads api's own [`AtomicNodeRegistry`] directly — the same
//!   data [`crate::node_client::NativeNodePlacement`] uses to answer
//!   `resolve_node`/`node_membership`.
//! - `Scheduler` (the default): `assemble_api` builds no local registry at
//!   all under this mode (Task 1's "connect nothing under Scheduler"
//!   constraint — see `src/bin/server.rs`'s `start_native_node_registry`),
//!   so there is nothing local to read. This mode instead makes a live
//!   `ListNodes` + `ListObservedNodes` RPC pair against the real scheduler,
//!   over api's already-configured `[cluster].scheduler_endpoint`, and
//!   reshapes the answer into the identical output format
//!   [`dump_native`] produces.
//!
//! Both branches emit the exact same JSON shape ([`NodeRegistryDump`]), so a
//! caller comparing this endpoint's output against a direct `grpcurl` at the
//! scheduler's `ListNodes`/`ListObservedNodes` is diffing apples to apples
//! either way — the only thing that changes between the two modes is *where*
//! the answer came from, which the `source` field on the response names.
//!
//! # 🔴 D6: the CPU intersection is *recomputed here*, not read from a cache
//!
//! [`compute_intersections`] runs
//! [`crate::node_registry::cpu_template::intersect_cpu_configs`] — the same
//! algorithm [`AtomicNodeRegistry::heartbeat`] uses internally — fresh, over
//! whatever `machine_info.cpu_config_json` values the dump just fetched
//! (from the local registry or from a live `ListObservedNodes`), rather than
//! reading a cached value out of the registry. This makes the dump a second,
//! independent invocation of the same algorithm on live data, on top of
//! `cpu_template`'s own byte-for-byte golden-output test against the real Go
//! implementation and `grpc_service`'s own end-to-end wire test — three
//! different angles on the same "must keep working" chain CLAUDE.md names.
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
//! by side so a diff shows the gap instead of hiding it — and, under
//! `Scheduler` mode, the applied map is always empty: no RPC exposes the real
//! scheduler's internal cache, so there is nothing honest to put there (see
//! [`dump_scheduler_proxy`]'s own note).
//!
//! # 🔴 P4: `sandbox_ids` is Native-only, for the same reason
//!
//! The heartbeat roster (which sandboxes a node reported holding) never
//! travels on `ObservedNode` — neither `ListObservedNodes` nor any other RPC
//! exposes it — so [`dump_native`] can enrich its own output from
//! [`NodeRegistry::roster_of`], the same call `AtomicNodeRegistry` itself
//! serves scheduling from, but [`dump_scheduler_proxy`] has nothing to ask
//! the real scheduler for and leaves every `sandbox_ids` empty. This matters
//! more than it looks: two dumps can report identical `sandbox_count`s while
//! disagreeing about *which* sandboxes those are — the exact shape of
//! inconsistency the binding reconcile in a real scheduler's `LookupNode`
//! would silently act on — and a count alone cannot show that.
//!
//! # Determinism
//!
//! `nodes` and `observed` are sorted by `node_id` in both branches. The
//! underlying sources are a `HashMap` (`dump_native`, via
//! `AtomicNodeRegistry`'s own internal storage) and whatever order a gRPC
//! response happens to arrive in (`dump_scheduler_proxy`) — neither is
//! stable across two calls, and an acceptance diff comparing this endpoint's
//! output against itself (or against the real scheduler's) needs identical
//! input to read as identical, not a reorder read as a difference.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::SystemTime;

use serde::Serialize;
use tonic::transport::Channel;

use crate::node_registry::cpu_template::intersect_cpu_configs;
use crate::node_registry::registry::{AtomicNodeRegistry, NodeRegistry};
use crate::proto::scheduler::{self, scheduler_client::SchedulerClient};

/// Where [`dump`] reads from — built once at `assemble_api` time (see
/// `src/bin/server.rs`) from whichever of the two the placement switch and
/// configuration make available.
#[derive(Clone)]
pub enum NodeRegistryDumpSource {
    /// `[cluster].node_placement_source = "native"`: api's own registry.
    Native(Arc<AtomicNodeRegistry>),
    /// `[cluster].node_placement_source = "scheduler"` (or `Native` without
    /// a registry, which should not happen given `assemble_api`'s own
    /// wiring, but this variant is also the safe fallback for that case): a
    /// lazily connected channel to the real scheduler, dialled fresh on
    /// every request via `ListNodes`/`ListObservedNodes`.
    SchedulerProxy(Channel),
}

#[derive(Debug, Serialize)]
pub struct NodeRegistryDump {
    /// `"native"` or `"scheduler-proxy"` — which of the two branches above
    /// actually answered this request.
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
    /// its next heartbeat — see the module doc's "D6" section. Always empty
    /// under `Scheduler` mode: no RPC exposes the real scheduler's internal
    /// cache to reconstruct this from.
    pub cpu_intersection_applied_by_cluster: BTreeMap<String, String>,
    /// Set when a `SchedulerProxy` RPC failed; the corresponding list above
    /// is then empty rather than partially populated, so a caller cannot
    /// mistake "the scheduler answered with nothing" for "the RPC failed".
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
    /// The heartbeat roster itself, sorted — not just its count. Native-only;
    /// see the module doc's own note on why `Scheduler` mode cannot fill
    /// this in. Two nodes can report the same `sandbox_count` while holding
    /// entirely different sandboxes, which a count alone cannot show and a
    /// binding reconcile would act on regardless.
    pub sandbox_ids: Vec<String>,
    pub allocated_cpu: u32,
    pub allocated_memory_bytes: u64,
    pub last_seen_unix_ms: i64,
}

impl From<scheduler::ObservedNode> for DumpObservedNode {
    /// `sandbox_ids` is always empty here — `ObservedNode` carries no
    /// roster on the wire in either implementation (see the module doc) —
    /// so [`dump_native`] fills it in separately, from
    /// [`NodeRegistry::roster_of`], after this conversion runs.
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

pub async fn dump(source: &NodeRegistryDumpSource) -> NodeRegistryDump {
    match source {
        NodeRegistryDumpSource::Native(registry) => dump_native(registry),
        NodeRegistryDumpSource::SchedulerProxy(channel) => dump_scheduler_proxy(channel).await,
    }
}

fn dump_native(registry: &Arc<AtomicNodeRegistry>) -> NodeRegistryDump {
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

async fn dump_scheduler_proxy(channel: &Channel) -> NodeRegistryDump {
    let mut client = SchedulerClient::new(channel.clone());
    let mut error: Option<String> = None;

    let mut nodes: Vec<DumpNode> = match client.list_nodes(scheduler::ListNodesRequest {}).await {
        Ok(response) => response
            .into_inner()
            .nodes
            .into_iter()
            .map(|n| DumpNode {
                node_id: n.node_id,
                endpoint: n.endpoint,
            })
            .collect(),
        Err(status) => {
            error = Some(format!("ListNodes: {status}"));
            Vec::new()
        }
    };
    nodes.sort_by(|a, b| a.node_id.cmp(&b.node_id));

    let observed_raw = match client
        .list_observed_nodes(scheduler::ListObservedNodesRequest {
            cluster_id: String::new(),
        })
        .await
    {
        Ok(response) => response.into_inner().nodes,
        Err(status) => {
            let message = format!("ListObservedNodes: {status}");
            error = Some(match error {
                Some(existing) => format!("{existing}; {message}"),
                None => message,
            });
            Vec::new()
        }
    };
    let cpu_intersection_recomputed_by_cluster = compute_intersections(&observed_raw);
    let mut observed: Vec<DumpObservedNode> = observed_raw
        .into_iter()
        .map(DumpObservedNode::from)
        .collect();
    observed.sort_by(|a, b| a.node_id.cmp(&b.node_id));

    NodeRegistryDump {
        source: "scheduler-proxy",
        nodes,
        observed,
        cpu_intersection_recomputed_by_cluster,
        // 🔴 Always empty here — see the module doc's own note: no RPC
        // exposes the real scheduler's internal, gated intersection cache
        // to reconstruct this field from over the wire.
        cpu_intersection_applied_by_cluster: BTreeMap::new(),
        error,
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

        let source = NodeRegistryDumpSource::Native(Arc::clone(&registry));
        let result = dump(&source).await;

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

        let source = NodeRegistryDumpSource::Native(Arc::clone(&registry));
        let result = dump(&source).await;

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

        let source = NodeRegistryDumpSource::Native(Arc::clone(&registry));
        let result = dump(&source).await;

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

    /// 🔴 Scheduler-proxy mode: same output shape, sourced from a live RPC
    /// instead of a local registry — the property task 4 is about (the
    /// endpoint stays usable, in the same shape, under the default switch
    /// position too).
    #[tokio::test]
    async fn scheduler_proxy_dump_has_the_same_shape_as_native() {
        use std::net::SocketAddr;

        use tokio::sync::oneshot;
        use tonic::{Request, Response, Status};

        use crate::proto::scheduler::scheduler_server::{Scheduler, SchedulerServer};

        struct FakeScheduler;

        #[tonic::async_trait]
        impl Scheduler for FakeScheduler {
            async fn list_nodes(
                &self,
                _r: Request<scheduler::ListNodesRequest>,
            ) -> Result<Response<scheduler::ListNodesResponse>, Status> {
                Ok(Response::new(scheduler::ListNodesResponse {
                    nodes: vec![scheduler::Node {
                        node_id: "node-a".to_string(),
                        endpoint: "http://10.0.0.7:8000".to_string(),
                    }],
                }))
            }
            async fn list_observed_nodes(
                &self,
                _r: Request<scheduler::ListObservedNodesRequest>,
            ) -> Result<Response<scheduler::ListObservedNodesResponse>, Status> {
                Ok(Response::new(scheduler::ListObservedNodesResponse {
                    nodes: vec![scheduler::ObservedNode {
                        node_id: "node-a".to_string(),
                        endpoint: "http://10.0.0.7:8000".to_string(),
                        cluster_id: "cluster-a".to_string(),
                        ..Default::default()
                    }],
                }))
            }
            async fn heartbeat(
                &self,
                _r: Request<scheduler::HeartbeatRequest>,
            ) -> Result<Response<scheduler::HeartbeatResponse>, Status> {
                Err(Status::unimplemented("not used by this test"))
            }
            async fn schedule(
                &self,
                _r: Request<scheduler::ScheduleRequest>,
            ) -> Result<Response<scheduler::ScheduleResponse>, Status> {
                Err(Status::unimplemented("not used by this test"))
            }
            async fn lookup_node(
                &self,
                _r: Request<scheduler::LookupNodeRequest>,
            ) -> Result<Response<scheduler::LookupNodeResponse>, Status> {
                Err(Status::unimplemented("not used by this test"))
            }
            async fn record_assignment(
                &self,
                _r: Request<scheduler::RecordAssignmentRequest>,
            ) -> Result<Response<scheduler::RecordAssignmentResponse>, Status> {
                Err(Status::unimplemented("not used by this test"))
            }
            async fn report_sandbox_event(
                &self,
                _r: Request<scheduler::ReportSandboxEventRequest>,
            ) -> Result<Response<scheduler::ReportSandboxEventResponse>, Status> {
                Err(Status::unimplemented("not used by this test"))
            }
            async fn list_p2p_peers(
                &self,
                _r: Request<scheduler::ListP2pPeersRequest>,
            ) -> Result<Response<scheduler::ListP2pPeersResponse>, Status> {
                Err(Status::unimplemented("not used by this test"))
            }
            async fn record_p2p_artifact(
                &self,
                _r: Request<scheduler::RecordP2pArtifactRequest>,
            ) -> Result<Response<scheduler::RecordP2pArtifactResponse>, Status> {
                Err(Status::unimplemented("not used by this test"))
            }
            async fn forget_p2p_artifact(
                &self,
                _r: Request<scheduler::ForgetP2pArtifactRequest>,
            ) -> Result<Response<scheduler::ForgetP2pArtifactResponse>, Status> {
                Err(Status::unimplemented("not used by this test"))
            }
            async fn lookup_p2p_artifact(
                &self,
                _r: Request<scheduler::LookupP2pArtifactRequest>,
            ) -> Result<Response<scheduler::LookupP2pArtifactResponse>, Status> {
                Err(Status::unimplemented("not used by this test"))
            }
            async fn get_node(
                &self,
                _r: Request<scheduler::GetNodeRequest>,
            ) -> Result<Response<scheduler::GetNodeResponse>, Status> {
                Err(Status::unimplemented("not used by this test"))
            }
            async fn unregister_node(
                &self,
                _r: Request<scheduler::UnregisterNodeRequest>,
            ) -> Result<Response<scheduler::UnregisterNodeResponse>, Status> {
                Err(Status::unimplemented("not used by this test"))
            }
            async fn list_registry_sandboxes(
                &self,
                _r: Request<scheduler::ListRegistrySandboxesRequest>,
            ) -> Result<Response<scheduler::ListRegistrySandboxesResponse>, Status> {
                Err(Status::unimplemented("not used by this test"))
            }
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind a port");
        let addr: SocketAddr = listener.local_addr().expect("bound address");
        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(SchedulerServer::new(FakeScheduler))
                .serve_with_incoming_shutdown(
                    tonic::transport::server::TcpIncoming::from(listener),
                    async {
                        let _ = rx.await;
                    },
                )
                .await;
        });
        for _ in 0..200 {
            if tokio::net::TcpStream::connect(addr).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
            .expect("valid endpoint")
            .connect()
            .await
            .expect("connect");

        let source = NodeRegistryDumpSource::SchedulerProxy(channel);
        let result = dump(&source).await;
        let _ = tx.send(());

        assert_eq!(result.source, "scheduler-proxy");
        assert_eq!(result.nodes.len(), 1);
        assert_eq!(result.nodes[0].node_id, "node-a");
        assert_eq!(result.observed.len(), 1);
        assert_eq!(result.observed[0].node_id, "node-a");
        assert!(
            result.error.is_none(),
            "unexpected error: {:?}",
            result.error
        );
    }
}
