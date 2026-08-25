//! [`NativeNodePlacement`]: the `Native` half of `[cluster].node_placement_source`
//! (`docs/proposals/_sd-phase4-stageA-node-inventory.md` §5 — task's own "D7").
//!
//! # 🔴 Only two of five methods actually go local
//!
//! `resolve_node` and `node_membership` answer from api's own
//! `src/node_registry` node registry (`crate::node_registry::registry`) —
//! both are, on the wire, the same question `GetNode` answers, and `GetNode`
//! is squarely Stage A's (`node_registry.go`'s `GetObserved`, ported to
//! [`crate::node_registry::registry::AtomicNodeRegistry::get_observed`]).
//! `place_new`, `place_existing`, and `record_placement` all need the
//! binding store — `Schedule`'s placement decision needs it to avoid double
//! -booking a node mid-decision in the real Go implementation's `selectNode`,
//! `LookupNode`'s three-step lookup (binding → roster → registry) needs it
//! as its first and most authoritative step, and `RecordAssignment` writes
//! directly into it — and the binding store is Stage D's, not Stage A's
//! (`docs/proposals/_sd-phase4-stageA-node-inventory.md` §10). So this type
//! holds an inner [`SchedulerNodePlacement`] and delegates those three
//! methods to it unchanged, exactly as
//! `docs/proposals/_sd-phase4-stageA-node-inventory.md` §5 recommends. This
//! also means `Native` never leaves a sandbox unplaceable or unroutable for
//! a reason Stage A introduced: the two methods it does take over are
//! read-only lookups of a node's own identity/address, not sandbox routing
//! decisions.
//!
//! # Why `resolve_node` and `node_membership` share one registry call
//!
//! Both call [`AtomicNodeRegistry::get_observed`] with an empty cluster id
//! (matching [`SchedulerNodePlacement`]'s own "blank means do not filter"
//! convention on the same two methods) — mirroring the real Go `GetNode` RPC
//! handler, which both `resolve_node` and `node_membership` dial on the
//! scheduler side. A node discovery knows about but that has never sent a
//! heartbeat has no `observed` record yet and therefore reads as
//! [`NodeMembership::Gone`] here, the same as it would through the scheduler
//! today — this is not a Stage A quirk to fix, it is the behaviour Stage A
//! is required to reproduce byte-for-byte (task's own "D2": port faithfully,
//! do not improve known warts in the same change).

use std::sync::Arc;
use std::time::SystemTime;

use anyhow::{anyhow, bail, Result};
use async_trait::async_trait;

use crate::node_registry::registry::{AtomicNodeRegistry, NodeRegistry};
use crate::proto::scheduler::ObservedNode;
use crate::types::{ExecutionId, SandboxId, SandboxResources};

use super::placement::{NodeEndpoint, NodeMembership, NodePlacement};
use super::scheduler_placement::rewrite_port;
use super::SchedulerNodePlacement;

/// Placement backed by api's own node registry for `resolve_node`/
/// `node_membership`, and by an inner [`SchedulerNodePlacement`] for
/// everything else. See the module doc for why the split is drawn there.
pub struct NativeNodePlacement {
    registry: Arc<AtomicNodeRegistry>,
    /// The port the node sandbox service listens on — same role as
    /// [`SchedulerNodePlacement`]'s own field of the same name, applied to
    /// the address the local registry holds rather than one the scheduler
    /// answered with.
    node_service_port: u16,
    fallback: SchedulerNodePlacement,
}

impl NativeNodePlacement {
    pub fn new(
        registry: Arc<AtomicNodeRegistry>,
        node_service_port: u16,
        fallback: SchedulerNodePlacement,
    ) -> Self {
        Self {
            registry,
            node_service_port,
            fallback,
        }
    }

    /// Turns an [`ObservedNode`] into the node service's address — the same
    /// two refusals [`SchedulerNodePlacement::node_service_endpoint`] makes
    /// (no id, no address), against the local registry's answer instead of
    /// the scheduler's.
    fn node_service_endpoint(&self, observed: ObservedNode) -> Result<NodeEndpoint> {
        if observed.node_id.is_empty() {
            bail!("the node registry named a node with no id");
        }
        if observed.endpoint.is_empty() {
            bail!(
                "the node registry named node {} with no address",
                observed.node_id
            );
        }
        Ok(NodeEndpoint {
            endpoint: rewrite_port(&observed.endpoint, self.node_service_port)?,
            advertised_endpoint: observed.endpoint,
            node_id: observed.node_id,
        })
    }
}

#[async_trait]
impl NodePlacement for NativeNodePlacement {
    async fn place_new(
        &self,
        sandbox_id: SandboxId,
        resources: SandboxResources,
    ) -> Result<NodeEndpoint> {
        self.fallback.place_new(sandbox_id, resources).await
    }

    async fn place_existing(&self, sandbox_id: SandboxId) -> Result<Option<NodeEndpoint>> {
        self.fallback.place_existing(sandbox_id).await
    }

    /// Answers from the local registry's heartbeat-derived view — see the
    /// module doc for why this is `get_observed`, the same call
    /// `node_membership` makes, rather than the discovery-only `resolve`.
    async fn resolve_node(&self, node_id: &str) -> Result<NodeEndpoint> {
        let observed = self
            .registry
            .get_observed(node_id, "", SystemTime::now())
            .ok_or_else(|| {
                anyhow!("the node registry has no observed record for node {node_id}")
            })?;
        let resolved = self.node_service_endpoint(observed)?;
        // Mirrors `SchedulerNodePlacement::resolve_node`'s own check: the
        // answer has to be about the node that was asked for.
        if resolved.node_id != node_id {
            bail!(
                "asked the node registry where node {node_id} is and it answered about node {}",
                resolved.node_id
            );
        }
        Ok(resolved)
    }

    /// See the module doc: this is the same registry call `resolve_node`
    /// makes, not a discovery-only snapshot — reproducing exactly what the
    /// scheduler's own `GetNode`-backed `node_membership` answers today.
    async fn node_membership(&self, node_id: &str) -> Result<NodeMembership> {
        match self.registry.get_observed(node_id, "", SystemTime::now()) {
            Some(_) => Ok(NodeMembership::Present),
            None => Ok(NodeMembership::Gone),
        }
    }

    async fn record_placement(
        &self,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
        node: &NodeEndpoint,
    ) -> Result<()> {
        self.fallback
            .record_placement(sandbox_id, execution_id, node)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use crate::node_registry::registry::AtomicNodeRegistry;
    use crate::node_registry::types::Node;
    use crate::proto::scheduler::HeartbeatRequest;

    use super::*;

    fn placement(registry: Arc<AtomicNodeRegistry>) -> NativeNodePlacement {
        // `connect_lazy` opens no socket — the fallback is unreachable in
        // these tests, which is fine: none of them exercise it.
        let fallback = SchedulerNodePlacement::connect_lazy("http://127.0.0.1:1", 8001).unwrap();
        NativeNodePlacement::new(registry, 8001, fallback)
    }

    fn heartbeat(node_id: &str) -> HeartbeatRequest {
        HeartbeatRequest {
            node_id: node_id.to_string(),
            cluster_id: "cluster-a".to_string(),
            service_instance_id: "instance-a".to_string(),
            ..Default::default()
        }
    }

    /// 🔴 Both faces of `resolve_node`: a node the registry has actually
    /// heard from resolves with its port rewritten to the node service's;
    /// a node discovery has never heartbeated for is refused, not defaulted.
    /// The refusal case is the control — without it, a `NativeNodePlacement`
    /// that resolved everything to some placeholder endpoint would also pass
    /// the first half.
    #[tokio::test]
    async fn resolve_node_answers_from_the_local_registry_and_refuses_the_unheard_from() {
        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![Node {
                id: "node-a".to_string(),
                endpoint: "http://10.0.0.7:8000".to_string(),
                pod_name: String::new(),
            }],
            Duration::from_secs(30),
        ));
        registry
            .heartbeat(&heartbeat("node-a"), SystemTime::now())
            .expect("node-a is in discovery");

        let placement = placement(Arc::clone(&registry));

        let resolved = placement
            .resolve_node("node-a")
            .await
            .expect("node-a heartbeated");
        assert_eq!(resolved.node_id, "node-a");
        assert_eq!(resolved.endpoint, "http://10.0.0.7:8001/");
        assert_eq!(resolved.advertised_endpoint, "http://10.0.0.7:8000");

        // node-b is discovered (in the registry's `nodes_by_id`) implicitly
        // through `Set`, which `AtomicNodeRegistry::new` never called for
        // it — so this also covers "discovery has never heard of this node
        // at all", not merely "no heartbeat yet".
        let err = placement
            .resolve_node("node-b")
            .await
            .expect_err("node-b never heartbeated");
        assert!(err.to_string().contains("no observed record"), "{err}");
    }

    /// 🔴 `node_membership` is `Gone` for a node discovery has never heard
    /// from — the same "not yet observed reads as gone" behaviour
    /// `resolve_node`'s refusal above proves, checked here through the
    /// other method so a future change that fixes one without the other is
    /// caught. Both are compared against the same registry so the only
    /// variable between the two assertions is which node is asked about.
    #[tokio::test]
    async fn node_membership_is_present_only_once_heartbeated() {
        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![
                Node {
                    id: "node-a".to_string(),
                    endpoint: "http://10.0.0.7:8000".to_string(),
                    pod_name: String::new(),
                },
                Node {
                    id: "node-b".to_string(),
                    endpoint: "http://10.0.0.9:8000".to_string(),
                    pod_name: String::new(),
                },
            ],
            Duration::from_secs(30),
        ));
        registry
            .heartbeat(&heartbeat("node-a"), SystemTime::now())
            .expect("node-a is in discovery");

        let placement = placement(registry);

        assert_eq!(
            placement.node_membership("node-a").await.unwrap(),
            NodeMembership::Present,
            "node-a heartbeated and should read as present"
        );
        assert_eq!(
            placement.node_membership("node-b").await.unwrap(),
            NodeMembership::Gone,
            "node-b is discovered but has never heartbeated, and should not read as present"
        );
        assert_eq!(
            placement.node_membership("node-c").await.unwrap(),
            NodeMembership::Gone,
            "node-c is not discovered at all"
        );
    }
}
