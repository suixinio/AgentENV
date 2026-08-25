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
//! heartbeat has no `observed` record yet — but whether that reads as
//! [`NodeMembership::Gone`] (or a bare refusal, for `resolve_node`) depends
//! on [`WarmupGate::warmed_up`]: a miss while the gate is cold is a question
//! this registry cannot yet answer honestly (it has not heard from every
//! node discovery already knows about, or the deadline has not passed) and
//! is refused with an `Err` rather than asserted as absence — see the field's
//! own doc comment and `docs/proposals/_sd-phase4-stageA-node-inventory.md`
//! §5's "D2" instruction: byte-for-byte once the registry is warm, which is
//! the steady state both `docs/proposals/_sd-phase4-stageA-node-inventory.md`
//! and `services/scheduler/internal/warmup.go`'s `TestLookupWithholdsNotFoundWhileCold`
//! describe — never before it.
//!
//! 🔴 This was originally byte-for-byte "no observed record is `Gone`,"
//! full stop, with no warm-up distinction at all — a straight, unguarded
//! port of `GetObserved`. Independent review of Stage A caught it: a
//! process's own registry starts *empty*, and every node is `Gone` by that
//! definition until the first heartbeat round finishes, however long that
//! takes on a large or slow-starting cluster — which is a state the real
//! scheduler-backed `SchedulerNodePlacement` never has (a scheduler that
//! cannot answer the question errors instead, `Err`, never `Gone`; see
//! `SchedulerNodePlacement::node_membership`'s own doc). `stub.rs`'s
//! contract for `node_membership` acts on `Gone` alone and treats it as
//! proof a sandbox's runtime is gone for good — so an api replica that had
//! just restarted, or a `Native` deployment missing
//! `dual_report_api_endpoint` outright (see `start_native_node_registry`'s
//! own refusal), would confirm every live sandbox as gone the moment
//! anything dialled it. [`WarmupGate`] is what closes that: not a Stage A
//! quirk to fix later, but the exact reason `warmup.rs` exists and was
//! already built, unconsumed, before this file was.

use std::sync::Arc;
use std::time::SystemTime;

use anyhow::{bail, Result};
use async_trait::async_trait;

use crate::node_registry::registry::{AtomicNodeRegistry, NodeRegistry};
use crate::node_registry::warmup::WarmupGate;
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
    /// Gates whether a registry miss may be answered as absence at all — see
    /// the module doc's "Why `resolve_node` and `node_membership` share one
    /// registry call" section. Shared with the heartbeat-receiving gRPC
    /// service (`start_native_node_registry` builds one `Arc` and hands it
    /// to both), so a heartbeat this process actually receives opens the
    /// gate for placement lookups too, not just for its own consumer.
    warmup: Arc<WarmupGate>,
    fallback: SchedulerNodePlacement,
}

impl NativeNodePlacement {
    pub fn new(
        registry: Arc<AtomicNodeRegistry>,
        node_service_port: u16,
        warmup: Arc<WarmupGate>,
        fallback: SchedulerNodePlacement,
    ) -> Self {
        Self {
            registry,
            node_service_port,
            warmup,
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
    ///
    /// A miss is always an `Err` here (unlike `node_membership`, which has a
    /// confident affirmative answer — `Gone` — to withhold): `resolve_node`
    /// never had one to give in the first place, so the [`WarmupGate`] only
    /// changes *why* the call failed, in the message, not whether it did.
    /// See the module doc's warm-up section.
    async fn resolve_node(&self, node_id: &str) -> Result<NodeEndpoint> {
        let Some(observed) = self.registry.get_observed(node_id, "", SystemTime::now()) else {
            if self.warmup.warmed_up(SystemTime::now()) {
                bail!("the node registry has no observed record for node {node_id}");
            }
            bail!(
                "the node registry has not finished warming up (not every node discovery \
                 already knows about has heartbeated yet) and cannot yet say whether node \
                 {node_id} has an observed record"
            );
        };
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
    /// scheduler's own `GetNode`-backed `node_membership` answers today,
    /// *once the registry is warm* — see the module doc's warm-up section
    /// and [`WarmupGate`]'s own doc comment for why a cold miss is refused
    /// (`Err`) rather than answered `Gone`.
    async fn node_membership(&self, node_id: &str) -> Result<NodeMembership> {
        match self.registry.get_observed(node_id, "", SystemTime::now()) {
            Some(_) => Ok(NodeMembership::Present),
            None if self.warmup.warmed_up(SystemTime::now()) => Ok(NodeMembership::Gone),
            None => bail!(
                "the node registry has not finished warming up (not every node discovery \
                 already knows about has heartbeated yet) and cannot yet say whether node \
                 {node_id} is still part of the cluster"
            ),
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
        placement_with_warmup(registry, warm_gate())
    }

    fn placement_with_warmup(
        registry: Arc<AtomicNodeRegistry>,
        warmup: Arc<WarmupGate>,
    ) -> NativeNodePlacement {
        // `connect_lazy` opens no socket — the fallback is unreachable in
        // these tests, which is fine: none of them exercise it.
        let fallback = SchedulerNodePlacement::connect_lazy("http://127.0.0.1:1", 8001).unwrap();
        NativeNodePlacement::new(registry, 8001, warmup, fallback)
    }

    /// A gate that is already warm regardless of when `warmed_up` is called
    /// against it, for every test in this file except the ones about the
    /// gate itself: its deadline is anchored at the Unix epoch, which every
    /// `SystemTime::now()` this process will ever observe is already past.
    fn warm_gate() -> Arc<WarmupGate> {
        Arc::new(WarmupGate::new(
            Arc::new(AtomicNodeRegistry::new(Vec::new(), Duration::from_secs(30)))
                as Arc<dyn crate::node_registry::registry::NodeRegistry>,
            Duration::from_secs(1),
            SystemTime::UNIX_EPOCH,
        ))
    }

    /// A gate that stays cold until its deadline passes, gated on `registry`
    /// — the same registry `placement_with_warmup` is given, so a heartbeat
    /// fed to one is visible to the other.
    fn cold_gate(registry: Arc<AtomicNodeRegistry>, now: SystemTime) -> Arc<WarmupGate> {
        Arc::new(WarmupGate::new(
            registry as Arc<dyn crate::node_registry::registry::NodeRegistry>,
            Duration::from_secs(60),
            now,
        ))
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

    /// 🔴 P1: the Rust equivalent of `warmup_test.go`'s
    /// `TestLookupWithholdsNotFoundWhileCold` — "the whole point" of the
    /// gate, per that test's own Go comment — applied to the actual Rust
    /// consumer wired to it, `NativeNodePlacement::node_membership`, since
    /// Go's own test drives `Service.LookupNode`, which needs a
    /// `BindingStore` this codebase has not ported yet (see `warmup.rs`'s
    /// module doc). Before this test existed, this exact case had zero
    /// coverage: `node_membership_is_present_only_once_heartbeated` above
    /// only ever exercises a warm gate.
    ///
    /// A registry with a known node that has never heartbeated (`node-b`)
    /// must not answer `Gone` while the gate is cold — an answer that flows
    /// straight into `stub.rs`'s `RuntimeConfirmedGone`, and from there into
    /// the orchestrator reading a perfectly live sandbox's runtime as gone.
    /// It must refuse instead (`Err`), which is the one answer no caller can
    /// mistake for confirmed absence.
    #[tokio::test]
    async fn node_membership_withholds_gone_while_the_registry_is_cold() {
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
        let now = SystemTime::now();
        let warmup = cold_gate(Arc::clone(&registry), now);
        let placement = placement_with_warmup(Arc::clone(&registry), Arc::clone(&warmup));

        // node-b has never heartbeated and the gate has not opened (node-a,
        // the other node discovery knows about, has not reported either) —
        // this must refuse, not answer `Gone`.
        let err = placement
            .node_membership("node-b")
            .await
            .expect_err("a cold registry must not assert a node is gone");
        assert!(
            err.to_string().contains("has not finished warming up"),
            "{err}"
        );

        // node-a heartbeats, but node-b — also known to discovery — still
        // has not, so the gate stays shut per `stays_cold_until_every_known_node_reports`
        // in `warmup.rs`'s own tests.
        registry
            .heartbeat(&heartbeat("node-a"), now)
            .expect("node-a is in discovery");
        warmup.reported_in(now);
        placement
            .node_membership("node-b")
            .await
            .expect_err("node-b still has not reported; the gate is still cold");

        // Once every node discovery knows about has reported (node-b now
        // heartbeats too), the gate opens and the same question gets the
        // real answer.
        registry
            .heartbeat(&heartbeat("node-b"), now)
            .expect("node-b is in discovery");
        warmup.reported_in(now);
        assert_eq!(
            placement.node_membership("node-b").await.unwrap(),
            NodeMembership::Present,
            "node-b just heartbeated and the gate is now warm"
        );

        // And a node discovery never listed at all reads as `Gone` once
        // warm, same as the always-warm test above.
        assert_eq!(
            placement.node_membership("node-c").await.unwrap(),
            NodeMembership::Gone,
            "node-c is not discovered at all, and the gate is warm"
        );
    }

    /// The deadline half of the same coverage: a node discovery knows about
    /// that never reports at all (genuinely down, not merely slow) must not
    /// wedge `node_membership` shut forever — the gate's deadline opens it
    /// regardless, exactly as `warmup.rs`'s own `opens_at_the_deadline`
    /// proves for the gate in isolation.
    #[tokio::test]
    async fn node_membership_opens_at_the_warmup_deadline_even_with_a_silent_node() {
        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![Node {
                id: "node-a".to_string(),
                endpoint: "http://10.0.0.7:8000".to_string(),
                pod_name: String::new(),
            }],
            Duration::from_secs(30),
        ));
        let start = SystemTime::now();
        let warmup = Arc::new(WarmupGate::new(
            Arc::clone(&registry) as Arc<dyn crate::node_registry::registry::NodeRegistry>,
            Duration::from_secs(15),
            start,
        ));
        let placement = placement_with_warmup(Arc::clone(&registry), Arc::clone(&warmup));

        placement
            .node_membership("node-a")
            .await
            .expect_err("node-a has never heartbeated and the deadline has not passed");

        // `warmed_up` reads whatever `now` it is handed, and `node_membership`
        // always calls it with `SystemTime::now()` — so proving the deadline
        // opens the gate for a caller that supplies a past `now` directly
        // (mirroring `warmup.rs`'s own `opens_at_the_deadline`) is done
        // against the gate here, then checked through the placement above.
        assert!(
            !warmup.warmed_up(start + Duration::from_secs(14)),
            "the gate must still be shut one second before its deadline"
        );
        assert!(
            warmup.warmed_up(start + Duration::from_secs(15)),
            "the gate must open at its deadline even with node-a still silent"
        );
    }

    /// `resolve_node`'s cold-registry case: always an `Err`, warm or not —
    /// unlike `node_membership`, it never had a confident affirmative
    /// answer to withhold, so the [`WarmupGate`] can only change *why* the
    /// call failed. The control here is that the message says so: a caller
    /// reading logs during a fleet-wide restart should not be told a
    /// perfectly live node has "no observed record" when the truth is that
    /// this replica has not warmed up yet.
    #[tokio::test]
    async fn resolve_node_names_warm_up_rather_than_absence_while_cold() {
        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![Node {
                id: "node-a".to_string(),
                endpoint: "http://10.0.0.7:8000".to_string(),
                pod_name: String::new(),
            }],
            Duration::from_secs(30),
        ));
        let now = SystemTime::now();
        let warmup = cold_gate(Arc::clone(&registry), now);
        let placement = placement_with_warmup(Arc::clone(&registry), warmup);

        let err = placement
            .resolve_node("node-a")
            .await
            .expect_err("node-a has never heartbeated and the gate is cold");
        assert!(
            err.to_string().contains("has not finished warming up"),
            "{err}"
        );
        assert!(
            !err.to_string().contains("no observed record"),
            "a cold-registry refusal must not be worded like a confirmed absence: {err}"
        );
    }
}
