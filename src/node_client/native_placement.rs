//! [`NativeNodePlacement`]: the `Native` half of `[cluster].node_placement_source`
//! (`docs/proposals/_sd-phase4-stageA-node-inventory.md` §5 — task's own "D7").
//!
//! # 🔴 P1 (task's own "phase4-close"): all five methods now go local
//!
//! Every method answers from api's own process: `resolve_node` and
//! `node_membership` from api's own `src/node_registry` node registry
//! (`crate::node_registry::registry`) — both are, on the wire, the same
//! question `GetNode` answers, and `GetNode` is squarely Stage A's
//! (`node_registry.go`'s `GetObserved`, ported to
//! [`crate::node_registry::registry::AtomicNodeRegistry::get_observed`]).
//!
//! `place_new`, `place_existing`, and `record_placement` now answer from
//! [`crate::node_registry::grpc_service::NodeRegistryGrpcService`] — the
//! exact same service object `assemble_api` hands `serve_on` to answer
//! `Schedule`/`LookupNode`/`RecordAssignment` over the wire for any other
//! caller (a query-only replica, a debugging `grpcurl`, ...) — called
//! in-process as plain async functions rather than dialled: no socket, no
//! serialization, and — the point of routing through that type rather than
//! re-deriving its answers here — zero duplicated selection/lookup logic.
//! `NodeRegistryGrpcService::schedule`/`lookup_node`/`record_assignment`
//! are themselves thin translations over
//! [`crate::binding_store::lookup::select_node`]/
//! [`crate::binding_store::lookup::lookup_node`] (`Schedule`'s placement
//! decision and `LookupNode`'s three-step binding → roster → registry
//! ladder) plus `record_assignment`'s own field validation/normalization —
//! see that module's own doc for why the three-stage lookup ladder is kept
//! there rather than folded into the gRPC method bodies. This is the same
//! reuse `docs/proposals/_sd-phase4-stageA-node-inventory.md` §5
//! recommended for the two discovery-only methods, extended to the three
//! placement-deciding ones now that Stage D exists to answer them.
//!
//! 🔴 Before this, `place_new`/`place_existing`/`record_placement` forwarded
//! unconditionally to an inner `SchedulerNodePlacement` — meaning `--role
//! api` under `Native` was simultaneously the gRPC *server* for
//! `Schedule`/`LookupNode`/`RecordAssignment` (Stage D) and, for placement
//! decisions, still a *client* of the Go scheduler for those same three
//! calls, with nothing in the process ever answering its own server. A Go
//! scheduler scaled to zero left every create failing even though the
//! exact logic needed to place it was already running, unreached, in the
//! same process. See `cluster_placement`'s own doc comment in
//! `src/bin/aenv-api.rs` for why `[cluster].scheduler_endpoint` is no longer
//! required at all once `[cluster].node_placement_source = "native"`.
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
//! quirk to fix later, but the exact reason `warmup.rs` exists.
//!
//! 🔴 A second, narrower version of the same bug survived Stage A and was
//! only caught in the P1 pass this module doc now describes:
//! [`WarmupGate::warmed_up`] used to latch warm at its wall-clock deadline
//! *regardless* of whether any node had ever reported at all — so a
//! registry that was simply cold (a freshly (re)started replica, or every
//! replica of a rolling DaemonSet restart at once) still turned every
//! `node_membership` answer into a confident `Gone` once the deadline
//! passed, deleting live sandboxes' records. See [`WarmupGate::warmed_up`]'s
//! own doc comment and this module's
//! `node_membership_stays_cold_past_the_deadline_when_nothing_has_ever_reported`
//! test for the fix and its proof.

use std::sync::Arc;
use std::time::SystemTime;

use anyhow::{anyhow, bail, Result};
use async_trait::async_trait;
use tonic::Code;

use crate::node_registry::grpc_service::NodeRegistryGrpcService;
use crate::node_registry::registry::{AtomicNodeRegistry, NodeRegistry};
use crate::node_registry::warmup::WarmupGate;
use crate::proto::scheduler::scheduler_server::Scheduler;
use crate::proto::scheduler::{self, ObservedNode};
use crate::types::{ExecutionId, SandboxId, SandboxResources};

use super::placement::{NodeEndpoint, NodeMembership, NodePlacement};
use super::scheduler_placement::rewrite_port;

/// Placement backed entirely by api's own process — see the module doc for
/// why the split that used to exist here between "answers locally" and
/// "forwards to the scheduler" is gone.
pub struct NativeNodePlacement {
    registry: Arc<AtomicNodeRegistry>,
    /// The port the node sandbox service listens on — same role as
    /// [`SchedulerNodePlacement`]'s own field of the same name, applied to
    /// the address the local registry (or the local scheduler surface)
    /// answers with.
    node_service_port: u16,
    /// Gates whether a registry miss may be answered as absence at all — see
    /// the module doc's "Why `resolve_node` and `node_membership` share one
    /// registry call" section. Shared with the heartbeat-receiving gRPC
    /// service (`start_native_node_registry` builds one `Arc` and hands it
    /// to both), so a heartbeat this process actually receives opens the
    /// gate for placement lookups too, not just for its own consumer.
    warmup: Arc<WarmupGate>,
    /// Answers `place_new`/`place_existing`/`record_placement` — see the
    /// module doc. A clone of the same service `spawn_grpc_surface` serves
    /// the real `Scheduler` RPCs from (cheap: every field behind it is an
    /// `Arc` or `Copy`, per `NodeRegistryGrpcService`'s own `Clone` doc),
    /// not a second instance with its own state.
    local: NodeRegistryGrpcService,
}

impl NativeNodePlacement {
    pub fn new(
        registry: Arc<AtomicNodeRegistry>,
        node_service_port: u16,
        warmup: Arc<WarmupGate>,
        local: NodeRegistryGrpcService,
    ) -> Self {
        Self {
            registry,
            node_service_port,
            warmup,
            local,
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

    /// [`Self::node_service_endpoint`]'s counterpart for the wire
    /// [`scheduler::Node`] shape `schedule`/`lookup_node` answer with,
    /// rather than the [`ObservedNode`] `get_observed` answers with —
    /// mirrors [`SchedulerNodePlacement::node_service_endpoint`] exactly,
    /// including its two refusals, worded to name the local scheduler
    /// surface rather than a remote one so an operator reading logs can
    /// tell the two apart.
    fn node_service_endpoint_from_wire(
        &self,
        node: Option<scheduler::Node>,
    ) -> Result<NodeEndpoint> {
        let node = node.ok_or_else(|| anyhow!("the local scheduler named no node"))?;
        if node.node_id.is_empty() {
            bail!("the local scheduler named a node with no id");
        }
        if node.endpoint.is_empty() {
            bail!(
                "the local scheduler named node {} with no address",
                node.node_id
            );
        }
        Ok(NodeEndpoint {
            endpoint: rewrite_port(&node.endpoint, self.node_service_port)?,
            advertised_endpoint: node.endpoint,
            node_id: node.node_id,
        })
    }
}

#[async_trait]
impl NodePlacement for NativeNodePlacement {
    /// Ports `SchedulerNodePlacement::place_new`'s call, in-process: the same
    /// `NewSandboxHint` (never `NewColdSandboxHint`, for the same reason —
    /// see that method's own doc) against
    /// [`NodeRegistryGrpcService::schedule`] instead of a dialled
    /// `Schedule` RPC.
    async fn place_new(
        &self,
        _sandbox_id: SandboxId,
        _resources: SandboxResources,
    ) -> Result<NodeEndpoint> {
        let response = self
            .local
            .schedule(tonic::Request::new(scheduler::ScheduleRequest {
                hint: Some(scheduler::ScheduleRequestHint {
                    kind: Some(scheduler::schedule_request_hint::Kind::NewSandbox(
                        scheduler::NewSandboxHint {
                            metadata: Default::default(),
                        },
                    )),
                }),
            }))
            .await
            .map_err(|status| anyhow!("the local scheduler refused to place a sandbox: {status}"))?
            .into_inner();
        self.node_service_endpoint_from_wire(response.node)
    }

    /// Ports `SchedulerNodePlacement::place_existing`'s call, in-process,
    /// against [`NodeRegistryGrpcService::lookup_node`] — including the same
    /// `NotFound`-is-`Ok(None)`, everything-else-is-`Err` split (see that
    /// method's own doc comment for why the two are not interchangeable).
    async fn place_existing(&self, sandbox_id: SandboxId) -> Result<Option<NodeEndpoint>> {
        let response = match self
            .local
            .lookup_node(tonic::Request::new(scheduler::LookupNodeRequest {
                sandbox_id: sandbox_id.to_string(),
            }))
            .await
        {
            Ok(response) => response.into_inner(),
            Err(status) if status.code() == Code::NotFound => return Ok(None),
            Err(status) => bail!("the local scheduler could not locate {sandbox_id}: {status}"),
        };
        self.node_service_endpoint_from_wire(response.node)
            .map(Some)
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

    /// Ports `SchedulerNodePlacement::record_placement`'s call, in-process,
    /// against [`NodeRegistryGrpcService::record_assignment`] — the same
    /// wire fields (the node's *advertised* address, never the rewritten
    /// node-service one; a zero `projection_ttl_secs`, read by the local
    /// service as "use the node's own `binding_ttl`") that call already
    /// sends, and the same discovery-backed node validation
    /// `record_assignment` already runs.
    async fn record_placement(
        &self,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
        node: &NodeEndpoint,
    ) -> Result<()> {
        self.local
            .record_assignment(tonic::Request::new(scheduler::RecordAssignmentRequest {
                sandbox_id: sandbox_id.to_string(),
                node: Some(scheduler::Node {
                    node_id: node.node_id.clone(),
                    endpoint: node.advertised_endpoint.clone(),
                }),
                execution_id: execution_id.to_string(),
                projection_ttl_secs: 0,
            }))
            .await
            .map_err(|status| {
                anyhow!("the local scheduler refused an assignment for {sandbox_id}: {status}")
            })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use crate::binding_store::{Binding, BindingStore, BindingStoreSettings, InMemoryBindingStore};
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
        let local = NodeRegistryGrpcService::new(Arc::clone(&registry), Arc::clone(&warmup));
        NativeNodePlacement::new(registry, 8001, warmup, local)
    }

    /// [`placement`], but with a binding store wired onto the local
    /// service — needed by every test that exercises `place_new`/
    /// `place_existing`/`record_placement`, since `LookupNode`/
    /// `RecordAssignment` answer `Unimplemented` without one (see
    /// `NodeRegistryGrpcService::lookup_node`'s own doc).
    fn placement_with_binding_store(
        registry: Arc<AtomicNodeRegistry>,
        binding_store: Arc<dyn BindingStore>,
    ) -> NativeNodePlacement {
        let warmup = warm_gate();
        let local = NodeRegistryGrpcService::new(Arc::clone(&registry), Arc::clone(&warmup))
            .with_binding_store(binding_store, false, Duration::ZERO);
        NativeNodePlacement::new(registry, 8001, warmup, local)
    }

    /// A gate that is already warm regardless of when `warmed_up` is called
    /// against it: `reported_in` is called once, immediately, against a
    /// deadline already long past (anchored at the Unix epoch, which every
    /// `SystemTime::now()` this process will ever observe is already past),
    /// which latches `warm` for good — mirrors
    /// `src/node_registry/grpc_service.rs`'s own `warm_gate` helper
    /// (including its own note on why `reported_in` has to be called
    /// explicitly since P1: an unreported cold registry no longer opens on
    /// wall-clock deadline alone, see `WarmupGate::warmed_up`'s own doc).
    fn warm_gate() -> Arc<WarmupGate> {
        let registry = Arc::new(AtomicNodeRegistry::new(Vec::new(), Duration::from_secs(30)))
            as Arc<dyn crate::node_registry::registry::NodeRegistry>;
        let gate = Arc::new(WarmupGate::new(
            registry,
            Duration::from_secs(1),
            SystemTime::UNIX_EPOCH,
        ));
        gate.reported_in(SystemTime::now());
        gate
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

    fn node(id: &str, endpoint: &str) -> Node {
        Node {
            id: id.to_string(),
            endpoint: endpoint.to_string(),
            pod_name: String::new(),
        }
    }

    /// 🔴 P1: `place_new` answers from the local strategy/registry, round-
    /// robining over the discovered nodes exactly as `Schedule` does over
    /// the wire — proving it never reaches for a scheduler at all (there is
    /// no scheduler endpoint anywhere in this test's setup for it to
    /// reach).
    #[tokio::test]
    async fn place_new_answers_locally_and_round_robins() {
        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![
                node("node-a", "http://10.0.0.1:8000"),
                node("node-b", "http://10.0.0.2:8000"),
            ],
            Duration::from_secs(30),
        ));
        let placement = placement(Arc::clone(&registry));

        let mut seen = Vec::new();
        for _ in 0..2 {
            let endpoint = placement
                .place_new(SandboxId::new(), SandboxResources::default())
                .await
                .expect("two discovered nodes, nothing else wired");
            seen.push(endpoint.node_id);
        }
        seen.sort();
        assert_eq!(
            seen,
            vec!["node-a".to_string(), "node-b".to_string()],
            "round-robin over two calls must visit both nodes exactly once"
        );
    }

    /// 🔴 P1: `place_new` with no discovered nodes at all must refuse
    /// rather than hang waiting on an unreachable scheduler -- the direct
    /// analogue of `NodeRegistryGrpcService`'s own
    /// `schedule_returns_unavailable_when_no_nodes_are_discovered`.
    #[tokio::test]
    async fn place_new_refuses_when_no_nodes_are_discovered() {
        let registry = Arc::new(AtomicNodeRegistry::new(Vec::new(), Duration::from_secs(30)));
        let placement = placement(registry);

        let err = placement
            .place_new(SandboxId::new(), SandboxResources::default())
            .await
            .expect_err("nothing discovered");
        assert!(err.to_string().contains("no nodes available"), "{err}");
    }

    /// 🔴 P1: `record_placement` writes into the local binding store, and
    /// `place_existing` reads the same write straight back out -- both
    /// in-process, both through `NodeRegistryGrpcService`. This is the
    /// exact round trip a create makes: place, then record, then (on a
    /// later request) look the same sandbox back up.
    #[tokio::test]
    async fn record_placement_then_place_existing_round_trips_through_the_local_binding_store() {
        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![node("node-a", "http://10.0.0.7:8000")],
            Duration::from_secs(30),
        ));
        let binding_store: Arc<dyn BindingStore> =
            Arc::new(InMemoryBindingStore::new(BindingStoreSettings::default()));
        let placement =
            placement_with_binding_store(Arc::clone(&registry), Arc::clone(&binding_store));
        let sandbox_id = SandboxId::new();
        let execution_id = ExecutionId::new();

        // The node has to be resolvable through discovery for
        // `record_assignment` to accept it -- the endpoint here is what
        // `place_new`/`resolve_node` would have handed back.
        let node_endpoint = NodeEndpoint {
            node_id: "node-a".to_string(),
            endpoint: "http://10.0.0.7:8001/".to_string(),
            advertised_endpoint: "http://10.0.0.7:8000".to_string(),
        };
        placement
            .record_placement(sandbox_id, execution_id, &node_endpoint)
            .await
            .expect("node-a is a known node");

        let resolved = placement
            .place_existing(sandbox_id)
            .await
            .expect("no transport error")
            .expect("the binding this test just recorded");
        assert_eq!(resolved.node_id, "node-a");
        assert_eq!(resolved.endpoint, "http://10.0.0.7:8001/");

        // And the control: a sandbox nothing was ever recorded for answers
        // `Ok(None)`, not an error -- proving the round trip above actually
        // depends on the write, not on `place_existing` always answering
        // `Some`.
        let absent = placement
            .place_existing(SandboxId::new())
            .await
            .expect("a clean miss is not a transport error");
        assert!(absent.is_none(), "nothing was ever recorded for this id");
    }

    /// 🔴 P1's own control against a mutation that would collapse
    /// `place_existing`'s `Ok(None)` and `Err` cases into one: a binding
    /// store that always fails must surface as `Err`, never as `Ok(None)` —
    /// the exact distinction `NodePlacement::place_existing`'s own doc
    /// comment says callers rely on.
    #[tokio::test]
    async fn place_existing_reports_a_binding_store_failure_as_an_error_not_a_clean_miss() {
        use crate::binding_store::{BindingDeleteOutcome, BindingStoreError};
        use crate::node_registry::types::RosterEntry;

        struct FailingBindingStore;
        #[async_trait]
        impl BindingStore for FailingBindingStore {
            async fn get(
                &self,
                _sandbox_id: &str,
                _now: SystemTime,
            ) -> Result<Option<Binding>, BindingStoreError> {
                Err(BindingStoreError::new("binding store is down"))
            }
            async fn record(
                &self,
                _sandbox_id: &str,
                _binding: Binding,
                _now: SystemTime,
            ) -> Result<crate::binding_store::BindingDecision, BindingStoreError> {
                unreachable!("this test never records")
            }
            async fn reconcile_node(
                &self,
                _node: Node,
                _roster: Vec<RosterEntry>,
                _now: SystemTime,
            ) -> Result<Vec<(String, crate::binding_store::BindingDecision)>, BindingStoreError>
            {
                unreachable!("this test never reconciles")
            }
            async fn delete(
                &self,
                _sandbox_id: &str,
                _execution_id: &str,
                _now: SystemTime,
            ) -> Result<BindingDeleteOutcome, BindingStoreError> {
                unreachable!("this test never deletes")
            }
        }

        let registry = Arc::new(AtomicNodeRegistry::new(Vec::new(), Duration::from_secs(30)));
        let placement =
            placement_with_binding_store(Arc::clone(&registry), Arc::new(FailingBindingStore));

        let err = placement
            .place_existing(SandboxId::new())
            .await
            .expect_err("a binding store failure must not read as a clean miss");
        assert!(err.to_string().contains("could not locate"), "{err}");
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

    /// The deadline half of the same coverage, updated for P1's fix to
    /// `WarmupGate::warmed_up`: the gate must still open at its deadline
    /// for a straggler once *something* has reported, but — unlike before
    /// P1 — must not open at all if nothing ever has. See
    /// `node_membership_stays_cold_past_the_deadline_when_nothing_has_ever_reported`
    /// below for the second half.
    #[tokio::test]
    async fn node_membership_opens_at_the_warmup_deadline_for_a_silent_straggler() {
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
        let start = SystemTime::now();
        let warmup = Arc::new(WarmupGate::new(
            Arc::clone(&registry) as Arc<dyn crate::node_registry::registry::NodeRegistry>,
            Duration::from_secs(15),
            start,
        ));
        let placement = placement_with_warmup(Arc::clone(&registry), Arc::clone(&warmup));

        // node-a reports; node-b never does for the rest of this test.
        registry
            .heartbeat(&heartbeat("node-a"), start)
            .expect("node-a is in discovery");
        warmup.reported_in(start);

        placement
            .node_membership("node-b")
            .await
            .expect_err("node-b has never heartbeated and the deadline has not passed");

        // The deadline boundary itself, checked directly against the gate
        // with a synthetic `now` (mirrors `warmup.rs`'s own
        // `opens_at_the_deadline_for_a_silent_straggler_once_something_has_reported`):
        // `node_membership` always calls `warmed_up` with a real
        // `SystemTime::now()`, so exercising the boundary through the
        // placement itself would require actually waiting real wall-clock
        // time.
        assert!(
            !warmup.warmed_up(start + Duration::from_secs(14)),
            "the gate must still be shut one second before its deadline, even with node-a \
             already reported"
        );
        assert!(
            warmup.warmed_up(start + Duration::from_secs(15)),
            "the gate must open at its deadline even with node-b still silent, now that \
             node-a has reported at least once"
        );
        // `warm` is a one-way latch (`WarmupGate::warmed_up`'s own doc), so
        // the direct check just above already flipped it for good --
        // `node_membership`'s own internal (real) `SystemTime::now()` call
        // now short-circuits on that latch regardless of the actual clock.
        assert_eq!(
            placement.node_membership("node-b").await.unwrap(),
            NodeMembership::Gone,
            "the gate is now latched warm, so node-b -- still silent -- must read as Gone"
        );
    }

    /// 🔴 P1: the actual consumer-level proof of the bug fix — a registry
    /// that has never received a single heartbeat must keep refusing
    /// `node_membership`, arbitrarily far past its deadline, rather than
    /// eventually asserting every node `Gone`. This is
    /// `warmup.rs`'s own `stays_cold_past_the_deadline_when_nothing_has_ever_reported`,
    /// proven through the real consumer the bug actually reached: before
    /// the fix, this test would have failed at the final assertion, with
    /// `node_membership("node-a")` answering `Gone` for a node that has
    /// simply not had the chance to report to a freshly (re)started
    /// replica yet.
    #[tokio::test]
    async fn node_membership_stays_cold_past_the_deadline_when_nothing_has_ever_reported() {
        let registry = Arc::new(AtomicNodeRegistry::new(
            vec![Node {
                id: "node-a".to_string(),
                endpoint: "http://10.0.0.7:8000".to_string(),
                pod_name: String::new(),
            }],
            Duration::from_secs(30),
        ));
        // The deadline is anchored at the Unix epoch -- already long past by
        // any `SystemTime::now()` this test (or `node_membership`'s own
        // internal one) will ever observe, the same trick `warm_gate` above
        // uses -- but *without* ever calling `reported_in`. The point is to
        // exercise the deadline genuinely passing, through
        // `node_membership`'s own real-clock `warmed_up` call, with zero
        // heartbeats ever received: before the fix, this alone was enough
        // to latch `warm` and answer `Gone`.
        let warmup = Arc::new(WarmupGate::new(
            Arc::clone(&registry) as Arc<dyn crate::node_registry::registry::NodeRegistry>,
            Duration::from_secs(1),
            SystemTime::UNIX_EPOCH,
        ));
        let placement = placement_with_warmup(Arc::clone(&registry), Arc::clone(&warmup));

        placement.node_membership("node-a").await.expect_err(
            "the deadline (anchored at the Unix epoch) has long since passed, but nothing has \
             ever reported -- this must still refuse rather than answer Gone",
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
