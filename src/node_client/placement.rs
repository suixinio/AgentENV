//! Deciding which node a sandbox goes to.
//!
//! # 🔴 Two questions, not one
//!
//! Starting a fresh sandbox asks *which machine has room*. Bringing a paused
//! one back asks *where this sandbox may go*, and those are different questions
//! with different answers: a paused sandbox whose bytes are only on the disk of
//! the node that paused it can go to exactly one machine, and asking the first
//! question about it would place it somewhere its bytes are not.
//!
//! Both are answered by the cluster scheduler in the assembled system. The
//! trait is here so that the piece which drives sandboxes over the wire does
//! not also have to know how placement is decided.
//!
//! # 🔴 And one statement, which is not a question at all
//!
//! [`NodePlacement::record_placement`] tells the placement source where a
//! sandbox *went*. Until it existed, nothing in this process ever told the
//! cluster that — the gateway used to, by reading the node off the create it
//! had just routed, and it stopped being able to the day user-facing REST
//! started going to the API half instead (`forwardToRestUpstream`, which names
//! this half as the owner of the write it can no longer make). The cost of
//! leaving it unowned is a window after every create in which the cluster
//! cannot say where the new sandbox is, and every call that has to ask —
//! delete, pause, snapshot and resume once the handle is gone — fails inside
//! it.

use async_trait::async_trait;

use crate::types::{ExecutionId, SandboxId, SandboxResources};

/// One node this client can talk to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeEndpoint {
    pub node_id: String,
    /// The gRPC address of the node service, as a URI. This is what this client
    /// dials.
    pub endpoint: String,
    /// The address the placement source names this node by, kept exactly as it
    /// said it.
    ///
    /// 🔴 A second address rather than a second port, and the difference
    /// matters in one direction only. `endpoint` above is this string with the
    /// node service's port substituted in — a *derived* value, and a lossy one:
    /// the substitution parses and re-renders the URI, so `10.0.0.7:8000`
    /// becomes `http://10.0.0.7:8001/` and there is no way back to the original
    /// spelling. The scheduler compares the address on a `RecordAssignment`
    /// byte-for-byte against the one discovery holds for that node
    /// (`AtomicNodeRegistry.Contains`) and refuses an assignment naming
    /// anything else, so a write that sent the derived address would be
    /// rejected as *an unknown node* — which reads like a discovery problem and
    /// is not one.
    ///
    /// It is also the address the rest of the cluster routes user traffic to,
    /// which is what a binding is for.
    pub advertised_endpoint: String,
}

impl NodeEndpoint {
    /// A node whose two addresses are the same string.
    ///
    /// For a placement source that names nodes by the address they are dialled
    /// on, which is every source except the cluster scheduler.
    pub fn same_address(node_id: impl Into<String>, endpoint: impl Into<String>) -> Self {
        let endpoint = endpoint.into();
        Self {
            node_id: node_id.into(),
            advertised_endpoint: endpoint.clone(),
            endpoint,
        }
    }
}

/// Whether a node the caller already has an id for is still part of the
/// cluster, as the placement source's own node registry currently has it.
///
/// This is a different question from [`NodePlacement::place_existing`]'s, and
/// deliberately answered from a different table — see
/// [`NodePlacement::node_membership`]'s doc for why the two cannot be folded
/// together.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeMembership {
    /// The registry still lists this node — a fresh heartbeat, or a stale one
    /// that has not (yet) aged all the way out. The node may yet report back
    /// on its own.
    Present,
    /// The registry holds nothing under this id: explicitly unregistered
    /// (`UnregisterNode`), or dropped once discovery stopped listing it at
    /// all. Nothing on this node is coming back to report anything.
    Gone,
}

#[async_trait]
pub trait NodePlacement: Send + Sync + 'static {
    /// A node with room for a new sandbox.
    async fn place_new(
        &self,
        sandbox_id: SandboxId,
        resources: SandboxResources,
    ) -> anyhow::Result<NodeEndpoint>;

    /// The node a sandbox that already exists must be driven on.
    ///
    /// 🔴 Separate from [`place_new`](Self::place_new) because the answer may
    /// be pinned rather than preferred. A caller that used the other one here
    /// would send a resume to a machine that does not have the bytes.
    ///
    /// # 🔴 `Ok(None)` and `Err` are not the same absence
    ///
    /// `Ok(None)` is the placement source saying, from a complete read, that it
    /// holds no record of this sandbox. `Err` is every other outcome, including
    /// every one where the source could not be asked — and the two lead callers
    /// to opposite conclusions, which is why they are separate here rather than
    /// flattened into one error the way they used to be.
    ///
    /// The scheduler goes to some length to keep this distinction on the wire:
    /// `lookupAbsent` is the only place it produces `NOT_FOUND`, and it is
    /// reached only after the bindings, the heartbeat rosters and the paused
    /// registry have each been read and each answered "no row". Anything that
    /// could not be consulted comes back `Unavailable` instead. Collapsing the
    /// two here threw that away one layer above the wire.
    async fn place_existing(&self, sandbox_id: SandboxId) -> anyhow::Result<Option<NodeEndpoint>>;

    /// The current address of a node whose identity the caller already holds.
    ///
    /// 🔴 Not a placement decision, and it must never be used as one. This
    /// answers *where is node N*, which is a discovery question with one
    /// answer; the two methods above answer *which node*, which is a decision.
    /// A caller that has not already established which machine it is entitled
    /// to talk to has no business here.
    async fn resolve_node(&self, node_id: &str) -> anyhow::Result<NodeEndpoint>;

    /// Whether a node this caller already has an id for is still part of the
    /// cluster.
    ///
    /// # 🔴 A different question from `place_existing`, and answered from a
    /// different table
    ///
    /// `place_existing` answers from the sandbox-to-node binding — plus its
    /// heartbeat-roster and paused-registry fallbacks — and its `Ok(None)` is
    /// reached the moment nothing currently fresh names a holder. A sandbox
    /// paused or deleted seconds after its own create can trigger that
    /// entirely legitimately, before the very first heartbeat has had a
    /// chance to seed the binding; see `NodePlacement::place_existing`'s own
    /// note on the same shape of answer, and the delete-before-first-heartbeat
    /// reproduction it exists to keep working. Reading that `Ok(None)` as "the
    /// node is gone" would forget a sandbox that is very much alive.
    ///
    /// This method answers from node discovery instead: whether the node
    /// itself is still known there, by a heartbeat fresh or merely stale, and
    /// [`NodeMembership::Gone`] only once discovery has actually stopped
    /// listing it — explicitly unregistered, or dropped once its heartbeat
    /// aged out. That is a much stronger claim, and it is the one a caller
    /// may safely use to conclude a runtime is never coming back on its own.
    ///
    /// `Err` means the source could not be asked at all, and every caller
    /// must treat it exactly like [`NodeMembership::Present`] — not knowing
    /// is not licence to conclude "gone", the same reasoning
    /// `place_existing`'s doc gives for keeping `Ok(None)` and `Err` apart.
    async fn node_membership(&self, node_id: &str) -> anyhow::Result<NodeMembership>;

    /// Tells the placement source that a sandbox is now on this node.
    ///
    /// # 🔴 Best-effort by contract, and the contract is the caller's
    ///
    /// The sandbox is already up on the node by the time this is called — that
    /// is what makes the statement true — so failing the operation over it
    /// would tear down a working sandbox to keep a cache honest. Every caller
    /// therefore logs and carries on, and the repair path is the node's own
    /// heartbeat roster, which re-seeds the binding within one interval.
    ///
    /// What this buys is that interval. Without it a sandbox is unroutable and
    /// undrivable-by-anyone-but-its-creator until the first heartbeat after it
    /// exists.
    ///
    /// 🔴 It returns a `Result` rather than swallowing failures itself so that
    /// "the write was attempted and refused" is visible to the caller's logs
    /// and to a test. An implementation that reported success unconditionally
    /// would make the difference between a working write and a rejected one
    /// invisible everywhere.
    async fn record_placement(
        &self,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
        node: &NodeEndpoint,
    ) -> anyhow::Result<()>;
}

/// Sends everything to one node.
///
/// For tests, and for a single-node deployment where the question has one
/// answer.
pub struct FixedNodePlacement {
    node: NodeEndpoint,
}

impl FixedNodePlacement {
    pub fn new(node: NodeEndpoint) -> Self {
        Self { node }
    }
}

#[async_trait]
impl NodePlacement for FixedNodePlacement {
    async fn place_new(
        &self,
        _sandbox_id: SandboxId,
        _resources: SandboxResources,
    ) -> anyhow::Result<NodeEndpoint> {
        Ok(self.node.clone())
    }

    /// Always an answer, and never `None`.
    ///
    /// 🔴 A fixed placement has one machine and therefore no way to *not* know
    /// where a sandbox is: there is nowhere else it could be. `None` here would
    /// claim a read happened and came up empty, which is a claim this type is
    /// not in a position to make about anything.
    async fn place_existing(&self, _sandbox_id: SandboxId) -> anyhow::Result<Option<NodeEndpoint>> {
        Ok(Some(self.node.clone()))
    }

    /// The one node, whatever was asked for.
    ///
    /// 🔴 The id is not checked here, and the caller checks it instead. This
    /// type exists to stand in for a cluster in tests and in a single-node
    /// deployment; the safety property — *a resume reaches the machine holding
    /// its bytes and no other* — belongs to the caller that has an origin to
    /// compare against, and putting a copy of it here would leave the real
    /// placement source's answer unchecked.
    async fn resolve_node(&self, _node_id: &str) -> anyhow::Result<NodeEndpoint> {
        Ok(self.node.clone())
    }

    /// Always present. A fixed placement stands in for a cluster with exactly
    /// one machine, and there is no registry for that one machine to have
    /// fallen out of — the id is not even checked, for the same reason
    /// `resolve_node` above does not check it.
    async fn node_membership(&self, _node_id: &str) -> anyhow::Result<NodeMembership> {
        Ok(NodeMembership::Present)
    }

    /// Nothing to tell: this placement is a constant, and a constant learns
    /// nothing from being told where a sandbox went.
    async fn record_placement(
        &self,
        _sandbox_id: SandboxId,
        _execution_id: ExecutionId,
        _node: &NodeEndpoint,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}
