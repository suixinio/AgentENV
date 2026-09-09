//! Selects nodes for new sandboxes, resolves existing sandbox holders, and records
//! completed placement. Existing placement is distinct because local captures may be
//! pinned to exactly one node.

use async_trait::async_trait;

use crate::types::{ExecutionId, SandboxId, SandboxResources};

/// Node identity plus advertised and sandbox-service addresses.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeEndpoint {
    pub node_id: String,
    /// Sandbox-service URI dialed by this client.
    pub endpoint: String,
    /// Original discovery address used for placement records and cluster routing.
    pub advertised_endpoint: String,
}

impl NodeEndpoint {
    /// Constructs a node whose dial and advertised addresses are identical.
    pub fn same_address(node_id: impl Into<String>, endpoint: impl Into<String>) -> Self {
        let endpoint = endpoint.into();
        Self {
            node_id: node_id.into(),
            advertised_endpoint: endpoint.clone(),
            endpoint,
        }
    }
}

/// Discovery-backed membership for a node identity already held by the caller.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeMembership {
    /// Discovery still lists the node.
    Present,
    /// Discovery no longer lists the node.
    Gone,
}

/// Node capabilities a launch depends on.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PlacementNeeds {
    /// The sandbox declares network rules and must land on a node whose
    /// heartbeat reports a usable egress broker.
    pub egress_broker: bool,
}

/// Placement found nodes but none with a capability the launch needs. The
/// API answers 503 rather than 500 when this is in the error chain.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum PlacementRefused {
    #[error(
        "no node reports a usable egress broker; sandboxes with network rules cannot be placed"
    )]
    NoEgressBrokerNode,
}

#[async_trait]
pub trait NodePlacement: Send + Sync + 'static {
    /// Selects a node for a new sandbox.
    ///
    /// `preferred_node_id` is honoured when that node is schedulable and
    /// silently ignored otherwise: it is where a paused sandbox's bytes were
    /// last warm, never where the sandbox must run. `excluded_node_ids` are
    /// nodes that refused this launch already; none of them is chosen, and
    /// an implementation that cannot avoid them returns one anyway, which the
    /// caller reads as the cluster having nowhere else.
    async fn place_new(
        &self,
        sandbox_id: SandboxId,
        resources: SandboxResources,
        preferred_node_id: Option<&str>,
        excluded_node_ids: &[String],
    ) -> anyhow::Result<NodeEndpoint>;

    /// `place_new` with what the sandbox needs from its node. The default
    /// ignores `needs`; a placement source that knows node capabilities
    /// overrides it and refuses with [`PlacementRefused`] when no node fits.
    async fn place_new_with(
        &self,
        sandbox_id: SandboxId,
        resources: SandboxResources,
        preferred_node_id: Option<&str>,
        excluded_node_ids: &[String],
        needs: PlacementNeeds,
    ) -> anyhow::Result<NodeEndpoint> {
        let _ = needs;
        self.place_new(sandbox_id, resources, preferred_node_id, excluded_node_ids)
            .await
    }

    /// Locates the node holding an existing sandbox.
    ///
    /// `Ok(None)` means a complete lookup found no record; `Err` means lookup failed.
    async fn place_existing(&self, sandbox_id: SandboxId) -> anyhow::Result<Option<NodeEndpoint>>;

    /// Resolves the current address of an already-selected node identity.
    async fn resolve_node(&self, node_id: &str) -> anyhow::Result<NodeEndpoint>;

    /// Reports whether discovery still lists an already-selected node.
    ///
    /// Callers must treat errors as unknown, never as [`NodeMembership::Gone`].
    async fn node_membership(&self, node_id: &str) -> anyhow::Result<NodeMembership>;

    /// Best-effort records a completed placement; heartbeats repair failed writes.
    ///
    /// `projection_ttl_secs` is the sandbox's remaining lifetime budget, the
    /// same one its heartbeats carry; zero delegates to the store's default.
    async fn record_placement(
        &self,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
        node: &NodeEndpoint,
        projection_ttl_secs: u32,
    ) -> anyhow::Result<()>;

    /// Records the chosen node before the runtime is asked for, so that no runtime
    /// exists that a complete lookup cannot find.
    ///
    /// Failing this must fail the launch: a lookup that finds nothing is read as a
    /// verdict, and a runtime started without a record would be reaped as an orphan.
    /// The reservation expires on its own, which is what bounds a caller that dies
    /// between here and [`Self::record_placement`].
    async fn reserve_placement(
        &self,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
        node: &NodeEndpoint,
    ) -> anyhow::Result<()>;

    /// Withdraws a reservation whose launch failed, leaving a confirmed placement
    /// of the same incarnation alone.
    async fn release_placement_reservation(
        &self,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
    ) -> anyhow::Result<()>;

    /// Takes the sandbox id for this launch before any node is chosen.
    ///
    /// A launch that meets one already holding the id fails with a
    /// [`crate::orchestrator::LaunchHeldElsewhere`] in its error chain, which
    /// the caller reads as "wait for that launch" rather than "this failed".
    /// The caller holds this across the whole launch, so an implementation
    /// that claims without writing says the id cannot be launched twice for
    /// some other reason, and has to say which.
    async fn reserve_launch(
        &self,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
    ) -> anyhow::Result<()>;

    /// Gives the sandbox id back once the launch has settled, either way.
    ///
    /// The record the launch wrote is what fences the id from here on, so
    /// holding the reservation past that only delays the next launch.
    async fn release_launch(
        &self,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
    ) -> anyhow::Result<()>;

    /// Retires the placement record of an incarnation being torn down, so no
    /// lookup routes at a runtime that is going away.
    ///
    /// Fenced on `execution_id`: a record naming another incarnation is left
    /// alone.
    async fn forget_placement(
        &self,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
    ) -> anyhow::Result<()>;
}

/// The placement source's binding answers whether anything still routes to a
/// sandbox's runtime.
pub struct PlacementRuntimeRouting(std::sync::Arc<dyn NodePlacement>);

impl PlacementRuntimeRouting {
    pub fn shared(
        placement: std::sync::Arc<dyn NodePlacement>,
    ) -> std::sync::Arc<dyn crate::orchestrator::RuntimeRouting> {
        std::sync::Arc::new(Self(placement))
    }
}

#[async_trait]
impl crate::orchestrator::RuntimeRouting for PlacementRuntimeRouting {
    async fn is_routed(&self, sandbox_id: SandboxId) -> anyhow::Result<bool> {
        Ok(self.0.place_existing(sandbox_id).await?.is_some())
    }

    async fn forget(&self, sandbox_id: SandboxId, execution_id: ExecutionId) -> anyhow::Result<()> {
        self.0.forget_placement(sandbox_id, execution_id).await
    }
}

/// How long a reservation outlives the process that wrote it.
///
/// It bounds the window in which a launch that died mid-flight keeps a sandbox id
/// unreapable, so it must exceed the slowest create — a cold image pull and
/// conversion — while staying far below any human's patience for a stuck id.
pub const PLACEMENT_RESERVATION_TTL: std::time::Duration = std::time::Duration::from_secs(300);

/// Sends every operation to one fixed node.
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
        _preferred_node_id: Option<&str>,
        _excluded_node_ids: &[String],
    ) -> anyhow::Result<NodeEndpoint> {
        Ok(self.node.clone())
    }

    /// Always returns the fixed node.
    async fn place_existing(&self, _sandbox_id: SandboxId) -> anyhow::Result<Option<NodeEndpoint>> {
        Ok(Some(self.node.clone()))
    }

    /// Resolves any identity to the fixed node; callers enforce identity constraints.
    async fn resolve_node(&self, _node_id: &str) -> anyhow::Result<NodeEndpoint> {
        Ok(self.node.clone())
    }

    /// Always reports the fixed node present.
    async fn node_membership(&self, _node_id: &str) -> anyhow::Result<NodeMembership> {
        Ok(NodeMembership::Present)
    }

    /// Does nothing because fixed placement has no mutable index.
    async fn record_placement(
        &self,
        _sandbox_id: SandboxId,
        _execution_id: ExecutionId,
        _node: &NodeEndpoint,
        _projection_ttl_secs: u32,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    /// Succeeds without writing: the one fixed node is already the whole answer,
    /// and a lookup against it can never come back empty.
    async fn reserve_placement(
        &self,
        _sandbox_id: SandboxId,
        _execution_id: ExecutionId,
        _node: &NodeEndpoint,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn release_placement_reservation(
        &self,
        _sandbox_id: SandboxId,
        _execution_id: ExecutionId,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    /// Claims without writing: one fixed node has no second replica to race,
    /// and two launches of one id reaching that node are excluded there by its
    /// own `launch_claims` before either allocates anything.
    async fn reserve_launch(
        &self,
        _sandbox_id: SandboxId,
        _execution_id: ExecutionId,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    /// Succeeds without writing: nothing was reserved.
    async fn release_launch(
        &self,
        _sandbox_id: SandboxId,
        _execution_id: ExecutionId,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    /// Succeeds without writing: there is no record to retire.
    async fn forget_placement(
        &self,
        _sandbox_id: SandboxId,
        _execution_id: ExecutionId,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}
