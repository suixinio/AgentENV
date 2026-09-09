//! What a node answers over HTTP, and the state behind it.
//!
//! A node serves its own report and the sandbox data plane. Every user-facing
//! route belongs to the api half, so a node's router does not carry them and an
//! unmatched request reaches the data plane's own not-found envelope.

use std::sync::Arc;

use crate::orchestrator::NodeOrchestration;
use aenv_core::observability::{node_detail, ObservabilityService};
use agentenv_http_server::models;

/// The node's HTTP state: what it runs, what it reports, and the domains its
/// data plane answers for.
pub struct NodeApi {
    orchestration: Arc<dyn NodeOrchestration>,
    /// `None` when `observability.enabled = false`; the report endpoints then
    /// answer as if this node had no details to give.
    observability: Option<Arc<ObservabilityService>>,
    sandbox_proxy_domains: Vec<String>,
}

/// Why a report request was refused, in the shape the router turns into a
/// status code.
pub enum ReportRefusal {
    /// This process keeps no observability service, or the request named
    /// another node.
    NotFound(String),
    /// The snapshot could not be taken.
    Unavailable(String),
}

/// A status only the scheduler derives; an operator may not set it.
pub struct DerivedStatus;

impl NodeApi {
    pub fn new(
        orchestration: Arc<dyn NodeOrchestration>,
        observability: Option<Arc<ObservabilityService>>,
        sandbox_proxy_domains: Vec<String>,
    ) -> Self {
        Self {
            orchestration,
            observability,
            sandbox_proxy_domains,
        }
    }

    pub fn orchestration(&self) -> &Arc<dyn NodeOrchestration> {
        &self.orchestration
    }

    pub fn sandbox_proxy_domains(&self) -> &[String] {
        &self.sandbox_proxy_domains
    }

    /// This node's own report, or an empty list when it has none to give.
    ///
    /// A cluster filter naming another cluster selects nothing rather than
    /// reporting this node under a cluster it is not in.
    pub async fn list_self(
        &self,
        cluster_id: Option<uuid::Uuid>,
    ) -> Result<Vec<models::Node>, ReportRefusal> {
        let Some(observability) = self.observability.as_ref() else {
            return Ok(Vec::new());
        };
        if cluster_id.is_some_and(|cluster_id| cluster_id != observability.cluster_id()) {
            return Ok(Vec::new());
        }
        match observability.node_snapshot().await {
            Ok(node) => Ok(vec![models::Node::from(node)]),
            Err(err) => Err(ReportRefusal::Unavailable(err.to_string())),
        }
    }

    /// This node's own report under the identity the caller named.
    pub async fn describe_self(
        &self,
        node_id: &str,
        cluster_id: Option<uuid::Uuid>,
    ) -> Result<models::NodeDetail, ReportRefusal> {
        let observability = self.addressed(node_id, cluster_id)?;
        match observability.node_snapshot().await {
            Ok(node) => Ok(node_detail(node)),
            Err(err) => Err(ReportRefusal::Unavailable(err.to_string())),
        }
    }

    /// Puts this node into or out of draining.
    pub async fn set_own_status(
        &self,
        node_id: &str,
        cluster_id: Option<uuid::Uuid>,
        status: models::NodeStatus,
    ) -> Result<Result<(), DerivedStatus>, ReportRefusal> {
        self.addressed(node_id, cluster_id)?;
        let disabled = match status {
            models::NodeStatus::NodeStatusReady => false,
            models::NodeStatus::NodeStatusDraining => true,
            _ => return Ok(Err(DerivedStatus)),
        };
        self.orchestration.set_scheduling_disabled(disabled);
        Ok(Ok(()))
    }

    /// The observability service, when both identities name this node.
    fn addressed(
        &self,
        node_id: &str,
        cluster_id: Option<uuid::Uuid>,
    ) -> Result<&Arc<ObservabilityService>, ReportRefusal> {
        let Some(observability) = self.observability.as_ref() else {
            return Err(ReportRefusal::NotFound(
                "observability is disabled on this node".to_string(),
            ));
        };
        let cluster_mismatch =
            cluster_id.is_some_and(|cluster_id| cluster_id != observability.cluster_id());
        if node_id != observability.node_id() || cluster_mismatch {
            return Err(ReportRefusal::NotFound(format!("node {node_id} not found")));
        }
        Ok(observability)
    }
}

impl AsRef<NodeApi> for NodeApi {
    fn as_ref(&self) -> &NodeApi {
        self
    }
}
