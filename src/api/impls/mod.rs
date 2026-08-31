pub mod admin;
pub mod attached_drives;
pub mod auth;
mod pagination;
pub use pagination::{snapshot_cursor_from_token, snapshot_next_token, PaginationError};
mod paused_coordinator;
mod paused_recovery;
mod resume_surface;
pub mod sandbox;
mod snapshots;
mod template;
mod template_helpers;

use std::sync::Arc;

use anyhow::Error as AnyhowError;
use async_trait::async_trait;

use super::proxy::{build_proxy_client, ProxyClient};
use crate::identity::NodeIdentity;
use crate::node_client::NodePlacement;
use crate::observability::ObservabilityService;
use crate::orchestrator::{PausedSandboxPublisher, PausedSandboxRegistry, SandboxOrchestration};
use crate::snapshot::repository::RepositoryError;
use crate::snapshot::SnapshotManager;
use agentenv_http_server::{apis, models};
pub use paused_coordinator::{PausedSandboxCoordinator, StaleReleaseOutcome};
// REST and data-plane resume share one arbitration path.
pub(in crate::api) use paused_recovery::ResumeArbitration;
pub use resume_surface::ResumeWiring;
pub(in crate::api) use resume_surface::{
    DataPlaneResume, DataPlaneResumeRequest, PinRefusalReason,
};
#[cfg(test)]
pub(in crate::api) use resume_surface::{
    PlacedNode, PlacementRefusal, ResumePlacement, ResumePlacementSource, WakeSite,
};

#[derive(Clone, Debug)]
pub struct Claims;

/// Shared pause registry and publication wiring.
pub struct PausedSandboxWiring {
    pub coordinator: Arc<PausedSandboxCoordinator>,
}

impl PausedSandboxWiring {
    pub fn new(
        registry: Arc<dyn PausedSandboxRegistry>,
        snapshot_manager: Arc<SnapshotManager>,
        identity: &NodeIdentity,
    ) -> Self {
        Self {
            coordinator: Arc::new(PausedSandboxCoordinator::new(
                registry,
                snapshot_manager,
                identity.id.clone(),
            )),
        }
    }

    /// The orchestrator's side of the wiring.
    pub fn publisher(&self) -> Arc<dyn PausedSandboxPublisher> {
        Arc::clone(&self.coordinator) as Arc<dyn PausedSandboxPublisher>
    }
}

#[derive(Clone)]
pub struct ApiImpl {
    /// Process-independent orchestration surface selected at startup.
    orchestrator: Arc<dyn SandboxOrchestration>,
    snapshot_manager: Arc<SnapshotManager>,
    /// Cluster pause bookkeeping; the local backend is a no-op.
    paused: Arc<PausedSandboxCoordinator>,
    observability: Option<Arc<ObservabilityService>>,
    proxy_client: ProxyClient,
    sandbox_proxy_domains: Vec<String>,
    /// Placement and wake-site policy for data-plane resume.
    resume_wiring: ResumeWiring,
    /// Placement for template builds this process cannot run locally.
    node_placement: Option<Arc<dyn NodePlacement>>,
}

impl ApiImpl {
    pub fn new(
        orchestrator: Arc<dyn SandboxOrchestration>,
        snapshot_manager: Arc<SnapshotManager>,
        observability: Option<Arc<ObservabilityService>>,
        paused: PausedSandboxWiring,
        sandbox_proxy_domains: Vec<String>,
        resume_wiring: ResumeWiring,
    ) -> Self {
        Self {
            orchestrator,
            snapshot_manager,
            paused: paused.coordinator,
            observability,
            proxy_client: build_proxy_client(),
            sandbox_proxy_domains,
            resume_wiring,
            node_placement: None,
        }
    }

    /// Configures remote placement for template builds.
    pub fn with_node_placement(mut self, node_placement: Arc<dyn NodePlacement>) -> Self {
        self.node_placement = Some(node_placement);
        self
    }

    /// Whether this process runs the sandboxes represented by this API.
    pub fn runs_sandbox_runtime(&self) -> bool {
        self.resume_wiring.runs_sandboxes_here()
    }

    /// Whether this process owns user-facing sandbox decisions.
    pub fn owns_sandboxes(&self) -> bool {
        !self.runs_sandbox_runtime()
    }

    /// Returns remote template-build placement, if configured.
    pub fn node_placement(&self) -> Option<Arc<dyn NodePlacement>> {
        self.node_placement.as_ref().map(Arc::clone)
    }

    pub fn orchestrator(&self) -> Arc<dyn SandboxOrchestration> {
        Arc::clone(&self.orchestrator)
    }

    pub fn proxy_client(&self) -> &ProxyClient {
        &self.proxy_client
    }

    pub fn sandbox_proxy_domains(&self) -> &[String] {
        &self.sandbox_proxy_domains
    }

    /// Returns the optional observability service backing node/admin
    /// observability endpoints. This is `None` when the server is configured
    /// with `observability.enabled = false`.
    pub fn observability(&self) -> Option<Arc<ObservabilityService>> {
        self.observability.as_ref().map(Arc::clone)
    }

    fn error(code: i32, message: impl Into<String>) -> models::Error {
        models::Error::new(code, message.into())
    }

    fn internal_error(err: &dyn std::error::Error) -> models::Error {
        let mut message = err.to_string();
        let mut current = err.source();
        while let Some(source) = current {
            let cause = source.to_string();
            if cause != message {
                message.push_str(": ");
                message.push_str(&cause);
            }
            current = source.source();
        }
        Self::error(500, message)
    }

    fn repository_error(err: &RepositoryError) -> models::Error {
        match err {
            RepositoryError::InvalidRequest { .. } => Self::error(400, err.to_string()),
            RepositoryError::SnapshotNotFound { .. }
            | RepositoryError::AliasNotFound { .. }
            | RepositoryError::ArtifactNotFound { .. }
            | RepositoryError::ManagedLayerNotFound { .. } => Self::error(404, err.to_string()),
            RepositoryError::AliasConflict { .. } | RepositoryError::IntegrityMismatch { .. } => {
                Self::error(409, err.to_string())
            }
            RepositoryError::Unsupported { .. } => Self::error(500, err.to_string()),
            RepositoryError::Backend { .. } => Self::internal_error(err),
        }
    }

    fn snapshot_manager_error(err: &AnyhowError) -> models::Error {
        if let Some(repo_err) = err
            .chain()
            .find_map(|e| e.downcast_ref::<RepositoryError>())
        {
            Self::repository_error(repo_err)
        } else {
            Self::internal_error(err.as_ref())
        }
    }

    /// Maps repository errors produced by build/publish flows to a client-facing
    /// error. `AliasConflict`, `InvalidRequest`, and `IntegrityMismatch` are
    /// treated as client-side input problems and returned as 400; any other
    /// variant falls back to `None` so the caller can choose a 500 default.
    fn bad_request_for_repository_build_error(err: &RepositoryError) -> Option<models::Error> {
        match err {
            RepositoryError::InvalidRequest { .. }
            | RepositoryError::AliasConflict { .. }
            | RepositoryError::IntegrityMismatch { .. } => Some(Self::error(400, err.to_string())),
            _ => None,
        }
    }

    /// Dispatches a built-up `models::Error` into either a 400 or 500 response
    /// variant based on its `code`. Keeps build/publish endpoints consistent
    /// across templates and snapshots.
    fn client_or_server_response<R>(
        err: models::Error,
        bad_request: impl FnOnce(models::Error) -> R,
        server_error: impl FnOnce(models::Error) -> R,
    ) -> R {
        if err.code == 400 {
            bad_request(err)
        } else {
            server_error(err)
        }
    }
}

#[async_trait]
impl apis::ErrorHandler<()> for ApiImpl {}

#[async_trait]
impl apis::default::Default<()> for ApiImpl {
    async fn health_get(
        &self,
        _method: &http::Method,
        _host: &headers::Host,
        _cookies: &axum_extra::extract::CookieJar,
    ) -> Result<apis::default::HealthGetResponse, ()> {
        Ok(apis::default::HealthGetResponse::Status204_TheServiceIsHealthy)
    }
}
