mod admin;
mod attached_drives;
mod auth;
mod pagination;
mod paused_coordinator;
mod paused_recovery;
mod sandbox;
mod snapshots;
mod template;
mod template_helpers;

use std::sync::Arc;

use anyhow::Error as AnyhowError;
use async_trait::async_trait;

use super::proxy::{build_proxy_client, ProxyClient};
use crate::identity::NodeIdentity;
use crate::image::ImageResolver;
use crate::observability::ObservabilityService;
use crate::orchestrator::{Orchestrator, PausedSandboxPublisher, PausedSandboxRegistry};
use crate::snapshot::repository::RepositoryError;
use crate::snapshot::SnapshotManager;
use crate::template::TemplateBuilder;
use agentenv_http_server::{apis, models};
pub use paused_coordinator::PausedSandboxCoordinator;

#[derive(Clone, Debug)]
pub struct Claims;

/// Everything needed to make a paused sandbox recoverable beyond the node that
/// paused it, built once at startup and shared by the two sides that need it:
/// the API, for resumes that arrive on a node which has never run the sandbox,
/// and the orchestrator, which drives publishing for every pause regardless of
/// what started it.
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
    orchestrator: Arc<Orchestrator>,
    snapshot_manager: Arc<SnapshotManager>,
    /// Cluster-wide bookkeeping for paused sandboxes. With the default `local`
    /// registry backend every call is a no-op and pause/resume stay node-local.
    paused: Arc<PausedSandboxCoordinator>,
    template_builder: Arc<TemplateBuilder>,
    image_resolver: Arc<ImageResolver>,
    observability: Option<Arc<ObservabilityService>>,
    proxy_client: ProxyClient,
    sandbox_proxy_domains: Vec<String>,
}

impl ApiImpl {
    pub fn new(
        orchestrator: Arc<Orchestrator>,
        snapshot_manager: Arc<SnapshotManager>,
        template_builder: Arc<TemplateBuilder>,
        image_resolver: Arc<ImageResolver>,
        observability: Option<Arc<ObservabilityService>>,
        paused: PausedSandboxWiring,
        sandbox_proxy_domains: Vec<String>,
    ) -> Self {
        Self {
            orchestrator,
            snapshot_manager,
            paused: paused.coordinator,
            template_builder,
            image_resolver,
            observability,
            proxy_client: build_proxy_client(),
            sandbox_proxy_domains,
        }
    }

    pub(crate) fn orchestrator(&self) -> Arc<Orchestrator> {
        Arc::clone(&self.orchestrator)
    }

    pub(crate) fn proxy_client(&self) -> &ProxyClient {
        &self.proxy_client
    }

    pub(crate) fn sandbox_proxy_domains(&self) -> &[String] {
        &self.sandbox_proxy_domains
    }

    pub(crate) fn image_resolver(&self) -> Arc<ImageResolver> {
        Arc::clone(&self.image_resolver)
    }

    /// Returns the optional observability service backing node/admin
    /// observability endpoints. This is `None` when the server is configured
    /// with `observability.enabled = false`.
    pub(crate) fn observability(&self) -> Option<Arc<ObservabilityService>> {
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
