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
use crate::image::RootfsImageResolver;
use crate::node_client::NodePlacement;
use crate::observability::ObservabilityService;
use crate::orchestrator::{PausedSandboxPublisher, PausedSandboxRegistry, SandboxOrchestration};
use crate::snapshot::repository::RepositoryError;
use crate::snapshot::SnapshotManager;
use crate::template::TemplateBuildDriver;
use agentenv_http_server::{apis, models};
pub use paused_coordinator::{PausedSandboxCoordinator, StaleReleaseOutcome};
// The data-plane auto-resume takes the same decision the REST resume does; both
// reach it through this one point.
//
// 🔴 Still true after the wake-up decision moved to `resume_surface`, and it is
// what made the move safe: the gRPC surface the gateway calls and the REST
// resume route both arbitrate here. The local reverse proxy's
// `try_auto_resume` was the third caller and is deleted; the point survives it
// — one place a resume can acquire the right to start.
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
    /// 🔴 The orchestration surface, not an `Orchestrator`. Which concrete
    /// orchestrator is behind it is the calling binary's decision, taken once
    /// at startup; see `crate::orchestrator::facade` for why it cannot be taken
    /// one type parameter at a time.
    orchestrator: Arc<dyn SandboxOrchestration>,
    snapshot_manager: Arc<SnapshotManager>,
    /// Cluster-wide bookkeeping for paused sandboxes. With the default `local`
    /// registry backend every call is a no-op and pause/resume stay node-local.
    paused: Arc<PausedSandboxCoordinator>,
    template_builder: Arc<dyn TemplateBuildDriver>,
    image_resolver: Arc<dyn RootfsImageResolver>,
    observability: Option<Arc<ObservabilityService>>,
    proxy_client: ProxyClient,
    sandbox_proxy_domains: Vec<String>,
    /// What the data-plane wake-up path needs beyond the above: where the
    /// cluster says a sandbox may be woken, and whether this process wakes them
    /// itself.
    resume_wiring: ResumeWiring,
    /// Where a template build this process cannot run itself should be sent.
    ///
    /// 🔴 `Some` only in `aenv-api`: [`Self::runs_sandbox_runtime`] is true in
    /// `aenv-node`, and a process that can build a template locally has no
    /// business picking a node to send one to instead. `None` there is not
    /// "not configured yet" — it is which binary this is showing through,
    /// matching the `resume_wiring` field's own `WakeSite::Local`/`Remote`
    /// split just above.
    node_placement: Option<Arc<dyn NodePlacement>>,
}

impl ApiImpl {
    // Eight, and each one is a distinct subsystem this surface needs rather
    // than a parameter that could be folded into another.
    //
    // 🔴 It was nine while a `role` sat beside `resume_wiring`. The two said
    // the same thing — see [`ResumeWiring::runs_sandboxes_here`] — and a pair
    // that must agree is a pair that can disagree.
    //
    // 🔴 Not nine now either: `node_placement` is deliberately not a
    // constructor parameter — see `with_node_placement` below for why.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        orchestrator: Arc<dyn SandboxOrchestration>,
        snapshot_manager: Arc<SnapshotManager>,
        template_builder: Arc<dyn TemplateBuildDriver>,
        image_resolver: Arc<dyn RootfsImageResolver>,
        observability: Option<Arc<ObservabilityService>>,
        paused: PausedSandboxWiring,
        sandbox_proxy_domains: Vec<String>,
        resume_wiring: ResumeWiring,
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
            resume_wiring,
            node_placement: None,
        }
    }

    /// Wires this process to send a template build somewhere else when it
    /// cannot run one itself.
    ///
    /// 🔴 A builder step and not a ninth constructor argument, on purpose:
    /// every call site of `new` but `assemble_api`'s has no placement to give
    /// it. A required argument would have meant editing all of them (and they
    /// have nothing to do with this feature) to pass `None`, for a value that
    /// only ever varies for one of them.
    pub fn with_node_placement(mut self, node_placement: Arc<dyn NodePlacement>) -> Self {
        self.node_placement = Some(node_placement);
        self
    }

    /// Whether the sandboxes this surface answers for run in *this* process.
    ///
    /// 🔴 `aenv-node` answers `true`, `aenv-api` answers `false`, and there is
    /// no third answer: they are two binaries with two dependency graphs, and
    /// `aenv-api` does not link a sandbox runtime at all
    /// (`make check-crate-boundaries`). This is a method rather than a
    /// compile-time constant only because `ApiImpl` lives in the crate *both*
    /// of them link; a constant here would be a lie for one of them.
    ///
    /// Read off [`ResumeWiring::runs_sandboxes_here`] — see there for why this
    /// surface no longer carries a second copy of the same fact.
    pub fn runs_sandbox_runtime(&self) -> bool {
        self.resume_wiring.runs_sandboxes_here()
    }

    /// Whether this process answers the user-facing REST surface — the
    /// `sandboxes`, `snapshots` and `templates` route groups — and decides, on
    /// its own initiative, that a paused sandbox should be woken.
    ///
    /// 🔴 The exact complement of [`Self::runs_sandbox_runtime`], and that is
    /// a property of there being exactly two halves rather than a coincidence
    /// worth hiding: a node runs VMs, and deciding that a sandbox should exist,
    /// be woken or be thrown away is the other half's job. A node that kept
    /// answering those routes while the API half believed it owned the same
    /// sandboxes would be a second ledger for one set of machines.
    ///
    /// These were two separate predicates while a third, `all`, answered `true`
    /// to both this and `runs_sandbox_runtime`. No process is both any more.
    pub fn owns_sandboxes(&self) -> bool {
        !self.runs_sandbox_runtime()
    }

    /// Where to send a template build this process cannot run itself, or
    /// `None` when it can (or, on a misconfigured `aenv-api`, when nobody gave
    /// it one — see the field's own doc).
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

    pub fn image_resolver(&self) -> Arc<dyn RootfsImageResolver> {
        Arc::clone(&self.image_resolver)
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
