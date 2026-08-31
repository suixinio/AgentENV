//! Resume routing for isolated nodes.
//!
//! A sandbox is rerouted only when the cluster registry confirms another node
//! can rebuild it; unannounced local copies always remain local.

use std::sync::Arc;

use axum::{
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use tracing::{info, warn};

use super::ApiImpl;
use crate::orchestrator::SandboxOrchestration;
use crate::types::SandboxId;

/// Gateway reroute signal for work another node can schedule.
pub const REROUTE_HEADER: &str = "x-agentenv-reroute";
pub const REROUTE_SCHEDULE: &str = "schedule";

/// Declines a resume another node can serve while this node is isolated.
pub async fn resume_isolation_gate<I>(
    State(api_impl): State<I>,
    request: Request,
    next: Next,
) -> Response
where
    I: AsRef<ApiImpl> + Clone + Send + Sync + 'static,
{
    let api = api_impl.as_ref();
    let orchestrator = api.orchestrator();

    if orchestrator.scheduling_disabled() {
        if let Some(sandbox_id) = resume_target(request.uri().path()) {
            if recoverable_elsewhere(&orchestrator, sandbox_id).await {
                info!(
                    %sandbox_id,
                    "declining resume on an isolated node; another node can rebuild it"
                );

                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    [(REROUTE_HEADER, REROUTE_SCHEDULE)],
                    Json(serde_json::json!({
                        "code": 503,
                        "message": "node is isolated; resume elsewhere",
                    })),
                )
                    .into_response();
            }

            info!(
                %sandbox_id,
                "node is isolated but this sandbox has no cluster-wide record; resuming locally"
            );
        }
    }

    next.run(request).await
}

fn resume_target(path: &str) -> Option<SandboxId> {
    let mut parts = path.trim_matches('/').split('/');
    match (parts.next(), parts.next(), parts.next(), parts.next()) {
        (Some("sandboxes"), Some(id), Some("resume"), None) => SandboxId::parse_str(id).ok(),
        _ => None,
    }
}

/// Returns whether the registry confirms another node can rebuild the sandbox.
async fn recoverable_elsewhere(
    orchestrator: &Arc<dyn SandboxOrchestration>,
    sandbox_id: SandboxId,
) -> bool {
    use crate::orchestrator::ClusterRegistration;

    match orchestrator
        .paused_record_cluster_registration(sandbox_id)
        .await
    {
        Ok(ClusterRegistration::Never) => false,
        Ok(_) => true,
        Err(err) => {
            // Uncertainty must keep the only possible copy local.
            warn!(
                %sandbox_id,
                error = %format_args!("{err:#}"),
                "could not tell whether the sandbox is recoverable elsewhere; resuming locally"
            );
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resume_target_reads_the_sandbox_out_of_a_resume_path() {
        let id = SandboxId::new();
        let path = format!("/sandboxes/{id}/resume");

        assert_eq!(resume_target(&path), Some(id));
    }

    #[test]
    fn resume_target_ignores_other_sandbox_paths() {
        let id = SandboxId::new();

        for path in [
            format!("/sandboxes/{id}/pause"),
            format!("/sandboxes/{id}"),
            format!("/sandboxes/{id}/resume/extra"),
            "/sandboxes".to_string(),
            format!("/nodes/{id}/isolation"),
        ] {
            assert_eq!(resume_target(&path), None, "path should not match: {path}");
        }
    }

    #[test]
    fn resume_target_ignores_an_unparseable_sandbox_id() {
        assert_eq!(resume_target("/sandboxes/not-a-uuid/resume"), None);
    }
}
