//! What node isolation means for resume.
//!
//! Setting and reading the flag itself belongs to the generated admin API
//! (`POST /nodes/{nodeID}`, see `impls::admin`). What lives here is the one
//! consequence that cannot be expressed as a status field: an isolated node is
//! on its way out, so a paused sandbox that somebody else could rebuild should
//! be resumed by somebody else.
//!
//! `resume_isolation_gate` runs ahead of the generated resume handler and turns
//! that into a routing decision the gateway can act on. It only declines a
//! sandbox the cluster actually knows about — one that was never announced to
//! the registry exists on this node alone, and refusing it here would turn a
//! slow resume into a lost sandbox.

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
use crate::orchestrator::Orchestrator;
use crate::types::SandboxId;

/// Asks the gateway to hand this request to a different node. Only ever sent
/// with 503, and only for work another node can pick up.
pub(crate) const REROUTE_HEADER: &str = "x-agentenv-reroute";
pub(crate) const REROUTE_SCHEDULE: &str = "schedule";

/// Declines a resume that another node could serve, while this node is
/// isolated.
///
/// Sits in front of the generated handler rather than inside the orchestrator
/// because the decision is about *routing*, not about the sandbox: the answer
/// is "somebody else should do this", and the gateway is who can act on it.
pub(crate) async fn resume_isolation_gate<I>(
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

/// The sandbox a `POST /sandboxes/{id}/resume` addresses, if that is what this
/// path is.
fn resume_target(path: &str) -> Option<SandboxId> {
    let mut parts = path.trim_matches('/').split('/');
    match (parts.next(), parts.next(), parts.next(), parts.next()) {
        (Some("sandboxes"), Some(id), Some("resume"), None) => SandboxId::parse_str(id).ok(),
        _ => None,
    }
}

/// Whether a paused sandbox is one the cluster can rebuild on another node.
///
/// Only a record that was announced to the registry qualifies. A local-only
/// record names artifacts that exist on this node and nowhere else, so handing
/// it to another node would produce a confident 404 instead of a sandbox.
async fn recoverable_elsewhere(orchestrator: &Arc<Orchestrator>, sandbox_id: SandboxId) -> bool {
    use crate::orchestrator::ClusterRegistration;

    match orchestrator
        .paused_record_cluster_registration(sandbox_id)
        .await
    {
        Ok(ClusterRegistration::Never) => false,
        Ok(_) => true,
        Err(err) => {
            // Unknown is not a licence to send the sandbox away: staying is the
            // outcome that cannot lose it.
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

    // Every other sandbox endpoint addresses a sandbox that lives on one
    // specific node. Matching them here would have the gateway hand ordinary
    // traffic to a node that never held the sandbox.
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
