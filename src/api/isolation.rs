//! Node isolation: the switch that stops a healthy node from being given new
//! sandboxes, without taking away the ones it already runs.
//!
//! Hand-written rather than generated, for the same reason `proxy` is: it is an
//! operator-facing control that does not belong in the sandbox API contract.
//!
//! Two pieces live here.
//!
//! * `router` serves `/nodes/{node_id}/isolation` — read, set, clear. The
//!   distributed gateway proxies the identical path through to the node that
//!   owns the flag, so the cluster-wide entrypoint and the node-local one are
//!   the same URL.
//! * `resume_isolation_gate` turns isolation into a routing decision for
//!   resume. An isolated node is about to go away, so a sandbox it can hand to
//!   somebody else should be resumed by somebody else — but only one that the
//!   cluster can actually rebuild elsewhere. A paused sandbox that was never
//!   announced to the registry exists on this node alone, and refusing it here
//!   would turn a slow resume into a lost sandbox.

use std::sync::Arc;

use axum::{
    extract::{Path, Request, State},
    http::{header::HeaderMap, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde::Serialize;
use tracing::{info, warn};

use super::ApiImpl;
use crate::orchestrator::Orchestrator;
use crate::types::SandboxId;

/// Asks the gateway to hand this request to a different node. Only ever sent
/// with 503, and only for work another node can pick up.
pub(crate) const REROUTE_HEADER: &str = "x-agentenv-reroute";
pub(crate) const REROUTE_SCHEDULE: &str = "schedule";

/// camelCase to match the rest of the node API, which the generated surface
/// serialises that way.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct IsolationState {
    node_id: String,
    scheduling_disabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    changed_at_unix_ms: Option<i64>,
}

pub(crate) fn router<I>(api_impl: I) -> Router
where
    I: AsRef<ApiImpl> + Clone + Send + Sync + 'static,
{
    Router::new()
        .route(
            "/nodes/{node_id}/isolation",
            get(read_isolation::<I>)
                .put(set_isolation::<I>)
                .delete(clear_isolation::<I>),
        )
        .with_state(api_impl)
}

/// Mirrors the generated API's key check. That check only asserts a credential
/// is present (see `impls::auth`), and this endpoint must not be the one place
/// that looks stricter than the rest of the surface while being no stricter.
fn authorized(headers: &HeaderMap) -> bool {
    ["X-API-Key", "X-Team-ID", "X-Admin-Token"]
        .iter()
        .any(|name| {
            headers
                .get(*name)
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| !value.is_empty())
        })
}

/// Rejects a call aimed at some other node.
///
/// The gateway resolves the node id to an endpoint before proxying, so a
/// mismatch here means the request reached the wrong node — answering it would
/// isolate a node nobody asked about. Nodes that do not report to a scheduler
/// have no identity to compare against and accept the call as addressed to
/// them.
fn addresses_this_node(api: &ApiImpl, node_id: &str) -> bool {
    match api.observability() {
        Some(observability) => observability.node_id() == node_id,
        None => true,
    }
}

async fn read_isolation<I>(
    State(api_impl): State<I>,
    Path(node_id): Path<String>,
    headers: HeaderMap,
) -> Response
where
    I: AsRef<ApiImpl>,
{
    respond_with_state(api_impl.as_ref(), &node_id, &headers, None)
}

async fn set_isolation<I>(
    State(api_impl): State<I>,
    Path(node_id): Path<String>,
    headers: HeaderMap,
) -> Response
where
    I: AsRef<ApiImpl>,
{
    respond_with_state(api_impl.as_ref(), &node_id, &headers, Some(true))
}

async fn clear_isolation<I>(
    State(api_impl): State<I>,
    Path(node_id): Path<String>,
    headers: HeaderMap,
) -> Response
where
    I: AsRef<ApiImpl>,
{
    respond_with_state(api_impl.as_ref(), &node_id, &headers, Some(false))
}

/// One shape for all three verbs: apply the change if there is one, then report
/// the resulting state. Setting a flag that already holds is a success, so
/// retries and concurrent operators converge instead of colliding.
fn respond_with_state(
    api: &ApiImpl,
    node_id: &str,
    headers: &HeaderMap,
    desired: Option<bool>,
) -> Response {
    if !authorized(headers) {
        return (StatusCode::UNAUTHORIZED, "missing credentials").into_response();
    }
    if !addresses_this_node(api, node_id) {
        return (StatusCode::NOT_FOUND, "node not found on this host").into_response();
    }

    let orchestrator = api.orchestrator();
    if let Some(disabled) = desired {
        if orchestrator.set_scheduling_disabled(disabled) {
            info!(
                node_id,
                scheduling_disabled = disabled,
                "node isolation set"
            );
        }
    }

    Json(IsolationState {
        node_id: node_id.to_string(),
        scheduling_disabled: orchestrator.scheduling_disabled(),
        changed_at_unix_ms: orchestrator.scheduling_disabled_changed_at_ms(),
    })
    .into_response()
}

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
    use axum::http::HeaderValue;

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

    #[test]
    fn authorization_accepts_any_of_the_credentials_the_generated_api_takes() {
        for name in ["X-API-Key", "X-Team-ID", "X-Admin-Token"] {
            let mut headers = HeaderMap::new();
            headers.insert(name, HeaderValue::from_static("something"));
            assert!(authorized(&headers), "{name} should be accepted");
        }
    }

    #[test]
    fn authorization_rejects_missing_and_empty_credentials() {
        assert!(!authorized(&HeaderMap::new()));

        let mut empty = HeaderMap::new();
        empty.insert("X-API-Key", HeaderValue::from_static(""));
        assert!(!authorized(&empty));
    }
}
