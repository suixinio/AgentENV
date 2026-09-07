//! The endpoints the per-node broker calls, behind one credential layer.
//!
//! Deliberately not in `src/api/openapi.yml`: a route declared there is
//! counted as part of the user-facing surface by the role gate, generated into
//! every client and printed in the public API reference. These are mounted
//! through the extra-routes seam instead and carry their own check — the
//! generated API-key authentication does not reach them, and a broker holding
//! a user API key would hold the whole REST surface.

mod intermediate;

use std::sync::Arc;

use axum::extract::{DefaultBodyLimit, Request, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use serde_json::json;
use tracing::{debug, warn};

use crate::egress_ca::EgressRootCa;
use crate::internal_auth::{CallerError, SharedCallerNode};

pub use intermediate::INTERMEDIATE_PATH;

/// A request is a handful of short identifiers; the credential check runs
/// before the body is read, and a body past this is refused unread.
pub(crate) const MAX_REQUEST_BYTES: usize = 4096;

/// The node a request was established to come from. Every admitted caller has
/// one: the only credential these endpoints accept is a broker's own projected
/// ServiceAccount token, and that names the machine it was mounted on.
#[derive(Clone, Debug)]
pub struct CallerNodeId(pub String);

/// What every internal route shares: who the caller is.
#[derive(Clone)]
pub struct InternalAuth {
    pub caller: SharedCallerNode,
    /// False where this half has no Kubernetes to establish identity against.
    pub enabled: bool,
}

/// Mounts the broker's endpoints. `root_ca` absent leaves the intermediate
/// endpoint out; `extra` carries whatever the credential backend added.
pub fn router(auth: InternalAuth, root_ca: Option<Arc<EgressRootCa>>, extra: Router) -> Router {
    let mut router = extra;
    if let Some(root_ca) = root_ca {
        router = router.merge(intermediate::router(root_ca));
    }
    router
        .route_layer(middleware::from_fn_with_state(auth.clone(), require_caller))
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BYTES))
}

fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
}

pub(crate) fn answer(status: StatusCode, body: serde_json::Value) -> Response {
    (
        status,
        // The broker caches on its own clock; nothing between the two may.
        [(header::CACHE_CONTROL, "no-store")],
        Json(body),
    )
        .into_response()
}

/// Establishes the caller before any extractor touches the body, so every
/// route downstream can read a [`CallerNodeId`] and none has to decide what an
/// absent one means.
async fn require_caller(
    State(auth): State<InternalAuth>,
    mut request: Request,
    next: Next,
) -> Response {
    if !auth.enabled {
        return answer(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({"error": "resolve disabled in static mode"}),
        );
    }
    let Some(presented) = bearer(request.headers()).map(str::to_string) else {
        return unauthorized();
    };
    match auth.caller.node_of(&presented).await {
        Ok(node_id) => {
            request.extensions_mut().insert(CallerNodeId(node_id));
            next.run(request).await
        }
        Err(CallerError::Unauthenticated) => unauthorized(),
        Err(CallerError::Unavailable(err)) => {
            warn!(error = %err, "could not establish an internal caller's identity");
            answer(
                StatusCode::SERVICE_UNAVAILABLE,
                json!({"error": "the caller's identity could not be established"}),
            )
        }
    }
}

fn unauthorized() -> Response {
    debug!(
        "an internal call presented no credential this half accepts; the broker's projected \
         token must carry the aenv-api audience"
    );
    answer(StatusCode::UNAUTHORIZED, json!({"error": "unauthorized"}))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::internal_auth::StaticCallerNode;

    #[test]
    fn a_token_no_node_claims_is_not_a_caller() {
        let auth = InternalAuth {
            caller: Arc::new(StaticCallerNode::new([("sa-token", "node-a")])),
            enabled: true,
        };

        assert!(futures::executor::block_on(auth.caller.node_of("sa-token")).is_ok());
        assert!(futures::executor::block_on(auth.caller.node_of("anything-else")).is_err());
    }
}
