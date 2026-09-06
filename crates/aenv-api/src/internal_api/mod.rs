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
use std::time::SystemTime;

use axum::extract::{DefaultBodyLimit, Request, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use serde_json::json;
use tracing::warn;
use zeroize::Zeroizing;

use crate::egress_ca::EgressRootCa;
use crate::internal_auth::{CallerError, SharedCallerNode};

pub use intermediate::INTERMEDIATE_PATH;

/// A request is a handful of short identifiers; the credential check runs
/// before the body is read, and a body past this is refused unread.
pub(crate) const MAX_REQUEST_BYTES: usize = 4096;

/// The node a request was established to come from. Absent when the caller
/// presented the legacy bearer, which names no machine.
#[derive(Clone, Debug)]
pub struct CallerNodeId(pub String);

/// What every internal route shares: who the caller is, and until when the
/// bearer that predates per-node identity is still accepted.
#[derive(Clone)]
pub struct InternalAuth {
    pub caller: SharedCallerNode,
    /// The bearer the broker used before it had an identity of its own.
    pub legacy_bearer: Option<Arc<Zeroizing<String>>>,
    /// When the legacy bearer stops being accepted. `None` never closes the
    /// window, which is what an un-migrated deployment leaves it at.
    pub legacy_bearer_until: Option<SystemTime>,
    /// False where this half has no Kubernetes to establish identity against.
    pub enabled: bool,
}

impl InternalAuth {
    fn legacy_accepts(&self, presented: &str, now: SystemTime) -> bool {
        let Some(bearer) = self.legacy_bearer.as_ref() else {
            return false;
        };
        if self.legacy_bearer_until.is_some_and(|until| now >= until) {
            return false;
        }
        aenv_core::api::constant_time_eq(presented.as_bytes(), bearer.as_bytes())
    }
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

/// Establishes the caller before any extractor touches the body.
///
/// The legacy bearer is tried first: it is a constant-time compare, while the
/// token costs two calls to the Kubernetes API. A caller it admits carries no
/// [`CallerNodeId`], so every route that scopes by node has to treat an absent
/// one as "unscoped" deliberately rather than by accident.
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
    if auth.legacy_accepts(&presented, SystemTime::now()) {
        return next.run(request).await;
    }
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
    warn!(
        "an internal call presented no credential this half accepts; the broker's projected \
         token must carry the aenv-api audience, or its resolver.token_file must hold the \
         value secrets.pg.resolver_token_file names while that bearer is still accepted"
    );
    answer(StatusCode::UNAUTHORIZED, json!({"error": "unauthorized"}))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::internal_auth::StaticCallerNode;
    use std::time::Duration;

    fn auth(until: Option<SystemTime>) -> InternalAuth {
        InternalAuth {
            caller: Arc::new(StaticCallerNode::new([("sa-token", "node-a")])),
            legacy_bearer: Some(Arc::new(Zeroizing::new("legacy".to_string()))),
            legacy_bearer_until: until,
            enabled: true,
        }
    }

    #[test]
    fn the_legacy_bearer_is_accepted_until_its_deadline_and_never_after() {
        let now = SystemTime::now();
        let open = auth(None);
        assert!(open.legacy_accepts("legacy", now));
        assert!(!open.legacy_accepts("other", now));

        let closing = auth(Some(now + Duration::from_secs(60)));
        assert!(closing.legacy_accepts("legacy", now));
        assert!(!closing.legacy_accepts("legacy", now + Duration::from_secs(61)));
    }

    #[test]
    fn a_deployment_that_configured_no_legacy_bearer_accepts_none() {
        let auth = InternalAuth {
            legacy_bearer: None,
            ..auth(None)
        };

        assert!(!auth.legacy_accepts("legacy", SystemTime::now()));
        assert!(!auth.legacy_accepts("", SystemTime::now()));
    }
}
