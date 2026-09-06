//! `POST /internal/egress/intermediate`: a node's broker asks for the
//! signing key it serves intercepted names with.
//!
//! The node is the caller's, established from its token — never a field in
//! the request. A broker that could name its own node could ask for any
//! node's key, and every key this issues signs the same names.

use std::sync::Arc;

use axum::extract::{Extension, State};
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::post;
use axum::Router;
use serde_json::json;
use tracing::{info, warn};

use super::{answer, CallerNodeId};
use crate::egress_ca::EgressRootCa;

pub const INTERMEDIATE_PATH: &str = "/internal/egress/intermediate";

pub(super) fn router(root_ca: Arc<EgressRootCa>) -> Router {
    Router::new()
        .route(INTERMEDIATE_PATH, post(issue))
        .with_state(root_ca)
}

async fn issue(
    State(root_ca): State<Arc<EgressRootCa>>,
    caller: Option<Extension<CallerNodeId>>,
) -> Response {
    // The legacy bearer names no machine, and an intermediate is per machine.
    let Some(Extension(CallerNodeId(node_id))) = caller else {
        warn!(
            "an intermediate was asked for by a caller with no node identity; only a projected \
             ServiceAccount token bound to a Pod can be issued one"
        );
        return answer(
            StatusCode::UNAUTHORIZED,
            json!({"error": "a node identity is required"}),
        );
    };

    match root_ca.issue_node_intermediate(&node_id) {
        Ok(issued) => {
            info!(
                node_id,
                not_after_unix = issued.not_after_unix,
                "issued an egress intermediate"
            );
            answer(
                StatusCode::OK,
                json!({
                    "certificate": String::from_utf8_lossy(&issued.certificate_pem),
                    "key": String::from_utf8_lossy(&issued.key_pem),
                    "root": String::from_utf8_lossy(&issued.root_pem),
                    "notAfterUnix": issued.not_after_unix,
                }),
            )
        }
        Err(err) => {
            warn!(node_id, error = %format_args!("{err:#}"), "could not issue an egress intermediate");
            answer(
                StatusCode::SERVICE_UNAVAILABLE,
                json!({"error": "the egress root could not sign"}),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use axum::body::Body;
    use axum::http::{header, Request as HttpRequest, StatusCode};
    use tower::ServiceExt as _;
    use zeroize::Zeroizing;

    use super::*;
    use crate::egress_ca::generate_root;
    use crate::internal_api::{router as internal_router, InternalAuth};
    use crate::internal_auth::StaticCallerNode;

    const NODE_TOKEN: &str = "sa-token-node-a";
    const LEGACY: &str = "broker-bearer";

    fn app(enabled: bool, legacy_until: Option<SystemTime>) -> Router {
        let (certificate, key) = generate_root("AgentENV Egress Test Root").unwrap();
        let root_ca = Arc::new(EgressRootCa::from_pem(&certificate, &key).unwrap());
        internal_router(
            InternalAuth {
                caller: Arc::new(StaticCallerNode::new([(NODE_TOKEN, "node-a")])),
                legacy_bearer: Some(Arc::new(Zeroizing::new(LEGACY.to_string()))),
                legacy_bearer_until: legacy_until,
                enabled,
            },
            Some(root_ca),
            Router::new(),
        )
    }

    async fn ask(app: Router, bearer: Option<&str>) -> (StatusCode, serde_json::Value) {
        let mut request = HttpRequest::builder().method("POST").uri(INTERMEDIATE_PATH);
        if let Some(bearer) = bearer {
            request = request.header(header::AUTHORIZATION, format!("Bearer {bearer}"));
        }
        let response = app
            .oneshot(request.body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .unwrap();
        (
            status,
            serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null),
        )
    }

    #[tokio::test]
    async fn a_nodes_own_token_gets_an_intermediate_naming_that_node() {
        let (status, body) = ask(app(true, None), Some(NODE_TOKEN)).await;

        assert_eq!(status, StatusCode::OK);
        let certificate =
            openssl::x509::X509::from_pem(body["certificate"].as_str().unwrap().as_bytes())
                .expect("a PEM certificate");
        let text = String::from_utf8(certificate.to_text().unwrap()).unwrap();
        assert!(text.contains("AgentENV Egress Node node-a"), "{text}");
        assert!(body["key"].as_str().unwrap().contains("PRIVATE KEY"));
        assert!(body["root"].as_str().unwrap().contains("CERTIFICATE"));
        assert!(body["notAfterUnix"].as_i64().unwrap() > 0);
    }

    #[tokio::test]
    async fn the_legacy_bearer_names_no_node_and_gets_no_key() {
        let (status, body) = ask(app(true, None), Some(LEGACY)).await;

        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert!(body.get("certificate").is_none(), "{body}");
    }

    #[tokio::test]
    async fn a_token_this_half_does_not_know_is_refused() {
        for presented in [None, Some(""), Some("someone-elses-token")] {
            let (status, _) = ask(app(true, None), presented).await;
            assert_eq!(status, StatusCode::UNAUTHORIZED, "{presented:?}");
        }
    }

    #[tokio::test]
    async fn an_expired_legacy_window_refuses_the_bearer_and_still_serves_a_token() {
        let closed = Some(SystemTime::now() - Duration::from_secs(1));

        assert_eq!(
            ask(app(true, closed), Some(LEGACY)).await.0,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            ask(app(true, closed), Some(NODE_TOKEN)).await.0,
            StatusCode::OK
        );
    }

    #[tokio::test]
    async fn a_deployment_with_no_kubernetes_closes_the_endpoint() {
        let (status, body) = ask(app(false, None), Some(NODE_TOKEN)).await;

        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["error"], "resolve disabled in static mode");
    }
}
