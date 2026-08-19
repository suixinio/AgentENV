use async_trait::async_trait;
use axum::http::header::HeaderMap;

use agentenv_http_server::apis;

use super::{ApiImpl, Claims};

fn non_empty_header(headers: &HeaderMap, name: &str) -> bool {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| !value.is_empty())
}

#[async_trait]
impl apis::ApiKeyAuthHeader for ApiImpl {
    type Claims = Claims;

    // 🔴 Presence, not validity. Any non-empty value is accepted, including one
    // a caller invented — measured, not inferred: a made-up `X-Admin-Token`
    // sent through the NodePort put a node into DRAINING twice, 204 both times.
    //
    // This is not an oversight to be fixed by comparing strings here. The node
    // API's protection is the network boundary, the same place e2b puts it —
    // its orchestrator's gRPC server has no authentication interceptor at all
    // (`packages/shared/pkg/grpc/server.go` chains recovery and logging and
    // nothing else), because only the control plane is meant to reach it. Our
    // headers exist to carry a caller's identity for the API contract, and
    // giving them real credentials would mean issuing, distributing and
    // rotating a secret for every caller across two repositories.
    //
    // 🔴 So the thing to check is the boundary, not this function. A cluster
    // that exposes port 8000 beyond the control plane — a NodePort, a
    // permissive NetworkPolicy — has an unauthenticated admin surface, and this
    // header will not tell it so. See docs/src/deployment/kubernetes.md.
    async fn extract_claims_from_header(
        &self,
        headers: &HeaderMap,
        key: &str,
    ) -> Option<Self::Claims> {
        let admin_token = non_empty_header(headers, "X-Admin-Token");
        if key == "X-Admin-Token" {
            return admin_token.then_some(Claims);
        }

        if non_empty_header(headers, "X-API-Key")
            || non_empty_header(headers, "X-Team-ID")
            || admin_token
        {
            Some(Claims)
        } else {
            None
        }
    }
}

#[async_trait]
impl apis::ApiAuthBasic for ApiImpl {
    type Claims = Claims;

    async fn extract_claims_from_auth_header(
        &self,
        kind: apis::BasicAuthKind,
        headers: &HeaderMap,
        key: &str,
    ) -> Option<Self::Claims> {
        let expected_scheme = match kind {
            apis::BasicAuthKind::Basic => "Basic",
            apis::BasicAuthKind::Bearer => "Bearer",
            _ => return None,
        };
        let value = headers.get(key)?.to_str().ok()?;
        let (scheme, credentials) = value.split_once(' ')?;
        (scheme.eq_ignore_ascii_case(expected_scheme) && !credentials.is_empty()).then_some(Claims)
    }
}
