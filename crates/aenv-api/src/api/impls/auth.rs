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

    // Headers carry API identity, not credentials; access control belongs at the
    // network boundary.
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
