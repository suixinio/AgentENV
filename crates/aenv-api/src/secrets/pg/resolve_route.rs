//! The broker's resolve endpoint, served by the api half for
//! `[secrets].backend = "postgres"`.
//!
//! Deliberately not in `src/api/openapi.yml`: a route declared there is
//! counted as part of the user-facing surface by the role gate, generated
//! into every client and printed in the public API reference. It is mounted
//! through the extra-routes seam instead, and carries its own bearer check —
//! the generated API-key authentication does not reach it, and a broker
//! holding a user API key would hold the whole REST surface.

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::extract::State;
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::json;
use tracing::warn;
use zeroize::Zeroizing;

use super::values::{PgSecretValues, ResolveError, ResolvedCredential};

/// Path the broker's `RESOLVE_PATH` lands on when its base URL ends in
/// `/internal`.
pub const RESOLVE_PATH: &str = "/internal/credentials/resolve";

#[derive(Clone)]
struct ResolveState {
    values: Arc<PgSecretValues>,
    token: Arc<Zeroizing<String>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ResolveRequest {
    sandbox_id: String,
    execution_id: String,
    name: String,
}

/// The routes `new_control_plane_only` merges for this backend.
pub fn router(values: Arc<PgSecretValues>, token: Zeroizing<String>) -> Router {
    Router::new()
        .route(RESOLVE_PATH, post(resolve))
        .with_state(ResolveState {
            values,
            token: Arc::new(token),
        })
}

/// Length first, then every byte: an early return on the first mismatch
/// turns the token into something a caller can walk one byte at a time.
fn token_matches(presented: &str, expected: &str) -> bool {
    if presented.len() != expected.len() {
        return false;
    }
    presented
        .bytes()
        .zip(expected.bytes())
        .fold(0u8, |acc, (a, b)| acc | (a ^ b))
        == 0
}

fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
}

fn answer(status: StatusCode, body: serde_json::Value) -> Response {
    (
        status,
        // The broker caches on its own clock; nothing between the two may.
        [(header::CACHE_CONTROL, "no-store")],
        Json(body),
    )
        .into_response()
}

async fn resolve(
    State(state): State<ResolveState>,
    headers: HeaderMap,
    body: Result<Json<ResolveRequest>, axum::extract::rejection::JsonRejection>,
) -> Response {
    if !bearer(&headers).is_some_and(|presented| token_matches(presented, &state.token)) {
        return answer(StatusCode::UNAUTHORIZED, json!({"error": "unauthorized"}));
    }
    let Ok(Json(request)) = body else {
        return answer(
            StatusCode::BAD_REQUEST,
            json!({"error": "malformed request"}),
        );
    };
    // A name the store could never hold is refused before it reaches a
    // query, and refused the way an ungranted one is.
    if aenv_core::secrets::validate_name(&request.name).is_err()
        || request.sandbox_id.is_empty()
        || request.execution_id.is_empty()
    {
        return answer(StatusCode::NOT_FOUND, json!({"error": "not found"}));
    }

    match state
        .values
        .resolve(&request.sandbox_id, &request.execution_id, &request.name)
        .await
    {
        Ok(ResolvedCredential::Opaque {
            value,
            allowed_hosts,
        }) => answer(
            StatusCode::OK,
            json!({"value": value.as_str(), "allowedHosts": allowed_hosts}),
        ),
        Ok(ResolvedCredential::Fields {
            fields,
            allowed_hosts,
        }) => {
            let fields: BTreeMap<&str, &str> = fields
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str()))
                .collect();
            answer(
                StatusCode::OK,
                json!({"fields": fields, "allowedHosts": allowed_hosts}),
            )
        }
        // 404 and not 500: the broker reads 401/403/404 as a refusal the
        // guest sees as a synthetic 403, and everything else as an outage it
        // reports as a 502. "You have no grant for this" is the first.
        Err(ResolveError::Denied) => answer(StatusCode::NOT_FOUND, json!({"error": "not found"})),
        Err(ResolveError::Unavailable(err)) => {
            // The name is the whole of what a span may carry here.
            warn!(secret = %request.name, error = %format_args!("{err:#}"), "failed to resolve a credential");
            answer(
                StatusCode::SERVICE_UNAVAILABLE,
                json!({"error": "the credential store is unavailable"}),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use aenv_core::secrets::SecretsBackend;
    use aenv_core::secrets::{SecretMetadata, SecretRefStore, SecretString, SecretValue};

    use super::*;
    use crate::pg::harness::isolated_schema_pool_or_skip;
    use crate::secrets::envelope::Envelope;
    use crate::secrets::PgSecretRefStore;
    use crate::snapshot::repository::backends::postgres::migrate::migrate;

    const TOKEN: &str = "broker-bearer";

    struct Endpoint {
        base: String,
        client: reqwest::Client,
    }

    impl Endpoint {
        async fn post(&self, token: Option<&str>, body: serde_json::Value) -> reqwest::Response {
            let mut request = self.client.post(format!("{}{RESOLVE_PATH}", self.base));
            if let Some(token) = token {
                request = request.header(header::AUTHORIZATION, token);
            }
            request.json(&body).send().await.unwrap()
        }

        async fn resolve(&self, sandbox: &str, execution: &str, name: &str) -> reqwest::Response {
            self.post(
                Some(&format!("Bearer {TOKEN}")),
                json!({"sandboxId": sandbox, "executionId": execution, "name": name}),
            )
            .await
        }
    }

    macro_rules! endpoint_or_skip {
        ($name:literal) => {{
            let pool = isolated_schema_pool_or_skip!($name);
            migrate(&pool).await.expect("migration should succeed");
            let refs = PgSecretRefStore::new(pool.clone());
            let values = Arc::new(PgSecretValues::new(
                pool,
                Envelope::from_key_bytes(&[4u8; 32]).unwrap(),
            ));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let app = router(Arc::clone(&values), Zeroizing::new(TOKEN.to_string()));
            tokio::spawn(async move {
                axum::serve(listener, app).await.unwrap();
            });
            (
                Endpoint {
                    base: format!("http://{addr}"),
                    client: reqwest::Client::new(),
                },
                values,
                refs,
            )
        }};
    }

    async fn seed(
        refs: &PgSecretRefStore,
        values: &PgSecretValues,
        name: &str,
        value: SecretValue,
    ) {
        refs.create(&format!("sec_{name}"), name, &SecretMetadata::new())
            .await
            .unwrap();
        values.put(name, &value, &[]).await.unwrap();
    }

    fn opaque(value: &str) -> SecretValue {
        SecretValue::Opaque(SecretString::new(value.to_string()))
    }

    #[tokio::test]
    async fn a_granted_name_comes_back_in_the_shape_the_broker_parses() {
        let (endpoint, values, refs) = endpoint_or_skip!("resolve_route_opaque");
        refs.create("sec_openai", "openai", &SecretMetadata::new())
            .await
            .unwrap();
        values
            .put(
                "openai",
                &opaque("sk-live"),
                &["api.openai.com".to_string()],
            )
            .await
            .unwrap();
        values
            .grant("sbx-1", "exec-1", &["openai".to_string()])
            .await
            .unwrap();

        let response = endpoint.resolve("sbx-1", "exec-1", "openai").await;
        assert_eq!(response.status(), 200);
        assert_eq!(
            response.headers().get(header::CACHE_CONTROL).unwrap(),
            "no-store",
            "nothing between the broker and this may keep a copy"
        );
        assert_eq!(
            response.json::<serde_json::Value>().await.unwrap(),
            json!({"value": "sk-live", "allowedHosts": ["api.openai.com"]})
        );
    }

    #[tokio::test]
    async fn a_structured_credential_comes_back_under_fields() {
        let (endpoint, values, refs) = endpoint_or_skip!("resolve_route_fields");
        seed(
            &refs,
            &values,
            "tenant_db",
            SecretValue::Fields(
                [("host", "pg.internal"), ("port", "5432")]
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), SecretString::new(v.to_string())))
                    .collect(),
            ),
        )
        .await;
        values
            .grant("sbx-1", "exec-1", &["tenant_db".to_string()])
            .await
            .unwrap();

        let response = endpoint.resolve("sbx-1", "exec-1", "tenant_db").await;
        assert_eq!(response.status(), 200);
        assert_eq!(
            response.json::<serde_json::Value>().await.unwrap(),
            json!({"fields": {"host": "pg.internal", "port": "5432"}, "allowedHosts": []})
        );
    }

    #[tokio::test]
    async fn only_a_matching_bearer_reaches_the_store() {
        let (endpoint, values, refs) = endpoint_or_skip!("resolve_route_auth");
        seed(&refs, &values, "openai", opaque("sk-live")).await;
        values
            .grant("sbx-1", "exec-1", &["openai".to_string()])
            .await
            .unwrap();

        let asked = json!({"sandboxId": "sbx-1", "executionId": "exec-1", "name": "openai"});
        for presented in [
            None,
            Some("Bearer wrong"),
            Some("Bearer "),
            Some(TOKEN),
            Some("Basic YnJva2VyLWJlYXJlcg=="),
        ] {
            let response = endpoint.post(presented, asked.clone()).await;
            assert_eq!(response.status(), 401, "{presented:?} must not be accepted");
            let body = response.text().await.unwrap();
            assert!(!body.contains("sk-live"), "{body}");
        }
        assert_eq!(
            endpoint.resolve("sbx-1", "exec-1", "openai").await.status(),
            200
        );
    }

    #[tokio::test]
    async fn everything_the_grant_does_not_cover_is_a_404_the_broker_reads_as_a_refusal() {
        let (endpoint, values, refs) = endpoint_or_skip!("resolve_route_denied");
        seed(&refs, &values, "openai", opaque("sk-live")).await;
        seed(&refs, &values, "gh", opaque("ghp")).await;
        values
            .grant("sbx-1", "exec-1", &["openai".to_string()])
            .await
            .unwrap();

        for (sandbox, execution, name, why) in [
            ("sbx-2", "exec-1", "openai", "another sandbox"),
            ("sbx-1", "exec-2", "openai", "another execution"),
            ("sbx-1", "exec-1", "gh", "a name outside the grant"),
            ("sbx-1", "exec-1", "absent", "a name with no row"),
            // Refused before it can become a path or a query.
            ("sbx-1", "exec-1", "../escape", "a name that is not a name"),
            ("sbx-1", "exec-1", "with/slash", "a name that is not a name"),
            ("", "exec-1", "openai", "no sandbox"),
            ("sbx-1", "", "openai", "no execution"),
        ] {
            let response = endpoint.resolve(sandbox, execution, name).await;
            assert_eq!(response.status(), 404, "{why}");
            let body = response.text().await.unwrap();
            assert!(
                !body.contains("sk-live") && !body.contains("ghp") && !body.contains(name),
                "a refusal must say nothing about what exists: {body}"
            );
        }
    }

    #[tokio::test]
    async fn a_body_that_is_not_the_request_is_refused_without_reaching_the_store() {
        let (endpoint, _values, _refs) = endpoint_or_skip!("resolve_route_malformed");
        for body in [
            json!({}),
            json!({"sandboxId": "sbx-1"}),
            json!({"sandbox_id": "sbx-1", "execution_id": "exec-1", "name": "openai"}),
            json!([1, 2, 3]),
        ] {
            let response = endpoint.post(Some(&format!("Bearer {TOKEN}")), body).await;
            assert_eq!(response.status(), 400);
        }
    }
}
