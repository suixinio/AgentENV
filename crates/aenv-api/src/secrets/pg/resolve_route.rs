//! The broker's resolve endpoint, served by the api half for
//! `[secrets].backend = "postgres"`.
//!
//! The credential layer is `crate::internal_api`'s. What is this route's own
//! is the node scope: a broker asks only about sandboxes bound to the machine
//! it runs on, so a compromised broker on one node reads nothing about
//! another's.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::SystemTime;

use aenv_core::binding_store::BindingStore;
use axum::extract::{Extension, State};
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::post;
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::json;
use tracing::{debug, warn};

use crate::internal_api::{answer, CallerNodeId};

use super::values::{PgSecretValues, ResolveError, ResolvedCredential};

/// Path the broker's `RESOLVE_PATH` lands on when its base URL ends in
/// `/internal`.
pub const RESOLVE_PATH: &str = "/internal/credentials/resolve";

#[derive(Clone)]
struct ResolveState {
    values: Arc<PgSecretValues>,
    /// Where a sandbox is running, as the routing table records it. Absent
    /// makes every question undecidable: this half cannot say whose sandbox
    /// it is, and answering anyway is what the node scope exists to prevent.
    bindings: Option<Arc<dyn BindingStore>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ResolveRequest {
    sandbox_id: String,
    execution_id: String,
    name: String,
}

/// The routes `crate::internal_api::router` merges for this backend.
pub fn router(values: Arc<PgSecretValues>, bindings: Option<Arc<dyn BindingStore>>) -> Router {
    Router::new()
        .route(RESOLVE_PATH, post(resolve))
        .with_state(ResolveState { values, bindings })
}

/// Whether the sandbox this question is about is running on the machine that
/// asked. Three answers, and the middle one is why this is not a bool: a
/// routing table that cannot be read is an outage, and reading it as "no"
/// would turn every Redis blip into a sandbox losing its credentials.
enum NodeScope {
    Admitted,
    Refused,
    Undecidable(String),
}

async fn scope_to_caller(
    state: &ResolveState,
    CallerNodeId(node_id): &CallerNodeId,
    sandbox_id: &str,
    execution_id: &str,
) -> NodeScope {
    // An undecidable question, not an open one. A deployment that reached
    // this route has a caller identity to scope by and no table to scope
    // against; answering it would be answering unscoped.
    let Some(bindings) = state.bindings.as_ref() else {
        return NodeScope::Undecidable("this half keeps no routing table".to_string());
    };
    match bindings.get(sandbox_id, SystemTime::now()).await {
        Ok(Some(binding)) => {
            if &binding.node.id != node_id {
                return NodeScope::Refused;
            }
            // An empty execution id is a binding that predates the run; it
            // says nothing, so it refuses nothing.
            if !binding.execution_id.is_empty() && binding.execution_id != execution_id {
                return NodeScope::Refused;
            }
            NodeScope::Admitted
        }
        // No binding is a refusal, not a pass. Delete and pause remove the
        // binding before the grant is reaped, and a sandbox in that window is
        // one no node may still be asked about — least of all a node that is
        // not the one it ran on.
        Ok(None) => NodeScope::Refused,
        Err(err) => NodeScope::Undecidable(err.to_string()),
    }
}

async fn resolve(
    State(state): State<ResolveState>,
    caller: Option<Extension<CallerNodeId>>,
    body: Result<Json<ResolveRequest>, axum::extract::rejection::JsonRejection>,
) -> Response {
    let request = match body {
        Ok(Json(request)) => request,
        Err(axum::extract::rejection::JsonRejection::BytesRejection(_)) => {
            return answer(
                StatusCode::PAYLOAD_TOO_LARGE,
                json!({"error": "request too large"}),
            );
        }
        Err(_) => {
            return answer(
                StatusCode::BAD_REQUEST,
                json!({"error": "malformed request"}),
            );
        }
    };
    // A name the store could never hold is refused before it reaches a
    // query, and refused the way an ungranted one is.
    if aenv_core::secrets::validate_name(&request.name).is_err()
        || request.sandbox_id.is_empty()
        || request.execution_id.is_empty()
    {
        return answer(StatusCode::NOT_FOUND, json!({"error": "not found"}));
    }

    // `require_caller` puts one on every request it admits, so this is a
    // route reached without the layer in front of it, not a caller with no
    // node.
    let Some(Extension(caller)) = caller else {
        return answer(StatusCode::UNAUTHORIZED, json!({"error": "unauthorized"}));
    };
    match scope_to_caller(&state, &caller, &request.sandbox_id, &request.execution_id).await {
        NodeScope::Admitted => {}
        NodeScope::Refused => {
            debug!(
                sandbox_id = %request.sandbox_id,
                node_id = caller.0,
                "a broker asked about a sandbox that is not bound to its node"
            );
            return answer(StatusCode::NOT_FOUND, json!({"error": "not found"}));
        }
        NodeScope::Undecidable(err) => {
            warn!(error = %err, "could not read where a sandbox is bound");
            return answer(
                StatusCode::SERVICE_UNAVAILABLE,
                json!({"error": "the routing table is unavailable"}),
            );
        }
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
        Ok(ResolvedCredential::Fields { fields }) => {
            let fields: BTreeMap<&str, &str> = fields
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str()))
                .collect();
            answer(StatusCode::OK, json!({"fields": fields}))
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

    use aenv_core::binding_store::{
        Binding, BindingState, BindingStoreSettings, InMemoryBindingStore,
    };
    use aenv_core::node_registry::types::Node;
    use axum::http::header;

    use super::*;
    use crate::internal_api::{InternalAuth, MAX_REQUEST_BYTES};
    use crate::internal_auth::StaticCallerNode;
    use crate::pg::harness::isolated_schema_pool_or_skip;
    use crate::secrets::envelope::Envelope;
    use crate::secrets::PgSecretRefStore;
    use crate::snapshot::repository::backends::postgres::migrate::migrate;

    /// What a broker on `node-a` presents: its own projected token.
    const NODE_A_TOKEN: &str = "sa-token-node-a";
    const NODE_B_TOKEN: &str = "sa-token-node-b";

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

        /// As the broker on `node-a`, which is where the default fixture
        /// binds every sandbox these tests name.
        async fn resolve(&self, sandbox: &str, execution: &str, name: &str) -> reqwest::Response {
            self.post(
                Some(&format!("Bearer {NODE_A_TOKEN}")),
                json!({"sandboxId": sandbox, "executionId": execution, "name": name}),
            )
            .await
        }

        async fn resolve_as(
            &self,
            token: &str,
            sandbox: &str,
            execution: &str,
            name: &str,
        ) -> reqwest::Response {
            self.post(
                Some(&format!("Bearer {token}")),
                json!({"sandboxId": sandbox, "executionId": execution, "name": name}),
            )
            .await
        }
    }

    fn serve(values: Arc<PgSecretValues>, bindings: Option<Arc<dyn BindingStore>>) -> Router {
        crate::internal_api::router(
            InternalAuth {
                caller: Arc::new(StaticCallerNode::new([
                    (NODE_A_TOKEN, "node-a"),
                    (NODE_B_TOKEN, "node-b"),
                ])),
                enabled: true,
            },
            router(values, bindings),
        )
    }

    fn bound_to(node_id: &str, sandbox_id: &str, execution_id: &str) -> Arc<dyn BindingStore> {
        let store = Arc::new(InMemoryBindingStore::new(BindingStoreSettings::default()));
        let recorded = Arc::clone(&store);
        let node_id = node_id.to_string();
        let sandbox_id = sandbox_id.to_string();
        let execution_id = execution_id.to_string();
        futures::executor::block_on(async move {
            recorded
                .record(
                    &sandbox_id,
                    Binding {
                        node: Node {
                            id: node_id,
                            endpoint: String::new(),
                            pod_name: String::new(),
                        },
                        execution_id,
                        projection_ttl: std::time::Duration::ZERO,
                        state: BindingState::Confirmed,
                    },
                    SystemTime::now(),
                )
                .await
                .expect("the in-memory store records");
        });
        store
    }

    macro_rules! endpoint_or_skip {
        // Every caller is a node now, so the default fixture is a table that
        // binds the sandbox these tests use to `node-a`.
        ($name:literal) => {
            endpoint_or_skip!($name, Some(bound_to("node-a", "sbx-1", "exec-1")))
        };
        ($name:literal, $bindings:expr) => {{
            let pool = isolated_schema_pool_or_skip!($name);
            migrate(&pool).await.expect("migration should succeed");
            let refs = PgSecretRefStore::new(pool.clone());
            let values = Arc::new(PgSecretValues::new(
                pool,
                Envelope::from_key_bytes(&[4u8; 32]).unwrap(),
            ));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let app = serve(Arc::clone(&values), $bindings);
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
            json!({"fields": {"host": "pg.internal", "port": "5432"}}),
            "a fields credential names its own upstream and carries no pin"
        );
    }

    #[tokio::test]
    async fn the_bearer_is_checked_before_the_body_is_read_and_a_large_body_is_refused() {
        let (endpoint, values, refs) = endpoint_or_skip!("resolve_route_body_limit");
        seed(&refs, &values, "openai", opaque("sk-live")).await;
        values
            .grant("sbx-1", "exec-1", &["openai".to_string()])
            .await
            .unwrap();

        let oversized = json!({
            "sandboxId": "sbx-1",
            "executionId": "exec-1",
            "name": "openai",
            "padding": "x".repeat(2 * MAX_REQUEST_BYTES),
        });
        let response = endpoint.post(Some("Bearer wrong"), oversized.clone()).await;
        assert_eq!(
            response.status(),
            401,
            "a wrong bearer is refused whatever the body carries"
        );
        let response = endpoint
            .post(Some(&format!("Bearer {NODE_A_TOKEN}")), oversized)
            .await;
        assert_eq!(
            response.status(),
            413,
            "an authenticated caller still cannot make the endpoint buffer a large body"
        );
        assert_eq!(
            endpoint.resolve("sbx-1", "exec-1", "openai").await.status(),
            200
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
            Some(NODE_A_TOKEN),
            Some("Basic c2EtdG9rZW4tbm9kZS1h"),
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
    async fn a_sandbox_no_binding_names_is_refused_rather_than_answered() {
        // Delete and pause remove the binding before the grant is reaped. A
        // sandbox in that window is one no node may still be asked about.
        let bindings: Arc<dyn BindingStore> =
            Arc::new(InMemoryBindingStore::new(BindingStoreSettings::default()));
        let (endpoint, values, refs) =
            endpoint_or_skip!("resolve_route_no_binding", Some(Arc::clone(&bindings)));
        seed(&refs, &values, "openai", opaque("sk-live")).await;
        values
            .grant("sbx-1", "exec-1", &["openai".to_string()])
            .await
            .unwrap();

        let response = endpoint
            .resolve_as(NODE_A_TOKEN, "sbx-1", "exec-1", "openai")
            .await;

        assert_eq!(response.status(), 404);
        assert!(!response.text().await.unwrap().contains("sk-live"));
    }

    #[tokio::test]
    async fn a_half_with_no_routing_table_answers_unavailable_rather_than_unscoped() {
        let (endpoint, values, refs) = endpoint_or_skip!("resolve_route_no_store", None);
        seed(&refs, &values, "openai", opaque("sk-live")).await;
        values
            .grant("sbx-1", "exec-1", &["openai".to_string()])
            .await
            .unwrap();

        let response = endpoint
            .resolve_as(NODE_A_TOKEN, "sbx-1", "exec-1", "openai")
            .await;

        assert_eq!(
            response.status(),
            503,
            "a caller with an identity and no table to scope it against is undecidable"
        );
        assert!(!response.text().await.unwrap().contains("sk-live"));
    }

    #[tokio::test]
    async fn a_broker_reads_only_the_sandboxes_bound_to_its_own_node() {
        let bindings = bound_to("node-a", "sbx-1", "exec-1");
        let (endpoint, values, refs) =
            endpoint_or_skip!("resolve_route_node_scope", Some(Arc::clone(&bindings)));
        seed(&refs, &values, "openai", opaque("sk-live")).await;
        values
            .grant("sbx-1", "exec-1", &["openai".to_string()])
            .await
            .unwrap();

        assert_eq!(
            endpoint
                .resolve_as(NODE_A_TOKEN, "sbx-1", "exec-1", "openai")
                .await
                .status(),
            200,
            "the node the sandbox is bound to"
        );
        let response = endpoint
            .resolve_as(NODE_B_TOKEN, "sbx-1", "exec-1", "openai")
            .await;
        assert_eq!(response.status(), 404, "another node's broker");
        let body = response.text().await.unwrap();
        assert!(!body.contains("sk-live"), "{body}");
    }

    #[tokio::test]
    async fn a_binding_naming_another_execution_refuses_the_run_that_is_gone() {
        let bindings = bound_to("node-a", "sbx-1", "exec-2");
        let (endpoint, values, refs) =
            endpoint_or_skip!("resolve_route_execution_scope", Some(Arc::clone(&bindings)));
        seed(&refs, &values, "openai", opaque("sk-live")).await;
        values
            .grant("sbx-1", "exec-1", &["openai".to_string()])
            .await
            .unwrap();

        assert_eq!(
            endpoint
                .resolve_as(NODE_A_TOKEN, "sbx-1", "exec-1", "openai")
                .await
                .status(),
            404,
            "the routing table names a different run of this sandbox"
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
            let response = endpoint
                .post(Some(&format!("Bearer {NODE_A_TOKEN}")), body)
                .await;
            assert_eq!(response.status(), 400);
        }
    }
}
