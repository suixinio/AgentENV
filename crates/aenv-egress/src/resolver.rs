//! The broker's other credential source: an operator-run HTTP resolver that
//! owns the credentials themselves. AgentENV stores no value for it; the
//! resolver answers one grant at a time and may leave fields out, which the
//! handler reads as "pass the guest's value through".

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use reqwest::{StatusCode, Url};
use serde::Serialize;
use zeroize::Zeroizing;

use crate::credential::{
    allowed_hosts, is_valid_secret_name, CredentialError, CredentialFields, CredentialSource,
    Secret,
};

/// Path the broker resolves a grant at, appended to the configured base.
pub const RESOLVE_PATH: &str = "credentials/resolve";

pub struct ResolverSource {
    client: reqwest::Client,
    resolve: Url,
    token: Option<Zeroizing<String>>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ResolveRequest<'a> {
    sandbox_id: &'a str,
    execution_id: &'a str,
    name: &'a str,
}

impl ResolverSource {
    /// `base` is a directory URL: a path that does not end in `/` would drop
    /// its last segment when the call path is joined onto it.
    pub fn new(base: &str, token: Option<&str>, timeout: Duration) -> anyhow::Result<Self> {
        let base = base.trim();
        let base = Url::parse(&if base.ends_with('/') {
            base.to_string()
        } else {
            format!("{base}/")
        })?;
        Ok(Self {
            client: reqwest::Client::builder().timeout(timeout).build()?,
            resolve: base.join(RESOLVE_PATH)?,
            token: token
                .map(str::trim)
                .filter(|token| !token.is_empty())
                .map(|token| Zeroizing::new(token.to_string())),
        })
    }

    async fn resolve(
        &self,
        sandbox_id: &str,
        execution_id: &str,
        name: &str,
    ) -> Result<serde_json::Value, CredentialError> {
        if !is_valid_secret_name(name) || execution_id.is_empty() || sandbox_id.is_empty() {
            return Err(CredentialError::Denied);
        }
        let mut request = self
            .client
            .post(self.resolve.clone())
            .json(&ResolveRequest {
                sandbox_id,
                execution_id,
                name,
            });
        if let Some(token) = &self.token {
            request = request.bearer_auth(token.as_str());
        }
        let response = request
            .send()
            .await
            .map_err(|err| CredentialError::Unavailable(format!("resolver: {err}")))?;
        match response.status() {
            StatusCode::FORBIDDEN | StatusCode::NOT_FOUND | StatusCode::UNAUTHORIZED => {
                Err(CredentialError::Denied)
            }
            status if status.is_success() => response
                .json()
                .await
                .map_err(|err| CredentialError::Unavailable(format!("resolver body: {err}"))),
            status => Err(CredentialError::Unavailable(format!(
                "resolver answered {status}"
            ))),
        }
    }
}

fn expiry_of(answer: &serde_json::Value) -> Option<SystemTime> {
    let seconds = answer.get("expiresAtUnix")?.as_u64()?;
    Some(UNIX_EPOCH + Duration::from_secs(seconds))
}

#[async_trait]
impl CredentialSource for ResolverSource {
    async fn get(
        &self,
        sandbox_id: &str,
        execution_id: &str,
        name: &str,
    ) -> Result<Secret, CredentialError> {
        let answer = self.resolve(sandbox_id, execution_id, name).await?;
        match answer.get("value").and_then(|value| value.as_str()) {
            Some(value) => Ok(Secret::new(value.as_bytes().to_vec(), expiry_of(&answer))
                .with_allowed_hosts(allowed_hosts(answer.get("allowedHosts")))),
            None => Err(CredentialError::Unavailable(
                "the resolver returned no opaque value for this name".into(),
            )),
        }
    }

    async fn get_fields(
        &self,
        sandbox_id: &str,
        execution_id: &str,
        name: &str,
    ) -> Result<CredentialFields, CredentialError> {
        let answer = self.resolve(sandbox_id, execution_id, name).await?;
        let Some(fields) = answer.get("fields").and_then(|fields| fields.as_object()) else {
            return Err(CredentialError::Unavailable(
                "the resolver returned no fields for this name".into(),
            ));
        };
        let fields = CredentialFields::from_json(fields, expiry_of(&answer))?;
        if fields.is_empty() {
            return Err(CredentialError::Unavailable(
                "the resolver returned an empty field set".into(),
            ));
        }
        Ok(fields)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use axum::http::{HeaderMap, StatusCode};
    use axum::routing::post;
    use axum::{Json, Router};
    use serde_json::{json, Value};

    use super::*;

    /// One call the fake resolver saw: its body and its Authorization header.
    type SeenCall = (Value, Option<String>);

    #[derive(Clone, Default)]
    struct Fake {
        answer: Arc<Mutex<Option<(StatusCode, Value)>>>,
        seen: Arc<Mutex<Vec<SeenCall>>>,
    }

    async fn resolve(
        axum::extract::State(fake): axum::extract::State<Fake>,
        headers: HeaderMap,
        Json(body): Json<Value>,
    ) -> (StatusCode, Json<Value>) {
        fake.seen.lock().unwrap().push((
            body,
            headers
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                .map(str::to_string),
        ));
        match fake.answer.lock().unwrap().clone() {
            Some((status, body)) => (status, Json(body)),
            None => (StatusCode::FORBIDDEN, Json(json!({}))),
        }
    }

    async fn serve(fake: Fake, base_suffix: &str) -> ResolverSource {
        let app = Router::new()
            .route(&format!("{base_suffix}/credentials/resolve"), post(resolve))
            .with_state(fake);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        ResolverSource::new(
            &format!("http://{addr}{base_suffix}"),
            Some("resolver-token"),
            Duration::from_secs(2),
        )
        .unwrap()
    }

    fn answering(fake: &Fake, status: StatusCode, body: Value) {
        *fake.answer.lock().unwrap() = Some((status, body));
    }

    #[tokio::test]
    async fn a_resolved_grant_becomes_fields_and_carries_the_grant_tuple_and_the_token() {
        let fake = Fake::default();
        answering(
            &fake,
            StatusCode::OK,
            json!({"fields": {"host": "db.internal", "port": 5432, "user": "rw_app", "password": "p"}}),
        );
        let source = serve(fake.clone(), "").await;

        let fields = source
            .get_fields("sbx-1", "exec-1", "tenant_db")
            .await
            .unwrap();
        assert_eq!(fields.get("host"), Some("db.internal"));
        assert_eq!(fields.get("port"), Some("5432"));
        assert_eq!(fields.get("database"), None);

        let seen = fake.seen.lock().unwrap();
        assert_eq!(
            seen[0].0,
            json!({"sandboxId": "sbx-1", "executionId": "exec-1", "name": "tenant_db"})
        );
        assert_eq!(seen[0].1.as_deref(), Some("Bearer resolver-token"));
    }

    #[tokio::test]
    async fn a_base_url_with_a_path_prefix_keeps_that_prefix() {
        let fake = Fake::default();
        answering(&fake, StatusCode::OK, json!({"fields": {"user": "u"}}));
        let source = serve(fake, "/internal/aenv").await;
        assert_eq!(
            source
                .get_fields("sbx-1", "exec-1", "db")
                .await
                .unwrap()
                .get("user"),
            Some("u")
        );
    }

    #[tokio::test]
    async fn a_refusal_is_denied_and_an_outage_is_unavailable() {
        let fake = Fake::default();
        answering(&fake, StatusCode::FORBIDDEN, json!({}));
        let source = serve(fake.clone(), "").await;
        assert_eq!(
            source.get_fields("sbx-1", "exec-1", "db").await.err(),
            Some(CredentialError::Denied)
        );

        answering(&fake, StatusCode::INTERNAL_SERVER_ERROR, json!({}));
        assert!(matches!(
            source.get_fields("sbx-1", "exec-1", "db").await,
            Err(CredentialError::Unavailable(_))
        ));

        let unreachable =
            ResolverSource::new("http://127.0.0.1:9", None, Duration::from_millis(300)).unwrap();
        assert!(matches!(
            unreachable.get_fields("s", "e", "n").await,
            Err(CredentialError::Unavailable(_))
        ));
    }

    #[tokio::test]
    async fn an_answer_without_the_form_the_caller_asked_for_is_unavailable_not_denied() {
        let fake = Fake::default();
        answering(&fake, StatusCode::OK, json!({"fields": {"user": "u"}}));
        let source = serve(fake.clone(), "").await;
        assert!(matches!(
            source.get("sbx-1", "exec-1", "db").await,
            Err(CredentialError::Unavailable(_))
        ));

        answering(&fake, StatusCode::OK, json!({"value": "sk"}));
        assert!(matches!(
            source.get_fields("sbx-1", "exec-1", "db").await,
            Err(CredentialError::Unavailable(_))
        ));
        assert_eq!(
            source.get("sbx-1", "exec-1", "db").await.unwrap().expose(),
            b"sk"
        );
    }

    #[tokio::test]
    async fn an_invalid_name_or_an_empty_identity_never_reaches_the_resolver() {
        let fake = Fake::default();
        answering(&fake, StatusCode::OK, json!({"fields": {"user": "u"}}));
        let source = serve(fake.clone(), "").await;
        for (sandbox, execution, name) in [
            ("sbx-1", "exec-1", "../escape"),
            ("sbx-1", "", "db"),
            ("", "exec-1", "db"),
        ] {
            assert_eq!(
                source.get_fields(sandbox, execution, name).await.err(),
                Some(CredentialError::Denied)
            );
        }
        assert!(fake.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_pin_the_resolver_states_reaches_the_handler() {
        let fake = Fake::default();
        answering(
            &fake,
            StatusCode::OK,
            json!({"value": "sk", "allowedHosts": ["api.openai.com", "*.github.com"]}),
        );
        let source = serve(fake, "").await;
        let secret = source.get("sbx-1", "exec-1", "openai").await.unwrap();
        assert_eq!(secret.allowed_hosts(), ["api.openai.com", "*.github.com"]);
        assert!(!secret.may_reach("evil.example"));
    }

    #[tokio::test]
    async fn an_expiry_the_resolver_states_reaches_the_cache() {
        let fake = Fake::default();
        answering(
            &fake,
            StatusCode::OK,
            json!({"fields": {"user": "u"}, "expiresAtUnix": 1_800_000_000}),
        );
        let source = serve(fake, "").await;
        let fields = source.get_fields("sbx-1", "exec-1", "db").await.unwrap();
        assert_eq!(
            fields.expires_at(),
            Some(UNIX_EPOCH + Duration::from_secs(1_800_000_000))
        );
    }
}
