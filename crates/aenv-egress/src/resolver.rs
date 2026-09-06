//! The broker's credential source: an HTTP endpoint that answers one grant
//! at a time. The api half serves it for `[secrets].backend = "postgres"`;
//! an operator's own service serves the same contract when the credentials
//! live there instead. Either may leave fields out, which the handler reads
//! as "pass the guest's value through".

use std::path::PathBuf;
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
    /// Read on every call, never held: the file may hold a projected
    /// ServiceAccount token, and kubelet rotates one in place well inside a
    /// broker's uptime. A value captured at startup would begin answering 401
    /// an hour in.
    token_file: PathBuf,
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
    /// its last segment when the call path is joined onto it. The endpoint
    /// always checks a bearer, so a file that holds none is refused here
    /// rather than answered 401 on every lookup.
    pub fn new(base: &str, token_file: PathBuf, timeout: Duration) -> anyhow::Result<Self> {
        let base = base.trim();
        let base = Url::parse(&if base.ends_with('/') {
            base.to_string()
        } else {
            format!("{base}/")
        })?;
        let source = Self {
            client: reqwest::Client::builder().timeout(timeout).build()?,
            resolve: base.join(RESOLVE_PATH)?,
            token_file,
        };
        if source.token()?.is_empty() {
            anyhow::bail!("the resolver token file {:?} is empty", source.token_file);
        }
        Ok(source)
    }

    fn token(&self) -> anyhow::Result<Zeroizing<String>> {
        let raw = std::fs::read_to_string(&self.token_file)
            .map_err(|err| anyhow::anyhow!("read {:?}: {err}", self.token_file))?;
        Ok(Zeroizing::new(raw.trim().to_string()))
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
        let response = self
            .client
            .post(self.resolve.clone())
            .json(&ResolveRequest {
                sandbox_id,
                execution_id,
                name,
            })
            .bearer_auth(
                self.token()
                    .map_err(|err| CredentialError::Unavailable(format!("{err:#}")))?
                    .as_str(),
            )
            .send()
            .await
            .map_err(|err| CredentialError::Unavailable(format!("resolver: {err}")))?;
        match response.status() {
            StatusCode::FORBIDDEN | StatusCode::NOT_FOUND => Err(CredentialError::Denied),
            // The endpoint refused this broker, not this grant: an operator
            // error that must read as an outage, not as a policy denial.
            StatusCode::UNAUTHORIZED => Err(CredentialError::Unavailable(
                "the resolver refused this broker's credential; resolver.token_file must hold \
                 either this Pod's projected token for the aenv-api audience or the value the \
                 api half's secrets.pg.resolver_token_file names"
                    .into(),
            )),
            // The api half reached its own store and could not answer. It is
            // an outage on that side, not a statement about this grant.
            StatusCode::SERVICE_UNAVAILABLE => Err(CredentialError::Unavailable(
                "the api half answered 503; its credential store or its routing table is \
                 unavailable"
                    .into(),
            )),
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

    /// A token file the source reads on every call, kept alive for the test.
    fn token_file(contents: &str) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("token");
        std::fs::write(&path, contents).unwrap();
        (dir, path)
    }

    async fn serve(fake: Fake, base_suffix: &str) -> (tempfile::TempDir, ResolverSource) {
        let app = Router::new()
            .route(&format!("{base_suffix}/credentials/resolve"), post(resolve))
            .with_state(fake);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let (dir, path) = token_file("resolver-token");
        let source = ResolverSource::new(
            &format!("http://{addr}{base_suffix}"),
            path,
            Duration::from_secs(2),
        )
        .unwrap();
        (dir, source)
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
            json!({"fields": {"host": "db.internal", "port": "5432", "user": "rw_app", "password": "p"}}),
        );
        let (_dir, source) = serve(fake.clone(), "").await;

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
        let (_dir, source) = serve(fake, "/internal/aenv").await;
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
        let (_dir, source) = serve(fake.clone(), "").await;
        assert_eq!(
            source.get_fields("sbx-1", "exec-1", "db").await.err(),
            Some(CredentialError::Denied)
        );

        answering(&fake, StatusCode::INTERNAL_SERVER_ERROR, json!({}));
        assert!(matches!(
            source.get_fields("sbx-1", "exec-1", "db").await,
            Err(CredentialError::Unavailable(_))
        ));

        let (_dir, path) = token_file("t");
        let unreachable =
            ResolverSource::new("http://127.0.0.1:9", path, Duration::from_millis(300)).unwrap();
        assert!(matches!(
            unreachable.get_fields("s", "e", "n").await,
            Err(CredentialError::Unavailable(_))
        ));
    }

    #[tokio::test]
    async fn a_refused_bearer_is_an_outage_the_operator_sees_not_a_denial() {
        let fake = Fake::default();
        answering(
            &fake,
            StatusCode::UNAUTHORIZED,
            json!({"error": "unauthorized"}),
        );
        let (_dir, source) = serve(fake, "").await;
        match source.get("sbx-1", "exec-1", "openai").await {
            Err(CredentialError::Unavailable(reason)) => {
                assert!(reason.contains("token_file"), "{reason}")
            }
            other => panic!("a 401 must not read as a policy denial, got {other:?}"),
        }
    }

    #[test]
    fn a_resolver_without_a_token_is_refused_at_construction() {
        for token in ["", "  \n"] {
            let (_dir, path) = token_file(token);
            assert!(
                ResolverSource::new("http://127.0.0.1:9", path, Duration::from_secs(1)).is_err()
            );
        }
        assert!(ResolverSource::new(
            "http://127.0.0.1:9",
            PathBuf::from("/nonexistent/token"),
            Duration::from_secs(1)
        )
        .is_err());
    }

    #[tokio::test]
    async fn a_rotated_token_reaches_the_next_call_without_a_restart() {
        let fake = Fake::default();
        answering(&fake, StatusCode::OK, json!({"value": "sk-live"}));
        let (dir, source) = serve(fake, "").await;

        source.get("sbx-1", "exec-1", "openai").await.unwrap();
        std::fs::write(dir.path().join("token"), "rotated").unwrap();

        assert_eq!(source.token().unwrap().as_str(), "rotated");
        source
            .get("sbx-1", "exec-1", "openai")
            .await
            .expect("the source reads the file again rather than the value it started with");
    }

    #[tokio::test]
    async fn an_answer_without_the_form_the_caller_asked_for_is_unavailable_not_denied() {
        let fake = Fake::default();
        answering(&fake, StatusCode::OK, json!({"fields": {"user": "u"}}));
        let (_dir, source) = serve(fake.clone(), "").await;
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
        let (_dir, source) = serve(fake.clone(), "").await;
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
        let (_dir, source) = serve(fake, "").await;
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
        let (_dir, source) = serve(fake, "").await;
        let fields = source.get_fields("sbx-1", "exec-1", "db").await.unwrap();
        assert_eq!(
            fields.expires_at(),
            Some(UNIX_EPOCH + Duration::from_secs(1_800_000_000))
        );
    }
}
