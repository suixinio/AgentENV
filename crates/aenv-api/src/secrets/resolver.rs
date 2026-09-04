use std::time::Duration;

use aenv_core::cfg::SecretsResolverConfig;
use aenv_core::secrets::{SecretString, SecretsBackend, SecretsError};
use anyhow::{Context, Result};
use async_trait::async_trait;
use reqwest::StatusCode;
use serde::Serialize;
use url::Url;
use zeroize::Zeroizing;

/// An operator-run service that owns the credentials themselves: this half
/// records which (sandbox, execution) may read which name, and the broker
/// asks the same service for the value. Nothing here writes or reads a value,
/// so `/secrets` refuses to store one against this backend.
pub struct ExternalResolverBackend {
    client: reqwest::Client,
    grant: Url,
    revoke: Url,
    exists: Url,
    token: Option<Zeroizing<String>>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct GrantRequest<'a> {
    sandbox_id: &'a str,
    execution_id: &'a str,
    names: &'a [String],
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RevokeRequest<'a> {
    sandbox_id: &'a str,
    execution_id: &'a str,
}

#[derive(Serialize)]
struct ExistsRequest<'a> {
    names: &'a [String],
}

impl ExternalResolverBackend {
    pub fn from_config(config: &SecretsResolverConfig) -> Result<Self> {
        let url = config
            .url
            .as_deref()
            .filter(|url| !url.trim().is_empty())
            .context("secrets.resolver.url is required")?;
        let token = match config.token_file.as_ref() {
            Some(path) => Some(
                std::fs::read_to_string(path)
                    .with_context(|| format!("read secrets.resolver.token_file {path:?}"))?,
            ),
            None => config.token.clone(),
        };
        Self::new(
            url,
            token.as_deref(),
            Duration::from_millis(config.timeout_ms),
        )
    }

    /// `base` is treated as a directory URL, so a configured path prefix
    /// survives joining the call paths onto it.
    pub fn new(base: &str, token: Option<&str>, timeout: Duration) -> Result<Self> {
        let base = base.trim();
        let base = Url::parse(&if base.ends_with('/') {
            base.to_string()
        } else {
            format!("{base}/")
        })
        .context("secrets.resolver.url is not a URL")?;
        Ok(Self {
            client: reqwest::Client::builder()
                .timeout(timeout)
                .build()
                .context("build the resolver HTTP client")?,
            grant: base.join("grants").context("build the grant url")?,
            revoke: base.join("grants/revoke").context("build the revoke url")?,
            exists: base
                .join("credentials/exists")
                .context("build the exists url")?,
            token: token
                .map(str::trim)
                .filter(|token| !token.is_empty())
                .map(|token| Zeroizing::new(token.to_string())),
        })
    }

    async fn post<B: Serialize>(
        &self,
        url: &Url,
        body: &B,
    ) -> Result<serde_json::Value, SecretsError> {
        let mut request = self.client.post(url.clone()).json(body);
        if let Some(token) = &self.token {
            request = request.bearer_auth(token.as_str());
        }
        let response = request
            .send()
            .await
            .map_err(|err| SecretsError::Unavailable(err.into()))?;
        let status = response.status();
        if !status.is_success() {
            return Err(SecretsError::Unavailable(anyhow::anyhow!(
                "the credential resolver answered {status} for {}",
                url.path()
            )));
        }
        if status == StatusCode::NO_CONTENT {
            return Ok(serde_json::Value::Null);
        }
        response
            .json()
            .await
            .or(Ok(serde_json::Value::Null))
            .map_err(|err: reqwest::Error| SecretsError::Unavailable(err.into()))
    }
}

#[async_trait]
impl SecretsBackend for ExternalResolverBackend {
    async fn put(
        &self,
        _name: &str,
        _value: &SecretString,
        _allowed_hosts: &[String],
    ) -> Result<i64, SecretsError> {
        Err(SecretsError::Unavailable(anyhow::anyhow!(
            "the credential resolver owns its values; register the name there, not through /secrets"
        )))
    }

    async fn delete(&self, _name: &str) -> Result<(), SecretsError> {
        Err(SecretsError::Unavailable(anyhow::anyhow!(
            "the credential resolver owns its values; remove the name there, not through /secrets"
        )))
    }

    async fn grant(
        &self,
        sandbox_id: &str,
        execution_id: &str,
        names: &[String],
    ) -> Result<(), SecretsError> {
        self.post(
            &self.grant,
            &GrantRequest {
                sandbox_id,
                execution_id,
                names,
            },
        )
        .await
        .map(|_| ())
    }

    async fn revoke(&self, sandbox_id: &str, execution_id: &str) -> Result<(), SecretsError> {
        self.post(
            &self.revoke,
            &RevokeRequest {
                sandbox_id,
                execution_id,
            },
        )
        .await
        .map(|_| ())
    }

    /// The resolver owns the names, so it is the one that can say which of
    /// them exist. An answer without a `missing` array is a malformed one and
    /// reports the store as unavailable rather than every name as present.
    async fn missing_names(&self, names: &[String]) -> Result<Option<Vec<String>>, SecretsError> {
        let answer = self.post(&self.exists, &ExistsRequest { names }).await?;
        let Some(missing) = answer.get("missing").and_then(|missing| missing.as_array()) else {
            return Err(SecretsError::Unavailable(anyhow::anyhow!(
                "the credential resolver answered without a \"missing\" array"
            )));
        };
        Ok(Some(
            missing
                .iter()
                .filter_map(|name| name.as_str())
                .map(str::to_string)
                .collect(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use axum::extract::State;
    use axum::http::{HeaderMap, StatusCode};
    use axum::routing::post;
    use axum::{Json, Router};
    use serde_json::{json, Value};

    use super::*;

    /// One call the fake resolver saw: its path, body and Authorization header.
    type SeenCall = (String, Value, Option<String>);

    #[derive(Clone, Default)]
    struct Fake {
        seen: Arc<Mutex<Vec<SeenCall>>>,
        exists_answer: Arc<Mutex<Value>>,
        status: Arc<Mutex<StatusCode>>,
    }

    async fn record(
        fake: &Fake,
        path: &str,
        headers: &HeaderMap,
        body: Value,
    ) -> (StatusCode, Json<Value>) {
        fake.seen.lock().unwrap().push((
            path.to_string(),
            body,
            headers
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                .map(str::to_string),
        ));
        let status = *fake.status.lock().unwrap();
        if path == "credentials/exists" {
            return (status, Json(fake.exists_answer.lock().unwrap().clone()));
        }
        (status, Json(json!({})))
    }

    async fn grants(
        State(fake): State<Fake>,
        headers: HeaderMap,
        Json(body): Json<Value>,
    ) -> (StatusCode, Json<Value>) {
        record(&fake, "grants", &headers, body).await
    }

    async fn revoke(
        State(fake): State<Fake>,
        headers: HeaderMap,
        Json(body): Json<Value>,
    ) -> (StatusCode, Json<Value>) {
        record(&fake, "grants/revoke", &headers, body).await
    }

    async fn exists(
        State(fake): State<Fake>,
        headers: HeaderMap,
        Json(body): Json<Value>,
    ) -> (StatusCode, Json<Value>) {
        record(&fake, "credentials/exists", &headers, body).await
    }

    async fn serve(fake: Fake, prefix: &str) -> ExternalResolverBackend {
        *fake.status.lock().unwrap() = StatusCode::OK;
        *fake.exists_answer.lock().unwrap() = json!({"missing": []});
        let app = Router::new()
            .route(&format!("{prefix}/grants"), post(grants))
            .route(&format!("{prefix}/grants/revoke"), post(revoke))
            .route(&format!("{prefix}/credentials/exists"), post(exists))
            .with_state(fake);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        ExternalResolverBackend::new(
            &format!("http://{addr}{prefix}"),
            Some("api-token"),
            Duration::from_secs(2),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn grants_and_revocations_reach_the_resolver_with_the_grant_tuple() {
        let fake = Fake::default();
        let backend = serve(fake.clone(), "").await;

        backend
            .grant("sbx-1", "exec-1", &["tenant_db".into()])
            .await
            .unwrap();
        backend.revoke("sbx-1", "exec-1").await.unwrap();

        let seen = fake.seen.lock().unwrap().clone();
        assert_eq!(
            seen[0].1,
            json!({"sandboxId": "sbx-1", "executionId": "exec-1", "names": ["tenant_db"]})
        );
        assert_eq!(seen[0].0, "grants");
        assert_eq!(seen[0].2.as_deref(), Some("Bearer api-token"));
        assert_eq!(seen[1].0, "grants/revoke");
        assert_eq!(
            seen[1].1,
            json!({"sandboxId": "sbx-1", "executionId": "exec-1"})
        );
    }

    #[tokio::test]
    async fn the_resolver_answers_which_names_it_does_not_know() {
        let fake = Fake::default();
        let backend = serve(fake.clone(), "/internal").await;
        *fake.exists_answer.lock().unwrap() = json!({"missing": ["absent"]});

        let missing = backend
            .missing_names(&["present".into(), "absent".into()])
            .await
            .unwrap();
        assert_eq!(missing, Some(vec!["absent".to_string()]));
        assert_eq!(
            fake.seen.lock().unwrap()[0].1,
            json!({"names": ["present", "absent"]})
        );
    }

    #[tokio::test]
    async fn an_answer_without_a_missing_array_is_an_outage_not_an_all_clear() {
        let fake = Fake::default();
        let backend = serve(fake.clone(), "").await;
        *fake.exists_answer.lock().unwrap() = json!({"ok": true});

        assert!(matches!(
            backend.missing_names(&["absent".into()]).await,
            Err(SecretsError::Unavailable(_))
        ));
    }

    #[tokio::test]
    async fn a_failing_resolver_is_an_outage_and_values_are_never_stored_here() {
        let fake = Fake::default();
        let backend = serve(fake.clone(), "").await;
        *fake.status.lock().unwrap() = StatusCode::INTERNAL_SERVER_ERROR;

        assert!(matches!(
            backend.grant("sbx-1", "exec-1", &["db".into()]).await,
            Err(SecretsError::Unavailable(_))
        ));
        assert!(matches!(
            backend.put("db", &SecretString::new("v".into()), &[]).await,
            Err(SecretsError::Unavailable(_))
        ));
        assert!(matches!(
            backend.delete("db").await,
            Err(SecretsError::Unavailable(_))
        ));
    }
}
