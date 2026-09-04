use std::time::Duration;

use aenv_core::cfg::VaultConfig;
use aenv_core::secrets::{SecretValue, SecretsBackend, SecretsError};
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use reqwest::{Method, StatusCode};
use url::Url;
use zeroize::Zeroizing;

/// HashiCorp Vault KV v2. Values live at `<mount>/secrets/<name>`, grants at
/// `<mount>/grants/<execution_id>`; the broker reads both, this side only
/// writes. Responses are read for status and version, never for a value.
pub struct VaultKv2Backend {
    client: reqwest::Client,
    base: Url,
    token: Zeroizing<String>,
    mount: String,
    namespace: Option<String>,
}

impl VaultKv2Backend {
    pub fn from_config(config: &VaultConfig) -> Result<Self> {
        let addr = config
            .addr
            .as_deref()
            .filter(|addr| !addr.trim().is_empty())
            .context("secrets.vault.addr is required")?;
        let token = config
            .token
            .as_deref()
            .filter(|token| !token.trim().is_empty())
            .context("secrets.vault.token is required")?;
        Self::new(
            addr,
            token,
            &config.mount,
            config.namespace.clone(),
            Duration::from_millis(config.timeout_ms),
        )
    }

    pub fn new(
        addr: &str,
        token: &str,
        mount: &str,
        namespace: Option<String>,
        timeout: Duration,
    ) -> Result<Self> {
        let base = Url::parse(addr.trim()).context("secrets.vault.addr is not a URL")?;
        if mount.is_empty() || mount.contains('/') {
            anyhow::bail!("secrets.vault.mount must be one path segment");
        }
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .context("build the Vault HTTP client")?;
        Ok(Self {
            client,
            base,
            token: Zeroizing::new(token.trim().to_string()),
            mount: mount.to_string(),
            namespace: namespace.filter(|ns| !ns.trim().is_empty()),
        })
    }

    fn url(&self, kind: &str, path: &str) -> Result<Url, SecretsError> {
        self.base
            .join(&format!("v1/{}/{kind}/{path}", self.mount))
            .map_err(|err| SecretsError::Unavailable(anyhow!("build Vault url: {err}")))
    }

    async fn send(
        &self,
        method: Method,
        url: Url,
        body: Option<serde_json::Value>,
    ) -> Result<(StatusCode, Option<serde_json::Value>), SecretsError> {
        let mut request = self
            .client
            .request(method.clone(), url.clone())
            .header("X-Vault-Token", self.token.as_str())
            .header("X-Vault-Request", "true");
        if let Some(namespace) = &self.namespace {
            request = request.header("X-Vault-Namespace", namespace);
        }
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request.send().await.map_err(|err| {
            SecretsError::Unavailable(anyhow!("{method} {}: {err}", redacted(&url)))
        })?;
        let status = response.status();
        let json = if status == StatusCode::NO_CONTENT {
            None
        } else {
            response.json::<serde_json::Value>().await.ok()
        };
        Ok((status, json))
    }

    fn expect_success(
        method: &Method,
        url: &Url,
        status: StatusCode,
        allow_not_found: bool,
    ) -> Result<(), SecretsError> {
        if status.is_success() || (allow_not_found && status == StatusCode::NOT_FOUND) {
            Ok(())
        } else {
            Err(SecretsError::Unavailable(anyhow!(
                "{method} {} answered {status}",
                redacted(url)
            )))
        }
    }
}

// Paths carry names and execution ids, never values; the host is enough for
// an operator to identify the store.
fn redacted(url: &Url) -> String {
    format!("{}{}", url.origin().ascii_serialization(), url.path())
}

#[async_trait]
impl SecretsBackend for VaultKv2Backend {
    async fn put(
        &self,
        name: &str,
        value: &SecretValue,
        allowed_hosts: &[String],
    ) -> Result<i64, SecretsError> {
        let url = self.url("data", &format!("secrets/{name}"))?;
        // The pin lives with the value because the broker reads it from the
        // same document; a version written without one is unpinned. Fields
        // are top-level keys of the same document, which is what the
        // broker's structured read takes everything but these two to be.
        let mut data = serde_json::Map::new();
        match value {
            SecretValue::Opaque(value) => {
                data.insert("value".into(), value.expose().into());
            }
            SecretValue::Fields(fields) => {
                for key in ["value", "allowed_hosts"] {
                    if fields.contains_key(key) {
                        return Err(SecretsError::InvalidMetadata(format!(
                            "this store keeps fields beside the value in one document, so it \
                             cannot hold a field named {key:?}"
                        )));
                    }
                }
                for (key, field) in fields {
                    data.insert(key.clone(), field.expose().into());
                }
            }
        }
        data.insert("allowed_hosts".into(), allowed_hosts.into());
        let body = serde_json::json!({ "data": data });
        let (status, json) = self.send(Method::POST, url.clone(), Some(body)).await?;
        Self::expect_success(&Method::POST, &url, status, false)?;
        json.as_ref()
            .and_then(|json| json.pointer("/data/version"))
            .and_then(serde_json::Value::as_i64)
            .ok_or_else(|| {
                SecretsError::Unavailable(anyhow!(
                    "POST {} answered without data.version",
                    redacted(&url)
                ))
            })
    }

    async fn delete(&self, name: &str) -> Result<(), SecretsError> {
        let url = self.url("metadata", &format!("secrets/{name}"))?;
        let (status, _) = self.send(Method::DELETE, url.clone(), None).await?;
        Self::expect_success(&Method::DELETE, &url, status, true)
    }

    async fn grant(
        &self,
        sandbox_id: &str,
        execution_id: &str,
        names: &[String],
    ) -> Result<(), SecretsError> {
        let url = self.url("data", &format!("grants/{execution_id}"))?;
        let body = serde_json::json!({
            "data": { "sandbox_id": sandbox_id, "execution_id": execution_id, "names": names }
        });
        let (status, _) = self.send(Method::POST, url.clone(), Some(body)).await?;
        Self::expect_success(&Method::POST, &url, status, false)
    }

    async fn revoke(&self, _sandbox_id: &str, execution_id: &str) -> Result<(), SecretsError> {
        let url = self.url("metadata", &format!("grants/{execution_id}"))?;
        let (status, _) = self.send(Method::DELETE, url.clone(), None).await?;
        Self::expect_success(&Method::DELETE, &url, status, true)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use axum::extract::{Path, State};
    use axum::http::{HeaderMap, StatusCode};
    use axum::routing::post;
    use axum::{Json, Router};
    use serde_json::Value;

    use super::*;

    #[derive(Clone, Debug)]
    struct Seen {
        method: String,
        path: String,
        token: Option<String>,
        namespace: Option<String>,
        body: Option<Value>,
    }

    #[derive(Clone, Default)]
    struct Fake {
        seen: Arc<Mutex<Vec<Seen>>>,
        fail_writes: bool,
    }

    async fn write(
        State(fake): State<Fake>,
        Path(path): Path<String>,
        headers: HeaderMap,
        Json(body): Json<Value>,
    ) -> (StatusCode, Json<Value>) {
        fake.seen.lock().unwrap().push(Seen {
            method: "POST".into(),
            path,
            token: header(&headers, "x-vault-token"),
            namespace: header(&headers, "x-vault-namespace"),
            body: Some(body),
        });
        if fake.fail_writes {
            return (
                StatusCode::FORBIDDEN,
                Json(serde_json::json!({ "errors": ["permission denied"] })),
            );
        }
        (
            StatusCode::OK,
            Json(serde_json::json!({ "data": { "version": 7 } })),
        )
    }

    async fn remove(
        State(fake): State<Fake>,
        Path(path): Path<String>,
        headers: HeaderMap,
    ) -> StatusCode {
        fake.seen.lock().unwrap().push(Seen {
            method: "DELETE".into(),
            path,
            token: header(&headers, "x-vault-token"),
            namespace: header(&headers, "x-vault-namespace"),
            body: None,
        });
        if fake.fail_writes {
            StatusCode::FORBIDDEN
        } else {
            StatusCode::NO_CONTENT
        }
    }

    fn opaque(value: &str) -> SecretValue {
        SecretValue::Opaque(value.to_string().into())
    }

    fn fields(pairs: &[(&str, &str)]) -> SecretValue {
        SecretValue::Fields(
            pairs
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string().into()))
                .collect(),
        )
    }

    fn header(headers: &HeaderMap, name: &str) -> Option<String> {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
    }

    async fn serve(fake: Fake) -> String {
        let app = Router::new()
            .route("/v1/{*path}", post(write).delete(remove))
            .with_state(fake);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{addr}")
    }

    fn backend(addr: &str) -> VaultKv2Backend {
        VaultKv2Backend::new(
            addr,
            "s.token",
            "aenv",
            Some("admin".into()),
            Duration::from_secs(2),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn put_writes_the_value_under_the_mount_and_returns_the_version() {
        let fake = Fake::default();
        let backend = backend(&serve(fake.clone()).await);
        let version = backend
            .put("openai", &opaque("sk-live"), &[])
            .await
            .unwrap();
        assert_eq!(version, 7);

        let seen = fake.seen.lock().unwrap().clone();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].method, "POST");
        assert_eq!(seen[0].path, "aenv/data/secrets/openai");
        assert_eq!(seen[0].token.as_deref(), Some("s.token"));
        assert_eq!(seen[0].namespace.as_deref(), Some("admin"));
        assert_eq!(
            seen[0].body,
            Some(serde_json::json!({
                "data": { "value": "sk-live", "allowed_hosts": [] }
            }))
        );
    }

    #[tokio::test]
    async fn a_pinned_value_carries_its_hosts_into_the_same_document() {
        let fake = Fake::default();
        let backend = backend(&serve(fake.clone()).await);
        backend
            .put(
                "openai",
                &opaque("sk-live"),
                &["api.openai.com".to_string(), "*.github.com".to_string()],
            )
            .await
            .unwrap();

        let seen = fake.seen.lock().unwrap().clone();
        assert_eq!(
            seen[0].body,
            Some(serde_json::json!({
                "data": {
                    "value": "sk-live",
                    "allowed_hosts": ["api.openai.com", "*.github.com"]
                }
            })),
            "the broker reads the pin from the value document, not from a ref row"
        );
    }

    #[tokio::test]
    async fn a_structured_credential_becomes_the_document_the_broker_takes_apart() {
        let fake = Fake::default();
        let backend = backend(&serve(fake.clone()).await);
        backend
            .put(
                "tenant_db",
                &fields(&[("host", "pg.internal"), ("port", "5432"), ("password", "p")]),
                &[],
            )
            .await
            .unwrap();

        assert_eq!(
            fake.seen.lock().unwrap()[0].body,
            Some(serde_json::json!({
                "data": {
                    "host": "pg.internal", "port": "5432", "password": "p",
                    "allowed_hosts": []
                }
            })),
            "the broker reads every key but value and allowed_hosts as a field"
        );

        // The two reserved keys of that shape have nowhere to go here.
        for reserved in ["value", "allowed_hosts"] {
            assert!(matches!(
                backend.put("tenant_db", &fields(&[(reserved, "x")]), &[]).await,
                Err(SecretsError::InvalidMetadata(message)) if message.contains(reserved)
            ));
        }
    }

    #[tokio::test]
    async fn grants_and_revocations_address_the_execution() {
        let fake = Fake::default();
        let backend = backend(&serve(fake.clone()).await);
        backend
            .grant("sbx-1", "exec-1", &["openai".into(), "gh".into()])
            .await
            .unwrap();
        backend.revoke("sbx-1", "exec-1").await.unwrap();
        backend.delete("openai").await.unwrap();

        let seen = fake.seen.lock().unwrap().clone();
        assert_eq!(
            seen.iter()
                .map(|s| (s.method.as_str(), s.path.as_str()))
                .collect::<Vec<_>>(),
            [
                ("POST", "aenv/data/grants/exec-1"),
                ("DELETE", "aenv/metadata/grants/exec-1"),
                ("DELETE", "aenv/metadata/secrets/openai"),
            ]
        );
        assert_eq!(
            seen[0].body,
            Some(serde_json::json!({
                "data": { "sandbox_id": "sbx-1", "execution_id": "exec-1", "names": ["openai", "gh"] }
            }))
        );
    }

    #[tokio::test]
    async fn a_refusal_is_unavailable_and_names_no_value() {
        let fake = Fake {
            fail_writes: true,
            ..Default::default()
        };
        let backend = backend(&serve(fake).await);
        let err = backend
            .put("openai", &opaque("sk-live"), &[])
            .await
            .err()
            .unwrap();
        let text = format!("{err:#}");
        assert!(matches!(err, SecretsError::Unavailable(_)));
        assert!(text.contains("403"));
        assert!(!text.contains("sk-live"));
        assert!(!text.contains("s.token"));
    }

    #[tokio::test]
    async fn an_unreachable_store_is_unavailable() {
        let backend = VaultKv2Backend::new(
            "http://127.0.0.1:9",
            "t",
            "aenv",
            None,
            Duration::from_millis(500),
        )
        .unwrap();
        assert!(matches!(
            backend.revoke("s", "e").await,
            Err(SecretsError::Unavailable(_))
        ));
    }

    #[test]
    fn the_mount_must_be_one_segment_and_the_addr_a_url() {
        assert!(
            VaultKv2Backend::new("not a url", "t", "aenv", None, Duration::from_secs(1)).is_err()
        );
        assert!(
            VaultKv2Backend::new("http://v:8200", "t", "a/b", None, Duration::from_secs(1))
                .is_err()
        );
    }
}
