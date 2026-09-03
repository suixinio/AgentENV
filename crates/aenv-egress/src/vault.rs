//! The broker's credential source: Vault KV v2, read only. A value is served
//! only when `<mount>/grants/<execution_id>` names the sandbox and the name.

use std::time::Duration;

use async_trait::async_trait;
use reqwest::{StatusCode, Url};
use zeroize::Zeroizing;

use crate::credential::{is_valid_secret_name, CredentialError, CredentialSource, Secret};

pub struct VaultSource {
    client: reqwest::Client,
    base: Url,
    token: Zeroizing<String>,
    mount: String,
    namespace: Option<String>,
}

impl VaultSource {
    pub fn new(
        addr: &str,
        token: &str,
        mount: &str,
        namespace: Option<String>,
        timeout: Duration,
    ) -> anyhow::Result<Self> {
        let base = Url::parse(addr.trim())?;
        if mount.is_empty() || mount.contains('/') {
            anyhow::bail!("the Vault mount must be one path segment");
        }
        Ok(Self {
            client: reqwest::Client::builder().timeout(timeout).build()?,
            base,
            token: Zeroizing::new(token.trim().to_string()),
            mount: mount.to_string(),
            namespace: namespace.filter(|ns| !ns.trim().is_empty()),
        })
    }

    async fn read(&self, path: &str) -> Result<Option<serde_json::Value>, CredentialError> {
        let url = self
            .base
            .join(&format!("v1/{}/data/{path}", self.mount))
            .map_err(|err| CredentialError::Unavailable(format!("build Vault url: {err}")))?;
        let mut request = self
            .client
            .get(url)
            .header("X-Vault-Token", self.token.as_str())
            .header("X-Vault-Request", "true");
        if let Some(namespace) = &self.namespace {
            request = request.header("X-Vault-Namespace", namespace);
        }
        let response = request
            .send()
            .await
            .map_err(|err| CredentialError::Unavailable(format!("vault: {err}")))?;
        match response.status() {
            StatusCode::NOT_FOUND => Ok(None),
            status if status.is_success() => {
                let json: serde_json::Value = response
                    .json()
                    .await
                    .map_err(|err| CredentialError::Unavailable(format!("vault body: {err}")))?;
                Ok(json.pointer("/data/data").cloned())
            }
            status => Err(CredentialError::Unavailable(format!(
                "vault answered {status}"
            ))),
        }
    }
}

#[async_trait]
impl CredentialSource for VaultSource {
    async fn get(
        &self,
        sandbox_id: &str,
        execution_id: &str,
        name: &str,
    ) -> Result<Secret, CredentialError> {
        if !is_valid_secret_name(name) || execution_id.contains('/') || execution_id.is_empty() {
            return Err(CredentialError::Denied);
        }
        let Some(grant) = self.read(&format!("grants/{execution_id}")).await? else {
            return Err(CredentialError::Denied);
        };
        let granted_sandbox = grant.get("sandbox_id").and_then(|v| v.as_str());
        let granted_names = grant
            .get("names")
            .and_then(|v| v.as_array())
            .map(|names| names.iter().any(|n| n.as_str() == Some(name)))
            .unwrap_or(false);
        if granted_sandbox != Some(sandbox_id) || !granted_names {
            return Err(CredentialError::Denied);
        }
        let Some(data) = self.read(&format!("secrets/{name}")).await? else {
            return Err(CredentialError::Denied);
        };
        match data.get("value").and_then(|v| v.as_str()) {
            Some(value) => Ok(Secret::new(value.as_bytes().to_vec(), None)),
            None => Err(CredentialError::Unavailable(
                "the secret carries no string value".into(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use axum::extract::{Path, State};
    use axum::http::StatusCode;
    use axum::routing::get;
    use axum::{Json, Router};
    use serde_json::{json, Value};

    use super::*;

    #[derive(Clone, Default)]
    struct Fake {
        docs: Arc<Mutex<HashMap<String, Value>>>,
    }

    async fn read(State(fake): State<Fake>, Path(path): Path<String>) -> (StatusCode, Json<Value>) {
        match fake.docs.lock().unwrap().get(&path) {
            Some(data) => (StatusCode::OK, Json(json!({ "data": { "data": data } }))),
            None => (StatusCode::NOT_FOUND, Json(json!({ "errors": [] }))),
        }
    }

    async fn serve(fake: Fake) -> VaultSource {
        let app = Router::new()
            .route("/v1/aenv/data/{*path}", get(read))
            .with_state(fake);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        VaultSource::new(
            &format!("http://{addr}"),
            "t",
            "aenv",
            None,
            Duration::from_secs(2),
        )
        .unwrap()
    }

    fn with(fake: &Fake, path: &str, value: Value) {
        fake.docs.lock().unwrap().insert(path.to_string(), value);
    }

    #[tokio::test]
    async fn a_granted_name_resolves_to_its_value() {
        let fake = Fake::default();
        with(
            &fake,
            "grants/exec-1",
            json!({ "sandbox_id": "sbx-1", "names": ["openai"] }),
        );
        with(&fake, "secrets/openai", json!({ "value": "sk-live" }));
        let source = serve(fake).await;
        let secret = source.get("sbx-1", "exec-1", "openai").await.unwrap();
        assert_eq!(secret.expose(), b"sk-live");
    }

    #[tokio::test]
    async fn no_grant_the_wrong_sandbox_or_an_ungranted_name_is_denied() {
        let fake = Fake::default();
        with(
            &fake,
            "grants/exec-1",
            json!({ "sandbox_id": "sbx-1", "names": ["openai"] }),
        );
        with(&fake, "secrets/openai", json!({ "value": "sk-live" }));
        with(&fake, "secrets/gh", json!({ "value": "ghp" }));
        let source = serve(fake).await;
        assert_eq!(
            source.get("sbx-1", "exec-2", "openai").await.err().unwrap(),
            CredentialError::Denied
        );
        assert_eq!(
            source.get("sbx-2", "exec-1", "openai").await.err().unwrap(),
            CredentialError::Denied
        );
        assert_eq!(
            source.get("sbx-1", "exec-1", "gh").await.err().unwrap(),
            CredentialError::Denied
        );
        assert_eq!(
            source.get("sbx-1", "exec-1", "../x").await.err().unwrap(),
            CredentialError::Denied
        );
    }

    #[tokio::test]
    async fn a_granted_name_whose_value_is_gone_is_denied_not_an_outage() {
        let fake = Fake::default();
        with(
            &fake,
            "grants/exec-1",
            json!({ "sandbox_id": "sbx-1", "names": ["openai"] }),
        );
        let source = serve(fake).await;
        assert_eq!(
            source.get("sbx-1", "exec-1", "openai").await.err().unwrap(),
            CredentialError::Denied
        );
    }

    #[tokio::test]
    async fn an_unreachable_store_is_unavailable() {
        let source = VaultSource::new(
            "http://127.0.0.1:9",
            "t",
            "aenv",
            None,
            Duration::from_millis(300),
        )
        .unwrap();
        assert!(matches!(
            source.get("s", "e", "n").await,
            Err(CredentialError::Unavailable(_))
        ));
    }
}
