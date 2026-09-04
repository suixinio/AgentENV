//! The broker's credential source: Vault KV v2, read only. A value is served
//! only when `<mount>/grants/<execution_id>` names the sandbox and the name.

use std::time::Duration;

use async_trait::async_trait;
use reqwest::{StatusCode, Url};
use zeroize::Zeroizing;

use crate::credential::{
    is_valid_secret_name, CredentialError, CredentialFields, CredentialSource, Secret,
};

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

impl VaultSource {
    /// The secret's data, once a grant for `execution_id` covers `name`.
    async fn granted_secret(
        &self,
        sandbox_id: &str,
        execution_id: &str,
        name: &str,
    ) -> Result<serde_json::Value, CredentialError> {
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
        self.read(&format!("secrets/{name}"))
            .await?
            .ok_or(CredentialError::Denied)
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
        let data = self.granted_secret(sandbox_id, execution_id, name).await?;
        match data.get("value").and_then(|v| v.as_str()) {
            Some(value) => Ok(Secret::new(value.as_bytes().to_vec(), None)
                .with_allowed_hosts(allowed_hosts(data.get("allowed_hosts")))),
            None => Err(CredentialError::Unavailable(
                "the secret carries no string value".into(),
            )),
        }
    }

    /// Every key of the secret except `value`, which belongs to the opaque
    /// form. A secret written only as `value` has no structured form here.
    async fn get_fields(
        &self,
        sandbox_id: &str,
        execution_id: &str,
        name: &str,
    ) -> Result<CredentialFields, CredentialError> {
        let data = self.granted_secret(sandbox_id, execution_id, name).await?;
        let Some(object) = data.as_object() else {
            return Err(CredentialError::Unavailable(
                "the secret is not an object".into(),
            ));
        };
        let mut object = object.clone();
        object.remove("value");
        object.remove("allowed_hosts");
        let fields = CredentialFields::from_json(&object, None)?;
        if fields.is_empty() {
            return Err(CredentialError::Unavailable(
                "the secret carries no fields besides value".into(),
            ));
        }
        Ok(fields)
    }
}

/// A `allowed_hosts` written as a JSON array or as one comma-separated
/// string, which is what a `vault kv put` on the command line produces.
pub(crate) fn allowed_hosts(value: Option<&serde_json::Value>) -> Vec<String> {
    match value {
        Some(serde_json::Value::Array(entries)) => entries
            .iter()
            .filter_map(|entry| entry.as_str())
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
            .map(str::to_string)
            .collect(),
        Some(serde_json::Value::String(entries)) => entries
            .split(',')
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
            .map(str::to_string)
            .collect(),
        _ => Vec::new(),
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
    async fn a_granted_secret_reads_as_fields_with_value_left_to_the_opaque_form() {
        let fake = Fake::default();
        with(
            &fake,
            "grants/exec-1",
            json!({ "sandbox_id": "sbx-1", "names": ["tenant_db"] }),
        );
        with(
            &fake,
            "secrets/tenant_db",
            json!({ "value": "ignored", "host": "db.internal", "port": 5432, "user": "rw_app" }),
        );
        let source = serve(fake).await;
        let fields = source
            .get_fields("sbx-1", "exec-1", "tenant_db")
            .await
            .unwrap();

        assert_eq!(fields.get("host"), Some("db.internal"));
        assert_eq!(fields.get("port"), Some("5432"));
        assert_eq!(fields.get("user"), Some("rw_app"));
        assert_eq!(fields.get("value"), None);
        assert_eq!(fields.get("password"), None);
        assert!(!format!("{fields:?}").contains("rw_app"));
    }

    #[tokio::test]
    async fn a_secret_with_only_a_value_has_no_structured_form_and_no_grant_is_denied() {
        let fake = Fake::default();
        with(
            &fake,
            "grants/exec-1",
            json!({ "sandbox_id": "sbx-1", "names": ["openai"] }),
        );
        with(&fake, "secrets/openai", json!({ "value": "sk-live" }));
        let source = serve(fake).await;
        assert!(matches!(
            source.get_fields("sbx-1", "exec-1", "openai").await,
            Err(CredentialError::Unavailable(_))
        ));
        assert_eq!(
            source
                .get_fields("sbx-1", "exec-2", "openai")
                .await
                .err()
                .unwrap(),
            CredentialError::Denied
        );
    }

    #[tokio::test]
    async fn a_secret_pinned_to_hosts_carries_that_pin_to_the_handler() {
        let fake = Fake::default();
        with(
            &fake,
            "grants/exec-1",
            json!({ "sandbox_id": "sbx-1", "names": ["openai", "gh"] }),
        );
        with(
            &fake,
            "secrets/openai",
            json!({ "value": "sk", "allowed_hosts": ["api.openai.com"] }),
        );
        with(
            &fake,
            "secrets/gh",
            json!({ "value": "ghp", "allowed_hosts": " *.github.com , codeload.github.com " }),
        );
        let source = serve(fake).await;

        let openai = source.get("sbx-1", "exec-1", "openai").await.unwrap();
        assert_eq!(openai.allowed_hosts(), ["api.openai.com"]);
        assert!(openai.may_reach("api.openai.com"));
        assert!(!openai.may_reach("evil.example"));

        let gh = source.get("sbx-1", "exec-1", "gh").await.unwrap();
        assert_eq!(gh.allowed_hosts(), ["*.github.com", "codeload.github.com"]);

        // The pin is not a credential field: it configures the value, it is
        // not part of it.
        assert!(matches!(
            source.get_fields("sbx-1", "exec-1", "openai").await,
            Err(CredentialError::Unavailable(_))
        ));
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
