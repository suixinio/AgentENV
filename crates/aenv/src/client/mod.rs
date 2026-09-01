pub mod files;
pub mod sandboxes;
pub mod snapshots;
pub mod templates;

use crate::auth::Credentials;
use crate::grpc::Transport;
use anyhow::{anyhow, bail, Result};
use std::time::Duration;
use ureq::Agent;

#[derive(Clone)]
pub struct Client {
    agent: Agent,
    base: String,
    /// Sandbox traffic goes here; the two addresses coincide until a deployment
    /// separates them.
    proxy_base: String,
    api_key: String,
}

impl Client {
    pub fn from_env() -> Result<Self> {
        let creds = Credentials::load()?;
        Self::with_proxy(&creds.url, creds.data_plane_url(), &creds.api_key)
    }

    pub fn new(url: &str, api_key: &str) -> Result<Self> {
        Self::with_proxy(url, url, api_key)
    }

    /// `url` answers REST; `proxy_url` carries sandbox data-plane traffic.
    pub fn with_proxy(url: &str, proxy_url: &str, api_key: &str) -> Result<Self> {
        let base = url.trim_end_matches('/').to_string();
        let proxy_base = proxy_url.trim_end_matches('/').to_string();
        let agent = ureq::AgentBuilder::new()
            .timeout_connect(Duration::from_secs(5))
            .timeout(Duration::from_secs(120))
            .build();
        Ok(Self {
            agent,
            base,
            proxy_base,
            api_key: api_key.to_string(),
        })
    }

    /// The data-plane base every envd connection is opened against.
    pub fn proxy_base(&self) -> &str {
        &self.proxy_base
    }

    pub fn transport(
        &self,
        sandbox_id: &str,
        envd_access_token: Option<&str>,
    ) -> Result<Transport> {
        Transport::new(
            &self.proxy_base,
            &self.api_key,
            sandbox_id,
            envd_access_token,
        )
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base, path)
    }

    pub fn get(&self, path: &str) -> ureq::Request {
        self.agent
            .get(&self.url(path))
            .set("X-API-Key", &self.api_key)
    }

    pub fn post(&self, path: &str) -> ureq::Request {
        self.agent
            .post(&self.url(path))
            .set("X-API-Key", &self.api_key)
    }

    pub fn delete(&self, path: &str) -> ureq::Request {
        self.agent
            .delete(&self.url(path))
            .set("X-API-Key", &self.api_key)
    }
}

impl Credentials {
    pub fn load() -> Result<Self> {
        crate::auth::load()
    }
}

pub fn handle_status(resp: Result<ureq::Response, ureq::Error>) -> Result<ureq::Response> {
    match resp {
        Ok(r) => Ok(r),
        Err(ureq::Error::Status(code, resp)) => {
            let body = resp.into_string().unwrap_or_default();
            let msg = parse_api_error(&body).unwrap_or_else(|| body.clone());
            bail!("HTTP {}: {}", code, msg.trim())
        }
        Err(ureq::Error::Transport(t)) => Err(anyhow!(t).context("transport error")),
    }
}

fn parse_api_error(body: &str) -> Option<String> {
    #[derive(serde::Deserialize)]
    struct ApiError {
        message: Option<String>,
    }
    serde_json::from_str::<ApiError>(body)
        .ok()
        .and_then(|e| e.message)
}

#[cfg(test)]
mod tests {
    use super::Client;

    #[test]
    fn one_address_still_carries_both_surfaces() {
        let client = Client::new("http://gateway:8000/", "k").expect("a client");

        assert_eq!(client.url("/sandboxes"), "http://gateway:8000/sandboxes");
        assert_eq!(client.proxy_base(), "http://gateway:8000");
    }

    #[test]
    fn a_data_plane_address_does_not_move_the_rest_address() {
        let client =
            Client::with_proxy("http://api:8010", "http://gateway:8000/", "k").expect("a client");

        assert_eq!(client.url("/sandboxes"), "http://api:8010/sandboxes");
        assert_eq!(client.proxy_base(), "http://gateway:8000");
    }
}
