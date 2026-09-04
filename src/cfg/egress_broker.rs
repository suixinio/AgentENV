use std::fmt;
use std::path::PathBuf;

use anyhow::{bail, Result};
use confique::Config;
use serde::{Deserialize, Serialize};

use super::{ClusterConfig, ClusterNodeDiscoveryMode};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EgressBrokerMode {
    /// The node opens no brokered listeners; a sandbox with `rules` cannot be placed here.
    Disabled,
    /// The broker core runs inside `aenv-node` over an in-memory pipe. Single-node only.
    Embedded,
    /// Brokered streams go over TLS to the `aenv-egress` deployment.
    Remote,
}

impl EgressBrokerMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Embedded => "embedded",
            Self::Remote => "remote",
        }
    }
}

/// How this node reaches the egress broker for sandboxes that declare `rules`.
#[derive(Config, Clone)]
pub struct EgressBrokerConfig {
    #[config(default = "disabled", env = "AENV_EGRESS_BROKER_MODE")]
    pub mode: EgressBrokerMode,
    /// `host:port` of the broker; required in `remote` mode.
    #[config(env = "AENV_EGRESS_BROKER_ENDPOINT")]
    pub endpoint: Option<String>,
    /// PEM bundle that verifies the broker's server certificate.
    #[config(env = "AENV_EGRESS_BROKER_CA_CERT_PATH")]
    pub ca_cert_path: Option<PathBuf>,
    /// PEM bundle guests with `rules` trust for intercepted names, when it is
    /// a different CA from the one above. Unset means the two are one CA.
    /// Splitting them is what lets the leaf-signing CA carry name constraints
    /// without those constraints reaching the broker's own server certificate.
    #[config(env = "AENV_EGRESS_BROKER_GUEST_CA_CERT_PATH")]
    pub guest_ca_cert_path: Option<PathBuf>,
    /// HMAC key the identity header is signed with; required in `remote` mode.
    #[config(env = "AENV_EGRESS_BROKER_SHARED_SECRET")]
    pub shared_secret: Option<String>,
    #[config(default = 30_000u64, env = "AENV_EGRESS_BROKER_MAX_SKEW_MS")]
    pub max_skew_ms: u64,
    #[config(default = 256u32, env = "AENV_EGRESS_BROKER_PER_SANDBOX_CONNS")]
    pub per_sandbox_conns: u32,
    #[config(default = 20_000u32, env = "AENV_EGRESS_BROKER_NODE_CONNS")]
    pub node_conns: u32,
    #[config(default = 3_000u64, env = "AENV_EGRESS_BROKER_OPEN_TIMEOUT_MS")]
    pub open_timeout_ms: u64,
    /// Where the embedded broker's `tcp` handler may connect. Empty leaves
    /// that handler reaching nothing, which is what an embedded node without
    /// this key should do. `remote` mode reads the broker's own config
    /// instead.
    #[config(default = [])]
    pub embedded_tcp_allowed_cidrs: Vec<String>,
}

impl fmt::Debug for EgressBrokerConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EgressBrokerConfig")
            .field("mode", &self.mode)
            .field("endpoint", &self.endpoint)
            .field("ca_cert_path", &self.ca_cert_path)
            .field("guest_ca_cert_path", &self.guest_ca_cert_path)
            .field(
                "shared_secret",
                &self.shared_secret.as_ref().map(|_| "[redacted]"),
            )
            .field("max_skew_ms", &self.max_skew_ms)
            .field("per_sandbox_conns", &self.per_sandbox_conns)
            .field("node_conns", &self.node_conns)
            .field("open_timeout_ms", &self.open_timeout_ms)
            .field(
                "embedded_tcp_allowed_cidrs",
                &self.embedded_tcp_allowed_cidrs,
            )
            .finish()
    }
}

impl EgressBrokerConfig {
    pub fn validate(&self, cluster: &ClusterConfig) -> Result<()> {
        match self.mode {
            EgressBrokerMode::Disabled => {}
            EgressBrokerMode::Embedded => {
                if cluster.node_discovery_mode != ClusterNodeDiscoveryMode::Static
                    || cluster.static_discovery_nodes.len() > 1
                {
                    bail!(
                        "egress_broker.mode = \"embedded\" runs the broker inside one node process \
                         and is only valid with [cluster].node_discovery_mode = \"static\" and at \
                         most one static_discovery_nodes entry; a multi-node cluster needs \
                         mode = \"remote\""
                    );
                }
            }
            EgressBrokerMode::Remote => {
                for (name, missing) in [
                    ("endpoint", is_blank(self.endpoint.as_deref())),
                    (
                        "ca_cert_path",
                        self.ca_cert_path
                            .as_ref()
                            .is_none_or(|path| path.as_os_str().is_empty()),
                    ),
                    ("shared_secret", is_blank(self.shared_secret.as_deref())),
                ] {
                    if missing {
                        bail!("egress_broker.mode = \"remote\" requires egress_broker.{name}");
                    }
                }
            }
        }
        if self.max_skew_ms == 0 {
            bail!("egress_broker.max_skew_ms must be > 0");
        }
        if self.per_sandbox_conns == 0 || self.node_conns == 0 {
            bail!("egress_broker.per_sandbox_conns and egress_broker.node_conns must be > 0");
        }
        if self.open_timeout_ms == 0 {
            bail!("egress_broker.open_timeout_ms must be > 0");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SecretsBackendKind {
    /// `/secrets` answers 503 and `rules` referencing secrets are refused.
    Disabled,
    /// aenv-api's own PostgreSQL, values encrypted under a master key this
    /// half holds. It also serves the broker's resolve endpoint.
    Postgres,
}

impl SecretsBackendKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Postgres => "postgres",
        }
    }
}

/// Where the api half writes secret values and grants. Only `aenv-api` reads
/// this section; a node never holds a store credential.
#[derive(Config, Clone)]
pub struct SecretsConfig {
    #[config(default = "disabled", env = "AENV_SECRETS_BACKEND")]
    pub backend: SecretsBackendKind,
    #[config(nested)]
    pub pg: SecretsPgConfig,
}

impl fmt::Debug for SecretsConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SecretsConfig")
            .field("backend", &self.backend)
            .field("pg", &self.pg)
            .finish()
    }
}

/// Values in aenv-api's own PostgreSQL. Both credentials are files, never
/// inline values: an environment variable holding a master key is readable
/// from `/proc`, a crash dump and `kubectl describe`.
#[derive(Config, Clone)]
pub struct SecretsPgConfig {
    /// File holding the base64 32-byte key values are encrypted under. It is
    /// mounted from a Kubernetes Secret and never written to the database
    /// that holds the ciphertexts.
    #[config(env = "AENV_SECRETS_PG_KEY_FILE")]
    pub key_file: Option<PathBuf>,
    /// File holding the bearer the broker presents at the internal resolve
    /// endpoint. The broker's own `resolver.token_file` holds the same value.
    #[config(env = "AENV_SECRETS_PG_RESOLVER_TOKEN_FILE")]
    pub resolver_token_file: Option<PathBuf>,
}

impl fmt::Debug for SecretsPgConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SecretsPgConfig")
            .field("key_file", &self.key_file)
            .field("resolver_token_file", &self.resolver_token_file)
            .finish()
    }
}

impl SecretsConfig {
    pub fn validate(&self) -> Result<()> {
        match self.backend {
            SecretsBackendKind::Disabled => Ok(()),
            SecretsBackendKind::Postgres => {
                for (name, path) in [
                    ("key_file", &self.pg.key_file),
                    ("resolver_token_file", &self.pg.resolver_token_file),
                ] {
                    if path.as_ref().is_none_or(|p| p.as_os_str().is_empty()) {
                        bail!("secrets.backend = \"postgres\" requires secrets.pg.{name}");
                    }
                }
                Ok(())
            }
        }
    }
}

fn is_blank(value: Option<&str>) -> bool {
    value.is_none_or(|value| value.trim().is_empty())
}

#[cfg(test)]
mod secrets_backend_tests {
    use super::SecretsBackendKind;

    #[test]
    fn a_removed_backend_name_is_refused_rather_than_ignored() {
        // `docs/src/configuration/env-vars.md` says setting one of the removed
        // AENV_SECRETS_VAULT_* names does nothing, while AENV_SECRETS_BACKEND
        // still naming a removed backend is refused. That asymmetry is what
        // keeps a half-migrated manifest from starting against a store this
        // build cannot reach.
        for gone in ["vault", "external_resolver"] {
            let parsed: Result<SecretsBackendKind, _> =
                serde_json::from_value(serde_json::Value::String(gone.to_string()));
            assert!(parsed.is_err(), "{gone:?} still parses");
        }
        for kept in ["disabled", "postgres"] {
            let parsed: SecretsBackendKind =
                serde_json::from_value(serde_json::Value::String(kept.to_string())).unwrap();
            assert_eq!(parsed.as_str(), kept);
        }
    }
}
