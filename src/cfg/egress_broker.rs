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
    /// PEM bundle that verifies the broker's server certificate and is handed
    /// to guests with `rules` as their extra trust anchor.
    #[config(env = "AENV_EGRESS_BROKER_CA_CERT_PATH")]
    pub ca_cert_path: Option<PathBuf>,
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
}

impl fmt::Debug for EgressBrokerConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EgressBrokerConfig")
            .field("mode", &self.mode)
            .field("endpoint", &self.endpoint)
            .field("ca_cert_path", &self.ca_cert_path)
            .field(
                "shared_secret",
                &self.shared_secret.as_ref().map(|_| "[redacted]"),
            )
            .field("max_skew_ms", &self.max_skew_ms)
            .field("per_sandbox_conns", &self.per_sandbox_conns)
            .field("node_conns", &self.node_conns)
            .field("open_timeout_ms", &self.open_timeout_ms)
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
    /// HashiCorp Vault KV v2.
    Vault,
}

impl SecretsBackendKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Vault => "vault",
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
    pub vault: VaultConfig,
}

impl fmt::Debug for SecretsConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SecretsConfig")
            .field("backend", &self.backend)
            .field("vault", &self.vault)
            .finish()
    }
}

#[derive(Config, Clone)]
pub struct VaultConfig {
    /// Base URL of the Vault server, such as `https://vault.vault.svc:8200`.
    #[config(env = "AENV_SECRETS_VAULT_ADDR")]
    pub addr: Option<String>,
    #[config(env = "AENV_SECRETS_VAULT_TOKEN")]
    pub token: Option<String>,
    /// KV v2 mount; values live under `<mount>/secrets/<name>`, grants under
    /// `<mount>/grants/<execution_id>`.
    #[config(default = "aenv", env = "AENV_SECRETS_VAULT_MOUNT")]
    pub mount: String,
    #[config(env = "AENV_SECRETS_VAULT_NAMESPACE")]
    pub namespace: Option<String>,
    #[config(default = 5_000u64, env = "AENV_SECRETS_VAULT_TIMEOUT_MS")]
    pub timeout_ms: u64,
}

impl fmt::Debug for VaultConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VaultConfig")
            .field("addr", &self.addr)
            .field("token", &self.token.as_ref().map(|_| "[redacted]"))
            .field("mount", &self.mount)
            .field("namespace", &self.namespace)
            .field("timeout_ms", &self.timeout_ms)
            .finish()
    }
}

impl SecretsConfig {
    pub fn validate(&self) -> Result<()> {
        match self.backend {
            SecretsBackendKind::Disabled => Ok(()),
            SecretsBackendKind::Vault => {
                if is_blank(self.vault.addr.as_deref()) {
                    bail!("secrets.backend = \"vault\" requires secrets.vault.addr");
                }
                if is_blank(self.vault.token.as_deref()) {
                    bail!("secrets.backend = \"vault\" requires secrets.vault.token");
                }
                if self.vault.mount.trim().is_empty() || self.vault.mount.contains('/') {
                    bail!("secrets.vault.mount must be a single non-empty path segment");
                }
                if self.vault.timeout_ms == 0 {
                    bail!("secrets.vault.timeout_ms must be > 0");
                }
                Ok(())
            }
        }
    }
}

fn is_blank(value: Option<&str>) -> bool {
    value.is_none_or(|value| value.trim().is_empty())
}
