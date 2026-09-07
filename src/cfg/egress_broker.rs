use std::fmt;
use std::path::PathBuf;

use anyhow::{bail, Result};
use confique::Config;
use serde::{Deserialize, Serialize};

use super::{ClusterConfig, ClusterNodeDiscoveryMode};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EgressBrokerMode {
    /// The node opens no brokered listeners; a sandbox with `rules` cannot be placed here.
    Disabled,
    /// The broker core runs inside `aenv-node` over an in-memory pipe. Single-node only.
    Embedded,
    /// Brokered streams go over a Unix socket to the `aenv-egress` DaemonSet
    /// on this same node.
    Local,
}

impl<'de> Deserialize<'de> for EgressBrokerMode {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        match raw.as_str() {
            "disabled" => Ok(Self::Disabled),
            "embedded" => Ok(Self::Embedded),
            "local" => Ok(Self::Local),
            "remote" => Err(serde::de::Error::custom(
                "egress_broker.mode = \"remote\" no longer exists: the broker runs as a per-node \
                 DaemonSet reached over a Unix socket. Set mode = \"local\" together with \
                 egress_broker.socket_path, and see the removed AENV_EGRESS_BROKER_ENDPOINT row \
                 in docs/src/configuration/env-vars.md",
            )),
            other => Err(serde::de::Error::custom(format!(
                "unknown egress_broker.mode {other:?}; expected \"disabled\", \"embedded\" or \"local\""
            ))),
        }
    }
}

impl EgressBrokerMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Embedded => "embedded",
            Self::Local => "local",
        }
    }
}

/// How this node reaches the egress broker for sandboxes that declare `rules`.
#[derive(Config, Clone)]
pub struct EgressBrokerConfig {
    #[config(default = "disabled", env = "AENV_EGRESS_BROKER_MODE")]
    pub mode: EgressBrokerMode,
    /// The broker's Unix socket on this node; required in `local` mode.
    #[config(env = "AENV_EGRESS_BROKER_SOCKET_PATH")]
    pub socket_path: Option<PathBuf>,
    /// PEM bundle guests with `rules` trust for intercepted names.
    #[config(env = "AENV_EGRESS_BROKER_GUEST_CA_CERT_PATH")]
    pub guest_ca_cert_path: Option<PathBuf>,
    /// The gid the broker runs under, which this node hands the socket
    /// directory to. It has to match `runAsGroup` in
    /// `deploy/k8s/base/aenv-egress-daemonset.yaml`: the broker creates the
    /// socket file in that directory, so its group needs write there.
    #[config(default = 65532u32, env = "AENV_EGRESS_BROKER_SOCKET_GROUP")]
    pub socket_group: u32,
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
            .field("socket_path", &self.socket_path)
            .field("guest_ca_cert_path", &self.guest_ca_cert_path)
            .field("socket_group", &self.socket_group)
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
                         mode = \"local\""
                    );
                }
            }
            EgressBrokerMode::Local => {
                if self
                    .socket_path
                    .as_ref()
                    .is_none_or(|path| path.as_os_str().is_empty())
                {
                    bail!("egress_broker.mode = \"local\" requires egress_broker.socket_path");
                }
            }
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

/// Values in aenv-api's own PostgreSQL. The master key is a file, never an
/// inline value: an environment variable holding one is readable from
/// `/proc`, a crash dump and `kubectl describe`.
#[derive(Config, Clone)]
pub struct SecretsPgConfig {
    /// File holding the base64 32-byte key values are encrypted under. It is
    /// mounted from a Kubernetes Secret and never written to the database
    /// that holds the ciphertexts.
    #[config(env = "AENV_SECRETS_PG_KEY_FILE")]
    pub key_file: Option<PathBuf>,
}

impl fmt::Debug for SecretsPgConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SecretsPgConfig")
            .field("key_file", &self.key_file)
            .finish()
    }
}

impl SecretsConfig {
    pub fn validate(&self) -> Result<()> {
        match self.backend {
            SecretsBackendKind::Disabled => Ok(()),
            SecretsBackendKind::Postgres => {
                if self
                    .pg
                    .key_file
                    .as_ref()
                    .is_none_or(|path| path.as_os_str().is_empty())
                {
                    bail!("secrets.backend = \"postgres\" requires secrets.pg.key_file");
                }
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod egress_broker_mode_tests {
    use super::EgressBrokerMode;

    #[test]
    fn the_removed_mode_is_refused_with_the_name_of_its_replacement() {
        let err = serde_json::from_value::<EgressBrokerMode>(serde_json::json!("remote"))
            .expect_err("remote is gone");

        let message = err.to_string();
        assert!(message.contains("local"), "{message}");
        assert!(message.contains("socket_path"), "{message}");
        assert!(message.contains("env-vars.md"), "{message}");
    }

    #[test]
    fn the_modes_this_build_serves_still_parse() {
        for kept in ["disabled", "embedded", "local"] {
            let parsed: EgressBrokerMode =
                serde_json::from_value(serde_json::json!(kept)).expect("a mode this build serves");
            assert_eq!(parsed.as_str(), kept);
        }
        assert!(serde_json::from_value::<EgressBrokerMode>(serde_json::json!("nonsense")).is_err());
    }
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
