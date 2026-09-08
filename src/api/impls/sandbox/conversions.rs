use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, SystemTime};

use crate::orchestrator::{SandboxMetadata, SandboxState, SandboxTimeoutAction};
use crate::sandbox::network::policy::{DomainRule, EndpointDeclaration, HeaderTransform};
use crate::sandbox::{BaseSandboxNetworkPolicy, SandboxNetworkPolicy};
use agentenv_http_server::models;
use agentenv_http_server::types::Nullable;

impl From<SandboxState> for models::SandboxState {
    fn from(state: SandboxState) -> Self {
        match state {
            SandboxState::Pausing | SandboxState::Snapshotting | SandboxState::Forking => {
                Self::Paused
            }
            _ => Self::Running,
        }
    }
}

fn started_at(created_at: SystemTime) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::<chrono::Utc>::from(created_at)
}

fn end_at(expires_at: Option<SystemTime>) -> chrono::DateTime<chrono::Utc> {
    expires_at
        .map(chrono::DateTime::<chrono::Utc>::from)
        // distant future for non-expiring sandbox
        .unwrap_or(chrono::DateTime::<chrono::Utc>::from(
            std::time::UNIX_EPOCH + Duration::from_secs(60 * 60 * 24 * 365 * 100),
        ))
}

impl From<SandboxTimeoutAction> for models::SandboxOnTimeout {
    fn from(action: SandboxTimeoutAction) -> Self {
        match action {
            SandboxTimeoutAction::Pause => Self::Pause,
            SandboxTimeoutAction::Delete => Self::Kill,
        }
    }
}

impl From<SandboxMetadata> for models::ListedSandbox {
    fn from(m: SandboxMetadata) -> Self {
        Self {
            template_id: m.snapshot_id,
            alias: m.snapshot_alias,
            sandbox_id: m.id.into(),
            client_id: "".to_string(), // Deprecated field, only reserved for E2B Python SDK.
            started_at: started_at(m.created_at),
            end_at: end_at(m.expires_at),
            cpu_count: m.resources.cpu_count,
            memory_mb: m.resources.memory_mib,
            disk_size_mb: m.resources.disk_size_mib,
            metadata: m.user_metadata,
            state: m.state.into(),
            envd_version: m.runtime_versions.envd_version.clone(),
            // Preserve the incarnation used by gateway projection arbitration.
            execution_id: Some(m.execution_id.to_string()),
        }
    }
}

impl From<SandboxMetadata> for models::Sandbox {
    fn from(m: SandboxMetadata) -> Self {
        Self {
            template_id: m.snapshot_id,
            sandbox_id: m.id.into(),
            alias: m.snapshot_alias,
            client_id: "".to_string(), // Deprecated field, only reserved for E2B Python SDK.
            envd_version: m.runtime_versions.envd_version.clone(),
            envd_access_token: None,
            // The create that minted it and a resume of the same sandbox —
            // both of which already carry the control-plane credential. No
            // listing or detail response is built from this model.
            traffic_access_token: m.traffic_access_token.map(Nullable::Present),
            domain: None,
            execution_id: Some(m.execution_id.to_string()),
        }
    }
}

/// The network block a detail response carries.
///
/// `locked` is the sandbox's own `allowPublicTraffic`, which the policy does
/// not hold: the lock lives on the record, as the token minted with it. A
/// response that answered `true` for a locked sandbox would describe a
/// sandbox its own client cannot reach without a token.
pub(in crate::api) fn network_config_model(
    policy: &SandboxNetworkPolicy,
    locked: bool,
) -> models::SandboxNetworkConfig {
    let egress = &policy.egress;
    models::SandboxNetworkConfig {
        allow_public_traffic: Some(!locked),
        allow_out: (!egress.allowed_cidrs.is_empty() || !egress.allowed_domains.is_empty()).then(
            || {
                egress
                    .allowed_cidrs
                    .iter()
                    .chain(egress.allowed_domains.iter())
                    .cloned()
                    .collect()
            },
        ),
        deny_out: (!egress.denied_cidrs.is_empty()).then(|| egress.denied_cidrs.clone()),
        rules: (!egress.rules.is_empty()).then(|| rules_model(&egress.rules)),
        x_aenv_endpoints: (!egress.endpoints.is_empty())
            .then(|| endpoints_model(&egress.endpoints)),
        mask_request_host: None,
    }
}

fn rules_model(
    rules: &BTreeMap<String, Vec<DomainRule>>,
) -> HashMap<String, Vec<models::SandboxNetworkRule>> {
    rules
        .iter()
        .map(|(domain, domain_rules)| {
            (
                domain.clone(),
                domain_rules
                    .iter()
                    .map(|rule| models::SandboxNetworkRule {
                        transform: Some(models::SandboxNetworkTransform {
                            headers: Some(rule.transform.headers.clone().into_iter().collect()),
                        }),
                    })
                    .collect(),
            )
        })
        .collect()
}

fn endpoints_model(endpoints: &[EndpointDeclaration]) -> Vec<models::SandboxBrokeredEndpoint> {
    endpoints
        .iter()
        .map(|endpoint| models::SandboxBrokeredEndpoint {
            port: u32::from(endpoint.port),
            handler: endpoint.handler.clone(),
            params: endpoint.params.as_object().map(|params| {
                params
                    .iter()
                    .map(|(key, value)| {
                        (
                            key.clone(),
                            agentenv_http_server::types::Object(value.clone()),
                        )
                    })
                    .collect()
            }),
            intercept_port: Some(endpoint.intercept_port),
        })
        .collect()
}

pub(super) fn endpoints_from_model(
    endpoints: Option<&Vec<models::SandboxBrokeredEndpoint>>,
) -> anyhow::Result<Option<Vec<EndpointDeclaration>>> {
    let Some(endpoints) = endpoints else {
        return Ok(None);
    };
    endpoints
        .iter()
        .map(|endpoint| {
            let port = u16::try_from(endpoint.port).map_err(|_| {
                anyhow::anyhow!("endpoint port {} is not between 1 and 65535", endpoint.port)
            })?;
            let params = endpoint
                .params
                .as_ref()
                .map(|params| {
                    serde_json::Value::Object(
                        params
                            .iter()
                            .map(|(key, value)| (key.clone(), value.0.clone()))
                            .collect(),
                    )
                })
                .unwrap_or(serde_json::Value::Null);
            Ok(EndpointDeclaration {
                port,
                handler: endpoint.handler.clone(),
                params,
                intercept_port: endpoint.intercept_port.unwrap_or(false),
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()
        .map(Some)
}

pub(super) fn rules_from_model(
    rules: Option<&HashMap<String, Vec<models::SandboxNetworkRule>>>,
) -> Option<BTreeMap<String, Vec<DomainRule>>> {
    rules.map(|rules| {
        rules
            .iter()
            .map(|(domain, domain_rules)| {
                (
                    domain.clone(),
                    domain_rules
                        .iter()
                        .map(|rule| DomainRule {
                            transform: HeaderTransform {
                                headers: rule
                                    .transform
                                    .as_ref()
                                    .and_then(|t| t.headers.as_ref())
                                    .map(|h| {
                                        h.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
                                    })
                                    .unwrap_or_default(),
                            },
                        })
                        .collect(),
                )
            })
            .collect()
    })
}

pub(super) fn base_policy_from_allow_internet_access(
    value: Option<bool>,
) -> BaseSandboxNetworkPolicy {
    match value {
        Some(true) => BaseSandboxNetworkPolicy::Allow,
        Some(false) => BaseSandboxNetworkPolicy::Deny,
        None => BaseSandboxNetworkPolicy::Default,
    }
}

pub(in crate::api) fn allow_internet_access_from_base_policy(
    policy: BaseSandboxNetworkPolicy,
) -> Nullable<bool> {
    match policy {
        BaseSandboxNetworkPolicy::Default => Nullable::Null,
        BaseSandboxNetworkPolicy::Allow => Nullable::Present(true),
        BaseSandboxNetworkPolicy::Deny => Nullable::Present(false),
    }
}

impl From<SandboxMetadata> for models::SandboxDetail {
    fn from(m: SandboxMetadata) -> Self {
        let locked = m.traffic_access_token.is_some();
        let network = (m.network_policy.has_explicit_egress_rules() || locked)
            .then(|| network_config_model(&m.network_policy, locked));
        let allow_internet_access = Some(allow_internet_access_from_base_policy(
            m.network_policy.base_policy,
        ));

        Self {
            template_id: m.snapshot_id,
            alias: m.snapshot_alias,
            sandbox_id: m.id.into(),
            client_id: "".to_string(), // Deprecated field, only reserved for E2B Python SDK.
            started_at: started_at(m.created_at),
            end_at: end_at(m.expires_at),
            envd_version: m.runtime_versions.envd_version.clone(),
            envd_access_token: None,
            allow_internet_access,
            domain: None,
            cpu_count: m.resources.cpu_count,
            memory_mb: m.resources.memory_mib,
            disk_size_mb: m.resources.disk_size_mib,
            metadata: m.user_metadata,
            state: m.state.into(),
            network,
            lifecycle: Some(models::SandboxLifecycle {
                auto_resume: m.auto_resume,
                on_timeout: m.timeout_action.into(),
            }),
            execution_id: Some(m.execution_id.to_string()),
        }
    }
}

/// Convert a generated params model into the internal params map.
pub(super) fn params_model_to_map(
    model: &std::collections::HashMap<String, agentenv_http_server::types::Object>,
) -> serde_json::Map<String, serde_json::Value> {
    model
        .iter()
        .map(|(key, value)| (key.clone(), value.0.clone()))
        .collect()
}

/// Convert stored params into the generated response model. Absent params
/// yield an empty object (empty params).
pub(super) fn params_map_to_model(
    params: Option<&serde_json::Map<String, serde_json::Value>>,
) -> std::collections::HashMap<String, agentenv_http_server::types::Object> {
    match params {
        Some(map) => map
            .iter()
            .map(|(key, value)| {
                (
                    key.clone(),
                    agentenv_http_server::types::Object(value.clone()),
                )
            })
            .collect(),
        None => std::collections::HashMap::new(),
    }
}

#[cfg(test)]
mod detail_model_tests {
    use super::*;

    #[test]
    fn a_locked_sandbox_reports_the_lock_it_was_created_with() {
        let locked = SandboxMetadata {
            traffic_access_token: Some("the-token".to_string()),
            ..Default::default()
        };
        let open = SandboxMetadata::default();

        let locked: models::SandboxDetail = locked.into();
        let open: models::SandboxDetail = open.into();

        assert_eq!(
            locked
                .network
                .as_ref()
                .and_then(|network| network.allow_public_traffic),
            Some(false),
            "a locked sandbox that declared no rules still has a lock to report"
        );
        assert!(
            open.network.is_none(),
            "an open sandbox with no rules has nothing to say about its network"
        );
    }
}

#[cfg(test)]
mod execution_exposure_tests {
    use super::*;

    #[test]
    fn every_sandbox_response_names_its_execution() {
        let metadata = SandboxMetadata::default();
        let expected = metadata.execution_id.to_string();

        let listed = models::ListedSandbox::from(metadata.clone());
        let sandbox = models::Sandbox::from(metadata.clone());
        let detail = models::SandboxDetail::from(metadata);

        for (name, value) in [
            ("ListedSandbox", listed.execution_id),
            ("Sandbox", sandbox.execution_id),
            ("SandboxDetail", detail.execution_id),
        ] {
            assert_eq!(
                value.as_deref(),
                Some(expected.as_str()),
                "{name} must name the run it describes"
            );
        }
    }

    #[test]
    fn the_execution_is_never_an_input() {
        let spec = include_str!("../../openapi.yml");
        let mut schema = None;
        let mut offenders = Vec::new();

        for line in spec.lines() {
            let indent = line.len() - line.trim_start().len();
            let trimmed = line.trim_end();
            if indent == 4 && trimmed.ends_with(':') && !trimmed.trim_start().starts_with('-') {
                schema = Some(trimmed.trim().trim_end_matches(':').to_string());
            }
            if trimmed.trim_start().starts_with("executionID:") {
                if let Some(schema) = schema.as_deref() {
                    let is_request_shape = schema.starts_with("New")
                        || schema.starts_with("Resumed")
                        || schema.ends_with("Request");
                    if is_request_shape {
                        offenders.push(schema.to_string());
                    }
                }
            }
        }

        assert!(
            offenders.is_empty(),
            "the incarnation must never be accepted as input, but these request shapes take it: \
             {offenders:?}"
        );
    }
}
