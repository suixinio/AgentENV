use agentenv_http_server::apis::sandboxes::*;
use agentenv_http_server::models;

use crate::orchestrator::OrchestratorError;
use crate::sandbox::{SandboxNetworkEgressPolicy, SandboxNetworkPolicy};
use crate::types::SandboxId;

use super::conversions::{
    base_policy_from_allow_internet_access, endpoints_from_model, rules_from_model,
};
use super::{sandbox_not_found, ApiImpl};

fn network_policy_from_update(
    body: &models::SandboxNetworkUpdateConfig,
) -> anyhow::Result<SandboxNetworkPolicy> {
    let policy = SandboxNetworkEgressPolicy::with_rules_and_endpoints(
        body.allow_out.clone(),
        body.deny_out.clone(),
        rules_from_model(body.rules.as_ref()),
        endpoints_from_model(body.x_aenv_endpoints.as_ref())?,
    )?;
    if policy.has_domain_allow_rules() {
        anyhow::bail!(
            "domain entries in allowOut are not supported until TCP egress proxy is enabled"
        );
    }
    Ok(SandboxNetworkPolicy::new(
        base_policy_from_allow_internet_access(body.allow_internet_access),
        policy,
    ))
}

impl ApiImpl {
    pub(super) async fn network_put(
        &self,
        path_params: &models::SandboxesSandboxIdNetworkPutPathParams,
        body: &models::SandboxNetworkUpdateConfig,
    ) -> Result<SandboxesSandboxIdNetworkPutResponse, ()> {
        let path_id = &path_params.sandbox_id;
        let Ok(sandbox_id) = SandboxId::parse_str(path_id) else {
            return Ok(SandboxesSandboxIdNetworkPutResponse::Status404_NotFound(
                sandbox_not_found(path_id),
            ));
        };
        let network = match network_policy_from_update(body) {
            Ok(network) => network,
            Err(err) => {
                return Ok(SandboxesSandboxIdNetworkPutResponse::Status400_BadRequest(
                    Self::error(400, err.to_string()),
                ));
            }
        };
        if let Err(err) = self.check_rule_secrets(&network).await {
            return Ok(if err.code == 503 {
                SandboxesSandboxIdNetworkPutResponse::Status503_NoSecretsStoreIsConfigured(err)
            } else {
                SandboxesSandboxIdNetworkPutResponse::Status400_BadRequest(err)
            });
        }

        match self
            .orchestrator()
            .replace_sandbox_network_policy(sandbox_id, network)
            .await
        {
            Ok(()) => Ok(SandboxesSandboxIdNetworkPutResponse::Status204_SuccessfullyUpdatedTheSandboxNetworkConfiguration),
            Err(OrchestratorError::SandboxNotFound(id)) => Ok(
                SandboxesSandboxIdNetworkPutResponse::Status404_NotFound(sandbox_not_found(id)),
            ),
            Err(OrchestratorError::InvalidSandboxState { state, .. }) => {
                Ok(SandboxesSandboxIdNetworkPutResponse::Status409_Conflict(
                    Self::error(
                        409,
                        format!("sandbox network cannot be updated from {} state", state),
                    ),
                ))
            }
            Err(err @ OrchestratorError::SandboxOperationConflict { .. }) => {
                Ok(SandboxesSandboxIdNetworkPutResponse::Status409_Conflict(
                    Self::error(409, err.to_string()),
                ))
            }
            Err(err) => Ok(SandboxesSandboxIdNetworkPutResponse::Status500_ServerError(err.into())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::BaseSandboxNetworkPolicy;

    #[test]
    fn network_update_replaces_base_policy_and_egress() {
        let body = models::SandboxNetworkUpdateConfig {
            rules: None,
            x_aenv_endpoints: None,
            allow_out: Some(vec!["8.8.8.8".to_string()]),
            deny_out: Some(vec!["203.0.113.0/24".to_string()]),
            allow_internet_access: Some(false),
        };

        let policy = network_policy_from_update(&body).unwrap();

        assert_eq!(policy.base_policy, BaseSandboxNetworkPolicy::Deny);
        assert_eq!(policy.egress.allowed_cidrs, ["8.8.8.8/32"]);
        assert_eq!(policy.egress.denied_cidrs, ["203.0.113.0/24"]);
    }

    #[test]
    fn empty_network_update_clears_base_policy_and_egress() {
        let policy = network_policy_from_update(&models::SandboxNetworkUpdateConfig::new())
            .expect("empty update should be valid");

        assert_eq!(policy, SandboxNetworkPolicy::default());
    }
}
