use tracing::{info, warn};

use crate::cfg::ConfigManager;
use crate::node_client::placement::PlacementRefused;
use crate::observability::prometheus::SandboxStageTimer;
use crate::orchestrator::{
    CreateSandboxRequest, OrchestratorError, SandboxExpiry, SandboxLaunchSource,
    SandboxTimeoutAction,
};
use crate::sandbox::{CustomExtensionParams, SandboxNetworkEgressPolicy, SandboxNetworkPolicy};
use crate::types::SandboxResources;
use agentenv_http_server::apis::sandboxes::*;
use agentenv_http_server::models;

use super::super::attached_drives::unresolved_attached_drives;
use super::conversions::{
    base_policy_from_allow_internet_access, endpoints_from_model, params_model_to_map,
    rules_from_model,
};
use super::{duration_from_secs, ApiImpl, RoutingHeaders};

/// The token a create mints when its caller asked for a locked sandbox.
///
/// `secure` is required with it because the two lock different doors and one
/// without the other is a sandbox that reads as locked and is not: this token
/// bounds the ports the guest serves, and `secure` bounds envd itself.
fn traffic_access_token_for(
    secure: Option<bool>,
    network: Option<&models::SandboxNetworkConfig>,
) -> Result<Option<String>, String> {
    if network.and_then(|network| network.allow_public_traffic) != Some(false) {
        return Ok(None);
    }
    if secure != Some(true) {
        return Err(
            "allowPublicTraffic=false requires secure=true: without it envd stays reachable \
             without a credential and the sandbox is not locked"
                .to_string(),
        );
    }
    Ok(Some(uuid::Uuid::new_v4().to_string()))
}

/// Whether a create or update was refused because no node can broker it.
fn placement_refused(err: &OrchestratorError) -> bool {
    match err {
        OrchestratorError::SandboxOperationFailed { source, .. } => source
            .chain()
            .any(|cause| cause.downcast_ref::<PlacementRefused>().is_some()),
        _ => false,
    }
}

/// Maps an omitted user timeout to the configured default.
fn requested_expiry(timeout: Option<u32>) -> SandboxExpiry {
    match duration_from_secs(timeout) {
        Some(timeout) => SandboxExpiry::After(timeout),
        None => SandboxExpiry::AfterConfiguredDefault,
    }
}

fn cold_start_resources(body: &models::NewColdSandbox) -> Result<SandboxResources, models::Error> {
    let config = ConfigManager::global_config();
    let default_cpu = config.machine.vcpu_count;
    let default_mem = config.machine.mem_size_mib;
    let cpu_count = body.cpu_count.unwrap_or(default_cpu);
    if cpu_count == 0 {
        return Err(ApiImpl::error(
            400,
            "cpuCount must be greater than 0".to_string(),
        ));
    }
    let memory_mib = body.memory_mb.unwrap_or(default_mem);
    if memory_mib < 128 {
        return Err(ApiImpl::error(
            400,
            "memoryMB must be at least 128".to_string(),
        ));
    }
    let disk_size_mib = body.disk_size_mb.unwrap_or(0);
    if body.disk_size_mb.is_some() && (disk_size_mib < 1024 || !disk_size_mib.is_multiple_of(1024))
    {
        return Err(ApiImpl::error(
            400,
            "diskSizeMB must be at least 1024 and divisible by 1024".to_string(),
        ));
    }
    Ok(SandboxResources {
        cpu_count,
        memory_mib,
        // Zero means omitted; the orchestrator fills it from the resolved rootfs.
        disk_size_mib,
    })
}

/// Reject non-empty custom extension params when no custom extension is
/// configured. Empty params (absent or `{}`) are always allowed.
fn validate_custom_extension_params(params: Option<&CustomExtensionParams>) -> anyhow::Result<()> {
    if !crate::sandbox::custom_extension_params_is_empty(params)
        && crate::sandbox::CustomExtensionClient::global().is_none()
    {
        anyhow::bail!(
            "customExtensionParams requires the custom extension to be configured ([custom_extension].url)"
        );
    }
    Ok(())
}

fn network_policy_from_create(
    allow_internet_access: Option<bool>,
    network: Option<&models::SandboxNetworkConfig>,
) -> anyhow::Result<SandboxNetworkPolicy> {
    let base_policy = base_policy_from_allow_internet_access(allow_internet_access);
    let allow_out = network.and_then(|network| network.allow_out.clone());
    let deny_out = network.and_then(|network| network.deny_out.clone());
    let rules = rules_from_model(network.and_then(|network| network.rules.as_ref()));
    let endpoints =
        endpoints_from_model(network.and_then(|network| network.x_aenv_endpoints.as_ref()))?;
    let egress = SandboxNetworkEgressPolicy::with_rules_and_endpoints(
        allow_out, deny_out, rules, endpoints,
    )?;
    let policy = SandboxNetworkPolicy::new(base_policy, egress);
    if policy.has_domain_allow_rules() {
        anyhow::bail!(
            "domain entries in allowOut are not supported until TCP egress proxy is enabled"
        );
    }
    Ok(policy)
}

impl ApiImpl {
    pub(super) async fn cold_post(
        &self,
        body: &models::NewColdSandbox,
    ) -> Result<SandboxesColdPostResponse, ()> {
        let timer = SandboxStageTimer::new("create_cold");

        let resources = match cold_start_resources(body) {
            Ok(resources) => resources,
            Err(err) => return Ok(SandboxesColdPostResponse::Status400_BadRequest(err)),
        };

        let network_policy =
            match network_policy_from_create(body.allow_internet_access, body.network.as_ref()) {
                Ok(network) => network,
                Err(err) => {
                    return Ok(SandboxesColdPostResponse::Status400_BadRequest(
                        Self::error(400, err.to_string()),
                    ));
                }
            };
        if let Err(err) = self.check_rule_secrets(&network_policy).await {
            return Ok(if err.code == 503 {
                SandboxesColdPostResponse::Status503_NoNodeCanTakeASandboxWithTheseNetworkRulesRightNow(err)
            } else {
                SandboxesColdPostResponse::Status400_BadRequest(err)
            });
        }

        let custom_params = body
            .custom_extension_params
            .as_ref()
            .map(params_model_to_map);
        if let Err(err) = validate_custom_extension_params(custom_params.as_ref()) {
            return Ok(SandboxesColdPostResponse::Status400_BadRequest(
                Self::error(400, err.to_string()),
            ));
        }
        let traffic_access_token =
            match traffic_access_token_for(body.secure, body.network.as_ref()) {
                Ok(token) => token,
                Err(err) => {
                    return Ok(SandboxesColdPostResponse::Status400_BadRequest(
                        Self::error(400, err),
                    ));
                }
            };

        // Keep image references unresolved for the target node that owns registry access.
        let attached_drives =
            match unresolved_attached_drives(body.attached_drives.as_deref().unwrap_or_default()) {
                Ok(drives) => drives,
                Err(err) => {
                    warn!(error = %err.message, "failed to validate attached drives");
                    return Ok(Self::client_or_server_response(
                        err,
                        SandboxesColdPostResponse::Status400_BadRequest,
                        SandboxesColdPostResponse::Status500_ServerError,
                    ));
                }
            };
        info!(
            image = %body.image,
            "cold sandbox create: this half has no regctl of its own, dispatching the \
             unresolved image reference to a node to resolve"
        );
        let source = SandboxLaunchSource::UnresolvedImage {
            image_ref: body.image.clone(),
            resources,
            attached_drives,
            extra_boot_args: body.extra_boot_args.clone(),
        };

        let request = CreateSandboxRequest {
            source,
            expiry: requested_expiry(body.timeout),
            timeout_action: match body.auto_pause {
                Some(false) => SandboxTimeoutAction::Delete,
                _ => SandboxTimeoutAction::Pause,
            },
            auto_resume: body.auto_resume.as_ref().is_some_and(|cfg| cfg.enabled),
            user_metadata: body.metadata.clone(),
            env_vars: body
                .env_vars
                .clone()
                .filter(|env_vars| !env_vars.is_empty()),
            network_policy,
            secure: body.secure == Some(true),
            traffic_access_token,
            custom_extension_params: custom_params,
            // User REST creates are not control-plane-owned.
            control_plane_config: None,
            // User REST creates mint their incarnation in the orchestrator.
            execution_id: None,
            preferred_node_id: None,
        };

        match timer
            .time(
                "create_sandbox",
                self.orchestrator().create_sandbox(request),
            )
            .await
        {
            Ok(metadata) => {
                let routing = RoutingHeaders::of(&metadata);
                Ok(
                    SandboxesColdPostResponse::Status201_TheSandboxWasCreatedSuccessfully {
                        body: self.sandbox_model(metadata),
                        x_agentenv_sandbox_id: Some(routing.sandbox_id),
                        x_agentenv_execution_id: Some(routing.execution_id),
                        x_agentenv_projection_ttl_secs: Some(routing.projection_ttl_secs),
                    },
                )
            }
            Err(err) => {
                let invalid_request = match &err {
                    OrchestratorError::SandboxOperationFailed { source, .. } => {
                        source.chain().find_map(|cause| {
                            cause
                                .downcast_ref::<crate::sandbox::InvalidSandboxRequest>()
                                .map(ToString::to_string)
                        })
                    }
                    _ => None,
                };
                match invalid_request {
                    Some(message) => Ok(SandboxesColdPostResponse::Status400_BadRequest(
                        Self::error(400, message),
                    )),
                    None if placement_refused(&err) => Ok(
                        SandboxesColdPostResponse::Status503_NoNodeCanTakeASandboxWithTheseNetworkRulesRightNow(
                            Self::error(503, err.to_string()),
                        ),
                    ),
                    None => Ok(SandboxesColdPostResponse::Status500_ServerError(
                        Self::internal_error(&err),
                    )),
                }
            }
        }
    }

    pub(super) async fn create_post(
        &self,
        body: &models::NewSandbox,
    ) -> Result<SandboxesPostResponse, ()> {
        let timer = SandboxStageTimer::new("create_warm");
        // Fetch only the catalog row; the target node resolves runtime artifacts.
        let loaded: anyhow::Result<Option<SandboxLaunchSource>> = timer
            .time(
                "load_snapshot",
                self.snapshot_manager.get(&body.template_id),
            )
            .await
            .map(|found| found.map(|record| SandboxLaunchSource::SnapshotRecord(Box::new(record))));
        let source = match loaded {
            Ok(Some(source)) => source,
            Ok(None) => {
                return Ok(SandboxesPostResponse::Status400_BadRequest(Self::error(
                    400,
                    format!("template {} not found", body.template_id),
                )));
            }
            Err(err) => {
                warn!(error = ?err, template_id = %body.template_id, "failed to load runnable snapshot");
                return Ok(SandboxesPostResponse::Status500_ServerError(
                    Self::snapshot_manager_error(&err),
                ));
            }
        };

        let network_policy =
            match network_policy_from_create(body.allow_internet_access, body.network.as_ref()) {
                Ok(network) => network,
                Err(err) => {
                    return Ok(SandboxesPostResponse::Status400_BadRequest(Self::error(
                        400,
                        err.to_string(),
                    )));
                }
            };
        if let Err(err) = self.check_rule_secrets(&network_policy).await {
            return Ok(if err.code == 503 {
                SandboxesPostResponse::Status503_NoNodeCanTakeASandboxWithTheseNetworkRulesRightNow(
                    err,
                )
            } else {
                SandboxesPostResponse::Status400_BadRequest(err)
            });
        }

        let custom_params = body
            .custom_extension_params
            .as_ref()
            .map(params_model_to_map);
        if let Err(err) = validate_custom_extension_params(custom_params.as_ref()) {
            return Ok(SandboxesPostResponse::Status400_BadRequest(Self::error(
                400,
                err.to_string(),
            )));
        }
        let traffic_access_token =
            match traffic_access_token_for(body.secure, body.network.as_ref()) {
                Ok(token) => token,
                Err(err) => {
                    return Ok(SandboxesPostResponse::Status400_BadRequest(Self::error(
                        400, err,
                    )));
                }
            };

        let request = CreateSandboxRequest {
            source,
            expiry: requested_expiry(body.timeout),
            timeout_action: match body.auto_pause {
                Some(false) => SandboxTimeoutAction::Delete,
                _ => SandboxTimeoutAction::Pause,
            },
            auto_resume: body.auto_resume.as_ref().is_some_and(|cfg| cfg.enabled),
            user_metadata: body.metadata.clone(),
            env_vars: body
                .env_vars
                .clone()
                .filter(|env_vars| !env_vars.is_empty()),
            network_policy,
            secure: body.secure == Some(true),
            traffic_access_token,
            custom_extension_params: custom_params,
            // User REST creates are not control-plane-owned.
            control_plane_config: None,
            // User REST creates mint their incarnation in the orchestrator.
            execution_id: None,
            preferred_node_id: None,
        };

        match timer
            .time(
                "create_sandbox",
                self.orchestrator().create_sandbox(request),
            )
            .await
        {
            Ok(metadata) => {
                let routing = RoutingHeaders::of(&metadata);
                Ok(
                    SandboxesPostResponse::Status201_TheSandboxWasCreatedSuccessfully {
                        body: self.sandbox_model(metadata),
                        x_agentenv_sandbox_id: Some(routing.sandbox_id),
                        x_agentenv_execution_id: Some(routing.execution_id),
                        x_agentenv_projection_ttl_secs: Some(routing.projection_ttl_secs),
                    },
                )
            }
            Err(err) if placement_refused(&err) => Ok(
                SandboxesPostResponse::Status503_NoNodeCanTakeASandboxWithTheseNetworkRulesRightNow(
                    Self::error(503, err.to_string()),
                ),
            ),
            Err(err) => Ok(SandboxesPostResponse::Status500_ServerError(
                Self::internal_error(&err),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn a_user_that_named_no_timeout_gets_the_configured_default_and_not_none() {
        assert_eq!(
            requested_expiry(None),
            SandboxExpiry::AfterConfiguredDefault
        );
        assert_ne!(
            requested_expiry(None),
            SandboxExpiry::NotKeptHere,
            "a user is a client, not a second orchestrator keeping its own deadline"
        );
        assert_eq!(
            requested_expiry(Some(600)),
            SandboxExpiry::After(Duration::from_secs(600))
        );
    }

    #[test]
    fn cold_start_resources_maps_and_validates_optional_disk_mb() {
        let mut body = models::NewColdSandbox::new("ubuntu:24.04".to_string());
        body.cpu_count = Some(2);
        body.memory_mb = Some(512);

        let resources = cold_start_resources(&body).unwrap();
        assert_eq!(resources.cpu_count, 2);
        assert_eq!(resources.memory_mib, 512);
        assert_eq!(resources.disk_size_mib, 0);

        body.disk_size_mb = Some(8192);
        assert_eq!(cold_start_resources(&body).unwrap().disk_size_mib, 8192);

        body.disk_size_mb = Some(0);
        assert!(cold_start_resources(&body).is_err());

        body.disk_size_mb = Some(512);
        assert!(cold_start_resources(&body).is_err());

        body.disk_size_mb = Some(1536);
        assert!(cold_start_resources(&body).is_err());
    }
}

#[cfg(test)]
mod create_model_tests {
    use agentenv_http_server::models;

    #[test]
    fn locking_public_traffic_mints_a_token_and_needs_a_secure_sandbox() {
        let mut network = models::SandboxNetworkConfig::new();
        network.allow_public_traffic = Some(false);

        let err = super::traffic_access_token_for(None, Some(&network))
            .expect_err("a locked sandbox with a wide-open envd is not locked");
        assert!(err.contains("secure=true"), "{err}");
        assert!(
            super::traffic_access_token_for(Some(false), Some(&network)).is_err(),
            "an explicit secure=false is not secure=true"
        );

        let token = super::traffic_access_token_for(Some(true), Some(&network))
            .expect("secure=true mints one")
            .expect("a locked sandbox has a token");
        assert!(
            uuid::Uuid::parse_str(&token).is_ok(),
            "{token} is not a UUID"
        );
        assert_ne!(
            token,
            super::traffic_access_token_for(Some(true), Some(&network))
                .unwrap()
                .unwrap(),
            "two sandboxes must not share a token"
        );
    }

    #[test]
    fn an_open_sandbox_mints_no_token_at_all() {
        let mut network = models::SandboxNetworkConfig::new();

        assert_eq!(
            super::traffic_access_token_for(Some(true), None).unwrap(),
            None
        );
        assert_eq!(
            super::traffic_access_token_for(Some(true), Some(&network)).unwrap(),
            None
        );
        network.allow_public_traffic = Some(true);
        assert_eq!(
            super::traffic_access_token_for(Some(true), Some(&network)).unwrap(),
            None
        );
    }

    #[test]
    fn an_ipv6_egress_entry_is_refused_by_create() {
        let mut network = models::SandboxNetworkConfig::new();
        network.allow_out = Some(vec!["2001:db8::/32".to_string()]);

        let err =
            super::network_policy_from_create(None, Some(&network)).expect_err("IPv6 is refused");

        assert!(format!("{err:#}").contains("IPv6"), "{err:#}");
    }
}

#[cfg(test)]
mod warm_start_source_tests {
    use std::sync::Arc;

    use axum_extra::extract::CookieJar;
    use headers::Host;
    use http::Method;

    use agentenv_http_server::apis::sandboxes::*;
    use agentenv_http_server::models;

    use super::ApiImpl;
    use crate::orchestrator::Orchestrator;
    use crate::sandbox::mock::MockBackendFactory;
    use crate::snapshot::mock::unresolvable_snapshot_manager;
    use crate::snapshot::{CommittedSnapshot, SnapshotRecord};

    async fn surface(row: SnapshotRecord) -> Arc<ApiImpl> {
        let orchestrator = Orchestrator::with_in_memory_store(MockBackendFactory::new()).await;
        Arc::new(ApiImpl::new(
            orchestrator,
            Arc::new(unresolvable_snapshot_manager(row)),
            None,
            Vec::new(),
            crate::api::ResumeWiring::api_half_for_test(),
        ))
    }

    fn ready_row() -> SnapshotRecord {
        SnapshotRecord::mock_ready(CommittedSnapshot::mock())
    }

    async fn create_from_template(api: &ApiImpl) -> SandboxesPostResponse {
        api.sandboxes_post(
            &Method::POST,
            &Host::from(http::uri::Authority::from_static("localhost")),
            &CookieJar::new(),
            &crate::api::impls::Claims,
            &models::NewSandbox::new("tpl-warm-start".to_string()),
        )
        .await
        .expect("the handler answers")
    }

    #[tokio::test]
    async fn a_warm_start_ships_the_catalog_row_without_resolving_it() {
        let api = surface(ready_row()).await;
        let response = create_from_template(&api).await;
        assert!(
            matches!(
                response,
                SandboxesPostResponse::Status201_TheSandboxWasCreatedSuccessfully { .. }
            ),
            "🔴 the assertion. This manager's runtime resolver fails every call, so a create \
             that touched it could not have got here — answering 201 over it is the proof that \
             this route never turned the catalog row into local bytes, got {response:?}"
        );
    }
}

#[cfg(test)]
mod rule_secret_check_tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use axum_extra::extract::CookieJar;
    use headers::Host;
    use http::Method;

    use agentenv_http_server::apis::sandboxes::*;
    use agentenv_http_server::models;

    use super::ApiImpl;
    use crate::orchestrator::Orchestrator;
    use crate::sandbox::mock::MockBackendFactory;
    use crate::secrets::memory::{InMemorySecretRefStore, InMemorySecretsBackend};
    use crate::secrets::{SecretMetadata, SecretString, SecretValue, SecretsService};
    use crate::snapshot::mock::unresolvable_snapshot_manager;
    use crate::snapshot::{CommittedSnapshot, SnapshotRecord};

    async fn surface(secrets: Option<Arc<SecretsService>>) -> Arc<ApiImpl> {
        let orchestrator = Orchestrator::with_in_memory_store(MockBackendFactory::new()).await;
        let row = SnapshotRecord::mock_ready(CommittedSnapshot::mock());
        let api = ApiImpl::new(
            orchestrator,
            Arc::new(unresolvable_snapshot_manager(row)),
            None,
            Vec::new(),
            crate::api::ResumeWiring::api_half_for_test(),
        );
        Arc::new(match secrets {
            Some(secrets) => api.with_secrets(secrets),
            None => api,
        })
    }

    async fn store_holding(names: &[&str]) -> Arc<SecretsService> {
        let service = SecretsService::new(
            Arc::new(InMemorySecretRefStore::default()),
            Arc::new(InMemorySecretsBackend::default()),
        );
        for name in names {
            service
                .create(
                    name,
                    SecretValue::Opaque(SecretString::new("v".to_string())),
                    SecretMetadata::new(),
                    Vec::new(),
                )
                .await
                .expect("seeding a secret");
        }
        Arc::new(service)
    }

    fn network_naming(secret: &str) -> models::SandboxNetworkConfig {
        let mut transform = models::SandboxNetworkTransform::new();
        transform.headers = Some(HashMap::from([(
            "authorization".to_string(),
            format!("Bearer ${{aenv.secrets.{secret}}}"),
        )]));
        let mut rule = models::SandboxNetworkRule::new();
        rule.transform = Some(transform);
        let mut network = models::SandboxNetworkConfig::new();
        network.rules = Some(HashMap::from([("api.example.com".to_string(), vec![rule])]));
        network
    }

    fn host() -> Host {
        Host::from(http::uri::Authority::from_static("localhost"))
    }

    async fn warm_create(api: &ApiImpl, secret: &str) -> SandboxesPostResponse {
        let mut body = models::NewSandbox::new("tpl-rules".to_string());
        body.network = Some(network_naming(secret));
        api.sandboxes_post(
            &Method::POST,
            &host(),
            &CookieJar::new(),
            &crate::api::impls::Claims,
            &body,
        )
        .await
        .expect("the handler answers")
    }

    async fn cold_create(api: &ApiImpl, secret: &str) -> SandboxesColdPostResponse {
        let mut body = models::NewColdSandbox::new("debian:bookworm".to_string());
        body.network = Some(network_naming(secret));
        api.sandboxes_cold_post(
            &Method::POST,
            &host(),
            &CookieJar::new(),
            &crate::api::impls::Claims,
            &body,
        )
        .await
        .expect("the handler answers")
    }

    #[tokio::test]
    async fn a_create_with_no_secrets_store_answers_503_not_a_400_carrying_503() {
        let api = surface(None).await;

        let warm = warm_create(&api, "openai").await;
        let SandboxesPostResponse::Status503_NoNodeCanTakeASandboxWithTheseNetworkRulesRightNow(
            error,
        ) = warm
        else {
            panic!("a 503-coded body must travel in the 503 variant, got {warm:?}");
        };
        assert_eq!(error.code, 503);

        let cold = cold_create(&api, "openai").await;
        let SandboxesColdPostResponse::Status503_NoNodeCanTakeASandboxWithTheseNetworkRulesRightNow(
            error,
        ) = cold
        else {
            panic!("a 503-coded body must travel in the 503 variant, got {cold:?}");
        };
        assert_eq!(error.code, 503);
    }

    #[tokio::test]
    async fn a_create_naming_an_unknown_secret_still_answers_400() {
        let api = surface(Some(store_holding(&["other"]).await)).await;

        let warm = warm_create(&api, "openai").await;
        let SandboxesPostResponse::Status400_BadRequest(error) = warm else {
            panic!("an unknown name is the caller's mistake, got {warm:?}");
        };
        assert_eq!(error.code, 400);
        assert!(error.message.contains("openai"), "{}", error.message);

        let cold = cold_create(&api, "openai").await;
        let SandboxesColdPostResponse::Status400_BadRequest(error) = cold else {
            panic!("an unknown name is the caller's mistake, got {cold:?}");
        };
        assert_eq!(error.code, 400);
    }

    #[tokio::test]
    async fn a_create_reading_a_fields_credential_as_a_header_value_answers_400() {
        let service = SecretsService::new(
            Arc::new(InMemorySecretRefStore::default()),
            Arc::new(InMemorySecretsBackend::default()),
        );
        service
            .create(
                "tenant_db",
                SecretValue::Fields(
                    [("host".to_string(), SecretString::new("pg.internal".into()))]
                        .into_iter()
                        .collect(),
                ),
                SecretMetadata::new(),
                Vec::new(),
            )
            .await
            .expect("seeding a fields credential");
        let api = surface(Some(Arc::new(service))).await;

        let warm = warm_create(&api, "tenant_db").await;
        let SandboxesPostResponse::Status400_BadRequest(error) = warm else {
            panic!("a shape mismatch is the caller's mistake, got {warm:?}");
        };
        assert_eq!(error.code, 400);
        assert!(
            error.message.contains("tenant_db") && error.message.contains("shape"),
            "{}",
            error.message
        );
    }
}
