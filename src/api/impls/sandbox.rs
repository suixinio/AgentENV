use std::collections::{BTreeMap, HashMap};
use std::sync::OnceLock;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use axum_extra::extract::CookieJar;
use headers::Host;
use http::Method;

use tracing::{info, warn};

use crate::cfg::ConfigManager;
use crate::node_client::placement::PlacementRefused;
use crate::observability::prometheus::SandboxStageTimer;
use crate::orchestrator::{
    CreateSandboxRequest, ForkChildren, NewTimeout, OrchestratorError, SandboxExpiry,
    SandboxLaunchSource, SandboxListFilter, SandboxMetadata, SandboxState, SandboxTimeoutAction,
    StoreError,
};
use crate::sandbox::network::policy::{DomainRule, EndpointDeclaration, HeaderTransform};
use crate::sandbox::CustomExtensionParams;
use crate::sandbox::{BaseSandboxNetworkPolicy, SandboxNetworkEgressPolicy, SandboxNetworkPolicy};
use crate::secrets::UnusableSecrets;
use crate::snapshot::SnapshotAlias;
use crate::types::{SandboxId, SandboxResources};
use agentenv_http_server::apis::sandboxes::*;
use agentenv_http_server::models;
use agentenv_http_server::types::Nullable;

use super::attached_drives::unresolved_attached_drives;
use super::pagination::PaginationCursor;
use super::ApiImpl;

fn sandbox_not_found(id: impl Into<String>) -> models::Error {
    ApiImpl::error(404, format!("sandbox {} not found", id.into()))
}

/// Routing values returned to the gateway for projection updates.
struct RoutingHeaders {
    sandbox_id: String,
    execution_id: String,
    /// `0` asks the receiver to use its default TTL.
    projection_ttl_secs: i64,
}

impl RoutingHeaders {
    fn of(metadata: &SandboxMetadata) -> Self {
        Self {
            sandbox_id: metadata.id.to_string(),
            execution_id: metadata.execution_id.to_string(),
            projection_ttl_secs: i64::from(metadata.projection_ttl_secs(SystemTime::now())),
        }
    }
}

fn default_sandbox_timeout() -> Duration {
    static DEFAULT_SANDBOX_TIMEOUT: OnceLock<Duration> = OnceLock::new();

    *DEFAULT_SANDBOX_TIMEOUT.get_or_init(|| {
        Duration::from_secs(
            ConfigManager::global_config()
                .orchestrator
                .default_sandbox_timeout_secs,
        )
    })
}

impl From<OrchestratorError> for models::Error {
    fn from(err: OrchestratorError) -> Self {
        match err {
            OrchestratorError::ShuttingDown => {
                Self::new(503, "orchestrator is shutting down".to_string())
            }
            OrchestratorError::NotAcceptingNewWork => Self::new(
                503,
                "node is isolated and is not taking new sandboxes".to_string(),
            ),
            OrchestratorError::SandboxNotFound(id) => sandbox_not_found(id),
            OrchestratorError::InvalidSandboxState { .. } => Self::new(400, err.to_string()),
            OrchestratorError::SandboxLifetimeExceeded { .. } => Self::new(400, err.to_string()),
            OrchestratorError::SandboxOperationFailed {
                sandbox_id,
                operation,
                source,
            } => Self::new(
                500,
                format!(
                    "sandbox {} operation {:?} failed: {}",
                    sandbox_id,
                    operation,
                    ApiImpl::internal_error(source.as_ref()).message
                ),
            ),
            OrchestratorError::SandboxOperationConflict { .. } => Self::new(409, err.to_string()),
            OrchestratorError::InvalidRequest(_) => Self::new(400, err.to_string()),
            other => ApiImpl::internal_error(&other),
        }
    }
}

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
            traffic_access_token: None,
            domain: None,
            execution_id: Some(m.execution_id.to_string()),
        }
    }
}

impl From<&SandboxNetworkPolicy> for models::SandboxNetworkConfig {
    fn from(policy: &SandboxNetworkPolicy) -> Self {
        let egress = &policy.egress;
        Self {
            allow_public_traffic: Some(true),
            allow_out: (!egress.allowed_cidrs.is_empty() || !egress.allowed_domains.is_empty())
                .then(|| {
                    egress
                        .allowed_cidrs
                        .iter()
                        .chain(egress.allowed_domains.iter())
                        .cloned()
                        .collect()
                }),
            deny_out: (!egress.denied_cidrs.is_empty()).then(|| egress.denied_cidrs.clone()),
            rules: (!egress.rules.is_empty()).then(|| rules_model(&egress.rules)),
            x_aenv_endpoints: (!egress.endpoints.is_empty())
                .then(|| endpoints_model(&egress.endpoints)),
            mask_request_host: None,
        }
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

fn endpoints_from_model(
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

fn rules_from_model(
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

/// Whether a create or update was refused because no node can broker it.
fn placement_refused(err: &OrchestratorError) -> bool {
    match err {
        OrchestratorError::SandboxOperationFailed { source, .. } => source
            .chain()
            .any(|cause| cause.downcast_ref::<PlacementRefused>().is_some()),
        _ => false,
    }
}

fn base_policy_from_allow_internet_access(value: Option<bool>) -> BaseSandboxNetworkPolicy {
    match value {
        Some(true) => BaseSandboxNetworkPolicy::Allow,
        Some(false) => BaseSandboxNetworkPolicy::Deny,
        None => BaseSandboxNetworkPolicy::Default,
    }
}

pub(super) fn allow_internet_access_from_base_policy(
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
        let network = m
            .network_policy
            .has_explicit_egress_rules()
            .then(|| models::SandboxNetworkConfig::from(&m.network_policy));
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

impl ApiImpl {
    /// The sandbox's record once any pause in flight on it has settled.
    ///
    /// `None` means no record: the sandbox's newest catalog row speaks for
    /// it, or it never existed. A pause that outlasts the orchestrator's wait
    /// budget is `InvalidSandboxState { state: Pausing }`.
    async fn record_after_any_pause(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<SandboxMetadata>, OrchestratorError> {
        match self.orchestrator.get_sandbox(&sandbox_id).await? {
            Some(metadata) if metadata.state == SandboxState::Pausing => {
                self.orchestrator.wait_for_pause_to_settle(sandbox_id).await
            }
            recorded => Ok(recorded),
        }
    }

    fn sandbox_model(&self, metadata: SandboxMetadata) -> models::Sandbox {
        let envd_access_token = self
            .orchestrator
            .get_envd_access_token(&metadata)
            .map(|token| token.expose().to_owned());
        let mut sandbox = models::Sandbox::from(metadata);
        sandbox.envd_access_token = envd_access_token;
        sandbox.domain = self
            .sandbox_proxy_domains()
            .first()
            .map(|domain| Nullable::Present(domain.clone()));
        sandbox
    }

    fn sandbox_detail_model(&self, metadata: SandboxMetadata) -> models::SandboxDetail {
        let envd_access_token = self
            .orchestrator
            .get_envd_access_token(&metadata)
            .map(|token| token.expose().to_owned());
        let mut sandbox = models::SandboxDetail::from(metadata);
        sandbox.envd_access_token = envd_access_token;
        sandbox.domain = self
            .sandbox_proxy_domains()
            .first()
            .map(|domain| Nullable::Present(domain.clone()));
        sandbox
    }

    fn resumed_response(&self, metadata: SandboxMetadata) -> SandboxesSandboxIdResumePostResponse {
        let routing = RoutingHeaders::of(&metadata);
        SandboxesSandboxIdResumePostResponse::Status201_TheSandboxWasResumedSuccessfully {
            body: self.sandbox_model(metadata),
            x_agentenv_sandbox_id: Some(routing.sandbox_id),
            x_agentenv_execution_id: Some(routing.execution_id),
            x_agentenv_projection_ttl_secs: Some(routing.projection_ttl_secs),
        }
    }
}

fn parse_metadata_filter(raw: &Option<String>) -> Option<HashMap<String, String>> {
    let raw = raw.as_ref()?;
    let map: HashMap<String, String> = url::form_urlencoded::parse(raw.as_bytes())
        .filter(|(key, value)| !key.is_empty() && !value.is_empty())
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect();
    if map.is_empty() {
        None
    } else {
        Some(map)
    }
}

fn duration_from_secs(secs: Option<u32>) -> Option<Duration> {
    secs.map(|s| Duration::from_secs(s as u64))
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

/// Convert a generated params model into the internal params map.
fn params_model_to_map(
    model: &std::collections::HashMap<String, agentenv_http_server::types::Object>,
) -> serde_json::Map<String, serde_json::Value> {
    model
        .iter()
        .map(|(key, value)| (key.clone(), value.0.clone()))
        .collect()
}

/// Convert stored params into the generated response model. Absent params
/// yield an empty object (empty params).
fn params_map_to_model(
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

impl ApiImpl {
    /// Every secret a policy names must exist, in the shape the policy reads
    /// it in, before the sandbox is placed; a mismatch found here is a 400
    /// rather than a 502 on the first brokered connection. Without a store,
    /// rules that name secrets cannot be honoured.
    async fn check_rule_secrets(&self, policy: &SandboxNetworkPolicy) -> Result<(), models::Error> {
        let wanted = policy.egress.referenced_secrets();
        if wanted.is_empty() {
            return Ok(());
        }
        let Some(secrets) = self.secrets() else {
            return Err(Self::error(
                503,
                "network rules reference secrets but no secrets store is configured",
            ));
        };
        match secrets.ensure_usable(&wanted).await {
            Ok(()) => Ok(()),
            Err(UnusableSecrets::Missing(missing)) => Err(Self::error(
                400,
                format!(
                    "network rules reference unknown secret(s): {}",
                    missing.join(", ")
                ),
            )),
            Err(UnusableSecrets::WrongShape(mismatched)) => Err(Self::error(
                400,
                format!(
                    "network rules use secret(s) in another shape than they are stored in: {}",
                    mismatched
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join("; ")
                ),
            )),
            Err(UnusableSecrets::Store(err)) => {
                warn!(error = %format_args!("{err:#}"), "could not check secret names");
                Err(Self::error(503, "the secrets store could not be reached"))
            }
        }
    }
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

#[async_trait]
impl Sandboxes<()> for ApiImpl {
    type Claims = super::Claims;

    async fn sandboxes_cold_post(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
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

    async fn sandboxes_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        query_params: &models::SandboxesGetQueryParams,
    ) -> Result<SandboxesGetResponse, ()> {
        let filter = SandboxListFilter {
            states: Some(vec![SandboxState::Running]),
            excluded_states: None,
            user_metadata: parse_metadata_filter(&query_params.metadata),
        };

        let list = match self.orchestrator.list_sandboxes_filtered(filter).await {
            Ok(list) => list,
            Err(err) => {
                return Ok(SandboxesGetResponse::Status500_ServerError(err.into()));
            }
        };

        let out = list.into_iter().map(models::ListedSandbox::from).collect();

        Ok(SandboxesGetResponse::Status200_SuccessfullyReturnedAllRunningSandboxes(out))
    }

    async fn sandboxes_post(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
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

    async fn sandboxes_sandbox_id_connect_post(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::SandboxesSandboxIdConnectPostPathParams,
        body: &models::ConnectSandbox,
    ) -> Result<SandboxesSandboxIdConnectPostResponse, ()> {
        let path_id = &path_params.sandbox_id;
        let Ok(sandbox_id) = SandboxId::parse_str(path_id) else {
            return Ok(SandboxesSandboxIdConnectPostResponse::Status404_NotFound(
                sandbox_not_found(path_id),
            ));
        };
        let timeout = Duration::from_secs(body.timeout as u64);
        match self.record_after_any_pause(sandbox_id).await {
            Ok(Some(metadata)) => match metadata.state {
                SandboxState::Creating
                | SandboxState::Running
                | SandboxState::Snapshotting
                | SandboxState::Forking => {
                    match self
                        .orchestrator
                        .keep_alive_for(sandbox_id, Some(timeout), false)
                        .await
                    {
                        Ok(_) => {}
                        Err(OrchestratorError::SandboxNotFound(id)) => {
                            return Ok(SandboxesSandboxIdConnectPostResponse::Status404_NotFound(
                                sandbox_not_found(id),
                            ));
                        }
                        Err(OrchestratorError::InvalidTimeout { timeout, .. }) => {
                            return Ok(
                                SandboxesSandboxIdConnectPostResponse::Status400_BadRequest(
                                    Self::error(400, format!("invalid timeout: {timeout:?}")),
                                ),
                            );
                        }
                        Err(err) => {
                            return Ok(
                                SandboxesSandboxIdConnectPostResponse::Status500_ServerError(
                                    err.into(),
                                ),
                            );
                        }
                    }
                    // Include the incarnation required for the gateway's projection write.
                    let routing = RoutingHeaders::of(&metadata);

                    return Ok(
                        SandboxesSandboxIdConnectPostResponse::Status200_TheSandboxWasAlreadyRunning {
                            body: self.sandbox_model(metadata),
                            x_agentenv_sandbox_id: Some(routing.sandbox_id),
                            x_agentenv_execution_id: Some(routing.execution_id),
                            x_agentenv_projection_ttl_secs: Some(routing.projection_ttl_secs),
                        },
                    );
                }
                SandboxState::Killing => {
                    return Ok(SandboxesSandboxIdConnectPostResponse::Status404_NotFound(
                        sandbox_not_found(sandbox_id),
                    ));
                }
                SandboxState::Pausing => {
                    return Ok(SandboxesSandboxIdConnectPostResponse::Status400_BadRequest(
                        Self::error(400, "sandbox is pausing; connect again once it has paused"),
                    ));
                }
            },
            Ok(None) => {}
            Err(OrchestratorError::InvalidSandboxState { state, .. }) => {
                return Ok(SandboxesSandboxIdConnectPostResponse::Status400_BadRequest(
                    Self::error(
                        400,
                        format!("sandbox is still {state} after waiting; connect again shortly"),
                    ),
                ));
            }
            Err(err) => {
                return Ok(
                    SandboxesSandboxIdConnectPostResponse::Status500_ServerError(err.into()),
                );
            }
        }
        if !self.owns_sandboxes() {
            return Ok(SandboxesSandboxIdConnectPostResponse::Status404_NotFound(
                sandbox_not_found(sandbox_id),
            ));
        }

        // No record: resume from the sandbox's newest pause, if it has one.
        let record = match self.latest_paused_snapshot(sandbox_id).await {
            Ok(Some(record)) => record,
            Ok(None) => {
                return Ok(SandboxesSandboxIdConnectPostResponse::Status404_NotFound(
                    sandbox_not_found(sandbox_id),
                ));
            }
            Err(err) => {
                return Ok(
                    SandboxesSandboxIdConnectPostResponse::Status500_ServerError(
                        Self::snapshot_manager_error(&err),
                    ),
                );
            }
        };
        let request = match Self::restore_request(record.clone(), NewTimeout::Set(timeout)) {
            Ok(request) => request,
            Err(err) => {
                return Ok(
                    SandboxesSandboxIdConnectPostResponse::Status500_ServerError(Self::error(
                        500,
                        err.to_string(),
                    )),
                );
            }
        };
        match self
            .orchestrator()
            .restore_sandbox(sandbox_id, request)
            .await
        {
            Ok(resumed_metadata) => {
                self.note_resume_landing(&record, &resumed_metadata).await;
                let routing = RoutingHeaders::of(&resumed_metadata);

                Ok(
                    SandboxesSandboxIdConnectPostResponse::Status201_TheSandboxWasResumedSuccessfully {
                        body: self.sandbox_model(resumed_metadata),
                        x_agentenv_sandbox_id: Some(routing.sandbox_id),
                        x_agentenv_execution_id: Some(routing.execution_id),
                        x_agentenv_projection_ttl_secs: Some(routing.projection_ttl_secs),
                    },
                )
            }
            Err(OrchestratorError::StoreOperationFailed(StoreError::SandboxAlreadyExists {
                ..
            })) => Ok(SandboxesSandboxIdConnectPostResponse::Status400_BadRequest(
                Self::error(400, "sandbox is already being resumed"),
            )),
            Err(OrchestratorError::InvalidSandboxState { state, .. }) => Ok(
                SandboxesSandboxIdConnectPostResponse::Status400_BadRequest(Self::error(
                    400,
                    format!("sandbox cannot be resumed from {} state", state),
                )),
            ),
            Err(err) => {
                Ok(SandboxesSandboxIdConnectPostResponse::Status500_ServerError(err.into()))
            }
        }
    }

    async fn sandboxes_sandbox_id_delete(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::SandboxesSandboxIdDeletePathParams,
    ) -> Result<SandboxesSandboxIdDeleteResponse, ()> {
        let path_id = &path_params.sandbox_id;
        let Ok(sandbox_id) = SandboxId::parse_str(path_id) else {
            return Ok(SandboxesSandboxIdDeleteResponse::Status404_NotFound(
                sandbox_not_found(path_id),
            ));
        };
        // Kill the running sandbox if there is one, then forget every pause of
        // it; only a sandbox that is neither running nor paused is not found.
        let killed = match self.orchestrator().delete_sandbox(sandbox_id).await {
            Ok(_) => true,
            Err(OrchestratorError::SandboxNotFound(_)) => false,
            Err(err) => {
                return Ok(SandboxesSandboxIdDeleteResponse::Status500_ServerError(
                    err.into(),
                ))
            }
        };
        let forgotten = if self.owns_sandboxes() {
            match self.forget_paused_snapshots(sandbox_id).await {
                Ok(deleted) => deleted > 0,
                Err(err) => {
                    return Ok(SandboxesSandboxIdDeleteResponse::Status500_ServerError(
                        Self::snapshot_manager_error(&err),
                    ))
                }
            }
        } else {
            false
        };
        if killed || forgotten {
            Ok(SandboxesSandboxIdDeleteResponse::Status204_TheSandboxWasKilledSuccessfully)
        } else {
            Ok(SandboxesSandboxIdDeleteResponse::Status404_NotFound(
                sandbox_not_found(sandbox_id),
            ))
        }
    }

    async fn sandboxes_sandbox_id_fork_post(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::SandboxesSandboxIdForkPostPathParams,
        body: &Option<models::SandboxForkRequest>,
    ) -> Result<SandboxesSandboxIdForkPostResponse, ()> {
        let path_id = &path_params.sandbox_id;
        let Ok(sandbox_id) = SandboxId::parse_str(path_id) else {
            return Ok(SandboxesSandboxIdForkPostResponse::Status404_NotFound(
                sandbox_not_found(path_id),
            ));
        };

        let count = body.as_ref().and_then(|b| b.count).unwrap_or(1);
        let new_timeout = body
            .as_ref()
            .and_then(|body| body.timeout)
            .map_or(NewTimeout::UseExisting, |timeout| {
                NewTimeout::Set(Duration::from_secs(timeout as u64))
            });
        let timer = SandboxStageTimer::new("fork");
        match timer
            .time(
                "fork",
                self.orchestrator()
                    // User-created fork children are not control-plane-owned.
                    .fork_sandbox(sandbox_id, ForkChildren::Fresh(count), new_timeout),
            )
            .await
        {
            Ok(outcomes) => {
                let results = outcomes
                    .into_iter()
                    .map(|outcome| match outcome {
                        Ok(metadata) => {
                            // Per-child routing data belongs in each response body entry.
                            let projection_ttl_secs =
                                i64::from(metadata.projection_ttl_secs(SystemTime::now()));

                            models::SandboxForkResult {
                                sandbox: Some(self.sandbox_model(metadata)),
                                error: None,
                                projection_ttl_secs: Some(projection_ttl_secs),
                            }
                        }
                        Err(err) => models::SandboxForkResult {
                            sandbox: None,
                            error: Some(models::Error::from(err)),
                            projection_ttl_secs: None,
                        },
                    })
                    .collect();
                Ok(
                    SandboxesSandboxIdForkPostResponse::Status201_TheSandboxWasSnapshottedAndTheForksWereAttempted(results),
                )
            }
            Err(OrchestratorError::SandboxNotFound(id)) => Ok(
                SandboxesSandboxIdForkPostResponse::Status404_NotFound(sandbox_not_found(id)),
            ),
            Err(OrchestratorError::InvalidSandboxState { state, .. }) => Ok(
                SandboxesSandboxIdForkPostResponse::Status409_Conflict(Self::error(
                    409,
                    format!("sandbox cannot be forked from {} state", state),
                )),
            ),
            Err(err) => Ok(SandboxesSandboxIdForkPostResponse::Status500_ServerError(
                err.into(),
            )),
        }
    }

    async fn sandboxes_sandbox_id_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::SandboxesSandboxIdGetPathParams,
    ) -> Result<SandboxesSandboxIdGetResponse, ()> {
        let path_id = &path_params.sandbox_id;
        let Ok(sandbox_id) = SandboxId::parse_str(path_id) else {
            return Ok(SandboxesSandboxIdGetResponse::Status404_NotFound(
                sandbox_not_found(path_id),
            ));
        };
        let metadata = match self.orchestrator.get_sandbox(&sandbox_id).await {
            Ok(Some(metadata)) => metadata,
            Ok(None) => {
                // No record: the sandbox is paused, or it does not exist.
                if !self.owns_sandboxes() {
                    return Ok(SandboxesSandboxIdGetResponse::Status404_NotFound(
                        sandbox_not_found(sandbox_id),
                    ));
                }
                return Ok(match self.latest_paused_snapshot(sandbox_id).await {
                    Ok(Some(record)) => match super::paused::paused_sandbox_detail(&record) {
                        Some(detail) => {
                            SandboxesSandboxIdGetResponse::Status200_SuccessfullyReturnedTheSandbox(
                                detail,
                            )
                        }
                        None => SandboxesSandboxIdGetResponse::Status404_NotFound(
                            sandbox_not_found(sandbox_id),
                        ),
                    },
                    Ok(None) => SandboxesSandboxIdGetResponse::Status404_NotFound(
                        sandbox_not_found(sandbox_id),
                    ),
                    Err(err) => SandboxesSandboxIdGetResponse::Status500_ServerError(
                        Self::snapshot_manager_error(&err),
                    ),
                });
            }
            Err(err) => {
                return Ok(SandboxesSandboxIdGetResponse::Status500_ServerError(
                    err.into(),
                ));
            }
        };
        Ok(
            SandboxesSandboxIdGetResponse::Status200_SuccessfullyReturnedTheSandbox(
                self.sandbox_detail_model(metadata),
            ),
        )
    }

    async fn sandboxes_sandbox_id_network_put(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
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

    async fn sandboxes_sandbox_id_custom_extension_params_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::SandboxesSandboxIdCustomExtensionParamsGetPathParams,
    ) -> Result<SandboxesSandboxIdCustomExtensionParamsGetResponse, ()> {
        let path_id = &path_params.sandbox_id;
        let Ok(sandbox_id) = SandboxId::parse_str(path_id) else {
            return Ok(
                SandboxesSandboxIdCustomExtensionParamsGetResponse::Status404_NotFound(
                    sandbox_not_found(path_id),
                ),
            );
        };

        match self.orchestrator.get_sandbox(&sandbox_id).await {
            Ok(Some(metadata)) => Ok(
                SandboxesSandboxIdCustomExtensionParamsGetResponse::Status200_TheCurrentCustomExtensionParams(
                    params_map_to_model(metadata.custom_extension_params.as_ref()),
                ),
            ),
            Ok(None) => Ok(
                SandboxesSandboxIdCustomExtensionParamsGetResponse::Status404_NotFound(
                    sandbox_not_found(path_id),
                ),
            ),
            Err(err) => Ok(
                SandboxesSandboxIdCustomExtensionParamsGetResponse::Status500_ServerError(err.into()),
            ),
        }
    }

    async fn sandboxes_sandbox_id_custom_extension_params_patch(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::SandboxesSandboxIdCustomExtensionParamsPatchPathParams,
        body: &std::collections::HashMap<String, agentenv_http_server::types::Object>,
    ) -> Result<SandboxesSandboxIdCustomExtensionParamsPatchResponse, ()> {
        let path_id = &path_params.sandbox_id;
        let Ok(sandbox_id) = SandboxId::parse_str(path_id) else {
            return Ok(
                SandboxesSandboxIdCustomExtensionParamsPatchResponse::Status404_NotFound(
                    sandbox_not_found(path_id),
                ),
            );
        };
        let patch = params_model_to_map(body);

        match self
            .orchestrator()
            .patch_sandbox_custom_extension_params(sandbox_id, patch)
            .await
        {
            Ok(new_params) => Ok(SandboxesSandboxIdCustomExtensionParamsPatchResponse::Status200_TheUpdatedFullCustomExtensionParams(
                params_map_to_model(new_params.as_ref()),
            )),
            Err(OrchestratorError::SandboxNotFound(id)) => Ok(
                SandboxesSandboxIdCustomExtensionParamsPatchResponse::Status404_NotFound(
                    sandbox_not_found(id),
                ),
            ),
            Err(OrchestratorError::InvalidSandboxState { state, .. }) => Ok(
                SandboxesSandboxIdCustomExtensionParamsPatchResponse::Status409_Conflict(
                    Self::error(
                        409,
                        format!("sandbox custom extension params cannot be patched from {} state", state),
                    ),
                ),
            ),
            Err(err @ OrchestratorError::SandboxOperationConflict { .. }) => Ok(
                SandboxesSandboxIdCustomExtensionParamsPatchResponse::Status409_Conflict(
                    Self::error(409, err.to_string()),
                ),
            ),
            // The extension rejected the patch, or no extension is configured.
            Err(err @ OrchestratorError::SandboxOperationFailed { .. }) => Ok(
                SandboxesSandboxIdCustomExtensionParamsPatchResponse::Status400_BadRequest(
                    Self::error(400, err.to_string()),
                ),
            ),
            Err(err) => Ok(
                SandboxesSandboxIdCustomExtensionParamsPatchResponse::Status500_ServerError(err.into()),
            ),
        }
    }

    async fn sandboxes_sandbox_id_pause_post(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::SandboxesSandboxIdPausePostPathParams,
    ) -> Result<SandboxesSandboxIdPausePostResponse, ()> {
        let path_id = &path_params.sandbox_id;
        let Ok(sandbox_id) = SandboxId::parse_str(path_id) else {
            return Ok(SandboxesSandboxIdPausePostResponse::Status404_NotFound(
                sandbox_not_found(path_id),
            ));
        };
        let timer = SandboxStageTimer::new("pause");
        match timer
            .time("pause", self.orchestrator().pause_sandbox(sandbox_id))
            .await
        {
            // The pause published its snapshot; the sandbox is gone from the node.
            Ok(_) => Ok(
                SandboxesSandboxIdPausePostResponse::Status204_TheSandboxWasPausedSuccessfullyAndCanBeResumed,
            ),
            Err(OrchestratorError::SandboxNotFound(id)) => {
                // Already paused reads as a conflict, never as absence.
                if self.owns_sandboxes()
                    && matches!(self.latest_paused_snapshot(id).await, Ok(Some(_)))
                {
                    return Ok(SandboxesSandboxIdPausePostResponse::Status409_Conflict(
                        Self::error(409, format!("sandbox {id} is already paused")),
                    ));
                }
                Ok(SandboxesSandboxIdPausePostResponse::Status404_NotFound(
                    sandbox_not_found(id),
                ))
            }
            Err(OrchestratorError::InvalidSandboxState { state, .. }) => Ok(
                SandboxesSandboxIdPausePostResponse::Status409_Conflict(Self::error(
                    409,
                    format!("sandbox cannot be paused from {} state", state),
                )),
            ),
            Err(err) => Ok(SandboxesSandboxIdPausePostResponse::Status500_ServerError(
                err.into(),
            )),
        }
    }

    async fn sandboxes_sandbox_id_snapshots_post(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::SandboxesSandboxIdSnapshotsPostPathParams,
        body: &models::SandboxSnapshotRequest,
    ) -> Result<SandboxesSandboxIdSnapshotsPostResponse, ()> {
        let timer = SandboxStageTimer::new("snapshot");
        let path_id = &path_params.sandbox_id;
        let Ok(sandbox_id) = SandboxId::parse_str(path_id) else {
            return Ok(SandboxesSandboxIdSnapshotsPostResponse::Status404_NotFound(
                sandbox_not_found(path_id),
            ));
        };

        let alias = match &body.name {
            Some(name) => match SnapshotAlias::parse(name) {
                Ok(alias) => Some(alias),
                Err(err) => {
                    return Ok(
                        SandboxesSandboxIdSnapshotsPostResponse::Status400_BadRequest(Self::error(
                            400,
                            format!("invalid snapshot alias: {}", err),
                        )),
                    );
                }
            },
            None => None,
        };

        let capture = match timer
            .time("capture", self.orchestrator().capture_snapshot(sandbox_id))
            .await
        {
            Ok(capture) => capture,
            Err(OrchestratorError::SandboxNotFound(id)) => {
                return Ok(SandboxesSandboxIdSnapshotsPostResponse::Status404_NotFound(
                    sandbox_not_found(id),
                ));
            }
            Err(OrchestratorError::InvalidSandboxState { state, .. }) => {
                return Ok(
                    SandboxesSandboxIdSnapshotsPostResponse::Status400_BadRequest(Self::error(
                        400,
                        format!("sandbox cannot be snapshotted from {} state", state),
                    )),
                );
            }
            Err(err) => {
                return Ok(
                    SandboxesSandboxIdSnapshotsPostResponse::Status500_ServerError(
                        Self::internal_error(&err),
                    ),
                );
            }
        };

        let published = match timer
            .time(
                "publish",
                // Use the staged result's id; a remote node may have chosen it.
                self.snapshot_manager.publish_captured(
                    crate::orchestrator::capture_publish_metadata(
                        &capture.metadata,
                        alias.clone(),
                        None,
                    ),
                    capture.captured_snapshot,
                ),
            )
            .await
        {
            Ok(snapshot) => snapshot,
            Err(err) => {
                let error =
                    Self::bad_request_for_repository_build_error(&err).unwrap_or_else(|| {
                        warn!(error = ?err, %sandbox_id, "failed to publish captured snapshot");
                        Self::error(500, err.to_string())
                    });
                return Ok(Self::client_or_server_response(
                    error,
                    SandboxesSandboxIdSnapshotsPostResponse::Status400_BadRequest,
                    SandboxesSandboxIdSnapshotsPostResponse::Status500_ServerError,
                ));
            }
        };

        let info = models::SnapshotInfo::from(published);

        Ok(SandboxesSandboxIdSnapshotsPostResponse::Status201_SnapshotCreatedSuccessfully(info))
    }

    async fn sandboxes_sandbox_id_refreshes_post(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::SandboxesSandboxIdRefreshesPostPathParams,
        body: &Option<models::SandboxRefreshRequest>,
    ) -> Result<SandboxesSandboxIdRefreshesPostResponse, ()> {
        let path_id = &path_params.sandbox_id;
        let Ok(sandbox_id) = SandboxId::parse_str(path_id) else {
            return Ok(SandboxesSandboxIdRefreshesPostResponse::Status404_NotFound(
                sandbox_not_found(path_id),
            ));
        };
        let timeout = body
            .as_ref()
            .and_then(|b| b.duration)
            .map(|d| Duration::from_secs(d as u64));

        match self
            .orchestrator
            .keep_alive_for(sandbox_id, timeout, false)
            .await
        {
            Ok(_) => Ok(
                SandboxesSandboxIdRefreshesPostResponse::Status204_SuccessfullyRefreshedTheSandbox,
            ),
            Err(OrchestratorError::SandboxNotFound(id)) => Ok(
                SandboxesSandboxIdRefreshesPostResponse::Status404_NotFound(sandbox_not_found(id)),
            ),
            // Preserve the 400 status for lifetime-limit refusals.
            Err(err @ OrchestratorError::SandboxLifetimeExceeded { .. }) => Ok(
                SandboxesSandboxIdRefreshesPostResponse::Status400_BadRequest(Self::error(
                    400,
                    err.to_string(),
                )),
            ),
            Err(err) => {
                Ok(SandboxesSandboxIdRefreshesPostResponse::Status500_ServerError(err.into()))
            }
        }
    }

    async fn sandboxes_sandbox_id_resume_post(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::SandboxesSandboxIdResumePostPathParams,
        body: &models::ResumedSandbox,
    ) -> Result<SandboxesSandboxIdResumePostResponse, ()> {
        let path_id = &path_params.sandbox_id;
        let Ok(sandbox_id) = SandboxId::parse_str(path_id) else {
            return Ok(SandboxesSandboxIdResumePostResponse::Status404_NotFound(
                sandbox_not_found(path_id),
            ));
        };
        let timeout = duration_from_secs(body.timeout).unwrap_or(default_sandbox_timeout());

        // A running sandbox is answered as it stands; a pause in flight is
        // waited out, since its end is what makes the row resumable; any other
        // transition is refused; only the absence of a record reads the row.
        match self.record_after_any_pause(sandbox_id).await {
            Ok(Some(metadata)) => match metadata.state {
                SandboxState::Running => {
                    let metadata = match self
                        .orchestrator
                        .keep_alive_for(sandbox_id, Some(timeout), true)
                        .await
                    {
                        Ok(Some(metadata)) => metadata,
                        Ok(None) => metadata,
                        Err(OrchestratorError::SandboxNotFound(id)) => {
                            return Ok(SandboxesSandboxIdResumePostResponse::Status404_NotFound(
                                sandbox_not_found(id),
                            ))
                        }
                        Err(err) => {
                            return Ok(SandboxesSandboxIdResumePostResponse::Status500_ServerError(
                                err.into(),
                            ))
                        }
                    };
                    return Ok(self.resumed_response(metadata));
                }
                SandboxState::Killing => {
                    return Ok(SandboxesSandboxIdResumePostResponse::Status404_NotFound(
                        sandbox_not_found(sandbox_id),
                    ));
                }
                state => {
                    return Ok(SandboxesSandboxIdResumePostResponse::Status409_Conflict(
                        Self::error(
                            409,
                            format!("sandbox cannot be resumed from {} state", state),
                        ),
                    ));
                }
            },
            Ok(None) => {}
            Err(OrchestratorError::InvalidSandboxState { state, .. }) => {
                return Ok(SandboxesSandboxIdResumePostResponse::Status409_Conflict(
                    Self::error(
                        409,
                        format!("sandbox is still {state} after waiting; resume it again shortly"),
                    ),
                ));
            }
            Err(err) => {
                return Ok(SandboxesSandboxIdResumePostResponse::Status500_ServerError(
                    err.into(),
                ));
            }
        }
        if !self.owns_sandboxes() {
            return Ok(SandboxesSandboxIdResumePostResponse::Status404_NotFound(
                sandbox_not_found(sandbox_id),
            ));
        }

        let record = match self.latest_paused_snapshot(sandbox_id).await {
            Ok(Some(record)) => record,
            Ok(None) => {
                return Ok(SandboxesSandboxIdResumePostResponse::Status404_NotFound(
                    sandbox_not_found(sandbox_id),
                ));
            }
            Err(err) => {
                return Ok(SandboxesSandboxIdResumePostResponse::Status500_ServerError(
                    Self::snapshot_manager_error(&err),
                ));
            }
        };
        let request = match Self::restore_request(record.clone(), NewTimeout::Set(timeout)) {
            Ok(request) => request,
            Err(err) => {
                return Ok(SandboxesSandboxIdResumePostResponse::Status500_ServerError(
                    Self::error(500, err.to_string()),
                ));
            }
        };

        let timer = SandboxStageTimer::new("resume");
        match timer
            .time(
                "resume",
                self.orchestrator().restore_sandbox(sandbox_id, request),
            )
            .await
        {
            Ok(metadata) => {
                self.note_resume_landing(&record, &metadata).await;
                Ok(self.resumed_response(metadata))
            }
            Err(OrchestratorError::StoreOperationFailed(StoreError::SandboxAlreadyExists {
                ..
            })) => Ok(SandboxesSandboxIdResumePostResponse::Status409_Conflict(
                Self::error(409, "sandbox is already being resumed"),
            )),
            Err(OrchestratorError::InvalidSandboxState { state, .. }) => Ok(
                SandboxesSandboxIdResumePostResponse::Status409_Conflict(Self::error(
                    409,
                    format!("sandbox cannot be resumed from {} state", state),
                )),
            ),
            Err(err) => Ok(SandboxesSandboxIdResumePostResponse::Status500_ServerError(
                err.into(),
            )),
        }
    }

    async fn sandboxes_sandbox_id_timeout_post(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::SandboxesSandboxIdTimeoutPostPathParams,
        body: &Option<models::SandboxTimeoutRequest>,
    ) -> Result<SandboxesSandboxIdTimeoutPostResponse, ()> {
        let path_id = &path_params.sandbox_id;
        let Ok(sandbox_id) = SandboxId::parse_str(path_id) else {
            return Ok(SandboxesSandboxIdTimeoutPostResponse::Status404_NotFound(
                sandbox_not_found(path_id),
            ));
        };
        let timeout = body.as_ref().map(|b| Duration::from_secs(b.timeout as u64));

        match self
            .orchestrator
            .keep_alive_for(sandbox_id, timeout, true)
            .await
        {
            Ok(_) => Ok(
                SandboxesSandboxIdTimeoutPostResponse::Status204_SuccessfullySetTheSandboxTimeout,
            ),
            Err(OrchestratorError::SandboxNotFound(id)) => Ok(
                SandboxesSandboxIdTimeoutPostResponse::Status404_NotFound(sandbox_not_found(id)),
            ),
            Err(err @ OrchestratorError::SandboxLifetimeExceeded { .. }) => {
                Ok(SandboxesSandboxIdTimeoutPostResponse::Status400_BadRequest(
                    Self::error(400, err.to_string()),
                ))
            }
            Err(err) => {
                Ok(SandboxesSandboxIdTimeoutPostResponse::Status500_ServerError(err.into()))
            }
        }
    }

    async fn v2_sandboxes_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        query_params: &models::V2SandboxesGetQueryParams,
    ) -> Result<V2SandboxesGetResponse, ()> {
        // Only two states are supported. With both, or neither, named, the
        // listing spans running records and paused rows alike.
        let (want_running, want_paused) = if query_params.state.len() == 1 {
            match query_params.state[0] {
                models::SandboxState::Running => (true, false),
                models::SandboxState::Paused => (false, true),
            }
        } else {
            (true, true)
        };
        let user_metadata = parse_metadata_filter(&query_params.metadata);
        let filter = SandboxListFilter {
            states: Some(vec![SandboxState::Running]),
            excluded_states: None,
            user_metadata: user_metadata.clone(),
        };

        let cursor = match query_params.next_token.as_deref() {
            Some(token) => match PaginationCursor::parse(token) {
                Ok(cursor) => cursor,
                Err(err) => {
                    return Ok(V2SandboxesGetResponse::Status400_BadRequest(Self::error(
                        400,
                        format!("invalid next token: {}", err),
                    )));
                }
            },
            None => PaginationCursor::new(SystemTime::now(), SandboxId::max()),
        };

        // Running records are read either way: a resumed sandbox keeps the row
        // it was resumed from, and only its record says it is not paused.
        let running = match self.orchestrator.list_sandboxes_filtered(filter).await {
            Ok(list) => list,
            Err(err) => {
                return Ok(V2SandboxesGetResponse::Status500_ServerError(err.into()));
            }
        };
        let mut listed: Vec<(SystemTime, SandboxId, models::ListedSandbox)> = if want_running {
            running
                .iter()
                .map(|sandbox| {
                    (
                        sandbox.created_at,
                        sandbox.id,
                        models::ListedSandbox::from(sandbox.clone()),
                    )
                })
                .collect()
        } else {
            Vec::new()
        };
        if want_paused && self.owns_sandboxes() {
            let running_ids: std::collections::HashSet<SandboxId> =
                running.iter().map(|sandbox| sandbox.id).collect();
            let paused = match self.list_paused_snapshots().await {
                Ok(paused) => paused,
                Err(err) => {
                    return Ok(V2SandboxesGetResponse::Status500_ServerError(
                        Self::snapshot_manager_error(&err),
                    ));
                }
            };
            for record in &paused {
                let Some(sandbox_id) = super::paused::paused_sandbox_id(record) else {
                    continue;
                };
                // A sandbox resumed since its last pause is listed once, as running.
                if running_ids.contains(&sandbox_id) {
                    continue;
                }
                let Some(model) = super::paused::listed_paused_sandbox(record) else {
                    continue;
                };
                if let Some(wanted) = user_metadata.as_ref() {
                    let has = model.metadata.as_ref();
                    if !wanted.iter().all(|(key, value)| {
                        has.is_some_and(|metadata| metadata.get(key) == Some(value))
                    }) {
                        continue;
                    }
                }
                listed.push((super::paused::paused_started_at(record), sandbox_id, model));
            }
        }

        let page = cursor.paginate(
            listed,
            query_params.limit,
            |a, b| PaginationCursor::compare_desc(a.0, &a.1, b.0, &b.1),
            |entry, cursor| {
                PaginationCursor::compare_desc(entry.0, &entry.1, cursor.time(), cursor.value())
            },
            |entry| PaginationCursor::new(entry.0, entry.1),
        );

        let out = page
            .items
            .into_iter()
            .map(|(_, _, model)| model)
            .collect::<Vec<_>>();

        Ok(
            V2SandboxesGetResponse::Status200_SuccessfullyReturnedAllRunningSandboxes {
                body: out,
                x_next_token: page.next_token,
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn parse_metadata_filter_with_none_returns_none() {
        assert_eq!(parse_metadata_filter(&None), None);
    }

    #[test]
    fn parse_metadata_filter_with_empty_string_returns_none() {
        assert_eq!(parse_metadata_filter(&Some(String::new())), None);
    }

    #[test]
    fn parse_metadata_filter_with_single_pair() {
        let result = parse_metadata_filter(&Some("key=value".to_string()));
        assert_eq!(
            result,
            Some(HashMap::from([("key".to_string(), "value".to_string())]))
        );
    }

    #[test]
    fn parse_metadata_filter_with_multiple_pairs() {
        let result = parse_metadata_filter(&Some("a=1&b=2".to_string()));
        assert_eq!(result.map(|m| m.len()), Some(2));
    }

    #[test]
    fn parse_metadata_filter_filters_empty_keys() {
        let result = parse_metadata_filter(&Some("=value&key=val".to_string()));
        let map = result.unwrap();
        assert!(!map.contains_key(""));
        assert!(map.contains_key("key"));
    }

    #[test]
    fn parse_metadata_filter_with_encoded_characters() {
        let result = parse_metadata_filter(&Some(
            "key%20with%20spaces=value%20with%20spaces".to_string(),
        ));
        assert_eq!(
            result,
            Some(HashMap::from([(
                "key with spaces".to_string(),
                "value with spaces".to_string()
            )]))
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

#[cfg(test)]
mod routing_header_tests {
    use super::*;
    use std::time::UNIX_EPOCH;

    const ROUTING_OPERATIONS: [&str; 4] = [
        "/sandboxes",
        "/sandboxes-cold",
        "/sandboxes/{sandboxID}/resume",
        "/sandboxes/{sandboxID}/connect",
    ];

    const ROUTING_HEADERS: [&str; 3] = [
        "x-agentenv-sandbox-id",
        "x-agentenv-execution-id",
        "x-agentenv-projection-ttl-secs",
    ];

    const SPEC: &str = include_str!("../openapi.yml");

    fn indent_of(line: &str) -> usize {
        line.len() - line.trim_start().len()
    }

    fn block_after(lines: &[&str], header_index: usize) -> Vec<String> {
        let base = indent_of(lines[header_index]);

        lines[header_index + 1..]
            .iter()
            .take_while(|line| line.trim().is_empty() || indent_of(line) > base)
            .map(|line| (*line).to_string())
            .collect()
    }

    fn schema_block(name: &str) -> Vec<String> {
        let lines: Vec<&str> = SPEC.lines().collect();
        let header = format!("    {name}:");
        let index = lines
            .iter()
            .position(|line| *line == header)
            .unwrap_or_else(|| panic!("{name} is not a schema in the spec"));

        block_after(&lines, index)
    }

    #[test]
    fn routing_headers_name_the_sandbox_the_run_and_the_budget() {
        let created_at = UNIX_EPOCH + Duration::from_secs(1_000);
        let metadata = SandboxMetadata {
            created_at,
            max_lifetime: Some(Duration::from_secs(86_400)),
            running_since: Some(created_at),
            ..Default::default()
        };

        let routing = RoutingHeaders::of(&metadata);

        assert_eq!(routing.sandbox_id, metadata.id.to_string());
        assert_eq!(routing.execution_id, metadata.execution_id.to_string());
        assert!(
            routing.projection_ttl_secs > 0,
            "a sandbox past its ceiling still asks for a short record, not an immortal one"
        );
    }

    #[test]
    fn routing_headers_report_zero_when_the_node_has_no_ceiling() {
        let metadata = SandboxMetadata {
            max_lifetime: None,
            ..Default::default()
        };

        assert_eq!(RoutingHeaders::of(&metadata).projection_ttl_secs, 0);
    }

    #[test]
    fn a_live_sandbox_reports_a_budget_that_outlives_it() {
        let metadata = SandboxMetadata {
            created_at: SystemTime::now(),
            max_lifetime: Some(Duration::from_secs(3_600)),
            ..Default::default()
        };

        let ttl = RoutingHeaders::of(&metadata).projection_ttl_secs;

        assert!(
            ttl >= 3_600,
            "expected at least the remaining hour, got {ttl}"
        );
    }

    #[test]
    fn every_routing_response_declares_all_three_headers() {
        let lines: Vec<&str> = SPEC.lines().collect();
        let mut path: Option<&str> = None;
        let mut offenders: Vec<String> = Vec::new();
        let mut checked = 0usize;

        for (index, line) in lines.iter().enumerate() {
            let trimmed = line.trim();

            if indent_of(line) == 2 && trimmed.starts_with('/') && trimmed.ends_with(':') {
                path = Some(trimmed.trim_end_matches(':'));
                continue;
            }
            if indent_of(line) != 8 || !trimmed.starts_with("\"2") {
                continue;
            }
            let Some(path) = path.filter(|path| ROUTING_OPERATIONS.contains(path)) else {
                continue;
            };
            if !lines[..index]
                .iter()
                .rev()
                .find(|line| indent_of(line) == 4 && line.trim().ends_with(':'))
                .is_some_and(|line| line.trim() == "post:")
            {
                continue;
            }

            let status = trimmed.trim_end_matches(':');
            let block = block_after(&lines, index);
            checked += 1;

            for header in ROUTING_HEADERS {
                if !block.iter().any(|line| line.trim() == format!("{header}:")) {
                    offenders.push(format!("{path} {status} does not return {header}"));
                }
            }
        }

        assert!(
            offenders.is_empty(),
            "every 2xx the gateway records a routing projection from must name the sandbox, \
             the run, and the budget: {offenders:?}"
        );
        assert_eq!(
            checked, 5,
            "expected create, cold, resume, and both connect outcomes"
        );
    }

    #[test]
    fn the_fork_result_carries_the_budget_and_the_user_facing_model_does_not() {
        let carries = |name: &str| {
            schema_block(name)
                .iter()
                .any(|line| line.trim() == "projectionTtlSecs:")
        };

        assert!(
            carries("SandboxForkResult"),
            "fork's per-child budget has nowhere else to travel: one response, N children, \
             N incarnations"
        );
        assert!(
            !carries("Sandbox"),
            "the user-facing model must not grow a routing-infrastructure field"
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
        let spec = include_str!("../openapi.yml");
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
            &super::super::Claims,
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
mod paused_sandbox_rest_tests {
    use std::sync::Arc;

    use axum_extra::extract::CookieJar;
    use headers::Host;
    use http::Method;

    use agentenv_http_server::apis::sandboxes::*;
    use agentenv_http_server::models;

    use super::ApiImpl;
    use crate::api::ResumeWiring;
    use crate::orchestrator::{Orchestrator, SandboxState};
    use crate::sandbox::mock::MockBackendFactory;
    use crate::snapshot::mock::{
        in_memory_snapshot_manager, mock_paused_sandbox_config, paused_sandbox_record,
        InMemorySnapshotCatalog,
    };
    use crate::snapshot::repository::interfaces::SnapshotCatalog;
    use crate::snapshot::PausedSandboxConfig;
    use crate::types::SandboxId;

    const ORIGIN: &str = "node-origin";

    struct Surface {
        api: Arc<ApiImpl>,
        catalog: Arc<InMemorySnapshotCatalog>,
    }

    impl Surface {
        async fn as_half(wiring: ResumeWiring) -> Self {
            let orchestrator = Orchestrator::with_in_memory_store(MockBackendFactory::new()).await;
            let (snapshot_manager, catalog) = in_memory_snapshot_manager();
            let api = Arc::new(ApiImpl::new(
                orchestrator,
                Arc::new(snapshot_manager),
                None,
                Vec::new(),
                wiring,
            ));
            Self { api, catalog }
        }

        async fn api_half() -> Self {
            Self::as_half(ResumeWiring::api_half_for_test()).await
        }

        async fn node_half() -> Self {
            Self::as_half(ResumeWiring::node_local(ORIGIN)).await
        }

        fn paused_at(&self, sandbox_id: SandboxId, paused: PausedSandboxConfig, at_unix_ms: i64) {
            self.catalog.seed(paused_sandbox_record(
                sandbox_id,
                Some(ORIGIN),
                paused,
                at_unix_ms,
            ));
        }

        fn paused(&self, paused: PausedSandboxConfig) -> SandboxId {
            let sandbox_id = SandboxId::new();
            self.paused_at(sandbox_id, paused, 1_700_000_000_000);
            sandbox_id
        }

        async fn state_of(&self, sandbox_id: SandboxId) -> Option<SandboxState> {
            self.api
                .orchestrator()
                .get_sandbox(&sandbox_id)
                .await
                .expect("the store answers")
                .map(|metadata| metadata.state)
        }

        async fn get(&self, sandbox_id: SandboxId) -> SandboxesSandboxIdGetResponse {
            self.api
                .sandboxes_sandbox_id_get(
                    &Method::GET,
                    &host(),
                    &CookieJar::new(),
                    &super::super::Claims,
                    &models::SandboxesSandboxIdGetPathParams {
                        sandbox_id: sandbox_id.to_string(),
                    },
                )
                .await
                .expect("the handler answers")
        }

        async fn pause(&self, sandbox_id: SandboxId) -> SandboxesSandboxIdPausePostResponse {
            self.api
                .sandboxes_sandbox_id_pause_post(
                    &Method::POST,
                    &host(),
                    &CookieJar::new(),
                    &super::super::Claims,
                    &models::SandboxesSandboxIdPausePostPathParams {
                        sandbox_id: sandbox_id.to_string(),
                    },
                )
                .await
                .expect("the handler answers")
        }

        async fn resume(&self, sandbox_id: SandboxId) -> SandboxesSandboxIdResumePostResponse {
            self.api
                .sandboxes_sandbox_id_resume_post(
                    &Method::POST,
                    &host(),
                    &CookieJar::new(),
                    &super::super::Claims,
                    &models::SandboxesSandboxIdResumePostPathParams {
                        sandbox_id: sandbox_id.to_string(),
                    },
                    &models::ResumedSandbox { timeout: Some(60) },
                )
                .await
                .expect("the handler answers")
        }

        async fn connect(&self, sandbox_id: SandboxId) -> SandboxesSandboxIdConnectPostResponse {
            self.api
                .sandboxes_sandbox_id_connect_post(
                    &Method::POST,
                    &host(),
                    &CookieJar::new(),
                    &super::super::Claims,
                    &models::SandboxesSandboxIdConnectPostPathParams {
                        sandbox_id: sandbox_id.to_string(),
                    },
                    &models::ConnectSandbox::new(60),
                )
                .await
                .expect("the handler answers")
        }

        async fn delete(&self, sandbox_id: SandboxId) -> SandboxesSandboxIdDeleteResponse {
            self.api
                .sandboxes_sandbox_id_delete(
                    &Method::DELETE,
                    &host(),
                    &CookieJar::new(),
                    &super::super::Claims,
                    &models::SandboxesSandboxIdDeletePathParams {
                        sandbox_id: sandbox_id.to_string(),
                    },
                )
                .await
                .expect("the handler answers")
        }

        async fn list_v2(&self, state: Vec<models::SandboxState>) -> Vec<models::ListedSandbox> {
            let response = self
                .api
                .v2_sandboxes_get(
                    &Method::GET,
                    &host(),
                    &CookieJar::new(),
                    &super::super::Claims,
                    &models::V2SandboxesGetQueryParams {
                        metadata: None,
                        state,
                        next_token: None,
                        limit: None,
                    },
                )
                .await
                .expect("the handler answers");
            match response {
                V2SandboxesGetResponse::Status200_SuccessfullyReturnedAllRunningSandboxes {
                    body,
                    ..
                } => body,
                other => panic!("expected a listing, got {other:?}"),
            }
        }
    }

    fn host() -> Host {
        Host::from(http::uri::Authority::from_static("localhost"))
    }

    fn detail(response: SandboxesSandboxIdGetResponse) -> models::SandboxDetail {
        match response {
            SandboxesSandboxIdGetResponse::Status200_SuccessfullyReturnedTheSandbox(detail) => {
                detail
            }
            other => panic!("expected the sandbox, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_paused_sandbox_is_read_from_its_row_with_state_paused() {
        let surface = Surface::api_half().await;
        let sandbox_id = surface.paused(PausedSandboxConfig {
            template_id: "tpl-from-the-row".to_string(),
            auto_resume: false,
            user_metadata: Some([("owner".to_string(), "row".to_string())].into()),
            ..mock_paused_sandbox_config()
        });

        let detail = detail(surface.get(sandbox_id).await);

        assert_eq!(detail.state, models::SandboxState::Paused);
        assert_eq!(detail.sandbox_id, sandbox_id.to_string());
        assert_eq!(detail.template_id, "tpl-from-the-row");
        assert_eq!(
            detail.metadata,
            Some([("owner".to_string(), "row".to_string())].into())
        );
        assert_eq!(
            detail
                .lifecycle
                .as_ref()
                .map(|lifecycle| lifecycle.auto_resume),
            Some(false),
            "the row's lifecycle flags are what the client reads"
        );
        assert!(
            matches!(
                surface.get(SandboxId::new()).await,
                SandboxesSandboxIdGetResponse::Status404_NotFound(_)
            ),
            "a sandbox with neither a record nor a row is not found"
        );
    }

    #[tokio::test]
    async fn the_node_half_does_not_read_paused_rows() {
        let node = Surface::node_half().await;
        let sandbox_id = node.paused(mock_paused_sandbox_config());

        assert!(
            matches!(
                node.get(sandbox_id).await,
                SandboxesSandboxIdGetResponse::Status404_NotFound(_)
            ),
            "the row sits in this process's catalog and the node half still answers 404"
        );
        assert!(matches!(
            node.resume(sandbox_id).await,
            SandboxesSandboxIdResumePostResponse::Status404_NotFound(_)
        ));
        assert!(matches!(
            node.pause(sandbox_id).await,
            SandboxesSandboxIdPausePostResponse::Status404_NotFound(_)
        ));
        assert!(
            matches!(
                node.delete(sandbox_id).await,
                SandboxesSandboxIdDeleteResponse::Status404_NotFound(_)
            ),
            "and it deletes no rows it does not own"
        );
        assert!(node.list_v2(Vec::new()).await.is_empty());
    }

    #[tokio::test]
    async fn the_newest_pause_of_a_sandbox_is_the_one_read() {
        let surface = Surface::api_half().await;
        let sandbox_id = SandboxId::new();
        surface.paused_at(
            sandbox_id,
            PausedSandboxConfig {
                template_id: "older".to_string(),
                ..mock_paused_sandbox_config()
            },
            1_700_000_001_000,
        );
        surface.paused_at(
            sandbox_id,
            PausedSandboxConfig {
                template_id: "newer".to_string(),
                ..mock_paused_sandbox_config()
            },
            1_700_000_002_000,
        );

        assert_eq!(detail(surface.get(sandbox_id).await).template_id, "newer");
    }

    #[tokio::test]
    async fn pausing_an_already_paused_sandbox_is_a_conflict_and_pausing_nothing_is_not_found() {
        let surface = Surface::api_half().await;
        let sandbox_id = surface.paused(mock_paused_sandbox_config());

        assert!(
            matches!(
                surface.pause(sandbox_id).await,
                SandboxesSandboxIdPausePostResponse::Status409_Conflict(_)
            ),
            "a paused sandbox exists, so pausing it again is a conflict, never absence"
        );
        assert!(matches!(
            surface.pause(SandboxId::new()).await,
            SandboxesSandboxIdPausePostResponse::Status404_NotFound(_)
        ));
    }

    #[tokio::test]
    async fn a_resume_rebuilds_the_sandbox_from_its_row_and_answers_created() {
        let surface = Surface::api_half().await;
        let sandbox_id = surface.paused(PausedSandboxConfig {
            user_metadata: Some([("owner".to_string(), "row".to_string())].into()),
            ..mock_paused_sandbox_config()
        });
        assert_eq!(surface.state_of(sandbox_id).await, None);

        let response = surface.resume(sandbox_id).await;

        let SandboxesSandboxIdResumePostResponse::Status201_TheSandboxWasResumedSuccessfully {
            body,
            x_agentenv_execution_id,
            ..
        } = response
        else {
            panic!("expected the sandbox to be rebuilt, got {response:?}");
        };
        assert_eq!(body.sandbox_id, sandbox_id.to_string());
        let rebuilt = surface
            .api
            .orchestrator()
            .get_sandbox(&sandbox_id)
            .await
            .expect("the store answers")
            .expect("the resume created a record under the sandbox's own id");
        assert_eq!(rebuilt.state, SandboxState::Running);
        assert_eq!(
            x_agentenv_execution_id,
            Some(rebuilt.execution_id.to_string()),
            "the routing headers name the incarnation the resume minted"
        );
        assert_eq!(
            rebuilt.user_metadata,
            Some([("owner".to_string(), "row".to_string())].into()),
            "the rebuilt sandbox carries the configuration its row paused with"
        );

        let detail = detail(surface.get(sandbox_id).await);
        assert_eq!(
            detail.state,
            models::SandboxState::Running,
            "once running, the record answers, not the row"
        );

        assert!(
            matches!(
                surface.resume(sandbox_id).await,
                SandboxesSandboxIdResumePostResponse::Status201_TheSandboxWasResumedSuccessfully { .. }
            ),
            "a resume of a running sandbox is answered as it stands"
        );
        assert!(matches!(
            surface.resume(SandboxId::new()).await,
            SandboxesSandboxIdResumePostResponse::Status404_NotFound(_)
        ));
    }

    #[tokio::test]
    async fn a_connect_rebuilds_a_paused_sandbox_and_answers_a_running_one_as_it_stands() {
        let surface = Surface::api_half().await;
        let sandbox_id = surface.paused(mock_paused_sandbox_config());

        assert!(matches!(
            surface.connect(sandbox_id).await,
            SandboxesSandboxIdConnectPostResponse::Status201_TheSandboxWasResumedSuccessfully { .. }
        ));
        assert_eq!(
            surface.state_of(sandbox_id).await,
            Some(SandboxState::Running)
        );
        assert!(matches!(
            surface.connect(sandbox_id).await,
            SandboxesSandboxIdConnectPostResponse::Status200_TheSandboxWasAlreadyRunning { .. }
        ));
        assert!(matches!(
            surface.connect(SandboxId::new()).await,
            SandboxesSandboxIdConnectPostResponse::Status404_NotFound(_)
        ));
    }

    #[tokio::test]
    async fn a_resume_that_lands_on_no_known_node_leaves_the_rows_origin_alone() {
        let surface = Surface::api_half().await;
        let sandbox_id = SandboxId::new();
        let record = paused_sandbox_record(
            sandbox_id,
            Some(ORIGIN),
            mock_paused_sandbox_config(),
            1_700_000_000_000,
        );
        surface.catalog.seed(record.clone());

        let _ = surface.resume(sandbox_id).await;

        assert_eq!(
            surface
                .api
                .orchestrator()
                .sandbox_holding_node_id(&sandbox_id)
                .await,
            None,
            "the premise: this backend names no node for the sandbox it runs"
        );
        let landed = surface
            .catalog
            .get(&record.id.to_string())
            .await
            .expect("the catalog answers")
            .expect("the row outlives the resume");
        assert_eq!(
            landed.origin_node_id.as_deref(),
            Some(ORIGIN),
            "a landing nobody can name must not erase the node whose cache is warm"
        );
    }

    #[tokio::test]
    async fn deleting_a_paused_sandbox_forgets_every_pause_of_it() {
        let surface = Surface::api_half().await;
        let sandbox_id = SandboxId::new();
        surface.paused_at(sandbox_id, mock_paused_sandbox_config(), 1_700_000_001_000);
        surface.paused_at(sandbox_id, mock_paused_sandbox_config(), 1_700_000_002_000);
        let other = surface.paused(mock_paused_sandbox_config());

        assert!(matches!(
            surface.delete(sandbox_id).await,
            SandboxesSandboxIdDeleteResponse::Status204_TheSandboxWasKilledSuccessfully
        ));

        assert!(
            matches!(
                surface.get(sandbox_id).await,
                SandboxesSandboxIdGetResponse::Status404_NotFound(_)
            ),
            "every pause of the sandbox is gone, not just the newest"
        );
        assert!(
            matches!(
                surface.delete(sandbox_id).await,
                SandboxesSandboxIdDeleteResponse::Status404_NotFound(_)
            ),
            "deleting what is neither running nor paused is not found"
        );
        assert!(
            matches!(
                surface.get(other).await,
                SandboxesSandboxIdGetResponse::Status200_SuccessfullyReturnedTheSandbox(_)
            ),
            "and another sandbox's pause is untouched"
        );
    }

    #[tokio::test]
    async fn deleting_a_running_sandbox_forgets_its_pauses_too() {
        let surface = Surface::api_half().await;
        let sandbox_id = surface.paused(mock_paused_sandbox_config());
        let _ = surface.resume(sandbox_id).await;
        // The resume left the row behind; a delete must not leave a paused ghost.
        assert!(matches!(
            surface.get(sandbox_id).await,
            SandboxesSandboxIdGetResponse::Status200_SuccessfullyReturnedTheSandbox(_)
        ));

        assert!(matches!(
            surface.delete(sandbox_id).await,
            SandboxesSandboxIdDeleteResponse::Status204_TheSandboxWasKilledSuccessfully
        ));

        assert_eq!(surface.state_of(sandbox_id).await, None);
        assert!(matches!(
            surface.get(sandbox_id).await,
            SandboxesSandboxIdGetResponse::Status404_NotFound(_)
        ));
    }

    #[tokio::test]
    async fn the_v2_listing_shows_each_sandbox_once_and_a_running_one_as_running() {
        let surface = Surface::api_half().await;
        let resumed = surface.paused(mock_paused_sandbox_config());
        let still_paused = surface.paused(mock_paused_sandbox_config());
        let _ = surface.resume(resumed).await;

        let all = surface.list_v2(Vec::new()).await;
        let mut states: Vec<(String, models::SandboxState)> = all
            .iter()
            .map(|sandbox| (sandbox.sandbox_id.clone(), sandbox.state))
            .collect();
        states.sort();
        let mut expected = vec![
            (resumed.to_string(), models::SandboxState::Running),
            (still_paused.to_string(), models::SandboxState::Paused),
        ];
        expected.sort();
        assert_eq!(
            states, expected,
            "a resumed sandbox still has its row and must be listed once, as running"
        );

        let running_only = surface.list_v2(vec![models::SandboxState::Running]).await;
        assert_eq!(
            running_only
                .iter()
                .map(|sandbox| sandbox.sandbox_id.clone())
                .collect::<Vec<_>>(),
            vec![resumed.to_string()]
        );
    }

    #[tokio::test]
    async fn the_paused_only_listing_does_not_show_a_sandbox_that_is_running() {
        let surface = Surface::api_half().await;
        let resumed = surface.paused(mock_paused_sandbox_config());
        let still_paused = surface.paused(mock_paused_sandbox_config());
        let _ = surface.resume(resumed).await;

        let paused_only = surface.list_v2(vec![models::SandboxState::Paused]).await;

        assert_eq!(
            paused_only
                .iter()
                .map(|sandbox| sandbox.sandbox_id.clone())
                .collect::<Vec<_>>(),
            vec![still_paused.to_string()],
            "a running sandbox keeps the row it was resumed from; the row is not a \
             second, paused sandbox"
        );
    }

    /// Puts a record mid-pause under `sandbox_id`, as a pause in flight leaves it.
    async fn pause_in_flight(surface: &Surface, sandbox_id: SandboxId) {
        surface
            .api
            .orchestrator()
            .set_metadata_state_for_test(sandbox_id, SandboxState::Pausing)
            .await
            .expect("the store answers");
    }

    /// Ends the pause in flight shortly, the way a real one ends: the record
    /// goes away and the row is all that is left.
    fn finish_pause_later(surface: &Surface, sandbox_id: SandboxId) {
        let orchestrator = surface.api.orchestrator();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            orchestrator
                .remove_sandbox_for_test(&sandbox_id)
                .await
                .expect("the store answers");
        });
    }

    /// Fails the pause in flight shortly: the VM is back and the record is running.
    fn fail_pause_later(surface: &Surface, sandbox_id: SandboxId) {
        let orchestrator = surface.api.orchestrator();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            orchestrator
                .set_metadata_state_for_test(sandbox_id, SandboxState::Running)
                .await
                .expect("the store answers");
        });
    }

    #[tokio::test]
    async fn a_resume_during_a_pause_waits_for_it_and_rebuilds_from_the_row() {
        let surface = Surface::api_half().await;
        let sandbox_id = surface.paused(mock_paused_sandbox_config());
        pause_in_flight(&surface, sandbox_id).await;
        finish_pause_later(&surface, sandbox_id);

        let response = surface.resume(sandbox_id).await;

        assert!(
            matches!(
                response,
                SandboxesSandboxIdResumePostResponse::Status201_TheSandboxWasResumedSuccessfully { .. }
            ),
            "the resume waited for the pause to finish and rebuilt from the row, got {response:?}"
        );
        assert_eq!(
            surface.state_of(sandbox_id).await,
            Some(SandboxState::Running)
        );
    }

    #[tokio::test]
    async fn a_connect_during_a_pause_waits_for_it_and_rebuilds_from_the_row() {
        let surface = Surface::api_half().await;
        let sandbox_id = surface.paused(mock_paused_sandbox_config());
        pause_in_flight(&surface, sandbox_id).await;
        finish_pause_later(&surface, sandbox_id);

        let response = surface.connect(sandbox_id).await;

        assert!(
            matches!(
                response,
                SandboxesSandboxIdConnectPostResponse::Status201_TheSandboxWasResumedSuccessfully { .. }
            ),
            "the connect waited for the pause to finish and rebuilt from the row, got {response:?}"
        );
        assert_eq!(
            surface.state_of(sandbox_id).await,
            Some(SandboxState::Running)
        );
    }

    #[tokio::test]
    async fn a_resume_during_a_pause_that_fails_answers_the_sandbox_that_kept_running() {
        let surface = Surface::api_half().await;
        let sandbox_id = surface.paused(mock_paused_sandbox_config());
        pause_in_flight(&surface, sandbox_id).await;
        fail_pause_later(&surface, sandbox_id);

        let response = surface.resume(sandbox_id).await;

        assert!(
            matches!(
                response,
                SandboxesSandboxIdResumePostResponse::Status201_TheSandboxWasResumedSuccessfully { .. }
            ),
            "a pause that failed leaves the sandbox running, and the resume answers it as it \
             stands, got {response:?}"
        );
        assert!(
            matches!(
                surface.connect(sandbox_id).await,
                SandboxesSandboxIdConnectPostResponse::Status200_TheSandboxWasAlreadyRunning { .. }
            ),
            "nothing was rebuilt over the sandbox that kept running"
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
            &super::super::Claims,
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
            &super::super::Claims,
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
