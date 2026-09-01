use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use axum_extra::extract::CookieJar;
use headers::Host;
use http::Method;

use tracing::{info, warn};

use crate::cfg::ConfigManager;
use crate::observability::prometheus::SandboxStageTimer;
use crate::orchestrator::{
    CreateSandboxRequest, ForkChildren, NewTimeout, OrchestratorError, SandboxExpiry,
    SandboxLaunchSource, SandboxListFilter, SandboxMetadata, SandboxState, SandboxTimeoutAction,
};
use crate::sandbox::CustomExtensionParams;
use crate::sandbox::{BaseSandboxNetworkPolicy, SandboxNetworkEgressPolicy, SandboxNetworkPolicy};
use crate::snapshot::SnapshotAlias;
use crate::types::{SandboxId, SandboxResources};
use agentenv_http_server::apis::sandboxes::*;
use agentenv_http_server::models;
use agentenv_http_server::types::Nullable;

use super::attached_drives::unresolved_attached_drives;
use super::pagination::PaginationCursor;
use super::paused_recovery::{CrossNodeResume, MissingLocalResume, ResumeArbitration};
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
            SandboxState::Pausing
            | SandboxState::Paused
            | SandboxState::Snapshotting
            | SandboxState::Forking => Self::Paused,
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
            mask_request_host: None,
        }
    }
}

fn base_policy_from_allow_internet_access(value: Option<bool>) -> BaseSandboxNetworkPolicy {
    match value {
        Some(true) => BaseSandboxNetworkPolicy::Allow,
        Some(false) => BaseSandboxNetworkPolicy::Deny,
        None => BaseSandboxNetworkPolicy::Default,
    }
}

fn allow_internet_access_from_base_policy(policy: BaseSandboxNetworkPolicy) -> Nullable<bool> {
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

    /// Maps a snapshot-backed rebuild onto the resume endpoint's responses.
    fn rebuilt_resume_response(
        &self,
        rebuilt: CrossNodeResume,
        sandbox_id: SandboxId,
    ) -> SandboxesSandboxIdResumePostResponse {
        match rebuilt {
            CrossNodeResume::Restored(metadata) => {
                let routing = RoutingHeaders::of(&metadata);

                SandboxesSandboxIdResumePostResponse::Status201_TheSandboxWasResumedSuccessfully {
                    body: self.sandbox_model(*metadata),
                    x_agentenv_sandbox_id: Some(routing.sandbox_id),
                    x_agentenv_execution_id: Some(routing.execution_id),
                    x_agentenv_projection_ttl_secs: Some(routing.projection_ttl_secs),
                }
            }
            CrossNodeResume::NotFound => SandboxesSandboxIdResumePostResponse::Status404_NotFound(
                sandbox_not_found(sandbox_id.to_string()),
            ),
            CrossNodeResume::Failed(reason) => {
                SandboxesSandboxIdResumePostResponse::Status500_ServerError(Self::error(
                    500,
                    format!("failed to restore paused sandbox: {reason}"),
                ))
            }
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

fn network_policy_from_create(
    allow_internet_access: Option<bool>,
    network: Option<&models::SandboxNetworkConfig>,
) -> anyhow::Result<SandboxNetworkPolicy> {
    let base_policy = base_policy_from_allow_internet_access(allow_internet_access);
    let allow_out = network.and_then(|network| network.allow_out.clone());
    let deny_out = network.and_then(|network| network.deny_out.clone());
    let egress = SandboxNetworkEgressPolicy::new(allow_out, deny_out)?;
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
    let policy = SandboxNetworkEgressPolicy::new(body.allow_out.clone(), body.deny_out.clone())?;
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
        let metadata = match self.orchestrator.get_sandbox(&sandbox_id).await {
            Ok(Some(metadata)) => metadata,
            Ok(None) => {
                return Ok(SandboxesSandboxIdConnectPostResponse::Status404_NotFound(
                    sandbox_not_found(sandbox_id),
                ));
            }
            Err(err) => {
                return Ok(
                    SandboxesSandboxIdConnectPostResponse::Status500_ServerError(err.into()),
                );
            }
        };

        match metadata.state {
            SandboxState::Creating
            | SandboxState::Resuming
            | SandboxState::Running
            | SandboxState::Snapshotting
            | SandboxState::Forking => {
                match self
                    .orchestrator
                    .keep_alive_for(sandbox_id, duration_from_secs(Some(body.timeout)), false)
                    .await
                {
                    Ok(_) => {}
                    Err(OrchestratorError::SandboxNotFound(id)) => {
                        return Ok(SandboxesSandboxIdConnectPostResponse::Status404_NotFound(
                            sandbox_not_found(id),
                        ));
                    }
                    Err(OrchestratorError::InvalidTimeout { timeout, .. }) => {
                        return Ok(SandboxesSandboxIdConnectPostResponse::Status400_BadRequest(
                            Self::error(400, format!("invalid timeout: {timeout:?}")),
                        ));
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
            SandboxState::Pausing | SandboxState::Paused => {}
        }

        // Arbitrate every resume path before touching the local sandbox.
        let (entry, claimed, connect_held) = match self.arbitrate_resume(sandbox_id).await {
            ResumeArbitration::Proceed(claimed) => (None, claimed, None),
            ResumeArbitration::Held(entry, claimed) => {
                let generation = entry.generation;
                (Some(entry), claimed, Some(generation))
            }
            ResumeArbitration::Blocked { origin_node_id } => {
                return Ok(SandboxesSandboxIdConnectPostResponse::Status400_BadRequest(
                    Self::error(400, format!("sandbox is held by node '{origin_node_id}'")),
                ));
            }
            ResumeArbitration::NotReady { origin_node_id } => {
                return Ok(SandboxesSandboxIdConnectPostResponse::Status400_BadRequest(
                    Self::error(
                        400,
                        format!(
                            "sandbox snapshot is still being published by node '{origin_node_id}'"
                        ),
                    ),
                ));
            }
            // Unavailability is not absence; 404 would permit a destructive rebuild.
            ResumeArbitration::Unavailable { reason } => {
                return Ok(
                    SandboxesSandboxIdConnectPostResponse::Status500_ServerError(Self::error(
                        500,
                        format!("cannot determine whether the sandbox is live elsewhere: {reason}"),
                    )),
                );
            }
        };

        // try to resume the sandbox
        match self
            .orchestrator()
            .resume_sandbox(
                sandbox_id,
                NewTimeout::Set(Duration::from_secs(body.timeout as u64)),
                claimed,
            )
            .await
        {
            Ok(resumed_metadata) => {
                // Confirm a held claim after the resume chooses its actual node.
                if connect_held.is_some() {
                    // Record the node where the resume actually landed.
                    let holding_node_id = self
                        .orchestrator()
                        .sandbox_holding_node_id(&sandbox_id)
                        .await;
                    self.paused
                        .mark_sandbox_running(
                            sandbox_id,
                            resumed_metadata.execution_id,
                            resumed_metadata.expires_at,
                            holding_node_id,
                        )
                        .await;
                }

                let routing = RoutingHeaders::of(&resumed_metadata);

                return Ok(
                SandboxesSandboxIdConnectPostResponse::Status201_TheSandboxWasResumedSuccessfully {
                    body: self.sandbox_model(resumed_metadata),
                    x_agentenv_sandbox_id: Some(routing.sandbox_id),
                    x_agentenv_execution_id: Some(routing.execution_id),
                    x_agentenv_projection_ttl_secs: Some(routing.projection_ttl_secs),
                },
            );
            }
            Err(OrchestratorError::SandboxNotFound(id)) => {
                if let Some(generation) = connect_held {
                    self.abandon_claim(sandbox_id, generation).await;
                }
                return Ok(SandboxesSandboxIdConnectPostResponse::Status404_NotFound(
                    sandbox_not_found(id),
                ));
            }
            Err(OrchestratorError::InvalidSandboxState { state, .. }) => {
                if let Some(generation) = connect_held {
                    self.abandon_claim(sandbox_id, generation).await;
                }
                return Ok(SandboxesSandboxIdConnectPostResponse::Status400_BadRequest(
                    Self::error(
                        400,
                        format!("sandbox cannot be resumed from {} state", state),
                    ),
                ));
            }
            Err(err) => {
                // The origin cannot serve this reopen, and the row names a snapshot.
                if let Some(entry) = entry.filter(|_| err.paused_resume_warrants_rebuild()) {
                    let rebuilt = self
                        .rebuild_instead_of_reopening(
                            entry,
                            NewTimeout::Set(Duration::from_secs(body.timeout as u64)),
                        )
                        .await;

                    return Ok(match rebuilt {
                        CrossNodeResume::Restored(metadata) => {
                            let routing = RoutingHeaders::of(&metadata);

                            SandboxesSandboxIdConnectPostResponse::Status201_TheSandboxWasResumedSuccessfully {
                                body: self.sandbox_model(*metadata),
                                x_agentenv_sandbox_id: Some(routing.sandbox_id),
                                x_agentenv_execution_id: Some(routing.execution_id),
                                x_agentenv_projection_ttl_secs: Some(routing.projection_ttl_secs),
                            }
                        }
                        CrossNodeResume::NotFound => {
                            SandboxesSandboxIdConnectPostResponse::Status404_NotFound(
                                sandbox_not_found(sandbox_id.to_string()),
                            )
                        }
                        CrossNodeResume::Failed(reason) => {
                            SandboxesSandboxIdConnectPostResponse::Status500_ServerError(
                                Self::error(
                                    500,
                                    format!("failed to restore paused sandbox: {reason}"),
                                ),
                            )
                        }
                    });
                }

                if let Some(generation) = connect_held {
                    self.abandon_claim(sandbox_id, generation).await;
                }
                return Ok(
                    SandboxesSandboxIdConnectPostResponse::Status500_ServerError(err.into()),
                );
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
        // Discard a stale local record before deleting cluster-owned state.
        self.discard_if_superseded(sandbox_id).await;

        match self.orchestrator().delete_sandbox(sandbox_id).await {
            // The orchestrator removes its cluster row and snapshot.
            Ok(_) => {
                Ok(SandboxesSandboxIdDeleteResponse::Status204_TheSandboxWasKilledSuccessfully)
            }
            Err(OrchestratorError::SandboxNotFound(id)) => {
                // A published paused record may exist even without a local sandbox.
                self.forget_paused_sandbox(sandbox_id).await;

                Ok(SandboxesSandboxIdDeleteResponse::Status404_NotFound(
                    sandbox_not_found(id),
                ))
            }
            Err(err) => Ok(SandboxesSandboxIdDeleteResponse::Status500_ServerError(
                err.into(),
            )),
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
                return Ok(SandboxesSandboxIdGetResponse::Status404_NotFound(
                    sandbox_not_found(sandbox_id),
                ));
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
            // The orchestrator publishes every pause path.
            Ok(_) => Ok(
                SandboxesSandboxIdPausePostResponse::Status204_TheSandboxWasPausedSuccessfullyAndCanBeResumed,
            ),
            Err(OrchestratorError::SandboxNotFound(id)) => Ok(
                SandboxesSandboxIdPausePostResponse::Status404_NotFound(sandbox_not_found(id)),
            ),
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
                    crate::orchestrator::capture_publish_metadata(&capture.metadata, alias.clone()),
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

        // Drop a superseded local record before attempting resume.
        self.discard_if_superseded(sandbox_id).await;

        // Arbitrate again to close the race with a concurrent resume.
        let arbitration = self.arbitrate_resume(sandbox_id).await;
        let held = match &arbitration {
            ResumeArbitration::Blocked { origin_node_id } => {
                return Ok(SandboxesSandboxIdResumePostResponse::Status409_Conflict(
                    Self::error(409, format!("sandbox is held by node '{origin_node_id}'")),
                ));
            }
            ResumeArbitration::NotReady { origin_node_id } => {
                return Ok(SandboxesSandboxIdResumePostResponse::Status409_Conflict(
                    Self::error(
                        409,
                        format!(
                            "sandbox snapshot is still being published by node '{origin_node_id}'"
                        ),
                    ),
                ));
            }
            // Unavailability is not absence; 404 would permit a destructive rebuild.
            // OpenAPI has no 503 response, so this remains the consumer-equivalent 500.
            ResumeArbitration::Unavailable { reason } => {
                return Ok(SandboxesSandboxIdResumePostResponse::Status500_ServerError(
                    Self::error(
                        500,
                        format!("cannot determine whether the sandbox is live elsewhere: {reason}"),
                    ),
                ));
            }
            ResumeArbitration::Held(entry, _) => Some(entry.generation),
            ResumeArbitration::Proceed(_) => None,
        };

        // Separate the optional rebuild row from the claim consumed by resume.
        let (entry, claimed) = match arbitration {
            ResumeArbitration::Held(entry, claimed) => (Some(entry), claimed),
            ResumeArbitration::Proceed(claimed) => (None, claimed),
            _ => unreachable!("refusals return before this point"),
        };

        let timer = SandboxStageTimer::new("resume");
        match timer
            .time(
                "resume",
                self.orchestrator()
                    .resume_sandbox(sandbox_id, NewTimeout::Set(timeout), claimed),
            )
            .await
        {
            Ok(metadata) => {
                // Confirm a held claim after the resume chooses its actual node.
                if held.is_some() {
                    // Record the node where the resume actually landed.
                    let holding_node_id = self
                        .orchestrator()
                        .sandbox_holding_node_id(&sandbox_id)
                        .await;
                    self.paused
                        .mark_sandbox_running(
                            sandbox_id,
                            metadata.execution_id,
                            metadata.expires_at,
                            holding_node_id,
                        )
                        .await;
                }

                let routing = RoutingHeaders::of(&metadata);

                return Ok(
                    SandboxesSandboxIdResumePostResponse::Status201_TheSandboxWasResumedSuccessfully {
                        body: self.sandbox_model(metadata),
                        x_agentenv_sandbox_id: Some(routing.sandbox_id),
                        x_agentenv_execution_id: Some(routing.execution_id),
                        x_agentenv_projection_ttl_secs: Some(routing.projection_ttl_secs),
                    },
                );
            }
            Err(OrchestratorError::SandboxNotFound(id)) => {
                // A claimed catalog row can rebuild a missing local sandbox.
                let Some(entry) = entry else {
                    // A concurrent resume loser can arrive here while the winner starts the sandbox.
                    // Confirm cluster absence before returning 404, which permits rebuild.
                    return Ok(
                        match self
                            .resolve_missing_local_resume(sandbox_id, NewTimeout::Set(timeout))
                            .await
                        {
                            MissingLocalResume::Unknown => {
                                SandboxesSandboxIdResumePostResponse::Status404_NotFound(
                                    sandbox_not_found(id),
                                )
                            }
                            MissingLocalResume::Resumed(metadata) => {
                                let routing = RoutingHeaders::of(&metadata);

                                SandboxesSandboxIdResumePostResponse::Status201_TheSandboxWasResumedSuccessfully {
                                body: self.sandbox_model(*metadata),
                                x_agentenv_sandbox_id: Some(routing.sandbox_id),
                                x_agentenv_execution_id: Some(routing.execution_id),
                                x_agentenv_projection_ttl_secs: Some(routing.projection_ttl_secs),
                            }
                            }
                            MissingLocalResume::Busy { holder } => {
                                SandboxesSandboxIdResumePostResponse::Status409_Conflict(
                                    Self::error(
                                        409,
                                        format!("sandbox is being resumed by node '{holder}'"),
                                    ),
                                )
                            }
                            MissingLocalResume::Undecided(reason) => {
                                SandboxesSandboxIdResumePostResponse::Status500_ServerError(
                                    Self::error(
                                        500,
                                        format!(
                                    "cannot determine whether the sandbox still exists: {reason}"
                                ),
                                    ),
                                )
                            }
                            MissingLocalResume::Failed(reason) => {
                                SandboxesSandboxIdResumePostResponse::Status500_ServerError(
                                    Self::error(
                                        500,
                                        format!("failed to restore paused sandbox: {reason}"),
                                    ),
                                )
                            }
                        },
                    );
                };

                let rebuilt = self
                    .restore_claimed_sandbox(*entry, NewTimeout::Set(timeout))
                    .await;

                return Ok(self.rebuilt_resume_response(rebuilt, sandbox_id));
            }
            Err(OrchestratorError::InvalidSandboxState { state, .. }) => {
                if let Some(generation) = held {
                    self.abandon_claim(sandbox_id, generation).await;
                }

                return Ok(SandboxesSandboxIdResumePostResponse::Status409_Conflict(
                    Self::error(
                        409,
                        format!("sandbox cannot be resumed from {} state", state),
                    ),
                ));
            }
            Err(err) => {
                // The origin cannot serve this reopen, and the row names a snapshot.
                if let Some(entry) = entry.filter(|_| err.paused_resume_warrants_rebuild()) {
                    let rebuilt = self
                        .rebuild_instead_of_reopening(entry, NewTimeout::Set(timeout))
                        .await;

                    return Ok(self.rebuilt_resume_response(rebuilt, sandbox_id));
                }

                if let Some(generation) = held {
                    self.abandon_claim(sandbox_id, generation).await;
                }

                return Ok(SandboxesSandboxIdResumePostResponse::Status500_ServerError(
                    err.into(),
                ));
            }
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
        let states = if query_params.state.len() == 1 {
            Some(vec![match query_params.state[0] {
                models::SandboxState::Running => SandboxState::Running,
                models::SandboxState::Paused => SandboxState::Paused,
            }])
        } else {
            // Only two states are supported. If multiple states are provided,
            // treat it as no state filter (i.e. return all sandboxes regardless of state)
            None
        };

        let filter = SandboxListFilter {
            states,
            excluded_states: None,
            user_metadata: parse_metadata_filter(&query_params.metadata),
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

        let list = match self.orchestrator.list_sandboxes_filtered(filter).await {
            Ok(list) => list,
            Err(err) => {
                return Ok(V2SandboxesGetResponse::Status500_ServerError(err.into()));
            }
        };

        let page = cursor.paginate(
            list,
            query_params.limit,
            |a, b| PaginationCursor::compare_desc(a.created_at, &a.id, b.created_at, &b.id),
            |sandbox, cursor| {
                PaginationCursor::compare_desc(
                    sandbox.created_at,
                    &sandbox.id,
                    cursor.time(),
                    cursor.value(),
                )
            },
            |sandbox| PaginationCursor::new(sandbox.created_at, sandbox.id),
        );

        let out = page
            .items
            .into_iter()
            .map(models::ListedSandbox::from)
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
    use crate::identity::NodeIdentity;
    use crate::orchestrator::{
        DisabledPausedSandboxRegistry, FileBackedSandboxPersister, InMemoryMetadataStore,
        Orchestrator,
    };
    use crate::sandbox::mock::MockBackendFactory;
    use crate::snapshot::mock::unresolvable_snapshot_manager;
    use crate::snapshot::{CommittedSnapshot, SnapshotRecord};

    async fn surface(row: SnapshotRecord) -> Arc<ApiImpl> {
        let root = tempfile::tempdir().expect("a temp dir");

        let orchestrator = Orchestrator::new(
            crate::sandbox::AccessTokenSeedPolicy::MayGenerate,
            InMemoryMetadataStore::new(),
            MockBackendFactory::new(),
            FileBackedSandboxPersister::new_for_test(root.path().join("paused")),
            crate::image::DisabledRuntimeImageRefs::shared(),
        )
        .await
        .expect("an orchestrator");

        let snapshot_manager = Arc::new(unresolvable_snapshot_manager(row));

        let api = Arc::new(ApiImpl::new(
            orchestrator,
            Arc::clone(&snapshot_manager),
            None,
            crate::api::PausedSandboxWiring::new(
                Arc::new(DisabledPausedSandboxRegistry),
                Arc::clone(&snapshot_manager),
                &NodeIdentity::from_config(&Default::default()),
            ),
            Vec::new(),
            crate::api::ResumeWiring::api_half_for_test(),
        ));

        std::mem::forget(root);

        api
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
