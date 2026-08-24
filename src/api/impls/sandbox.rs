use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use axum_extra::extract::CookieJar;
use headers::Host;
use http::Method;

use tracing::warn;

use crate::cfg::ConfigManager;
use crate::image::ResolvedBlockImage;
use crate::observability::prometheus::SandboxStageTimer;
use crate::orchestrator::{
    CreateSandboxRequest, ForkChildren, NewTimeout, OrchestratorError, SandboxExpiry,
    SandboxLaunchSource, SandboxListFilter, SandboxMetadata, SandboxState, SandboxTimeoutAction,
};
use crate::sandbox::CustomExtensionParams;
use crate::sandbox::{BaseSandboxNetworkPolicy, SandboxNetworkEgressPolicy, SandboxNetworkPolicy};
use crate::snapshot::{CommandContext, SnapshotAlias};
use crate::types::{ImageConfigs, SandboxId, SandboxResources};
use agentenv_http_server::apis::sandboxes::*;
use agentenv_http_server::models;
use agentenv_http_server::types::Nullable;

use super::attached_drives::resolve_attached_drives;
use super::pagination::PaginationCursor;
use super::paused_recovery::{CrossNodeResume, MissingLocalResume, ResumeArbitration};
use super::ApiImpl;

fn sandbox_not_found(id: impl Into<String>) -> models::Error {
    ApiImpl::error(404, format!("sandbox {} not found", id.into()))
}

/// The routing values a control-plane 2xx hands back to the gateway so it can
/// write the sandbox's routing projection without asking the scheduler first.
///
/// 🔴 All three, not just the id. `x-agentenv-execution-id` used to be written
/// only by the data-plane proxy's echo, so control-plane responses never
/// carried one — which meant every projection write the gateway made was
/// unincarnated, and under `enforce` arbitration an unincarnated write against
/// an existing record is refused *silently*. On resume that is the whole point
/// of the write: resume mints a fresh incarnation, so without this header the
/// record keeps naming the run that ended.
struct RoutingHeaders {
    sandbox_id: String,
    execution_id: String,
    /// 🔴 0 means "use your own default TTL". It never means "do not expire".
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
            // 🔴 Easiest of the three to leave out, and the one whose absence is
            // hardest to see: the gateway's cluster listing is decoded from
            // this shape, so a missing value there is an empty field and no
            // error anywhere.
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

/// What a user-facing create means by the `timeout` it did or did not send.
///
/// 🔴 Never [`SandboxExpiry::NotKeptHere`]. A user is a client, not a second
/// orchestrator: it keeps no record of the sandbox, runs no expiry index and
/// evicts nothing, so "said nothing" can only mean the configured default. The
/// third answer belongs to the one caller that does keep all three — the API
/// half asking a node — and it is sent by `RemoteSandboxBackendFactory`, a
/// layer below this one.
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

/// Build source image configs from the resolved rootfs and attached drives.
fn build_image_configs(
    rootfs: &ResolvedBlockImage,
    attached: &[super::attached_drives::ResolvedAttachedDrive],
) -> ImageConfigs {
    let mut image_configs = ImageConfigs::new();
    if let Some(config) = &rootfs.raw_config {
        image_configs.add(None::<String>, "/", config.clone());
    }
    for r in attached {
        if let Some(config) = &r.raw_config {
            image_configs.add(
                Some(r.drive.drive_id()),
                r.drive.mount_path().display().to_string(),
                config.clone(),
            );
        }
    }
    image_configs
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
        // 🔴 First, ahead of the image resolver, because the resolver is what
        // was answering instead. A cold start's first act is to resolve
        // `body.image` on the machine serving this call: `regctl` fetches the
        // manifest, pulls the blobs and converts them into a local overlaybd
        // image, which is then handed to a local Firecracker VM. The `api` Pod
        // installs no `regctl` — by design, that is a node's tooling — so the
        // very first step failed with `regctl is required for OCI registry
        // access: {deps}/regctl/.../regctl` and returned, and the caller was
        // told a registry tool was missing rather than that this half cannot
        // cold-start at all.
        //
        // Worse, the refusal that *does* say it — the one in
        // `RemoteSandboxBackendFactory::build`, which is where a cold create
        // runs out of road because the build spec it is handed no longer
        // carries the user's image reference — sits behind that resolve and so
        // was structurally unreachable on `--role api`. This gate is what makes
        // the answer the role's rather than the toolchain's; that `bail!` stays
        // as the backstop for any caller that gets there another way.
        //
        // Nothing has been resolved, allocated or created at this point, so
        // there is nothing to roll back.
        if !self.role().runs_sandbox_runtime() {
            warn!(
                image = %body.image,
                role = self.role().as_str(),
                "refused a cold sandbox create: this role runs no sandbox runtime"
            );
            return Ok(SandboxesColdPostResponse::Status500_ServerError(
                Self::error(
                    // 🔴 500 because it is the only code this operation
                    // declares that means "this server, not your request"
                    // (`src/api/openapi.yml`: 201/400/401/500). The request is
                    // well-formed and would work verbatim against the other
                    // half, so 400 would blame the caller for the deployment's
                    // shape; 501 and 503 say it better and neither is in the
                    // schema. The cost is that this refusal and a genuine
                    // resolve failure share a status code — which is why the
                    // test below discriminates on whether the image resolver
                    // ran, not on the code.
                    500,
                    "cold sandbox creation is not available on --role api: a cold start resolves \
                     the OCI image on the machine that serves this call — regctl pulls and \
                     converts the layers into a local overlaybd image, which is then handed to a \
                     local Firecracker VM — and this process has no regctl, no /dev/kvm and no \
                     ublk. Create the sandbox from a template or snapshot with POST /sandboxes, \
                     which this half does place on a node; for a cold start, run this call \
                     against a server started with --role all. A --role node server answers this \
                     route with 404, so there is no node for this one to forward it to. Nothing \
                     was created.",
                ),
            ));
        }

        let image_resolver = self.image_resolver();
        let timer = SandboxStageTimer::new("create_cold");
        // TODO: Move cold-start image resolution into an async create operation
        // once the API supports 202 Accepted + status polling.
        let resolved_rootfs = match timer
            .time("resolve_rootfs", image_resolver.resolve(&body.image))
            .await
        {
            Ok(resolved) => resolved,
            Err(err) if err.is_user_error() => {
                return Ok(SandboxesColdPostResponse::Status400_BadRequest(
                    Self::error(400, err.to_string()),
                ));
            }
            Err(err) => {
                warn!(error = %format_args!("{err:#}"), image = %body.image, "failed to resolve sandbox rootfs image");
                return Ok(SandboxesColdPostResponse::Status500_ServerError(
                    Self::error(
                        500,
                        format!("resolve sandbox rootfs image '{}': {err:#}", body.image),
                    ),
                ));
            }
        };
        let resources = match cold_start_resources(body) {
            Ok(resources) => resources,
            Err(err) => return Ok(SandboxesColdPostResponse::Status400_BadRequest(err)),
        };
        let resolved_attached = match timer
            .time(
                "resolve_attached_drives",
                resolve_attached_drives(
                    body.attached_drives.as_deref().unwrap_or_default(),
                    image_resolver.as_ref(),
                ),
            )
            .await
        {
            Ok(resolved) => resolved,
            Err(err) => {
                warn!(error = %err.message, "failed to resolve attached drives");
                return Ok(Self::client_or_server_response(
                    err,
                    SandboxesColdPostResponse::Status400_BadRequest,
                    SandboxesColdPostResponse::Status500_ServerError,
                ));
            }
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

        let image_configs = build_image_configs(&resolved_rootfs, &resolved_attached);
        let extra_drives = resolved_attached.into_iter().map(|r| r.drive).collect();

        let base = resolved_rootfs.base_context;
        let request = CreateSandboxRequest {
            source: SandboxLaunchSource::Image {
                image_ref: resolved_rootfs.image_ref,
                overlaybd_config_path: resolved_rootfs.overlaybd_config_path,
                context: Box::new(
                    CommandContext::from_env_and_workdir(base.env_vars, base.workdir)
                        .with_user(base.user)
                        .with_exposed_ports(base.exposed_ports)
                        .with_entrypoint(base.entrypoint)
                        .with_cmd(base.cmd)
                        .with_volumes(base.volumes)
                        .with_labels(base.labels),
                ),
                resources: Some(resources),
                extra_drives,
                extra_boot_args: body.extra_boot_args.clone(),
                image_configs: Box::new(image_configs),
            },
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
            // 🔴 Never set from the user-facing REST surface: what marks a
            // sandbox as the control plane's is the node gRPC create, and
            // nothing else.
            control_plane_config: None,
            // A user asking for a sandbox is not an orchestrator quoting an
            // incarnation it already recorded, so this one is minted.
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
                                .downcast_ref::<uvm_ublk_daemon::InvalidRequestError>()
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
        let snapshot = match timer
            .time(
                "load_snapshot",
                self.snapshot_manager.load_runnable(&body.template_id),
            )
            .await
        {
            Ok(Some(snapshot)) => snapshot,
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
            source: SandboxLaunchSource::Snapshot(Box::new(snapshot)),
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
            // 🔴 Never set from the user-facing REST surface: what marks a
            // sandbox as the control plane's is the node gRPC create, and
            // nothing else.
            control_plane_config: None,
            // A user asking for a sandbox is not an orchestrator quoting an
            // incarnation it already recorded, so this one is minted.
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
                // The sandbox did not move, but the gateway still records a
                // routing projection for this request, and a write without an
                // incarnation is the one arbitration refuses without saying so.
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

        // Ask the cluster who may resume this sandbox before resuming it. This
        // route used to go straight to the orchestrator, which made it a second
        // way to bring a sandbox up beside one that is already live elsewhere;
        // there is now one decision point and every resume path goes through
        // it.
        let (claimed, connect_held) = match self.arbitrate_resume(sandbox_id).await {
            ResumeArbitration::Proceed(claimed) => (claimed, None),
            ResumeArbitration::Held(entry, claimed) => (claimed, Some(entry.generation)),
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
            // 🔴 Not a 404, for the reason spelled out on the resume route: a
            // 404 here reads downstream as "the sandbox is gone, rebuild it".
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
                // Same as the resume route: the orchestrator repoints the row
                // for a resume it performed, but not for one that found the
                // sandbox already running, and a claim nobody confirms sits in
                // `resuming` until its lease lapses.
                if connect_held.is_some() {
                    self.paused
                        .mark_sandbox_running(
                            sandbox_id,
                            resumed_metadata.execution_id,
                            resumed_metadata.expires_at,
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
        // A leftover paused record here is not the sandbox — it may well be
        // running on another node. Drop it first so the delete below acts on
        // something this node actually owns.
        self.discard_if_superseded(sandbox_id).await;

        match self.orchestrator().delete_sandbox(sandbox_id).await {
            // The orchestrator drops the cluster record and its snapshot as
            // part of the delete.
            Ok(_) => {
                Ok(SandboxesSandboxIdDeleteResponse::Status204_TheSandboxWasKilledSuccessfully)
            }
            Err(OrchestratorError::SandboxNotFound(id)) => {
                // The local node does not have it, but the cluster may still
                // hold a paused record — deleting a sandbox that lives only as
                // a published snapshot must still remove it.
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
                    // 🔴 `Fresh`: a fork over the user-facing REST surface
                    // produces sandboxes the control plane did not ask for and
                    // does not own, so no child carries a marker. Inheriting
                    // the source's is what would put a child into the control
                    // plane's listing under its parent's identity.
                    .fork_sandbox(sandbox_id, ForkChildren::Fresh(count), new_timeout),
            )
            .await
        {
            Ok(outcomes) => {
                let results = outcomes
                    .into_iter()
                    .map(|outcome| match outcome {
                        Ok(metadata) => {
                            // 🔴 On the body, not on a header: one response,
                            // N children, N incarnations. The child's own
                            // incarnation is already inside `sandbox`.
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
            // The orchestrator publishes and registers the pause itself, so
            // every pause path gets it — this one, expiry, and shutdown alike.
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
                // 🔴 The id inside this value is a proposal, not a decision.
                // When the sandbox runs on another node the capture arrives
                // already staged — under the id *that* node wrote the bytes
                // into — and only the alias here survives. Which is why the
                // response below is built from `published`, never from this.
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
            // 🔴 Matched explicitly. The catch-all below hands the error to
            // `From<OrchestratorError>`, which builds a body whose `code` says
            // 400 and then ships it inside an HTTP 500 — a mismatch a client
            // cannot act on.
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

        // If the cluster has moved past this sandbox — another node resumed it
        // while this one was away — drop the leftover local record first, so the
        // resume below cannot start a second copy alongside the live one.
        self.discard_if_superseded(sandbox_id).await;

        // Then ask the cluster who may resume it. Discarding above closes the
        // case where this node is behind by a whole reconciliation; this closes
        // the one where it is behind by a moment, which is the case that
        // actually produces two live copies of one sandbox. Both resume paths
        // below run under this single decision.
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
            // 🔴 Not a 404. The downstream contract for a 404 on this route is
            // "the sandbox is gone, rebuild it", which resets the user's
            // workspace to its template — and what actually happened is that
            // nobody could be asked. This says "ask again", which is the only
            // honest answer and the only retryable one.
            //
            // 🔴 And 500 rather than the 503 this obviously is, on purpose.
            // `src/api/openapi.yml` has no 503 anywhere, so introducing one
            // means regenerating the whole server crate for a code that buys
            // nothing: the only consumer of this route maps both onto the same
            // "the sandbox's state is unknown" branch, so 500 and 503 are
            // literally indistinguishable to it. The wording below is what
            // carries the meaning, and it is deliberately unique to this
            // branch so an operator grepping for it lands here and nowhere
            // else. Revisit if a 503 ever exists in the spec for its own
            // reasons — not for this.
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

        // Split the granting answers into the row (which the rebuild below
        // still needs) and the claim token (which the resume consumes). The
        // rebuild path does not need a token: it is a create, and a create mints
        // its own incarnation.
        let (entry, claimed) = match arbitration {
            ResumeArbitration::Held(entry, claimed) => (Some(entry), claimed),
            ResumeArbitration::Proceed(claimed) => (None, claimed),
            // Every refusing variant returned above.
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
            // Resumed from local artifacts.
            Ok(metadata) => {
                // The orchestrator repoints the cluster record as part of a
                // resume it actually performed, but not on the already-running
                // path, which returns without touching the registry. Saying it
                // again here is idempotent and keeps a claim from sitting in
                // `resuming` until its lease lapses.
                if held.is_some() {
                    self.paused
                        .mark_sandbox_running(
                            sandbox_id,
                            metadata.execution_id,
                            metadata.expires_at,
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
                // Nothing local to resume. If the claim above came with a row,
                // the cluster still has a snapshot to rebuild it from — under
                // the same ID, on this node.
                let Some(entry) = entry else {
                    // 既没有本地副本、也没拿到认领权。单发请求下这确实是"沙箱没了"，
                    // 但并发下不是 —— 输家会走到这里，而赢家正把同一台拉起来。
                    // 🔴 回 404 之前必须先问清集群：404 的下游契约是"可以重建"，
                    // 而重建等于把用户工作区退回模板初始态。
                    return Ok(match self.resolve_missing_local_resume(sandbox_id).await {
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
                            SandboxesSandboxIdResumePostResponse::Status409_Conflict(Self::error(
                                409,
                                format!("sandbox is being resumed by node '{holder}'"),
                            ))
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
                    });
                };

                return Ok(
                    match self
                        .restore_claimed_sandbox(*entry, NewTimeout::Set(timeout))
                        .await
                    {
                        CrossNodeResume::Restored(metadata) => {
                            let routing = RoutingHeaders::of(&metadata);

                            SandboxesSandboxIdResumePostResponse::Status201_TheSandboxWasResumedSuccessfully {
                            body: self.sandbox_model(*metadata),
                            x_agentenv_sandbox_id: Some(routing.sandbox_id),
                            x_agentenv_execution_id: Some(routing.execution_id),
                            x_agentenv_projection_ttl_secs: Some(routing.projection_ttl_secs),
                        }
                        }
                        CrossNodeResume::NotFound => {
                            SandboxesSandboxIdResumePostResponse::Status404_NotFound(
                                sandbox_not_found(id),
                            )
                        }
                        CrossNodeResume::Failed(reason) => {
                            SandboxesSandboxIdResumePostResponse::Status500_ServerError(
                                Self::error(
                                    500,
                                    format!("failed to restore paused sandbox: {reason}"),
                                ),
                            )
                        }
                    },
                );
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
            // Explicit for the same reason as on the refresh route above.
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

    /// 🔴 A user who sends no `timeout` gets this orchestrator's default, and
    /// never "keep no deadline".
    ///
    /// The same absence means two different things depending on who is asking,
    /// and this is the half where it means the default. The other half — the
    /// API process asking a *node* — is `RemoteSandboxBackendFactory`, which
    /// says `caller_kept` on the wire; the two must not converge, because the
    /// sandbox would then be one nobody ever expires.
    ///
    /// The control face is the same call with a number in it: an implementation
    /// that returned `AfterConfiguredDefault` for everything would satisfy the
    /// first assertion on its own.
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
    fn build_image_configs_preserves_rootfs_and_attached_drives() {
        let rootfs_config = serde_json::json!({
            "Cmd": ["/bin/bash"],
            "WorkingDir": "/workspace"
        });
        let rootfs = ResolvedBlockImage {
            image_ref: "ubuntu:24.04".to_string(),
            overlaybd_config_path: "/tmp/rootfs-image.json".into(),
            base_context: crate::image::ImageBaseContext::default(),
            raw_config: Some(rootfs_config.clone()),
        };
        let drive_config = serde_json::json!({
            "Env": ["DATA=1"]
        });
        let attached = super::super::attached_drives::ResolvedAttachedDrive {
            drive: crate::sandbox::ExtraDrive::try_new_overlaybd_with_mount_path(
                "data",
                "/tmp/data-image.json",
                true,
                "/data",
                None::<std::path::PathBuf>,
            )
            .expect("valid test drive"),
            raw_config: Some(drive_config.clone()),
        };

        let image_configs = build_image_configs(&rootfs, &[attached]);

        assert_eq!(image_configs.len(), 2);
        let entries = image_configs.entries();
        assert_eq!(entries[0].drive_id, None);
        assert_eq!(entries[0].mount_path, "/");
        assert_eq!(entries[0].config, rootfs_config);
        assert_eq!(entries[1].drive_id.as_deref(), Some("data"));
        assert_eq!(entries[1].mount_path, "/data");
        assert_eq!(entries[1].config, drive_config);
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

    /// The four operations whose success responses the gateway turns into a
    /// routing projection write. All four are POSTs; the GET that shares two of
    /// these paths is not one of them.
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

    /// The lines of one block, from the line after `header` up to the next line
    /// indented no further than `header` was.
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
            // Running since 1970, so its budget really is long gone —
            // `created_at` alone would not say that any more, because paused
            // time does not count against the ceiling.
            running_since: Some(created_at),
            ..Default::default()
        };

        let routing = RoutingHeaders::of(&metadata);

        assert_eq!(routing.sandbox_id, metadata.id.to_string());
        assert_eq!(routing.execution_id, metadata.execution_id.to_string());
        // A sandbox that has been running since the epoch is long past its
        // ceiling. What comes out is still positive: the receiver has to be
        // able to read every value here as a duration, and 0 is spoken for.
        assert!(
            routing.projection_ttl_secs > 0,
            "a sandbox past its ceiling still asks for a short record, not an immortal one"
        );
    }

    /// 🔴 The control face. Without a ceiling the node has nothing to derive a
    /// TTL from and says so with 0 — which the receiver reads as "use your own
    /// default", never as "this record should not expire".
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

        // At least the hour the sandbox has left. A projection that expires
        // before the sandbox it points at is the failure this value exists to
        // prevent, and it is invisible from the outside: the route simply
        // misses, exactly as a cold cache would.
        assert!(
            ttl >= 3_600,
            "expected at least the remaining hour, got {ttl}"
        );
    }

    /// 🔴 Pinned against the spec rather than against the handlers, so it also
    /// holds for a route added later. Dropping a header here does not break the
    /// build anywhere a reader would look: it regenerates a response variant
    /// with one fewer field, and what fails downstream is a projection write
    /// that incarnation arbitration refuses without saying so.
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
            // Only the POST on these paths creates or resumes a sandbox; the
            // listing GET that shares one of them does not.
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
        // Guards the parser itself: an expression that silently matches
        // nothing would otherwise pass this test forever.
        assert_eq!(
            checked, 5,
            "expected create, cold, resume, and both connect outcomes"
        );
    }

    /// The other half of the carrier. Fork answers with a top-level array, so
    /// its TTL rides in the body — and on the infrastructure wrapper rather
    /// than on the user-facing model.
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

    /// T-A6-1. Every shape that names a sandbox names the run it is on.
    ///
    /// 🔴 All three, from one record, compared against each other. `ListedSandbox`
    /// is the one that gets forgotten, and the symptom of forgetting it is not an
    /// error: the gateway decodes the cluster listing into its own struct, an
    /// absent field decodes as empty, and the field is simply blank forever.
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

    /// T-A6-2. 🔴 Read-only, and that is a line in the contract rather than an
    /// oversight.
    ///
    /// A client that could name the incarnation it wants could name one that has
    /// already been replaced, and every fencing rule downstream is built on the
    /// assumption that the value came from the control plane. Pinned against the
    /// spec, so it holds for shapes nobody has written a handler for yet.
    #[test]
    fn the_execution_is_never_an_input() {
        let spec = include_str!("../openapi.yml");
        let mut schema = None;
        let mut offenders = Vec::new();

        for line in spec.lines() {
            let indent = line.len() - line.trim_start().len();
            let trimmed = line.trim_end();
            // Schema names sit at six spaces of indent under `schemas:`.
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

/// 🔴 What `--role api` answers when it is asked to cold-start a sandbox.
///
/// A cold start is the one create path whose first act is to resolve an OCI
/// image *here*: `regctl` fetches the manifest and converts the layers into a
/// local overlaybd image for a local Firecracker VM. The `api` Pod installs no
/// `regctl` — that is a node's tooling — so the call died on step one with
/// `regctl is required for OCI registry access`, and the refusal that actually
/// names the problem (`RemoteSandboxBackendFactory::build`) sat behind that
/// resolve and was never reached. The operator was told a tool was missing;
/// the truth was that this half cannot cold-start at all.
#[cfg(all(test, unix))]
mod cold_start_role_tests {
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use axum_extra::extract::CookieJar;
    use headers::Host;
    use http::Method;

    use agentenv_http_server::apis::sandboxes::*;
    use agentenv_http_server::models;

    use super::ApiImpl;
    use crate::cfg::AppConfig;
    use crate::identity::NodeIdentity;
    use crate::image::ImageResolver;
    use crate::orchestrator::{
        DisabledPausedSandboxRegistry, FileBackedSandboxPersister, InMemoryMetadataStore,
        Orchestrator,
    };
    use crate::role::ServerRole;
    use crate::sandbox::FirecrackerSandboxFactory;
    use crate::template::TemplateBuilder;

    /// The image every fixture here asks for.
    ///
    /// Fully qualified on purpose: an unqualified name is expanded across
    /// `image.resolver.search_registries` into one candidate per registry, and
    /// the fake `regctl` below would then be run once per candidate. One
    /// candidate makes "was the resolver reached" a single, unambiguous fact.
    /// `.invalid` is reserved by RFC 6761 and can never resolve, so a fixture
    /// that stopped installing the fake `regctl` would fail rather than reach
    /// a real registry.
    const IMAGE: &str = "registry.invalid/agentenv/cold-start:pinned";

    /// One API surface, plus the place the fake `regctl` leaves its evidence.
    struct Surface {
        api: Arc<ApiImpl>,
        /// `{deps_path}/regctl/{version}/`: the directory the fake `regctl` is
        /// symlinked into, and therefore the one it writes `argv` into.
        regctl_dir: PathBuf,
    }

    /// A surface differing from every other one here in exactly one value:
    /// which half of the split the process serving it runs as.
    ///
    /// 🔴 The orchestrator stays `All` in every case, for the reason
    /// `template_read_scope_tests::surface_as` gives: the cold-start route asks
    /// `ApiImpl::role()`, not the orchestrator's, and an `Orchestrator` built
    /// as `Api` has construction-time demands of its own that would make the
    /// refusing half of this test fail on the fixture instead of on the gate.
    async fn surface_as(role: ServerRole) -> Surface {
        let root = tempfile::tempdir().expect("a temp dir");

        // A fake `regctl` that answers every lookup with a registry 404. The
        // 404 matters twice over: `run_regctl` treats `[http 404]` as final and
        // skips its five-attempt retry budget, so the test neither sleeps nor
        // spawns more than once, and `ImageError::NotFound` is a *user* error,
        // so the road not refused ends in a 400 that could never be mistaken
        // for the 500 the refusal uses.
        //
        // 🔴 Symlinked from the repository rather than written out here: while
        // any thread in this process holds a write fd on an executable, every
        // concurrent `fork` inherits it and the following `execve` is refused
        // with `ETXTBSY`. Symlinking never opens the target for writing.
        let deps_path = root.path().join("deps");
        let regctl = crate::cfg::regctl_path(&deps_path);
        let regctl_dir = regctl
            .parent()
            .expect("the regctl path has a parent")
            .to_path_buf();
        std::fs::create_dir_all(&regctl_dir).expect("create the fake regctl's directory");
        std::fs::write(regctl_dir.join("stdout"), "").expect("write the stdout fixture");
        std::fs::write(
            regctl_dir.join("stderr"),
            format!("failed to get manifest {IMAGE}: request failed: not found [http 404]: {{}}\n"),
        )
        .expect("write the stderr fixture");
        std::fs::write(regctl_dir.join("exit_code"), "1\n").expect("write the exit code fixture");
        std::os::unix::fs::symlink(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/regctl-recorder.sh"),
            &regctl,
        )
        .expect("link the fake regctl");

        // The only field that matters: `resolved_regctl_binary()` is derived
        // from it, so this is what points the resolver at the fake.
        let config = AppConfig {
            deps_path,
            ..Default::default()
        };

        let orchestrator = Orchestrator::new(
            ServerRole::All,
            InMemoryMetadataStore::new(),
            FirecrackerSandboxFactory::new(),
            FileBackedSandboxPersister::new_for_test(root.path().join("paused")),
        )
        .await
        .expect("an orchestrator");

        let snapshot_manager = Arc::new(crate::snapshot::mock::mock_snapshot_manager());

        let api = Arc::new(ApiImpl::new(
            orchestrator,
            Arc::clone(&snapshot_manager),
            Arc::new(TemplateBuilder::new()),
            Arc::new(ImageResolver::new(&config)),
            None,
            crate::api::PausedSandboxWiring::new(
                Arc::new(DisabledPausedSandboxRegistry),
                Arc::clone(&snapshot_manager),
                &NodeIdentity::from_config(&Default::default()),
            ),
            Vec::new(),
            // 🔴 The one value this fixture varies.
            role,
            crate::api::ResumeWiring::node_local(NodeIdentity::from_config(&Default::default()).id),
        ));

        // Held for the process's lifetime: the persister above goes on reading
        // it, and the fake `regctl` is under it too.
        std::mem::forget(root);

        Surface { api, regctl_dir }
    }

    fn claims() -> super::super::Claims {
        super::super::Claims
    }

    fn host() -> Host {
        Host::from(http::uri::Authority::from_static("localhost"))
    }

    /// `POST /sandboxes-cold`, with the one body every fixture here sends.
    async fn cold_start(s: &Surface) -> SandboxesColdPostResponse {
        s.api
            .sandboxes_cold_post(
                &Method::POST,
                &host(),
                &CookieJar::new(),
                &claims(),
                &models::NewColdSandbox::new(IMAGE.to_string()),
            )
            .await
            .expect("the handler answers")
    }

    /// The message a role refusal carries, or `None` when the handler did not
    /// refuse on the role.
    ///
    /// 🔴 The status code cannot tell the two apart, and that is a fact about
    /// the API rather than about this helper: `/sandboxes-cold` declares
    /// 201/400/401/500 and nothing else, so the refusal reuses 500 — which is
    /// also what an unresolvable image or a failed create answers.
    fn role_refusal(response: &SandboxesColdPostResponse) -> Option<&str> {
        match response {
            SandboxesColdPostResponse::Status500_ServerError(error)
                if error.message.contains("--role api") =>
            {
                Some(error.message.as_str())
            }
            _ => None,
        }
    }

    /// The argv of the fake `regctl`'s last run, or `None` if it never ran.
    ///
    /// 🔴 This is the discriminator. "Did this call reach the image resolver"
    /// is the question the defect is about, and it is answerable only as a
    /// side effect: the resolver's first act is to shell out to `regctl`, and
    /// the fake records what it was asked for. A status code cannot answer it.
    fn regctl_argv(s: &Surface) -> Option<Vec<String>> {
        std::fs::read_to_string(s.regctl_dir.join("argv"))
            .ok()
            .map(|raw| raw.lines().map(ToString::to_string).collect())
    }

    /// 🔴 The same call on each half of the split, and the point is that the
    /// three do not answer alike.
    ///
    /// Both faces in one test, because either alone is satisfied by a constant:
    /// "api is refused" passes on a gate that is always true, "all and node are
    /// not" passes on one that is always false. And the discriminator is
    /// whether `regctl` ran rather than the status code, because the refusal
    /// and an unresolvable image are both answered by this route — only one of
    /// them got as far as asking a registry anything.
    ///
    /// `Node` is asserted alongside `All` even though a node never reaches this
    /// handler in production — `RoleGate` answers `POST /sandboxes-cold` with
    /// 404 there (`crate::api::role_gate`) — because the gate under test is
    /// `runs_sandbox_runtime`, and a node runs one. That 404 is also why the
    /// refusal tells the caller there is no node to forward to.
    #[tokio::test]
    async fn a_cold_start_is_refused_by_the_half_that_runs_no_sandbox_runtime_and_attempted_by_the_halves_that_do(
    ) {
        let s = surface_as(ServerRole::Api).await;
        let response = cold_start(&s).await;
        let refusal = role_refusal(&response).unwrap_or_else(|| {
            panic!(
                "--role api has no regctl and no /dev/kvm to cold-start on, and answering \
                 anything but a refusal here is what made the operator read a missing-tool error \
                 instead of a wrong-half one, got {response:?}"
            )
        });
        assert!(
            refusal.contains("--role all"),
            "a refusal that does not say where a cold start can be run is a dead end, got \
             {refusal:?}"
        );
        assert!(
            refusal.contains("POST /sandboxes"),
            "the create path this half *does* serve is the actionable half of the answer, got \
             {refusal:?}"
        );
        assert!(
            refusal.contains("404"),
            "a caller told only 'not here' will ask which node to send it to; a --role node \
             server answers this route with 404, so the answer is 'none', got {refusal:?}"
        );
        assert_eq!(
            regctl_argv(&s),
            None,
            "🔴 and it must refuse before the image resolver: resolution is the first thing this \
             route does, so a refusal placed after it is a refusal no --role api caller can ever \
             reach"
        );

        for role in [ServerRole::All, ServerRole::Node] {
            let s = surface_as(role).await;
            let response = cold_start(&s).await;
            assert!(
                role_refusal(&response).is_none(),
                "--role {} runs a sandbox runtime, so this create takes the road it always took, \
                 got {response:?}",
                role.as_str()
            );
            let argv = regctl_argv(&s).unwrap_or_else(|| {
                panic!(
                    "--role {} must still resolve the image itself, and the resolver's first act \
                     is to run regctl; it never ran, so this create was cut short somewhere it \
                     never used to be, got {response:?}",
                    role.as_str()
                )
            });
            assert!(
                argv.iter().any(|arg| arg == IMAGE),
                "the road being taken is the image resolver's, which is more than a status code \
                 that merely is not the refusal's, got argv {argv:?}"
            );
            assert!(
                matches!(response, SandboxesColdPostResponse::Status400_BadRequest(_)),
                "the fake registry answers 404, so this create ends where image resolution ends \
                 and the caller is told about the image rather than about the server, got \
                 {response:?}"
            );
        }
    }
}
