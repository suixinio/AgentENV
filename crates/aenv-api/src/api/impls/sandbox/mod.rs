mod connect;
mod create;
mod custom_params;
mod delete;
mod fork;
mod get;
mod list;
mod network;
mod pause;
mod resume;
mod snapshot;
mod timeout;

#[cfg(test)]
mod paused_surface_tests;

use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use axum_extra::extract::CookieJar;
use headers::Host;
use http::Method;

use tracing::warn;

use crate::orchestrator::{OrchestratorError, SandboxMetadata, SandboxState, StoreError};
use crate::sandbox::SandboxNetworkPolicy;
use crate::secrets::UnusableSecrets;
use crate::types::SandboxId;
use agentenv_http_server::apis::sandboxes::*;
use agentenv_http_server::models;
use agentenv_http_server::types::Nullable;

use super::ApiImpl;

pub(in crate::api) use aenv_core::api::wire::{
    allow_internet_access_from_base_policy, network_config_model,
};

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

/// How one endpoint words the refusals a wake can end in.
///
/// connect and resume refuse the same conditions: a pause that outlasted the
/// wait, a launch somebody else already started, a state no wake starts from.
/// They answer them with different statuses, and each names itself in the
/// retry it suggests, so both travel with the endpoint and neither with the
/// condition.
struct WakeRefusal {
    code: i32,
    /// The action the caller should repeat, as that endpoint calls it.
    retry: &'static str,
}

impl WakeRefusal {
    fn still_settling(&self, state: SandboxState) -> models::Error {
        ApiImpl::error(
            self.code,
            format!(
                "sandbox is still {state} after waiting; {} shortly",
                self.retry
            ),
        )
    }

    fn already_resuming(&self) -> models::Error {
        ApiImpl::error(self.code, "sandbox is already being resumed")
    }

    fn not_resumable_from(&self, state: SandboxState) -> models::Error {
        ApiImpl::error(
            self.code,
            format!("sandbox cannot be resumed from {state} state"),
        )
    }

    /// The refusal a restore ended in, when it ended in one this endpoint has
    /// words for. Anything else is the endpoint's server error, whatever
    /// status the orchestrator's own mapping would have given it.
    fn of_restore(&self, err: &OrchestratorError) -> Option<models::Error> {
        match err {
            OrchestratorError::StoreOperationFailed(StoreError::SandboxAlreadyExists {
                ..
            }) => Some(self.already_resuming()),
            OrchestratorError::InvalidSandboxState { state, .. } => {
                Some(self.not_resumable_from(*state))
            }
            _ => None,
        }
    }
}

fn duration_from_secs(secs: Option<u32>) -> Option<Duration> {
    secs.map(|s| Duration::from_secs(s as u64))
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
        self.cold_post(body).await
    }

    async fn sandboxes_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        query_params: &models::SandboxesGetQueryParams,
    ) -> Result<SandboxesGetResponse, ()> {
        self.list_get(query_params).await
    }

    async fn sandboxes_post(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        body: &models::NewSandbox,
    ) -> Result<SandboxesPostResponse, ()> {
        self.create_post(body).await
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
        self.connect_post(path_params, body).await
    }

    async fn sandboxes_sandbox_id_delete(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::SandboxesSandboxIdDeletePathParams,
    ) -> Result<SandboxesSandboxIdDeleteResponse, ()> {
        self.delete_one(path_params).await
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
        self.fork_post(path_params, body).await
    }

    async fn sandboxes_sandbox_id_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::SandboxesSandboxIdGetPathParams,
    ) -> Result<SandboxesSandboxIdGetResponse, ()> {
        self.get_one(path_params).await
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
        self.network_put(path_params, body).await
    }

    async fn sandboxes_sandbox_id_custom_extension_params_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::SandboxesSandboxIdCustomExtensionParamsGetPathParams,
    ) -> Result<SandboxesSandboxIdCustomExtensionParamsGetResponse, ()> {
        self.custom_params_get(path_params).await
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
        self.custom_params_patch(path_params, body).await
    }

    async fn sandboxes_sandbox_id_pause_post(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::SandboxesSandboxIdPausePostPathParams,
    ) -> Result<SandboxesSandboxIdPausePostResponse, ()> {
        self.pause_post(path_params).await
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
        self.snapshot_post(path_params, body).await
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
        self.refresh_post(path_params, body).await
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
        self.resume_post(path_params, body).await
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
        self.timeout_post(path_params, body).await
    }

    async fn v2_sandboxes_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        query_params: &models::V2SandboxesGetQueryParams,
    ) -> Result<V2SandboxesGetResponse, ()> {
        self.list_v2_get(query_params).await
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

    const SPEC: &str = include_str!("../../../../../../src/api/openapi.yml");

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
mod wake_refusal_tests {
    use super::*;
    use crate::types::SandboxId;

    #[test]
    fn each_endpoint_refuses_a_settling_pause_in_its_own_status_and_its_own_words() {
        let connect = connect::REFUSAL.still_settling(SandboxState::Pausing);
        assert_eq!(
            (connect.code, connect.message.as_str()),
            (
                400,
                "sandbox is still pausing after waiting; connect again shortly"
            )
        );

        let resume = resume::REFUSAL.still_settling(SandboxState::Pausing);
        assert_eq!(
            (resume.code, resume.message.as_str()),
            (
                409,
                "sandbox is still pausing after waiting; resume it again shortly"
            )
        );
    }

    #[test]
    fn a_restore_failure_neither_endpoint_words_stays_its_server_error() {
        assert!(
            resume::REFUSAL
                .of_restore(&OrchestratorError::ShuttingDown)
                .is_none(),
            "a refusal the endpoint has no words for must not be dressed as one it has"
        );

        let refused = OrchestratorError::InvalidSandboxState {
            sandbox_id: SandboxId::new(),
            state: SandboxState::Killing,
        };
        assert_eq!(
            connect::REFUSAL.of_restore(&refused).map(|err| err.code),
            Some(400)
        );
        assert_eq!(
            resume::REFUSAL.of_restore(&refused).map(|err| err.code),
            Some(409)
        );
    }
}
