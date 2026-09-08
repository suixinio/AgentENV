use std::sync::OnceLock;
use std::time::Duration;

use agentenv_http_server::apis::sandboxes::*;
use agentenv_http_server::models;

use crate::cfg::ConfigManager;
use crate::observability::prometheus::SandboxStageTimer;
use crate::orchestrator::{
    NewTimeout, OrchestratorError, RestoredSandbox, SandboxMetadata, SandboxState, StoreError,
};
use crate::types::SandboxId;

use super::{duration_from_secs, sandbox_not_found, ApiImpl, RoutingHeaders};

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

impl ApiImpl {
    fn resumed_response(&self, metadata: SandboxMetadata) -> SandboxesSandboxIdResumePostResponse {
        let routing = RoutingHeaders::of(&metadata);
        SandboxesSandboxIdResumePostResponse::Status201_TheSandboxWasResumedSuccessfully {
            body: self.sandbox_model(metadata),
            x_agentenv_sandbox_id: Some(routing.sandbox_id),
            x_agentenv_execution_id: Some(routing.execution_id),
            x_agentenv_projection_ttl_secs: Some(routing.projection_ttl_secs),
        }
    }

    pub(super) async fn resume_post(
        &self,
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
                self.orchestrator()
                    .restore_or_join_launch(sandbox_id, request),
            )
            .await
        {
            Ok(RestoredSandbox {
                metadata,
                joined: true,
            }) => Ok(self.resumed_response(metadata)),
            Ok(RestoredSandbox {
                metadata,
                joined: false,
            }) => {
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
}
