use std::time::Duration;

use agentenv_http_server::apis::sandboxes::*;
use agentenv_http_server::models;

use crate::orchestrator::{
    NewTimeout, OrchestratorError, RestoredSandbox, SandboxState, StoreError,
};
use crate::types::SandboxId;

use super::{sandbox_not_found, ApiImpl, RoutingHeaders};

impl ApiImpl {
    pub(super) async fn connect_post(
        &self,
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
        // `None` falls through to the cold path: either there is no record, or
        // the record named a runtime nothing routes to and was dropped.
        let answered = match self.record_after_any_pause(sandbox_id).await {
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
                        Ok(_) => {
                            // Include the incarnation required for the gateway's projection write.
                            let routing = RoutingHeaders::of(&metadata);
                            Some(
                                SandboxesSandboxIdConnectPostResponse::Status200_TheSandboxWasAlreadyRunning {
                                    body: self.sandbox_model(metadata),
                                    x_agentenv_sandbox_id: Some(routing.sandbox_id),
                                    x_agentenv_execution_id: Some(routing.execution_id),
                                    x_agentenv_projection_ttl_secs: Some(routing.projection_ttl_secs),
                                },
                            )
                        }
                        Err(OrchestratorError::SandboxNotFound(_)) => None,
                        Err(OrchestratorError::InvalidTimeout { timeout, .. }) => {
                            Some(SandboxesSandboxIdConnectPostResponse::Status400_BadRequest(
                                Self::error(400, format!("invalid timeout: {timeout:?}")),
                            ))
                        }
                        Err(err) => Some(
                            SandboxesSandboxIdConnectPostResponse::Status500_ServerError(
                                err.into(),
                            ),
                        ),
                    }
                }
                SandboxState::Killing => {
                    Some(SandboxesSandboxIdConnectPostResponse::Status404_NotFound(
                        sandbox_not_found(sandbox_id),
                    ))
                }
                SandboxState::Pausing => {
                    Some(SandboxesSandboxIdConnectPostResponse::Status400_BadRequest(
                        Self::error(400, "sandbox is pausing; connect again once it has paused"),
                    ))
                }
            },
            Ok(None) => None,
            Err(OrchestratorError::InvalidSandboxState { state, .. }) => Some(
                SandboxesSandboxIdConnectPostResponse::Status400_BadRequest(Self::error(
                    400,
                    format!("sandbox is still {state} after waiting; connect again shortly"),
                )),
            ),
            Err(err) => {
                Some(SandboxesSandboxIdConnectPostResponse::Status500_ServerError(err.into()))
            }
        };
        if let Some(response) = answered {
            return Ok(response);
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
            .restore_or_join_launch(sandbox_id, request)
            .await
        {
            // A caller that waited out somebody else's launch is answered the
            // way an already-running sandbox is, because that is what it found.
            Ok(RestoredSandbox {
                metadata,
                joined: true,
            }) => {
                let routing = RoutingHeaders::of(&metadata);
                Ok(
                    SandboxesSandboxIdConnectPostResponse::Status200_TheSandboxWasAlreadyRunning {
                        body: self.sandbox_model(metadata),
                        x_agentenv_sandbox_id: Some(routing.sandbox_id),
                        x_agentenv_execution_id: Some(routing.execution_id),
                        x_agentenv_projection_ttl_secs: Some(routing.projection_ttl_secs),
                    },
                )
            }
            Ok(RestoredSandbox {
                metadata: resumed_metadata,
                joined: false,
            }) => {
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
}
