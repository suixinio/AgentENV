use agentenv_http_server::apis::sandboxes::*;
use agentenv_http_server::models;

use crate::observability::prometheus::SandboxStageTimer;
use crate::orchestrator::OrchestratorError;
use crate::types::SandboxId;

use super::{sandbox_not_found, ApiImpl};

impl ApiImpl {
    pub(super) async fn pause_post(
        &self,
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
}
