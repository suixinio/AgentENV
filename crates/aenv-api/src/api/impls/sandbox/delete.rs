use agentenv_http_server::apis::sandboxes::*;
use agentenv_http_server::models;

use crate::orchestrator::OrchestratorError;
use crate::types::SandboxId;

use super::{sandbox_not_found, ApiImpl};

impl ApiImpl {
    pub(super) async fn delete_one(
        &self,
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
        let forgotten = match self.forget_paused_snapshots(sandbox_id).await {
            Ok(deleted) => deleted > 0,
            Err(err) => {
                return Ok(SandboxesSandboxIdDeleteResponse::Status500_ServerError(
                    Self::snapshot_manager_error(&err),
                ))
            }
        };
        if killed || forgotten {
            Ok(SandboxesSandboxIdDeleteResponse::Status204_TheSandboxWasKilledSuccessfully)
        } else {
            Ok(SandboxesSandboxIdDeleteResponse::Status404_NotFound(
                sandbox_not_found(sandbox_id),
            ))
        }
    }
}
