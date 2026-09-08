use std::time::Duration;

use agentenv_http_server::apis::sandboxes::*;
use agentenv_http_server::models;

use crate::orchestrator::OrchestratorError;
use crate::types::SandboxId;

use super::{sandbox_not_found, ApiImpl};

impl ApiImpl {
    pub(super) async fn refresh_post(
        &self,
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

    pub(super) async fn timeout_post(
        &self,
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
}
