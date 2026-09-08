use agentenv_http_server::apis::sandboxes::*;
use agentenv_http_server::models;

use crate::types::SandboxId;

use super::super::paused;
use super::{sandbox_not_found, ApiImpl};

impl ApiImpl {
    pub(super) async fn get_one(
        &self,
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
                    Ok(Some(record)) => match paused::paused_sandbox_detail(&record) {
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
}
