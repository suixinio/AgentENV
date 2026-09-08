use std::time::{Duration, SystemTime};

use agentenv_http_server::apis::sandboxes::*;
use agentenv_http_server::models;

use crate::observability::prometheus::SandboxStageTimer;
use crate::orchestrator::{ForkChildren, NewTimeout, OrchestratorError};
use crate::types::SandboxId;

use super::{sandbox_not_found, ApiImpl};

impl ApiImpl {
    pub(super) async fn fork_post(
        &self,
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
}
