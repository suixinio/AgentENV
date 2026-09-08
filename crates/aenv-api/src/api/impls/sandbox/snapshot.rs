use tracing::warn;

use agentenv_http_server::apis::sandboxes::*;
use agentenv_http_server::models;

use crate::observability::prometheus::SandboxStageTimer;
use crate::orchestrator::OrchestratorError;
use crate::snapshot::SnapshotAlias;
use crate::types::SandboxId;

use super::{sandbox_not_found, ApiImpl};

impl ApiImpl {
    pub(super) async fn snapshot_post(
        &self,
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
                    crate::orchestrator::capture_publish_metadata(
                        &capture.metadata,
                        alias.clone(),
                        None,
                    ),
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
}
