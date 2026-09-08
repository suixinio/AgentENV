use agentenv_http_server::apis::sandboxes::*;
use agentenv_http_server::models;

use crate::orchestrator::OrchestratorError;
use crate::types::SandboxId;

use super::{sandbox_not_found, ApiImpl};
use aenv_core::api::wire::{params_map_to_model, params_model_to_map};

impl ApiImpl {
    pub(super) async fn custom_params_get(
        &self,
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

    pub(super) async fn custom_params_patch(
        &self,
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
}
