use agentenv_http_server::models;

use crate::orchestrator::OrchestratorError;

use super::internal_error;

impl From<OrchestratorError> for models::Error {
    fn from(err: OrchestratorError) -> Self {
        match err {
            OrchestratorError::ShuttingDown => {
                Self::new(503, "orchestrator is shutting down".to_string())
            }
            OrchestratorError::NotAcceptingNewWork => Self::new(
                503,
                "node is isolated and is not taking new sandboxes".to_string(),
            ),
            OrchestratorError::SandboxNotFound(id) => {
                Self::new(404, format!("sandbox {id} not found"))
            }
            OrchestratorError::InvalidSandboxState { .. } => Self::new(400, err.to_string()),
            OrchestratorError::SandboxLifetimeExceeded { .. } => Self::new(400, err.to_string()),
            OrchestratorError::SandboxOperationFailed {
                sandbox_id,
                operation,
                source,
            } => Self::new(
                500,
                format!(
                    "sandbox {} operation {:?} failed: {}",
                    sandbox_id,
                    operation,
                    internal_error(source.as_ref()).message
                ),
            ),
            OrchestratorError::SandboxOperationConflict { .. } => Self::new(409, err.to_string()),
            OrchestratorError::InvalidRequest(_) => Self::new(400, err.to_string()),
            other => internal_error(&other),
        }
    }
}
