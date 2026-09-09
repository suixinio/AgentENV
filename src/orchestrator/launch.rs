//! What both halves say about a launch: its result, and the marker that says
//! somebody else is already running one under this id.
//!
//! The exclusion itself belongs to whichever half performs launches; these two
//! are the vocabulary a caller on either side reads.

use super::store::SandboxMetadata;
use super::OrchestratorError;
use crate::types::SandboxId;

/// A restore's result, and whether this caller performed the launch or waited
/// out one that was already running.
#[derive(Debug, Clone)]
pub struct RestoredSandbox {
    pub metadata: SandboxMetadata,
    pub joined: bool,
}

/// Marker a placement source attaches when it refuses to reserve a sandbox
/// another replica is still launching.
#[derive(Debug, thiserror::Error)]
#[error("sandbox {sandbox_id} is being launched by another replica")]
pub struct LaunchHeldElsewhere {
    pub sandbox_id: SandboxId,
}

impl LaunchHeldElsewhere {
    /// Whether the launch `error` ended is one another replica already holds.
    pub fn refused(error: &OrchestratorError) -> bool {
        let OrchestratorError::SandboxOperationFailed { source, .. } = error else {
            return false;
        };
        source
            .chain()
            .any(|cause| cause.is::<LaunchHeldElsewhere>())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orchestrator::SandboxOperation;

    #[test]
    fn a_launch_held_elsewhere_is_recognised_through_the_layers_that_wrap_it() {
        let sandbox_id = SandboxId::new();
        let refused = OrchestratorError::SandboxOperationFailed {
            sandbox_id,
            operation: SandboxOperation::Start,
            source: anyhow::Error::new(LaunchHeldElsewhere { sandbox_id })
                .context("reserve a routing record before starting it")
                .context("start sandbox on node node-a"),
        };
        assert!(LaunchHeldElsewhere::refused(&refused));

        let other = OrchestratorError::SandboxOperationFailed {
            sandbox_id,
            operation: SandboxOperation::Start,
            source: anyhow::anyhow!("the node refused the launch"),
        };
        assert!(
            !LaunchHeldElsewhere::refused(&other),
            "waiting for a launch nobody is running would hang every ordinary failure"
        );
    }
}
