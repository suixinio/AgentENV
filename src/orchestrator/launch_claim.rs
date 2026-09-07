//! In-process exclusion for launches that share one sandbox id.
//!
//! A launch takes its claim before it allocates anything, which is earlier than
//! any handle or record exists, so it is the only thing that can tell a second
//! launch of the same id that the id is taken.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::watch;

use super::store::SandboxMetadata;
use super::types::SandboxState;
use super::OrchestratorError;
use crate::types::{ExecutionId, SandboxId};

/// How a launch ended, for the callers that waited on it.
#[derive(Clone, Debug)]
pub enum LaunchSettlement {
    Launched(Box<SandboxMetadata>),
    Failed(LaunchFailure),
}

/// A failed launch's error class, kept so a joiner answers what the launch it
/// joined answered instead of starting a second launch of its own.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LaunchFailure {
    AlreadyExists,
    NotAcceptingNewWork,
    ShuttingDown,
    InvalidState(SandboxState),
    Other(String),
}

impl LaunchFailure {
    pub fn of(error: &OrchestratorError) -> Self {
        match error {
            OrchestratorError::StoreOperationFailed(
                super::store::StoreError::SandboxAlreadyExists { .. },
            )
            | OrchestratorError::LaunchInFlight { .. } => Self::AlreadyExists,
            OrchestratorError::NotAcceptingNewWork => Self::NotAcceptingNewWork,
            OrchestratorError::ShuttingDown => Self::ShuttingDown,
            OrchestratorError::InvalidSandboxState { state, .. } => Self::InvalidState(*state),
            other => Self::Other(other.to_string()),
        }
    }

    pub fn into_error(self, sandbox_id: SandboxId) -> OrchestratorError {
        match self {
            Self::AlreadyExists => OrchestratorError::StoreOperationFailed(
                super::store::StoreError::SandboxAlreadyExists { sandbox_id },
            ),
            Self::NotAcceptingNewWork => OrchestratorError::NotAcceptingNewWork,
            Self::ShuttingDown => OrchestratorError::ShuttingDown,
            Self::InvalidState(state) => {
                OrchestratorError::InvalidSandboxState { sandbox_id, state }
            }
            Self::Other(message) => OrchestratorError::InternalError(message),
        }
    }
}

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

type Settled = Arc<watch::Sender<Option<LaunchSettlement>>>;

/// A launch in flight, as seen by a caller that could not claim the id.
#[derive(Clone, Debug)]
pub struct LaunchInFlight {
    execution_id: ExecutionId,
    settled: Settled,
}

impl LaunchInFlight {
    pub fn execution_id(&self) -> ExecutionId {
        self.execution_id
    }

    /// Waits out the launch holding this claim. `None` is the wait running out
    /// with the claimant still working.
    pub async fn join(&self, wait: Duration) -> Option<LaunchSettlement> {
        // Subscribing through the retained sender keeps the channel open for
        // the whole wait, so a closed channel can never be read as an answer.
        let mut rx = self.settled.subscribe();
        let settled = async {
            loop {
                let current = rx.borrow_and_update().clone();
                if let Some(settlement) = current {
                    return settlement;
                }
                if rx.changed().await.is_err() {
                    return LaunchSettlement::Failed(LaunchFailure::Other(
                        "the launch this caller joined ended without an answer".to_string(),
                    ));
                }
            }
        };
        tokio::time::timeout(wait, settled).await.ok()
    }
}

/// The sandbox ids this process is launching right now.
#[derive(Default, Debug)]
pub struct LaunchClaims {
    inner: Mutex<HashMap<SandboxId, LaunchInFlight>>,
}

impl LaunchClaims {
    /// Takes the id for `execution_id`, or reports the launch that holds it.
    pub fn claim(
        self: &Arc<Self>,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
    ) -> Result<LaunchClaimGuard, LaunchInFlight> {
        let mut claims = self.inner.lock().expect("launch claim table poisoned");
        if let Some(held) = claims.get(&sandbox_id) {
            return Err(held.clone());
        }
        let settled = Arc::new(watch::channel(None).0);
        claims.insert(
            sandbox_id,
            LaunchInFlight {
                execution_id,
                settled: Arc::clone(&settled),
            },
        );
        Ok(LaunchClaimGuard {
            claims: Arc::clone(self),
            sandbox_id,
            settled,
        })
    }

    /// The launch in flight under `sandbox_id`, if this process has one.
    pub fn in_flight(&self, sandbox_id: SandboxId) -> Option<LaunchInFlight> {
        self.inner
            .lock()
            .expect("launch claim table poisoned")
            .get(&sandbox_id)
            .cloned()
    }

    fn release(&self, sandbox_id: SandboxId, settled: &Settled) {
        let mut claims = self.inner.lock().expect("launch claim table poisoned");
        let is_ours = claims
            .get(&sandbox_id)
            .is_some_and(|held| Arc::ptr_eq(&held.settled, settled));
        if is_ours {
            claims.remove(&sandbox_id);
        }
    }
}

/// Holds one sandbox id for one launch. Dropping it frees the id.
#[derive(Debug)]
pub struct LaunchClaimGuard {
    claims: Arc<LaunchClaims>,
    sandbox_id: SandboxId,
    settled: Settled,
}

impl LaunchClaimGuard {
    /// Publishes what the launch produced to everyone waiting on it.
    ///
    /// `send_replace`, not `send`: a launch that finished before anyone
    /// subscribed still has to leave its answer for the next caller to read.
    pub fn settle(self, settlement: LaunchSettlement) {
        self.settled.send_replace(Some(settlement));
    }
}

impl Drop for LaunchClaimGuard {
    fn drop(&mut self) {
        if self.settled.borrow().is_none() {
            self.settled
                .send_replace(Some(LaunchSettlement::Failed(LaunchFailure::Other(
                    "the launch this caller joined ended without an answer".to_string(),
                ))));
        }
        self.claims.release(self.sandbox_id, &self.settled);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn claims() -> Arc<LaunchClaims> {
        Arc::new(LaunchClaims::default())
    }

    #[tokio::test]
    async fn a_second_claim_on_a_held_id_is_refused_and_names_the_holder() {
        let claims = claims();
        let sandbox_id = SandboxId::new();
        let holder = ExecutionId::new();
        let guard = claims.claim(sandbox_id, holder).expect("the id is free");
        let refused = claims
            .claim(sandbox_id, ExecutionId::new())
            .expect_err("a claimed id is not free");
        assert_eq!(refused.execution_id(), holder);
        drop(guard);
        assert!(claims.claim(sandbox_id, ExecutionId::new()).is_ok());
    }

    #[tokio::test]
    async fn a_joiner_reads_the_outcome_the_claimant_published() {
        let claims = claims();
        let sandbox_id = SandboxId::new();
        let guard = claims
            .claim(sandbox_id, ExecutionId::new())
            .expect("the id is free");
        let waiting = claims.in_flight(sandbox_id).expect("a launch is in flight");
        let joined = tokio::spawn(async move { waiting.join(Duration::from_secs(5)).await });
        guard.settle(LaunchSettlement::Failed(LaunchFailure::NotAcceptingNewWork));
        assert!(matches!(
            joined.await.unwrap(),
            Some(LaunchSettlement::Failed(LaunchFailure::NotAcceptingNewWork))
        ));
    }

    #[tokio::test]
    async fn a_claim_dropped_without_an_outcome_answers_its_joiners() {
        let claims = claims();
        let sandbox_id = SandboxId::new();
        let guard = claims
            .claim(sandbox_id, ExecutionId::new())
            .expect("the id is free");
        let waiting = claims.in_flight(sandbox_id).expect("a launch is in flight");
        drop(guard);
        assert!(matches!(
            waiting.join(Duration::from_secs(5)).await,
            Some(LaunchSettlement::Failed(LaunchFailure::Other(_)))
        ));
        assert!(
            claims.in_flight(sandbox_id).is_none(),
            "the id has to be free again once its launch is gone"
        );
    }

    #[test]
    fn a_launch_held_elsewhere_is_recognised_through_the_layers_that_wrap_it() {
        let sandbox_id = SandboxId::new();
        let refused = OrchestratorError::SandboxOperationFailed {
            sandbox_id,
            operation: crate::orchestrator::SandboxOperation::Start,
            source: anyhow::Error::new(LaunchHeldElsewhere { sandbox_id })
                .context("reserve a routing record before starting it")
                .context("start sandbox on node node-a"),
        };
        assert!(LaunchHeldElsewhere::refused(&refused));

        let other = OrchestratorError::SandboxOperationFailed {
            sandbox_id,
            operation: crate::orchestrator::SandboxOperation::Start,
            source: anyhow::anyhow!("the node refused the launch"),
        };
        assert!(
            !LaunchHeldElsewhere::refused(&other),
            "waiting for a launch nobody is running would hang every ordinary failure"
        );
    }

    #[tokio::test]
    async fn a_joiner_that_runs_out_of_patience_answers_nothing() {
        let claims = claims();
        let sandbox_id = SandboxId::new();
        let _guard = claims
            .claim(sandbox_id, ExecutionId::new())
            .expect("the id is free");
        let waiting = claims.in_flight(sandbox_id).expect("a launch is in flight");
        assert!(waiting.join(Duration::from_millis(20)).await.is_none());
    }
}
