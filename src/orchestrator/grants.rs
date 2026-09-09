use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::secret_kind::SecretKind;
use crate::types::{ExecutionId, SandboxId};

/// Records which secret names one incarnation of a sandbox may read. The
/// orchestrator grants before a sandbox starts and revokes when it is gone;
/// the broker's credential source honours only what is granted.
#[async_trait]
pub trait GrantIssuer: Send + Sync {
    async fn grant(
        &self,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
        names: &BTreeSet<String>,
    ) -> anyhow::Result<()>;

    async fn revoke(&self, sandbox_id: SandboxId, execution_id: ExecutionId) -> anyhow::Result<()>;

    /// The subset of `wanted` the store holds no secret for, or holds in a
    /// shape other than the one the policy uses it in; `None` when this
    /// issuer cannot answer. A store outage and an all-present answer must
    /// not look alike to a caller that only warns, which is why the two are
    /// different values rather than an empty list.
    async fn unusable_names(&self, _wanted: &BTreeMap<String, SecretKind>) -> Option<Vec<String>> {
        None
    }

    /// Grants at least `min_age` old, oldest first and at most `limit`, for
    /// the orchestrator to compare with its records and revoke where no
    /// record backs them. An issuer that keeps no grants answers none.
    async fn stale_grant_candidates(
        &self,
        _min_age: Duration,
        _limit: usize,
    ) -> anyhow::Result<Vec<(SandboxId, ExecutionId)>> {
        Ok(Vec::new())
    }
}

/// The issuer of an api half with `[secrets].backend = "disabled"`. A grant
/// for a non-empty name set is refused, so a sandbox that needs credentials
/// never starts silently without them.
pub struct NoGrants;

impl NoGrants {
    pub fn shared() -> Arc<dyn GrantIssuer> {
        Arc::new(Self)
    }
}

#[async_trait]
impl GrantIssuer for NoGrants {
    async fn grant(
        &self,
        _sandbox_id: SandboxId,
        _execution_id: ExecutionId,
        names: &BTreeSet<String>,
    ) -> anyhow::Result<()> {
        if names.is_empty() {
            Ok(())
        } else {
            anyhow::bail!(
                "this process has no secrets store to grant {} secret name(s) from",
                names.len()
            )
        }
    }

    async fn revoke(
        &self,
        _sandbox_id: SandboxId,
        _execution_id: ExecutionId,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

/// The issuer of the node half. The api half that dispatched the create
/// recorded the grant and revokes it; the node holds no store and records
/// nothing, but must not refuse a policy the api half already granted.
pub struct GrantsIssuedUpstream;

impl GrantsIssuedUpstream {
    pub fn shared() -> Arc<dyn GrantIssuer> {
        Arc::new(Self)
    }
}

#[async_trait]
impl GrantIssuer for GrantsIssuedUpstream {
    async fn grant(
        &self,
        _sandbox_id: SandboxId,
        _execution_id: ExecutionId,
        _names: &BTreeSet<String>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn revoke(
        &self,
        _sandbox_id: SandboxId,
        _execution_id: ExecutionId,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

/// Remembers every call, for orchestrator tests.
#[cfg(any(test, feature = "test-support"))]
pub mod recording {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use async_trait::async_trait;

    use super::GrantIssuer;
    use crate::secret_kind::SecretKind;
    use crate::types::{ExecutionId, SandboxId};

    #[derive(Clone, Debug, PartialEq, Eq)]
    pub enum GrantEvent {
        Grant {
            sandbox_id: SandboxId,
            execution_id: ExecutionId,
            names: BTreeSet<String>,
        },
        Revoke {
            sandbox_id: SandboxId,
            execution_id: ExecutionId,
        },
    }

    #[derive(Default)]
    pub struct RecordingGrantIssuer {
        events: Mutex<Vec<GrantEvent>>,
        unknown: Mutex<Option<Vec<String>>>,
        stale: Mutex<Vec<(SandboxId, ExecutionId)>>,
    }

    impl RecordingGrantIssuer {
        pub fn shared() -> Arc<Self> {
            Arc::new(Self::default())
        }

        pub fn events(&self) -> Vec<GrantEvent> {
            self.events.lock().unwrap().clone()
        }

        /// What this issuer answers when asked which names are unusable.
        /// Unset leaves it unable to say, which is the default.
        pub fn answer_unusable_names(&self, unknown: Vec<String>) {
            *self.unknown.lock().unwrap() = Some(unknown);
        }

        /// What this issuer offers the reaper as aged grants.
        pub fn answer_stale_grants(&self, stale: Vec<(SandboxId, ExecutionId)>) {
            *self.stale.lock().unwrap() = stale;
        }
    }

    #[async_trait]
    impl GrantIssuer for RecordingGrantIssuer {
        async fn grant(
            &self,
            sandbox_id: SandboxId,
            execution_id: ExecutionId,
            names: &BTreeSet<String>,
        ) -> anyhow::Result<()> {
            self.events.lock().unwrap().push(GrantEvent::Grant {
                sandbox_id,
                execution_id,
                names: names.clone(),
            });
            Ok(())
        }

        async fn revoke(
            &self,
            sandbox_id: SandboxId,
            execution_id: ExecutionId,
        ) -> anyhow::Result<()> {
            self.events.lock().unwrap().push(GrantEvent::Revoke {
                sandbox_id,
                execution_id,
            });
            Ok(())
        }

        async fn unusable_names(
            &self,
            _wanted: &BTreeMap<String, SecretKind>,
        ) -> Option<Vec<String>> {
            self.unknown.lock().unwrap().clone()
        }

        async fn stale_grant_candidates(
            &self,
            _min_age: Duration,
            limit: usize,
        ) -> anyhow::Result<Vec<(SandboxId, ExecutionId)>> {
            Ok(self
                .stale
                .lock()
                .unwrap()
                .iter()
                .take(limit)
                .copied()
                .collect())
        }
    }
}
