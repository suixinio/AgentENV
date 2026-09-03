use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;

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
    use std::collections::BTreeSet;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;

    use super::GrantIssuer;
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
    }

    impl RecordingGrantIssuer {
        pub fn shared() -> Arc<Self> {
            Arc::new(Self::default())
        }

        pub fn events(&self) -> Vec<GrantEvent> {
            self.events.lock().unwrap().clone()
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
    }
}
