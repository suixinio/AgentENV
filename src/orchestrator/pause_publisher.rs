//! Where a pause capture becomes durable.
//!
//! The node stages the bytes it captured and hands the staged value back to
//! its caller; the api half commits the staged value into the catalog. Both
//! are the same hook on the orchestrator, chosen at wiring time.

use std::sync::Arc;

use anyhow::Context as _;
use async_trait::async_trait;

use super::store::SandboxMetadata;
use super::types::{capture_publish_metadata, PublishedPause};
use crate::sandbox::CapturedSandboxSnapshot;
use crate::snapshot::{PausedSandboxConfig, SnapshotManager};

/// Makes a pause capture durable for the cluster.
///
/// Called with the VM still paused in place: a failure lets the orchestrator
/// resume the VM and report the pause as failed, so an implementation must not
/// leave the capture half-published.
#[async_trait]
pub trait PausePublisher: Send + Sync {
    async fn publish(
        &self,
        metadata: &SandboxMetadata,
        capture: CapturedSandboxSnapshot,
    ) -> anyhow::Result<PublishedPause>;
}

/// The node's half: stages the capture on this machine's repository and
/// returns the value the api half commits. The `PausedSandboxConfig` it
/// stages is a placeholder: the node's record lacks the sandbox's user-facing
/// configuration, and the api half's commit replaces it with its own.
pub struct StagingPausePublisher {
    snapshots: Arc<SnapshotManager>,
}

impl StagingPausePublisher {
    pub fn new(snapshots: Arc<SnapshotManager>) -> Self {
        Self { snapshots }
    }
}

#[async_trait]
impl PausePublisher for StagingPausePublisher {
    async fn publish(
        &self,
        metadata: &SandboxMetadata,
        capture: CapturedSandboxSnapshot,
    ) -> anyhow::Result<PublishedPause> {
        let publish = capture_publish_metadata(
            metadata,
            None,
            Some(PausedSandboxConfig::of(
                metadata,
                std::time::SystemTime::now(),
            )),
        );
        let staged = self
            .snapshots
            .stage_captured(publish, capture)
            .await
            .with_context(|| format!("stage the pause capture of sandbox {}", metadata.id))?
            .into_staged();
        Ok(PublishedPause::Staged(Box::new(staged)))
    }
}

/// The api half: commits a value a node staged, making the sandbox resumable
/// anywhere.
pub struct CommittingPausePublisher {
    snapshots: Arc<SnapshotManager>,
}

impl CommittingPausePublisher {
    pub fn new(snapshots: Arc<SnapshotManager>) -> Self {
        Self { snapshots }
    }
}

#[async_trait]
impl PausePublisher for CommittingPausePublisher {
    async fn publish(
        &self,
        metadata: &SandboxMetadata,
        capture: CapturedSandboxSnapshot,
    ) -> anyhow::Result<PublishedPause> {
        let publish = capture_publish_metadata(
            metadata,
            None,
            Some(PausedSandboxConfig::of(
                metadata,
                std::time::SystemTime::now(),
            )),
        );
        let record = self
            .snapshots
            .publish_captured(publish, capture)
            .await
            .with_context(|| format!("commit the pause capture of sandbox {}", metadata.id))?;
        Ok(PublishedPause::Committed(record.id))
    }
}

/// Publishes nothing and reports a fresh committed id: for tests that exercise
/// the state machine without a repository.
#[cfg(any(test, feature = "test-support"))]
#[derive(Default)]
pub struct DiscardingPausePublisher {
    published: std::sync::Mutex<Vec<crate::types::SandboxId>>,
}

#[cfg(any(test, feature = "test-support"))]
impl DiscardingPausePublisher {
    pub fn shared() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Sandboxes whose pause this publisher was handed, in order.
    pub fn published(&self) -> Vec<crate::types::SandboxId> {
        self.published
            .lock()
            .expect("published mutex poisoned")
            .clone()
    }
}

#[cfg(any(test, feature = "test-support"))]
#[async_trait]
impl PausePublisher for DiscardingPausePublisher {
    async fn publish(
        &self,
        metadata: &SandboxMetadata,
        _capture: CapturedSandboxSnapshot,
    ) -> anyhow::Result<PublishedPause> {
        self.published
            .lock()
            .expect("published mutex poisoned")
            .push(metadata.id);
        Ok(PublishedPause::Committed(
            crate::snapshot::SnapshotId::generate(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::orchestrator::SandboxTimeoutAction;
    use crate::snapshot::mock::{in_memory_snapshot_manager, mock_paused_sandbox_config};
    use crate::snapshot::repository::interfaces::SnapshotCatalog;
    use crate::snapshot::repository::{SnapshotCommit, StagedSnapshot};
    use crate::snapshot::{CommittedSnapshot, SnapshotId, SnapshotPublishSource};
    use crate::types::SandboxId;

    /// What a node stages: its record knows nothing of the sandbox's
    /// user-facing configuration, so the config it writes is the defaults.
    fn staged_by_a_node(sandbox_id: SandboxId) -> StagedSnapshot {
        let mut committed = CommittedSnapshot::mock();
        committed.paused_sandbox = Some(PausedSandboxConfig {
            auto_resume: false,
            user_metadata: None,
            timeout_action: SandboxTimeoutAction::Pause,
            ..mock_paused_sandbox_config()
        });
        StagedSnapshot {
            commit: SnapshotCommit {
                id: SnapshotId::generate(),
                alias: None,
                source: SnapshotPublishSource::Sandbox {
                    source_sandbox_id: sandbox_id.to_string(),
                },
                resources: Default::default(),
                created_at_unix_ms: Some(1_700_000_000_000),
                origin_node_id: Some("node-a".to_string()),
                committed,
            },
            staged_at_unix_ms: 1_700_000_000_000,
            origin_node_id: "node-a".to_string(),
        }
    }

    #[tokio::test]
    async fn the_committed_row_carries_the_api_halfs_configuration_not_the_nodes() {
        let (manager, catalog) = in_memory_snapshot_manager();
        let publisher = CommittingPausePublisher::new(Arc::new(manager));
        let sandbox_id = SandboxId::new();
        let user_metadata = Some([("owner".to_string(), "api".to_string())].into());
        let metadata = SandboxMetadata {
            id: sandbox_id,
            auto_resume: true,
            user_metadata: user_metadata.clone(),
            timeout_action: SandboxTimeoutAction::Delete,
            ..Default::default()
        };

        let published = publisher
            .publish(
                &metadata,
                CapturedSandboxSnapshot::staged(staged_by_a_node(sandbox_id)),
            )
            .await
            .expect("the staged capture commits");

        let PublishedPause::Committed(id) = published else {
            panic!("the api half commits, it does not stage: {published:?}");
        };
        let row = catalog
            .get(&id.to_string())
            .await
            .expect("the catalog answers")
            .expect("the committed row");
        let paused = row
            .paused_sandbox()
            .expect("a pause row carries its config");
        assert!(
            paused.auto_resume,
            "the data-plane wake-up gate reads this flag; the node staged it as false"
        );
        assert_eq!(paused.user_metadata, user_metadata);
        assert!(matches!(
            paused.timeout_action,
            SandboxTimeoutAction::Delete
        ));
    }
}
