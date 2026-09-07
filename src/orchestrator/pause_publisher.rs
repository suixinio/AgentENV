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

    /// The snapshot a pause of this sandbox published no earlier than
    /// `not_before_unix_ms`, for a caller that watched a pause it did not run.
    ///
    /// `Ok(None)` is a complete answer that no such snapshot exists. An
    /// implementation with no catalog to ask returns `Err`, which callers read
    /// as unknown and never as absence.
    async fn published_since(
        &self,
        sandbox_id: crate::types::SandboxId,
        not_before_unix_ms: i64,
    ) -> anyhow::Result<Option<crate::snapshot::SnapshotId>>;
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

    /// A node holds no catalog, so it cannot answer for a pause it did not run.
    async fn published_since(
        &self,
        sandbox_id: crate::types::SandboxId,
        _not_before_unix_ms: i64,
    ) -> anyhow::Result<Option<crate::snapshot::SnapshotId>> {
        anyhow::bail!(
            "this process keeps no snapshot catalog, so it cannot say whether a pause of \
             sandbox {sandbox_id} published anything"
        )
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

    async fn published_since(
        &self,
        sandbox_id: crate::types::SandboxId,
        not_before_unix_ms: i64,
    ) -> anyhow::Result<Option<crate::snapshot::SnapshotId>> {
        let filter = crate::snapshot::repository::interfaces::SnapshotListFilter::sandbox_snapshots(
            Some(sandbox_id.to_string()),
            None,
        );
        let mut cursor = None;
        loop {
            let page = self
                .snapshots
                .list_page_scoped(
                    filter.clone().paginated(None, cursor.take()),
                    crate::snapshot::repository::interfaces::CatalogReadScope::Resolvable,
                )
                .await?;
            // Rows come back newest first, so the walk stops as soon as one is
            // older than the window the caller asked about.
            for record in &page.items {
                if record.created_at_unix_ms < not_before_unix_ms {
                    return Ok(None);
                }
                if record.paused_sandbox().is_some() {
                    return Ok(Some(record.id.clone()));
                }
            }
            match page.next {
                Some(next) => cursor = Some(next),
                None => return Ok(None),
            }
        }
    }
}

/// Publishes nothing and reports a fresh committed id: for tests that exercise
/// the state machine without a repository.
#[cfg(any(test, feature = "test-support"))]
#[derive(Default)]
pub struct DiscardingPausePublisher {
    published: std::sync::Mutex<Vec<(crate::types::SandboxId, crate::snapshot::SnapshotId)>>,
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
            .iter()
            .map(|(sandbox_id, _)| *sandbox_id)
            .collect()
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
        let snapshot_id = crate::snapshot::SnapshotId::generate();
        self.published
            .lock()
            .expect("published mutex poisoned")
            .push((metadata.id, snapshot_id.clone()));
        Ok(PublishedPause::Committed(snapshot_id))
    }

    async fn published_since(
        &self,
        sandbox_id: crate::types::SandboxId,
        _not_before_unix_ms: i64,
    ) -> anyhow::Result<Option<crate::snapshot::SnapshotId>> {
        Ok(self
            .published
            .lock()
            .expect("published mutex poisoned")
            .iter()
            .rev()
            .find(|(id, _)| *id == sandbox_id)
            .map(|(_, snapshot_id)| snapshot_id.clone()))
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
