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
/// returns the value the api half commits.
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
