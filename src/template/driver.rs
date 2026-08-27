//! What the API surface asks of the template builder.
//!
//! # 🔴 Two arms, one of which runs a microVM
//!
//! `POST /v2/templates/{id}/builds/{id}` has always had two shapes: run the
//! build in this process, or hand it to a machine that can. The first drives a
//! real Firecracker sandbox through every RUN step; the second sends a request
//! and waits. The route decides between them, but only one of the two is
//! something a process without `/dev/kvm` could even link.
//!
//! So the route holds the local arm behind this trait. A process that runs
//! sandboxes wires in the real builder; one that does not wires in
//! [`RefusingTemplateBuildDriver`] and never reaches the arm that would call
//! it.

use async_trait::async_trait;

use super::errors::{TemplateBuildError, TemplatePipelineError, TemplatePipelineResult};
use super::TemplateBuildSpec;
use crate::runtime_snapshot::RunnableSnapshot;
use crate::snapshot::{SnapshotId, SnapshotManager, SnapshotRecord};

/// Builds a snapshot by running its steps in *this* process.
#[async_trait]
pub trait TemplateBuildDriver: Send + Sync {
    /// Builds and publishes a new snapshot using a caller-provided id.
    async fn build_and_publish_with_id(
        &self,
        snapshot_manager: &SnapshotManager,
        snapshot_id: SnapshotId,
        spec: TemplateBuildSpec,
    ) -> TemplatePipelineResult<SnapshotRecord>;

    /// Builds a new snapshot from an existing committed snapshot and publishes it.
    async fn build_from_snapshot_and_publish(
        &self,
        snapshot_manager: &SnapshotManager,
        spec: TemplateBuildSpec,
        snapshot_id: SnapshotId,
        base_snapshot: &RunnableSnapshot,
    ) -> TemplatePipelineResult<SnapshotRecord>;
}

/// The driver for a process that runs no builds itself.
///
/// 🔴 Refuses rather than panicking. The route reaches the local arm only when
/// [`ApiImpl::runs_sandbox_runtime`][crate::api::ApiImpl::runs_sandbox_runtime]
/// says so, and this half answers `false` — so a call landing here is a routing
/// mistake, and a build that fails with a message beats a replica that dies.
#[derive(Debug, Default)]
pub struct RefusingTemplateBuildDriver;

impl RefusingTemplateBuildDriver {
    fn refusal() -> TemplatePipelineError {
        TemplatePipelineError::Build(TemplateBuildError::system(
            "this process runs no sandboxes, so it cannot run a template build itself: the build \
             belongs on a node (see run_the_build_on_a_node)",
        ))
    }
}

#[async_trait]
impl TemplateBuildDriver for RefusingTemplateBuildDriver {
    async fn build_and_publish_with_id(
        &self,
        _snapshot_manager: &SnapshotManager,
        _snapshot_id: SnapshotId,
        _spec: TemplateBuildSpec,
    ) -> TemplatePipelineResult<SnapshotRecord> {
        Err(Self::refusal())
    }

    async fn build_from_snapshot_and_publish(
        &self,
        _snapshot_manager: &SnapshotManager,
        _spec: TemplateBuildSpec,
        _snapshot_id: SnapshotId,
        _base_snapshot: &RunnableSnapshot,
    ) -> TemplatePipelineResult<SnapshotRecord> {
        Err(Self::refusal())
    }
}
