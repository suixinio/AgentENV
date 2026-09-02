use std::time::SystemTime;

use super::store::{NewTimeout, SandboxMetadata};
use super::types::SandboxState;
use crate::runtime_snapshot::RunnableSnapshot;
use crate::sandbox::{FreshSandboxBuildSpec, SandboxLaunchConfig, UnresolvedImageBuildSpec};
use crate::snapshot::SnapshotRecord;
use crate::types::{ExecutionId, SandboxId, SandboxResources};

/// Everything a launch needs, with the incarnation it runs under fixed at
/// construction.
pub struct LaunchPlan {
    pub sandbox_id: SandboxId,
    // Private so plans can only be built through incarnation-accounting constructors.
    execution_id: ExecutionId,
    pub source: LaunchSource,
    pub launch_config: SandboxLaunchConfig,
    pub metadata: SandboxMetadata,
    pub timeout: NewTimeout,
}

pub enum LaunchSource {
    Snapshot {
        snapshot: Box<RunnableSnapshot>,
    },
    /// A catalog snapshot resolved by the node-side factory.
    SnapshotRecord {
        record: Box<SnapshotRecord>,
    },
    Fresh {
        build_spec: Box<FreshSandboxBuildSpec>,
    },
    /// An image reference resolved by the node-side factory.
    UnresolvedImage {
        build_spec: Box<UnresolvedImageBuildSpec>,
    },
}

impl LaunchPlan {
    fn new(
        sandbox_id: SandboxId,
        source: LaunchSource,
        launch_config: SandboxLaunchConfig,
        mut metadata: SandboxMetadata,
        timeout: NewTimeout,
        run_as: Option<ExecutionId>,
    ) -> Self {
        let execution_id = run_as.unwrap_or_else(ExecutionId::new);
        // Keep plan and metadata on the same incarnation.
        metadata.execution_id = execution_id;
        Self {
            sandbox_id,
            execution_id,
            source,
            launch_config,
            metadata,
            timeout,
        }
    }

    /// Builds a plan from a resolved snapshot, minting an incarnation unless
    /// `run_as` supplies one.
    pub fn from_snapshot(
        sandbox_id: SandboxId,
        snapshot: Box<RunnableSnapshot>,
        launch_config: SandboxLaunchConfig,
        metadata: SandboxMetadata,
        timeout: NewTimeout,
        run_as: Option<ExecutionId>,
    ) -> Self {
        Self::new(
            sandbox_id,
            LaunchSource::Snapshot { snapshot },
            launch_config,
            metadata,
            timeout,
            run_as,
        )
    }

    /// The unresolved counterpart of [`Self::from_snapshot`].
    pub fn from_snapshot_record(
        sandbox_id: SandboxId,
        record: Box<SnapshotRecord>,
        launch_config: SandboxLaunchConfig,
        metadata: SandboxMetadata,
        timeout: NewTimeout,
        run_as: Option<ExecutionId>,
    ) -> Self {
        Self::new(
            sandbox_id,
            LaunchSource::SnapshotRecord { record },
            launch_config,
            metadata,
            timeout,
            run_as,
        )
    }

    pub fn fresh(
        sandbox_id: SandboxId,
        build_spec: FreshSandboxBuildSpec,
        launch_config: SandboxLaunchConfig,
        metadata: SandboxMetadata,
        timeout: NewTimeout,
        run_as: Option<ExecutionId>,
    ) -> Self {
        Self::new(
            sandbox_id,
            LaunchSource::Fresh {
                build_spec: Box::new(build_spec),
            },
            launch_config,
            metadata,
            timeout,
            run_as,
        )
    }

    /// The unresolved counterpart of [`Self::fresh`].
    pub fn from_unresolved_image(
        sandbox_id: SandboxId,
        build_spec: UnresolvedImageBuildSpec,
        launch_config: SandboxLaunchConfig,
        metadata: SandboxMetadata,
        timeout: NewTimeout,
        run_as: Option<ExecutionId>,
    ) -> Self {
        Self::new(
            sandbox_id,
            LaunchSource::UnresolvedImage {
                build_spec: Box::new(build_spec),
            },
            launch_config,
            metadata,
            timeout,
            run_as,
        )
    }

    /// The routing projection budget of the sandbox this plan starts.
    pub fn projection_ttl_secs(&self, now: SystemTime) -> u32 {
        self.metadata.projection_ttl_secs(now)
    }

    /// The incarnation this launch runs under.
    pub fn execution_id(&self) -> ExecutionId {
        self.execution_id
    }

    pub fn transitional_state(&self) -> SandboxState {
        SandboxState::Creating
    }

    pub fn resources(&self) -> SandboxResources {
        self.metadata.resources
    }
}
