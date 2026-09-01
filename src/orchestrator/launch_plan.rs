use std::sync::Arc;
use std::time::SystemTime;

use super::store::{NewTimeout, SandboxMetadata};
use super::types::SandboxState;
use crate::runtime_snapshot::RunnableSnapshot;
use crate::sandbox::{
    EnvdAccessToken, FreshSandboxBuildSpec, PausedSandboxState, SandboxLaunchConfig,
    UnresolvedImageBuildSpec,
};
use crate::snapshot::SnapshotRecord;
use crate::types::{ExecutionId, SandboxId, SandboxResources};

/// A granted resume claim carrying the incarnation allocated by arbitration.
#[derive(Debug)]
pub struct ClaimedExecution(ExecutionId);

impl ClaimedExecution {
    /// Mints a token only after resume arbitration grants the claim.
    pub fn from_claim(execution_id: ExecutionId) -> Self {
        Self(execution_id)
    }

    /// Adopts an incarnation already claimed by an orchestrator in another process.
    pub fn adopted_from_remote_claim(execution_id: ExecutionId) -> Self {
        Self(execution_id)
    }

    /// Test-only claim token for integration tests.
    pub fn minted_for_test() -> Self {
        Self(ExecutionId::new())
    }

    /// Returns the incarnation allocated by this claim.
    pub fn execution_id(&self) -> ExecutionId {
        self.0
    }
}

pub struct CreateLaunchPlan {
    pub sandbox_id: SandboxId,
    // Private so plans can only be built through incarnation-accounting constructors.
    execution_id: ExecutionId,
    pub source: CreateLaunchSource,
    pub launch_config: SandboxLaunchConfig,
    pub metadata: SandboxMetadata,
    pub timeout: NewTimeout,
}

pub enum CreateLaunchSource {
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

pub struct ResumeLaunchPlan {
    pub sandbox_id: SandboxId,
    // Sourced from the consumed claim token.
    execution_id: ExecutionId,
    pub paused_state: Arc<dyn PausedSandboxState>,
    pub timeout: NewTimeout,
    pub resources: SandboxResources,
    pub envd_access_token: Option<EnvdAccessToken>,
    /// The paused record's remaining lifetime budget for the routing
    /// projection, read where the record is; zero delegates to the store.
    pub projection_ttl_secs: u32,
}

pub enum LaunchPlan {
    Create(Box<CreateLaunchPlan>),
    Resume(Box<ResumeLaunchPlan>),
}

impl LaunchPlan {
    /// Builds a create plan, minting an incarnation unless `run_as` supplies one.
    pub fn for_create_from_snapshot(
        sandbox_id: SandboxId,
        snapshot: Box<RunnableSnapshot>,
        launch_config: SandboxLaunchConfig,
        mut metadata: SandboxMetadata,
        timeout: NewTimeout,
        run_as: Option<ExecutionId>,
    ) -> Self {
        // Snapshot-based construction is still a create, not a resume.
        let execution_id = run_as.unwrap_or_else(ExecutionId::new);
        // Keep plan and metadata on the same incarnation.
        metadata.execution_id = execution_id;
        Self::Create(Box::new(CreateLaunchPlan {
            sandbox_id,
            execution_id,
            source: CreateLaunchSource::Snapshot { snapshot },
            launch_config,
            metadata,
            timeout,
        }))
    }

    /// Builds the unresolved counterpart of [`Self::for_create_from_snapshot`].
    pub fn for_create_from_snapshot_record(
        sandbox_id: SandboxId,
        record: Box<SnapshotRecord>,
        launch_config: SandboxLaunchConfig,
        mut metadata: SandboxMetadata,
        timeout: NewTimeout,
        run_as: Option<ExecutionId>,
    ) -> Self {
        let execution_id = run_as.unwrap_or_else(ExecutionId::new);
        metadata.execution_id = execution_id;
        Self::Create(Box::new(CreateLaunchPlan {
            sandbox_id,
            execution_id,
            source: CreateLaunchSource::SnapshotRecord { record },
            launch_config,
            metadata,
            timeout,
        }))
    }

    pub fn for_create_fresh(
        sandbox_id: SandboxId,
        build_spec: FreshSandboxBuildSpec,
        launch_config: SandboxLaunchConfig,
        mut metadata: SandboxMetadata,
        timeout: NewTimeout,
        run_as: Option<ExecutionId>,
    ) -> Self {
        let execution_id = run_as.unwrap_or_else(ExecutionId::new);
        metadata.execution_id = execution_id;
        Self::Create(Box::new(CreateLaunchPlan {
            sandbox_id,
            execution_id,
            source: CreateLaunchSource::Fresh {
                build_spec: Box::new(build_spec),
            },
            launch_config,
            metadata,
            timeout,
        }))
    }

    /// Builds the unresolved counterpart of [`Self::for_create_fresh`].
    pub fn for_create_unresolved_image(
        sandbox_id: SandboxId,
        build_spec: UnresolvedImageBuildSpec,
        launch_config: SandboxLaunchConfig,
        mut metadata: SandboxMetadata,
        timeout: NewTimeout,
        run_as: Option<ExecutionId>,
    ) -> Self {
        let execution_id = run_as.unwrap_or_else(ExecutionId::new);
        metadata.execution_id = execution_id;
        Self::Create(Box::new(CreateLaunchPlan {
            sandbox_id,
            execution_id,
            source: CreateLaunchSource::UnresolvedImage {
                build_spec: Box::new(build_spec),
            },
            launch_config,
            metadata,
            timeout,
        }))
    }

    /// Builds a resume plan using the granted claim's incarnation.
    pub fn for_resume(
        sandbox_id: SandboxId,
        claimed: ClaimedExecution,
        paused_state: Arc<dyn PausedSandboxState>,
        timeout: NewTimeout,
        resources: SandboxResources,
        envd_access_token: Option<EnvdAccessToken>,
        projection_ttl_secs: u32,
    ) -> Self {
        Self::Resume(Box::new(ResumeLaunchPlan {
            sandbox_id,
            execution_id: claimed.execution_id(),
            paused_state,
            timeout,
            resources,
            envd_access_token,
            projection_ttl_secs,
        }))
    }

    /// The routing projection budget of the sandbox this plan starts.
    pub fn projection_ttl_secs(&self, now: SystemTime) -> u32 {
        match self {
            Self::Create(plan) => plan.metadata.projection_ttl_secs(now),
            Self::Resume(plan) => plan.projection_ttl_secs,
        }
    }

    pub fn sandbox_id(&self) -> SandboxId {
        match self {
            Self::Create(plan) => plan.sandbox_id,
            Self::Resume(plan) => plan.sandbox_id,
        }
    }

    /// The incarnation this launch runs under.
    pub fn execution_id(&self) -> ExecutionId {
        match self {
            Self::Create(plan) => plan.execution_id,
            Self::Resume(plan) => plan.execution_id,
        }
    }

    pub fn transitional_state(&self) -> SandboxState {
        match self {
            Self::Create(_) => SandboxState::Creating,
            Self::Resume(_) => SandboxState::Resuming,
        }
    }

    pub fn transitional_metadata(&self) -> Option<&SandboxMetadata> {
        match self {
            Self::Create(plan) => Some(&plan.metadata),
            Self::Resume(_) => None,
        }
    }

    pub fn timeout(&self) -> NewTimeout {
        match self {
            Self::Create(plan) => plan.timeout,
            Self::Resume(plan) => plan.timeout,
        }
    }

    pub fn resources(&self) -> SandboxResources {
        match self {
            Self::Create(plan) => plan.metadata.resources,
            Self::Resume(plan) => plan.resources,
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn the_test_only_claim_token_is_never_minted_in_production() {
        fn visit(dir: &std::path::Path, offenders: &mut Vec<String>) {
            for entry in std::fs::read_dir(dir).expect("src is readable") {
                let path = entry.expect("readable dir entry").path();
                if path.is_dir() {
                    // Test directories may use the test-only constructor.
                    if path.file_name().and_then(|name| name.to_str()) == Some("tests") {
                        continue;
                    }
                    visit(&path, offenders);
                    continue;
                }
                if path.extension().and_then(|ext| ext.to_str()) != Some("rs") {
                    continue;
                }
                if path.ends_with("orchestrator/launch_plan.rs") {
                    continue;
                }
                let source = std::fs::read_to_string(&path).expect("source is utf-8");
                if source.contains("minted_for_test") {
                    offenders.push(path.display().to_string());
                }
            }
        }

        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let mut offenders = Vec::new();
        visit(&root.join("src"), &mut offenders);
        visit(&root.join("crates"), &mut offenders);

        assert!(
            offenders.is_empty(),
            "the test-only claim token was minted inside the crate, which is a resume path that \
             never asked the cluster whether it may run: {offenders:?}"
        );
    }

    #[test]
    fn the_adopted_claim_token_is_only_taken_where_a_remote_claim_arrives() {
        const TOKEN: &str = "adopted_from_remote_claim";
        let allowed = std::path::Path::new("node_server").join("service.rs");

        fn visit(dir: &std::path::Path, token: &str, found: &mut Vec<std::path::PathBuf>) {
            for entry in std::fs::read_dir(dir).expect("src is readable") {
                let path = entry.expect("readable dir entry").path();
                if path.is_dir() {
                    // Test directories may use the test-only constructor.
                    if path.file_name().and_then(|name| name.to_str()) == Some("tests") {
                        continue;
                    }
                    visit(&path, token, found);
                    continue;
                }
                if path.extension().and_then(|ext| ext.to_str()) != Some("rs") {
                    continue;
                }
                if path.ends_with("orchestrator/launch_plan.rs") {
                    continue;
                }
                let source = std::fs::read_to_string(&path).expect("source is utf-8");
                if source.contains(token) {
                    found.push(path);
                }
            }
        }

        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let mut found = Vec::new();
        visit(&root.join("src"), TOKEN, &mut found);
        visit(&root.join("crates"), TOKEN, &mut found);

        assert_eq!(
            found.len(),
            1,
            "expected exactly the node service to take an adopted claim, found {found:?}"
        );
        assert!(
            found[0].ends_with(&allowed),
            "an adopted claim token is taken outside the node service, which is a resume that \
             started because something had an incarnation in hand rather than a licence: {found:?}"
        );
    }
}
