use std::sync::Arc;

use super::store::{NewTimeout, SandboxMetadata};
use super::types::SandboxState;
use crate::sandbox::{
    EnvdAccessToken, FreshSandboxBuildSpec, PausedSandboxState, SandboxLaunchConfig,
};
use crate::snapshot::RunnableSnapshot;
use crate::types::{ExecutionId, SandboxId, SandboxResources};

/// A resume claim that has been granted, carrying the incarnation the claim
/// allocated for it.
///
/// 🔴 The field is private and there is no `Default` and no `Clone`, so the
/// only way to obtain one is [`ClaimedExecution::from_claim`] — which is called
/// on the resume-arbitration path and nowhere else — and one token starts
/// exactly one sandbox, because [`LaunchPlan::for_resume`] takes it by value.
///
/// This is what keeps "holding a `LaunchPlan` means exactly one new incarnation
/// was minted for it" a property of the type system after the mint point moved
/// out of `for_resume` and into the claim. A caller cannot hand `for_resume` a
/// stale incarnation, because it cannot build a value `for_resume` accepts
/// without going through a claim first.
#[derive(Debug)]
pub struct ClaimedExecution(ExecutionId);

impl ClaimedExecution {
    /// Mints the token for a resume decision that has just been made.
    ///
    /// 🔴 Call this from the resume arbitration and nowhere else. Both resume
    /// paths — the cluster claim, and the local decision taken when no registry
    /// has a say — go through that one point on purpose: a second mint site is
    /// a second way for a resume to start without anybody deciding it may.
    pub(crate) fn from_claim(execution_id: ExecutionId) -> Self {
        Self(execution_id)
    }

    /// A token for tests that drive [`Orchestrator::resume_sandbox`] directly.
    ///
    /// 🔴 Never callable from production code, and that is enforced rather
    /// than asked for: `the_test_only_claim_token_is_never_minted_in_production`
    /// below fails the build if the name appears anywhere in `src/` outside
    /// this file. It exists because integration tests live outside the crate
    /// and so cannot see [`from_claim`](Self::from_claim), which is
    /// `pub(crate)` precisely so that production code cannot mint a claim
    /// outside the arbitration.
    pub fn minted_for_test() -> Self {
        Self(ExecutionId::new())
    }

    /// The incarnation this claim allocated.
    ///
    /// 🔴 Read it to report the claim, never to re-mint: the value the registry
    /// wrote alongside `claimed_by_node_id` is the one `mark_running` has to
    /// quote, and a freshly minted one matches no predicate on the other side.
    pub fn execution_id(&self) -> ExecutionId {
        self.0
    }
}

pub(super) struct CreateLaunchPlan {
    pub sandbox_id: SandboxId,
    /// 🔴 Private: a struct literal with a private field cannot be written
    /// outside this module, so the three `for_*` constructors below are the
    /// only way to build a plan, and each of them accounts for exactly one
    /// incarnation.
    execution_id: ExecutionId,
    pub source: CreateLaunchSource,
    pub launch_config: SandboxLaunchConfig,
    pub metadata: SandboxMetadata,
    pub timeout: NewTimeout,
}

pub(super) enum CreateLaunchSource {
    Snapshot {
        snapshot: Box<RunnableSnapshot>,
    },
    Fresh {
        build_spec: Box<FreshSandboxBuildSpec>,
    },
}

pub(super) struct ResumeLaunchPlan {
    pub sandbox_id: SandboxId,
    /// Private for the same reason as on [`CreateLaunchPlan`]; the value comes
    /// from the [`ClaimedExecution`] this plan consumed.
    execution_id: ExecutionId,
    pub paused_state: Arc<dyn PausedSandboxState>,
    pub timeout: NewTimeout,
    pub resources: SandboxResources,
    pub envd_access_token: Option<EnvdAccessToken>,
}

pub(super) enum LaunchPlan {
    Create(Box<CreateLaunchPlan>),
    Resume(Box<ResumeLaunchPlan>),
}

impl LaunchPlan {
    /// Builds the plan for a create.
    ///
    /// # 🔴 `run_as`, and why it is not a second mint site
    ///
    /// `None` mints here, which is what every user-facing create does. `Some`
    /// runs under an incarnation the caller already minted, and there is
    /// exactly one caller that may do that: a node executing a create on behalf
    /// of the orchestrator that owns the sandbox. In the split, that
    /// orchestrator is a different process — but it is still *the* orchestrator,
    /// it minted the incarnation in this same constructor, and it has already
    /// written that value into its own record of the sandbox.
    ///
    /// The alternative is worse than it looks: if the node minted its own, the
    /// two records of one sandbox would name two different runs, and fencing —
    /// which compares exactly that value — would refuse writes from the sandbox
    /// that is actually running.
    pub(super) fn for_create_from_snapshot(
        sandbox_id: SandboxId,
        snapshot: Box<RunnableSnapshot>,
        launch_config: SandboxLaunchConfig,
        mut metadata: SandboxMetadata,
        timeout: NewTimeout,
        run_as: Option<ExecutionId>,
    ) -> Self {
        // Creating from a snapshot is a create, not a resume. The backend below
        // it boots through `LaunchMode::Resume`, which is why the decision is
        // taken on the plan variant here and never on the launch mode or the
        // hook kind further down.
        let execution_id = run_as.unwrap_or_else(ExecutionId::new);
        // Stamped onto the record here rather than at the call site so the two
        // cannot drift: the plan and the metadata it carries name one run.
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

    pub(super) fn for_create_fresh(
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

    /// Builds the plan for a resume that has already been granted.
    ///
    /// 🔴 Takes the claim by value and does not mint anything itself. The
    /// incarnation was allocated when the claim was taken, in the same write
    /// that named this node as the claimant, and the resume has to run under
    /// that one — minting a second one here would leave `mark_running` quoting
    /// a value the row never had.
    pub(super) fn for_resume(
        sandbox_id: SandboxId,
        claimed: ClaimedExecution,
        paused_state: Arc<dyn PausedSandboxState>,
        timeout: NewTimeout,
        resources: SandboxResources,
        envd_access_token: Option<EnvdAccessToken>,
    ) -> Self {
        Self::Resume(Box::new(ResumeLaunchPlan {
            sandbox_id,
            execution_id: claimed.execution_id(),
            paused_state,
            timeout,
            resources,
            envd_access_token,
        }))
    }

    pub(super) fn sandbox_id(&self) -> SandboxId {
        match self {
            Self::Create(plan) => plan.sandbox_id,
            Self::Resume(plan) => plan.sandbox_id,
        }
    }

    /// The incarnation this launch runs under.
    pub(super) fn execution_id(&self) -> ExecutionId {
        match self {
            Self::Create(plan) => plan.execution_id,
            Self::Resume(plan) => plan.execution_id,
        }
    }

    pub(super) fn transitional_state(&self) -> SandboxState {
        match self {
            Self::Create(_) => SandboxState::Creating,
            Self::Resume(_) => SandboxState::Resuming,
        }
    }

    pub(super) fn transitional_metadata(&self) -> Option<&SandboxMetadata> {
        match self {
            Self::Create(plan) => Some(&plan.metadata),
            Self::Resume(_) => None,
        }
    }

    pub(super) fn timeout(&self) -> NewTimeout {
        match self {
            Self::Create(plan) => plan.timeout,
            Self::Resume(plan) => plan.timeout,
        }
    }

    pub(super) fn resources(&self) -> SandboxResources {
        match self {
            Self::Create(plan) => plan.metadata.resources,
            Self::Resume(plan) => plan.resources,
        }
    }
}

#[cfg(test)]
mod tests {
    /// 🔴 Guards the one hole in the claim token's story.
    ///
    /// `ClaimedExecution::from_claim` is `pub(crate)`, so nothing outside this
    /// crate can mint a claim — but `minted_for_test` is `pub`, because
    /// integration tests are outside the crate. This walks `src/` and fails if
    /// that constructor is ever named anywhere but here, which is what keeps
    /// "every production resume goes through the arbitration" a fact rather
    /// than a habit.
    #[test]
    fn the_test_only_claim_token_is_never_minted_in_production() {
        fn visit(dir: &std::path::Path, offenders: &mut Vec<String>) {
            for entry in std::fs::read_dir(dir).expect("src is readable") {
                let path = entry.expect("readable dir entry").path();
                if path.is_dir() {
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

        let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut offenders = Vec::new();
        visit(&src, &mut offenders);

        assert!(
            offenders.is_empty(),
            "the test-only claim token was minted inside the crate, which is a resume path that \
             never asked the cluster whether it may run: {offenders:?}"
        );
    }
}
