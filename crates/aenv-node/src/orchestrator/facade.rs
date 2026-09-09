//! The forwarding implementations that put a real orchestrator behind both
//! orchestration surfaces, plus the surface only this half can answer.
//!
//! `aenv-core` owns [`SandboxOrchestration`]'s signature list and hands it to
//! an emitter; the emitter here writes the implementation that forwards to
//! [`Orchestrator`]. `NodeOrchestration` is written from a list of its own and
//! emitted twice, as the trait and as that same implementation.

use std::sync::Arc;

use aenv_core::orchestration_trait;
use aenv_core::sandbox::{
    CustomExtensionParams, EnvdAccessToken, SandboxBackendFactory, SandboxNetworkPolicy,
};
use aenv_core::sandbox_orchestration_surface;
use aenv_core::types::{ExecutionId, SandboxId};

use super::proxy::ProxyLookupResult;
#[cfg(any(test, feature = "test-support"))]
use super::proxy::ProxyTarget;
use super::service::Orchestrator;
use super::store::{MetadataStore, NewTimeout, SandboxListFilter, SandboxMetadata};
#[cfg(any(test, feature = "test-support"))]
use super::SandboxState;
use super::{
    CreateSandboxRequest, ForkChildren, LiveSandbox, OrchestratorMetrics, PauseOutcome,
    RestoredSandbox, Result, SandboxForkOutcome, SandboxLifecycleEvent, SandboxOrchestration,
    SandboxRosterEntry, SnapshotCaptureResult,
};

/// Writes the implementation forwarding an orchestration surface to
/// [`Orchestrator`]'s inherent methods.
macro_rules! orchestration_impl {
    (
        $(#[$trait_meta:meta])*
        trait $trait_name:ident $(: $supertrait:ident)? ;
        owned {
            $(
                $(#[$owned_meta:meta])*
                fn $owned_name:ident ( $( $owned_arg:ident : $owned_ty:ty ),* $(,)? )
                    $( -> $owned_ret:ty )? ;
            )*
        }
        borrowed {
            $(
                $(#[$borrowed_meta:meta])*
                fn $borrowed_name:ident ( $( $borrowed_arg:ident : $borrowed_ty:ty ),* $(,)? )
                    $( -> $borrowed_ret:ty )? ;
            )*
        }
        sync {
            $(
                $(#[$sync_meta:meta])*
                fn $sync_name:ident ( $( $sync_arg:ident : $sync_ty:ty ),* $(,)? )
                    $( -> $sync_ret:ty )? ;
            )*
        }
        seeded {
            $(
                $(#[$seeded_meta:meta])*
                fn $seeded_name:ident ( $( $seeded_arg:ident : $seeded_ty:ty ),* $(,)? );
            )*
        }
    ) => {
        #[async_trait::async_trait]
        impl<S, F> $trait_name for Orchestrator<S, F>
        where
            S: MetadataStore + 'static,
            F: SandboxBackendFactory,
        {
            $(
                $(#[$owned_meta])*
                async fn $owned_name(self: Arc<Self> $(, $owned_arg: $owned_ty )* )
                    $( -> $owned_ret )?
                {
                    // Fully qualify the inherent method to avoid trait recursion.
                    Orchestrator::<S, F>::$owned_name(&self $(, $owned_arg )* ).await
                }
            )*
            $(
                $(#[$borrowed_meta])*
                async fn $borrowed_name(&self $(, $borrowed_arg: $borrowed_ty )* )
                    $( -> $borrowed_ret )?
                {
                    Orchestrator::<S, F>::$borrowed_name(self $(, $borrowed_arg )* ).await
                }
            )*
            $(
                $(#[$sync_meta])*
                fn $sync_name(&self $(, $sync_arg: $sync_ty )* ) $( -> $sync_ret )? {
                    Orchestrator::<S, F>::$sync_name(self $(, $sync_arg )* )
                }
            )*
            $(
                $(#[$seeded_meta])*
                async fn $seeded_name(&self $(, $seeded_arg: $seeded_ty )* ) -> Result<()> {
                    Orchestrator::<S, F>::$seeded_name(self $(, $seeded_arg )* ).await
                }
            )*
        }
    };
}

/// The signature list of [`NodeOrchestration`], handed to `$emit`.
macro_rules! node_orchestration_surface {
    ($emit:ident) => {
        $emit! {
            /// What only a process that runs sandboxes can answer: the handle
            /// table, the proxy route table, and the launches in flight inside
            /// this process.
            trait NodeOrchestration: SandboxOrchestration;
            owned {
                /// Rebuilds a sandbox that already has an identity elsewhere in the
                /// cluster, under that identity.
                fn restore_sandbox(
                    sandbox_id: SandboxId,
                    request: CreateSandboxRequest,
                ) -> Result<SandboxMetadata>;
                /// Applies already-approved custom extension parameters without invoking hooks.
                fn replace_sandbox_custom_extension_params(
                    sandbox_id: SandboxId,
                    params: Option<CustomExtensionParams>,
                ) -> Result<()>;
            }
            borrowed {
                /// Every sandbox this orchestrator has a record of.
                fn list_sandboxes() -> Result<Vec<SandboxMetadata>>;
                /// The ids of every sandbox this orchestrator has a record of.
                fn list_sandbox_ids() -> Result<Vec<SandboxId>>;
                /// Lists locally live handles without applying ownership filtering.
                fn list_live_sandboxes() -> Result<Vec<LiveSandbox>>;
                /// What this process holds under one id: its live handle, or the
                /// record it kept. `None` is the id being free here.
                fn held_sandbox(sandbox_id: SandboxId) -> Result<Option<LiveSandbox>>;
                /// The execution currently live for a sandbox, if one is.
                fn live_execution_id(sandbox_id: &SandboxId) -> Option<ExecutionId>;
                /// Where the local proxy should send traffic for a sandbox, or why it
                /// cannot.
                fn proxy_lookup_for(sandbox_id: &SandboxId) -> Result<ProxyLookupResult>;

                // Test-only facade seed helpers.
                #[cfg(any(test, feature = "test-support"))]
                fn set_proxy_target_for_test(
                    sandbox_id: SandboxId,
                    target: ProxyTarget,
                    state: SandboxState,
                );
                #[cfg(any(test, feature = "test-support"))]
                fn remove_proxy_route_for_test(sandbox_id: &SandboxId);
                #[cfg(any(test, feature = "test-support"))]
                fn set_live_execution_for_test(
                    sandbox_id: SandboxId,
                    target: ProxyTarget,
                    execution_id: ExecutionId,
                );
            }
            sync {
                /// The incarnation of a launch this process is running under this id,
                /// before any handle or record names it.
                fn launch_in_flight(sandbox_id: SandboxId) -> Option<ExecutionId>;
            }
            seeded {}
        }
    };
}

sandbox_orchestration_surface!(orchestration_impl);
node_orchestration_surface!(orchestration_trait);
node_orchestration_surface!(orchestration_impl);

#[cfg(test)]
mod tests {
    use super::*;
    use aenv_core::orchestrator::OrchestratorError;
    use aenv_core::sandbox::mock::MockBackendFactory;

    #[tokio::test]
    async fn the_facade_answers_from_the_orchestrator_behind_it() {
        let concrete = Orchestrator::with_in_memory_store(MockBackendFactory::new()).await;
        let orchestration: Arc<dyn NodeOrchestration> = Arc::clone(&concrete) as _;

        assert!(!orchestration.scheduling_disabled());
        assert!(orchestration.set_scheduling_disabled(true));
        assert!(orchestration.scheduling_disabled());
        assert!(concrete.scheduling_disabled());
        assert!(orchestration.scheduling_disabled_changed_at_ms().is_some());

        assert!(!orchestration.set_scheduling_disabled(true));

        let seeded = SandboxId::new();
        orchestration
            .set_metadata_state_for_test(seeded, SandboxState::Running)
            .await
            .unwrap();

        let listed = orchestration.list_sandboxes().await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].id, seeded, "the list is about the seeded sandbox");
        assert_eq!(
            orchestration.list_sandbox_ids().await.unwrap(),
            vec![seeded]
        );
        assert_eq!(orchestration.list_sandbox_roster().await.unwrap().len(), 1);
        let metrics = orchestration.metrics_snapshot().await.unwrap();
        assert_eq!(metrics.running_sandbox_count, 1);
        assert_eq!(metrics.create_successes, 0);

        let unknown = SandboxId::new();
        assert!(orchestration.get_sandbox(&seeded).await.unwrap().is_some());
        assert!(orchestration.get_sandbox(&unknown).await.unwrap().is_none());
        assert!(orchestration.live_execution_id(&unknown).await.is_none());
        assert!(!orchestration.validate_envd_access_token(unknown, "nonsense"));
    }

    #[tokio::test]
    async fn every_by_arc_method_reaches_the_orchestrator_through_dyn() {
        let orchestration: Arc<dyn NodeOrchestration> =
            Orchestrator::with_in_memory_store(MockBackendFactory::new()).await;
        let unknown = SandboxId::new();

        fn refuses<T: std::fmt::Debug>(what: &str, unknown: SandboxId, result: Result<T>) {
            match result {
                Err(OrchestratorError::SandboxNotFound(id)) => {
                    assert_eq!(id, unknown, "{what} named the wrong sandbox")
                }
                other => panic!("{what} should not have found {unknown}; got {other:?}"),
            }
        }

        refuses(
            "delete_sandbox",
            unknown,
            Arc::clone(&orchestration).delete_sandbox(unknown).await,
        );
        refuses(
            "pause_sandbox",
            unknown,
            Arc::clone(&orchestration).pause_sandbox(unknown).await,
        );
        refuses(
            "capture_snapshot",
            unknown,
            Arc::clone(&orchestration).capture_snapshot(unknown).await,
        );
        refuses(
            "replace_sandbox_network_policy",
            unknown,
            Arc::clone(&orchestration)
                .replace_sandbox_network_policy(unknown, SandboxNetworkPolicy::default())
                .await,
        );
        refuses(
            "patch_sandbox_custom_extension_params",
            unknown,
            Arc::clone(&orchestration)
                .patch_sandbox_custom_extension_params(unknown, serde_json::Map::new())
                .await,
        );
        refuses(
            "replace_sandbox_custom_extension_params",
            unknown,
            Arc::clone(&orchestration)
                .replace_sandbox_custom_extension_params(unknown, None)
                .await,
        );
        refuses(
            "fork_sandbox",
            unknown,
            Arc::clone(&orchestration)
                .fork_sandbox(unknown, ForkChildren::Fresh(1), NewTimeout::UseExisting)
                .await,
        );
    }
}
