//! Object-safe orchestration facade over role-specific generic orchestrators.
//! A single macro defines both the trait and forwarding implementation.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::broadcast;

use crate::sandbox::{
    CustomExtensionParams, EnvdAccessToken, SandboxBackendFactory, SandboxNetworkPolicy,
};
use crate::types::{ExecutionId, SandboxId};

use super::launch_plan::ClaimedExecution;
use super::metrics::OrchestratorMetrics;
use super::paused_registry::PausedSandboxPublisher;
use super::persistence::{ClusterRegistration, SandboxPersister};
use super::proxy::ProxyLookupResult;
#[cfg(test)]
use super::proxy::ProxyTarget;
use super::service::Orchestrator;
use super::store::{MetadataStore, NewTimeout, SandboxListFilter, SandboxMetadata};
#[cfg(test)]
use super::types::SandboxState;
use super::types::{
    CreateSandboxRequest, ForkChildren, LiveSandbox, PauseOutcome, SandboxLifecycleEvent,
    SandboxRosterEntry, SnapshotCaptureResult,
};
use super::{Result, SandboxForkOutcome};

/// Declares the facade and forwarding implementation from one signature list.
/// Owned methods translate `&Arc<Self>` receivers to object-safe `Arc<Self>`.
macro_rules! orchestration_surface {
    (
        // Methods taking `self: &Arc<Self>`.
        owned {
            $(
                $(#[$owned_meta:meta])*
                fn $owned_name:ident ( $( $owned_arg:ident : $owned_ty:ty ),* $(,)? )
                    $( -> $owned_ret:ty )? ;
            )*
        }
        // Methods taking `&self`.
        borrowed {
            $(
                $(#[$borrowed_meta:meta])*
                fn $borrowed_name:ident ( $( $borrowed_arg:ident : $borrowed_ty:ty ),* $(,)? )
                    $( -> $borrowed_ret:ty )? ;
            )*
        }
        // Synchronous methods taking `&self`.
        sync {
            $(
                $(#[$sync_meta:meta])*
                fn $sync_name:ident ( $( $sync_arg:ident : $sync_ty:ty ),* $(,)? )
                    $( -> $sync_ret:ty )? ;
            )*
        }
    ) => {
        /// Object-safe orchestration surface used outside this module.
        #[async_trait]
        pub trait SandboxOrchestration: Send + Sync + 'static {
            $(
                $(#[$owned_meta])*
                async fn $owned_name(self: Arc<Self> $(, $owned_arg: $owned_ty )* )
                    $( -> $owned_ret )? ;
            )*
            $(
                $(#[$borrowed_meta])*
                async fn $borrowed_name(&self $(, $borrowed_arg: $borrowed_ty )* )
                    $( -> $borrowed_ret )? ;
            )*
            $(
                $(#[$sync_meta])*
                fn $sync_name(&self $(, $sync_arg: $sync_ty )* ) $( -> $sync_ret )? ;
            )*
        }

        #[async_trait]
        impl<S, F, P> SandboxOrchestration for Orchestrator<S, F, P>
        where
            S: MetadataStore + 'static,
            F: SandboxBackendFactory,
            P: SandboxPersister + 'static,
        {
            $(
                $(#[$owned_meta])*
                async fn $owned_name(self: Arc<Self> $(, $owned_arg: $owned_ty )* )
                    $( -> $owned_ret )?
                {
                    // Fully qualify the inherent method to avoid trait recursion.
                    Orchestrator::<S, F, P>::$owned_name(&self $(, $owned_arg )* ).await
                }
            )*
            $(
                $(#[$borrowed_meta])*
                async fn $borrowed_name(&self $(, $borrowed_arg: $borrowed_ty )* )
                    $( -> $borrowed_ret )?
                {
                    Orchestrator::<S, F, P>::$borrowed_name(self $(, $borrowed_arg )* ).await
                }
            )*
            $(
                $(#[$sync_meta])*
                fn $sync_name(&self $(, $sync_arg: $sync_ty )* ) $( -> $sync_ret )? {
                    Orchestrator::<S, F, P>::$sync_name(self $(, $sync_arg )* )
                }
            )*
        }
    };
}

orchestration_surface! {
    owned {
        /// Creates and starts a sandbox.
        fn create_sandbox(request: CreateSandboxRequest) -> Result<SandboxMetadata>;
        /// Rebuilds a sandbox that already has an identity elsewhere in the
        /// cluster, under that identity.
        fn restore_sandbox(
            sandbox_id: SandboxId,
            request: CreateSandboxRequest,
        ) -> Result<SandboxMetadata>;
        /// Forks into one result per requested child, preserving request order.
        fn fork_sandbox(
            source_sandbox_id: SandboxId,
            children: ForkChildren,
            new_timeout: NewTimeout,
        ) -> Result<Vec<SandboxForkOutcome>>;
        /// Tears a sandbox down and forgets it, cluster record included.
        fn delete_sandbox(sandbox_id: SandboxId) -> Result<()>;
        /// Tears down a local copy of a sandbox that is alive somewhere else,
        /// leaving the cluster's record of it alone.
        fn discard_superseded_sandbox(sandbox_id: SandboxId) -> Result<()>;
        /// Drops this node's paused record for a sandbox the cluster has moved
        /// past. Returns whether there was one.
        fn discard_local_paused_record(sandbox_id: SandboxId) -> Result<bool>;
        /// Pauses every running sandbox and stops accepting work.
        fn shutdown() -> Result<()>;
        /// Pauses one sandbox, publishing it to the cluster if publishing is
        /// wired up.
        fn pause_sandbox(sandbox_id: SandboxId) -> Result<SandboxMetadata>;
        /// Pauses one sandbox and hands the capture back for the caller to
        /// publish, instead of offering it to this process's own publisher.
        fn pause_sandbox_for_publication(sandbox_id: SandboxId) -> Result<PauseOutcome>;
        /// Brings a paused sandbox back, under a fresh execution.
        fn resume_sandbox(
            sandbox_id: SandboxId,
            timeout: NewTimeout,
            claimed: ClaimedExecution,
        ) -> Result<SandboxMetadata>;
        /// Captures a snapshot of a running sandbox and leaves it running.
        fn capture_snapshot(sandbox_id: SandboxId) -> Result<SnapshotCaptureResult>;
        /// Replaces a running sandbox's egress policy.
        fn replace_sandbox_network_policy(
            sandbox_id: SandboxId,
            network_policy: SandboxNetworkPolicy,
        ) -> Result<()>;
        /// Applies an extension-defined custom-parameter patch.
        fn patch_sandbox_custom_extension_params(
            sandbox_id: SandboxId,
            patch: serde_json::Map<String, serde_json::Value>,
        ) -> Result<Option<CustomExtensionParams>>;
        /// Applies already-approved custom extension parameters without invoking hooks.
        fn replace_sandbox_custom_extension_params(
            sandbox_id: SandboxId,
            params: Option<CustomExtensionParams>,
        ) -> Result<()>;
    }
    borrowed {
        /// One sandbox's metadata, or `None` when this orchestrator has no
        /// record of it.
        fn get_sandbox(sandbox_id: &SandboxId) -> Result<Option<SandboxMetadata>>;
        /// Every sandbox this orchestrator has a record of.
        fn list_sandboxes() -> Result<Vec<SandboxMetadata>>;
        /// The ids of every sandbox this orchestrator has a record of.
        fn list_sandbox_ids() -> Result<Vec<SandboxId>>;
        /// Lists locally live handles without applying ownership filtering.
        fn list_live_sandboxes() -> Result<Vec<LiveSandbox>>;
        /// The heartbeat roster: what this node claims to be holding.
        fn list_sandbox_roster() -> Result<Vec<SandboxRosterEntry>>;
        /// Sandbox metadata narrowed by a filter.
        fn list_sandboxes_filtered(filter: SandboxListFilter) -> Result<Vec<SandboxMetadata>>;
        /// The execution currently live for a sandbox, if one is.
        fn live_execution_id(sandbox_id: &SandboxId) -> Option<ExecutionId>;
        /// Where the local proxy should send traffic for a sandbox, or why it
        /// cannot.
        fn proxy_lookup_for(sandbox_id: &SandboxId) -> Result<ProxyLookupResult>;
        /// Extends a sandbox's lifetime.
        fn keep_alive_for(
            sandbox_id: SandboxId,
            timeout: Option<Duration>,
            allow_shorter: bool,
        ) -> Result<Option<SandboxMetadata>>;
        /// Where on this machine's disk a paused sandbox's capture was
        /// written, or `None` when this node holds no paused record for it.
        fn paused_artifact_root(sandbox_id: &SandboxId) -> Result<Option<PathBuf>>;
        /// Whether a paused record was ever announced to a cluster registry,
        /// and under which node identity.
        fn paused_record_cluster_registration(
            sandbox_id: SandboxId,
        ) -> Result<ClusterRegistration>;
        /// The real machine a paused sandbox will reopen on, when that is
        /// already knowable — before anything has tried to resume it.
        fn paused_origin_node_id(sandbox_id: &SandboxId) -> Option<String>;
        /// The real machine currently running a sandbox, straight from its
        /// live backend.
        fn sandbox_holding_node_id(sandbox_id: &SandboxId) -> Option<String>;
        /// Runtime counters and derived resource totals, sampled now.
        fn metrics_snapshot() -> Result<OrchestratorMetrics>;

        // Test-only facade seed helpers.
        #[cfg(test)]
        fn set_proxy_target_for_test(
            sandbox_id: SandboxId,
            target: ProxyTarget,
            state: SandboxState,
        );
        #[cfg(test)]
        fn set_metadata_state_for_test(sandbox_id: SandboxId, state: SandboxState) -> Result<()>;
        #[cfg(test)]
        fn set_auto_resume_for_test(
            sandbox_id: &SandboxId,
            auto_resume_enabled: bool,
        ) -> Result<()>;
        #[cfg(test)]
        fn set_secure_for_test(sandbox_id: &SandboxId, secure: bool) -> Result<()>;
        #[cfg(test)]
        fn set_max_lifetime_for_test(sandbox_id: &SandboxId, max_lifetime: Duration) -> Result<()>;
        #[cfg(test)]
        fn remove_proxy_route_for_test(sandbox_id: &SandboxId);
        #[cfg(test)]
        fn set_live_execution_for_test(
            sandbox_id: SandboxId,
            target: ProxyTarget,
            execution_id: ExecutionId,
        );
    }
    sync {
        /// The envd access token for a sandbox, when it has one.
        fn get_envd_access_token(metadata: &SandboxMetadata) -> Option<EnvdAccessToken>;
        /// Whether `candidate` is the envd access token for this sandbox.
        fn validate_envd_access_token(sandbox_id: SandboxId, candidate: &str) -> bool;
        /// Wires in cluster-wide pause bookkeeping. The first wiring wins.
        fn set_paused_publisher(publisher: Arc<dyn PausedSandboxPublisher>);
        /// Subscribes to sandbox lifecycle events. Best-effort and lossy.
        fn subscribe_sandbox_events() -> broadcast::Receiver<SandboxLifecycleEvent>;
        /// Whether this node is refusing new work.
        fn scheduling_disabled() -> bool;
        /// Sets whether this node refuses new work. Returns whether that
        /// changed anything.
        fn set_scheduling_disabled(disabled: bool) -> bool;
        /// When isolation last changed, in unix milliseconds.
        fn scheduling_disabled_changed_at_ms() -> Option<i64>;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orchestrator::OrchestratorError;

    #[tokio::test]
    async fn the_facade_answers_from_the_orchestrator_behind_it() {
        let concrete =
            Orchestrator::with_in_memory_store(crate::sandbox::mock::MockBackendFactory::new())
                .await;
        let orchestration: Arc<dyn SandboxOrchestration> = Arc::clone(&concrete) as _;

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
        let orchestration: Arc<dyn SandboxOrchestration> =
            Orchestrator::with_in_memory_store(crate::sandbox::mock::MockBackendFactory::new())
                .await;
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
        refuses(
            "resume_sandbox",
            unknown,
            Arc::clone(&orchestration)
                .resume_sandbox(
                    unknown,
                    NewTimeout::UseExisting,
                    ClaimedExecution::from_claim(ExecutionId::new()),
                )
                .await,
        );

        refuses(
            "discard_superseded_sandbox",
            unknown,
            Arc::clone(&orchestration)
                .discard_superseded_sandbox(unknown)
                .await,
        );

        assert!(
            !Arc::clone(&orchestration)
                .discard_local_paused_record(unknown)
                .await
                .expect("asking about a record this node never had is not an error"),
            "there was no local paused record to discard"
        );
    }
}
