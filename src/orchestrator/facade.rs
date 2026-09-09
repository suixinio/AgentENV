//! The object-safe orchestration surface both halves answer.
//!
//! The trait is declared here and implemented in the crate that owns each
//! half's orchestrator, so its signature list is a macro rather than a written
//! trait body: [`sandbox_orchestration_surface`] hands the list to an emitter
//! and [`orchestration_trait`] is the emitter that writes the trait. The half
//! that forwards to a concrete `Orchestrator` passes a second emitter over the
//! same list; the half whose control path is the implementation writes it out,
//! and a signature it does not answer is a missing method rather than a drift.

/// Writes a trait from an orchestration surface's signature list.
///
/// Owned methods take `Arc<Self>` so the surface stays object-safe.
#[macro_export]
macro_rules! orchestration_trait {
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
        // Test seeds. The trait body refuses, so an implementor in another
        // crate -- which cannot read this crate's `test-support` feature to
        // gate an override on -- still satisfies the trait when it keeps none.
        seeded {
            $(
                $(#[$seeded_meta:meta])*
                fn $seeded_name:ident ( $( $seeded_arg:ident : $seeded_ty:ty ),* $(,)? );
            )*
        }
    ) => {
        $(#[$trait_meta])*
        #[::async_trait::async_trait]
        pub trait $trait_name: $( $supertrait + )? Send + Sync + 'static {
            $(
                $(#[$owned_meta])*
                async fn $owned_name(self: std::sync::Arc<Self> $(, $owned_arg: $owned_ty )* )
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
            $(
                $(#[$seeded_meta])*
                async fn $seeded_name(&self $(, $seeded_arg: $seeded_ty )* )
                    -> $crate::orchestrator::Result<()>
                {
                    $( let _ = $seeded_arg; )*
                    Err($crate::orchestrator::OrchestratorError::InternalError(format!(
                        "{} keeps no test seed",
                        stringify!($seeded_name)
                    )))
                }
            )*
        }
    };
}

/// The signature list of [`SandboxOrchestration`], handed to `$emit`.
///
/// The half that owns a concrete orchestrator invokes this with an emitter of
/// its own to write the implementation that forwards to it.
#[macro_export]
macro_rules! sandbox_orchestration_surface {
    ($emit:ident) => {
        $emit! {
            /// What the REST layer and the observability reporter ask of
            /// whichever half they run in. Both halves answer all of it.
            trait SandboxOrchestration;
            owned {
                /// Creates and starts a sandbox.
                fn create_sandbox(request: CreateSandboxRequest) -> Result<SandboxMetadata>;
                /// Rebuilds a sandbox that already has an identity elsewhere in the
                /// cluster under that identity, waiting out a launch of the same id
                /// already in flight rather than starting a second one.
                fn restore_or_join_launch(
                    sandbox_id: SandboxId,
                    request: CreateSandboxRequest,
                ) -> Result<RestoredSandbox>;
                /// Forks into one result per requested child, preserving request order.
                fn fork_sandbox(
                    source_sandbox_id: SandboxId,
                    children: ForkChildren,
                    new_timeout: NewTimeout,
                ) -> Result<Vec<SandboxForkOutcome>>;
                /// Tears a sandbox down and forgets it.
                fn delete_sandbox(sandbox_id: SandboxId) -> Result<()>;
                /// Stops every sandbox and stops accepting work.
                fn shutdown() -> Result<()>;
                /// Pauses one sandbox: publishes its capture and forgets the record.
                fn pause_sandbox(sandbox_id: SandboxId) -> Result<PauseOutcome>;
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
            }
            borrowed {
                /// One sandbox's metadata, or `None` when this orchestrator has no
                /// record of it.
                fn get_sandbox(sandbox_id: &SandboxId) -> Result<Option<SandboxMetadata>>;
                /// Waits for a pause in flight to settle: `None` once the record is
                /// gone, the record itself if the pause did not finish.
                fn wait_for_pause_to_settle(sandbox_id: SandboxId) -> Result<Option<SandboxMetadata>>;
                /// Sandbox metadata narrowed by a filter.
                fn list_sandboxes_filtered(filter: SandboxListFilter) -> Result<Vec<SandboxMetadata>>;
                /// The heartbeat roster: what this node claims to be holding.
                fn list_sandbox_roster() -> Result<Vec<SandboxRosterEntry>>;
                /// Extends a sandbox's lifetime.
                fn keep_alive_for(
                    sandbox_id: SandboxId,
                    timeout: Option<std::time::Duration>,
                    allow_shorter: bool,
                ) -> Result<Option<SandboxMetadata>>;
                /// The real machine currently running a sandbox, straight from its
                /// live backend.
                fn sandbox_holding_node_id(sandbox_id: &SandboxId) -> Option<String>;
                /// Runtime counters and derived resource totals, sampled now.
                fn metrics_snapshot() -> Result<OrchestratorMetrics>;
            }
            sync {
                /// The envd access token for a sandbox, when it has one.
                fn get_envd_access_token(metadata: &SandboxMetadata) -> Option<EnvdAccessToken>;
                /// Whether `candidate` is the envd access token for this sandbox.
                fn validate_envd_access_token(sandbox_id: SandboxId, candidate: &str) -> bool;
                /// Subscribes to sandbox lifecycle events. Best-effort and lossy.
                fn subscribe_sandbox_events()
                    -> tokio::sync::broadcast::Receiver<SandboxLifecycleEvent>;
                /// Whether this node is refusing new work.
                fn scheduling_disabled() -> bool;
                /// Sets whether this node refuses new work. Returns whether that
                /// changed anything.
                fn set_scheduling_disabled(disabled: bool) -> bool;
                /// When isolation last changed, in unix milliseconds.
                fn scheduling_disabled_changed_at_ms() -> Option<i64>;
            }
            seeded {
                #[cfg(any(test, feature = "test-support"))]
                fn set_metadata_state_for_test(sandbox_id: SandboxId, state: SandboxState);
                #[cfg(any(test, feature = "test-support"))]
                fn remove_sandbox_for_test(sandbox_id: &SandboxId);
                #[cfg(any(test, feature = "test-support"))]
                fn set_auto_resume_for_test(sandbox_id: &SandboxId, auto_resume_enabled: bool);
            }
        }
    };
}

use crate::sandbox::{CustomExtensionParams, EnvdAccessToken, SandboxNetworkPolicy};
use crate::types::SandboxId;

use super::launch::RestoredSandbox;
use super::metrics::OrchestratorMetrics;
use super::store::{NewTimeout, SandboxListFilter, SandboxMetadata};
#[cfg(any(test, feature = "test-support"))]
use super::types::SandboxState;
use super::types::{
    CreateSandboxRequest, ForkChildren, PauseOutcome, SandboxLifecycleEvent, SandboxRosterEntry,
    SnapshotCaptureResult,
};
use super::{Result, SandboxForkOutcome};

sandbox_orchestration_surface!(orchestration_trait);
