//! The orchestrator surface everything outside `src/orchestrator/` uses.
//!
//! # Why this exists
//!
//! [`Orchestrator`] is generic over three parameters — the metadata store, the
//! backend factory and the paused-sandbox persister — and the split gives the
//! two halves *different* instantiations of it: the node keeps the in-memory
//! store, the Firecracker factory and the file-backed persister, while the API
//! half gets a cluster store, a factory that drives sandboxes over the wire, and
//! no persister at all. Two roles, two concrete types, one `ApiImpl`.
//!
//! 🔴 **The obvious fix does not compile.** [`MetadataStore`] and
//! [`SandboxPersister`] both have generic methods —
//! `MetadataStore::update_if_state<F>`, `MetadataStore::list_with_callback<F>`,
//! `SandboxPersister::load_all<F>` — which makes both traits object-unsafe, so
//! `Box<dyn MetadataStore>` and `Box<dyn SandboxPersister>` are not types that
//! exist. Only `F` could be boxed. That rules out selecting the backend one
//! type parameter at a time and leaves exactly one place where the two
//! assemblies can meet: above all three of them, here.
//!
//! The alternative — making `ApiImpl` generic over the same three parameters —
//! spreads them through every `impl apis::*` block, every free function in
//! `src/api/proxy.rs`, the router's trait bounds and the observability service,
//! and monomorphises all of it twice. This trait costs one vtable jump and one
//! boxed future per call, against operations whose *cheapest* member takes a
//! lock and whose most expensive boots a virtual machine.
//!
//! # Keeping it honest
//!
//! Both the declaration and the forwarding body are generated from a single
//! list by [`orchestration_surface!`], so a method's name is written once. That
//! is deliberate: hand-written forwarding is thirty chances to route
//! `pause_sandbox` into `delete_sandbox`, and nothing about the resulting code
//! would look wrong. Adding a method to `Orchestrator` that callers outside the
//! module need means adding one line to the list below.

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
    CreateSandboxRequest, SandboxLifecycleEvent, SandboxRosterEntry, SnapshotCaptureResult,
};
use super::{Result, SandboxForkOutcome};

/// Declares [`SandboxOrchestration`] and the blanket forwarding impl from one
/// list of signatures.
///
/// The list is split by receiver, because the receiver is the one thing that
/// cannot be inferred from a signature: the methods in `owned` are the ones
/// `Orchestrator` declares as `self: &Arc<Self>` — they hand a clone of the
/// orchestrator to a spawned task — and a trait cannot dispatch dynamically on
/// `&Arc<Self>`. They become `self: Arc<Self>`, which it can.
macro_rules! orchestration_surface {
    (
        // `async fn (self: &Arc<Self>)` on `Orchestrator`.
        owned {
            $(
                $(#[$owned_meta:meta])*
                fn $owned_name:ident ( $( $owned_arg:ident : $owned_ty:ty ),* $(,)? )
                    $( -> $owned_ret:ty )? ;
            )*
        }
        // `async fn (&self)` on `Orchestrator`.
        borrowed {
            $(
                $(#[$borrowed_meta:meta])*
                fn $borrowed_name:ident ( $( $borrowed_arg:ident : $borrowed_ty:ty ),* $(,)? )
                    $( -> $borrowed_ret:ty )? ;
            )*
        }
        // `fn (&self)` on `Orchestrator`.
        sync {
            $(
                $(#[$sync_meta:meta])*
                fn $sync_name:ident ( $( $sync_arg:ident : $sync_ty:ty ),* $(,)? )
                    $( -> $sync_ret:ty )? ;
            )*
        }
    ) => {
        /// Everything outside `src/orchestrator/` may ask of the orchestrator.
        ///
        /// 🔴 Object-safe on purpose; see the module documentation for what
        /// stops the type parameters from being swapped one at a time.
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
                    // Fully qualified, and to the inherent method: `Self::` here
                    // would be free to resolve back into this trait, and the
                    // failure that produces is an unbounded recursion rather
                    // than a compile error.
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
        /// Forks one running sandbox into `count` running children.
        ///
        /// One outcome per requested child, in order.
        fn fork_sandbox(
            source_sandbox_id: SandboxId,
            count: u32,
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
        /// Applies an extension-defined patch to a sandbox's custom extension
        /// parameters.
        fn patch_sandbox_custom_extension_params(
            sandbox_id: SandboxId,
            patch: serde_json::Map<String, serde_json::Value>,
        ) -> Result<Option<CustomExtensionParams>>;
    }
    borrowed {
        /// One sandbox's metadata, or `None` when this orchestrator has no
        /// record of it.
        fn get_sandbox(sandbox_id: &SandboxId) -> Result<Option<SandboxMetadata>>;
        /// Every sandbox this orchestrator has a record of.
        fn list_sandboxes() -> Result<Vec<SandboxMetadata>>;
        /// The ids of every sandbox this orchestrator has a record of.
        fn list_sandbox_ids() -> Result<Vec<SandboxId>>;
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
        /// Whether a paused record was ever announced to a cluster registry,
        /// and under which node identity.
        fn paused_record_cluster_registration(
            sandbox_id: SandboxId,
        ) -> Result<ClusterRegistration>;
        /// Runtime counters and derived resource totals, sampled now.
        fn metrics_snapshot() -> Result<OrchestratorMetrics>;

        // The seeding helpers the data-plane tests drive the proxy with. They
        // are on the facade rather than reached around it because the tests
        // hold an `ApiImpl`, and an `ApiImpl` no longer knows which concrete
        // orchestrator is behind it — which is the point of the facade.
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

    /// Drives the facade through `Arc<dyn SandboxOrchestration>` rather than
    /// asserting that it compiles.
    ///
    /// 🔴 Two failures this rules out, both of which type-check: forwarding a
    /// method into a different one, and forwarding it into *itself* — the
    /// blanket impl calls `Orchestrator::<S, F, P>::name`, and an inherent
    /// method that stopped shadowing the trait one would recurse until the
    /// stack ran out.
    #[tokio::test]
    async fn the_facade_answers_from_the_orchestrator_behind_it() {
        let concrete = Orchestrator::with_in_memory_store().await;
        let orchestration: Arc<dyn SandboxOrchestration> = Arc::clone(&concrete) as _;

        assert!(!orchestration.scheduling_disabled());
        assert!(orchestration.set_scheduling_disabled(true));
        // Read back through the facade, and through the concrete type: both
        // have to see the write, or the trait is talking to something else.
        assert!(orchestration.scheduling_disabled());
        assert!(concrete.scheduling_disabled());
        assert!(orchestration.scheduling_disabled_changed_at_ms().is_some());

        // A second write of the same value changes nothing, which is how the
        // shutdown path tells "I isolated the node" from "it already was".
        assert!(!orchestration.set_scheduling_disabled(true));

        assert!(orchestration.list_sandboxes().await.unwrap().is_empty());
        assert!(orchestration.list_sandbox_ids().await.unwrap().is_empty());
        assert!(orchestration
            .list_sandbox_roster()
            .await
            .unwrap()
            .is_empty());
        let metrics = orchestration.metrics_snapshot().await.unwrap();
        assert_eq!(metrics.running_sandbox_count, 0);
        assert_eq!(metrics.create_successes, 0);

        let unknown = SandboxId::new();
        assert!(orchestration.get_sandbox(&unknown).await.unwrap().is_none());
        assert!(orchestration.live_execution_id(&unknown).await.is_none());
        assert!(!orchestration.validate_envd_access_token(unknown, "nonsense"));
    }
}
