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
        /// Forks one running sandbox into running children.
        ///
        /// One outcome per requested child, in order. `children` decides both
        /// how many and — when the caller is the control plane — their
        /// identities and ownership; none of that is inherited from the source.
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
        /// The sandboxes this node is *running*, from its live handles rather
        /// than from its records.
        ///
        /// 🔴 Not filtered by ownership. Which sandboxes a caller may see is a
        /// property of the surface it is being served through, not of this
        /// list — see [`LiveSandbox::control_plane_config`].
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
    use crate::orchestrator::OrchestratorError;

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

        // 🔴 Seed a record before asking anything about the lists.
        //
        // This test used to assert that the four list-shaped answers were
        // empty, which they were — because nothing had happened yet. A
        // forwarding that answered every one of them with
        // `Default::default()` satisfies that perfectly, so the assertions
        // were about the shape of the return type and not about where the
        // answer came from. The node lane hit the same trap from the other
        // side: a handle-table read and a store read agree exactly when the
        // node is empty, so an empty node cannot tell them apart.
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
        // Seeding a record is not a create, and the counter knows the
        // difference — which is what makes the 1 above mean something.
        assert_eq!(metrics.create_successes, 0);

        // The contrast, so the answers above are about *this* sandbox rather
        // than about anything the orchestrator would say to any question.
        let unknown = SandboxId::new();
        assert!(orchestration.get_sandbox(&seeded).await.unwrap().is_some());
        assert!(orchestration.get_sandbox(&unknown).await.unwrap().is_none());
        assert!(orchestration.live_execution_id(&unknown).await.is_none());
        assert!(!orchestration.validate_envd_access_token(unknown, "nonsense"));
    }

    /// Every method the concrete orchestrator takes by `Arc`, driven once
    /// through `dyn`.
    ///
    /// 🔴 This exists because of what a control probe measured. Breaking the
    /// forwarding for this whole group of methods — create, pause, resume,
    /// delete, fork, snapshot — turns only *three* of the crate's twelve
    /// hundred unit tests red, and the orchestrator integration tests do not
    /// close the gap: they hold a concrete `Orchestrator` and never cross this
    /// layer at all. "The existing tests are green" is therefore not, by
    /// itself, evidence about these twelve methods.
    ///
    /// `create_sandbox`, `restore_sandbox` and `fork_sandbox` want a live
    /// sandbox to work from, and `shutdown` stops process-global runtime
    /// managers and would take the rest of the test binary with it. The rest
    /// answer here for a sandbox that does not exist, which is enough to show
    /// that each one arrives somewhere and comes back.
    #[tokio::test]
    async fn every_by_arc_method_reaches_the_orchestrator_through_dyn() {
        let orchestration: Arc<dyn SandboxOrchestration> =
            Orchestrator::with_in_memory_store().await;
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
                    // 🔴 The in-crate constructor, not the `pub` one meant for
                    // tests outside the crate: a guard in `launch_plan.rs`
                    // fails the build if that one is so much as named under
                    // `src/`, which is what keeps every production resume
                    // going through the resume arbitration. Using this one
                    // from a test adds no production path to a claim.
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

        // The one exception, and it is deliberate: asking whether there is a
        // local paused record to drop is a question, and "there was not one"
        // is an answer to it rather than a failure.
        assert!(
            !Arc::clone(&orchestration)
                .discard_local_paused_record(unknown)
                .await
                .expect("asking about a record this node never had is not an error"),
            "there was no local paused record to discard"
        );
    }
}
