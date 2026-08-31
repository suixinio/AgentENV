//! Cluster-visible paused-sandbox registry.
//! It stores durable snapshot references and metadata, never artifacts; the
//! disabled implementation preserves node-local pause/resume behavior.

pub mod disabled;
pub mod types;

use std::sync::Arc;
use std::time::SystemTime;

use anyhow::Context;
use async_trait::async_trait;
use tracing::{debug, error, info, warn};

use crate::cfg::{PausedRegistryBackendKind, PausedRegistryConfig};
use crate::identity::NodeIdentity;
use crate::node_registry::registry::NodeRegistry;
use crate::orchestrator::PauseOutcome;
use crate::snapshot::SnapshotId;
use crate::types::{ExecutionId, SandboxId};

pub use disabled::DisabledPausedSandboxRegistry;
pub use types::{
    BeganPause, ConflictReason, DeadlineRenewalOutcome, HeldSandbox, MarkRunningOutcome,
    PausedRegistryListEntry, PausedRegistryListing, PausedRegistryRows, PausedRegistryState,
    PausedSandboxEntry, ReclaimedHoldings, ReleasedHoldings, ResumeClaim,
};

pub type RegistryResult<T> = std::result::Result<T, PausedRegistryError>;

#[derive(thiserror::Error, Debug)]
pub enum PausedRegistryError {
    #[error("paused sandbox registry backend failed during {operation}: {source}")]
    Backend {
        operation: &'static str,
        #[source]
        source: anyhow::Error,
    },
    #[error("paused sandbox registry holds an unreadable record for '{sandbox_id}': {reason}")]
    InvalidRecord {
        sandbox_id: String,
        reason: String,
        #[source]
        source: Option<anyhow::Error>,
    },
    /// The caller's generation is stale; re-read before deciding.
    #[error("paused sandbox '{sandbox_id}' moved on from generation {expected}")]
    GenerationConflict { sandbox_id: String, expected: i64 },
    /// The writing execution has been superseded and must never be retried.
    #[error("paused sandbox '{sandbox_id}' has been taken over by a newer incarnation")]
    ExecutionFenced { sandbox_id: String },
}

impl PausedRegistryError {
    pub fn backend(operation: &'static str, source: impl Into<anyhow::Error>) -> Self {
        Self::Backend {
            operation,
            source: source.into(),
        }
    }
}

/// Concurrent cluster registry fenced by row generation and execution id.
#[async_trait]
pub trait PausedSandboxRegistry: Send + Sync {
    /// Begins publication and returns the generation plus superseded snapshot.
    async fn begin_pause(&self, entry: &PausedSandboxEntry) -> RegistryResult<BeganPause>;

    /// Marks the snapshot durable, making the sandbox resumable on any node.
    async fn complete_pause(
        &self,
        sandbox_id: &SandboxId,
        generation: i64,
        snapshot_id: &SnapshotId,
    ) -> RegistryResult<()>;

    /// Keeps an unpublished pause pinned to its origin node.
    async fn mark_local_only(&self, sandbox_id: &SandboxId, generation: i64) -> RegistryResult<()>;

    /// Reads a row without taking ownership.
    async fn get(&self, sandbox_id: &SandboxId) -> RegistryResult<Option<PausedSandboxEntry>>;

    /// Reads rows in a batch and reports authoritative coverage.
    /// Destructive callers may act on absence only for covered ids.
    async fn get_many(&self, sandbox_ids: &[SandboxId]) -> RegistryResult<PausedRegistryRows>;

    /// Claims a paused sandbox under the execution id the resume must use.
    async fn claim_for_resume(
        &self,
        sandbox_id: &SandboxId,
        node_id: &str,
        execution_id: ExecutionId,
    ) -> RegistryResult<ResumeClaim>;

    /// Releases a failed resume claim; `false` means the row moved on.
    async fn release_claim(&self, sandbox_id: &SandboxId, generation: i64) -> RegistryResult<bool>;

    /// Renews leases and current sandbox deadlines only for rows held by `node_id`.
    async fn renew_lease(&self, node_id: &str, held: &[HeldSandbox]) -> RegistryResult<u64>;

    /// Reclaims rows only after both holder lease and sandbox deadline expire.
    async fn reclaim_expired_holdings(&self) -> RegistryResult<ReclaimedHoldings>;

    /// Marks a claimed sandbox running.
    /// `node_id` is the claim CAS identity; `holder_node_id` is the real machine.
    /// `execution_id` must be the incarnation allocated by the claim.
    async fn mark_running(
        &self,
        sandbox_id: &SandboxId,
        node_id: &str,
        holder_node_id: &str,
        execution_id: ExecutionId,
        expires_at: Option<SystemTime>,
    ) -> RegistryResult<MarkRunningOutcome>;

    /// Updates only the deadline of a `Running` row matching `execution_id`.
    /// `Superseded` is terminal for that deadline update.
    async fn renew_sandbox_deadline(
        &self,
        sandbox_id: &SandboxId,
        execution_id: ExecutionId,
        expires_at: Option<SystemTime>,
    ) -> RegistryResult<DeadlineRenewalOutcome>;

    /// Releases holdings from a previous process on this node.
    /// Call only during startup before the new process can hold sandboxes.
    async fn release_node_holdings(&self, node_id: &str) -> RegistryResult<ReleasedHoldings>;

    /// Removes a row only at the observed generation.
    async fn remove(&self, sandbox_id: &SandboxId, generation: i64) -> RegistryResult<bool>;

    /// Lists the scoped registry with the database clock used for lease judgments.
    /// Callers must first require a cluster-backed registry.
    async fn list_all(&self) -> RegistryResult<PausedRegistryListing>;

    /// Whether this backend authoritatively tracks cluster-wide state.
    fn is_cluster_backed(&self) -> bool {
        false
    }
}

/// Best-effort hook from local lifecycle operations into cluster pause bookkeeping.
#[async_trait]
pub trait PausedSandboxPublisher: Send + Sync {
    /// Publishes a paused capture and returns the node identity recorded.
    async fn publish_paused(&self, outcome: PauseOutcome) -> Option<String>;

    /// Whether the publisher will attempt to commit a publishable capture.
    fn wants_publishable_capture(&self) -> bool;

    /// Records a resumed sandbox on its real holding node with its current deadline.
    async fn mark_running(
        &self,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
        expires_at: Option<SystemTime>,
        holding_node_id: Option<String>,
    );

    /// Mirrors an already-clamped running deadline into the cluster registry.
    async fn renew_deadline(
        &self,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
        expires_at: Option<SystemTime>,
    );

    /// Removes the registry row and snapshot after permanent sandbox deletion.
    async fn forget(&self, sandbox_id: SandboxId, holding_node_id: Option<String>);
}

/// Logs whether a granted claim restored a durable or rewound snapshot.
pub fn log_claim_outcome(
    sandbox_id: &SandboxId,
    node_id: &str,
    entry: &PausedSandboxEntry,
    previous_state: PausedRegistryState,
) {
    match previous_state {
        PausedRegistryState::Paused => debug!(
            %sandbox_id,
            node_id,
            generation = entry.generation,
            claim_outcome = "durable",
            "claimed a sandbox from its published snapshot"
        ),
        PausedRegistryState::Publishing | PausedRegistryState::LocalOnly => warn!(
            %sandbox_id,
            node_id,
            claim_outcome = "rewound",
            previous_state = ?previous_state,
            previous_holder = %entry.origin_node_id,
            "took over a sandbox parked on a node that stopped renewing its lease; \
             restoring from the last snapshot that reached the repository, so any \
             work since that snapshot is lost"
        ),
        // Claim predicates must never admit a live state.
        live => error!(
            %sandbox_id,
            node_id,
            claim_outcome = "invariant_violation",
            previous_state = ?live,
            previous_holder = %entry.origin_node_id,
            "claimed a sandbox that was not parked; a live sandbox may now exist twice"
        ),
    }
}

/// Builds the PostgreSQL registry in the component that owns its pool.
#[async_trait(?Send)]
pub trait PostgresPausedRegistryFactory: Send + Sync {
    /// Migrates schema, enters restart grace, then returns the registry.
    async fn build(
        &self,
        identity: &NodeIdentity,
        lease_ttl: std::time::Duration,
    ) -> anyhow::Result<Arc<dyn PausedSandboxRegistry>>;
}

/// Builds the configured registry; missing dependencies are startup errors.
pub async fn build_paused_registry(
    config: &PausedRegistryConfig,
    identity: &NodeIdentity,
    postgres: Option<&dyn PostgresPausedRegistryFactory>,
    node_registry: Option<Arc<dyn NodeRegistry>>,
) -> anyhow::Result<Arc<dyn PausedSandboxRegistry>> {
    let registry: Arc<dyn PausedSandboxRegistry> = match config.backend {
        PausedRegistryBackendKind::Local => Arc::new(DisabledPausedSandboxRegistry),
        PausedRegistryBackendKind::Postgres => {
            let factory = postgres.context(
                "paused_registry.backend = \"postgres\" requires [pg].dsn to be configured \
                 (the shared PostgreSQL pool this process already builds for the snapshot \
                 catalog, if [pg] is set)",
            )?;

            // PostgreSQL requires a roster source for running-row lease renewal.
            node_registry.as_ref().context(
                "paused_registry.backend = \"postgres\" requires a heartbeat roster \
                 source (aenv-api's own node registry, which assemble_api always builds) \
                 -- without it, running sandboxes' registry leases have no renewal path \
                 and will eventually be wrongly reclaimed even while healthy",
            )?;

            // Background loops consume the validated roster after assembly.
            drop(node_registry);

            let lease_ttl = std::time::Duration::from_secs(config.lease_ttl_secs());

            // Schema migration and restart grace complete before return.
            factory.build(identity, lease_ttl).await?
        }
    };

    // Report the selected backend once after successful assembly.
    info!(
        backend = config.backend.as_str(),
        cluster_id = %identity.cluster_id,
        lease_ttl_secs = config.lease_ttl_secs(),
        "paused sandbox registry ready"
    );

    Ok(registry)
}

#[cfg(test)]
mod claim_outcome_tests {
    use super::*;
    use crate::logging::capture::Recorder;
    use crate::orchestrator::store::SandboxMetadata;

    fn entry(state: PausedRegistryState) -> PausedSandboxEntry {
        PausedSandboxEntry {
            sandbox_id: SandboxId::new(),
            cluster_id: uuid::Uuid::nil(),
            state,
            generation: 3,
            origin_node_id: "node-b".to_string(),
            claimed_by_node_id: None,
            snapshot_id: Some(SnapshotId::generate()),
            metadata: Some(SandboxMetadata::default()),
            execution_id: Some(ExecutionId::new()),
            paused_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        }
    }

    #[test]
    fn a_claim_that_cost_somebody_their_last_pause_is_a_warning() {
        for state in [
            PausedRegistryState::Publishing,
            PausedRegistryState::LocalOnly,
        ] {
            let recorder = Recorder::default();
            let guard = recorder.install();
            let entry = entry(state);

            log_claim_outcome(&entry.sandbox_id, "node-a", &entry, state);
            drop(guard);

            assert!(
                recorder.saw(tracing::Level::WARN, "work since that snapshot is lost"),
                "{state:?} must warn: {:?}",
                recorder.events()
            );
        }
    }

    #[test]
    fn an_ordinary_claim_is_not_a_warning() {
        let recorder = Recorder::default();
        let guard = recorder.install();
        let entry = entry(PausedRegistryState::Paused);

        log_claim_outcome(
            &entry.sandbox_id,
            "node-a",
            &entry,
            PausedRegistryState::Paused,
        );
        drop(guard);

        assert!(recorder.saw(tracing::Level::DEBUG, "durable"));
        assert!(!recorder.saw(tracing::Level::WARN, "lost"));
    }

    #[test]
    fn claiming_a_sandbox_that_was_not_parked_is_an_error() {
        let recorder = Recorder::default();
        let guard = recorder.install();
        let entry = entry(PausedRegistryState::Running);

        log_claim_outcome(
            &entry.sandbox_id,
            "node-a",
            &entry,
            PausedRegistryState::Running,
        );
        drop(guard);

        assert!(recorder.saw(tracing::Level::ERROR, "may now exist twice"));
    }
}

#[cfg(test)]
mod build_tests {
    use super::*;
    use crate::cfg::PausedRegistryConfig;
    use crate::logging::capture::Recorder;

    struct NoopNodeRegistry;
    impl NodeRegistry for NoopNodeRegistry {
        fn snapshot(&self, _allow_lingering: bool) -> Vec<crate::node_registry::types::Node> {
            Vec::new()
        }
        fn contains(&self, _node: &crate::node_registry::types::Node) -> bool {
            false
        }
        fn resolve(&self, _node_id: &str) -> Option<crate::node_registry::types::Node> {
            None
        }
        fn heartbeat(
            &self,
            _req: &crate::proto::scheduler::HeartbeatRequest,
            _now: SystemTime,
        ) -> Result<
            (crate::node_registry::types::Node, String),
            crate::node_registry::registry::NodeNotInRegistry,
        > {
            Err(crate::node_registry::registry::NodeNotInRegistry)
        }
        fn list_observed(
            &self,
            _cluster_id: &str,
            _now: SystemTime,
        ) -> Vec<crate::proto::scheduler::ObservedNode> {
            Vec::new()
        }
        fn list_p2p_peers(
            &self,
            _cluster_id: &str,
            _backend: &str,
            _exclude_node_id: &str,
            _now: SystemTime,
        ) -> Vec<crate::proto::scheduler::P2pPeer> {
            Vec::new()
        }
        fn filter_p2p_peers(
            &self,
            _cluster_id: &str,
            _backend: &str,
            _node_ids: &[String],
            _exclude_node_id: &str,
            _now: SystemTime,
        ) -> Vec<crate::proto::scheduler::P2pPeer> {
            Vec::new()
        }
        fn get_observed(
            &self,
            _node_id: &str,
            _cluster_id: &str,
            _now: SystemTime,
        ) -> Option<crate::proto::scheduler::ObservedNode> {
            None
        }
        fn peek_observed(&self, _node_id: &str) -> Option<crate::proto::scheduler::NodeSnapshot> {
            None
        }
        fn peek_observed_with_freshness(
            &self,
            _node_id: &str,
            _now: SystemTime,
        ) -> Option<(
            crate::proto::scheduler::NodeSnapshot,
            crate::node_registry::placement::score::SnapshotFreshness,
        )> {
            None
        }
        fn roster_of(
            &self,
            _node_id: &str,
        ) -> Option<(Vec<crate::node_registry::types::RosterEntry>, SystemTime)> {
            None
        }
        fn nodes_holding(&self, _sandbox_id: &str) -> Vec<String> {
            Vec::new()
        }
        fn rosters_in_cluster(
            &self,
            _cluster_id: &str,
        ) -> Vec<crate::node_registry::types::Roster> {
            Vec::new()
        }
        fn unregister_observed(
            &self,
            _node_id: &str,
            _service_instance_id: &str,
        ) -> Result<(), crate::node_registry::registry::ServiceInstanceMismatch> {
            Ok(())
        }
        fn applied_cpu_intersection(&self, _cluster_id: &str) -> Option<String> {
            None
        }
    }

    fn config(backend: PausedRegistryBackendKind) -> PausedRegistryConfig {
        PausedRegistryConfig {
            backend,
            reconcile_interval_secs: 30,
            lease_ttl_secs: 90,
            reclaim_interval_secs: 30,
        }
    }

    pub fn identity() -> NodeIdentity {
        NodeIdentity::from_config(&Default::default())
    }

    #[tokio::test]
    async fn the_default_backend_is_node_local() {
        let registry = build_paused_registry(
            &config(PausedRegistryBackendKind::Local),
            &identity(),
            None,
            None,
        )
        .await
        .expect("the local backend needs nothing");

        assert!(!registry.is_cluster_backed());
    }

    #[tokio::test]
    async fn a_local_backend_ignores_a_postgres_factory_it_was_handed() {
        struct CountingFactory(std::sync::atomic::AtomicUsize);
        #[async_trait::async_trait(?Send)]
        impl PostgresPausedRegistryFactory for CountingFactory {
            async fn build(
                &self,
                _identity: &NodeIdentity,
                _lease_ttl: std::time::Duration,
            ) -> anyhow::Result<Arc<dyn PausedSandboxRegistry>> {
                self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                anyhow::bail!("the local backend must never build a postgres registry");
            }
        }

        let factory = CountingFactory(std::sync::atomic::AtomicUsize::new(0));
        let registry = build_paused_registry(
            &config(PausedRegistryBackendKind::Local),
            &identity(),
            Some(&factory as &dyn PostgresPausedRegistryFactory),
            Some(std::sync::Arc::new(NoopNodeRegistry) as std::sync::Arc<dyn NodeRegistry>),
        )
        .await
        .expect("the local backend needs nothing, and must ignore what it is given");

        assert!(
            !registry.is_cluster_backed(),
            "a local backend handed a postgres factory must still be node-local"
        );
        assert_eq!(
            factory.0.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "the local arm must never touch the factory"
        );
    }

    #[tokio::test]
    async fn the_postgres_backend_without_a_pool_is_a_startup_failure() {
        let failure = build_paused_registry(
            &config(PausedRegistryBackendKind::Postgres),
            &identity(),
            None,
            Some(std::sync::Arc::new(NoopNodeRegistry) as std::sync::Arc<dyn NodeRegistry>),
        )
        .await;

        let Err(failure) = failure else {
            panic!("a postgres backend with no pool must not build a registry");
        };
        assert!(
            failure.to_string().contains("[pg].dsn"),
            "the refusal has to name the missing setting, got {failure}"
        );
    }

    #[tokio::test]
    async fn every_backend_reports_which_one_it_is() {
        for backend in [PausedRegistryBackendKind::Local] {
            let recorder = Recorder::default();
            let guard = recorder.install();
            build_paused_registry(&config(backend), &identity(), None, None)
                .await
                .expect("the local backend dials nothing here");
            drop(guard);

            // These fields distinguish backend selection and registry scope.
            for field in [
                &format!("backend={}", backend.as_str()),
                "cluster_id=00000000-0000-0000-0000-000000000000",
                "lease_ttl_secs=90",
                "paused sandbox registry ready",
            ] {
                assert!(
                    recorder.saw(tracing::Level::INFO, field),
                    "{backend:?} did not report {field:?}: {:?}",
                    recorder.events()
                );
            }
        }
    }
}
