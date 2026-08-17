//! Cluster-wide registry of paused sandboxes.
//!
//! The node-local [`SandboxPersister`](super::SandboxPersister) keeps a paused
//! sandbox resumable on the node that paused it: its artifacts live under that
//! node's artifact root and its record in that node's local store. Nothing else
//! in the cluster knows the sandbox exists, so a resume that lands anywhere else
//! can only answer "not found", and losing the node loses the sandbox.
//!
//! This registry is the missing half. It holds a durable, cluster-visible record
//! of "this sandbox is paused, its snapshot is `<id>`, it was last on `<node>`" —
//! never the artifacts themselves. A node that receives a resume for a sandbox it
//! has never seen looks the sandbox up here, resolves the snapshot through the
//! repository (object storage, shared by all nodes), and rebuilds the sandbox
//! under its original ID.
//!
//! The registry is optional: [`DisabledPausedSandboxRegistry`] is the default and
//! makes every operation a no-op, which leaves pause/resume behaving exactly as
//! it did before this module existed.

mod disabled;
mod postgres;
mod types;

use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;

use crate::cfg::{PausedRegistryBackendKind, PausedRegistryConfig};
use crate::identity::NodeIdentity;
use crate::orchestrator::PauseOutcome;
use crate::snapshot::SnapshotId;
use crate::types::SandboxId;

pub use disabled::DisabledPausedSandboxRegistry;
pub use postgres::PostgresPausedSandboxRegistry;
pub use types::{BeganPause, PausedRegistryState, PausedSandboxEntry, ResumeClaim};

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
    /// The caller's generation no longer matches the stored row, so another
    /// writer has moved the sandbox on. Never fatal by itself: the caller
    /// re-reads and decides.
    #[error("paused sandbox '{sandbox_id}' moved on from generation {expected}")]
    GenerationConflict { sandbox_id: String, expected: i64 },
}

impl PausedRegistryError {
    pub(super) fn backend(operation: &'static str, source: impl Into<anyhow::Error>) -> Self {
        Self::Backend {
            operation,
            source: source.into(),
        }
    }
}

/// Durable, cluster-visible index of paused sandboxes.
///
/// Implementations must be safe to call concurrently from several nodes: the
/// generation carried by [`PausedSandboxEntry`] is the arbitration token, and
/// every mutating call states the generation it expects to act on.
#[async_trait]
pub trait PausedSandboxRegistry: Send + Sync {
    /// Records that a sandbox has been paused locally but its snapshot is not
    /// durable yet. The returned [`BeganPause`] carries the generation the
    /// caller must pass to [`complete_pause`](Self::complete_pause) or
    /// [`mark_local_only`](Self::mark_local_only), plus the snapshot this pause
    /// supersedes so the caller can retire it once the new one lands.
    async fn begin_pause(&self, entry: &PausedSandboxEntry) -> RegistryResult<BeganPause>;

    /// Marks the snapshot durable, making the sandbox resumable on any node.
    async fn complete_pause(
        &self,
        sandbox_id: &SandboxId,
        generation: i64,
        snapshot_id: &SnapshotId,
    ) -> RegistryResult<()>;

    /// Marks a pause whose snapshot never reached the repository.
    ///
    /// The row is kept, not deleted: the sandbox really is paused, it just
    /// cannot be resumed anywhere but its origin node. Deleting it here would
    /// make it indistinguishable from a sandbox that has been resumed
    /// elsewhere or destroyed, and reconciliation would then throw away the
    /// origin node's local record — which in this case is the only copy.
    async fn mark_local_only(&self, sandbox_id: &SandboxId, generation: i64) -> RegistryResult<()>;

    /// Reads a row without taking ownership.
    async fn get(&self, sandbox_id: &SandboxId) -> RegistryResult<Option<PausedSandboxEntry>>;

    /// Takes ownership of a paused sandbox so this node can resume it.
    async fn claim_for_resume(
        &self,
        sandbox_id: &SandboxId,
        node_id: &str,
    ) -> RegistryResult<ResumeClaim>;

    /// Returns a claimed sandbox to the paused state after a failed resume.
    async fn release_claim(&self, sandbox_id: &SandboxId, generation: i64) -> RegistryResult<()>;

    /// Refreshes the liveness lease on every row among `sandbox_ids` that
    /// `node_id` is the holder of, and reports how many that was.
    ///
    /// This is the only evidence the registry has that a node still holds what
    /// its rows claim. A row whose lease runs out becomes claimable by any
    /// node — that is how a sandbox survives losing the node it was on — so a
    /// node that stops renewing is, by definition, one that has let its
    /// sandboxes go.
    ///
    /// Callers pass their whole local roster and let the implementation decide
    /// which entries they have standing to renew; a node must not be able to
    /// extend a lease on a sandbox it does not hold.
    async fn renew_lease(&self, node_id: &str, sandbox_ids: &[SandboxId]) -> RegistryResult<u64>;

    /// Records that the sandbox is live on `node_id` again.
    ///
    /// The row survives the resume rather than being deleted, still naming the
    /// snapshot it came back from. Two things depend on that: losing `node_id`
    /// before the next pause no longer loses the sandbox, and every other node
    /// holding a stale local copy can see from `origin_node_id` that its copy
    /// has been superseded.
    ///
    /// Never creates a row — a sandbox the cluster does not already track stays
    /// untracked.
    async fn mark_running(&self, sandbox_id: &SandboxId, node_id: &str) -> RegistryResult<()>;

    /// Removes the row, and with it the cluster's memory of the sandbox.
    /// Only correct once the sandbox itself is gone.
    async fn remove(&self, sandbox_id: &SandboxId) -> RegistryResult<()>;

    /// Whether this registry actually tracks sandboxes cluster-wide.
    ///
    /// Reconciliation keys off this and must never run against a registry that
    /// does not: a disabled registry answers "no record" for everything, which
    /// reads as "every paused sandbox has moved on" and would discard all of
    /// them. Defaults to `false` so a new backend has to opt in deliberately.
    fn is_cluster_backed(&self) -> bool {
        false
    }
}

/// The orchestrator's hook into cluster-wide pause bookkeeping.
///
/// The orchestrator pauses, resumes and deletes sandboxes from three places —
/// the API, the expiry evictor, and graceful shutdown — and every one of them
/// has to reach the registry. Rather than repeat that at each call site (which
/// is exactly how the evictor and shutdown paths came to be the two that did
/// not), the orchestrator calls this hook itself and every path inherits it.
///
/// Implemented outside the orchestrator because publishing needs the snapshot
/// repository, which the orchestrator has no other reason to know about. The
/// implementation must not hold the orchestrator back, or the wiring becomes a
/// cycle.
///
/// Every method is best-effort by contract: the sandbox operation has already
/// succeeded locally by the time these run, and failing one of them must cost
/// cross-node recovery and nothing else.
#[async_trait]
pub trait PausedSandboxPublisher: Send + Sync {
    /// Publishes a just-paused sandbox's snapshot and records it cluster-wide.
    ///
    /// Returns the node identity the row was written under, which the
    /// orchestrator stamps onto the local record; `None` when nothing was
    /// recorded. Only a record carrying that stamp may later be discarded for
    /// disagreeing with the registry, so a `None` here is what keeps
    /// reconciliation off records that predate the registry.
    ///
    /// The identity is returned rather than read back later because it is the
    /// thing reconciliation compares the registry row against, and a node's ID
    /// can change between the pause and the comparison.
    async fn publish_paused(&self, outcome: PauseOutcome) -> Option<String>;

    /// Records that the sandbox is live on this node again.
    async fn mark_running(&self, sandbox_id: SandboxId);

    /// Drops the cluster's record of the sandbox and the snapshot behind it.
    async fn forget(&self, sandbox_id: SandboxId);
}

/// Builds the configured registry.
///
/// A `postgres` backend without a DSN is a startup failure rather than a silent
/// fallback to `local`: the operator asked for cluster-wide recovery, and a node
/// that quietly serves node-local semantics instead would only reveal the
/// difference when a node is lost and the sandboxes turn out to be gone.
pub async fn build_paused_registry(
    config: &PausedRegistryConfig,
    identity: &NodeIdentity,
) -> anyhow::Result<Arc<dyn PausedSandboxRegistry>> {
    match config.backend {
        PausedRegistryBackendKind::Local => Ok(Arc::new(DisabledPausedSandboxRegistry)),
        PausedRegistryBackendKind::Postgres => {
            let dsn = config
                .dsn
                .as_deref()
                .map(str::trim)
                .filter(|dsn| !dsn.is_empty())
                .context(
                    "paused_registry.backend = \"postgres\" requires a DSN; \
                     set AENV_PAUSED_REGISTRY_DSN",
                )?;

            Ok(Arc::new(
                PostgresPausedSandboxRegistry::connect(
                    dsn,
                    identity.cluster_id,
                    config.max_connections,
                    config.lease_ttl_secs() as f64,
                )
                .await?,
            ))
        }
    }
}
