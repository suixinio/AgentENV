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

mod central;
mod disabled;
mod postgres;
mod types;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use anyhow::Context;
use async_trait::async_trait;
use tracing::{debug, error, info, warn};

use crate::cfg::{
    ClusterConfig, ObservabilitySchedulerReportConfig, PausedRegistryBackendKind,
    PausedRegistryConfig,
};
use crate::identity::NodeIdentity;
use crate::node_registry::registry::NodeRegistry;
use crate::orchestrator::PauseOutcome;
use crate::pg::SingletonTaskHandle;
use crate::snapshot::SnapshotId;
use crate::types::{ExecutionId, SandboxId};

pub use central::CentralPausedSandboxRegistry;
pub use disabled::DisabledPausedSandboxRegistry;
pub use postgres::PostgresPausedSandboxRegistry;
pub use types::{
    BeganPause, ConflictReason, DeadlineRenewalOutcome, HeldSandbox, MarkRunningOutcome,
    PausedRegistryListEntry, PausedRegistryListing, PausedRegistryState, PausedSandboxEntry,
    ReclaimedHoldings, ReleasedHoldings, ResumeClaim,
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
    /// The caller's generation no longer matches the stored row, so another
    /// writer has moved the sandbox on. Never fatal by itself: the caller
    /// re-reads and decides.
    #[error("paused sandbox '{sandbox_id}' moved on from generation {expected}")]
    GenerationConflict { sandbox_id: String, expected: i64 },
    /// The write came from an incarnation the registry has already replaced.
    ///
    /// 🔴 Terminal, and deliberately *not* the same thing as
    /// [`GenerationConflict`](Self::GenerationConflict). A generation conflict
    /// says "your view of the row is stale, re-read and try again"; this says
    /// "the run you are writing on behalf of is over". Retrying it would re-read
    /// the row, pick up the current generation and send the same write again —
    /// walking straight around the fence. Callers must not retry it, must not
    /// publish behind it, and must not delete the snapshot it was about to
    /// replace.
    ///
    /// On the wire this is `codes.PermissionDenied` from `PausedRegistry`, and
    /// wherever it has to be named as a string it is
    /// `sandbox_execution_superseded`.
    #[error("paused sandbox '{sandbox_id}' has been taken over by a newer incarnation")]
    ExecutionFenced { sandbox_id: String },
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

    /// Reads every row among `sandbox_ids` that exists, in as few round trips as
    /// the backend can manage.
    ///
    /// Reconciliation compares a node's whole roster against the registry, and
    /// doing that one [`get`](Self::get) at a time costs a round trip per
    /// sandbox on every pass, from every node. Worse, the rows then come from
    /// different instants: a sandbox read early can be judged against a cluster
    /// state that a sandbox read late already contradicts. One query answers
    /// both.
    ///
    /// A sandbox missing from the returned map has no row — the same answer
    /// `get` gives as `None`, and never "we did not look".
    async fn get_many(
        &self,
        sandbox_ids: &[SandboxId],
    ) -> RegistryResult<HashMap<SandboxId, PausedSandboxEntry>>;

    /// Takes ownership of a paused sandbox so this node can resume it.
    ///
    /// `execution_id` is the incarnation this node will run the sandbox under.
    /// It is allocated *before* the claim and written by the same statement
    /// that names the claimant, so a `resuming` row names the run that is about
    /// to happen rather than the stopped one it replaces — which is what keeps
    /// the resume window fenced instead of open.
    ///
    /// 🔴 The value to quote at [`mark_running`](Self::mark_running) is the one
    /// on the returned entry, not the one passed in. They are normally the
    /// same; when they are not, the registry's is the one the row holds.
    async fn claim_for_resume(
        &self,
        sandbox_id: &SandboxId,
        node_id: &str,
        execution_id: ExecutionId,
    ) -> RegistryResult<ResumeClaim>;

    /// Returns a claimed sandbox to the paused state after a failed resume.
    ///
    /// The `bool` is whether the write matched a row. Matching nothing stays a
    /// success — somebody else has already moved the row on, which is the
    /// outcome this was trying to produce — but it is worth knowing: it is the
    /// only signal that this node is quoting a generation it has already lost.
    async fn release_claim(&self, sandbox_id: &SandboxId, generation: i64) -> RegistryResult<bool>;

    /// Refreshes the liveness lease on every row among `sandbox_ids` that
    /// `node_id` is the holder of, and reports how many that was.
    ///
    /// This is the only evidence the registry has that a node is still attached
    /// to the cluster. A **parked** row whose lease runs out becomes claimable
    /// by any node — that is how a sandbox whose snapshot never published
    /// survives losing the node it was parked on.
    ///
    /// It does not work that way for rows whose sandbox is live. Renewals stop
    /// for two very different reasons — the process died, or it merely cannot
    /// reach the database — and only the first makes rebuilding the sandbox
    /// elsewhere safe. Those rows are released by the node's own successor
    /// process instead; see
    /// [`release_node_holdings`](Self::release_node_holdings).
    ///
    /// Callers pass their whole local roster and let the implementation decide
    /// which entries they have standing to renew; a node must not be able to
    /// extend a lease on a sandbox it does not hold.
    ///
    /// Each entry also carries the sandbox's current deadline, which the
    /// registry stores alongside the lease. That is what makes
    /// [`reclaim_expired_holdings`](Self::reclaim_expired_holdings) possible at
    /// all: the deadline has to come from the holder, because only the holder
    /// knows about the timeout extensions that happened since the sandbox was
    /// paused.
    async fn renew_lease(&self, node_id: &str, held: &[HeldSandbox]) -> RegistryResult<u64>;

    /// Reclaims live rows whose holder stopped renewing *and* whose sandbox has
    /// since outlived its own deadline.
    ///
    /// The last resort for a machine that is never coming back. Nothing else
    /// releases its rows: `release_node_holdings` needs a successor process on
    /// that machine, and `claim_for_resume` refuses live rows outright — so
    /// without this a decommissioned node's sandboxes would stay unrecoverable
    /// and their rows unremovable, forever.
    ///
    /// 🔴 The deadline, not the lease, is what makes this safe. A lapsed lease
    /// alone says only that the holder cannot reach the database; acting on it
    /// is the mistake `claim_for_resume` exists to avoid. But a sandbox that is
    /// *also* past the deadline its own user gave it has no claim on being kept
    /// alive: it should already have been evicted, and would have been if
    /// anyone could still reach the node. Reclaiming it is enforcing the
    /// timeout, not guessing at the node's health.
    ///
    /// This is where e2b puts the same decision. Its eviction runs in the
    /// control plane off a cluster-wide expiry index, entirely independent of
    /// node state (`e2b/packages/api/internal/orchestrator/evictor/evict.go`),
    /// and it drops the sandbox from its store even when the node cannot be
    /// reached to be told (`delete_instance.go:104`, the unconditional
    /// `defer o.sandboxStore.Remove(...)`). Our eviction lives on the node
    /// instead, which is why losing the node used to mean losing the eviction
    /// with it.
    ///
    /// Both conditions are required, and the lease one is what keeps this out
    /// of the way of the normal path: a reachable node evicts its own expired
    /// sandboxes itself, pausing them properly and publishing a fresh snapshot.
    /// Only when nobody has renewed for a full lease does the cluster step in.
    async fn reclaim_expired_holdings(&self) -> RegistryResult<ReclaimedHoldings>;

    /// Records that the sandbox is live again, brought up by `node_id` and
    /// physically running on `holder_node_id`.
    ///
    /// # 🔴 Two identities, two jobs, never swapped
    ///
    /// `node_id` is the CAS guard: it must be the exact identity
    /// [`claim_for_resume`](Self::claim_for_resume) was called with for this
    /// resume (or, on the local-reopen path where no claim was taken, this
    /// process's own identity — see that method's implementations). It is
    /// compared against the row, never written anywhere but the predicate.
    ///
    /// `holder_node_id` is the opposite: a plain write, participating in no
    /// comparison at all. It becomes `origin_node_id`, which is what every
    /// later lookup, heartbeat comparison and reconciliation pass judges the
    /// row against — so it must be the real machine the sandbox is running
    /// on, which on an api replica is never `node_id` (a pod identity the
    /// scheduler's heartbeats do not track).
    ///
    /// Passing the same value for both is correct and is what every backend
    /// that runs its own sandboxes does — `node_id` already names the real
    /// machine there. Passing `holder_node_id` where `node_id` belongs is the
    /// mistake a0487f0 made: it turned the CAS guard into a comparison against a
    /// value [`claim_for_resume`](Self::claim_for_resume) never wrote, so
    /// every cross-node `mark_running` matched zero rows.
    ///
    /// The row survives the resume rather than being deleted, still naming the
    /// snapshot it came back from. Two things depend on that: losing `node_id`
    /// before the next pause no longer loses the sandbox, and every other node
    /// holding a stale local copy can see from `origin_node_id` that its copy
    /// has been superseded.
    ///
    /// Never creates a row — a sandbox the cluster does not already track stays
    /// untracked.
    ///
    /// Returns whether a row now names this write's holder. `false` covers
    /// both "the cluster does not track this sandbox" and "someone else holds
    /// the claim", and the caller must not treat the registry as having
    /// anything to say about the sandbox in either case: reconciliation reads
    /// an absent row as "the cluster has moved past this sandbox", which for an
    /// untracked one would be a freshly created sandbox being torn down.
    ///
    /// 🔴 `execution_id` must be the incarnation the claim allocated — the one
    /// carried by [`ResumeClaim::Claimed`]'s entry — and never a freshly minted
    /// one. The cross-node branch of this write matches on it, so quoting a new
    /// value makes every cross-node resume fail.
    async fn mark_running(
        &self,
        sandbox_id: &SandboxId,
        node_id: &str,
        holder_node_id: &str,
        execution_id: ExecutionId,
        expires_at: Option<SystemTime>,
    ) -> RegistryResult<MarkRunningOutcome>;

    /// Updates `sandbox_expires_at` alone on a `Running` row — never the lease,
    /// never any identity column, never `generation`.
    ///
    /// This is the write [`mark_running`](Self::mark_running) alone cannot be:
    /// `mark_running` stamps a deadline once, at resume, and never again for
    /// the rest of the sandbox's run — so a later `POST /timeout` extending
    /// that deadline had nowhere on this interface to go. This is that
    /// somewhere.
    ///
    /// # 🔴 One identity, and it answers a different question than
    /// `mark_running`'s two
    ///
    /// `mark_running`'s `node_id`/`holder_node_id` split exists because that
    /// write decides *who may run the sandbox* — a question with a wrong
    /// answerer to guard against (another node's stale claim, another node's
    /// incarnation). This write decides only *when a sandbox already agreed
    /// to be running is due to end*, and touches no column a wrong answerer
    /// could corrupt: not `origin_node_id`, not `claimed_by_node_id`, not
    /// `execution_id` itself. There is no rival actor to fence against, only
    /// this same row's own later history — a fresh resume, under a fresh
    /// `execution_id`, since the caller last observed the sandbox `Running`
    /// locally. `execution_id` alone is what tells that apart, the same way
    /// `mark_running`'s own branch ③ does for its retry-vs-takeover question.
    /// No `node_id`, no generation: this write does not participate in the
    /// registry's ownership questions at all.
    ///
    /// A caller that gets back [`DeadlineRenewalOutcome::Superseded`] must not
    /// retry with the same deadline — the row it would be retrying against is
    /// not the sandbox that asked for the extension any more.
    async fn renew_sandbox_deadline(
        &self,
        sandbox_id: &SandboxId,
        execution_id: ExecutionId,
        expires_at: Option<SystemTime>,
    ) -> RegistryResult<DeadlineRenewalOutcome>;

    /// Hands back every live sandbox this node was holding when its previous
    /// process died, and reports what was found.
    ///
    /// 🔴 **Only ever correct at process startup, before this node can hold
    /// anything.** It releases rows by node identity alone, so running it once
    /// the node is serving would hand this node's own live sandboxes to
    /// whoever resumes them next — the exact duplication the rest of this
    /// module exists to prevent.
    ///
    /// Why startup is nevertheless the strongest evidence in the system: a
    /// node's ID names the machine, not the process, so a row saying
    /// "`running` on this node" that is being read by a process which has just
    /// started and holds nothing can only have been written by a previous
    /// process on this same machine. That process is gone, and its sandboxes
    /// went with it — the VMs are its children, in its PID namespace. No
    /// timeout can establish that; only being the successor can.
    ///
    /// Rows naming a snapshot go back to `paused` and can be resumed anywhere.
    /// Rows without one are deleted: the sandbox was live, its local artifacts
    /// were consumed by the resume that started it, and nothing was ever
    /// published — there is nothing left to bring back, and a row that can
    /// never be claimed would just accumulate.
    async fn release_node_holdings(&self, node_id: &str) -> RegistryResult<ReleasedHoldings>;

    /// Removes the row this node last read, and with it the cluster's memory
    /// of the sandbox. Only correct once the sandbox itself is gone.
    ///
    /// 🔴 Conditional on `generation`, and that condition is the whole guard.
    /// The caller's own check — read the row, decide the sandbox is not live
    /// elsewhere, delete it — is two statements with a window between them, and
    /// a node returning from a partition walks straight into it: it holds a
    /// view from before it left, and the sandbox it is about to forget has
    /// since been resumed somewhere else. The delete would match, taking the
    /// snapshot that node is running from with it, and neither side would
    /// report anything.
    ///
    /// The `bool` is whether it matched. Not matching is not an error: the row
    /// this caller meant to delete is already gone, which is what it wanted.
    async fn remove(&self, sandbox_id: &SandboxId, generation: i64) -> RegistryResult<bool>;

    /// Reads every row in scope, plus the database clock they were read
    /// against — the cluster-wide debug/admin listing this registry answers
    /// as `ListRegistrySandboxes` (`src/node_registry/grpc_service.rs`) and,
    /// through it, the gateway's `GET /registry/sandboxes`
    /// (`services/gateway/internal/registry_list.go`). Ports Go's
    /// `Reader.List` (`registry.go:179-186`, `PostgresReader.List` in
    /// `postgres.go:135-183`).
    ///
    /// No pagination, no filtering: this hands back the whole scoped table
    /// exactly like `PostgresReader.List` does, and `state`/`node_id`
    /// filtering plus keyset paging are the RPC layer's job — mirrors Go's
    /// own `listRegistrySandboxes` (`service.go:849-943`), which rejects an
    /// unknown `state` filter before ever consulting the reader so that
    /// answer does not depend on whether a registry happens to be
    /// configured.
    ///
    /// 🔴 Callers must gate this on [`Self::is_cluster_backed`] first, the
    /// same way `lookup_node`'s stage 3 does
    /// (`crate::binding_store::lookup`'s module doc): a registry that
    /// cannot speak for the whole cluster has nothing here worth trusting an
    /// empty answer from. This mirrors Go's own default -- `Service.registry`
    /// is never a literal nil pointer, it defaults to `pausedregistry.Disabled()`,
    /// whose `List` answers `ErrDisabled` rather than an empty slice, so
    /// "not configured" and "configured, holds nothing" can never be
    /// confused for one another.
    async fn list_all(&self) -> RegistryResult<PausedRegistryListing>;

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

    /// Whether a publishable capture offered to [`Self::publish_paused`] would
    /// actually be committed.
    ///
    /// # 🔴 A promise, asked before the pause, because one backend has to spend
    /// to answer it
    ///
    /// A pause on this machine produces its publishable capture by borrowing
    /// artifacts the persister has already written — it costs nothing, and a
    /// backend that produces one regardless is not wasting anything. A pause on
    /// *another* machine produces one by writing a whole snapshot into durable
    /// storage. Those bytes become unreachable the moment nobody commits a row
    /// naming them: no read path resolves an unannounced snapshot, so nothing
    /// finds them again, and this build has no sweep that would.
    ///
    /// So the question has to be asked before the capture is made rather than
    /// after it is offered, and this is where it is asked. A publisher that
    /// answers `true` is undertaking to reach `publish_captured` — not to
    /// succeed, which no one can promise, but to try.
    fn wants_publishable_capture(&self) -> bool;

    /// Records that the sandbox is live again, on the machine that actually
    /// brought it up.
    ///
    /// `expires_at` is when the sandbox is due to end, and it travels with the
    /// write rather than waiting for the first lease renewal. Reclamation needs
    /// a lapsed lease *and* a passed deadline, and a row with no deadline
    /// satisfies the second condition at no point ever — so a node lost in the
    /// interval before its first renewal used to leave a row nothing could
    /// reclaim, claim or remove again. `None` means the sandbox was asked never
    /// to expire, which is a different fact and stays one.
    ///
    /// `holding_node_id` is the real machine, read from the backend that just
    /// started it ([`SandboxBackend::holding_node_id`][crate::sandbox::SandboxBackend::holding_node_id]),
    /// not this process's own identity. `None` covers every backend that runs
    /// the VM in this same process, which is the correct, common case and the
    /// one every implementation must fall back to its own identity for —
    /// mirroring [`PausedSandboxPublisher::publish_paused`]'s identical
    /// fallback for the paused half of this same question.
    async fn mark_running(
        &self,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
        expires_at: Option<SystemTime>,
        holding_node_id: Option<String>,
    );

    /// Extends the deadline the cluster registry holds for an already-running
    /// sandbox, without disturbing anything else about the row.
    ///
    /// `mark_running` above stamps a deadline once, at resume; every
    /// `POST /timeout` after that has nowhere else on this trait to reach the
    /// registry, and until this method existed such an extension never
    /// travelled past the orchestrator's own metadata store — the cluster
    /// kept enforcing whatever `mark_running` last wrote.
    ///
    /// `execution_id` must be the sandbox's *current* incarnation, exactly as
    /// the orchestrator's own metadata names it at the moment `keep_alive_for`
    /// observed the sandbox `Running` — see
    /// [`PausedSandboxRegistry::renew_sandbox_deadline`] for what it fences.
    /// `expires_at` must be the deadline *after* `keep_alive_for`'s own
    /// lifetime-ceiling clamp, never the caller's raw request: this method has
    /// no ceiling of its own to apply, and a caller that forwarded the raw
    /// value would let a request that only *looked* like it asked for more
    /// than the ceiling allows quietly outlive it in the registry, even though
    /// the orchestrator's own record stayed clamped.
    ///
    /// Best-effort, like every other method on this trait: the local timeout
    /// extension has already succeeded by the time this runs, and this write
    /// is only a backstop mirror for [`ReclaimExpiredHoldings`
    /// ](super::PausedSandboxRegistry::reclaim_expired_holdings)'s cluster-wide
    /// reclaim — not the sandbox's own enforcement, which the orchestrator's
    /// local record already carries. A transport failure here costs that
    /// mirror staying stale until the sandbox's next resume, and nothing else.
    async fn renew_deadline(
        &self,
        sandbox_id: SandboxId,
        execution_id: ExecutionId,
        expires_at: Option<SystemTime>,
    );

    /// Drops the cluster's record of the sandbox and the snapshot behind it.
    ///
    /// `holding_node_id` is the real machine this delete just stopped the
    /// sandbox on, read from the backend the same way
    /// [`Self::mark_running`]'s `holding_node_id` is — see that parameter's
    /// doc. `None` covers every backend that runs the VM in this same
    /// process, which is the correct, common case and the one every
    /// implementation must fall back to its own identity for, mirroring
    /// `mark_running`'s identical fallback for the mirror-image question (a
    /// sandbox this process is bringing up, rather than tearing down).
    ///
    /// Without this, an implementation deciding whether the row still
    /// describes a sandbox live elsewhere has nothing but its own process
    /// identity to compare the row's real machine against — which is never
    /// equal to it on a half that never holds a sandbox itself, and made
    /// every such delete refuse to clear the row it had just made accurate.
    async fn forget(&self, sandbox_id: SandboxId, holding_node_id: Option<String>);
}

/// Says what a granted claim cost, once, wherever the claim came from.
///
/// Kept here rather than in each backend because the middle arm is an
/// acceptance condition, not a log line: a claim that overrode a node which
/// never finished uploading brings the sandbox back one snapshot behind, and
/// the work since that snapshot is gone. That event has to be findable, and a
/// second copy of this decision in another backend is how one of them would
/// come to be missing it.
pub(super) fn log_claim_outcome(
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
        // Unreachable through the claim's own predicate, which is precisely why
        // it is loud: reaching it means the predicate and this match have
        // drifted apart, and the claim just duplicated a sandbox that was live
        // somewhere else.
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

/// Builds the configured registry, and says which one it built.
///
/// A cluster backend that is missing what it needs to reach the cluster is a
/// startup failure rather than a silent fallback to `local`: the operator asked
/// for cluster-wide recovery, and a node that quietly serves node-local
/// semantics instead would only reveal the difference when a node is lost and
/// the sandboxes turn out to be gone.
///
/// The backends that *are* reachable this way — `local` because the override
/// never arrived — are told apart by the one info line at the end.
///
/// `pg_pool`/`node_registry` are Stage C's own additions, both `None` for
/// every caller not selecting `postgres` (the `Local`/`Central` arms never
/// touch either). `node_registry` is `Some` only when `--role api` built a
/// real `crate::node_registry::registry::AtomicNodeRegistry`
/// (`[cluster].node_placement_source = "native"`, `src/bin/server.rs`'s
/// `assemble_api`) -- `--role all` never builds one at all.
///
/// `role` (M1) decides whether a missing `node_registry` is refused: see the
/// `postgres` arm's own comment.
pub async fn build_paused_registry(
    config: &PausedRegistryConfig,
    cluster: &ClusterConfig,
    scheduler_report: &ObservabilitySchedulerReportConfig,
    identity: &NodeIdentity,
    role: crate::role::ServerRole,
    pg_pool: Option<sqlx::PgPool>,
    node_registry: Option<Arc<dyn NodeRegistry>>,
) -> anyhow::Result<Arc<dyn PausedSandboxRegistry>> {
    let (registry, scheduler_endpoint): (Arc<dyn PausedSandboxRegistry>, &str) =
        match config.backend {
            PausedRegistryBackendKind::Local => (Arc::new(DisabledPausedSandboxRegistry), ""),
            PausedRegistryBackendKind::Postgres => {
                let pool = pg_pool.context(
                    "paused_registry.backend = \"postgres\" requires [pg].dsn to be configured \
                     (the shared PostgreSQL pool this process already builds for Stage B's \
                     catalog, if [pg] is set)",
                )?;

                // 🔴 M1: the roster requirement is `--role api`'s alone, not
                // this backend's in general. `role.runs_sandbox_runtime()`
                // is exactly the distinguishing fact: under `--role all`
                // this process's own identity already coincides with
                // `origin_node_id` for everything it runs, so the ordinary
                // `renew_lease` trait method (this process's own periodic
                // self-renewal, `spawn_paused_record_upkeep` in
                // `src/bin/server.rs`) already covers what D2 Fix A exists
                // to cover under the split node/api identity model -- see
                // `postgres::replica_renewal`'s own module doc for the full
                // argument. Under `--role api` (`runs_sandbox_runtime() ==
                // false`), that identity never coincides, so a missing
                // roster really would leave `running` rows with no renewal
                // path at all -- the exact failure Fix A (commit `151d00b`)
                // closed for the central/gRPC backend -- and is refused here
                // exactly as before.
                if !role.runs_sandbox_runtime() {
                    node_registry.as_ref().context(
                        "paused_registry.backend = \"postgres\" requires a heartbeat roster \
                         source under --role api, which only exists under \
                         [cluster].node_placement_source = \"native\" -- without it, running \
                         sandboxes' registry leases have no renewal path and will eventually be \
                         wrongly reclaimed even while healthy (this is the exact failure Fix A, \
                         commit 151d00b, closed for the central/gRPC backend). Set \
                         AENV_NODE_PLACEMENT_SOURCE=native, or keep this backend on \
                         \"central\"",
                    )?;
                }

                // 🔴 `node_registry` is not consumed here -- it is only
                // needed by the background reconcile/reclaim/renewal loops
                // [`spawn_paused_registry_background_tasks`] starts, which
                // this function's caller must invoke separately (mirroring
                // `spawn_pg_singleton_tasks` alongside `build_pg_pool` in
                // `src/bin/server.rs`) so their task handles land in the
                // same buckets every other PostgreSQL-backed background task
                // already shuts down through. Validated present here
                // anyway, under `--role api`, so a `postgres` backend that
                // will fail to start its safety net a few lines later in the
                // caller is a startup failure discovered too late to
                // matter, not one avoided.
                drop(node_registry);

                postgres::schema::migrate(&pool)
                    .await
                    .context("bootstrap the paused_sandboxes schema")?;

                let lease_ttl = std::time::Duration::from_secs(config.lease_ttl_secs());

                // 🔴 B2(1): a synchronous, best-effort attempt to enter
                // restart grace *before* this function returns and its
                // caller opens for traffic -- see
                // `postgres::grace::attempt_initial_entry`'s own doc for why
                // this closes (most of) the window between this process
                // serving its first request and the reconcile leader's own
                // background loop landing its first `enter`. Never fails
                // this call: a database that cannot be reached for this
                // attempt will be retried by the background reconcile loop
                // regardless.
                postgres::attempt_initial_grace_entry(
                    &pool,
                    identity.cluster_id,
                    lease_ttl.as_secs_f64(),
                )
                .await;

                let registry = Arc::new(PostgresPausedSandboxRegistry::new(
                    pool,
                    identity.cluster_id,
                    lease_ttl,
                ));

                (registry, "")
            }
            PausedRegistryBackendKind::Central => {
                let endpoint = cluster
                    .scheduler_endpoint
                    .as_deref()
                    .map(str::trim)
                    .filter(|endpoint| !endpoint.is_empty())
                    .context(
                        "paused_registry.backend = \"central\" requires a scheduler endpoint; \
                         set AENV_OBSERVABILITY_SCHEDULER_ENDPOINT",
                    )?;

                (
                    Arc::new(CentralPausedSandboxRegistry::connect_hot_reloadable(
                        endpoint,
                        cluster,
                        scheduler_report,
                        identity.cluster_id,
                        identity.id.clone(),
                        config.lease_ttl_secs(),
                        // The raw configured value, not the clamped one:
                        // reporting `lease_ttl_secs()`'s own input back to the
                        // controller is what lets it see the two knobs
                        // disagreeing. Clamping first would make every node
                        // look correctly configured by construction.
                        config.reconcile_interval_secs,
                    )?),
                    endpoint,
                )
            }
        };

    // 🔴 One statement for both backends, after the match rather than inside
    // each arm. A per-arm line is a line an arm can be missing, and the arm
    // that was missing it was `central`: a node switched over to it said
    // nothing at all, so "the switch took" and "the value never reached the
    // node and it stayed on `local`" read identically in the log — while the
    // difference between them only surfaces later, when a node is lost and its
    // sandboxes turn out to have gone with it. Which backend this node ended
    // up on is the one fact this path has to state out loud.
    //
    // `scheduler_endpoint` is empty for `local`, which does not have one.
    info!(
        backend = config.backend.as_str(),
        cluster_id = %identity.cluster_id,
        lease_ttl_secs = config.lease_ttl_secs(),
        scheduler_endpoint,
        "paused sandbox registry ready"
    );

    Ok(registry)
}

/// [`spawn_paused_registry_background_tasks`]'s result -- split by shutdown
/// mechanism, mirroring `postgres::BackgroundTasks` (which this simply
/// forwards): `singleton` needs `SingletonTaskHandle::shutdown()`'s async
/// advisory-lock release and belongs in `Assembly::pg_singleton_tasks`
/// (`src/bin/server.rs`); `plain` is safe to `.abort()` and belongs in
/// `Assembly::upkeep` alongside `spawn_paused_record_upkeep`'s own tasks.
#[derive(Default)]
pub struct PausedRegistryBackgroundTasks {
    pub singleton: Vec<SingletonTaskHandle>,
    pub plain: Vec<tokio::task::JoinHandle<()>>,
}

/// Starts the `postgres` backend's background tasks (reconcile leader loop,
/// reclaim leader loop, and -- M1 -- B1's per-replica renewal loop when a
/// roster source is available; see `postgres::spawn_background_tasks`'s own
/// doc), if and only if `config.backend == Postgres`. Every other backend
/// returns everything empty.
///
/// Kept separate from [`build_paused_registry`] deliberately, mirroring
/// `src/bin/server.rs`'s own `build_pg_pool` + `spawn_pg_singleton_tasks`
/// split: the registry itself has to exist before `ApiImpl`/`Orchestrator`
/// can be constructed, but the resulting task handles belong in the buckets
/// every other PostgreSQL-backed background task in this process already
/// shuts down through.
///
/// 🔴 Call this only after a preceding [`build_paused_registry`] call with
/// the same `config`/`pg_pool` has already succeeded (it performed the
/// "postgres requires a pool" validation this function relies on without
/// repeating). `node_registry` may legitimately be `None` here even when
/// `config.backend == Postgres` (M1: `--role all`) -- see
/// `postgres::spawn_background_tasks`'s own doc for what that does and does
/// not skip.
pub fn spawn_paused_registry_background_tasks(
    config: &PausedRegistryConfig,
    identity: &NodeIdentity,
    pg_pool: Option<sqlx::PgPool>,
    node_registry: Option<Arc<dyn NodeRegistry>>,
) -> PausedRegistryBackgroundTasks {
    if config.backend != PausedRegistryBackendKind::Postgres {
        return PausedRegistryBackgroundTasks::default();
    }
    let Some(pool) = pg_pool else {
        return PausedRegistryBackgroundTasks::default();
    };

    let lease_ttl = std::time::Duration::from_secs(config.lease_ttl_secs());
    let tasks = postgres::spawn_background_tasks(
        pool,
        identity.cluster_id,
        lease_ttl,
        config.reconcile_interval(),
        config.reclaim_interval(),
        node_registry,
    );
    PausedRegistryBackgroundTasks {
        singleton: tasks.singleton,
        plain: tasks.plain,
    }
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

    /// 🔴 The one event on this path an operator has to be able to find: the
    /// claim overrode a node that never finished uploading, so the sandbox
    /// comes back one snapshot behind and everything since it is gone. It is a
    /// warning because somebody lost work, not because something failed.
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

    /// An ordinary cross-node resume costs nothing and must not read as a
    /// takeover — reporting every one of them as a lease takeover is a bug this
    /// path has already had once.
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

    /// A state the claim's own predicate cannot produce means the predicate and
    /// this match have drifted apart, and a live sandbox may now exist twice.
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
    use crate::cfg::{ObservabilitySchedulerReportConfig, PausedRegistryConfig};
    use crate::logging::capture::Recorder;

    /// A `NodeRegistry` that answers every question with "nothing" -- only
    /// used to satisfy `build_paused_registry`'s type signature in tests
    /// that never reach a code path calling any of these methods (the
    /// `postgres` backend's own construction fails, in every test that uses
    /// this fixture, before its background tasks -- the only consumer of a
    /// real `NodeRegistry` -- are ever spawned).
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

    pub(super) fn cluster(scheduler_endpoint: Option<&str>) -> ClusterConfig {
        ClusterConfig {
            node_placement_source: crate::cfg::NodePlacementSource::Scheduler,
            scheduler_endpoint: scheduler_endpoint.map(str::to_string),
            scheduler_endpoint_file: String::new(),
            node_service_addr: "0.0.0.0:8001".to_string(),
            api_grpc_addr: "0.0.0.0:8002".to_string(),
            node_service_port: 8001,
            kubernetes_discovery: Default::default(),
            native_warmup_timeout_secs: 15,
            node_registry_store: Default::default(),
        }
    }

    pub(super) fn scheduler_report() -> ObservabilitySchedulerReportConfig {
        ObservabilitySchedulerReportConfig {
            enabled: false,
            interval_secs: 5,
            scheduler_endpoint_file: String::new(),
            dual_report_api_endpoint: String::new(),
        }
    }

    pub(super) fn identity() -> NodeIdentity {
        NodeIdentity::from_config(&Default::default())
    }

    #[tokio::test]
    async fn the_default_backend_is_node_local() {
        let registry = build_paused_registry(
            &config(PausedRegistryBackendKind::Local),
            &cluster(None),
            &scheduler_report(),
            &identity(),
            crate::role::ServerRole::Api,
            None,
            None,
        )
        .await
        .expect("the local backend needs nothing");

        assert!(!registry.is_cluster_backed());
    }

    #[tokio::test]
    async fn the_central_backend_comes_up_against_an_endpoint() {
        let registry = build_paused_registry(
            &config(PausedRegistryBackendKind::Central),
            &cluster(Some("http://scheduler.invalid:9090")),
            &scheduler_report(),
            &identity(),
            crate::role::ServerRole::Api,
            None,
            None,
        )
        .await
        .expect("the endpoint is dialled on first use, not here");

        assert!(registry.is_cluster_backed());
    }

    /// 🔴 A cluster backend that cannot reach the cluster must stop the node,
    /// not quietly serve node-local semantics. The difference between the two
    /// only shows up when a node is lost and its sandboxes turn out to have
    /// gone with it.
    #[tokio::test]
    async fn the_central_backend_without_an_endpoint_is_a_startup_failure() {
        for endpoint in [None, Some(""), Some("   ")] {
            assert!(
                build_paused_registry(
                    &config(PausedRegistryBackendKind::Central),
                    &cluster(endpoint),
                    &scheduler_report(),
                    &identity(),
                    crate::role::ServerRole::Api,
                    None,
                    None,
                )
                .await
                .is_err(),
                "endpoint {endpoint:?} must not build a registry"
            );
        }
    }

    /// 🔴 D1 (Stage C report): the `postgres` backend refuses to start
    /// without a PostgreSQL pool -- never silently falls back to `local`,
    /// which would drop cluster-wide recovery with no error at all.
    #[tokio::test]
    async fn the_postgres_backend_without_a_pool_is_a_startup_failure() {
        let failure = build_paused_registry(
            &config(PausedRegistryBackendKind::Postgres),
            &cluster(None),
            &scheduler_report(),
            &identity(),
            crate::role::ServerRole::Api,
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

    // 🔴 The node-registry-only refusal (a real `Some(pool)`, `None`
    // `node_registry`) needs an actual reachable PostgreSQL pool to
    // construct — `sqlx::PgPool::connect` dials on construction, so there is
    // no fake to substitute here. That case is covered by the `pg::`-gated
    // suite's `the_postgres_backend_without_a_node_registry_is_a_startup_failure`
    // below, alongside the rest of this backend's real-database coverage.

    /// 🔴 Every backend says which one it is, out loud, at assembly.
    ///
    /// The one that did not was `central`, and the cost was that a node
    /// switched over to it produced no line at all — indistinguishable in the
    /// log from a node whose override never arrived and that quietly stayed on
    /// `local`, which is the failure `AENV_PAUSED_REGISTRY_BACKEND` was
    /// introduced to prevent in the first place. The two arms reachable
    /// without a database are checked here; the third shares the single
    /// statement they all reach.
    #[tokio::test]
    async fn every_backend_reports_which_one_it_is() {
        for (backend, endpoint) in [
            (PausedRegistryBackendKind::Local, None),
            (
                PausedRegistryBackendKind::Central,
                Some("http://scheduler.invalid:9090"),
            ),
        ] {
            let recorder = Recorder::default();
            let guard = recorder.install();
            build_paused_registry(
                &config(backend),
                &cluster(endpoint),
                &scheduler_report(),
                &identity(),
                crate::role::ServerRole::Api,
                None,
                None,
            )
            .await
            .expect("neither backend dials anything here");
            drop(guard);

            // The backend, so "the switch took" is readable on its own; the
            // cluster id and the lease, because a registry scoped to the wrong
            // cluster or holding leases for the wrong length looks healthy
            // from every other angle.
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

            // And where it is reaching, for the backend that reaches anywhere:
            // an endpoint pointing at the wrong scheduler is the other way a
            // node can be configured on and useless.
            assert_eq!(
                recorder.saw(
                    tracing::Level::INFO,
                    "scheduler_endpoint=http://scheduler.invalid:9090"
                ),
                endpoint.is_some(),
                "{backend:?} reported the wrong endpoint: {:?}",
                recorder.events()
            );
        }
    }
}

#[cfg(test)]
mod pg {
    use std::sync::Arc;
    use std::time::SystemTime;

    use super::build_tests::*;
    use super::*;
    use crate::cfg::PausedRegistryConfig;
    use crate::pg::harness::isolated_schema_pool_or_skip;

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

    fn config() -> PausedRegistryConfig {
        PausedRegistryConfig {
            backend: PausedRegistryBackendKind::Postgres,
            reconcile_interval_secs: 30,
            lease_ttl_secs: 90,
            reclaim_interval_secs: 30,
        }
    }

    /// 🔴 D1's central startup guard, proved with a real (reachable) pool
    /// this time: under `--role api`, a `postgres` backend still refuses to
    /// start without a node registry, even once the *other* precondition
    /// (`the_postgres_backend_without_a_pool_is_a_startup_failure`, in
    /// `build_tests`) is satisfied. M1 narrowed this guard to `--role api`
    /// specifically -- see
    /// `the_postgres_backend_under_role_all_does_not_require_a_node_registry`
    /// below for the role this guard must *not* fire for.
    #[tokio::test]
    async fn the_postgres_backend_without_a_node_registry_is_a_startup_failure_under_role_api() {
        let pool = isolated_schema_pool_or_skip!(
            "the_postgres_backend_without_a_node_registry_is_a_startup_failure_under_role_api"
        );

        let failure = build_paused_registry(
            &config(),
            &cluster(None),
            &scheduler_report(),
            &identity(),
            crate::role::ServerRole::Api,
            Some(pool),
            None,
        )
        .await;

        let Err(failure) = failure else {
            panic!("a postgres backend with no node registry must not build a registry under --role api");
        };
        assert!(
            failure.to_string().contains("node_placement_source"),
            "the refusal has to name the missing setting, got {failure}"
        );
    }

    /// 🔴 M1's own regression pin: `--role all` never builds a
    /// `NodeRegistry` at all (it has no equivalent of `--role api`'s
    /// `[cluster].node_placement_source = "native"` wiring), and unlike
    /// `--role api` it does not need one -- this process's own identity
    /// coincides with `origin_node_id` for everything it runs, so
    /// `renew_lease` already covers what D2 Fix A exists to cover under the
    /// split identity model (see `postgres::replica_renewal`'s own module
    /// doc). Before M1, this configuration was an unconditional startup
    /// failure -- the rollback target could not select this backend at
    /// all. Without this test, a change that silently widened the `--role
    /// api` guard back to every role would look identical to every other
    /// green test in this file.
    #[tokio::test]
    async fn the_postgres_backend_under_role_all_does_not_require_a_node_registry() {
        let pool = isolated_schema_pool_or_skip!(
            "the_postgres_backend_under_role_all_does_not_require_a_node_registry"
        );

        let registry = build_paused_registry(
            &config(),
            &cluster(None),
            &scheduler_report(),
            &identity(),
            crate::role::ServerRole::All,
            Some(pool),
            None,
        )
        .await
        .expect("--role all must not require a node registry to select the postgres backend");

        assert!(registry.is_cluster_backed());
    }

    /// The happy path: a real pool and a (fake, but present) node registry
    /// build a working, cluster-backed registry, and the schema bootstrap
    /// this function is documented to run actually leaves the table usable.
    #[tokio::test]
    async fn the_postgres_backend_builds_a_working_cluster_backed_registry() {
        let pool = isolated_schema_pool_or_skip!(
            "the_postgres_backend_builds_a_working_cluster_backed_registry"
        );

        let registry = build_paused_registry(
            &config(),
            &cluster(None),
            &scheduler_report(),
            &identity(),
            crate::role::ServerRole::Api,
            Some(pool.clone()),
            Some(Arc::new(NoopNodeRegistry) as Arc<dyn NodeRegistry>),
        )
        .await
        .expect("a real pool and a real node registry should build successfully");

        assert!(registry.is_cluster_backed());

        // The migration this call is documented to run actually happened:
        // the table is queryable without the caller having to migrate it
        // separately first.
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM paused_sandboxes")
            .fetch_one(&pool)
            .await
            .expect("paused_sandboxes should exist after build_paused_registry");
        assert_eq!(count, 0);
    }

    /// 🔴 B2(1)'s own wiring pin: `build_paused_registry`'s `postgres` arm
    /// must actually call `postgres::attempt_initial_grace_entry` before it
    /// returns -- proving the connection, not just
    /// `postgres::grace::attempt_initial_entry`'s own isolated pg-gated
    /// tests (`postgres::grace::pg`), which exercise the function directly
    /// but say nothing about whether `build_paused_registry` ever calls it.
    /// A `paused_registry_grace` row for this cluster must exist the moment
    /// this call returns, with no leader-elected background loop having run
    /// yet.
    #[tokio::test]
    async fn building_the_postgres_backend_enters_grace_synchronously() {
        let pool = isolated_schema_pool_or_skip!(
            "building_the_postgres_backend_enters_grace_synchronously"
        );

        let node_identity = identity();
        let registry = build_paused_registry(
            &config(),
            &cluster(None),
            &scheduler_report(),
            &node_identity,
            crate::role::ServerRole::Api,
            Some(pool.clone()),
            Some(Arc::new(NoopNodeRegistry) as Arc<dyn NodeRegistry>),
        )
        .await
        .expect("a real pool and a real node registry should build successfully");
        assert!(registry.is_cluster_backed());

        let row_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM paused_registry_grace WHERE cluster_id = $1)",
        )
        .bind(node_identity.cluster_id)
        .fetch_one(&pool)
        .await
        .expect("query should succeed");
        assert!(
            row_exists,
            "build_paused_registry must have entered grace synchronously before returning -- \
             no background loop has run yet at this point in the test"
        );
    }
}
