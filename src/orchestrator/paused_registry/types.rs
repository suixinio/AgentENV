use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::orchestrator::store::SandboxMetadata;
use crate::snapshot::SnapshotId;
use crate::types::{ExecutionId, SandboxId};

/// Lifecycle of a registry row.
///
/// The row is created by the node that pauses the sandbox and lives until the
/// sandbox is deleted. It deliberately outlives the paused period: once a
/// sandbox is resumed the row stays behind as `Running`, still naming the last
/// durable snapshot, so losing the node that resumed it does not lose the
/// sandbox — the next resume rebuilds it from that snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PausedRegistryState {
    /// The sandbox is paused locally but its snapshot has not landed in the
    /// repository yet. Only the origin node can resume it in this state.
    Publishing,
    /// The snapshot is durable in the repository. Any node can resume it.
    Paused,
    /// A node has claimed this sandbox and is bringing it back up.
    Resuming,
    /// The sandbox is paused but its snapshot never reached the repository, so
    /// only `origin_node_id` can bring it back. The row exists purely so the
    /// cluster can tell "this sandbox is still parked on its node" apart from
    /// "this sandbox is gone" — a distinction reconciliation depends on.
    LocalOnly,
    /// The sandbox is live on `origin_node_id`. `snapshot_id` still names the
    /// snapshot it was last resumed from, which is what makes it recoverable if
    /// that node is lost before the sandbox is paused again.
    Running,
}

impl PausedRegistryState {
    /// The five values the `state` column may hold, in the order Go's
    /// `KnownStates()` presented them (`registry.go:44-46`).
    ///
    /// Used to name the accepted set in a `ListRegistrySandboxes` "unknown
    /// state" error and to seed the per-state row gauge, so that a state added
    /// to the enum shows up in both without either being edited.
    pub const ALL: [Self; 5] = [
        Self::Publishing,
        Self::Paused,
        Self::Resuming,
        Self::LocalOnly,
        Self::Running,
    ];

    /// Decodes the textual form stored in the registry. The encoded values are
    /// written literally by the backend's SQL and pinned there by a CHECK
    /// constraint, so this is the only place that has to know them.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "publishing" => Some(Self::Publishing),
            "paused" => Some(Self::Paused),
            "resuming" => Some(Self::Resuming),
            "local_only" => Some(Self::LocalOnly),
            "running" => Some(Self::Running),
            _ => None,
        }
    }

    /// [`parse`](Self::parse)'s inverse, and the only place these literals are
    /// produced.
    ///
    /// 🔴 The strings are wire and schema, not display text: the
    /// `paused_sandboxes` CHECK constraint, the `Scheduler` proto's state
    /// filter, and the gateway's own tests all pin these exact five values, so
    /// a rename here is a migration rather than an edit. That is precisely why
    /// there is one copy — three call sites used to carry a private `match`
    /// each (`node_registry::grpc_service`, `binding_store::lookup`, and the
    /// postgres backend's `reconcile`), byte-identical and free to drift apart
    /// one at a time.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Publishing => "publishing",
            Self::Paused => "paused",
            Self::Resuming => "resuming",
            Self::LocalOnly => "local_only",
            Self::Running => "running",
        }
    }
}

/// A paused sandbox as seen by the whole cluster.
///
/// This never carries artifacts. `snapshot_id` is the reference the resuming
/// node hands to the snapshot repository to rebuild the sandbox; `metadata` is
/// the same [`SandboxMetadata`] the node-local persister stores, so a resume on
/// a different node reconstructs identical sandbox identity and configuration.
#[derive(Clone, Debug)]
pub struct PausedSandboxEntry {
    pub sandbox_id: SandboxId,
    pub cluster_id: Uuid,
    pub state: PausedRegistryState,
    /// Bumped on every state transition. Callers pass the generation they
    /// observed back into the registry so a stale writer cannot overwrite a
    /// newer decision (for example a pause completing after another node has
    /// already claimed the sandbox for resume).
    pub generation: i64,
    /// The node that currently holds the sandbox: the one that paused it, or
    /// the one it was last resumed on. A scheduling hint, never a binding —
    /// but also the answer to "whose local copy is authoritative", which is
    /// what lets every other node recognise its own copy as superseded.
    pub origin_node_id: String,
    /// The node that took the sandbox for a resume. Only set while
    /// `state == Resuming`; distinct from `origin_node_id`, which still names
    /// the node whose disk holds the local artifacts.
    pub claimed_by_node_id: Option<String>,
    /// `None` while `state == Publishing`.
    pub snapshot_id: Option<SnapshotId>,
    /// The sandbox's identity and configuration, as it looked when it was
    /// paused. Present on the write path and on a granted claim; `None` on a
    /// bulk read.
    ///
    /// 🔴 `None` says *this answer did not carry the record*, never *this
    /// sandbox has no record*. Every row in the table has one — the write that
    /// creates the row is refused without it — so there is no such thing as a
    /// sandbox whose record is absent, and code that reads `None` as an empty
    /// or default record would rebuild a sandbox that is not the one that was
    /// asked for.
    ///
    /// 🔴 Optional because one backend genuinely cannot supply it, not because
    /// it is unimportant. Exactly one caller reads it — the cross-node rebuild,
    /// whose entry comes from [`ResumeClaim::Claimed`] — while the two bulk
    /// consumers look only at `state`, `origin_node_id`, `claimed_by_node_id`
    /// and `generation`. A backend that fetches rows over the network therefore
    /// leaves it out of the batch read, which keeps a node's whole roster well
    /// under any message size limit and confines the byte-for-byte round trip
    /// of this record to the two calls that actually carry it.
    ///
    /// Absent, not defaulted: a default `SandboxMetadata` carries a freshly
    /// generated id that matches no sandbox, so it would rebuild something that
    /// is not the sandbox that was asked for, and nothing would report an
    /// error.
    pub metadata: Option<SandboxMetadata>,
    /// The incarnation this row is fenced against, or `None` when the row's
    /// state has none — a parked row names no run.
    ///
    /// 🔴 On a granted claim this is the incarnation the claim allocated, and
    /// it is the one the claimant must run under and quote at `mark_running`.
    /// A claimant that mints its own instead matches no predicate on the
    /// registry side, and every cross-node resume fails.
    pub execution_id: Option<ExecutionId>,
    pub paused_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// A batched registry read, plus the ids it can actually answer for.
///
/// 🔴 The `covered` list is the whole point, and it exists because the two
/// answers a plain map conflates have opposite consequences. Reconciliation
/// destroys things on the strength of an absence — a paused record and its
/// artifacts (`discard_paused_record`), or a *live VM*
/// (`discard_superseded_sandbox`) — so "asked, and the cluster holds no row"
/// must be distinguishable from "did not ask" and from "asked, and this build
/// could not read the answer". In a bare `HashMap` all three are the same
/// missing key.
///
/// A backend puts an id in `covered` only when it can stand behind the answer
/// for it, present and absent alike. A row it read but could not decode is
/// therefore *not* covered: the sandbox exists as far as the cluster is
/// concerned, and treating it as gone is the exact inversion that turns one
/// unreadable row into a torn-down sandbox.
///
/// This mirrors [`MetadataRows`](crate::orchestrator::store::MetadataRows) on
/// the sandbox-metadata side, which already has this shape and a contract test
/// (`store::contract::get_many_reports_what_it_covered`) pinning it.
#[derive(Debug, Default)]
pub struct PausedRegistryRows {
    pub entries: HashMap<SandboxId, PausedSandboxEntry>,
    pub covered: Vec<SandboxId>,
}

impl PausedRegistryRows {
    /// Every id asked about, answered for.
    ///
    /// Callers that act destructively on absence must check this before reading
    /// [`entries`](Self::entries) — see the type's own doc for why a shortened
    /// batch is indistinguishable from a cluster that holds nothing.
    pub fn covers(&self, ids: &[SandboxId]) -> bool {
        self.covered.len() == ids.len()
    }

    /// This batch narrowed to the ids it can answer for.
    ///
    /// Built once per pass rather than scanned per id: reconciliation asks
    /// about every sandbox on the node, so a linear membership test inside that
    /// loop is quadratic in the roster.
    pub fn answered(&self) -> AnsweredRows<'_> {
        AnsweredRows {
            entries: &self.entries,
            covered: self.covered.iter().copied().collect(),
        }
    }

    /// A batch that answered for every id it was given.
    ///
    /// The shape a backend with nothing to skip returns, and the only one a
    /// caller acting on absence will accept.
    pub fn fully_covering(
        entries: HashMap<SandboxId, PausedSandboxEntry>,
        ids: &[SandboxId],
    ) -> Self {
        Self {
            entries,
            covered: ids.to_vec(),
        }
    }
}

/// A [`PausedRegistryRows`] that will not hand out a row without first saying
/// whether it answered for that id.
///
/// 🔴 This exists to make the fail-open shape unrepresentable at the call site
/// rather than merely discouraged. The guard it replaces was a hand-written
/// `if !covered.contains(&id) { continue; }` sitting above a plain map lookup:
/// correct, invisible, and deletable without breaking a single test or type —
/// which is exactly how the original defect arrived. Going through
/// [`get`](Self::get) means a caller cannot reach an entry, or an absence,
/// without having been handed the coverage answer in the same expression.
pub struct AnsweredRows<'a> {
    entries: &'a HashMap<SandboxId, PausedSandboxEntry>,
    covered: HashSet<SandboxId>,
}

impl<'a> AnsweredRows<'a> {
    /// The batch's answer for one id, or `None` when it has none.
    ///
    /// The two `None`s are different facts and the nesting is what keeps them
    /// apart:
    ///
    /// - `None` — the batch did not answer for this id (a row it could not
    ///   read, or an id it was never asked about). **Judge nothing.**
    /// - `Some(None)` — asked, and the cluster holds no row. A real absence,
    ///   and the one a caller may act on.
    /// - `Some(Some(entry))` — asked, and here is the row.
    pub fn get(&self, sandbox_id: &SandboxId) -> Option<Option<&'a PausedSandboxEntry>> {
        self.covered
            .contains(sandbox_id)
            .then(|| self.entries.get(sandbox_id))
    }

    /// How many ids this batch answered for, for the caller's own log line.
    pub fn len(&self) -> usize {
        self.covered.len()
    }

    pub fn is_empty(&self) -> bool {
        self.covered.is_empty()
    }
}

/// What [`PausedSandboxRegistry::begin_pause`](super::PausedSandboxRegistry::begin_pause) hands back.
#[derive(Debug, Clone)]
pub struct BeganPause {
    /// The generation the caller must quote when completing or aborting.
    pub generation: i64,
    /// The snapshot the row pointed at before this pause replaced it, if any.
    ///
    /// Nothing references it once the new pause completes, so the caller is
    /// responsible for deleting it — that deletion is deliberately deferred to
    /// here rather than done at resume time, so a sandbox always has one
    /// durable snapshot behind it while it runs.
    pub previous_snapshot_id: Option<SnapshotId>,
}

/// One sandbox a node is reporting itself the holder of, and when that sandbox
/// is currently due to end.
///
/// The deadline travels with the renewal rather than being derived from the
/// stored `metadata` because the two disagree in exactly the case that matters.
/// `metadata` is whatever the sandbox looked like when it was paused; a resume
/// may set a different timeout, and callers extend timeouts on live sandboxes
/// all the time. Reading the deadline out of the row would therefore reclaim
/// sandboxes that still had hours to run.
///
/// `None` means the sandbox has no deadline at all, which is not the same as
/// "unknown": it is a sandbox that was asked never to expire, and reclamation
/// leaves it alone forever.
#[derive(Clone, Copy, Debug)]
pub struct HeldSandbox {
    pub sandbox_id: SandboxId,
    pub expires_at: Option<DateTime<Utc>>,
}

/// What a node's successor process found waiting for it in the registry.
///
/// Both numbers describe sandboxes the previous process on this machine was
/// holding when it died. They are reported separately because they mean
/// different things to an operator: `released` sandboxes are recoverable and
/// will come back on the next resume, `discarded` ones are gone.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReleasedHoldings {
    /// Rows handed back to the cluster as `paused`, recoverable from the
    /// snapshot they name.
    pub released: u64,
    /// Rows deleted because no snapshot was ever published for them, so nothing
    /// remains to rebuild the sandbox from.
    pub discarded: u64,
}

/// What a reclamation pass did to the rows of nodes that stopped reporting.
///
/// Same two outcomes as [`ReleasedHoldings`], for the same reason — the
/// difference is only in what established that the holder is gone: there, being
/// its successor; here, the sandbox outliving its own deadline while nobody
/// renewed for it.
pub type ReclaimedHoldings = ReleasedHoldings;

impl ReleasedHoldings {
    /// Whether anything at all was found, i.e. whether the previous process
    /// died holding sandboxes rather than shutting down cleanly.
    pub fn is_empty(&self) -> bool {
        self.released == 0 && self.discarded == 0
    }
}

/// Outcome of trying to take ownership of a paused sandbox for a resume.
#[derive(Debug)]
pub enum ResumeClaim {
    /// The caller owns the sandbox and must either resume it or release the claim.
    Claimed {
        entry: Box<PausedSandboxEntry>,
        /// What the row said *before* the claim moved it to `Resuming`.
        ///
        /// 🔴 It cannot be read off `entry`: the claim is a single conditional
        /// `UPDATE`, and `RETURNING` hands back the row as the statement left
        /// it — `state` is therefore always `Resuming` there, whatever it was
        /// a moment earlier. Reading it from `entry` is how this claim came to
        /// report every ordinary resume as a lease takeover for months.
        ///
        /// The distinction it carries is not cosmetic. `Paused` means the
        /// snapshot was durable and nothing was lost. `Publishing` /
        /// `LocalOnly` mean the claim overrode a node that never finished
        /// uploading, so the sandbox comes back one snapshot behind and the
        /// work since that snapshot is gone — the one event on this path an
        /// operator has to be able to find.
        previous_state: PausedRegistryState,
    },
    /// No registry row: the sandbox is unknown to the cluster.
    NotFound,
    /// The snapshot is still uploading, so only the origin node can serve this
    /// resume. Carries the origin node so the caller can redirect.
    NotReady { origin_node_id: String },
    /// Another node claimed it first.
    Conflict {
        origin_node_id: String,
        reason: ConflictReason,
    },
}

/// Which of the two situations a [`ResumeClaim::Conflict`] describes.
///
/// They call for opposite responses. `LiveElsewhere` means the sandbox is
/// running on another node and this one must not touch it — retrying is how a
/// second live copy happens. `ClaimLost` means the row is claimable again and
/// this caller merely lost a race, so retrying is exactly right. Flattened into
/// one variant, a caller either retries something it must not or gives up on
/// something it could have had.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictReason {
    /// The sandbox is live on the node named alongside this. Nobody may take it.
    LiveElsewhere,
    /// This caller held the claim and no longer does; the row is claimable.
    ClaimLost,
    /// The backend did not say. Treated as `LiveElsewhere` wherever the two
    /// differ, because that is the answer whose mistake is recoverable.
    Unspecified,
}

/// Which of the three answers [`mark_running`] gave.
///
/// [`mark_running`]: super::PausedSandboxRegistry::mark_running
///
/// 🔴 `Untracked` and `HeldElsewhere` were one `false` until D11, and they mean
/// opposite things. Untracked is the common, healthy case: a sandbox that has
/// never been paused has no row, and the node carries on. HeldElsewhere means a
/// row exists and another node holds the claim on it — two nodes believe they
/// are bringing the same sandbox up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarkRunningOutcome {
    /// No row. What a sandbox that has never been paused looks like.
    Untracked,
    /// The row now names this node as its holder.
    Adopted,
    /// A row exists and another node holds the claim on it.
    HeldElsewhere,
}

impl MarkRunningOutcome {
    /// Whether the cluster tracks this sandbox and now names this node.
    ///
    /// Kept as a helper rather than left to callers comparing variants, so the
    /// two non-adopted cases cannot quietly collapse back into one at a call
    /// site that only wanted the boolean.
    pub fn adopted(self) -> bool {
        matches!(self, Self::Adopted)
    }
}

/// Which of the three answers [`renew_sandbox_deadline`] gave.
///
/// [`renew_sandbox_deadline`]: super::PausedSandboxRegistry::renew_sandbox_deadline
///
/// A bare "did it write" would run together the same two situations
/// [`MarkRunningOutcome`] exists to split apart: a sandbox this cluster never
/// tracked (the common, healthy case — a disabled registry, or a resume whose
/// own `mark_running` has not landed yet) and a row that exists but has moved
/// on to a different incarnation since the caller last observed it `Running`
/// locally. The second case is the one a caller must never retry the same
/// deadline against: the row it would be retrying is not the sandbox that
/// asked for the extension any more.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeadlineRenewalOutcome {
    /// The row now carries the new deadline.
    Renewed,
    /// No row. What a sandbox this cluster does not track looks like.
    NotTracked,
    /// A row exists but is not `Running` under the incarnation this call
    /// named — it moved on since. The deadline was not written.
    Superseded,
}

/// One row of the cluster-wide debug/admin listing (`ListRegistrySandboxes`,
/// `src/node_registry/grpc_service.rs`; `GET /registry/sandboxes` on the
/// gateway, `services/gateway/internal/registry_list.go`) — mirrors Go's
/// `Sandbox` (`registry.go:73-116`) rather than the narrower
/// [`PausedSandboxEntry`].
///
/// It deliberately carries the lease/execution columns
/// [`PausedSandboxEntry`]'s own doc says node-side reads never see: this
/// listing *is* the one reader that is meant to. No `metadata` — the wire
/// response (`RegistrySandbox` in `scheduler.proto`) has no field for it and
/// no consumer of this type ever asks.
#[derive(Clone, Debug)]
pub struct PausedRegistryListEntry {
    pub sandbox_id: SandboxId,
    pub cluster_id: Uuid,
    pub state: PausedRegistryState,
    pub generation: i64,
    pub origin_node_id: String,
    pub claimed_by_node_id: Option<String>,
    pub snapshot_id: Option<SnapshotId>,
    pub paused_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    /// `None` when the column is NULL, which the nodes treat as already
    /// expired — see `RegistryRow::lease_expired`'s identical column.
    pub lease_expires_at: Option<DateTime<Utc>>,
    /// `None` means the sandbox was asked never to expire, never "unknown".
    pub sandbox_expires_at: Option<DateTime<Utc>>,
    /// The incarnation this row is fenced against, `None` when the row's
    /// state pins the column to NULL.
    pub execution_id: Option<ExecutionId>,
}

impl PausedRegistryListEntry {
    /// The node this row makes authoritative for the sandbox — mirrors Go's
    /// `Sandbox.Holder()` (`registry.go:126-139`): always `origin_node_id`,
    /// never `claimed_by_node_id`. See that method's own doc for why.
    pub fn holder(&self) -> &str {
        &self.origin_node_id
    }
}

/// One read of the whole registry: every row in scope, plus the database
/// clock they were read against. Mirrors Go's `Listing` (`registry.go:165-170`).
///
/// The two travel together deliberately — every lease judgement
/// (`LeaseExpiresAtUnixMs`/`SandboxExpiresAtUnixMs` against
/// `DatabaseNowUnixMs` on the wire) is a comparison against the database
/// clock the rows were actually read against, never the reader's own.
#[derive(Clone, Debug)]
pub struct PausedRegistryListing {
    pub sandboxes: Vec<PausedRegistryListEntry>,
    pub now: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(sandbox_id: SandboxId) -> PausedSandboxEntry {
        PausedSandboxEntry {
            sandbox_id,
            cluster_id: Uuid::nil(),
            state: PausedRegistryState::Paused,
            generation: 1,
            origin_node_id: "node-a".to_string(),
            claimed_by_node_id: None,
            snapshot_id: None,
            metadata: None,
            execution_id: None,
            paused_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    /// 🔴 The three answers a destructive caller has to tell apart, and the one
    /// assertion that says they are told apart: an id the batch did not cover
    /// must not be reachable as an absence.
    #[test]
    fn answered_rows_separates_an_uncovered_id_from_a_real_absence() {
        let present = SandboxId::new();
        let absent = SandboxId::new();
        let unreadable = SandboxId::new();

        let rows = PausedRegistryRows {
            entries: HashMap::from([(present, entry(present))]),
            // `unreadable` was asked about and is deliberately not covered --
            // the shape a backend returns for a row it could not decode.
            covered: vec![present, absent],
        };
        let answered = rows.answered();

        assert!(
            matches!(answered.get(&present), Some(Some(_))),
            "a covered id with a row answers with the row"
        );
        assert!(
            matches!(answered.get(&absent), Some(None)),
            "a covered id with no row is a real absence the caller may act on"
        );
        assert!(
            answered.get(&unreadable).is_none(),
            "🔴 an uncovered id must answer 'do not judge', never 'no row'. \
             Collapsing this into Some(None) is what tore down live sandboxes"
        );
    }

    #[test]
    fn a_fully_covering_batch_covers_exactly_what_it_was_asked() {
        let first = SandboxId::new();
        let second = SandboxId::new();
        let ids = [first, second];

        let rows = PausedRegistryRows::fully_covering(HashMap::from([(first, entry(first))]), &ids);

        assert!(rows.covers(&ids));
        assert_eq!(rows.answered().len(), 2);
        assert!(matches!(rows.answered().get(&second), Some(None)));
    }

    /// A default batch has looked at nothing, so it answers for nothing --
    /// the `DisabledPausedSandboxRegistry` shape.
    #[test]
    fn a_default_batch_answers_for_nothing() {
        let rows = PausedRegistryRows::default();
        let id = SandboxId::new();

        assert!(rows.answered().is_empty());
        assert!(rows.answered().get(&id).is_none());
        assert!(!rows.covers(&[id]));
        assert!(rows.covers(&[]));
    }

    /// `as_str` and `parse` must stay inverses: these five strings are the
    /// column's CHECK constraint and the proto's filter vocabulary, so a
    /// one-sided edit is a silent schema mismatch.
    #[test]
    fn every_state_round_trips_through_its_wire_literal() {
        for state in PausedRegistryState::ALL {
            assert_eq!(
                PausedRegistryState::parse(state.as_str()),
                Some(state),
                "{state:?} must decode from the literal it encodes to"
            );
        }
        assert_eq!(PausedRegistryState::ALL.len(), 5);
    }
}
