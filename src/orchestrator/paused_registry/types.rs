use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::orchestrator::store::SandboxMetadata;
use crate::snapshot::SnapshotId;
use crate::types::{ExecutionId, SandboxId};

/// Durable lifecycle of a paused-registry row, retained through resumed runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PausedRegistryState {
    /// Paused locally while the snapshot is not yet durable.
    Publishing,
    /// The snapshot is durable in the repository. Any node can resume it.
    Paused,
    /// A node has claimed this sandbox and is bringing it back up.
    Resuming,
    /// Paused with no durable snapshot; only the origin node can resume it.
    LocalOnly,
    /// Live on `origin_node_id`, retaining the last durable snapshot.
    Running,
}

impl PausedRegistryState {
    /// All valid database state values.
    pub const ALL: [Self; 5] = [
        Self::Publishing,
        Self::Paused,
        Self::Resuming,
        Self::LocalOnly,
        Self::Running,
    ];

    /// Decodes a registry state column value.
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

    /// Encodes the schema and wire state value.
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

/// Cluster-visible paused-sandbox record.
/// Artifacts remain in the snapshot repository; `metadata` reconstructs identity.
#[derive(Clone, Debug)]
pub struct PausedSandboxEntry {
    pub sandbox_id: SandboxId,
    pub cluster_id: Uuid,
    pub state: PausedRegistryState,
    /// Generation fence supplied by callers on later mutations.
    pub generation: i64,
    /// Node holding the authoritative local copy or running sandbox.
    pub origin_node_id: String,
    /// Resume claimant, set only while `Resuming`.
    pub claimed_by_node_id: Option<String>,
    /// `None` while `state == Publishing`.
    pub snapshot_id: Option<SnapshotId>,
    /// Full metadata on write and granted claims; bulk reads may omit it.
    /// `None` never means the sandbox lacks a record.
    pub metadata: Option<SandboxMetadata>,
    /// Incarnation fenced by this row; granted claims must run under it.
    pub execution_id: Option<ExecutionId>,
    pub paused_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Batched rows plus every id the backend can authoritatively answer for.
/// Destructive callers may act on absence only for covered ids; unreadable rows
/// must remain uncovered.
#[derive(Debug, Default)]
pub struct PausedRegistryRows {
    pub entries: HashMap<SandboxId, PausedSandboxEntry>,
    pub covered: Vec<SandboxId>,
}

impl PausedRegistryRows {
    /// Whether this batch answered every requested id.
    pub fn covers(&self, ids: &[SandboxId]) -> bool {
        self.covered.len() == ids.len()
    }

    /// Builds a lookup view with O(1) coverage checks.
    pub fn answered(&self) -> AnsweredRows<'_> {
        AnsweredRows {
            entries: &self.entries,
            covered: self.covered.iter().copied().collect(),
        }
    }

    /// Builds a batch covering every supplied id.
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

/// Coverage-aware view of batched paused-registry rows.
pub struct AnsweredRows<'a> {
    entries: &'a HashMap<SandboxId, PausedSandboxEntry>,
    covered: HashSet<SandboxId>,
}

impl<'a> AnsweredRows<'a> {
    /// Returns `None` when uncovered, `Some(None)` for confirmed absence,
    /// and `Some(Some(entry))` for a covered row.
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

/// Result of beginning a pause.
#[derive(Debug, Clone)]
pub struct BeganPause {
    /// The generation the caller must quote when completing or aborting.
    pub generation: i64,
    /// The snapshot the row pointed at before this pause replaced it, if any.
    /// Snapshot superseded by this pause; the caller retires it after completion.
    pub previous_snapshot_id: Option<SnapshotId>,
}

/// A sandbox held by a node and its current deadline.
/// `None` means the sandbox never expires.
#[derive(Clone, Copy, Debug)]
pub struct HeldSandbox {
    pub sandbox_id: SandboxId,
    pub expires_at: Option<DateTime<Utc>>,
}

/// Holdings recovered from a previous process on the same node.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReleasedHoldings {
    /// Rows handed back to the cluster as `paused`, recoverable from the
    /// snapshot they name.
    pub released: u64,
    /// Rows deleted because no snapshot was ever published for them, so nothing
    /// remains to rebuild the sandbox from.
    pub discarded: u64,
}

/// Holdings reclaimed after lease and sandbox deadlines expire.
pub type ReclaimedHoldings = ReleasedHoldings;

impl ReleasedHoldings {
    /// Whether no holdings were found.
    pub fn is_empty(&self) -> bool {
        self.released == 0 && self.discarded == 0
    }
}

/// Result of claiming a paused sandbox for resume.
#[derive(Debug)]
pub enum ResumeClaim {
    /// The caller owns the sandbox and must either resume it or release the claim.
    Claimed {
        entry: Box<PausedSandboxEntry>,
        /// State before the claim atomically changed it to `Resuming`.
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

/// Reason another claimant may not proceed.
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

/// Result of recording a resumed sandbox as running.
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
    /// Whether the row now names this node.
    pub fn adopted(self) -> bool {
        matches!(self, Self::Adopted)
    }
}

/// Result of updating a running sandbox deadline under its incarnation fence.
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

/// Registry row used by cluster-wide admin listings.
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
    /// Database lease deadline; `None` is treated as expired.
    pub lease_expires_at: Option<DateTime<Utc>>,
    /// `None` means the sandbox was asked never to expire, never "unknown".
    pub sandbox_expires_at: Option<DateTime<Utc>>,
    /// Incarnation fenced by this row, absent in states with no run.
    pub execution_id: Option<ExecutionId>,
}

impl PausedRegistryListEntry {
    /// Returns the authoritative holder, always `origin_node_id`.
    pub fn holder(&self) -> &str {
        &self.origin_node_id
    }
}

/// Whole-registry listing paired with the database clock used for lease judgments.
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

    #[test]
    fn answered_rows_separates_an_uncovered_id_from_a_real_absence() {
        let present = SandboxId::new();
        let absent = SandboxId::new();
        let unreadable = SandboxId::new();

        let rows = PausedRegistryRows {
            entries: HashMap::from([(present, entry(present))]),
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

    #[test]
    fn a_default_batch_answers_for_nothing() {
        let rows = PausedRegistryRows::default();
        let id = SandboxId::new();

        assert!(rows.answered().is_empty());
        assert!(rows.answered().get(&id).is_none());
        assert!(!rows.covers(&[id]));
        assert!(rows.covers(&[]));
    }

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
