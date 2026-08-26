//! Row decoding shared by every query that reads `paused_sandboxes` through
//! [`super::sql::ENTRY_COLUMNS`] -- the Rust analogue of Go's `scanEntry`/
//! `scanClaim` (`store_postgres.go:1930-2033`).
//!
//! Mirrors `central.rs`'s own `decode_entry`: refuse anything that cannot be
//! read rather than pass on a plausible-looking record, and treat metadata
//! the same way that backend already does (decoded into
//! [`SandboxMetadata`] at the trait boundary) even though Go's own `Entry`
//! keeps it as opaque `json.RawMessage` -- that split is a pre-existing
//! choice of the Rust trait layer this backend has to honour, not something
//! introduced here.

use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::super::types::{PausedRegistryListEntry, PausedRegistryState};
use super::super::{PausedRegistryError, PausedSandboxEntry, RegistryResult};
use crate::orchestrator::store::SandboxMetadata;
use crate::snapshot::SnapshotId;
use crate::types::{ExecutionId, SandboxId};

/// Every column [`super::sql::ENTRY_COLUMNS`] selects, decoded only as far as
/// `sqlx` can do losslessly (uuid columns stay `String`, matching Go's own
/// `::text` casts and its comment on why: independence from whichever uuid
/// codec the driver happens to register).
#[derive(Debug, sqlx::FromRow)]
pub(super) struct EntryRow {
    pub sandbox_id: String,
    pub cluster_id: String,
    pub state: String,
    pub generation: i64,
    pub origin_node_id: String,
    pub claimed_by_node_id: Option<String>,
    pub snapshot_id: Option<String>,
    pub metadata: serde_json::Value,
    pub paused_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub sandbox_expires_at: Option<DateTime<Utc>>,
    pub execution_id: Option<String>,
    pub execution_started_at: Option<DateTime<Utc>>,
}

/// `claim_for_resume_sql`/`claim_for_resume_durable_only_sql`'s result set:
/// [`ENTRY_COLUMNS`](super::sql::ENTRY_COLUMNS) plus the trailing
/// `previous_state` column those two statements alone add (`claimed.*,
/// previous.previous_state` -- `scanClaim`, `store_postgres.go:1980-2033`).
#[derive(Debug, sqlx::FromRow)]
pub(super) struct ClaimRow {
    pub sandbox_id: String,
    pub cluster_id: String,
    pub state: String,
    pub generation: i64,
    pub origin_node_id: String,
    pub claimed_by_node_id: Option<String>,
    pub snapshot_id: Option<String>,
    pub metadata: serde_json::Value,
    pub paused_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub sandbox_expires_at: Option<DateTime<Utc>>,
    pub execution_id: Option<String>,
    pub execution_started_at: Option<DateTime<Utc>>,
    pub previous_state: String,
}

impl ClaimRow {
    fn into_entry_row(self) -> EntryRow {
        EntryRow {
            sandbox_id: self.sandbox_id,
            cluster_id: self.cluster_id,
            state: self.state,
            generation: self.generation,
            origin_node_id: self.origin_node_id,
            claimed_by_node_id: self.claimed_by_node_id,
            snapshot_id: self.snapshot_id,
            metadata: self.metadata,
            paused_at: self.paused_at,
            updated_at: self.updated_at,
            lease_expires_at: self.lease_expires_at,
            sandbox_expires_at: self.sandbox_expires_at,
            execution_id: self.execution_id,
            execution_started_at: self.execution_started_at,
        }
    }
}

/// Decodes a claim result into the entry plus the state it moved *from* --
/// the trait's [`super::super::ResumeClaim::Claimed::previous_state`] must
/// never be read off the post-UPDATE row (always `Resuming`), which is
/// exactly the bug [`super::super::types`]'s own doc warns silently
/// mis-reported every ordinary resume as a takeover for months.
pub(super) fn decode_claim(
    row: ClaimRow,
) -> RegistryResult<(PausedSandboxEntry, PausedRegistryState)> {
    let previous_state = PausedRegistryState::parse(&row.previous_state).ok_or_else(|| {
        invalid(
            &row.sandbox_id,
            format!("unknown previous state '{}'", row.previous_state),
        )
    })?;
    let entry = decode_entry(row.into_entry_row())?;
    Ok((entry, previous_state))
}

/// The same row, fully parsed -- for internal callers ([`super::reconcile`],
/// [`super::reclaim`]) that need the lease/execution-start columns Go's own
/// `Sandbox` type (`registry.go`) carries and the trait-facing
/// [`PausedSandboxEntry`] does not.
///
/// 🔴 Not every field is read by [`super::reconcile::compute_reconcile`]
/// today (`cluster_id`/`generation`/`claimed_by_node_id`/`paused_at`/
/// `execution_id`/`execution_started_at`) -- this type mirrors Go's
/// `Sandbox` row shape in full deliberately, matching every column
/// [`super::sql::ENTRY_COLUMNS`] actually selects, rather than trimming it
/// down to today's one consumer's exact needs. A future internal reader
/// (a debug/inspection endpoint, a richer reconcile pass) should not have to
/// widen a narrowed struct or add a second query.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(super) struct RegistryRow {
    pub sandbox_id: SandboxId,
    pub cluster_id: Uuid,
    pub state: PausedRegistryState,
    pub generation: i64,
    pub origin_node_id: String,
    pub claimed_by_node_id: Option<String>,
    pub snapshot_id: Option<SnapshotId>,
    pub paused_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub sandbox_expires_at: Option<DateTime<Utc>>,
    pub execution_id: Option<ExecutionId>,
    pub execution_started_at: Option<DateTime<Utc>>,
}

impl RegistryRow {
    /// `LEASE_EXPIRED` (`store_postgres.go`'s `leaseExpired`), evaluated in
    /// Rust for callers (`super::reconcile`) that need the same predicate
    /// without a round trip -- `COALESCE(lease_expires_at, updated_at) <
    /// now`. A row written before the lease column existed reads as already
    /// expired, the safe direction: a live holder refreshes it within one
    /// interval, a dead one never does.
    pub fn lease_expired(&self, now: DateTime<Utc>) -> bool {
        self.lease_expires_at.unwrap_or(self.updated_at) < now
    }
}

fn invalid(sandbox_id: &str, reason: impl Into<String>) -> PausedRegistryError {
    PausedRegistryError::InvalidRecord {
        sandbox_id: sandbox_id.to_string(),
        reason: reason.into(),
        source: None,
    }
}

/// Parses the identity/state columns every caller needs regardless of
/// whether it wants metadata decoded -- shared by [`decode_entry`] and
/// [`decode_registry_row`].
struct ParsedIdentity {
    sandbox_id: SandboxId,
    cluster_id: Uuid,
    state: PausedRegistryState,
    snapshot_id: Option<SnapshotId>,
    execution_id: Option<ExecutionId>,
}

fn parse_identity(row: &EntryRow) -> RegistryResult<ParsedIdentity> {
    let sandbox_id = SandboxId::parse_str(&row.sandbox_id)
        .map_err(|_| invalid(&row.sandbox_id, "sandbox id is not a uuid"))?;
    let cluster_id = Uuid::parse_str(&row.cluster_id)
        .map_err(|_| invalid(&row.sandbox_id, "cluster id is not a uuid"))?;
    let state = PausedRegistryState::parse(&row.state)
        .ok_or_else(|| invalid(&row.sandbox_id, format!("unknown state '{}'", row.state)))?;
    let snapshot_id = match row.snapshot_id.as_deref() {
        None | Some("") => None,
        Some(raw) => {
            Some(SnapshotId::parse(raw).map_err(|e| invalid(&row.sandbox_id, e.to_string()))?)
        }
    };
    // A durable row must name the snapshot it can be rebuilt from -- mirrors
    // `Sandbox.Invalid()` (registry.go:158-160) and `central.rs::decode_entry`'s
    // identical check.
    if state == PausedRegistryState::Paused && snapshot_id.is_none() {
        return Err(invalid(
            &row.sandbox_id,
            "paused entry carries no snapshot reference",
        ));
    }
    let execution_id = match row.execution_id.as_deref() {
        None | Some("") => None,
        Some(raw) => Some(
            ExecutionId::parse_str(raw)
                .map_err(|_| invalid(&row.sandbox_id, "execution id is not a uuid"))?,
        ),
    };
    Ok(ParsedIdentity {
        sandbox_id,
        cluster_id,
        state,
        snapshot_id,
        execution_id,
    })
}

/// The trait-facing decode: [`PausedSandboxEntry`]'s 11 fields, metadata
/// decoded into [`SandboxMetadata`] (mirrors `central.rs::decode_entry`).
pub(super) fn decode_entry(row: EntryRow) -> RegistryResult<PausedSandboxEntry> {
    let identity = parse_identity(&row)?;
    let metadata: SandboxMetadata =
        serde_json::from_value(row.metadata).map_err(|e| PausedRegistryError::InvalidRecord {
            sandbox_id: row.sandbox_id.clone(),
            reason: "metadata is not a sandbox record".to_string(),
            source: Some(e.into()),
        })?;

    Ok(PausedSandboxEntry {
        sandbox_id: identity.sandbox_id,
        cluster_id: identity.cluster_id,
        state: identity.state,
        generation: row.generation,
        origin_node_id: row.origin_node_id,
        claimed_by_node_id: row.claimed_by_node_id.filter(|node| !node.is_empty()),
        snapshot_id: identity.snapshot_id,
        metadata: Some(metadata),
        execution_id: identity.execution_id,
        paused_at: row.paused_at,
        updated_at: row.updated_at,
    })
}

/// The `ListRegistrySandboxes` decode: [`PausedRegistryListEntry`]'s
/// columns -- the lease/execution-id fields [`decode_entry`] leaves out
/// (see that struct's own doc), metadata left undecoded like
/// [`decode_registry_row`] (the wire response has no field for it and
/// nothing downstream reads it).
pub(super) fn decode_list_entry(row: EntryRow) -> RegistryResult<PausedRegistryListEntry> {
    let identity = parse_identity(&row)?;
    Ok(PausedRegistryListEntry {
        sandbox_id: identity.sandbox_id,
        cluster_id: identity.cluster_id,
        state: identity.state,
        generation: row.generation,
        origin_node_id: row.origin_node_id,
        claimed_by_node_id: row.claimed_by_node_id.filter(|node| !node.is_empty()),
        snapshot_id: identity.snapshot_id,
        paused_at: row.paused_at,
        updated_at: row.updated_at,
        lease_expires_at: row.lease_expires_at,
        sandbox_expires_at: row.sandbox_expires_at,
        execution_id: identity.execution_id,
    })
}

/// The internal decode: every column, metadata left undecoded (nothing
/// internal to this backend needs it -- reconcile/reclaim never read it).
pub(super) fn decode_registry_row(row: EntryRow) -> RegistryResult<RegistryRow> {
    let identity = parse_identity(&row)?;
    Ok(RegistryRow {
        sandbox_id: identity.sandbox_id,
        cluster_id: identity.cluster_id,
        state: identity.state,
        generation: row.generation,
        origin_node_id: row.origin_node_id,
        claimed_by_node_id: row.claimed_by_node_id.filter(|node| !node.is_empty()),
        snapshot_id: identity.snapshot_id,
        paused_at: row.paused_at,
        updated_at: row.updated_at,
        lease_expires_at: row.lease_expires_at,
        sandbox_expires_at: row.sandbox_expires_at,
        execution_id: identity.execution_id,
        execution_started_at: row.execution_started_at,
    })
}
