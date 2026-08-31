//! Shared strict decoding for every `paused_sandboxes` query using
//! [`super::sql::ENTRY_COLUMNS`].

use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::super::types::{PausedRegistryListEntry, PausedRegistryState};
use super::super::{PausedRegistryError, PausedSandboxEntry, RegistryResult};
use crate::orchestrator::store::SandboxMetadata;
use crate::snapshot::SnapshotId;
use crate::types::{ExecutionId, SandboxId};

/// Lossless SQL row matching [`super::sql::ENTRY_COLUMNS`].
#[derive(Debug, sqlx::FromRow)]
pub struct EntryRow {
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

/// Claim row with the state that preceded the update.
#[derive(Debug, sqlx::FromRow)]
pub struct ClaimRow {
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

/// Decodes the claimed entry and its pre-claim state.
pub fn decode_claim(row: ClaimRow) -> RegistryResult<(PausedSandboxEntry, PausedRegistryState)> {
    let previous_state = PausedRegistryState::parse(&row.previous_state).ok_or_else(|| {
        invalid(
            &row.sandbox_id,
            format!("unknown previous state '{}'", row.previous_state),
        )
    })?;
    let entry = decode_entry(row.into_entry_row())?;
    Ok((entry, previous_state))
}

/// Fully parsed internal registry row.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct RegistryRow {
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
    /// Applies `COALESCE(lease_expires_at, updated_at) < now`.
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
    // Durable paused rows must name the snapshot they can restore.
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

/// Decodes a trait-facing paused entry, including metadata.
pub fn decode_entry(row: EntryRow) -> RegistryResult<PausedSandboxEntry> {
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

/// Decodes the administrative listing fields.
pub fn decode_list_entry(row: EntryRow) -> RegistryResult<PausedRegistryListEntry> {
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

/// Decodes the complete internal row without parsing unused metadata.
pub fn decode_registry_row(row: EntryRow) -> RegistryResult<RegistryRow> {
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
