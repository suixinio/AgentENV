//! Strict conversion between PostgreSQL catalog rows and snapshot records.
//! Unknown statuses, source kinds, or malformed payloads are errors.

use serde::Deserialize;
use sqlx::postgres::PgRow;
use sqlx::Row;
use uuid::Uuid;

use crate::snapshot::repository::{RepositoryError, RepositoryResult};
use crate::snapshot::types::{
    CommittedSnapshot, SnapshotAlias, SnapshotId, SnapshotRecord, SnapshotSource,
    TemplateBuildErrorReason, TemplateBuildInfo, TemplateBuildStatus,
};
use crate::types::SandboxResources;

/// Schema version shared with the central backend's committed payload.
pub const COMMITTED_PAYLOAD_SCHEMA: i32 = 1;

pub const STATUS_WAITING: &str = "waiting";
pub const STATUS_BUILDING: &str = "building";
pub const STATUS_READY: &str = "ready";
pub const STATUS_ERROR: &str = "error";

pub const SOURCE_KIND_TEMPLATE: &str = "template";
pub const SOURCE_KIND_SANDBOX: &str = "sandbox";

/// Runtime-decoded row matching the catalog query columns.
///
/// Implemented manually because this crate does not enable sqlx macros.
pub struct CatalogRow {
    pub id: String,
    pub cluster_id: String,
    pub source_kind: String,
    pub source_sandbox_id: Option<String>,
    pub cpu_count: i32,
    pub memory_mib: i32,
    pub disk_size_mib: i32,
    pub status: String,
    #[allow(dead_code)] // Scanned for column-order parity with the SQL; not read — see decode_row.
    pub status_group: String,
    pub alias: Option<String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub committed_payload: Option<Vec<u8>>,
    pub committed_schema: Option<i32>,
    pub build_error: Option<serde_json::Value>,
    #[allow(dead_code)] // Projected for parity with the Go row; not consumed by the general trait.
    pub published: bool,
    #[allow(dead_code)]
    pub origin_node_id: Option<String>,
}

impl<'r> sqlx::FromRow<'r, PgRow> for CatalogRow {
    fn from_row(row: &'r PgRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            cluster_id: row.try_get("cluster_id")?,
            source_kind: row.try_get("source_kind")?,
            source_sandbox_id: row.try_get("source_sandbox_id")?,
            cpu_count: row.try_get("cpu_count")?,
            memory_mib: row.try_get("memory_mib")?,
            disk_size_mib: row.try_get("disk_size_mib")?,
            status: row.try_get("status")?,
            status_group: row.try_get("status_group")?,
            alias: row.try_get("alias")?,
            created_at_ms: row.try_get("created_at_ms")?,
            updated_at_ms: row.try_get("updated_at_ms")?,
            committed_payload: row.try_get("committed_payload")?,
            committed_schema: row.try_get("committed_schema")?,
            build_error: row.try_get("build_error")?,
            published: row.try_get("published")?,
            origin_node_id: row.try_get("origin_node_id")?,
        })
    }
}

fn malformed(row_id: &str, reason: impl Into<String>) -> RepositoryError {
    RepositoryError::Backend {
        message: format!(
            "snapshot catalog answered off contract for '{row_id}': {}",
            reason.into()
        ),
        source: None,
    }
}

/// Strictly decodes one catalog row for `expected_cluster`.
pub fn decode_row(row: CatalogRow, expected_cluster: Uuid) -> RepositoryResult<SnapshotRecord> {
    let id = SnapshotId::parse(&row.id).map_err(|_| malformed(&row.id, "id is not a uuid"))?;

    let cluster_id = Uuid::parse_str(&row.cluster_id)
        .map_err(|_| malformed(&row.id, "cluster id is not a uuid"))?;
    if cluster_id != expected_cluster {
        return Err(malformed(
            &row.id,
            format!(
                "row belongs to cluster '{cluster_id}' but this replica is in cluster \
                 '{expected_cluster}'"
            ),
        ));
    }

    let alias = match &row.alias {
        Some(alias) if !alias.is_empty() => Some(
            SnapshotAlias::parse(alias).map_err(|error| malformed(&row.id, error.to_string()))?,
        ),
        _ => None,
    };

    let status = decode_status(&row.id, &row.status)?;

    let source = match row.source_kind.as_str() {
        SOURCE_KIND_SANDBOX => {
            let source_sandbox_id = row.source_sandbox_id.clone().unwrap_or_default();
            if source_sandbox_id.is_empty() {
                return Err(malformed(
                    &row.id,
                    "a sandbox row must name the sandbox it was captured from",
                ));
            }
            SnapshotSource::Sandbox { source_sandbox_id }
        }
        SOURCE_KIND_TEMPLATE => SnapshotSource::Template {
            build: TemplateBuildInfo {
                status,
                // Build timestamps are not selected by this catalog query.
                started_at_unix_ms: None,
                finished_at_unix_ms: None,
                error_reason: decode_build_error(&row.id, row.build_error.as_ref())?,
            },
        },
        other => {
            return Err(malformed(&row.id, format!("unknown source kind '{other}'")));
        }
    };

    let committed = decode_committed(
        &row.id,
        row.committed_payload.as_deref(),
        row.committed_schema,
    )?;
    if status == TemplateBuildStatus::Ready && committed.is_none() {
        return Err(malformed(
            &row.id,
            "a ready row carries no committed payload",
        ));
    }

    Ok(SnapshotRecord {
        id,
        alias,
        source,
        resources: SandboxResources {
            cpu_count: row.cpu_count.max(0) as u32,
            memory_mib: row.memory_mib.max(0) as u32,
            disk_size_mib: row.disk_size_mib.max(0) as u32,
        },
        created_at_unix_ms: row.created_at_ms,
        updated_at_unix_ms: row.updated_at_ms,
        committed,
    })
}

fn decode_status(row_id: &str, status: &str) -> RepositoryResult<TemplateBuildStatus> {
    match status {
        STATUS_WAITING => Ok(TemplateBuildStatus::Waiting),
        STATUS_BUILDING => Ok(TemplateBuildStatus::Building),
        STATUS_READY => Ok(TemplateBuildStatus::Ready),
        STATUS_ERROR => Ok(TemplateBuildStatus::Error),
        other => Err(malformed(row_id, format!("unknown status '{other}'"))),
    }
}

fn decode_build_error(
    row_id: &str,
    raw: Option<&serde_json::Value>,
) -> RepositoryResult<Option<TemplateBuildErrorReason>> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let reason: TemplateBuildErrorReason = TemplateBuildErrorReason::deserialize(raw)
        .map_err(|error| malformed(row_id, format!("build_error is not readable: {error}")))?;
    Ok(Some(reason))
}

fn decode_committed(
    row_id: &str,
    payload: Option<&[u8]>,
    schema: Option<i32>,
) -> RepositoryResult<Option<CommittedSnapshot>> {
    match (payload, schema) {
        (None, None) => Ok(None),
        (Some(payload), Some(version)) => {
            if version != COMMITTED_PAYLOAD_SCHEMA {
                return Err(malformed(
                    row_id,
                    format!(
                        "committed payload is schema {version}, this build only reads \
                         {COMMITTED_PAYLOAD_SCHEMA}"
                    ),
                ));
            }
            serde_json::from_slice(payload)
                .map(Some)
                .map_err(|error| malformed(row_id, format!("committed payload: {error}")))
        }
        _ => Err(malformed(
            row_id,
            "committed_payload and committed_schema must both be present or both absent",
        )),
    }
}

/// Encodes the versioned committed payload as JSON bytes.
pub fn encode_committed(committed: &CommittedSnapshot) -> RepositoryResult<Vec<u8>> {
    serde_json::to_vec(committed).map_err(|error| RepositoryError::Backend {
        message: "serialize committed snapshot payload for the catalog".to_string(),
        source: Some(error.into()),
    })
}

/// Encodes build failure details as a JSON object.
pub fn encode_build_error(reason: &TemplateBuildErrorReason) -> serde_json::Value {
    serde_json::json!({"message": reason.message, "step": reason.step})
}

pub fn opening_status(record: &SnapshotRecord) -> &'static str {
    match &record.source {
        SnapshotSource::Template { build } => match build.status {
            TemplateBuildStatus::Building => STATUS_BUILDING,
            _ => STATUS_WAITING,
        },
        SnapshotSource::Sandbox { .. } => STATUS_BUILDING,
    }
}

pub fn source_kind_str(record: &SnapshotRecord) -> &'static str {
    match &record.source {
        SnapshotSource::Sandbox { .. } => SOURCE_KIND_SANDBOX,
        SnapshotSource::Template { .. } => SOURCE_KIND_TEMPLATE,
    }
}

pub fn source_sandbox_id(record: &SnapshotRecord) -> Option<String> {
    match &record.source {
        SnapshotSource::Sandbox { source_sandbox_id } => Some(source_sandbox_id.clone()),
        SnapshotSource::Template { .. } => None,
    }
}
