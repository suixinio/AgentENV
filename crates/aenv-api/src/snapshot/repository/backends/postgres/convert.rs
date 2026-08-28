//! The row and the domain, and the one place either is turned into the
//! other.
//!
//! 🔴 Same contract as the proto version: a row this build cannot make sense
//! of must reach the caller as an error, never as a plausible-looking record
//! it invented by guessing. `status` is read and matched by name, not
//! flattened; an unknown value is refused here, by name.

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

/// Which encoding of [`CommittedSnapshot`] this build writes into
/// `committed_payload`. Matches `central/convert.rs`'s `COMMITTED_PAYLOAD_SCHEMA`
/// — the two backends encode the same Rust type the same way (`serde_json`),
/// so there is no reason for the version numbers to diverge, and every
/// reason for them not to: a payload this build wrote through one backend
/// must decode through the other unchanged.
pub const COMMITTED_PAYLOAD_SCHEMA: i32 = 1;

pub const STATUS_WAITING: &str = "waiting";
pub const STATUS_BUILDING: &str = "building";
pub const STATUS_READY: &str = "ready";
pub const STATUS_ERROR: &str = "error";

pub const SOURCE_KIND_TEMPLATE: &str = "template";
pub const SOURCE_KIND_SANDBOX: &str = "sandbox";

/// One row as `reads.rs`'s queries scan it — column order matches
/// `SNAPSHOT_COLUMNS` in that file exactly; [`sqlx::FromRow`] below binds by
/// name, not position, but the two are kept in the same order anyway so a
/// reviewer can check them side by side.
///
/// 🔴 A hand-written [`sqlx::FromRow`] impl rather than `#[derive(FromRow)]`
/// on purpose — the derive lives behind sqlx's `macros` feature, which this
/// crate does not enable (`Cargo.toml`'s own comment: runtime-query API
/// only, `sqlx::query`/`query_as`, never the compile-time macros). This impl
/// costs the same handful of `try_get` calls the derive would generate.
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

/// Turns one row into a record, refusing anything it cannot read — never a
/// plausible-looking record for a row it did not understand.
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
                // 🔴 Deliberately `None`/`None` here, unlike the central
                // backend's `decode_row`. The build's own timestamps are a
                // LEFT JOIN LATERAL onto the newest `builds` row in Go's
                // query, which this module's `reads.rs` does not join —
                // nothing in the `SnapshotCatalog` trait surface this backs
                // reads `build_started_at_ms`/`build_finished_at_ms` off a
                // `SnapshotRecord`; the record's own `TemplateBuildInfo` only
                // carries them for the object-store backends' benefit, which
                // populate them from their own local bookkeeping. Wiring the
                // join is tracked in the Stage B report's "not done" list
                // rather than silently answered with a wrong value.
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

/// Encodes a committed payload for the `bytea` column — matches
/// `central/convert.rs::encode_committed` (same type, same `serde_json`
/// encoding, same schema version).
pub fn encode_committed(committed: &CommittedSnapshot) -> RepositoryResult<Vec<u8>> {
    serde_json::to_vec(committed).map_err(|error| RepositoryError::Backend {
        message: "serialize committed snapshot payload for the catalog".to_string(),
        source: Some(error.into()),
    })
}

/// Encodes a build failure for the `jsonb` column. Always an object — see
/// `central/convert.rs::encode_build_error`'s comment on why a bare-string
/// encoding (which `TemplateBuildErrorReason`'s `Deserialize` also accepts,
/// for legacy rows) is never written here.
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
