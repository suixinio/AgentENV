//! The wire and the domain, and the one place either is turned into the other.
//!
//! 🔴 The contract this file has to keep is the one in the proto's own header:
//! the server sees scalar columns and two opaque blobs, and nothing else. Every
//! type that is *not* a scalar — `CommittedSnapshot`, `OverlaybdLayerRef`,
//! `ManagedLayer`, `CommittedAttachedDrive`, `PersistedDiskImagePublication`,
//! `ImageConfigs`, `CustomExtensionParams`, `TemplateBuildErrorReason` —
//! crosses as bytes this module encodes and decodes, and has no counterpart on
//! the other side. That is what keeps this seam from repeating the twelve
//! "Rust semantics that cannot be expressed on a Go interface" the paused
//! registry's contract tests had to write down.
//!
//! The other half of the contract is that a value the server does not
//! understand must arrive here intact rather than flattened. `status` and
//! `source_kind` are strings for that reason, and a value this build has not
//! heard of is refused *here*, by name — never silently mapped onto something
//! plausible.

use uuid::Uuid;

use crate::proto::scheduler as pb;
use crate::snapshot::repository::{RepositoryError, RepositoryResult};
use crate::snapshot::types::{
    CommittedSnapshot, SnapshotAlias, SnapshotId, SnapshotRecord, SnapshotSource,
    SnapshotSourceKind, TemplateBuildErrorReason, TemplateBuildInfo, TemplateBuildStatus,
};
use crate::types::SandboxResources;

/// Which encoding of [`CommittedSnapshot`] this build writes.
///
/// 🔴 Sent on every commit and checked on every read. The column exists so that
/// a payload written by a build whose `CommittedSnapshot` has since changed
/// shape is refused rather than silently half-decoded — serde would happily
/// drop fields it does not know and hand back a snapshot missing its memory
/// layers. Bump it when the encoding stops being backward compatible, never for
/// an added optional field.
pub const COMMITTED_PAYLOAD_SCHEMA: u32 = 1;

pub const STATUS_WAITING: &str = "waiting";
pub const STATUS_BUILDING: &str = "building";
pub const STATUS_READY: &str = "ready";
pub const STATUS_ERROR: &str = "error";

pub const SOURCE_KIND_TEMPLATE: &str = "template";
pub const SOURCE_KIND_SANDBOX: &str = "sandbox";

pub fn source_kind_str(kind: SnapshotSourceKind) -> &'static str {
    match kind {
        SnapshotSourceKind::Template => SOURCE_KIND_TEMPLATE,
        SnapshotSourceKind::Sandbox => SOURCE_KIND_SANDBOX,
    }
}

pub fn build_status_str(status: TemplateBuildStatus) -> &'static str {
    match status {
        TemplateBuildStatus::Waiting => STATUS_WAITING,
        TemplateBuildStatus::Building => STATUS_BUILDING,
        TemplateBuildStatus::Ready => STATUS_READY,
        TemplateBuildStatus::Error => STATUS_ERROR,
    }
}

/// The status a record is asking the catalog to open its row in.
///
/// A sandbox record has no build state of its own — a pause is `building` from
/// the moment its bytes start landing until the commit — so only templates
/// carry a status worth reading off the record.
pub fn opening_status(record: &SnapshotRecord) -> &'static str {
    match &record.source {
        SnapshotSource::Template { build } => match build.status {
            // 🔴 Refused server-side rather than corrected: a row cannot be born
            // `ready` or `error`, so anything but the two opening states is a
            // caller believing something untrue about what it is creating. The
            // clamp is here so the refusal names a status this build chose.
            TemplateBuildStatus::Building => STATUS_BUILDING,
            _ => STATUS_WAITING,
        },
        SnapshotSource::Sandbox { .. } => STATUS_BUILDING,
    }
}

pub fn source_sandbox_id(record: &SnapshotRecord) -> String {
    match &record.source {
        SnapshotSource::Sandbox { source_sandbox_id } => source_sandbox_id.clone(),
        SnapshotSource::Template { .. } => String::new(),
    }
}

pub fn record_source_kind(record: &SnapshotRecord) -> &'static str {
    match &record.source {
        SnapshotSource::Sandbox { .. } => SOURCE_KIND_SANDBOX,
        SnapshotSource::Template { .. } => SOURCE_KIND_TEMPLATE,
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

/// Encodes a committed payload for the `bytea` column.
pub fn encode_committed(committed: &CommittedSnapshot) -> RepositoryResult<Vec<u8>> {
    serde_json::to_vec(committed).map_err(|error| RepositoryError::Backend {
        message: "serialize committed snapshot payload for the catalog".to_string(),
        source: Some(error.into()),
    })
}

/// Encodes a build failure for the `jsonb` column.
///
/// 🔴 Always an object. `TemplateBuildErrorReason` has a hand-written
/// `Deserialize` that also accepts a bare string, and a bare string is a legal
/// JSON document the column would take and the server's own guard would then
/// refuse — so the encoder is pinned to the struct form rather than left to
/// whatever the type happens to serialise as.
pub fn encode_build_error(reason: &TemplateBuildErrorReason) -> RepositoryResult<Vec<u8>> {
    serde_json::to_vec(reason).map_err(|error| RepositoryError::Backend {
        message: "serialize template build error for the catalog".to_string(),
        source: Some(error.into()),
    })
}

/// Turns one row on the wire into a record, refusing anything it cannot read.
///
/// 🔴 Never returns a plausible-looking record for a row it did not understand.
/// Downstream a `SnapshotRecord` is launched from, deleted on the strength of,
/// and counted in a page; a row decoded on a guess is worse than no row at all.
pub fn decode_row(
    row: pb::SnapshotRow,
    expected_cluster: Uuid,
) -> RepositoryResult<SnapshotRecord> {
    let id = SnapshotId::parse(&row.snapshot_id)
        .map_err(|_| malformed(&row.snapshot_id, "snapshot id is not a uuid"))?;

    // The scope every statement already carries. Checking it here as well is
    // what rules out a controller answering for a different cluster — the one
    // thing a request can merely *ask* for and not enforce.
    let cluster_id = Uuid::parse_str(&row.cluster_id)
        .map_err(|_| malformed(&row.snapshot_id, "cluster id is not a uuid"))?;
    if cluster_id != expected_cluster {
        return Err(malformed(
            &row.snapshot_id,
            format!(
                "row belongs to cluster '{cluster_id}' but this node is in cluster '{expected_cluster}'"
            ),
        ));
    }

    let alias = if row.alias.is_empty() {
        None
    } else {
        Some(
            SnapshotAlias::parse(&row.alias)
                .map_err(|error| malformed(&row.snapshot_id, error.to_string()))?,
        )
    };

    let status = decode_status(&row.snapshot_id, &row.status)?;

    let source = match row.source_kind.as_str() {
        SOURCE_KIND_SANDBOX => {
            if row.source_sandbox_id.is_empty() {
                return Err(malformed(
                    &row.snapshot_id,
                    "a sandbox row must name the sandbox it was captured from",
                ));
            }
            SnapshotSource::Sandbox {
                source_sandbox_id: row.source_sandbox_id,
            }
        }
        SOURCE_KIND_TEMPLATE => SnapshotSource::Template {
            build: TemplateBuildInfo {
                status,
                started_at_unix_ms: row.build_started_at_unix_ms,
                finished_at_unix_ms: row.build_finished_at_unix_ms,
                error_reason: decode_build_error(&row.snapshot_id, &row.build_error_json)?,
            },
        },
        other => {
            return Err(malformed(
                &row.snapshot_id,
                format!("unknown source kind '{other}'"),
            ))
        }
    };

    let committed = decode_committed(
        &row.snapshot_id,
        &row.committed_payload,
        row.committed_schema,
    )?;
    // The table states this as `status <> 'ready' OR committed_payload IS NOT
    // NULL`. Restated here because the record's own shape says it too — a
    // `Ready` template with no payload is a snapshot a resume would try to
    // launch from nothing.
    if status == TemplateBuildStatus::Ready && committed.is_none() {
        return Err(malformed(
            &row.snapshot_id,
            "a ready row carries no committed payload",
        ));
    }

    Ok(SnapshotRecord {
        id,
        alias,
        source,
        resources: SandboxResources {
            cpu_count: row.cpu_count,
            memory_mib: row.memory_mib,
            disk_size_mib: row.disk_size_mib,
        },
        created_at_unix_ms: row.created_at_unix_ms,
        updated_at_unix_ms: row.updated_at_unix_ms,
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
    raw: &[u8],
) -> RepositoryResult<Option<TemplateBuildErrorReason>> {
    if raw.is_empty() {
        return Ok(None);
    }
    serde_json::from_slice(raw)
        .map(Some)
        .map_err(|error| malformed(row_id, format!("build error is not readable: {error}")))
}

fn decode_committed(
    row_id: &str,
    payload: &[u8],
    schema: Option<u32>,
) -> RepositoryResult<Option<CommittedSnapshot>> {
    match (payload.is_empty(), schema) {
        (true, None) => Ok(None),
        // 🔴 Both halves or neither. The table pins the equivalence with a
        // CHECK; a row that reached here with one of them is a row written by
        // something that is not this contract, and decoding the payload without
        // knowing its version is exactly the guess this column exists to stop.
        (true, Some(version)) => Err(malformed(
            row_id,
            format!("payload schema {version} with no payload"),
        )),
        (false, None) => Err(malformed(row_id, "payload with no schema version")),
        (false, Some(version)) => {
            if version != COMMITTED_PAYLOAD_SCHEMA {
                return Err(malformed(
                    row_id,
                    format!(
                        "payload schema {version} is not one this build reads \
                         (it writes and reads {COMMITTED_PAYLOAD_SCHEMA})"
                    ),
                ));
            }
            serde_json::from_slice(payload)
                .map(Some)
                .map_err(|error| malformed(row_id, format!("payload is not readable: {error}")))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::types::{SnapshotSource, TemplateBuildInfo};

    fn cluster() -> Uuid {
        Uuid::parse_str("0198f0a1-0000-7000-8000-0000000c0ffe").expect("a fixed cluster id")
    }

    fn row() -> pb::SnapshotRow {
        pb::SnapshotRow {
            snapshot_id: SnapshotId::generate().to_string(),
            cluster_id: cluster().to_string(),
            source_kind: SOURCE_KIND_TEMPLATE.to_string(),
            source_sandbox_id: String::new(),
            cpu_count: 2,
            memory_mib: 512,
            disk_size_mib: 2048,
            status: STATUS_WAITING.to_string(),
            status_group: "pending".to_string(),
            alias: String::new(),
            created_at_unix_ms: 1_700_000_000_000,
            updated_at_unix_ms: 1_700_000_000_001,
            sandbox_started_at_unix_ms: None,
            committed_payload: Vec::new(),
            committed_schema: None,
            build_error_json: Vec::new(),
            build_started_at_unix_ms: None,
            build_finished_at_unix_ms: None,
            published: true,
            origin_node_id: String::new(),
        }
    }

    fn ready_row() -> pb::SnapshotRow {
        pb::SnapshotRow {
            status: STATUS_READY.to_string(),
            status_group: "ready".to_string(),
            committed_payload: encode_committed(&CommittedSnapshot::mock())
                .expect("a payload should encode"),
            committed_schema: Some(COMMITTED_PAYLOAD_SCHEMA),
            ..row()
        }
    }

    /// 🔴 `published` and `origin_node_id` are a projection, and this side does
    /// not read them.
    ///
    /// The two columns are designed to be droppable — one migration file and
    /// three deletions — and what buys that is their never reaching a `WHERE`
    /// and never being re-derived from something else. Decoding them into
    /// `SnapshotRecord` would put them into the node's domain model, where the
    /// next thing to want them is a filter. A behavioural test is the only kind
    /// that can say "this code does not depend on that": two rows differing in
    /// nothing else must decode to the same record.
    #[test]
    fn the_origin_columns_are_projected_and_this_side_does_not_read_them() {
        let published = ready_row();
        let pinned = pb::SnapshotRow {
            published: false,
            origin_node_id: "worker-01".to_string(),
            ..published.clone()
        };

        let from_published = decode_row(published, cluster()).expect("a row should decode");
        let from_pinned = decode_row(pinned, cluster()).expect("a row should decode");

        assert_eq!(from_published.id, from_pinned.id);
        assert_eq!(from_published.alias, from_pinned.alias);
        assert_eq!(from_published.resources, from_pinned.resources);
        assert_eq!(
            from_published.created_at_unix_ms,
            from_pinned.created_at_unix_ms
        );
        assert_eq!(
            serde_json::to_value(&from_published).expect("a record should serialize"),
            serde_json::to_value(&from_pinned).expect("a record should serialize"),
            "an unpublished row pinned to a node must decode to the same record as a published \
             one; anything else means this side has started depending on columns that are meant \
             to be droppable"
        );
    }

    #[test]
    fn a_ready_row_decodes_with_its_payload() {
        let decoded = decode_row(ready_row(), cluster()).expect("a ready row should decode");
        assert!(decoded.committed.is_some());
        assert!(matches!(
            decoded.source,
            SnapshotSource::Template {
                build: TemplateBuildInfo {
                    status: TemplateBuildStatus::Ready,
                    ..
                }
            }
        ));
    }

    /// 🔴 A value this build has not heard of must reach the caller as a
    /// refusal, not be flattened onto the nearest one it does know. The column's
    /// CHECK belongs to whoever migrates the database, so a status appearing
    /// here means the two sides have drifted — and guessing which of the four
    /// it resembles is how a `building` row gets launched.
    #[test]
    fn an_unknown_status_is_refused_rather_than_guessed() {
        let error = decode_row(
            pb::SnapshotRow {
                status: "snapshotting".to_string(),
                ..row()
            },
            cluster(),
        )
        .expect_err("an unknown status must not decode");
        assert!(format!("{error}").contains("unknown status 'snapshotting'"));
    }

    #[test]
    fn an_unknown_source_kind_is_refused_rather_than_guessed() {
        let error = decode_row(
            pb::SnapshotRow {
                source_kind: "workspace".to_string(),
                ..row()
            },
            cluster(),
        )
        .expect_err("an unknown source kind must not decode");
        assert!(format!("{error}").contains("unknown source kind 'workspace'"));
    }

    /// 🔴 The scope check. Every statement already carries the cluster, but a
    /// request can only *ask* for one — this is what rules out a controller
    /// answering for a different cluster.
    #[test]
    fn a_row_from_another_cluster_is_refused() {
        let error = decode_row(row(), Uuid::now_v7()).expect_err("a foreign row must not decode");
        assert!(format!("{error}").contains("but this node is in cluster"));
    }

    /// 🔴 The payload's version is checked, not assumed. serde would take a
    /// payload written by a build whose `CommittedSnapshot` has since changed
    /// and quietly drop the fields it did not recognise — handing back a
    /// snapshot missing its memory layers.
    #[test]
    fn a_payload_this_build_cannot_read_is_refused() {
        let error = decode_row(
            pb::SnapshotRow {
                committed_schema: Some(COMMITTED_PAYLOAD_SCHEMA + 1),
                ..ready_row()
            },
            cluster(),
        )
        .expect_err("an unknown payload version must not decode");
        assert!(format!("{error}").contains("is not one this build reads"));
    }

    #[test]
    fn a_payload_without_a_version_and_a_version_without_a_payload_are_both_refused() {
        decode_row(
            pb::SnapshotRow {
                committed_schema: None,
                ..ready_row()
            },
            cluster(),
        )
        .expect_err("a payload with no version must not decode");

        decode_row(
            pb::SnapshotRow {
                committed_payload: Vec::new(),
                committed_schema: Some(COMMITTED_PAYLOAD_SCHEMA),
                status: STATUS_BUILDING.to_string(),
                ..row()
            },
            cluster(),
        )
        .expect_err("a version with no payload must not decode");
    }

    /// The table says `status <> 'ready' OR committed_payload IS NOT NULL`.
    /// Said again here, because a `ready` row with nothing in it is a snapshot
    /// a resume would try to launch from nothing.
    #[test]
    fn a_ready_row_with_no_payload_is_refused() {
        let error = decode_row(
            pb::SnapshotRow {
                status: STATUS_READY.to_string(),
                ..row()
            },
            cluster(),
        )
        .expect_err("a ready row with no payload must not decode");
        assert!(format!("{error}").contains("carries no committed payload"));
    }

    #[test]
    fn a_sandbox_row_that_names_no_sandbox_is_refused() {
        decode_row(
            pb::SnapshotRow {
                source_kind: SOURCE_KIND_SANDBOX.to_string(),
                source_sandbox_id: String::new(),
                ..row()
            },
            cluster(),
        )
        .expect_err("a sandbox row must name its sandbox");
    }

    /// 🔴 A row opens at `waiting` or `building` and never at `ready` — the
    /// server refuses anything else, so a record whose build has already
    /// finished or failed must not ask for its own status back.
    #[test]
    fn a_row_only_ever_opens_at_waiting_or_building() {
        for status in [
            TemplateBuildStatus::Waiting,
            TemplateBuildStatus::Building,
            TemplateBuildStatus::Ready,
            TemplateBuildStatus::Error,
        ] {
            let record = SnapshotRecord {
                id: SnapshotId::generate(),
                alias: None,
                source: SnapshotSource::Template {
                    build: TemplateBuildInfo {
                        status,
                        started_at_unix_ms: None,
                        finished_at_unix_ms: None,
                        error_reason: None,
                    },
                },
                resources: SandboxResources::default(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
                committed: None,
            };
            let opening = opening_status(&record);
            assert!(
                opening == STATUS_WAITING || opening == STATUS_BUILDING,
                "{status:?} opened a row at '{opening}'"
            );
        }
    }

    /// A build failure crosses as an object, never as the bare string its
    /// deserialiser also accepts: the server refuses anything that is not an
    /// object, because a JSON scalar stores and then fails to decode.
    #[test]
    fn a_build_error_is_encoded_as_an_object() {
        let encoded = encode_build_error(&TemplateBuildErrorReason::with_step("boom", "RUN"))
            .expect("a reason should encode");
        let value: serde_json::Value = serde_json::from_slice(&encoded).expect("it should be JSON");
        assert!(value.is_object(), "got {value}");
        assert_eq!(value["message"], "boom");

        let decoded = decode_build_error("row", &encoded)
            .expect("it should decode")
            .expect("and be present");
        assert_eq!(decoded.message, "boom");
        assert_eq!(decoded.step.as_deref(), Some("RUN"));
    }
}
