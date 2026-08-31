//! Shared vocabulary for a snapshot catalog write against PostgreSQL: what
//! came back, or the specific reason the write did not happen.

use anyhow::anyhow;

use crate::snapshot::repository::interfaces::SnapshotCommit;
use crate::snapshot::repository::RepositoryError;
use crate::snapshot::types::{SnapshotAlias, SnapshotId, SnapshotRecord, SnapshotSource};

/// A refusal the caller has to act on, as opposed to a failure it can only
/// report.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CatalogRefusal {
    NotFound,
    /// The fencing predicate did not match; the row is no longer in the status
    /// this call acts from.
    StatusMismatch {
        observed: String,
    },
    AliasTaken {
        holder: String,
    },
    GenerationMismatch {
        observed: Option<i64>,
    },
    /// Terminal: a superseded incarnation tried to write.
    ExecutionSuperseded,
    BuildInProgress {
        active_build_id: String,
    },
    BuildQueueFull,
    AlreadyExists,
    /// A reason this build has not heard of. Reported rather than treated as
    /// any of the above.
    Unknown(i32),
}

impl std::fmt::Display for CatalogRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound => write!(f, "no such snapshot"),
            Self::StatusMismatch { observed } => {
                write!(
                    f,
                    "the row is '{observed}', not the status this write acts from"
                )
            }
            Self::AliasTaken { holder } => write!(f, "the alias is held by '{holder}'"),
            Self::GenerationMismatch { observed } => match observed {
                Some(generation) => write!(f, "the registry row is at generation {generation}"),
                None => write!(f, "the registry row moved"),
            },
            Self::ExecutionSuperseded => write!(f, "sandbox_execution_superseded"),
            Self::BuildInProgress { active_build_id } => {
                write!(f, "build '{active_build_id}' already holds this template")
            }
            Self::BuildQueueFull => write!(f, "the cluster is at its concurrent-build ceiling"),
            Self::AlreadyExists => write!(f, "a row with this id already exists"),
            Self::Unknown(reason) => {
                write!(f, "refusal code {reason}, which this build cannot read")
            }
        }
    }
}

/// One write's outcome: the row, or the reason the catalog would not write it.
pub enum CatalogWrite<T> {
    Applied(T),
    Refused(CatalogRefusal),
}

fn now_unix_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// Derives the row opened by `publish_commit`.
///
/// The commit retains the alias so binding happens atomically with publication.
pub fn commit_opening_record(commit: &SnapshotCommit) -> SnapshotRecord {
    let source = match &commit.source {
        crate::snapshot::types::SnapshotPublishSource::Sandbox { source_sandbox_id } => {
            SnapshotSource::Sandbox {
                source_sandbox_id: source_sandbox_id.clone(),
            }
        }
        crate::snapshot::types::SnapshotPublishSource::Template => SnapshotSource::Template {
            build: crate::snapshot::types::TemplateBuildInfo::waiting(),
        },
    };

    // Preserve the commit's creation time across replay; only `updated_at` uses this clock.
    let now = now_unix_ms();
    SnapshotRecord {
        id: commit.id.clone(),
        alias: None,
        source,
        resources: commit.resources,
        created_at_unix_ms: commit.created_at_unix_ms.unwrap_or(now),
        updated_at_unix_ms: now,
        committed: None,
    }
}

/// Builds an alias conflict from the refused write's alias, not the derived row.
pub fn alias_conflict(
    alias: Option<&SnapshotAlias>,
    id: &SnapshotId,
    holder: String,
) -> RepositoryError {
    match SnapshotId::parse(&holder) {
        Ok(existing) => RepositoryError::AliasConflict {
            alias: alias.map(ToString::to_string).unwrap_or_default(),
            existing,
            new_id: id.clone(),
        },
        // An unreadable holder still means the alias is taken.
        Err(_) => RepositoryError::backend(
            "snapshot catalog refused an alias binding",
            anyhow!("the alias is held by '{holder}'"),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_opening_row_leaves_the_alias_to_the_commit() {
        let commit = SnapshotCommit {
            id: SnapshotId::generate(),
            alias: Some(
                crate::snapshot::types::SnapshotAlias::parse("named").expect("alias parses"),
            ),
            source: crate::snapshot::types::SnapshotPublishSource::Sandbox {
                source_sandbox_id: "sbx".to_string(),
            },
            resources: crate::types::SandboxResources::default(),
            created_at_unix_ms: None,
            committed: crate::snapshot::types::CommittedSnapshot::mock(),
        };

        let opening = commit_opening_record(&commit);
        assert!(opening.alias.is_none());
        assert_eq!(opening.id, commit.id);
        assert!(matches!(
            opening.source,
            SnapshotSource::Sandbox { ref source_sandbox_id } if source_sandbox_id == "sbx"
        ));
    }

    #[test]
    fn a_refused_alias_is_reported_by_the_name_the_caller_asked_for() {
        let holder = SnapshotId::generate();
        let mine = SnapshotId::generate();
        let alias = crate::snapshot::types::SnapshotAlias::parse("contested").expect("parses");

        match alias_conflict(Some(&alias), &mine, holder.to_string()) {
            RepositoryError::AliasConflict {
                alias: reported,
                existing,
                new_id,
            } => {
                assert_eq!(reported, "contested");
                assert_eq!(existing, holder);
                assert_eq!(new_id, mine);
            }
            other => panic!("expected an alias conflict, got {other:?}"),
        }
    }

    #[test]
    fn an_unreadable_holder_still_reports_the_name_as_taken() {
        let error = alias_conflict(None, &SnapshotId::generate(), "not-a-uuid".to_string());
        assert!(format!("{error}").contains("refused an alias binding"));
    }
}
