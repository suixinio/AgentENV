//! Two catalogs, one write.
//!
//! The catalog is moving from object storage into PostgreSQL, and this is the
//! phase where both hold it. What that buys is a second copy somebody can
//! *check* — and what makes the check worth anything is the order the two are
//! written in and what happens when one of them says no.
//!
//! ```text
//!   central (PostgreSQL, via the scheduler)   written first   its failure fails the write
//!   object store                              written second  its failure is recorded, not returned
//! ```
//!
//! 🔴 That order is not arbitrary and it may not be reversed. The compensator
//! only runs one way — central to object store — because an object-store row
//! can be rebuilt from a catalog row and not the other way round: the catalog
//! carries `status_group`, `published` and a build's heartbeat, and none of
//! those exist in the object store. A mirror that could drift in both
//! directions would have one direction nothing could repair.
//!
//! 🔴 Reads still come from the object store in this phase. Pointing them at
//! PostgreSQL is the next batch's switch, and it is guarded: switching back
//! is only lossless while nothing is outstanding, which is what
//! [`MirrorBacklog::lag`] answers and what
//! [`DualWriteCatalog::preflight_read_from_object_store`] refuses on.

mod backlog;
mod compensator;
mod metrics;

use std::sync::Arc;

use async_trait::async_trait;
use tracing::{error, warn};

use crate::snapshot::repository::backends::central::{
    commit_opening_record, opening_status, CatalogReadScope, CatalogRefusal, CatalogWrite,
    CentralSnapshotCatalog, STATUS_BUILDING,
};
use crate::snapshot::repository::interfaces::{
    SnapshotCatalog, SnapshotCommit, SnapshotListFilter,
};
use crate::snapshot::repository::{RepositoryError, RepositoryResult};
use crate::snapshot::types::{SnapshotId, SnapshotRecord, TemplateBuildErrorReason};

pub use backlog::{CatalogReadSide, MirrorBacklog, RepairPass};
pub use compensator::{MirrorCompensator, DEFAULT_COMPENSATOR_INTERVAL};

use backlog::MirrorOp;
use metrics::{
    record_central_diverged, record_central_refused, record_central_request, record_mirror_failed,
    CentralOutcome,
};

/// How a central refusal is treated.
enum RefusalPolicy {
    /// The request is wrong. Fail it, and do not write the object store —
    /// otherwise the object store ends up holding a row the catalog refused,
    /// which is the one direction the compensator cannot walk back.
    Fatal,
    /// The catalog's row is behind what the object store's already is, in a way
    /// only the batch that wires build admission can close. Counted, and the
    /// write goes on to the object store, which is the side that answers reads.
    Diverged,
    /// The catalog is already in the state this write wanted.
    Satisfied,
}

fn policy_for(refusal: &CatalogRefusal) -> RefusalPolicy {
    match refusal {
        // Somebody else holds the name. Both stores would refuse it and the
        // caller has something to do about it.
        CatalogRefusal::AliasTaken { .. } => RefusalPolicy::Fatal,
        // The row is already open. `publish_commit` opens one before flipping
        // it, and on the template path the row was created when the template
        // was — this is the ordinary answer, not a problem.
        CatalogRefusal::AlreadyExists => RefusalPolicy::Satisfied,
        // 🔴 The known gap. The catalog's `waiting -> building` transition is
        // build admission, which this batch does not wire, so a template row
        // here is still `waiting` when the commit arrives and the commit's
        // fence refuses it. See `CentralSnapshotCatalog::try_start_build`.
        CatalogRefusal::StatusMismatch { .. }
        | CatalogRefusal::NotFound
        | CatalogRefusal::BuildInProgress { .. }
        | CatalogRefusal::BuildQueueFull => RefusalPolicy::Diverged,
        // Neither can reach this client yet — it sends no paused transition and
        // no execution id — so reaching one means the catalog is answering
        // something this build did not ask. Refuse rather than guess.
        CatalogRefusal::GenerationMismatch { .. }
        | CatalogRefusal::ExecutionSuperseded
        | CatalogRefusal::Unknown(_) => RefusalPolicy::Fatal,
    }
}

/// A [`SnapshotCatalog`] that writes both stores and reads one.
pub struct DualWriteCatalog {
    central: Arc<CentralSnapshotCatalog>,
    object_store: Arc<dyn SnapshotCatalog>,
    backlog: Arc<MirrorBacklog>,
}

impl DualWriteCatalog {
    pub fn new(
        central: Arc<CentralSnapshotCatalog>,
        object_store: Arc<dyn SnapshotCatalog>,
        backlog: Arc<MirrorBacklog>,
    ) -> Self {
        Self {
            central,
            object_store,
            backlog,
        }
    }

    pub fn backlog(&self) -> Arc<MirrorBacklog> {
        Arc::clone(&self.backlog)
    }

    /// Runs one central write, and says what the caller should do next.
    ///
    /// `Ok(true)` means go on and write the object store; `Ok(false)` never
    /// happens today and exists so the branch is written rather than implied.
    async fn central_write<T>(
        &self,
        op: &'static str,
        outcome: RepositoryResult<CatalogWrite<T>>,
    ) -> RepositoryResult<Option<T>> {
        match outcome {
            Ok(CatalogWrite::Applied(value)) => {
                record_central_request(op, CentralOutcome::Ok);
                Ok(Some(value))
            }
            Ok(CatalogWrite::Refused(refusal)) => {
                record_central_request(op, CentralOutcome::Refused);
                match policy_for(&refusal) {
                    RefusalPolicy::Satisfied => Ok(None),
                    RefusalPolicy::Diverged => {
                        record_central_diverged(op, refusal.as_metric_label());
                        warn!(
                            catalog_op = op,
                            refusal = %refusal,
                            "the central snapshot catalog would not take a write the object \
                             store will; its row for this snapshot is now behind"
                        );
                        Ok(None)
                    }
                    RefusalPolicy::Fatal => {
                        record_central_refused(op, refusal.as_metric_label());
                        Err(RepositoryError::backend(
                            format!("central snapshot catalog refused '{op}'"),
                            anyhow::anyhow!("{refusal}"),
                        ))
                    }
                }
            }
            Err(error) => {
                record_central_request(op, CentralOutcome::Error);
                // 🔴 The whole write fails and the object store is not touched.
                // Writing it anyway would leave a row the central catalog does
                // not have, and the compensator only walks the other way.
                Err(error)
            }
        }
    }

    /// Writes the object store, and treats a failure as owed rather than fatal.
    ///
    /// 🔴 The operation still succeeds. Failing it here would make publishing a
    /// snapshot depend on both stores being up at once, which is strictly worse
    /// availability than either alone — and the write is recoverable, because
    /// the central catalog already has it and the compensator can replay it.
    async fn mirror_write(
        &self,
        op: &'static str,
        outcome: RepositoryResult<()>,
        owed: impl FnOnce() -> MirrorOp,
    ) {
        let Err(error) = outcome else {
            return;
        };
        record_mirror_failed(op);
        error!(
            catalog_op = op,
            %error,
            "the object-store catalog mirror refused a write the central catalog took; \
             the write is recorded and will be replayed"
        );
        self.backlog.record(owed()).await;
    }
}

#[async_trait]
impl SnapshotCatalog for DualWriteCatalog {
    async fn create(&self, record: SnapshotRecord) -> RepositoryResult<SnapshotRecord> {
        let central = self
            .central
            .begin_snapshot(&record, opening_status(&record), true)
            .await;
        self.central_write("create", central).await?;

        let mirrored = self.object_store.create(record.clone()).await;
        match mirrored {
            Ok(stored) => Ok(stored),
            Err(error) => {
                self.mirror_write("create", Err(error), || MirrorOp::Create {
                    record: record.clone(),
                })
                .await;
                Ok(record)
            }
        }
    }

    async fn publish_commit(&self, commit: SnapshotCommit) -> RepositoryResult<SnapshotRecord> {
        let opening = commit_opening_record(&commit);
        let began = self
            .central
            .begin_snapshot(&opening, STATUS_BUILDING, false)
            .await;
        self.central_write("publish_commit.begin", began).await?;

        let committed = self
            .central
            .commit_snapshot(&commit, true, now_unix_ms())
            .await;
        self.central_write("publish_commit.commit", committed)
            .await?;

        let mirrored = self.object_store.publish_commit(commit.clone()).await;
        match mirrored {
            Ok(record) => Ok(record),
            Err(error) => {
                // 🔴 An alias the object store will not bind is the caller's
                // problem and not the mirror's: reporting success would hand
                // back a snapshot the user cannot reach by the name they asked
                // for, which is exactly the defect the central catalog's unique
                // index exists to remove.
                if matches!(error, RepositoryError::AliasConflict { .. }) {
                    return Err(error);
                }
                self.mirror_write("publish_commit", Err(error), || MirrorOp::PublishCommit {
                    commit: commit.clone(),
                })
                .await;
                // Nothing local can answer with the committed row, so ask the
                // side that took the write.
                self.central
                    .get_scoped(&commit.id.to_string(), CatalogReadScope::Resolvable)
                    .await?
                    .ok_or_else(|| RepositoryError::SnapshotNotFound {
                        lookup: commit.id.to_string(),
                    })
            }
        }
    }

    async fn get(&self, id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>> {
        self.object_store.get(id_or_alias).await
    }

    async fn list(&self, filter: SnapshotListFilter) -> RepositoryResult<Vec<SnapshotRecord>> {
        self.object_store.list(filter).await
    }

    async fn delete_record(&self, record: &SnapshotRecord) -> RepositoryResult<()> {
        let deleted = self
            .central
            .delete_snapshot(&record.id.to_string(), now_unix_ms())
            .await;
        match deleted {
            Ok(_) => record_central_request("delete_record", CentralOutcome::Ok),
            Err(error) => {
                record_central_request("delete_record", CentralOutcome::Error);
                return Err(error);
            }
        }

        let mirrored = self.object_store.delete_record(record).await;
        self.mirror_write("delete_record", mirrored, || MirrorOp::DeleteRecord {
            record: record.clone(),
        })
        .await;
        Ok(())
    }

    async fn resolve_alias(&self, alias: &str) -> RepositoryResult<Option<SnapshotId>> {
        self.object_store.resolve_alias(alias).await
    }

    /// 🔴 Object store only, on purpose, and the one write that is not doubled.
    ///
    /// The central catalog's `waiting -> building` transition is build
    /// admission — a cluster-wide ceiling, a per-template unique index, and a
    /// build row only a heartbeat and a reaper release. Neither is wired, so
    /// admitting a build here would hold the first crashed builds' templates
    /// shut permanently and the twentieth would hold the cluster shut. The
    /// object store is still the side that answers reads, so it decides; the
    /// catalog's row stays behind, and the commit that follows counts the
    /// divergence rather than discovering it.
    async fn try_start_build(&self, id: &SnapshotId) -> RepositoryResult<SnapshotRecord> {
        record_central_diverged("try_start_build", "build_admission_not_wired");
        self.object_store.try_start_build(id).await
    }

    async fn mark_build_error(
        &self,
        id: &SnapshotId,
        reason: TemplateBuildErrorReason,
    ) -> RepositoryResult<()> {
        let failed = self.central.fail_snapshot(id, &reason, now_unix_ms()).await;
        self.central_write("mark_build_error", failed).await?;

        let mirrored = self.object_store.mark_build_error(id, reason.clone()).await;
        self.mirror_write("mark_build_error", mirrored, || MirrorOp::MarkBuildError {
            id: id.clone(),
            reason,
        })
        .await;
        Ok(())
    }

    async fn retains_artifacts_on_publish_failure(
        &self,
        id: &SnapshotId,
    ) -> RepositoryResult<bool> {
        // 🔴 Asked of the object store, which is the side that owns the bytes
        // and the side whose answer decides whether they are deleted. The
        // central catalog knows whether a *row* was committed; it does not know
        // whether this backend's artifacts belong to it.
        self.object_store
            .retains_artifacts_on_publish_failure(id)
            .await
    }
}

fn now_unix_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 🔴 The one refusal that must fail the write. Somebody else holds the
    /// name; letting it through would commit a snapshot the user cannot reach
    /// by the name they asked for, which is the defect the central catalog's
    /// unique index exists to remove.
    #[test]
    fn a_taken_alias_fails_the_write() {
        assert!(matches!(
            policy_for(&CatalogRefusal::AliasTaken {
                holder: "somebody".to_string()
            }),
            RefusalPolicy::Fatal
        ));
    }

    /// 🔴 A refusal this build cannot read must not be assumed harmless. It
    /// means the catalog is answering something this client did not ask, and
    /// treating that as "the mirror is a little behind" would let an unknown
    /// class of failure through as a success.
    #[test]
    fn a_refusal_this_build_cannot_read_fails_the_write() {
        for refusal in [
            CatalogRefusal::Unknown(4242),
            CatalogRefusal::ExecutionSuperseded,
            CatalogRefusal::GenerationMismatch { observed: Some(7) },
        ] {
            assert!(
                matches!(policy_for(&refusal), RefusalPolicy::Fatal),
                "{refusal} must not be waved through"
            );
        }
    }

    /// `publish_commit` opens a row before flipping it, and on the template
    /// path the row already exists. That is the ordinary answer.
    #[test]
    fn an_existing_row_is_what_opening_one_was_for() {
        assert!(matches!(
            policy_for(&CatalogRefusal::AlreadyExists),
            RefusalPolicy::Satisfied
        ));
    }

    /// 🔴 The batch's known gap. Build admission is not wired, so the central
    /// row for a template is still `waiting` when the commit arrives and the
    /// commit's fence refuses it. Counted and carried on with — object storage
    /// is still the side that answers reads, so failing here would fail a
    /// publish nothing is actually wrong with.
    #[test]
    fn a_row_the_catalog_could_not_advance_is_a_divergence_and_not_a_failure() {
        for refusal in [
            CatalogRefusal::StatusMismatch {
                observed: "waiting".to_string(),
            },
            CatalogRefusal::NotFound,
            CatalogRefusal::BuildInProgress {
                active_build_id: "b".to_string(),
            },
            CatalogRefusal::BuildQueueFull,
        ] {
            assert!(
                matches!(policy_for(&refusal), RefusalPolicy::Diverged),
                "{refusal} should be counted, not fatal"
            );
        }
    }

    /// Every refusal has a metric label, and no two share one — a reason that
    /// silently merged into another would make the divergence counter unable
    /// to say what happened.
    #[test]
    fn every_refusal_reason_has_its_own_label() {
        let labels = [
            CatalogRefusal::NotFound,
            CatalogRefusal::StatusMismatch {
                observed: String::new(),
            },
            CatalogRefusal::AliasTaken {
                holder: String::new(),
            },
            CatalogRefusal::GenerationMismatch { observed: None },
            CatalogRefusal::ExecutionSuperseded,
            CatalogRefusal::BuildInProgress {
                active_build_id: String::new(),
            },
            CatalogRefusal::BuildQueueFull,
            CatalogRefusal::AlreadyExists,
            CatalogRefusal::Unknown(0),
        ]
        .iter()
        .map(CatalogRefusal::as_metric_label)
        .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(labels.len(), 9);
    }
}
