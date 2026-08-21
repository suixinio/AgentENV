//! Two catalogs, one write.
//!
//! The catalog is moving from object storage into PostgreSQL, and this is the
//! phase where both hold it. What that buys is a second copy somebody can
//! *check* — and what makes the check worth anything is what happens when one
//! of them says no.
//!
//! ```text
//!   central (PostgreSQL, via the scheduler)   refusal fails the write
//!                                             unreachable is recorded, not returned
//!   object store                              refusal is the caller's only for an alias
//!                                             unreachable is recorded, not returned
//! ```
//!
//! 🔴 **Neither store being reachable may fail a user's operation.** It used to
//! be asymmetric — an object-store failure was recorded and a central failure
//! propagated — on the argument that the compensator only ran one way. What
//! that actually bought was this: a pause whose scheduler was unreachable
//! failed at `commit_staged`, which rolled the publish back, which asked
//! whether the artifacts were still owned, which was answered *from the object
//! store* that had never been written, which said no — and deleted the bytes of
//! the sandbox the user had just paused. A mirror being down is not a reason to
//! destroy a workspace.
//!
//! 🔴 The one-way rule was never about the queue. It was argued from "an
//! object-store row can be rebuilt from a catalog row and not the reverse",
//! which is a statement about *reconstructing a row by diffing state*. This
//! queue does not do that: it records the **operation** and replays it, and an
//! operation replays into either store. See [`backlog`].
//!
//! 🔴 **A refusal is still a refusal.** `RefusalPolicy::Fatal` is untouched:
//! when the central catalog says the name is taken, or answers something this
//! build cannot read, the write fails and the object store is not touched. This
//! is about transport, not about a catalog correctly saying no.
//!
//! 🔴 Reads still come from the object store in this phase. Pointing them at
//! PostgreSQL is the next batch's switch, and it is guarded: switching is only
//! lossless while the side being switched *to* is neither behind nor known to
//! disagree, which is what [`MirrorBacklog::lag_toward`] and
//! [`MirrorBacklog::diverged_toward`] answer and what
//! [`MirrorBacklog::guard_read_side`] refuses on.

mod backlog;
mod central;
mod compensator;
mod metrics;
#[cfg(test)]
pub(crate) mod test_doubles;

use std::sync::Arc;

use async_trait::async_trait;
use tracing::{error, warn};

use crate::snapshot::repository::backends::central::{
    commit_opening_record, opening_status, CatalogRefusal, CatalogWrite, STATUS_BUILDING,
};
use crate::snapshot::repository::interfaces::{
    SnapshotCatalog, SnapshotCommit, SnapshotListFilter,
};
use crate::snapshot::repository::{RepositoryError, RepositoryResult};
use crate::snapshot::types::{SnapshotId, SnapshotRecord, TemplateBuildErrorReason};

pub use backlog::{CatalogReadSide, MirrorBacklog, MirrorDirection, MirrorTargets, RepairPass};
pub use central::CentralCatalogWrites;
pub use compensator::{MirrorCompensator, DEFAULT_COMPENSATOR_INTERVAL};

use backlog::MirrorOp;
use metrics::{
    record_central_diverged, record_central_refused, record_central_request, record_mirror_failed,
    CentralOutcome,
};

/// How a central refusal is treated.
enum RefusalPolicy {
    /// The request is wrong. Fail it, and do not write the object store —
    /// otherwise the object store ends up holding a row the catalog refused.
    ///
    /// 🔴 Deliberately still fatal. What stopped being fatal is a catalog
    /// nobody can *reach*; a catalog that answered, and said no, has told the
    /// caller something the caller can act on.
    Fatal,
    /// The catalog's row is behind what the object store's already is, in a way
    /// only the batch that wires build admission can close. Recorded as a
    /// divergence, and the write goes on to the object store, which is the side
    /// that answers reads.
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

/// What a central write settled, from the caller's point of view.
enum CentralWrite<T> {
    /// The catalog answered. Nothing is owed.
    Settled(Option<T>),
    /// The catalog could not be reached. The write is owed to it.
    Owed,
}

impl<T> CentralWrite<T> {
    fn is_owed(&self) -> bool {
        matches!(self, Self::Owed)
    }
}

/// A [`SnapshotCatalog`] that writes both stores and reads one.
pub struct DualWriteCatalog {
    central: Arc<dyn CentralCatalogWrites>,
    object_store: Arc<dyn SnapshotCatalog>,
    backlog: Arc<MirrorBacklog>,
}

impl DualWriteCatalog {
    pub fn new(
        central: Arc<dyn CentralCatalogWrites>,
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

    /// Runs one central write and says what it settled.
    ///
    /// `Err` is a refusal the caller has to fail on. [`CentralWrite::Owed`] is
    /// the catalog being unreachable, which the caller records and carries on
    /// from.
    async fn central_write<T>(
        &self,
        op: &'static str,
        id: &SnapshotId,
        outcome: RepositoryResult<CatalogWrite<T>>,
    ) -> RepositoryResult<CentralWrite<T>> {
        match outcome {
            Ok(CatalogWrite::Applied(value)) => {
                record_central_request(op, CentralOutcome::Ok);
                Ok(CentralWrite::Settled(Some(value)))
            }
            Ok(CatalogWrite::Refused(refusal)) => {
                record_central_request(op, CentralOutcome::Refused);
                match policy_for(&refusal) {
                    RefusalPolicy::Satisfied => Ok(CentralWrite::Settled(None)),
                    RefusalPolicy::Diverged => {
                        record_central_diverged(op, refusal.as_metric_label());
                        warn!(
                            catalog_op = op,
                            snapshot_id = %id,
                            refusal = %refusal,
                            "the central snapshot catalog would not take a write the object \
                             store will; its row for this snapshot is now behind"
                        );
                        // 🔴 Written down, not just counted. A counter of
                        // events cannot answer "do the two catalogs agree
                        // *now*", and that is the question the switch to
                        // reading PostgreSQL turns on.
                        self.backlog
                            .note_divergence(MirrorDirection::Central, id, op, refusal.to_string())
                            .await;
                        Ok(CentralWrite::Settled(None))
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
                record_mirror_failed(MirrorDirection::Central, op);
                // 🔴 Not returned. The operation still succeeds and the write
                // is owed — see the module header for the workspace this used
                // to destroy.
                error!(
                    catalog_op = op,
                    snapshot_id = %id,
                    %error,
                    "the central snapshot catalog could not be reached; the write is recorded \
                     and will be replayed"
                );
                Ok(CentralWrite::Owed)
            }
        }
    }

    /// Writes the object store, and treats a failure as owed rather than fatal.
    ///
    /// 🔴 The operation still succeeds. Failing it here would make publishing a
    /// snapshot depend on both stores being up at once, which is strictly worse
    /// availability than either alone — and the write is recoverable, because
    /// the queue holds the operation and the compensator can replay it.
    async fn mirror_write(
        &self,
        op: &'static str,
        outcome: RepositoryResult<()>,
        owed: impl FnOnce() -> MirrorOp,
    ) {
        let Err(error) = outcome else {
            return;
        };
        record_mirror_failed(MirrorDirection::ObjectStore, op);
        error!(
            catalog_op = op,
            %error,
            "the object-store catalog mirror could not take a write; it is recorded and will be \
             replayed"
        );
        self.backlog
            .record(MirrorDirection::ObjectStore, owed())
            .await;
    }
}

#[async_trait]
impl SnapshotCatalog for DualWriteCatalog {
    async fn create(&self, record: SnapshotRecord) -> RepositoryResult<SnapshotRecord> {
        let central = self
            .central
            .begin(&record, opening_status(&record), true)
            .await;
        if self
            .central_write("create", &record.id, central)
            .await?
            .is_owed()
        {
            self.backlog
                .record(
                    MirrorDirection::Central,
                    MirrorOp::Create {
                        record: record.clone(),
                    },
                )
                .await;
        }

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
        let began = self.central.begin(&opening, STATUS_BUILDING, false).await;
        let mut central_owed = self
            .central_write("publish_commit.begin", &commit.id, began)
            .await?
            .is_owed();

        // 🔴 Only if the row was opened. The commit's fence requires a
        // `building` row, so running it against a catalog that never took the
        // opening statement would refuse for a reason that says nothing about
        // this snapshot — and would be counted as a divergence that is not one.
        if !central_owed {
            let committed = self.central.commit(&commit, true, now_unix_ms()).await;
            central_owed = self
                .central_write("publish_commit.commit", &commit.id, committed)
                .await?
                .is_owed();
        }
        if central_owed {
            // One entry for the pair. Replaying it re-runs both statements, and
            // the opening one answering `ALREADY_EXISTS` is the ordinary case
            // rather than a failure.
            self.backlog
                .record(
                    MirrorDirection::Central,
                    MirrorOp::PublishCommit {
                        commit: commit.clone(),
                    },
                )
                .await;
        }

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
                    // It is also a real disagreement, and the caller's error
                    // does not record one. The central catalog took this commit
                    // and bound this name; the object store did neither, and
                    // reads still come from the object store.
                    if !central_owed {
                        self.backlog
                            .note_divergence(
                                MirrorDirection::ObjectStore,
                                &commit.id,
                                "publish_commit",
                                error.to_string(),
                            )
                            .await;
                    }
                    return Err(error);
                }
                self.mirror_write("publish_commit", Err(error), || MirrorOp::PublishCommit {
                    commit: commit.clone(),
                })
                .await;

                if central_owed {
                    // Neither store took it and both owe it. The operation
                    // succeeded all the same — the queue is durable and holds
                    // the whole commit — so the row it describes is what the
                    // caller gets, rather than an error over a snapshot whose
                    // bytes are safely written.
                    return Ok(committed_record(&commit));
                }
                // Nothing local can answer with the committed row, so ask the
                // side that took the write.
                self.central
                    .get_resolvable(&commit.id.to_string())
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
            .delete(&record.id.to_string(), now_unix_ms())
            .await;
        match deleted {
            Ok(_) => record_central_request("delete_record", CentralOutcome::Ok),
            Err(error) => {
                record_central_request("delete_record", CentralOutcome::Error);
                record_mirror_failed(MirrorDirection::Central, "delete_record");
                error!(
                    catalog_op = "delete_record",
                    snapshot_id = %record.id,
                    %error,
                    "the central snapshot catalog could not be reached; the delete is recorded \
                     and will be replayed"
                );
                self.backlog
                    .record(
                        MirrorDirection::Central,
                        MirrorOp::DeleteRecord {
                            record: record.clone(),
                        },
                    )
                    .await;
            }
        }

        let mirrored = self.object_store.delete_record(record).await;
        self.mirror_write("delete_record", mirrored, || MirrorOp::DeleteRecord {
            record: record.clone(),
        })
        .await;

        // 🔴 A delete settles every disagreement about this snapshot: both
        // catalogs are losing the row, one now and the other now or from the
        // queue. Anything they used to disagree about is superseded, so the
        // recorded divergence goes with it rather than pinning the read side
        // over a snapshot that no longer exists.
        self.backlog.clear_divergences(&record.id).await;
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
    /// object store is still the side that answers reads, so it decides.
    ///
    /// 🔴 What that costs is written down rather than counted and forgotten.
    /// The central row stays `waiting` while this one moves to `building`, and
    /// no replay closes the gap — so it is recorded as a divergence, which is
    /// what stops the switch to reading PostgreSQL from being authorised on a
    /// cluster whose every template would vanish from the API the moment it
    /// took effect.
    async fn try_start_build(&self, id: &SnapshotId) -> RepositoryResult<SnapshotRecord> {
        let record = self.object_store.try_start_build(id).await?;
        record_central_diverged("try_start_build", "build_admission_not_wired");
        self.backlog
            .note_divergence(
                MirrorDirection::Central,
                id,
                "try_start_build",
                "build admission is not wired, so the central row stays waiting".to_string(),
            )
            .await;
        Ok(record)
    }

    async fn mark_build_error(
        &self,
        id: &SnapshotId,
        reason: TemplateBuildErrorReason,
    ) -> RepositoryResult<()> {
        let failed = self.central.fail(id, &reason, now_unix_ms()).await;
        if self
            .central_write("mark_build_error", id, failed)
            .await?
            .is_owed()
        {
            self.backlog
                .record(
                    MirrorDirection::Central,
                    MirrorOp::MarkBuildError {
                        id: id.clone(),
                        reason: reason.clone(),
                    },
                )
                .await;
        }

        let mirrored = self.object_store.mark_build_error(id, reason.clone()).await;
        self.mirror_write("mark_build_error", mirrored, || MirrorOp::MarkBuildError {
            id: id.clone(),
            reason,
        })
        .await;
        Ok(())
    }

    /// 🔴 Either catalog's yes is enough to keep the bytes.
    ///
    /// This is what a failed publish consults before deleting a snapshot's
    /// artifacts, and the only thing it can get catastrophically wrong is
    /// answering "no" when some committed row still points at them. Both
    /// catalogs are written, so both are asked, and the artifacts survive if
    /// either says they are owned. The case that makes it matter: the central
    /// catalog commits, the object store then refuses the alias, and the error
    /// travels back to a rollback that would otherwise delete the bytes of a
    /// snapshot PostgreSQL is holding a `ready` row for.
    async fn retains_artifacts_on_publish_failure(
        &self,
        id: &SnapshotId,
    ) -> RepositoryResult<bool> {
        if self
            .object_store
            .retains_artifacts_on_publish_failure(id)
            .await?
        {
            return Ok(true);
        }
        // A read failure here is treated as "nothing to protect" by the caller
        // either way; a central catalog that cannot be reached must not turn a
        // rollback into an error, so it only ever adds a reason to keep.
        Ok(matches!(
            self.central.get_any_status(&id.to_string()).await,
            Ok(Some(record)) if record.committed.is_some()
        ))
    }
}

/// The row a commit describes, for the one case where no catalog can be asked
/// for it.
fn committed_record(commit: &SnapshotCommit) -> SnapshotRecord {
    let mut record = commit_opening_record(commit);
    record.mark_committed(
        commit.alias.clone(),
        commit.resources,
        commit.committed.clone(),
        commit.source.clone(),
        now_unix_ms(),
    );
    record
}

fn now_unix_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// Arrangements this module's private queue makes available to tests outside
/// it.
///
/// [`MirrorOp`] stays private — what may enter the queue is this module's
/// business, and a test that could push anything into it would be testing a
/// contract nothing else has. What a caller's test legitimately needs is a
/// backlog with something owed in it, so that is what is offered.
#[cfg(test)]
pub(crate) mod test_support {
    use super::{MirrorBacklog, MirrorDirection, MirrorOp};
    use crate::snapshot::types::SnapshotRecord;

    pub(crate) async fn owe_a_create(
        backlog: &MirrorBacklog,
        direction: MirrorDirection,
        record: SnapshotRecord,
    ) {
        backlog.record(direction, MirrorOp::Create { record }).await;
    }
}

#[cfg(test)]
mod tests;
