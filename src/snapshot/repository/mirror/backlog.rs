//! The writes the object store still owes, and the pass that pays them.
//!
//! 🔴 Durable, and that is the whole reason it is here rather than a `Vec` in a
//! field. The number this holds is what decides whether the read side may be
//! pointed back at the object store — "no data is lost by rolling back" is only
//! true while nothing is outstanding — and a backlog that emptied itself on
//! restart would make that check pass by forgetting rather than by being right.
//!
//! 🔴 One direction only: central catalog to object store. The reverse is not
//! reconstructible — a catalog row carries `status_group`, `published` and the
//! build's heartbeat, none of which the object store has — so a mirror that
//! could drift both ways would have one direction it could never repair.

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::Context;
use serde::{Deserialize, Serialize};
use tracing::{error, warn};

use crate::local_store::{LocalKvStore, LocalStoreDurability};
use crate::snapshot::repository::interfaces::{SnapshotCatalog, SnapshotCommit};
use crate::snapshot::repository::{RepositoryError, RepositoryResult};
use crate::snapshot::types::{SnapshotId, SnapshotRecord, TemplateBuildErrorReason};

use super::metrics::{record_mirror_repair_failed, record_mirror_repaired, set_mirror_lag};

/// One catalog write the object store has not taken yet.
///
/// The *operation*, not the row it would produce. Recording the operation is
/// what lets a replay be judged against the same contract the original call
/// was: a `delete_record` whose target is already gone succeeded, and a row
/// diff could not tell that from a delete that never ran.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) enum MirrorOp {
    Create {
        record: SnapshotRecord,
    },
    PublishCommit {
        commit: SnapshotCommit,
    },
    TryStartBuild {
        id: SnapshotId,
    },
    MarkBuildError {
        id: SnapshotId,
        reason: TemplateBuildErrorReason,
    },
    DeleteRecord {
        record: SnapshotRecord,
    },
}

impl MirrorOp {
    pub(super) fn name(&self) -> &'static str {
        match self {
            Self::Create { .. } => "create",
            Self::PublishCommit { .. } => "publish_commit",
            Self::TryStartBuild { .. } => "try_start_build",
            Self::MarkBuildError { .. } => "mark_build_error",
            Self::DeleteRecord { .. } => "delete_record",
        }
    }

    /// Which snapshot this write is about.
    ///
    /// The replay's ordering key: two writes to one snapshot must be replayed
    /// in the order they were made, and writes to different snapshots are
    /// independent of each other.
    pub(super) fn snapshot_id(&self) -> &SnapshotId {
        match self {
            Self::Create { record } | Self::DeleteRecord { record } => &record.id,
            Self::PublishCommit { commit } => &commit.id,
            Self::TryStartBuild { id } | Self::MarkBuildError { id, .. } => id,
        }
    }

    async fn replay(&self, target: &dyn SnapshotCatalog) -> RepositoryResult<()> {
        match self {
            Self::Create { record } => target.create(record.clone()).await.map(|_| ()),
            Self::PublishCommit { commit } => {
                target.publish_commit(commit.clone()).await.map(|_| ())
            }
            Self::TryStartBuild { id } => target.try_start_build(id).await.map(|_| ()),
            Self::MarkBuildError { id, reason } => {
                target.mark_build_error(id, reason.clone()).await
            }
            Self::DeleteRecord { record } => target.delete_record(record).await,
        }
    }

    /// Whether the target already reflects this write.
    ///
    /// 🔴 A read, not an error-message match. A replay can fail because the
    /// first attempt actually landed and only its acknowledgement was lost, and
    /// the two are indistinguishable from the error alone — every backend
    /// spells "already exists" differently and one of them spells it
    /// `InvalidRequest`. Asking the target what it has settles it.
    async fn already_applied(&self, target: &dyn SnapshotCatalog) -> RepositoryResult<bool> {
        let existing = target.get(&self.snapshot_id().to_string()).await?;
        Ok(match self {
            Self::Create { .. } => existing.is_some(),
            Self::PublishCommit { .. } => existing
                .map(|record| record.committed.is_some())
                .unwrap_or(false),
            Self::DeleteRecord { .. } => existing.is_none(),
            Self::TryStartBuild { .. } => existing
                .map(|record| build_status_at_least_started(&record))
                .unwrap_or(false),
            Self::MarkBuildError { .. } => existing
                .map(|record| {
                    matches!(
                        build_status(&record),
                        Some(crate::snapshot::types::TemplateBuildStatus::Error)
                    )
                })
                .unwrap_or(false),
        })
    }
}

fn build_status(record: &SnapshotRecord) -> Option<crate::snapshot::types::TemplateBuildStatus> {
    match &record.source {
        crate::snapshot::types::SnapshotSource::Template { build } => Some(build.status),
        crate::snapshot::types::SnapshotSource::Sandbox { .. } => None,
    }
}

/// True once a template has left `waiting`, in either direction.
///
/// A `try_start_build` whose replay finds the row already `ready` or `error`
/// has been overtaken, not lost: something later in this snapshot's own queue
/// already moved it, and re-running the transition would be refused for the
/// right reason.
fn build_status_at_least_started(record: &SnapshotRecord) -> bool {
    !matches!(
        build_status(record),
        Some(crate::snapshot::types::TemplateBuildStatus::Waiting) | None
    )
}

/// What one replay attempt settled.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RepairVerdict {
    /// The object store now reflects the write. Drop the entry.
    Repaired,
    /// Nothing was learned; the same attempt may work later. Keep the entry and
    /// stop touching this snapshot for the rest of the pass.
    Retry,
    /// The object store refused in a way a retry cannot change.
    ///
    /// 🔴 The entry stays. It is a real outstanding write — the two stores
    /// disagree about this snapshot and nothing here can settle it — so the lag
    /// must keep counting it, which is what keeps the read side from being
    /// switched back over the top of a divergence.
    Diverged,
}

impl RepairVerdict {
    fn as_str(self) -> &'static str {
        match self {
            Self::Repaired => "repaired",
            Self::Retry => "retry",
            Self::Diverged => "diverged",
        }
    }
}

/// Decides what a replay attempt means.
///
/// Split out from the pass so it can be exercised against every outcome
/// without a store on either side.
pub(super) fn verdict_for(
    replay: &RepositoryResult<()>,
    already_applied: Option<bool>,
) -> RepairVerdict {
    if replay.is_ok() {
        return RepairVerdict::Repaired;
    }
    match already_applied {
        // The first attempt landed after all, or a later write already carried
        // this snapshot past the state this one wanted.
        Some(true) => RepairVerdict::Repaired,
        // The probe itself failed, so the store is not answering and the
        // replay's own failure says nothing either way.
        None => RepairVerdict::Retry,
        Some(false) => match replay {
            // 🔴 Only `Backend` is retried. Everything else is the store
            // stating a rule — a name already taken, a request it will not
            // accept, an operation it does not have — and asking again in
            // thirty seconds gets the same answer while the queue behind it
            // stops moving.
            Err(RepositoryError::Backend { .. }) => RepairVerdict::Retry,
            _ => RepairVerdict::Diverged,
        },
    }
}

/// What one pass over the backlog did.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RepairPass {
    pub repaired: usize,
    pub retry: usize,
    pub diverged: usize,
    /// Entries the pass did not attempt because an earlier entry for the same
    /// snapshot had not cleared.
    pub skipped: usize,
    pub remaining: u64,
}

/// Which store answers catalog reads.
///
/// Recorded across restarts so the guard below can tell an ordinary restart
/// from somebody moving the read side back.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CatalogReadSide {
    ObjectStore,
    Postgres,
}

impl CatalogReadSide {
    fn as_bytes(self) -> &'static [u8] {
        match self {
            Self::ObjectStore => b"object_store",
            Self::Postgres => b"postgres",
        }
    }

    fn parse(raw: &[u8]) -> Option<Self> {
        match raw {
            b"object_store" => Some(Self::ObjectStore),
            b"postgres" => Some(Self::Postgres),
            _ => None,
        }
    }
}

/// Key holding the read side the last process ran with.
const READ_SIDE_KEY: &[u8] = b"meta:read_side";

/// First byte of every queued entry's key.
const OWED_PREFIX: u8 = b'o';

/// The durable queue of owed object-store writes.
pub struct MirrorBacklog {
    store: LocalKvStore,
    next_seq: AtomicU64,
    lag: AtomicU64,
}

impl MirrorBacklog {
    /// Opens the backlog at `path`, recovering whatever the last process left.
    ///
    /// 🔴 `Wal`, not `Memory`. What is in here is the difference between "the
    /// two stores agree" and "they do not", and a process that crashed holding
    /// the answer must not come back believing they agreed.
    pub async fn open(path: impl Into<std::path::PathBuf>) -> anyhow::Result<Arc<Self>> {
        let store = LocalKvStore::open(path, LocalStoreDurability::Wal)
            .await
            .context("open the snapshot catalog mirror backlog")?;
        let entries = store
            .entries()
            .await
            .context("read the snapshot catalog mirror backlog")?;

        let mut highest = 0u64;
        let mut lag = 0u64;
        for (key, _) in &entries {
            if let Some(seq) = decode_seq(key) {
                highest = highest.max(seq);
                lag += 1;
            }
        }
        set_mirror_lag(lag);
        if lag > 0 {
            warn!(
                mirror_lag = lag,
                "the snapshot catalog mirror starts with writes the object store still owes"
            );
        }

        Ok(Arc::new(Self {
            store,
            next_seq: AtomicU64::new(highest.saturating_add(1)),
            lag: AtomicU64::new(lag),
        }))
    }

    /// How many writes the object store owes right now.
    pub fn lag(&self) -> u64 {
        self.lag.load(Ordering::Acquire)
    }

    /// Refuses to point reads back at the object store while it is behind.
    ///
    /// 🔴 This is what makes "rolling the read side back loses nothing" a fact
    /// rather than a hope. Every outstanding entry is a snapshot the central
    /// catalog knows about and the object store does not, so a node serving
    /// reads from object storage would answer "no such snapshot" for each of
    /// them — and callers delete artifacts and refuse resumes on that answer.
    ///
    /// 🔴 It fires on the *switch*, not on every start. A node that crashed
    /// holding a backlog while already reading the object store has to come
    /// back — refusing there would turn a mirror that is behind into a node
    /// that is down, which is exactly the trade the double write exists to
    /// avoid. So the side the last process ran with is recorded, and only
    /// moving from PostgreSQL back to object storage is refused.
    pub async fn guard_read_side(&self, configured: CatalogReadSide) -> anyhow::Result<()> {
        let previous = self
            .store
            .get(READ_SIDE_KEY.to_vec())
            .await
            .context("read the catalog mirror's recorded read side")?
            .and_then(|raw| CatalogReadSide::parse(&raw));

        let lag = self.lag();
        if previous == Some(CatalogReadSide::Postgres)
            && configured == CatalogReadSide::ObjectStore
            && lag > 0
        {
            anyhow::bail!(
                "snapshot.catalog.read is being moved from \"postgres\" back to \"object_store\" \
                 while the object-store catalog is behind by {lag} write(s). Those snapshots exist \
                 in the central catalog and not in object storage, so object storage would report \
                 them as absent and callers would treat them as deleted. Leave read = \"postgres\" \
                 until agentenv_snapshot_catalog_mirror_lag reaches 0, then switch."
            );
        }

        self.store
            .put(READ_SIDE_KEY.to_vec(), configured.as_bytes().to_vec())
            .await
            .context("record the catalog mirror's read side")
    }

    /// Records one owed write.
    ///
    /// 🔴 A failure here is loud but not fatal to the caller: the central write
    /// already succeeded and the operation already did what it was asked. What
    /// is lost is the record that the object store is behind, so it is logged
    /// at `error` — this is the one place the lag can be understated.
    pub(super) async fn record(&self, op: MirrorOp) {
        let seq = self.next_seq.fetch_add(1, Ordering::AcqRel);
        let encoded = match serde_json::to_vec(&op) {
            Ok(encoded) => encoded,
            Err(error) => {
                error!(
                    op = op.name(),
                    snapshot_id = %op.snapshot_id(),
                    %error,
                    "could not record an owed catalog mirror write; the object store is now \
                     behind by one write nothing will replay"
                );
                return;
            }
        };
        if let Err(error) = self.store.put(encode_seq(seq), encoded).await {
            error!(
                op = op.name(),
                snapshot_id = %op.snapshot_id(),
                %error,
                "could not record an owed catalog mirror write; the object store is now \
                 behind by one write nothing will replay"
            );
            return;
        }
        set_mirror_lag(self.lag.fetch_add(1, Ordering::AcqRel) + 1);
    }

    /// Replays everything owed, in order, once.
    ///
    /// 🔴 Per-snapshot ordering, not global ordering. Two writes to one
    /// snapshot have to land in the order they were made — a commit replayed
    /// before the create it depends on is a different outcome — but a snapshot
    /// whose queue is stuck must not hold up every other snapshot's, which is
    /// what a single global stop-on-first-failure would do.
    pub async fn drain_once(&self, target: &dyn SnapshotCatalog) -> anyhow::Result<RepairPass> {
        let entries = self
            .store
            .entries()
            .await
            .context("read the snapshot catalog mirror backlog")?;

        let mut pass = RepairPass::default();
        let mut blocked: HashSet<SnapshotId> = HashSet::new();

        for (key, value) in entries {
            if decode_seq(&key).is_none() {
                continue;
            }
            let op: MirrorOp = match serde_json::from_slice(&value) {
                Ok(op) => op,
                Err(error) => {
                    // Unreadable and never going to become readable. Left in
                    // place so the lag still counts it and the read side stays
                    // pinned; an operator has to look.
                    error!(
                        %error,
                        "an owed catalog mirror write cannot be decoded; the object store is \
                         behind by a write nothing can replay"
                    );
                    pass.diverged += 1;
                    continue;
                }
            };

            if blocked.contains(op.snapshot_id()) {
                pass.skipped += 1;
                continue;
            }

            let replay = op.replay(target).await;
            let already_applied = if replay.is_ok() {
                None
            } else {
                op.already_applied(target).await.ok()
            };
            let verdict = verdict_for(&replay, already_applied);

            match verdict {
                RepairVerdict::Repaired => {
                    if let Err(error) = self.store.delete(key).await {
                        // The write landed; only the note saying it was owed
                        // did not go away. Retrying it is safe — every replay
                        // is checked against the target first — so leave it.
                        warn!(%error, "could not clear a repaired catalog mirror entry");
                        pass.retry += 1;
                        blocked.insert(op.snapshot_id().clone());
                        continue;
                    }
                    set_mirror_lag(self.lag.fetch_sub(1, Ordering::AcqRel).saturating_sub(1));
                    record_mirror_repaired(op.name());
                    pass.repaired += 1;
                }
                RepairVerdict::Retry | RepairVerdict::Diverged => {
                    if verdict == RepairVerdict::Diverged {
                        error!(
                            op = op.name(),
                            snapshot_id = %op.snapshot_id(),
                            error = ?replay.as_ref().err(),
                            "the object store refused an owed catalog mirror write in a way a \
                             retry cannot change; the two catalogs disagree about this snapshot"
                        );
                        pass.diverged += 1;
                    } else {
                        pass.retry += 1;
                    }
                    record_mirror_repair_failed(op.name(), verdict.as_str());
                    blocked.insert(op.snapshot_id().clone());
                }
            }
        }

        pass.remaining = self.lag();
        Ok(pass)
    }
}

/// Big-endian so the store's key order is the order the writes were made.
///
/// Prefixed so the one piece of metadata this store also holds — which side
/// reads came from last — cannot land inside the queue's key range and be
/// replayed as if it were a write.
fn encode_seq(seq: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + 8);
    key.push(OWED_PREFIX);
    key.extend_from_slice(&seq.to_be_bytes());
    key
}

fn decode_seq(key: &[u8]) -> Option<u64> {
    let (prefix, seq) = key.split_first()?;
    if *prefix != OWED_PREFIX {
        return None;
    }
    seq.try_into().ok().map(u64::from_be_bytes)
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;

    use super::*;
    use crate::snapshot::repository::interfaces::SnapshotListFilter;
    use crate::snapshot::types::{CommittedSnapshot, SnapshotAlias, SnapshotPublishSource};
    use crate::types::SandboxResources;

    /// A catalog that answers however a test tells it to, and writes down what
    /// it was asked.
    #[derive(Default)]
    struct ScriptedCatalog {
        calls: Mutex<Vec<String>>,
        /// Ids whose writes fail, and how.
        failing: Mutex<Vec<(SnapshotId, bool)>>,
        /// Ids the catalog claims to already hold.
        holding: Mutex<Vec<SnapshotId>>,
        /// When set, `get` itself fails.
        get_fails: bool,
    }

    impl ScriptedCatalog {
        fn calls(&self) -> Vec<String> {
            self.calls.lock().expect("calls").clone()
        }

        fn record(&self, entry: impl Into<String>) {
            self.calls.lock().expect("calls").push(entry.into());
        }

        /// `retryable` picks which error kind the write fails with.
        fn fail(&self, id: &SnapshotId, retryable: bool) {
            self.failing
                .lock()
                .expect("failing")
                .push((id.clone(), retryable));
        }

        fn hold(&self, id: &SnapshotId) {
            self.holding.lock().expect("holding").push(id.clone());
        }

        fn outcome(&self, id: &SnapshotId) -> RepositoryResult<()> {
            let failing = self.failing.lock().expect("failing");
            match failing.iter().find(|(failing, _)| failing == id) {
                Some((_, true)) => Err(RepositoryError::Backend {
                    message: "the store is unreachable".to_string(),
                    source: None,
                }),
                Some((_, false)) => Err(RepositoryError::InvalidRequest {
                    reason: "the store will not take this".to_string(),
                }),
                None => Ok(()),
            }
        }
    }

    #[async_trait]
    impl SnapshotCatalog for ScriptedCatalog {
        async fn create(&self, record: SnapshotRecord) -> RepositoryResult<SnapshotRecord> {
            self.record(format!("create:{}", record.id));
            self.outcome(&record.id).map(|()| record)
        }

        async fn publish_commit(&self, commit: SnapshotCommit) -> RepositoryResult<SnapshotRecord> {
            self.record(format!("publish_commit:{}", commit.id));
            self.outcome(&commit.id)?;
            let mut record = SnapshotRecord::template_waiting(
                commit.id.clone(),
                commit.alias.clone(),
                commit.resources,
            );
            record.mark_committed(
                commit.alias,
                commit.resources,
                commit.committed,
                commit.source,
                0,
            );
            Ok(record)
        }

        async fn get(&self, id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>> {
            self.record(format!("get:{id_or_alias}"));
            if self.get_fails {
                return Err(RepositoryError::Backend {
                    message: "cannot read".to_string(),
                    source: None,
                });
            }
            let held = self
                .holding
                .lock()
                .expect("holding")
                .iter()
                .any(|id| id.to_string() == id_or_alias);
            if !held {
                return Ok(None);
            }
            let id = SnapshotId::parse(id_or_alias).expect("a held id parses");
            let mut record =
                SnapshotRecord::template_waiting(id, None, SandboxResources::default());
            record.mark_committed(
                None,
                SandboxResources::default(),
                committed(),
                SnapshotPublishSource::Template,
                0,
            );
            Ok(Some(record))
        }

        async fn list(&self, _filter: SnapshotListFilter) -> RepositoryResult<Vec<SnapshotRecord>> {
            Ok(Vec::new())
        }

        async fn delete_record(&self, record: &SnapshotRecord) -> RepositoryResult<()> {
            self.record(format!("delete_record:{}", record.id));
            self.outcome(&record.id)
        }

        async fn resolve_alias(&self, _alias: &str) -> RepositoryResult<Option<SnapshotId>> {
            Ok(None)
        }

        async fn try_start_build(&self, id: &SnapshotId) -> RepositoryResult<SnapshotRecord> {
            self.record(format!("try_start_build:{id}"));
            self.outcome(id)?;
            Ok(SnapshotRecord::template_waiting(
                id.clone(),
                None,
                SandboxResources::default(),
            ))
        }

        async fn mark_build_error(
            &self,
            id: &SnapshotId,
            _reason: TemplateBuildErrorReason,
        ) -> RepositoryResult<()> {
            self.record(format!("mark_build_error:{id}"));
            self.outcome(id)
        }
    }

    fn committed() -> CommittedSnapshot {
        CommittedSnapshot::mock()
    }

    fn commit_for(id: &SnapshotId, alias: Option<&str>) -> SnapshotCommit {
        SnapshotCommit {
            id: id.clone(),
            alias: alias.map(|alias| SnapshotAlias::parse(alias).expect("alias parses")),
            source: SnapshotPublishSource::Template,
            resources: SandboxResources::default(),
            committed: committed(),
        }
    }

    fn record_for(id: &SnapshotId) -> SnapshotRecord {
        SnapshotRecord::template_waiting(id.clone(), None, SandboxResources::default())
    }

    async fn backlog(dir: &tempfile::TempDir) -> Arc<MirrorBacklog> {
        MirrorBacklog::open(dir.path().join("mirror"))
            .await
            .expect("the backlog should open")
    }

    // ── the repair decision ─────────────────────────────────────────────

    #[test]
    fn a_successful_replay_is_repaired() {
        assert_eq!(verdict_for(&Ok(()), None), RepairVerdict::Repaired);
    }

    /// 🔴 The lost-acknowledgement case. The first attempt landed and only its
    /// answer went missing, so the replay is refused — and the target, asked
    /// directly, already has it. Reading this as anything but repaired leaves
    /// an entry nothing can ever clear.
    #[test]
    fn a_replay_the_target_already_reflects_is_repaired() {
        let refused: RepositoryResult<()> = Err(RepositoryError::InvalidRequest {
            reason: "already exists".to_string(),
        });
        assert_eq!(verdict_for(&refused, Some(true)), RepairVerdict::Repaired);
    }

    /// 🔴 A probe that failed says nothing either way, so the failure of the
    /// replay says nothing either. Calling this diverged would abandon a write
    /// over a store that was merely unreachable for a moment.
    #[test]
    fn an_unanswerable_probe_is_retried() {
        let refused: RepositoryResult<()> = Err(RepositoryError::InvalidRequest {
            reason: "already exists".to_string(),
        });
        assert_eq!(verdict_for(&refused, None), RepairVerdict::Retry);
    }

    #[test]
    fn a_transport_failure_the_target_does_not_reflect_is_retried() {
        let unreachable: RepositoryResult<()> = Err(RepositoryError::Backend {
            message: "connection refused".to_string(),
            source: None,
        });
        assert_eq!(verdict_for(&unreachable, Some(false)), RepairVerdict::Retry);
    }

    /// 🔴 The one that must not be retried forever. The store stated a rule —
    /// the name is taken — and asking again in thirty seconds gets the same
    /// answer while everything behind it stops.
    #[test]
    fn a_refusal_the_target_does_not_reflect_is_diverged() {
        for refusal in [
            RepositoryError::AliasConflict {
                alias: "taken".to_string(),
                existing: SnapshotId::generate(),
                new_id: SnapshotId::generate(),
            },
            RepositoryError::InvalidRequest {
                reason: "no".to_string(),
            },
            RepositoryError::Unsupported {
                feature: "no".to_string(),
            },
        ] {
            let refused: RepositoryResult<()> = Err(refusal);
            assert_eq!(
                verdict_for(&refused, Some(false)),
                RepairVerdict::Diverged,
                "a rule the store stated must not be retried forever"
            );
        }
    }

    // ── the pass ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn a_recorded_write_raises_the_lag_and_a_repaired_one_lowers_it() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let backlog = backlog(&dir).await;
        let id = SnapshotId::generate();

        assert_eq!(backlog.lag(), 0);
        backlog
            .record(MirrorOp::PublishCommit {
                commit: commit_for(&id, None),
            })
            .await;
        assert_eq!(backlog.lag(), 1);

        let target = ScriptedCatalog::default();
        let pass = backlog
            .drain_once(&target)
            .await
            .expect("the pass should run");

        assert_eq!(pass.repaired, 1);
        assert_eq!(pass.remaining, 0);
        assert_eq!(backlog.lag(), 0);
        assert_eq!(target.calls(), vec![format!("publish_commit:{id}")]);
    }

    /// 🔴 Per-snapshot ordering. A commit replayed before the create it depends
    /// on is a different outcome, so one snapshot's stuck entry holds the rest
    /// of *its own* queue — and nothing else's.
    #[tokio::test]
    async fn a_stuck_snapshot_holds_only_its_own_queue() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let backlog = backlog(&dir).await;
        let stuck = SnapshotId::generate();
        let healthy = SnapshotId::generate();

        backlog
            .record(MirrorOp::Create {
                record: record_for(&stuck),
            })
            .await;
        backlog
            .record(MirrorOp::PublishCommit {
                commit: commit_for(&stuck, None),
            })
            .await;
        backlog
            .record(MirrorOp::Create {
                record: record_for(&healthy),
            })
            .await;

        let target = ScriptedCatalog::default();
        target.fail(&stuck, true);

        let pass = backlog
            .drain_once(&target)
            .await
            .expect("the pass should run");

        assert_eq!(pass.retry, 1, "the stuck snapshot's first entry retries");
        assert_eq!(pass.skipped, 1, "its second entry is not attempted");
        assert_eq!(pass.repaired, 1, "the other snapshot's entry is repaired");
        assert_eq!(backlog.lag(), 2);
        assert!(
            !target.calls().contains(&format!("publish_commit:{stuck}")),
            "the second write for a snapshot whose first is stuck must not be attempted: {:?}",
            target.calls()
        );
        assert!(target.calls().contains(&format!("create:{healthy}")));
    }

    /// A diverged entry stays counted. It is a real disagreement between the
    /// two stores, and the lag is what keeps the read side from being moved
    /// over the top of it.
    #[tokio::test]
    async fn a_diverged_entry_keeps_counting_against_the_lag() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let backlog = backlog(&dir).await;
        let id = SnapshotId::generate();
        backlog
            .record(MirrorOp::Create {
                record: record_for(&id),
            })
            .await;

        let target = ScriptedCatalog::default();
        target.fail(&id, false);

        let pass = backlog
            .drain_once(&target)
            .await
            .expect("the pass should run");

        assert_eq!(pass.diverged, 1);
        assert_eq!(pass.repaired, 0);
        assert_eq!(backlog.lag(), 1);
    }

    /// The lost-acknowledgement case end to end: the replay is refused, the
    /// target turns out to have it, and the entry clears.
    #[tokio::test]
    async fn an_entry_the_target_already_holds_clears_without_being_rewritten() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let backlog = backlog(&dir).await;
        let id = SnapshotId::generate();
        backlog
            .record(MirrorOp::PublishCommit {
                commit: commit_for(&id, None),
            })
            .await;

        let target = ScriptedCatalog::default();
        target.fail(&id, false);
        target.hold(&id);

        let pass = backlog
            .drain_once(&target)
            .await
            .expect("the pass should run");

        assert_eq!(pass.repaired, 1);
        assert_eq!(backlog.lag(), 0);
    }

    /// 🔴 The whole reason this is on disk. A process that crashed holding
    /// owed writes must come back still owing them.
    #[tokio::test]
    async fn owed_writes_survive_the_process_that_recorded_them() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let id = SnapshotId::generate();
        {
            let backlog = backlog(&dir).await;
            backlog
                .record(MirrorOp::PublishCommit {
                    commit: commit_for(&id, None),
                })
                .await;
            assert_eq!(backlog.lag(), 1);
        }

        let reopened = backlog(&dir).await;
        assert_eq!(reopened.lag(), 1, "a restart must not forget what is owed");

        let target = ScriptedCatalog::default();
        let pass = reopened
            .drain_once(&target)
            .await
            .expect("the pass should run");
        assert_eq!(pass.repaired, 1);
        assert_eq!(target.calls(), vec![format!("publish_commit:{id}")]);
    }

    // ── the read-side guard ─────────────────────────────────────────────

    /// 🔴 The guard fires on the switch, not on the start. A node that crashed
    /// while already reading the object store has to come back; refusing there
    /// would turn a mirror that is behind into a node that is down.
    #[tokio::test]
    async fn an_ordinary_restart_on_the_object_store_is_allowed_with_a_backlog() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let backlog = backlog(&dir).await;
        backlog
            .guard_read_side(CatalogReadSide::ObjectStore)
            .await
            .expect("the first start records the side");
        backlog
            .record(MirrorOp::Create {
                record: record_for(&SnapshotId::generate()),
            })
            .await;

        backlog
            .guard_read_side(CatalogReadSide::ObjectStore)
            .await
            .expect("a restart on the same side must be allowed");
    }

    /// 🔴 Moving reads back onto a store that is behind would report every
    /// owed snapshot as absent, and absence is an instruction downstream.
    #[tokio::test]
    async fn moving_reads_back_to_the_object_store_is_refused_while_it_is_behind() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let backlog = backlog(&dir).await;
        backlog
            .guard_read_side(CatalogReadSide::Postgres)
            .await
            .expect("recording the side should work");
        backlog
            .record(MirrorOp::Create {
                record: record_for(&SnapshotId::generate()),
            })
            .await;

        let error = backlog
            .guard_read_side(CatalogReadSide::ObjectStore)
            .await
            .expect_err("the switch must be refused while writes are owed");
        assert!(
            error.to_string().contains("behind by 1 write"),
            "the refusal must say how far behind: {error}"
        );

        // The control: with nothing owed, the same switch is allowed.
        let target = ScriptedCatalog::default();
        backlog
            .drain_once(&target)
            .await
            .expect("the pass should run");
        assert_eq!(backlog.lag(), 0);
        backlog
            .guard_read_side(CatalogReadSide::ObjectStore)
            .await
            .expect("a drained mirror must let the switch through");
    }
}
