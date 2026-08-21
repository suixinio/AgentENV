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
