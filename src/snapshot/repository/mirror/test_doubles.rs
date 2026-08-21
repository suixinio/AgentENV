//! Stores that answer however a test tells them to.
//!
//! Shared by the backlog's tests, the compensator's and the double write's, so
//! that "the object store refused" means the same thing in all three.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use async_trait::async_trait;

use crate::snapshot::repository::backends::central::{CatalogRefusal, CatalogWrite};
use crate::snapshot::repository::interfaces::{
    SnapshotCatalog, SnapshotCommit, SnapshotListFilter,
};
use crate::snapshot::repository::{RepositoryError, RepositoryResult};
use crate::snapshot::types::{
    CommittedSnapshot, SnapshotAlias, SnapshotId, SnapshotPublishSource, SnapshotRecord,
    TemplateBuildErrorReason, TemplateBuildStatus,
};
use crate::types::SandboxResources;

use super::central::CentralCatalogWrites;

pub(crate) fn committed() -> CommittedSnapshot {
    CommittedSnapshot::mock()
}

pub(crate) fn record_for(id: &SnapshotId) -> SnapshotRecord {
    SnapshotRecord::template_waiting(id.clone(), None, SandboxResources::default())
}

pub(crate) fn commit_for(id: &SnapshotId, alias: Option<&str>) -> SnapshotCommit {
    SnapshotCommit {
        id: id.clone(),
        alias: alias.map(|alias| SnapshotAlias::parse(alias).expect("alias parses")),
        source: SnapshotPublishSource::Template,
        resources: SandboxResources::default(),
        committed: committed(),
    }
}

/// A committed row for `id`, as a store that took the commit would hold it.
pub(crate) fn committed_record(id: &SnapshotId) -> SnapshotRecord {
    let mut record = record_for(id);
    record.mark_committed(
        None,
        SandboxResources::default(),
        committed(),
        SnapshotPublishSource::Template,
        0,
    );
    record
}

// ─────────────────────────────────────────────────────────────────────────────
// The object-store side
// ─────────────────────────────────────────────────────────────────────────────

/// A catalog that answers however a test tells it to, and writes down what it
/// was asked.
#[derive(Default)]
pub(crate) struct ScriptedCatalog {
    calls: Mutex<Vec<String>>,
    /// Ids whose writes fail, and how.
    failing: Mutex<Vec<(SnapshotId, bool)>>,
    /// Ids the catalog claims to already hold.
    holding: Mutex<Vec<SnapshotId>>,
    /// When set, every write fails as unreachable.
    broken: AtomicBool,
    /// When set, `get` itself fails.
    get_fails: AtomicBool,
    /// When set, `publish_commit` refuses with an alias conflict.
    alias_conflict: Mutex<Option<SnapshotId>>,
}

impl ScriptedCatalog {
    pub(crate) fn calls(&self) -> Vec<String> {
        self.calls.lock().expect("calls").clone()
    }

    fn note(&self, entry: impl Into<String>) {
        self.calls.lock().expect("calls").push(entry.into());
    }

    /// `retryable` picks which error kind the write fails with.
    pub(crate) fn fail(&self, id: &SnapshotId, retryable: bool) {
        self.failing
            .lock()
            .expect("failing")
            .push((id.clone(), retryable));
    }

    pub(crate) fn hold(&self, id: &SnapshotId) {
        self.holding.lock().expect("holding").push(id.clone());
    }

    pub(crate) fn break_it(&self) {
        self.broken.store(true, Ordering::SeqCst);
    }

    pub(crate) fn fix_it(&self) {
        self.broken.store(false, Ordering::SeqCst);
    }

    pub(crate) fn break_reads(&self) {
        self.get_fails.store(true, Ordering::SeqCst);
    }

    pub(crate) fn refuse_alias_to(&self, holder: SnapshotId) {
        *self.alias_conflict.lock().expect("alias") = Some(holder);
    }

    fn outcome(&self, id: &SnapshotId) -> RepositoryResult<()> {
        if self.broken.load(Ordering::SeqCst) {
            return Err(RepositoryError::Backend {
                message: "object storage is unreachable".to_string(),
                source: None,
            });
        }
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
        self.note(format!("create:{}", record.id));
        self.outcome(&record.id).map(|()| record)
    }

    async fn publish_commit(&self, commit: SnapshotCommit) -> RepositoryResult<SnapshotRecord> {
        self.note(format!("publish_commit:{}", commit.id));
        if let Some(holder) = self.alias_conflict.lock().expect("alias").clone() {
            return Err(RepositoryError::AliasConflict {
                alias: commit
                    .alias
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_default(),
                existing: holder,
                new_id: commit.id.clone(),
            });
        }
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
        self.note(format!("get:{id_or_alias}"));
        if self.get_fails.load(Ordering::SeqCst) || self.broken.load(Ordering::SeqCst) {
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
        Ok(Some(committed_record(&id)))
    }

    async fn list(&self, _filter: SnapshotListFilter) -> RepositoryResult<Vec<SnapshotRecord>> {
        if self.broken.load(Ordering::SeqCst) {
            return Err(RepositoryError::Backend {
                message: "cannot read".to_string(),
                source: None,
            });
        }
        Ok(Vec::new())
    }

    async fn delete_record(&self, record: &SnapshotRecord) -> RepositoryResult<()> {
        self.note(format!("delete_record:{}", record.id));
        self.outcome(&record.id)
    }

    async fn resolve_alias(&self, _alias: &str) -> RepositoryResult<Option<SnapshotId>> {
        Ok(None)
    }

    async fn try_start_build(&self, id: &SnapshotId) -> RepositoryResult<SnapshotRecord> {
        self.note(format!("try_start_build:{id}"));
        self.outcome(id)?;
        let mut record = record_for(id);
        if let crate::snapshot::types::SnapshotSource::Template { build } = &mut record.source {
            build.status = TemplateBuildStatus::Building;
        }
        Ok(record)
    }

    async fn mark_build_error(
        &self,
        id: &SnapshotId,
        _reason: TemplateBuildErrorReason,
    ) -> RepositoryResult<()> {
        self.note(format!("mark_build_error:{id}"));
        self.outcome(id)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// The central side
// ─────────────────────────────────────────────────────────────────────────────

/// Which central call a test wants to break.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CentralCall {
    Begin,
    Commit,
    Fail,
    Delete,
}

impl CentralCall {
    fn as_str(self) -> &'static str {
        match self {
            Self::Begin => "begin",
            Self::Commit => "commit",
            Self::Fail => "fail",
            Self::Delete => "delete",
        }
    }
}

/// A central catalog whose every call can be broken or made to refuse on its
/// own.
///
/// 🔴 That is the whole point of it. `publish_commit` makes two central calls
/// and each one is a separate guard; against one real endpoint the only failure
/// inducible is "the whole catalog is down", which breaks both at once and
/// leaves either guard removable without a test noticing.
#[derive(Default)]
pub(crate) struct ScriptedCentral {
    calls: Mutex<Vec<String>>,
    rows: Mutex<HashMap<String, SnapshotRecord>>,
    unreachable: Mutex<Vec<CentralCall>>,
    refusals: Mutex<Vec<(CentralCall, CatalogRefusal)>>,
}

impl ScriptedCentral {
    pub(crate) fn calls(&self) -> Vec<String> {
        self.calls.lock().expect("calls").clone()
    }

    fn note(&self, entry: impl Into<String>) {
        self.calls.lock().expect("calls").push(entry.into());
    }

    /// Makes one call fail as if nothing answered.
    pub(crate) fn unreachable_on(&self, call: CentralCall) {
        self.unreachable.lock().expect("unreachable").push(call);
    }

    pub(crate) fn reachable_again(&self) {
        self.unreachable.lock().expect("unreachable").clear();
    }

    /// Makes one call answer with a refusal.
    pub(crate) fn refuse(&self, call: CentralCall, refusal: CatalogRefusal) {
        self.refusals
            .lock()
            .expect("refusals")
            .push((call, refusal));
    }

    /// Puts a row in without going through a write, so a test can arrange a
    /// catalog that already holds something.
    pub(crate) fn seed(&self, record: SnapshotRecord) {
        self.rows
            .lock()
            .expect("rows")
            .insert(record.id.to_string(), record);
    }

    pub(crate) fn holds(&self, id: &SnapshotId) -> Option<SnapshotRecord> {
        self.rows
            .lock()
            .expect("rows")
            .get(&id.to_string())
            .cloned()
    }

    fn scripted<T>(&self, call: CentralCall) -> Option<RepositoryResult<CatalogWrite<T>>> {
        if self
            .unreachable
            .lock()
            .expect("unreachable")
            .contains(&call)
        {
            return Some(Err(RepositoryError::Backend {
                message: format!("the central catalog's '{}' is unreachable", call.as_str()),
                source: None,
            }));
        }
        let refusal = self
            .refusals
            .lock()
            .expect("refusals")
            .iter()
            .find(|(scripted, _)| *scripted == call)
            .map(|(_, refusal)| refusal.clone());
        refusal.map(|refusal| Ok(CatalogWrite::Refused(refusal)))
    }
}

#[async_trait]
impl CentralCatalogWrites for ScriptedCentral {
    async fn begin(
        &self,
        record: &SnapshotRecord,
        status: &str,
        _published: bool,
    ) -> RepositoryResult<CatalogWrite<SnapshotRecord>> {
        self.note(format!("begin:{}:{status}", record.id));
        if let Some(scripted) = self.scripted(CentralCall::Begin) {
            return scripted;
        }
        let mut rows = self.rows.lock().expect("rows");
        if rows.contains_key(&record.id.to_string()) {
            return Ok(CatalogWrite::Refused(CatalogRefusal::AlreadyExists));
        }
        rows.insert(record.id.to_string(), record.clone());
        Ok(CatalogWrite::Applied(record.clone()))
    }

    async fn commit(
        &self,
        commit: &SnapshotCommit,
        _published: bool,
        _updated_at_unix_ms: i64,
    ) -> RepositoryResult<CatalogWrite<SnapshotRecord>> {
        self.note(format!("commit:{}", commit.id));
        if let Some(scripted) = self.scripted(CentralCall::Commit) {
            return scripted;
        }
        let mut rows = self.rows.lock().expect("rows");
        let Some(row) = rows.get_mut(&commit.id.to_string()) else {
            return Ok(CatalogWrite::Refused(CatalogRefusal::NotFound));
        };
        row.mark_committed(
            commit.alias.clone(),
            commit.resources,
            commit.committed.clone(),
            commit.source.clone(),
            0,
        );
        Ok(CatalogWrite::Applied(row.clone()))
    }

    async fn fail(
        &self,
        id: &SnapshotId,
        _reason: &TemplateBuildErrorReason,
        _updated_at_unix_ms: i64,
    ) -> RepositoryResult<CatalogWrite<SnapshotRecord>> {
        self.note(format!("fail:{id}"));
        if let Some(scripted) = self.scripted(CentralCall::Fail) {
            return scripted;
        }
        let mut rows = self.rows.lock().expect("rows");
        let Some(row) = rows.get_mut(&id.to_string()) else {
            return Ok(CatalogWrite::Refused(CatalogRefusal::NotFound));
        };
        if let crate::snapshot::types::SnapshotSource::Template { build } = &mut row.source {
            build.status = TemplateBuildStatus::Error;
        }
        Ok(CatalogWrite::Applied(row.clone()))
    }

    async fn delete(&self, id_or_alias: &str, _deleted_at_unix_ms: i64) -> RepositoryResult<bool> {
        self.note(format!("delete:{id_or_alias}"));
        if self
            .unreachable
            .lock()
            .expect("unreachable")
            .contains(&CentralCall::Delete)
        {
            return Err(RepositoryError::Backend {
                message: "the central catalog's 'delete' is unreachable".to_string(),
                source: None,
            });
        }
        Ok(self
            .rows
            .lock()
            .expect("rows")
            .remove(id_or_alias)
            .is_some())
    }

    async fn get_any_status(&self, id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>> {
        Ok(self.rows.lock().expect("rows").get(id_or_alias).cloned())
    }

    async fn get_resolvable(&self, id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>> {
        Ok(self
            .rows
            .lock()
            .expect("rows")
            .get(id_or_alias)
            .filter(|record| record.committed.is_some())
            .cloned())
    }
}
