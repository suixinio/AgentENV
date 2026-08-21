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
    SnapshotCatalog, SnapshotCommit, SnapshotListFilter, StartedBuild,
};
use crate::snapshot::repository::{RepositoryError, RepositoryResult};
use crate::snapshot::types::{
    CommittedSnapshot, SnapshotAlias, SnapshotId, SnapshotPublishSource, SnapshotRecord,
    TemplateBuildErrorReason, TemplateBuildStatus,
};
use crate::types::SandboxResources;

use super::central::CentralCatalogWrites;

/// One instant, shared by every fixture here.
///
/// 🔴 Fixed rather than read from the clock. The checks these doubles feed
/// compare the two catalogs' rows against each other, and two fixtures built a
/// millisecond apart would disagree about a snapshot's creation time for a
/// reason that has nothing to do with what the test is holding still.
pub(crate) const CREATED_AT: i64 = 1_700_000_000_000;

pub(crate) fn committed() -> CommittedSnapshot {
    CommittedSnapshot::mock()
}

pub(crate) fn record_for(id: &SnapshotId) -> SnapshotRecord {
    let mut record =
        SnapshotRecord::template_waiting(id.clone(), None, SandboxResources::default());
    record.created_at_unix_ms = CREATED_AT;
    record.updated_at_unix_ms = CREATED_AT;
    record
}

pub(crate) fn commit_for(id: &SnapshotId, alias: Option<&str>) -> SnapshotCommit {
    SnapshotCommit {
        id: id.clone(),
        alias: alias.map(|alias| SnapshotAlias::parse(alias).expect("alias parses")),
        source: SnapshotPublishSource::Template,
        resources: SandboxResources::default(),
        created_at_unix_ms: Some(CREATED_AT),
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
/// 🔴 It keeps the rows it is given, rather than answering `get` from a set of
/// ids. The double write's own checks now compare this store's row against the
/// central catalog's, and a `get` that returned a *synthesised* record would
/// make every one of those comparisons a test of the fixture rather than of the
/// code — agreeing or disagreeing for reasons no production store has.
#[derive(Default)]
pub(crate) struct ScriptedCatalog {
    calls: Mutex<Vec<String>>,
    /// Ids whose writes fail, and how.
    failing: Mutex<Vec<(SnapshotId, bool)>>,
    /// The rows this store holds.
    rows: Mutex<HashMap<String, SnapshotRecord>>,
    /// When set, every write fails as unreachable.
    broken: AtomicBool,
    /// When set, `get` itself fails.
    get_fails: AtomicBool,
    /// When set, the writes that bind a name refuse with an alias conflict.
    alias_conflict: Mutex<Option<SnapshotId>>,
    /// What `list` answers with — the rows this store held before anybody
    /// started mirroring it.
    history: Mutex<Vec<SnapshotRecord>>,
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

    /// Puts a committed row in without going through a write.
    pub(crate) fn hold(&self, id: &SnapshotId) {
        self.keep(committed_record(id));
    }

    /// Puts any row in without going through a write.
    pub(crate) fn seed(&self, record: SnapshotRecord) {
        self.keep(record);
    }

    pub(crate) fn holds(&self, id: &SnapshotId) -> Option<SnapshotRecord> {
        self.rows
            .lock()
            .expect("rows")
            .get(&id.to_string())
            .cloned()
    }

    fn keep(&self, record: SnapshotRecord) {
        self.rows
            .lock()
            .expect("rows")
            .insert(record.id.to_string(), record);
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

    /// Gives the store a past: rows that exist and that nothing mirrored.
    ///
    /// They go into the rows as well as into what `list` answers, because a
    /// store that lists a snapshot it cannot then be asked about is not a state
    /// any real one reaches.
    pub(crate) fn with_history(self, records: Vec<SnapshotRecord>) -> Self {
        for record in &records {
            self.keep(record.clone());
        }
        *self.history.lock().expect("history") = records;
        self
    }

    /// The conflict a name-binding write answers with, if one is armed.
    fn refused_alias(
        &self,
        alias: Option<&SnapshotAlias>,
        id: &SnapshotId,
    ) -> Option<RepositoryError> {
        let holder = self.alias_conflict.lock().expect("alias").clone()?;
        Some(RepositoryError::AliasConflict {
            alias: alias.map(ToString::to_string).unwrap_or_default(),
            existing: holder,
            new_id: id.clone(),
        })
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
        // `create` binds a name too, and the store refuses it the same way
        // `publish_commit` does. Modelling the conflict on only one of them is
        // what let the missing carve-out on the other go unnoticed.
        if let Some(conflict) = self.refused_alias(record.alias.as_ref(), &record.id) {
            return Err(conflict);
        }
        self.outcome(&record.id)?;
        self.keep(record.clone());
        Ok(record)
    }

    async fn publish_commit(&self, commit: SnapshotCommit) -> RepositoryResult<SnapshotRecord> {
        self.note(format!("publish_commit:{}", commit.id));
        if let Some(conflict) = self.refused_alias(commit.alias.as_ref(), &commit.id) {
            return Err(conflict);
        }
        self.outcome(&commit.id)?;
        // Folds into the row this store already holds, exactly as the real
        // backends do — which is what keeps the creation time of a template
        // published long after it was created from moving.
        let mut record = self.holds(&commit.id).unwrap_or_else(|| {
            let mut fresh = record_for(&commit.id);
            fresh.created_at_unix_ms = commit.created_at_unix_ms.unwrap_or(CREATED_AT);
            fresh.updated_at_unix_ms = fresh.created_at_unix_ms;
            fresh
        });
        record.mark_committed(
            commit.alias,
            commit.resources,
            commit.committed,
            commit.source,
            record.created_at_unix_ms,
        );
        self.keep(record.clone());
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
        Ok(self.rows.lock().expect("rows").get(id_or_alias).cloned())
    }

    async fn list(&self, _filter: SnapshotListFilter) -> RepositoryResult<Vec<SnapshotRecord>> {
        if self.broken.load(Ordering::SeqCst) {
            return Err(RepositoryError::Backend {
                message: "cannot read".to_string(),
                source: None,
            });
        }
        Ok(self.history.lock().expect("history").clone())
    }

    async fn delete_record(&self, record: &SnapshotRecord) -> RepositoryResult<()> {
        self.note(format!("delete_record:{}", record.id));
        self.outcome(&record.id)?;
        self.rows
            .lock()
            .expect("rows")
            .remove(&record.id.to_string());
        Ok(())
    }

    async fn resolve_alias(&self, alias: &str) -> RepositoryResult<Option<SnapshotId>> {
        self.note(format!("resolve_alias:{alias}"));
        if self.get_fails.load(Ordering::SeqCst) || self.broken.load(Ordering::SeqCst) {
            return Err(RepositoryError::Backend {
                message: "cannot read".to_string(),
                source: None,
            });
        }
        Ok(self
            .rows
            .lock()
            .expect("rows")
            .values()
            .find(|record| {
                record
                    .alias
                    .as_ref()
                    .is_some_and(|held| held.to_string() == alias)
            })
            .map(|record| record.id.clone()))
    }

    async fn try_start_build(&self, id: &SnapshotId) -> RepositoryResult<StartedBuild> {
        self.note(format!("try_start_build:{id}"));
        self.outcome(id)?;
        let mut record = self.holds(id).unwrap_or_else(|| record_for(id));
        if let crate::snapshot::types::SnapshotSource::Template { build } = &mut record.source {
            build.status = TemplateBuildStatus::Building;
        }
        self.keep(record.clone());
        Ok(StartedBuild::untracked(record))
    }

    async fn mark_build_error(
        &self,
        id: &SnapshotId,
        reason: TemplateBuildErrorReason,
    ) -> RepositoryResult<()> {
        self.note(format!("mark_build_error:{id}"));
        self.outcome(id)?;
        let mut record = self.holds(id).unwrap_or_else(|| record_for(id));
        if let crate::snapshot::types::SnapshotSource::Template { build } = &mut record.source {
            build.status = TemplateBuildStatus::Error;
            build.error_reason = Some(reason);
        }
        self.keep(record);
        Ok(())
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
    StartBuild,
}

impl CentralCall {
    fn as_str(self) -> &'static str {
        match self {
            Self::Begin => "begin",
            Self::Commit => "commit",
            Self::Fail => "fail",
            Self::Delete => "delete",
            Self::StartBuild => "start_build",
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
    rejected: Mutex<Vec<CentralCall>>,
    refusals: Mutex<Vec<(CentralCall, CatalogRefusal)>>,
    /// Builds this double has admitted and not yet had taken away.
    ///
    /// 🔴 Held so `renew_build_lease` can answer `false` for a build the
    /// reaper took. A double that always said "still yours" would make the one
    /// notice a builder gets untestable — and the bug it prevents, two builders
    /// publishing into one template, is not one anybody finds afterwards.
    live_builds: Mutex<std::collections::HashSet<String>>,
}

impl ScriptedCentral {
    pub(crate) fn calls(&self) -> Vec<String> {
        self.calls.lock().expect("calls").clone()
    }

    fn note(&self, entry: impl Into<String>) {
        self.calls.lock().expect("calls").push(entry.into());
    }

    /// Takes a build away, as the reaper does when a heartbeat lapses.
    pub(crate) fn reap_build(&self, id: &SnapshotId) {
        self.live_builds
            .lock()
            .expect("live builds")
            .remove(&id.to_string());
    }

    /// Makes one call fail as if nothing answered.
    pub(crate) fn unreachable_on(&self, call: CentralCall) {
        self.unreachable.lock().expect("unreachable").push(call);
    }

    pub(crate) fn reachable_again(&self) {
        self.unreachable.lock().expect("unreachable").clear();
    }

    /// Makes one call fail the way a controller that will never accept this
    /// request fails: an answer, arriving as a status code rather than as a
    /// [`CatalogRefusal`], that asking again cannot change.
    pub(crate) fn reject_permanently_on(&self, call: CentralCall) {
        self.rejected.lock().expect("rejected").push(call);
    }

    /// Makes one call answer with a refusal.
    pub(crate) fn refuse(&self, call: CentralCall, refusal: CatalogRefusal) {
        self.refusals
            .lock()
            .expect("refusals")
            .push((call, refusal));
    }

    /// Stops one call refusing, so a test can arrange a disagreement and then
    /// go on using the double for something else.
    pub(crate) fn stop_refusing(&self, call: CentralCall) {
        self.refusals
            .lock()
            .expect("refusals")
            .retain(|(scripted, _)| *scripted != call);
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
        if self.rejected.lock().expect("rejected").contains(&call) {
            // The shape `CentralSnapshotCatalog::unreachable` produces for a
            // status the controller will repeat.
            return Some(Err(RepositoryError::InvalidRequest {
                reason: format!(
                    "the snapshot catalog rejected '{}' permanently: InvalidArgument: no",
                    call.as_str()
                ),
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
        // 🔴 The row opens in the status the *caller stated*, not in whatever
        // the record it was derived from happened to carry. The real catalog
        // refuses a row born `ready` or `error`, which is why `opening_status`
        // clamps — and a double that ignored the argument would hide every
        // consequence of that clamp.
        let mut opened = record.clone();
        if let crate::snapshot::types::SnapshotSource::Template { build } = &mut opened.source {
            build.status = match status {
                "building" => TemplateBuildStatus::Building,
                "ready" => TemplateBuildStatus::Ready,
                "error" => TemplateBuildStatus::Error,
                _ => TemplateBuildStatus::Waiting,
            };
        }
        rows.insert(opened.id.to_string(), opened.clone());
        Ok(CatalogWrite::Applied(opened))
    }

    /// Admission, as much of it as a double can be: it moves the row to
    /// `building` and remembers which build is live, so a lease renewal can
    /// answer something other than a constant.
    async fn start_build(
        &self,
        id: &SnapshotId,
        build_id: &SnapshotId,
        _started_at_unix_ms: i64,
    ) -> RepositoryResult<CatalogWrite<StartedBuild>> {
        self.note(format!("start_build:{id}"));
        if let Some(scripted) = self.scripted(CentralCall::StartBuild) {
            return scripted;
        }
        let mut rows = self.rows.lock().expect("rows");
        let Some(row) = rows.get_mut(&id.to_string()) else {
            return Ok(CatalogWrite::Refused(CatalogRefusal::NotFound));
        };
        if let crate::snapshot::types::SnapshotSource::Template { build } = &mut row.source {
            build.status = TemplateBuildStatus::Building;
        }
        self.live_builds
            .lock()
            .expect("live builds")
            .insert(build_id.to_string());
        Ok(CatalogWrite::Applied(StartedBuild {
            record: row.clone(),
            build_id: build_id.clone(),
        }))
    }

    async fn renew_build_lease(&self, build_id: &SnapshotId) -> RepositoryResult<bool> {
        self.note(format!("renew_build_lease:{build_id}"));
        Ok(self
            .live_builds
            .lock()
            .expect("live builds")
            .contains(&build_id.to_string()))
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
