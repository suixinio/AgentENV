use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::task;

use super::layout::PosixFsSnapshotArtifactLayout;
use crate::snapshot::repository::interfaces::{SnapshotCatalog, SnapshotCommit};
use crate::snapshot::repository::metrics::{
    record_object_store_request, ObjectStoreOp, ObjectStoreOutcome, ObjectStoreSurface,
};
use crate::snapshot::repository::SnapshotListFilter;
use crate::snapshot::{
    RepositoryError, RepositoryResult, SnapshotAlias, SnapshotId, SnapshotPublishSource,
    SnapshotRecord, SnapshotSource, SnapshotSourceKind, TemplateBuildErrorReason,
    TemplateBuildInfo, TemplateBuildStatus,
};
const FILE_LOCK_TIMEOUT: Option<Duration> = Some(Duration::from_secs(10));
const ALIAS_LOCK_STALE_AGE: Duration = Duration::from_secs(60);
const RECORD_LOCK_STALE_AGE: Duration = Duration::from_secs(60);

/// Snapshot rows as one JSON file per record under `catalog/`, with an alias
/// directory beside it and a commit marker in each snapshot's artifact
/// directory.
///
/// The marker is the one place this store writes outside `catalog/`: it is what
/// makes "committed" mean "the row says so *and* the directory was sealed", and
/// it is what `retains_artifacts_on_publish_failure` reads to keep a failed
/// re-publish from deleting a live snapshot's bytes.
#[derive(Clone)]
pub struct PosixFsCatalogStore {
    root: PathBuf,
}

#[derive(Debug)]
struct PosixFileLockGuard {
    path: PathBuf,
}

impl Drop for PosixFileLockGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

impl PosixFsCatalogStore {
    /// Creates a catalog store rooted at the repository's durable POSIX directory.
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    fn layout(&self, snapshot_id: &SnapshotId) -> PosixFsSnapshotArtifactLayout {
        PosixFsSnapshotArtifactLayout::new(&self.root, snapshot_id)
    }

    fn commit_marker_path(&self, snapshot_id: &SnapshotId) -> PathBuf {
        self.layout(snapshot_id)
            .path(super::layout::POSIXFS_SNAPSHOT_COMMIT_MARKER)
    }

    fn record_path(&self, snapshot_id: &SnapshotId) -> PathBuf {
        PosixFsSnapshotArtifactLayout::record_path(&self.root, snapshot_id)
    }

    /// Commits one imported snapshot into the catalog and makes it visible via the commit marker.
    ///
    /// Flow:
    /// 1. acquire the alias lock when an alias is present
    /// 2. bind the alias
    /// 3. write the commit marker
    /// 4. write the committed snapshot record
    fn publish_commit_sync(&self, commit: SnapshotCommit) -> RepositoryResult<SnapshotRecord> {
        self.ensure_layout()?;
        let now = now_unix_ms();
        let snapshot_id = commit.id.clone();
        let write_result = if let Some(alias) = commit.alias.clone() {
            let alias = &alias;
            self.with_alias_lock(alias, |store| {
                let record = store.committed_record_unlocked(&commit, now)?;
                let alias_path = PosixFsSnapshotArtifactLayout::alias_path(&store.root, alias);
                if let Some(existing) = store.load_alias_target(alias)? {
                    if existing != snapshot_id {
                        if store.load_record_by_id_unlocked(&existing)?.is_some() {
                            return Err(RepositoryError::AliasConflict {
                                alias: alias.to_string(),
                                existing,
                                new_id: snapshot_id.clone(),
                            });
                        }
                        store.remove_file_if_exists(&alias_path)?;
                    }
                }
                store.write_json(&alias_path, &snapshot_id)?;
                store.write_commit_marker(&snapshot_id)?;
                store.write_committed_record_unlocked(&record)?;
                Ok(record)
            })
        } else {
            (|| {
                let record = self.committed_record_unlocked(&commit, now)?;
                self.write_commit_marker(&snapshot_id)?;
                self.write_committed_record_unlocked(&record)?;
                Ok(record)
            })()
        };

        match write_result {
            Ok(record) => Ok(record),
            Err(error) => {
                if let Some(alias) = commit.alias.as_ref() {
                    let _ = self.with_alias_lock(alias, |store| {
                        let alias_path =
                            PosixFsSnapshotArtifactLayout::alias_path(&store.root, alias);
                        if store.load_alias_target(alias)?.as_ref() == Some(&snapshot_id) {
                            store.remove_file_if_exists(&alias_path)?;
                        }
                        Ok(())
                    });
                }
                // The snapshot directory is not removed here: rolling back
                // bytes belongs to the artifact store, and the composite calls
                // it after asking `retains_artifacts_on_publish_failure`.
                Err(error)
            }
        }
    }

    fn create_sync(&self, record: SnapshotRecord) -> RepositoryResult<SnapshotRecord> {
        self.ensure_layout()?;
        if !matches!(record.source, SnapshotSource::Template { .. }) {
            return Err(RepositoryError::InvalidRequest {
                reason: "only template snapshots can be pre-created".to_string(),
            });
        }
        if record.committed.is_some() {
            return Err(RepositoryError::InvalidRequest {
                reason: "pre-created template snapshots must not already be committed".to_string(),
            });
        }
        if self.load_record_by_id_unlocked(&record.id)?.is_some() {
            return Err(RepositoryError::InvalidRequest {
                reason: format!("snapshot '{}' already exists", record.id),
            });
        }

        if let Some(alias) = record.alias.as_ref() {
            self.with_alias_lock(alias, |store| {
                store.ensure_alias_available(alias, &record.id)?;
                store.write_record_unlocked(&record)?;
                store.write_json(
                    &PosixFsSnapshotArtifactLayout::alias_path(&store.root, alias),
                    &record.id,
                )
            })?;
        } else {
            self.write_record_unlocked(&record)?;
        }
        Ok(record)
    }

    fn get_sync(&self, id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>> {
        self.ensure_layout()?;
        if let Ok(direct_id) = SnapshotId::parse(id_or_alias) {
            if let Some(record) = self.load_record_by_id_unlocked(&direct_id)? {
                return Ok(Some(record));
            }
        }

        let alias =
            SnapshotAlias::parse(id_or_alias).map_err(|error| RepositoryError::InvalidRequest {
                reason: error.to_string(),
            })?;
        self.with_alias_lock(&alias, |store| {
            let Some(id) = store.load_alias_target(&alias)? else {
                return Ok(None);
            };
            match store.load_record_by_id_unlocked(&id)? {
                Some(record) => Ok(Some(record)),
                None => {
                    store.remove_file_if_exists(&PosixFsSnapshotArtifactLayout::alias_path(
                        &store.root,
                        &alias,
                    ))?;
                    Ok(None)
                }
            }
        })
    }

    fn list_sync(&self, filter: SnapshotListFilter) -> RepositoryResult<Vec<SnapshotRecord>> {
        self.ensure_layout()?;
        let records_dir = self.records_dir();
        let mut records = Vec::new();
        let listed = fs::read_dir(&records_dir);
        record_object_store_request(
            ObjectStoreOp::List,
            ObjectStoreSurface::Catalog,
            ObjectStoreOutcome::from_success(listed.is_ok()),
        );
        for entry in listed.map_err(|error| {
            RepositoryError::backend(
                format!("read records dir '{}'", records_dir.display()),
                error,
            )
        })? {
            let entry = entry.map_err(|error| {
                RepositoryError::backend(
                    format!("read entry in '{}'", records_dir.display()),
                    error,
                )
            })?;
            if !entry
                .file_type()
                .map_err(|error| {
                    RepositoryError::backend(
                        format!("inspect file type '{}'", entry.path().display()),
                        error,
                    )
                })?
                .is_file()
            {
                continue;
            }
            if entry.path().extension().and_then(|ext| ext.to_str()) != Some("json") {
                continue;
            }
            let record: SnapshotRecord = self.read_json(&entry.path())?;
            if Self::matches_record_filter(&record, &filter) {
                records.push(record);
            }
        }
        records.sort_by(|left, right| {
            right
                .created_at_unix_ms
                .cmp(&left.created_at_unix_ms)
                .then_with(|| left.id.to_string().cmp(&right.id.to_string()))
        });
        Ok(records)
    }

    /// Removes the commit marker, the alias binding, and the row.
    ///
    /// The snapshot's artifact directory is *not* removed here; that is the
    /// artifact store's, and the composite calls it once this returns. Clearing
    /// the marker first still means a crash mid-delete leaves the snapshot
    /// uncommitted rather than half-committed.
    fn delete_record_sync(&self, record: &SnapshotRecord) -> RepositoryResult<()> {
        let id = &record.id;
        if let Some(alias) = record.alias.as_ref() {
            self.with_alias_lock(alias, |store| {
                let alias_path = PosixFsSnapshotArtifactLayout::alias_path(&store.root, alias);
                store.remove_file_if_exists(&store.commit_marker_path(id))?;
                if store.load_alias_target(alias)?.as_ref() == Some(id) {
                    store.remove_file_if_exists(&alias_path)?;
                }
                store.remove_file_if_exists(&store.record_path(id))
            })?;
            return Ok(());
        }
        self.remove_file_if_exists(&self.commit_marker_path(id))?;
        self.remove_file_if_exists(&self.record_path(id))?;
        Ok(())
    }

    /// Resolves one alias to a committed snapshot id and drops stale alias entries on the way.
    fn resolve_alias_sync(&self, alias: &str) -> RepositoryResult<Option<SnapshotId>> {
        let alias =
            SnapshotAlias::parse(alias).map_err(|error| RepositoryError::InvalidRequest {
                reason: error.to_string(),
            })?;
        self.with_alias_lock(&alias, |store| {
            let Some(id) = store.load_alias_target(&alias)? else {
                return Ok(None);
            };
            if store.load_record_by_id_unlocked(&id)?.is_some() {
                return Ok(Some(id));
            }
            let alias_path = PosixFsSnapshotArtifactLayout::alias_path(&store.root, &alias);
            store.remove_file_if_exists(&alias_path)?;
            Ok(None)
        })
    }

    fn aliases_dir(&self) -> PathBuf {
        PosixFsSnapshotArtifactLayout::aliases_dir(&self.root)
    }

    fn records_dir(&self) -> PathBuf {
        PosixFsSnapshotArtifactLayout::records_dir(&self.root)
    }

    fn snapshots_dir(&self) -> PathBuf {
        PosixFsSnapshotArtifactLayout::snapshots_dir(&self.root)
    }

    fn ensure_layout(&self) -> RepositoryResult<()> {
        let catalog_dir = PosixFsSnapshotArtifactLayout::catalog_dir(&self.root);
        let aliases_dir = self.aliases_dir();
        let records_dir = self.records_dir();
        let snapshots_dir = self.snapshots_dir();
        for dir in [&catalog_dir, &aliases_dir, &records_dir, &snapshots_dir] {
            fs::create_dir_all(dir).map_err(|error| {
                RepositoryError::backend(format!("create catalog dir '{}'", dir.display()), error)
            })?;
        }
        Ok(())
    }

    fn try_start_sync(&self, id: &SnapshotId) -> RepositoryResult<SnapshotRecord> {
        let _guard = self.acquire_record_lock(id)?;
        let mut record = self.load_record_by_id_unlocked(id)?.ok_or_else(|| {
            RepositoryError::SnapshotNotFound {
                lookup: id.to_string(),
            }
        })?;
        let now = now_unix_ms();
        let SnapshotSource::Template { build } = &mut record.source else {
            return Err(RepositoryError::InvalidRequest {
                reason: format!("snapshot '{id}' is not a template build"),
            });
        };
        if build.status != TemplateBuildStatus::Waiting {
            return Err(RepositoryError::InvalidRequest {
                reason: format!("template build '{id}' is not in waiting state"),
            });
        }
        build.status = TemplateBuildStatus::Building;
        build.started_at_unix_ms = Some(now);
        build.error_reason = None;
        record.updated_at_unix_ms = now;
        self.write_record_unlocked(&record)?;
        Ok(record)
    }

    fn mark_error_sync(
        &self,
        id: &SnapshotId,
        reason: TemplateBuildErrorReason,
    ) -> RepositoryResult<()> {
        let _guard = self.acquire_record_lock(id)?;
        let mut record = self.load_record_by_id_unlocked(id)?.ok_or_else(|| {
            RepositoryError::SnapshotNotFound {
                lookup: id.to_string(),
            }
        })?;
        let now = now_unix_ms();
        let SnapshotSource::Template { build } = &mut record.source else {
            return Err(RepositoryError::InvalidRequest {
                reason: format!("snapshot '{id}' is not a template build"),
            });
        };
        build.status = TemplateBuildStatus::Error;
        build.finished_at_unix_ms = Some(now);
        build.error_reason = Some(reason);
        record.updated_at_unix_ms = now;
        self.write_record_unlocked(&record)
    }

    fn read_json<T>(&self, path: &Path) -> RepositoryResult<T>
    where
        T: DeserializeOwned,
    {
        self.read_json_if_exists(path)?.ok_or_else(|| {
            RepositoryError::backend(
                format!("read '{}'", path.display()),
                std::io::Error::from(std::io::ErrorKind::NotFound),
            )
        })
    }

    /// Reads one catalog row, treating a missing file as an answer rather than
    /// a failure.
    ///
    /// Reading and handling `ENOENT` replaces an `exists()`-then-read pair: it
    /// is one syscall instead of two, it has no window between the two, and it
    /// gives the miss a metric of its own. Callers that previously returned
    /// `Ok(None)` from an `exists()` guard recorded nothing at all, which made
    /// the POSIX and OSS backends disagree about what a lookup costs.
    fn read_json_if_exists<T>(&self, path: &Path) -> RepositoryResult<Option<T>>
    where
        T: DeserializeOwned,
    {
        // Every path this store reads is a catalog row: a snapshot record or
        // an alias binding. Snapshot bytes live in `artifacts.rs`.
        let read = fs::read(path);
        record_object_store_request(
            ObjectStoreOp::Get,
            ObjectStoreSurface::Catalog,
            match &read {
                Ok(_) => ObjectStoreOutcome::Ok,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    ObjectStoreOutcome::NotFound
                }
                Err(_) => ObjectStoreOutcome::Error,
            },
        );
        let bytes = match read {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(RepositoryError::backend(
                    format!("read '{}'", path.display()),
                    error,
                ))
            }
        };
        serde_json::from_slice(&bytes).map(Some).map_err(|error| {
            RepositoryError::backend(format!("parse json '{}'", path.display()), error)
        })
    }

    fn write_json<T>(&self, path: &Path, value: &T) -> RepositoryResult<()>
    where
        T: Serialize,
    {
        // As with `read_json`, every write this store makes is a catalog row.
        let result = self.write_json_inner(path, value);
        record_object_store_request(
            ObjectStoreOp::Put,
            ObjectStoreSurface::Catalog,
            ObjectStoreOutcome::from_success(result.is_ok()),
        );
        result
    }

    fn write_json_inner<T>(&self, path: &Path, value: &T) -> RepositoryResult<()>
    where
        T: Serialize,
    {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                RepositoryError::backend(format!("create '{}'", parent.display()), error)
            })?;
        }
        let bytes = serde_json::to_vec_pretty(value).map_err(|error| {
            RepositoryError::backend(format!("serialize json '{}'", path.display()), error)
        })?;
        let parent = path.parent().ok_or_else(|| RepositoryError::Backend {
            message: format!("resolve parent for '{}'", path.display()),
            source: None,
        })?;
        let mut temp = tempfile::NamedTempFile::new_in(parent).map_err(|error| {
            RepositoryError::backend(format!("create temp file in '{}'", parent.display()), error)
        })?;
        temp.write_all(&bytes).map_err(|error| {
            RepositoryError::backend(
                format!("write temp json '{}'", temp.path().display()),
                error,
            )
        })?;
        temp.as_file().sync_all().map_err(|error| {
            RepositoryError::backend(format!("sync temp json '{}'", temp.path().display()), error)
        })?;
        let tmp_path = temp.path().to_path_buf();
        temp.persist(path).map_err(|error| {
            RepositoryError::backend(
                format!(
                    "persist json '{}' -> '{}'",
                    tmp_path.display(),
                    path.display()
                ),
                error.error,
            )
        })?;
        Ok(())
    }

    fn write_commit_marker(&self, id: &SnapshotId) -> RepositoryResult<()> {
        let path = self.commit_marker_path(id);
        let parent = path.parent().ok_or_else(|| RepositoryError::Backend {
            message: format!("resolve parent for '{}'", path.display()),
            source: None,
        })?;
        fs::create_dir_all(parent).map_err(|error| {
            RepositoryError::backend(format!("create '{}'", parent.display()), error)
        })?;
        let mut temp = tempfile::NamedTempFile::new_in(parent).map_err(|error| {
            RepositoryError::backend(
                format!("create temp commit marker in '{}'", path.display()),
                error,
            )
        })?;
        temp.write_all(b"committed").map_err(|error| {
            RepositoryError::backend(
                format!("write commit marker '{}'", temp.path().display()),
                error,
            )
        })?;
        temp.persist(&path).map_err(|error| {
            RepositoryError::backend(
                format!("persist commit marker '{}'", path.display()),
                error.error,
            )
        })?;
        Ok(())
    }

    fn remove_file_if_exists(&self, path: &Path) -> RepositoryResult<()> {
        match fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(RepositoryError::backend(
                format!("remove '{}'", path.display()),
                error,
            )),
        }
    }

    /// Whether a completed publish still owns this snapshot's artifacts.
    ///
    /// Both halves matter: the marker alone can survive a crash between writing
    /// it and writing the row, and a committed row alone can predate a
    /// directory that was since removed. Read errors resolve to "not
    /// committed", which is what the pre-split rollback did.
    fn is_committed(&self, id: &SnapshotId) -> bool {
        self.commit_marker_path(id).exists()
            && self
                .load_record_by_id_unlocked(id)
                .ok()
                .flatten()
                .is_some_and(|record| record.committed.is_some())
    }

    fn load_record_by_id_unlocked(
        &self,
        id: &SnapshotId,
    ) -> RepositoryResult<Option<SnapshotRecord>> {
        self.read_json_if_exists(&self.record_path(id))
    }

    fn load_alias_target(&self, alias: &SnapshotAlias) -> RepositoryResult<Option<SnapshotId>> {
        self.read_json_if_exists(&PosixFsSnapshotArtifactLayout::alias_path(
            &self.root, alias,
        ))
    }

    fn acquire_file_lock(
        &self,
        lock_path: PathBuf,
        contents: String,
        stale_age: Duration,
        label: &'static str,
        on_locked: impl Fn() -> RepositoryResult<PosixFileLockGuard>,
    ) -> RepositoryResult<PosixFileLockGuard> {
        if let Some(parent) = lock_path.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                RepositoryError::backend(
                    format!("create {label} lock dir '{}'", parent.display()),
                    error,
                )
            })?;
        }

        let deadline = FILE_LOCK_TIMEOUT.map(|timeout| Instant::now() + timeout);
        loop {
            match fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&lock_path)
            {
                Ok(mut file) => {
                    let guard = PosixFileLockGuard {
                        path: lock_path.clone(),
                    };
                    file.write_all(contents.as_bytes()).map_err(|error| {
                        RepositoryError::backend(
                            format!("write {label} lock '{}'", lock_path.display()),
                            error,
                        )
                    })?;
                    return Ok(guard);
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    let stale = fs::metadata(&lock_path)
                        .ok()
                        .and_then(|meta| meta.modified().ok())
                        .and_then(|modified| modified.elapsed().ok())
                        .map(|age| age > stale_age)
                        .unwrap_or(false);
                    if stale {
                        let _ = fs::remove_file(&lock_path);
                        continue;
                    }
                    if let Some(deadline) = deadline {
                        if Instant::now() < deadline {
                            thread::sleep(Duration::from_millis(25));
                            continue;
                        }
                    }
                    return on_locked();
                }
                Err(error) => {
                    return Err(RepositoryError::backend(
                        format!("create {label} lock '{}'", lock_path.display()),
                        error,
                    ));
                }
            }
        }
    }

    fn acquire_alias_lock(&self, alias: &SnapshotAlias) -> RepositoryResult<PosixFileLockGuard> {
        let lock_path = PosixFsSnapshotArtifactLayout::alias_lock_path(&self.root, alias);
        self.acquire_file_lock(
            lock_path.clone(),
            std::process::id().to_string(),
            ALIAS_LOCK_STALE_AGE,
            "alias",
            || {
                Err(RepositoryError::Backend {
                    message: format!("timed out waiting for alias lock '{}'", lock_path.display()),
                    source: None,
                })
            },
        )
    }

    fn acquire_record_lock(&self, id: &SnapshotId) -> RepositoryResult<PosixFileLockGuard> {
        let lock_path = PosixFsSnapshotArtifactLayout::record_lock_path(&self.root, id);
        self.acquire_file_lock(
            lock_path.clone(),
            std::process::id().to_string(),
            RECORD_LOCK_STALE_AGE,
            "record",
            || {
                Err(RepositoryError::Backend {
                    message: format!(
                        "timed out waiting for record lock '{}'",
                        lock_path.display()
                    ),
                    source: None,
                })
            },
        )
    }

    fn with_alias_lock<T>(
        &self,
        alias: &SnapshotAlias,
        action: impl FnOnce(&Self) -> RepositoryResult<T>,
    ) -> RepositoryResult<T> {
        let _guard = self.acquire_alias_lock(alias)?;
        action(self)
    }

    fn ensure_alias_available(
        &self,
        alias: &SnapshotAlias,
        new_id: &SnapshotId,
    ) -> RepositoryResult<()> {
        let alias_path = PosixFsSnapshotArtifactLayout::alias_path(&self.root, alias);
        if let Some(existing) = self.load_alias_target(alias)? {
            if &existing == new_id {
                return Ok(());
            }
            if self.load_record_by_id_unlocked(&existing)?.is_some() {
                return Err(RepositoryError::AliasConflict {
                    alias: alias.to_string(),
                    existing,
                    new_id: new_id.clone(),
                });
            }
            self.remove_file_if_exists(&alias_path)?;
        }
        Ok(())
    }

    fn write_record_unlocked(&self, record: &SnapshotRecord) -> RepositoryResult<()> {
        self.write_json(&self.record_path(&record.id), record)
    }

    fn committed_record_unlocked(
        &self,
        commit: &SnapshotCommit,
        now_unix_ms: i64,
    ) -> RepositoryResult<SnapshotRecord> {
        let id = commit.id.clone();
        let alias = commit.alias.clone();
        let resources = commit.resources;
        let source = commit.source.clone();
        let committed = commit.committed.clone();
        if let Some(mut record) = self.load_record_by_id_unlocked(&id)? {
            record.mark_committed(alias, resources, committed, source, now_unix_ms);
            return Ok(record);
        }

        let source = match source {
            SnapshotPublishSource::Template => SnapshotSource::Template {
                build: TemplateBuildInfo {
                    status: TemplateBuildStatus::Ready,
                    started_at_unix_ms: None,
                    finished_at_unix_ms: Some(now_unix_ms),
                    error_reason: None,
                },
            },
            SnapshotPublishSource::Sandbox { source_sandbox_id } => {
                SnapshotSource::Sandbox { source_sandbox_id }
            }
        };

        Ok(SnapshotRecord {
            id,
            alias,
            source,
            resources,
            created_at_unix_ms: now_unix_ms,
            updated_at_unix_ms: now_unix_ms,
            committed: Some(committed),
        })
    }

    fn write_committed_record_unlocked(&self, record: &SnapshotRecord) -> RepositoryResult<()> {
        self.write_record_unlocked(record)
    }

    fn matches_record_filter(record: &SnapshotRecord, filter: &SnapshotListFilter) -> bool {
        if let Some(alias_prefix) = filter.alias_prefix.as_deref() {
            match record.alias.as_ref() {
                Some(alias) if alias.to_string().starts_with(alias_prefix) => {}
                _ => return false,
            }
        }

        if let Some(ids) = filter.snapshot_ids.as_ref() {
            if !ids.iter().any(|id| id == &record.id) {
                return false;
            }
        }

        if let Some(id_or_alias) = filter.snapshot_id_or_alias.as_deref() {
            if record.id.to_string() != id_or_alias
                && record
                    .alias
                    .as_ref()
                    .is_none_or(|alias| alias.as_ref() != id_or_alias)
            {
                return false;
            }
        }

        if let Some(source_sandbox_id) = filter.source_sandbox_id.as_deref() {
            match &record.source {
                SnapshotSource::Sandbox {
                    source_sandbox_id: record_source_sandbox_id,
                } if record_source_sandbox_id == source_sandbox_id => {}
                _ => return false,
            }
        }

        if let Some(sources) = filter.sources.as_ref() {
            let source = match &record.source {
                SnapshotSource::Template { .. } => SnapshotSourceKind::Template,
                SnapshotSource::Sandbox { .. } => SnapshotSourceKind::Sandbox,
            };
            if !sources.contains(&source) {
                return false;
            }
        }

        if let Some(statuses) = filter.template_statuses.as_ref() {
            let SnapshotSource::Template { build } = &record.source else {
                return false;
            };
            if !statuses.contains(&build.status) {
                return false;
            };
        }

        true
    }
}

fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

/// Every method here is one `spawn_blocking` around the synchronous body above.
/// The POSIX catalog is file I/O and file locks; keeping the blocking work off
/// the reactor is the whole of what this layer does.
#[async_trait]
impl SnapshotCatalog for PosixFsCatalogStore {
    async fn create(&self, record: SnapshotRecord) -> RepositoryResult<SnapshotRecord> {
        let store = self.clone();
        run_catalog_blocking("create snapshot record", move || store.create_sync(record)).await
    }

    async fn publish_commit(&self, commit: SnapshotCommit) -> RepositoryResult<SnapshotRecord> {
        let store = self.clone();
        run_catalog_blocking("commit snapshot record", move || {
            store.publish_commit_sync(commit)
        })
        .await
    }

    async fn get(&self, id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>> {
        let store = self.clone();
        let id_or_alias = id_or_alias.to_string();
        run_catalog_blocking("load snapshot", move || store.get_sync(&id_or_alias)).await
    }

    async fn list(&self, filter: SnapshotListFilter) -> RepositoryResult<Vec<SnapshotRecord>> {
        let store = self.clone();
        run_catalog_blocking("list snapshots", move || store.list_sync(filter)).await
    }

    async fn delete_record(&self, record: &SnapshotRecord) -> RepositoryResult<()> {
        let store = self.clone();
        let record = record.clone();
        run_catalog_blocking("delete snapshot record", move || {
            store.delete_record_sync(&record)
        })
        .await
    }

    async fn resolve_alias(&self, alias: &str) -> RepositoryResult<Option<SnapshotId>> {
        let store = self.clone();
        let alias = alias.to_string();
        run_catalog_blocking("resolve snapshot alias", move || {
            store.resolve_alias_sync(&alias)
        })
        .await
    }

    async fn try_start_build(&self, id: &SnapshotId) -> RepositoryResult<SnapshotRecord> {
        let store = self.clone();
        let id = id.clone();
        run_catalog_blocking("start template build", move || store.try_start_sync(&id)).await
    }

    async fn mark_build_error(
        &self,
        id: &SnapshotId,
        reason: TemplateBuildErrorReason,
    ) -> RepositoryResult<()> {
        let store = self.clone();
        let id = id.clone();
        run_catalog_blocking("mark template build error", move || {
            store.mark_error_sync(&id, reason)
        })
        .await
    }

    /// Unlike the OSS backend, this one refuses to delete artifacts a completed
    /// publish still owns, so a failed re-publish of an existing id cannot take
    /// the live snapshot's bytes with it.
    async fn retains_artifacts_on_publish_failure(
        &self,
        id: &SnapshotId,
    ) -> RepositoryResult<bool> {
        let store = self.clone();
        let id = id.clone();
        run_catalog_blocking("check snapshot commit state", move || {
            Ok(store.is_committed(&id))
        })
        .await
    }
}

async fn run_catalog_blocking<T, F>(operation: &'static str, work: F) -> RepositoryResult<T>
where
    T: Send + 'static,
    F: FnOnce() -> RepositoryResult<T> + Send + 'static,
{
    task::spawn_blocking(work)
        .await
        .map_err(|error| RepositoryError::Backend {
            message: format!("catalog blocking task panicked while trying to {operation}"),
            source: Some(anyhow::Error::from(error)),
        })?
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use metrics_util::debugging::DebuggingRecorder;
    use tempfile::TempDir;

    use super::super::layout::PosixFsSnapshotArtifactLayout;
    use super::PosixFsCatalogStore;
    use crate::snapshot::repository::metrics::test_support::object_store_requests;
    use crate::snapshot::repository::SnapshotCommit;
    use crate::snapshot::{
        CommittedSnapshot, SnapshotAlias, SnapshotId, SnapshotListFilter, SnapshotPublishSource,
        SnapshotRecord, SnapshotSourceKind, TemplateBuildStatus,
    };
    use crate::types::SandboxResources;

    /// A catalog read must be *visible* as object-store traffic: one LIST for
    /// the records directory plus one GET per record. The whole point of the
    /// counter is that this number can later be shown to drop to zero, and a
    /// probe that reads zero both before and after proves nothing — so assert
    /// the exact `1 + N` here, while it is still non-zero.
    #[test]
    fn catalog_list_counts_one_list_plus_one_get_per_record() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let store = PosixFsCatalogStore::new(tempdir.path().to_path_buf());
        // Seed outside the recorder so only the read under test is counted.
        let record_count = 3_u64;
        for index in 0..record_count {
            store
                .create_sync(SnapshotRecord::template_waiting(
                    SnapshotId::generate(),
                    Some(SnapshotAlias::parse(&format!("template-{index}")).expect("alias parses")),
                    SandboxResources::default(),
                ))
                .expect("create should work");
        }

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        // Deliberately the synchronous body rather than the `SnapshotCatalog`
        // method: `with_local_recorder` installs a *thread-local* recorder, and
        // the trait method runs the same work on a `spawn_blocking` thread that
        // would not see it. In production the recorder is global, so the two
        // paths record identically.
        let listed = metrics::with_local_recorder(&recorder, || {
            store.list_sync(SnapshotListFilter::matches_all())
        })
        .expect("list should work");
        assert_eq!(listed.len(), record_count as usize);

        let counters = object_store_requests(&snapshotter);
        assert_eq!(
            counters,
            BTreeMap::from([
                ("list/catalog/ok".to_owned(), 1),
                ("get/catalog/ok".to_owned(), record_count),
            ]),
            "listing {record_count} records should cost exactly 1 + {record_count} catalog \
             requests and touch no byte traffic"
        );
    }

    #[test]
    fn commit_makes_snapshot_visible_and_seals_its_directory() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let store = PosixFsCatalogStore::new(tempdir.path().to_path_buf());
        let snapshot_id = SnapshotId::generate();

        store
            .publish_commit_sync(SnapshotCommit {
                id: snapshot_id.clone(),
                alias: None,
                source: SnapshotPublishSource::Template,
                resources: SandboxResources::default(),
                committed: CommittedSnapshot::mock(),
            })
            .expect("commit should work");

        assert!(store
            .get_sync(&snapshot_id.to_string())
            .expect("get should work")
            .expect("snapshot should exist")
            .committed
            .is_some());
        assert!(
            PosixFsSnapshotArtifactLayout::new(tempdir.path(), &snapshot_id)
                .path(super::super::layout::POSIXFS_SNAPSHOT_COMMIT_MARKER)
                .exists()
        );
    }

    fn committed_metadata(
        id: SnapshotId,
        alias: &str,
        source: SnapshotPublishSource,
    ) -> SnapshotCommit {
        SnapshotCommit {
            id,
            alias: Some(SnapshotAlias::parse(alias).expect("alias should parse")),
            source,
            resources: SandboxResources::default(),
            committed: CommittedSnapshot::mock(),
        }
    }

    fn commit_record(store: &PosixFsCatalogStore, commit: SnapshotCommit) -> SnapshotId {
        let snapshot_id = commit.id.clone();
        store
            .publish_commit_sync(commit)
            .expect("commit should work");
        snapshot_id
    }

    fn listed_ids(store: &PosixFsCatalogStore, filter: SnapshotListFilter) -> Vec<SnapshotId> {
        store
            .list_sync(filter)
            .expect("list should work")
            .into_iter()
            .map(|record| record.id)
            .collect()
    }

    #[test]
    fn list_applies_record_filters() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let store = PosixFsCatalogStore::new(tempdir.path().to_path_buf());
        let template_alpha = commit_record(
            &store,
            committed_metadata(
                SnapshotId::generate(),
                "template-alpha",
                SnapshotPublishSource::Template,
            ),
        );
        let template_beta = commit_record(
            &store,
            committed_metadata(
                SnapshotId::generate(),
                "template-beta",
                SnapshotPublishSource::Template,
            ),
        );
        let sandbox_one = commit_record(
            &store,
            committed_metadata(
                SnapshotId::generate(),
                "sandbox-one",
                SnapshotPublishSource::Sandbox {
                    source_sandbox_id: "sandbox-1".to_string(),
                },
            ),
        );
        let sandbox_two = commit_record(
            &store,
            committed_metadata(
                SnapshotId::generate(),
                "sandbox-two",
                SnapshotPublishSource::Sandbox {
                    source_sandbox_id: "sandbox-2".to_string(),
                },
            ),
        );
        let errored_template = SnapshotId::generate();
        store
            .create_sync(SnapshotRecord::template_waiting(
                errored_template.clone(),
                Some(SnapshotAlias::parse("template-error").expect("alias should parse")),
                Default::default(),
            ))
            .expect("create template should work");
        store
            .mark_error_sync(
                &errored_template,
                crate::snapshot::TemplateBuildErrorReason::new("boom"),
            )
            .expect("mark error should work");

        let ids = listed_ids(
            &store,
            SnapshotListFilter::by_ids([template_alpha.clone(), sandbox_one.clone()]),
        );
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&template_alpha));
        assert!(ids.contains(&sandbox_one));

        let ids = listed_ids(
            &store,
            SnapshotListFilter {
                alias_prefix: Some("template-".to_string()),
                ..SnapshotListFilter::default()
            },
        );
        assert_eq!(ids.len(), 3);
        assert!(ids.contains(&template_alpha));
        assert!(ids.contains(&template_beta));
        assert!(ids.contains(&errored_template));

        let ids = listed_ids(&store, SnapshotListFilter::templates());
        assert_eq!(ids.len(), 3);
        assert!(ids.contains(&template_alpha));
        assert!(ids.contains(&template_beta));
        assert!(ids.contains(&errored_template));
        assert!(!ids.contains(&sandbox_one));

        let ids = listed_ids(
            &store,
            SnapshotListFilter::sandbox_snapshots(Some("sandbox-1".to_string()), None),
        );
        assert_eq!(ids, vec![sandbox_one.clone()]);

        let ids = listed_ids(
            &store,
            SnapshotListFilter::sandbox_snapshots(None, Some("team/sandbox-one:v1".to_string())),
        );
        assert_eq!(ids, vec![sandbox_one.clone()]);

        let ids = listed_ids(
            &store,
            SnapshotListFilter::sandbox_snapshots(None, Some(format!("{}:v1", sandbox_one))),
        );
        assert_eq!(ids, vec![sandbox_one.clone()]);

        let ids = listed_ids(
            &store,
            SnapshotListFilter::sandbox_snapshots(
                Some("sandbox-2".to_string()),
                Some("sandbox-one".to_string()),
            ),
        );
        assert!(ids.is_empty());

        let ids = listed_ids(
            &store,
            SnapshotListFilter {
                template_statuses: Some(vec![TemplateBuildStatus::Error]),
                ..SnapshotListFilter::templates()
            },
        );
        assert_eq!(ids, vec![errored_template]);

        let ids = listed_ids(
            &store,
            SnapshotListFilter {
                alias_prefix: Some("sandbox-".to_string()),
                sources: Some(vec![SnapshotSourceKind::Sandbox]),
                snapshot_ids: Some(vec![sandbox_two.clone(), template_alpha]),
                ..SnapshotListFilter::default()
            },
        );
        assert_eq!(ids, vec![sandbox_two]);
    }

    #[test]
    fn get_rejects_path_traversal_as_alias() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let store = PosixFsCatalogStore::new(tempdir.path().to_path_buf());
        // "../../etc/passwd" is not a valid alias (nor a UUID), so alias parsing
        // validation rejects it as InvalidRequest.
        let err = store
            .get_sync("../../etc/passwd")
            .expect_err("path traversal should be rejected");
        assert!(
            matches!(err, crate::snapshot::RepositoryError::InvalidRequest { .. }),
            "expected InvalidRequest, got: {err:?}"
        );
    }

    #[test]
    fn get_returns_none_for_unknown_valid_uuid() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let store = PosixFsCatalogStore::new(tempdir.path().to_path_buf());
        let unknown = SnapshotId::generate();
        let result = store
            .get_sync(&unknown.to_string())
            .expect("valid UUID lookup should not error");
        assert!(result.is_none(), "non-existent snapshot should return None");
    }
}
