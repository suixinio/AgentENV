use std::cmp::Ordering;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use super::errors::RepositoryResult;
use crate::runtime_snapshot::RunnableSnapshot;
use crate::snapshot::types::{
    CommittedAttachedDrive, CommittedSnapshot, ManagedLayer, OverlaybdLayerRef,
    PersistedDiskImagePublication, SnapshotAlias, SnapshotId, SnapshotPublishMetadata,
    SnapshotPublishSource, SnapshotRecord, SnapshotSourceKind, TemplateBuildErrorReason,
    TemplateBuildStatus,
};
use crate::types::FirecrackerSnapshotManifest;
use crate::types::SandboxResources;

/// Default page size when no limit is specified.
pub const DEFAULT_LIST_PAGE_LIMIT: u32 = 100;

/// The most rows one page may hold, whatever the caller asked for.
///
/// Matches the central catalog's `maxListLimit`. A request above it is clamped
/// rather than refused: the cursor still walks the rest.
pub const MAX_LIST_PAGE_LIMIT: u32 = 1000;

/// Catalog-level listing position decoded from the public pagination token.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotCursor {
    pub created_at_unix_ms: i64,
    pub snapshot_id: SnapshotId,
}

impl SnapshotCursor {
    pub fn new(created_at_unix_ms: i64, snapshot_id: SnapshotId) -> Self {
        Self {
            created_at_unix_ms,
            snapshot_id,
        }
    }

    /// The cursor that resumes a listing immediately after `record`.
    pub fn of(record: &SnapshotRecord) -> Self {
        Self::new(record.created_at_unix_ms, record.id.clone())
    }

    /// Orders records newest first, breaking timestamp ties by ascending id.
    pub fn order(a: &SnapshotRecord, b: &SnapshotRecord) -> Ordering {
        b.created_at_unix_ms
            .cmp(&a.created_at_unix_ms)
            .then_with(|| a.id.cmp(&b.id))
    }

    /// Returns whether `record` belongs after this cursor.
    ///
    /// Its UUID ordering must match the catalog's textual UUID ordering.
    pub fn is_before(&self, record: &SnapshotRecord) -> bool {
        match record.created_at_unix_ms.cmp(&self.created_at_unix_ms) {
            Ordering::Less => true,
            Ordering::Greater => false,
            Ordering::Equal => record.id > self.snapshot_id,
        }
    }
}

/// One page of a listing, and where the next one starts.
#[derive(Clone, Debug)]
pub struct SnapshotListPage {
    pub items: Vec<SnapshotRecord>,
    /// Absent on the last page.
    pub next: Option<SnapshotCursor>,
}

impl SnapshotListPage {
    pub fn single(items: Vec<SnapshotRecord>) -> Self {
        Self { items, next: None }
    }
}

/// Snapshot record list filter.
///
/// When multiple fields are present they combine with AND semantics.
/// When all fields are `None`, the filter matches all snapshot records,
/// including pending template builds and committed snapshots.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SnapshotListFilter {
    /// Match aliases that start with this prefix.
    pub alias_prefix: Option<String>,
    /// Restrict results to this exact set of snapshot ids.
    pub snapshot_ids: Option<Vec<SnapshotId>>,
    /// Restrict results to a single snapshot id or exact alias.
    pub snapshot_id_or_alias: Option<String>,
    /// Restrict results to snapshots captured from this source sandbox id.
    ///
    /// This only matches records whose source is [`SnapshotSourceKind::Sandbox`].
    pub source_sandbox_id: Option<String>,
    /// Restrict results to records with these source kinds.
    pub sources: Option<Vec<SnapshotSourceKind>>,
    /// Restrict results to template records whose build status is in this set.
    ///
    /// Sandbox records never match this field.
    pub template_statuses: Option<Vec<TemplateBuildStatus>>,
    /// Page size; `None` uses [`DEFAULT_LIST_PAGE_LIMIT`].
    pub limit: Option<u32>,
    /// Page start; `None` starts at the newest row.
    pub cursor: Option<SnapshotCursor>,
}

impl SnapshotListFilter {
    pub fn matches_all() -> Self {
        Self::default()
    }

    /// Adds the page bounds to a filter that describes what to match.
    pub fn paginated(mut self, limit: Option<u32>, cursor: Option<SnapshotCursor>) -> Self {
        self.limit = limit;
        self.cursor = cursor;
        self
    }

    /// The page size this filter asks for, clamped to what a page may hold.
    pub fn effective_limit(&self) -> u32 {
        self.limit
            .unwrap_or(DEFAULT_LIST_PAGE_LIMIT)
            .min(MAX_LIST_PAGE_LIMIT)
    }

    pub fn templates() -> Self {
        Self {
            sources: Some(vec![SnapshotSourceKind::Template]),
            ..Self::default()
        }
    }

    pub fn sandbox_snapshots(
        source_sandbox_id: Option<String>,
        snapshot_id_or_alias: Option<String>,
    ) -> Self {
        Self {
            sources: Some(vec![SnapshotSourceKind::Sandbox]),
            source_sandbox_id,
            snapshot_id_or_alias: snapshot_id_or_alias.map(|value| {
                let unqualified = value
                    .rsplit_once('/')
                    .map_or(value.as_str(), |(_, name)| name);
                unqualified
                    .split_once(':')
                    .map_or(unqualified, |(name, _)| name)
                    .to_string()
            }),
            ..Self::default()
        }
    }
}

/// Durable logical references produced by importing one snapshot's bytes.
#[derive(Clone, Debug, Default)]
pub struct ImportedSnapshotArtifacts {
    pub rootfs_layers: Vec<OverlaybdLayerRef>,
    /// Managed overlaybd layers for the memory snapshot image, ordered bottom-up.
    pub memory_layers: Vec<ManagedLayer>,
    pub attached_drives: Vec<CommittedAttachedDrive>,
    /// External registry publications produced by the source-registry policy.
    /// Empty for backends that keep every layer in their own store.
    pub disk_publications: Vec<PersistedDiskImagePublication>,
}

/// Serializable catalog commit for artifacts that are already durable.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotCommit {
    pub id: SnapshotId,
    /// At most one alias per snapshot, bound as part of the commit.
    pub alias: Option<SnapshotAlias>,
    pub source: SnapshotPublishSource,
    pub resources: SandboxResources,
    /// Source-provided creation time shared by every catalog; `None` lets the
    /// receiving store choose for compatibility with older commits.
    #[serde(default)]
    pub created_at_unix_ms: Option<i64>,
    pub committed: CommittedSnapshot,
    /// The node whose disk staged the bytes; the row records it as the
    /// preferred place to resume a paused sandbox.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin_node_id: Option<String>,
}

impl SnapshotCommit {
    /// Joins publish metadata with imported artifact references without I/O.
    pub fn new(
        metadata: &SnapshotPublishMetadata,
        imported: ImportedSnapshotArtifacts,
        created_at_unix_ms: i64,
        origin_node_id: Option<String>,
    ) -> Self {
        Self {
            id: metadata.id.clone(),
            alias: metadata.alias.clone(),
            source: metadata.source.clone(),
            resources: metadata.resources,
            created_at_unix_ms: Some(created_at_unix_ms),
            origin_node_id,
            committed: CommittedSnapshot {
                context: metadata.context.clone(),
                startup: metadata.startup.clone(),
                runtime_versions: metadata.runtime_versions.clone(),
                virtualization_mode: metadata.virtualization_mode,
                image_configs: metadata.image_configs.clone(),
                custom_extension_params: metadata.custom_extension_params.clone(),
                paused_sandbox: metadata.paused_sandbox.clone(),
                rootfs_layers: imported.rootfs_layers,
                attached_drives: imported.attached_drives,
                memory_layers: imported.memory_layers,
                disk_publications: imported.disk_publications,
            },
        }
    }
}

/// Serializable handoff between staging bytes and committing their catalog row.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StagedSnapshot {
    /// Identity plus the payload the row will carry.
    pub commit: SnapshotCommit,
    /// When the bytes finished landing, by the staging node's clock.
    pub staged_at_unix_ms: i64,
    /// Machine whose disk holds the staged bytes.
    pub origin_node_id: String,
}

impl StagedSnapshot {
    pub fn id(&self) -> &SnapshotId {
        &self.commit.id
    }

    pub fn alias(&self) -> Option<&SnapshotAlias> {
        self.commit.alias.as_ref()
    }
}

/// A template row admitted under a distinct build identity.
#[derive(Clone, Debug)]
pub struct StartedBuild {
    /// The template row as the transition left it: building.
    pub record: SnapshotRecord,
    /// The admitted build, to be quoted when renewing its lease.
    ///
    /// Equal to the template id for a backend with no build rows, which has
    /// nothing to renew either.
    pub build_id: SnapshotId,
}

impl StartedBuild {
    /// The answer from a backend that has no separate notion of a build.
    pub fn untracked(record: SnapshotRecord) -> Self {
        Self {
            build_id: record.id.clone(),
            record,
        }
    }
}

/// Returns whether a template status may transition to a new build.
pub fn build_may_start_from(status: TemplateBuildStatus) -> bool {
    matches!(
        status,
        TemplateBuildStatus::Waiting | TemplateBuildStatus::Error
    )
}

/// Controls whether catalog reads include non-resolvable lifecycle states.
///
/// Callers launching a VM must use `Resolvable`; template-management surfaces
/// use `AnyStatus`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CatalogReadScope {
    /// Only rows a caller may launch from: `status_group = 'ready'`.
    Resolvable,
    /// Every row, including `waiting`, `building` and `error`.
    ///
    /// The template surface is what this exists for. Nothing that resolves a
    /// snapshot in order to run it may ask for it.
    AnyStatus,
}

impl CatalogReadScope {
    pub fn allow_any_status(self) -> bool {
        matches!(self, Self::AnyStatus)
    }
}

#[cfg(test)]
mod catalog_read_scope_tests {
    use super::*;

    #[test]
    fn the_resolvable_scope_never_asks_for_any_status() {
        assert!(!CatalogReadScope::Resolvable.allow_any_status());
        assert!(CatalogReadScope::AnyStatus.allow_any_status());
    }
}

/// Whether catalog absence is settled enough for destructive action.
///
/// Pending writes or another store can make a missing row unsettled.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SnapshotAbsence {
    /// Nothing this node can consult holds the snapshot, and nothing owes a
    /// write that would produce it. Absence is a fact.
    Settled,
    /// Absence is not the last word, and `because` says what contradicted it.
    Unsettled { because: String },
}

impl SnapshotAbsence {
    /// Absence contradicted, for a reason worth logging.
    pub fn unsettled(because: impl Into<String>) -> Self {
        Self::Unsettled {
            because: because.into(),
        }
    }
}

#[async_trait]
/// The rows: snapshot records, template build state, and alias bindings.
///
/// A record is the catalog identity and lifecycle state for a snapshot:
///
/// - template records may exist before build artifacts are committed
/// - sandbox records are created by publishing an already captured runtime snapshot
/// - committed records carry a [`CommittedSnapshot`] payload of artifact references
///
/// Nothing in this trait reads or writes a snapshot byte. That separation is
/// load-bearing rather than cosmetic: an implementation of this trait can live
/// behind an RPC, in a database, or in front of both, without the byte path
/// knowing. The counterpart is [`SnapshotArtifactStore`], and
/// [`SnapshotRepository`] is the only thing that sequences the two.
///
/// Backend guidance:
///
/// - alias claim / release should be concurrency-safe
/// - a commit should only make an alias visible once the record it points at is
///   durable enough for subsequent readers to resolve
/// - delete should avoid exposing partially removed records
pub trait SnapshotCatalog: Send + Sync {
    /// Creates a durable template snapshot record before build artifacts exist.
    ///
    /// Backends should reject records that already contain a committed artifact
    /// payload and should only accept records whose source kind is template.
    async fn create(&self, record: SnapshotRecord) -> RepositoryResult<SnapshotRecord>;

    /// Commits one snapshot's row, given a payload whose artifacts are already
    /// durable.
    ///
    /// This is the flip: before it the snapshot is bytes nobody can find, after
    /// it the snapshot is resolvable. It must bind the alias and mark the
    /// record committed, and it must not assume the artifacts are reachable
    /// from this process — [`SnapshotCommit`] is the only description of them
    /// it gets.
    async fn publish_commit(&self, commit: SnapshotCommit) -> RepositoryResult<SnapshotRecord>;

    /// Loads one snapshot record by repository id or alias.
    ///
    /// Resolvable rows only. See [`Self::get_scoped`] for the surface that
    /// needs to see a template before it has ever been built.
    async fn get(&self, id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>>;

    /// Reads at an explicit scope; backends without status filtering may use
    /// the default.
    async fn get_scoped(
        &self,
        id_or_alias: &str,
        _scope: CatalogReadScope,
    ) -> RepositoryResult<Option<SnapshotRecord>> {
        self.get(id_or_alias).await
    }

    /// Lists one bounded page, newest first.
    ///
    /// Implementations honor the filter's limit and cursor and set `next` to
    /// the last row returned.
    async fn list_page(&self, filter: SnapshotListFilter) -> RepositoryResult<SnapshotListPage>;

    /// Lists at an explicit scope; defaults to the backend's ordinary listing.
    async fn list_page_scoped(
        &self,
        filter: SnapshotListFilter,
        _scope: CatalogReadScope,
    ) -> RepositoryResult<SnapshotListPage> {
        self.list_page(filter).await
    }

    /// Deletes one snapshot's row and any alias binding that still points at
    /// it. Idempotent. Leaves the artifacts to [`SnapshotArtifactStore`].
    ///
    /// Takes the record rather than the id because unbinding the alias needs
    /// it, and the caller has just read it: asking for the id alone would make
    /// every backend re-read the row it was handed.
    async fn delete_record(&self, record: &SnapshotRecord) -> RepositoryResult<()>;

    /// Removes every pause row of one sandbox and returns the records removed,
    /// so their artifacts can follow. A checkpoint of the same sandbox is a
    /// template of it and is left where it is.
    ///
    /// One sandbox has at most one live pause row, but a backend that retired
    /// earlier ones still owns their bytes, so the answer includes those too.
    /// The default walks the listing, which is what a backend with no
    /// set-at-a-time delete can do.
    async fn delete_sandbox_pauses(
        &self,
        source_sandbox_id: &str,
    ) -> RepositoryResult<Vec<SnapshotRecord>> {
        let filter =
            SnapshotListFilter::sandbox_snapshots(Some(source_sandbox_id.to_string()), None);
        let mut removed = Vec::new();
        loop {
            let page = self
                .list_page_scoped(filter.clone(), CatalogReadScope::AnyStatus)
                .await?;
            let paused: Vec<SnapshotRecord> = page
                .items
                .into_iter()
                .filter(|record| record.paused_sandbox().is_some())
                .collect();
            if paused.is_empty() {
                return Ok(removed);
            }
            for record in paused {
                self.delete_record(&record).await?;
                removed.push(record);
            }
            // Deleting shifts the page window; start over from the newest.
        }
    }

    /// Resolves a human-readable alias to the current snapshot id.
    ///
    /// Resolvable rows only. See [`Self::resolve_alias_scoped`].
    async fn resolve_alias(&self, alias: &str) -> RepositoryResult<Option<SnapshotId>>;

    /// Records the node a resume of this snapshot's sandbox landed on, so the
    /// next resume prefers it. A preference only; backends without the column
    /// keep the default and lose nothing but warmth.
    async fn set_origin_node_id(
        &self,
        _id: &SnapshotId,
        _origin_node_id: &str,
    ) -> RepositoryResult<()> {
        Ok(())
    }

    /// [`Self::resolve_alias`] at an explicitly chosen scope.
    ///
    /// See [`Self::get_scoped`] on why the default ignores the scope.
    async fn resolve_alias_scoped(
        &self,
        alias: &str,
        _scope: CatalogReadScope,
    ) -> RepositoryResult<Option<SnapshotId>> {
        self.resolve_alias(alias).await
    }

    /// Atomically transitions one template build from waiting to building.
    ///
    /// Backends should reject non-template records and template records that are
    /// no longer waiting.
    async fn try_start_build(&self, id: &SnapshotId) -> RepositoryResult<StartedBuild>;

    /// Renews a build lease; `false` means the builder lost ownership and must stop.
    async fn renew_build_lease(&self, _build_id: &SnapshotId) -> RepositoryResult<bool> {
        Ok(true)
    }

    /// Marks one template build as failed.
    ///
    /// Backends should preserve the existing record identity, alias, resources,
    /// and source while recording the failure state and reason.
    async fn mark_build_error(
        &self,
        id: &SnapshotId,
        reason: TemplateBuildErrorReason,
    ) -> RepositoryResult<()>;

    /// Whether an earlier commit still owns this id's artifacts after a failed publish.
    async fn retains_artifacts_on_publish_failure(
        &self,
        _id: &SnapshotId,
    ) -> RepositoryResult<bool> {
        Ok(false)
    }

    /// Determines whether absence is settled enough for destructive action.
    ///
    /// Errors must propagate; they are never settled absence.
    async fn absence_of(&self, id: &SnapshotId) -> RepositoryResult<SnapshotAbsence> {
        Ok(
            match self
                .get_scoped(&id.to_string(), CatalogReadScope::AnyStatus)
                .await?
            {
                Some(_) => SnapshotAbsence::unsettled("the catalog holds a row for it"),
                None => SnapshotAbsence::Settled,
            },
        )
    }
}

#[async_trait]
/// The bytes: snapshot artifacts and managed layers.
///
/// Implementations move a captured or built snapshot's local files into durable
/// shared storage and report back what they stored, as
/// [`ImportedSnapshotArtifacts`]. They never touch a catalog row, and they are
/// never the thing that makes a snapshot visible — a snapshot whose bytes are
/// all present but whose row was never committed is unresolvable, by design.
///
/// Backend guidance:
///
/// - shared artifact imports should use atomic protocols so concurrent writers
///   never expose half-written managed layers
/// - content-addressed layers are shared between snapshots; they are not part
///   of any one snapshot's rollback and need separate GC
pub trait SnapshotArtifactStore: Send + Sync {
    /// Writes one snapshot's bytes into durable storage.
    ///
    /// `publications` accumulates external registry publications *as they are
    /// made*, rather than only on success, because rolling back a partial
    /// import needs the ones that already landed. It is the caller's, so the
    /// caller still holds them when this returns `Err`.
    async fn import_built_artifacts(
        &self,
        metadata: &SnapshotPublishMetadata,
        manifest: &FirecrackerSnapshotManifest,
        publications: &mut Vec<PersistedDiskImagePublication>,
    ) -> RepositoryResult<ImportedSnapshotArtifacts>;

    /// Removes everything stored for `id`, plus the listed external
    /// publications. Best effort: failures are logged, not returned, because
    /// every caller is already on an error path or has already removed the row.
    ///
    /// Content-addressed managed layers are deliberately out of scope — they
    /// are shared across snapshots.
    async fn delete_artifacts(
        &self,
        id: &SnapshotId,
        publications: &[PersistedDiskImagePublication],
    );
}

#[async_trait]
/// Resolves committed snapshot records into node-local runnable paths.
///
/// This trait sits at the boundary between repository truth and launch-time derived state.
/// Implementations consume a committed [`SnapshotRecord`] and may materialize node-local helper files
/// such as runnable overlaybd `image.json` configs for the current node.
///
/// Contract:
///
/// - consumes a [`SnapshotRecord`] whose committed payload is present
/// - returns paths that are directly usable by sandbox / firecracker launch code on the current node
/// - may materialize node-local derived files such as runnable `image.json`
/// - must not mutate committed repository truth when generating node-local cache files
///
/// Backend guidance:
///
/// - runtime cache directories should be treated as node-local derived state
/// - shared repository storage and node-local runtime cache should be separated whenever possible
/// - concurrent resolves on one node should prefer atomic cache writes so readers never observe
///   partially written derived configs
pub trait SnapshotRuntimeResolver: Send + Sync {
    /// Resolves one committed snapshot record into a runtime-ready view for the current node.
    async fn resolve(&self, snapshot: Arc<SnapshotRecord>) -> RepositoryResult<RunnableSnapshot>;
}

/// Pagination contract tests against the in-memory catalog.
#[cfg(test)]
mod pagination_tests {
    use super::*;
    use crate::snapshot::mock::InMemorySnapshotCatalog;
    use crate::snapshot::types::{SnapshotSource, TemplateBuildInfo, TemplateBuildStatus};
    use crate::types::SandboxResources;
    use std::collections::HashSet;
    use uuid::Uuid;

    fn catalog_of(rows: &[SnapshotRecord]) -> InMemorySnapshotCatalog {
        let catalog = InMemorySnapshotCatalog::default();
        for row in rows {
            catalog.seed(row.clone());
        }
        catalog
    }

    async fn page_of(
        catalog: &InMemorySnapshotCatalog,
        limit: u32,
        cursor: Option<&SnapshotCursor>,
    ) -> SnapshotListPage {
        catalog
            .list_page(SnapshotListFilter::matches_all().paginated(Some(limit), cursor.cloned()))
            .await
            .expect("the in-memory catalog answers every listing")
    }

    fn record(created_at_unix_ms: i64, id: SnapshotId) -> SnapshotRecord {
        SnapshotRecord {
            id,
            alias: None,
            source: SnapshotSource::Template {
                build: TemplateBuildInfo {
                    status: TemplateBuildStatus::Ready,
                    started_at_unix_ms: None,
                    finished_at_unix_ms: Some(created_at_unix_ms),
                    error_reason: None,
                },
            },
            resources: SandboxResources::default(),
            created_at_unix_ms,
            updated_at_unix_ms: created_at_unix_ms,
            committed: None,
            origin_node_id: None,
        }
    }

    fn sandbox_record(created_at_unix_ms: i64, id: SnapshotId) -> SnapshotRecord {
        SnapshotRecord {
            source: SnapshotSource::Sandbox {
                source_sandbox_id: "sbx-1".to_string(),
            },
            ..record(created_at_unix_ms, id)
        }
    }

    fn id(n: u8) -> SnapshotId {
        SnapshotId::from_uuid(Uuid::from_bytes([n; 16]))
    }

    #[test]
    fn a_snapshot_ids_ordering_is_its_text_ordering() {
        let mut ids: Vec<SnapshotId> = (0..64)
            .map(|_| SnapshotId::generate())
            .chain([id(0x00), id(0x0f), id(0x10), id(0xff)])
            .collect();
        ids.sort();

        let as_text: Vec<String> = ids.iter().map(ToString::to_string).collect();
        let mut sorted_text = as_text.clone();
        sorted_text.sort();

        assert_eq!(
            as_text, sorted_text,
            "the binary order and the text order must be the same order"
        );
    }

    #[test]
    fn the_listing_order_is_newest_first_then_ascending_id() {
        let mut rows = [
            record(100, id(9)),
            record(200, id(5)),
            record(100, id(1)),
            record(200, id(7)),
        ];
        rows.sort_by(SnapshotCursor::order);

        let order: Vec<(i64, SnapshotId)> = rows
            .iter()
            .map(|row| (row.created_at_unix_ms, row.id.clone()))
            .collect();
        assert_eq!(
            order,
            vec![(200, id(5)), (200, id(7)), (100, id(1)), (100, id(9)),]
        );
    }

    #[test]
    fn a_row_is_after_the_cursor_when_it_is_older_or_ties_with_a_larger_id() {
        let cursor = SnapshotCursor::new(200, id(5));

        assert!(
            cursor.is_before(&record(100, id(9))),
            "an older row is on a later page whatever its id"
        );
        assert!(
            cursor.is_before(&record(100, id(1))),
            "an older row is on a later page even with a smaller id"
        );
        assert!(
            !cursor.is_before(&record(300, id(1))),
            "a newer row was on an earlier page"
        );
        assert!(
            cursor.is_before(&record(200, id(7))),
            "a tie is broken by ascending id: a larger id comes later"
        );
        assert!(
            !cursor.is_before(&record(200, id(3))),
            "a tie with a smaller id was already returned"
        );
        assert!(
            !cursor.is_before(&record(200, id(5))),
            "the cursor's own row is not returned again"
        );
    }

    #[tokio::test]
    async fn paging_to_the_end_returns_every_row_exactly_once() {
        let rows: Vec<SnapshotRecord> = (0..40u8)
            .map(|n| record(1_000 - i64::from(n / 4), id(n)))
            .collect();
        let total = rows.len();
        let catalog = catalog_of(&rows);

        let mut seen: Vec<SnapshotId> = Vec::new();
        let mut cursor: Option<SnapshotCursor> = None;
        for _ in 0..total + 1 {
            let page = page_of(&catalog, 3, cursor.as_ref()).await;
            seen.extend(page.items.iter().map(|row| row.id.clone()));
            match page.next {
                None => break,
                Some(next) => cursor = Some(next),
            }
        }

        assert_eq!(seen.len(), total, "every row is returned");
        assert_eq!(
            seen.iter().collect::<HashSet<_>>().len(),
            total,
            "and none of them twice"
        );

        let mut expected = rows.clone();
        expected.sort_by(SnapshotCursor::order);
        assert_eq!(
            seen,
            expected
                .iter()
                .map(|row| row.id.clone())
                .collect::<Vec<_>>(),
            "and in the listing's order"
        );
    }

    #[tokio::test]
    async fn rows_written_during_a_walk_do_not_shift_the_pages() {
        let rows: Vec<SnapshotRecord> = (0..10u8)
            .map(|n| record(1_000 - i64::from(n), id(n)))
            .collect();
        let catalog = catalog_of(&rows);

        let first = page_of(&catalog, 4, None).await;
        let cursor = first.next.clone().expect("there is a second page");
        let first_ids: Vec<SnapshotId> = first.items.iter().map(|row| row.id.clone()).collect();

        catalog.seed(record(999, id(200)));
        catalog.seed(record(5_000, id(201)));

        let second = page_of(&catalog, 4, Some(&cursor)).await;
        let second_ids: Vec<SnapshotId> = second.items.iter().map(|row| row.id.clone()).collect();

        assert!(
            !second_ids.contains(&id(200)),
            "a row written into an already-walked range must not appear on a later page"
        );
        assert!(
            !second_ids.contains(&id(201)),
            "a row newer than the walk started must not appear on a later page"
        );
        assert!(
            second_ids.iter().all(|id| !first_ids.contains(id)),
            "and the second page repeats nothing from the first"
        );
    }

    #[tokio::test]
    async fn the_cursor_names_the_last_row_of_the_page() {
        let rows: Vec<SnapshotRecord> = (0..5u8)
            .map(|n| record(1_000 - i64::from(n), id(n)))
            .collect();

        let page = page_of(&catalog_of(&rows), 2, None).await;

        assert_eq!(page.items.len(), 2);
        let next = page.next.expect("there is another page");
        assert_eq!(next.snapshot_id, page.items[1].id);
        assert_eq!(next.created_at_unix_ms, page.items[1].created_at_unix_ms);
    }

    #[tokio::test]
    async fn a_page_that_exactly_fits_has_no_next_cursor() {
        let rows: Vec<SnapshotRecord> = (0..3u8)
            .map(|n| record(1_000 - i64::from(n), id(n)))
            .collect();

        let page = page_of(&catalog_of(&rows), 3, None).await;

        assert_eq!(page.items.len(), 3);
        assert!(page.next.is_none());
    }

    #[tokio::test]
    async fn a_page_of_no_rows_is_empty_and_ends_the_walk() {
        let rows: Vec<SnapshotRecord> = (0..3u8)
            .map(|n| record(1_000 - i64::from(n), id(n)))
            .collect();

        let page = page_of(&catalog_of(&rows), 0, None).await;

        assert!(page.items.is_empty());
        assert!(
            page.next.is_none(),
            "a cursor here would offer to continue a walk that cannot advance"
        );
    }

    #[test]
    fn a_filter_with_no_limit_asks_for_the_default_page() {
        assert_eq!(
            SnapshotListFilter::matches_all().effective_limit(),
            DEFAULT_LIST_PAGE_LIMIT
        );
        assert_eq!(DEFAULT_LIST_PAGE_LIMIT, 100);
    }

    #[test]
    fn a_limit_above_the_ceiling_is_clamped_rather_than_refused() {
        let filter = SnapshotListFilter::matches_all().paginated(Some(u32::MAX), None);
        assert_eq!(filter.effective_limit(), MAX_LIST_PAGE_LIMIT);
        assert_eq!(MAX_LIST_PAGE_LIMIT, 1000);
    }

    #[test]
    fn an_explicit_zero_limit_survives_into_the_page() {
        let filter = SnapshotListFilter::matches_all().paginated(Some(0), None);
        assert_eq!(
            filter.effective_limit(),
            0,
            "clamping this up would answer a request for nothing with rows"
        );
    }

    #[tokio::test]
    async fn the_page_bounds_and_the_match_are_both_applied() {
        let mut rows: Vec<SnapshotRecord> = (0..3u8)
            .map(|n| record(1_000 - i64::from(n), id(n)))
            .collect();
        rows.extend((10..13u8).map(|n| sandbox_record(1_000 - i64::from(n), id(n))));
        let catalog = catalog_of(&rows);

        let page = catalog
            .list_page(SnapshotListFilter::templates().paginated(Some(2), None))
            .await
            .expect("the in-memory catalog answers every listing");

        assert_eq!(
            page.items.len(),
            2,
            "the limit is honoured even though the filter also matches"
        );
        assert!(
            page.items
                .iter()
                .all(|row| matches!(row.source, SnapshotSource::Template { .. })),
            "and the sandbox rows are excluded even though the page had room"
        );

        let cursor = page.next.expect("a third template row is left");
        let rest = catalog
            .list_page(SnapshotListFilter::templates().paginated(Some(2), Some(cursor)))
            .await
            .expect("the in-memory catalog answers every listing");
        assert_eq!(rest.items.len(), 1, "the walk continues under the filter");
        assert!(rest.next.is_none());
    }
}
