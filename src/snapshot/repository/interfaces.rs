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

/// How many rows a listing returns when the caller asks for no particular
/// number.
///
/// 🔴 A caller that asks for nothing is not asking for everything. The HTTP
/// layer used to read a missing `limit` as "every row", which is how one
/// request comes to pull the whole catalog into memory; the central catalog
/// answers the same way ([`services/scheduler/internal/catalog`]'s
/// `defaultListLimit`), and the two read sides have to agree or a rollback
/// changes what an unbounded request means.
pub const DEFAULT_LIST_PAGE_LIMIT: u32 = 100;

/// The most rows one page may hold, whatever the caller asked for.
///
/// Matches the central catalog's `maxListLimit`. A request above it is clamped
/// rather than refused: the cursor still walks the rest.
pub const MAX_LIST_PAGE_LIMIT: u32 = 1000;

/// One row's position in the listing order.
///
/// 🔴 **Not the public cursor.** The public one is the base64url token in the
/// `x-next-token` response header, and its shape — an RFC3339 instant, `__`,
/// the id — is part of the HTTP API: changing it breaks in-flight clients and
/// makes a read-side rollback lossy, because a token minted by one side has to
/// be understood by the other. That rendering stays in the API layer
/// (`api::impls::pagination`). This is the two values it decodes to, and the
/// only form the catalog — or the wire to a remote one — ever sees.
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

    /// The listing order: newest first, ties broken by ascending id.
    ///
    /// 🔴 Ties are real and this is the half that makes the cursor work. Two
    /// snapshots created in the same millisecond order by id, and a comparison
    /// that stopped at the timestamp would leave their relative order up to the
    /// sort — so a page boundary landing between them would either repeat one
    /// or skip one, silently and only under load.
    pub fn order(a: &SnapshotRecord, b: &SnapshotRecord) -> Ordering {
        b.created_at_unix_ms
            .cmp(&a.created_at_unix_ms)
            .then_with(|| a.id.cmp(&b.id))
    }

    /// Whether `record` belongs on a page *after* this cursor.
    ///
    /// 🔴 The same predicate the central catalog writes as
    /// `(s.created_at_ms, cursor_id::text) < (cursor_ms, s.id::text)`. The two
    /// read sides answer the same listing, so a row either side would place
    /// differently is a row a rollback loses or repeats.
    ///
    /// The id comparison is on [`SnapshotId`]'s own ordering, which for the
    /// lowercase-hyphenated UUIDs this type guarantees is the same order as the
    /// text comparison SQL does — see the test that pins it.
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
    /// How many rows one page may hold. `None` means
    /// [`DEFAULT_LIST_PAGE_LIMIT`], not "all of them".
    ///
    /// Every catalog read honours this: [`SnapshotCatalog::list_page`] is the
    /// only listing there is, so there is no longer a way to ask a catalog for
    /// every row at once.
    pub limit: Option<u32>,
    /// Where the page starts. `None` starts at the newest row.
    ///
    /// 🔴 No sentinel. The open-ended start used to be a cursor at *now* with
    /// the maximum UUID, which works until the instant it carries is more
    /// precise than the rows it is compared against — then a snapshot created
    /// during the current millisecond sorts as a tie and loses the id
    /// comparison against the sentinel, and drops off the first page.
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

/// The facts a [`SnapshotArtifactStore`] establishes by writing one snapshot's
/// bytes into durable storage.
///
/// This is the whole of what the byte side tells the row side. Everything here
/// is a *logical* reference — a digest, a size, a registry coordinate — never a
/// local path, a temp directory, or a handle. That is what lets the two halves
/// end up in different processes: `SnapshotCatalog::publish_commit` can be
/// answered by something that has never seen the bytes.
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

/// One snapshot ready to be committed: its identity, and the payload describing
/// artifacts that are already durable.
///
/// 🔴 Every field is a plain value — no `PathBuf`, no `Arc`, no temp-directory
/// guard — and the whole struct is `Serialize`/`Deserialize`. That is what makes
/// [`SnapshotCatalog::publish_commit`] answerable by a process that never saw
/// the bytes.
///
/// It is deliberately *narrower* than [`SnapshotPublishMetadata`]: the six
/// fields it drops (context, startup, runtime versions, virtualization mode,
/// image configs, custom extension params) are already inside `committed`, so
/// carrying the whole request here would ask the row half to hold six values it
/// has no use for and cannot serialize.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotCommit {
    pub id: SnapshotId,
    /// At most one alias per snapshot, bound as part of the commit.
    pub alias: Option<SnapshotAlias>,
    pub source: SnapshotPublishSource,
    pub resources: SandboxResources,
    /// The instant the row this commit opens must record as its creation.
    ///
    /// 🔴 Carried rather than left to each store's clock, and the two reasons
    /// are different sizes. The small one: a pause writes two catalogs, each
    /// stamped its own `now`, so every snapshot's `createdAt` differed between
    /// them by one RPC's latency and no comparison of the two could ever be
    /// exact. The large one: a *replayed* commit stamps the replay's clock, so
    /// the backfill that queued the object store's history into the central
    /// catalog rewrote all thirty-two rows' creation times to the moment the
    /// backfill ran — which is the column the listing orders by and the one
    /// `createdAt` is served from.
    ///
    /// `None` means "whichever store takes this decides", which is what a
    /// commit recorded by a build older than this field means and all a caller
    /// with no better answer than *now* can say. It is not a default anything
    /// new should choose.
    #[serde(default)]
    pub created_at_unix_ms: Option<i64>,
    pub committed: CommittedSnapshot,
}

impl SnapshotCommit {
    /// Joins a publish request with what the artifact store stored.
    ///
    /// Pure: no I/O, no backend knowledge, and no clock — `created_at_unix_ms`
    /// is passed in for the same reason the rest of this is a value. This is
    /// what crosses from the byte half to the row half, and the reason the row
    /// half can be remote.
    pub fn new(
        metadata: &SnapshotPublishMetadata,
        imported: ImportedSnapshotArtifacts,
        created_at_unix_ms: i64,
    ) -> Self {
        Self {
            id: metadata.id.clone(),
            alias: metadata.alias.clone(),
            source: metadata.source.clone(),
            resources: metadata.resources,
            created_at_unix_ms: Some(created_at_unix_ms),
            committed: CommittedSnapshot {
                context: metadata.context.clone(),
                startup: metadata.startup.clone(),
                runtime_versions: metadata.runtime_versions.clone(),
                virtualization_mode: metadata.virtualization_mode,
                image_configs: metadata.image_configs.clone(),
                custom_extension_params: metadata.custom_extension_params.clone(),
                rootfs_layers: imported.rootfs_layers,
                attached_drives: imported.attached_drives,
                memory_layers: imported.memory_layers,
                disk_publications: imported.disk_publications,
            },
        }
    }
}

/// One snapshot whose bytes are durable and whose row has not been announced.
///
/// This is the value that opens the seam §5.1 of the decomposition asks for:
/// the byte half runs where the sandbox is, the row half runs where the
/// database is, and this is everything the second half needs from the first.
///
/// 🔴 It is a pure value and must stay one. No `PathBuf`, no `Arc`, no
/// temp-directory guard, no [`FirecrackerSnapshotManifest`] — that last one
/// most of all. The manifest carries the local paths a capture wrote into, and
/// every one of them is `#[serde(skip)]`, so a manifest that crossed a wire
/// would arrive with empty paths and read as a manifest rather than as an
/// error. `commit_staged` must not be able to reach back for a local file, and
/// the way to guarantee that is for it never to be handed one.
///
/// What travels *instead* of the manifest is [`SnapshotCommit`], derived from
/// it while the files were still there.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StagedSnapshot {
    /// Identity plus the payload the row will carry.
    pub commit: SnapshotCommit,
    /// When the bytes finished landing, by the staging node's clock.
    pub staged_at_unix_ms: i64,
    /// The machine whose disk the bytes are on.
    ///
    /// 🔴 Decided by `stage`, not by the commit. Whether a snapshot is
    /// `published` is something only the commit knows — it is the last write
    /// and the one that can see whether shared storage took the bytes — but
    /// *which node* holds them is a fact about where staging ran, and by the
    /// time a remote committer is looking at this value there is nothing left
    /// to ask.
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

/// One admitted build: the template row it moved, and the build's own identity.
///
/// 🔴 The two ids are separate, and this is the type that stops them being
/// assumed equal. The HTTP layer forces `templateID == buildID` today, but the
/// catalog keys a build row by the *build* id, so reusing the template's meant
/// a template could be built exactly once ever — the second admission was
/// refused because a row with that id already existed, which is what a rebuild
/// after a failed build is. A build is a new thing each time it is admitted,
/// and the id it carries is the one a lease renewal has to quote.
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

/// Whether a template in this state may be moved under a new build.
///
/// 🔴 `Waiting` and `Error`, and deliberately both. A template nobody has
/// built yet is `Waiting`; one whose last build failed — or was reaped — is
/// `Error`, and a retry of that is the ordinary case rather than an exception.
/// `Building` is refused because that exclusion is what the transition exists
/// for, and `Ready` because a published template is rebuilt under a new id
/// rather than in place.
///
/// 🔴 Identical, statement for statement, to the central catalog's
/// `markSnapshotBuildingSQL` (`status IN ('waiting', 'error')`). It was not:
/// this side took `Waiting` alone, so on a node writing to both, PostgreSQL
/// admitted the retry and inserted a heartbeat-bearing `builds` row, this side
/// then refused it, and the caller got a 400 over a build the cluster had
/// already counted against its ceiling and its per-template exclusion — held
/// there until the reaper's TTL expired. The second attempt at a failed build
/// is the case the whole retry path is for, and it was the one case neither
/// store could complete.
pub fn build_may_start_from(status: TemplateBuildStatus) -> bool {
    matches!(
        status,
        TemplateBuildStatus::Waiting | TemplateBuildStatus::Error
    )
}

/// Whether a read may see rows that are not resolvable yet.
///
/// 🔴 There is deliberately no `Default`, and the enum is deliberately not a
/// `bool`. `Resolvable` is what stops a snapshot whose bytes are still
/// uploading from starting a VM; `AnyStatus` is what lets the *template*
/// surface see a template that has never been built. Neither is safe to guess.
///
/// 🔴 The two are a property of the **surface**, not of the backend. A
/// template is `waiting` from the moment it is created until its first build
/// commits, so every endpoint under `/templates` is asking about rows that are
/// deliberately not resolvable, and every endpoint that resolves a snapshot in
/// order to *run* it — create-from-template, the build's base image, a
/// cross-node resume — is asking for the opposite. Reading the template
/// surface at `Resolvable` is what made a newly created template invisible:
/// 404 from `GET /templates/{id}`, absent from `GET /templates`, and 404 from
/// the build start that would have moved it out of `waiting` in the first
/// place, so it could never become resolvable either.
///
/// The wire field is spelled the other way round — `allow_any_status`, whose
/// zero value is the safe reading — because proto3 cannot make a bool
/// required. The inversion happens in exactly one place,
/// [`Self::allow_any_status`].
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

    /// 🔴 The inversion, in the one place it happens.
    ///
    /// The wire field is `allow_any_status` so that its zero value — what a
    /// caller that never heard of it sends — keeps the predicate that stops a
    /// half-uploaded snapshot from starting a VM. This side keeps the positive
    /// reading, and this is the seam between them. A `Resolvable` scope that
    /// sent `true` would resolve exactly the rows the field exists to hide.
    #[test]
    fn the_resolvable_scope_never_asks_for_any_status() {
        assert!(!CatalogReadScope::Resolvable.allow_any_status());
        assert!(CatalogReadScope::AnyStatus.allow_any_status());
    }
}

/// Whether "no such snapshot" is the last word on one snapshot.
///
/// 🔴 This exists for the one caller in the tree that *destroys* something on
/// absence: a cross-node resume drops the paused sandbox's registry row — the
/// cluster's only record that the sandbox exists — when the catalog cannot
/// find the snapshot it names. Every other reader treats absence as a 404 and
/// moves on, which costs a retry; this one costs the workspace.
///
/// A read answers what one store holds *now*. That is not the same question as
/// whether the snapshot is gone, because a write that has been accepted and
/// not yet replayed is a snapshot on its way into a store that does not hold it
/// yet. [`SnapshotCatalog::absence_of`] is the question with that gap closed.
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

    /// [`Self::get`] at an explicitly chosen scope.
    ///
    /// 🔴 The default ignores the scope, and that is the right answer for a
    /// catalog whose storage has no status predicate to apply: an object-store
    /// catalog is one object per record and hands back whatever it holds, so
    /// both scopes already mean the same thing there. A backend that *can*
    /// hide a row by status must override this — the central catalog does —
    /// and one that could and did not would put back the defect the scope
    /// exists for, a template invisible for the whole of its life.
    async fn get_scoped(
        &self,
        id_or_alias: &str,
        _scope: CatalogReadScope,
    ) -> RepositoryResult<Option<SnapshotRecord>> {
        self.get(id_or_alias).await
    }

    /// Lists one page of snapshot records, newest first.
    ///
    /// 🔴 The only catalog read that returns more than one row, and it is
    /// required rather than defaulted. There used to be an unbounded `list`
    /// beside it, with `list_page` defaulting to reading every row and slicing
    /// the result; both of that default's justifications are gone — the
    /// object-store catalogs it existed for hold no rows any more, and the
    /// callers that genuinely wanted every row (the mirror's history backfill,
    /// the population comparison behind the read-side switch) were deleted with
    /// the migration they served. Leaving the default in place would leave a
    /// way to pull an entire catalog into memory that nothing needs and no
    /// review would catch.
    ///
    /// Implementations must honour [`SnapshotListFilter::effective_limit`] and
    /// [`SnapshotListFilter::cursor`], order rows by [`SnapshotCursor::order`],
    /// and set [`SnapshotListPage::next`] to the *last row of this page* — so a
    /// caller holding nothing but a token walks the listing exactly once. A
    /// backend whose storage can express that keyset (PostgreSQL's SQL) pushes
    /// it down; one that cannot sorts and slices in memory.
    async fn list_page(&self, filter: SnapshotListFilter) -> RepositoryResult<SnapshotListPage>;

    /// [`Self::list_page`] at an explicitly chosen scope.
    ///
    /// See [`Self::get_scoped`] on why the default ignores the scope.
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

    /// Resolves a human-readable alias to the current snapshot id.
    ///
    /// Resolvable rows only. See [`Self::resolve_alias_scoped`].
    async fn resolve_alias(&self, alias: &str) -> RepositoryResult<Option<SnapshotId>>;

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

    /// Says this node is still running `build_id`.
    ///
    /// 🔴 `false` means the build is no longer the live one — its lease lapsed
    /// and the template was handed to somebody else — and the builder must
    /// **stop**, not retry. It must also write nothing about the template: by
    /// the time it hears this, whatever holds that row belongs to its
    /// successor.
    ///
    /// The default is `true`, which is the honest answer for a backend with no
    /// admission: nothing there can take a build away, so nothing can have.
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

    /// Whether an earlier commit still owns `id`'s artifacts, so a publish that
    /// failed now must not delete them.
    ///
    /// Only the catalog can answer this: it is the half that knows whether a
    /// previous publish of the same id ever committed. The default is "no",
    /// which rolls back unconditionally — the behaviour the OSS backend has
    /// always had.
    async fn retains_artifacts_on_publish_failure(
        &self,
        _id: &SnapshotId,
    ) -> RepositoryResult<bool> {
        Ok(false)
    }

    /// Whether this catalog's not holding `id` is the last word on it.
    ///
    /// 🔴 Asked only by a caller that is about to destroy something over the
    /// answer, and it is a different question from [`Self::get_scoped`]
    /// answering `None`. A read reports what one store holds at one scope; this
    /// reports whether anything else this node can consult still holds the
    /// snapshot or still owes a write that would produce it.
    ///
    /// The default is the honest answer for a catalog that is the only copy
    /// there is: it holds the row or it does not, and there is no queue behind
    /// it and no second store to disagree. It reads at
    /// [`CatalogReadScope::AnyStatus`] because a row that exists and is not
    /// `ready` is a snapshot that exists — the distinction the resolvable
    /// reading cannot make, and the one this call is for.
    ///
    /// An error is never an absence. Implementations must propagate a store
    /// they could not reach rather than reporting [`SnapshotAbsence::Settled`]
    /// from a question nobody answered.
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

/// The paging contract, exercised end to end through the one implementation of
/// it a unit test can reach.
///
/// 🔴 These used to call `paginate_records`, a free function that was the body
/// of `SnapshotCatalog::list_page`'s trait default. The default is gone —
/// `list_page` is required — so the assertions were moved onto
/// [`InMemorySnapshotCatalog`], which is now the only non-SQL implementation of
/// the contract. What they pin is unchanged: the listing order, the keyset
/// cursor, where a page is cut, and what `next` names.
#[cfg(test)]
mod pagination_tests {
    use super::*;
    use crate::snapshot::mock::InMemorySnapshotCatalog;
    use crate::snapshot::types::{SnapshotSource, TemplateBuildInfo, TemplateBuildStatus};
    use crate::types::SandboxResources;
    use std::collections::HashSet;
    use uuid::Uuid;

    /// A catalog holding exactly `rows`, so a listing over it is a listing over
    /// them.
    fn catalog_of(rows: &[SnapshotRecord]) -> InMemorySnapshotCatalog {
        let catalog = InMemorySnapshotCatalog::default();
        for row in rows {
            catalog.seed(row.clone());
        }
        catalog
    }

    /// One page of everything in `catalog`, at an explicit page size.
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
        }
    }

    /// The same row with the other source kind, so a filter has something to
    /// exclude.
    fn sandbox_record(created_at_unix_ms: i64, id: SnapshotId) -> SnapshotRecord {
        SnapshotRecord {
            source: SnapshotSource::Sandbox {
                source_sandbox_id: "sbx-1".to_string(),
            },
            ..record(created_at_unix_ms, id)
        }
    }

    /// An id whose text form sorts by `n`, so a test can say which row it means.
    fn id(n: u8) -> SnapshotId {
        SnapshotId::from_uuid(Uuid::from_bytes([n; 16]))
    }

    /// 🔴 The assumption the whole cursor rests on: `SnapshotId`'s ordering is
    /// its *text* ordering.
    ///
    /// The central catalog compares `s.id::text`, because the public token
    /// carries the id as a string. This side compares `SnapshotId`, which is a
    /// `Uuid`, which orders by bytes. For the lowercase-hyphenated form this
    /// type guarantees the two agree — the hyphens sit at fixed positions and
    /// hex digits ascend in both — but nothing in the type system says so, and
    /// if it ever stopped being true the two read sides would cut pages in
    /// different places with no error anywhere.
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

    /// Newest first, and a tie inside one millisecond broken by ascending id.
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

    /// 🔴 The predicate, spelled out against every case that matters. This is
    /// the Rust half of `(s.created_at_ms, cursor_id::text) < (cursor_ms,
    /// s.id::text)`, and the two have to answer identically or a rollback of
    /// the read side moves a page boundary.
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

    /// 🔴 P2, on the in-memory pager: walk a catalog to the end in small pages
    /// and get every row exactly once.
    ///
    /// The rows deliberately share timestamps in groups, so most page
    /// boundaries land *inside* a tie. A cursor that compared only the
    /// timestamp passes a test whose rows all differ and fails this one.
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

    /// 🔴 P2's control, both halves. A keyset cursor is not an offset: a row
    /// inserted into a range already walked must not appear, and one inserted
    /// ahead of the walk must not either. Offset paging fails one of these in
    /// each direction — it would skip a row on the first and repeat one on the
    /// second.
    #[tokio::test]
    async fn rows_written_during_a_walk_do_not_shift_the_pages() {
        let rows: Vec<SnapshotRecord> = (0..10u8)
            .map(|n| record(1_000 - i64::from(n), id(n)))
            .collect();
        let catalog = catalog_of(&rows);

        let first = page_of(&catalog, 4, None).await;
        let cursor = first.next.clone().expect("there is a second page");
        let first_ids: Vec<SnapshotId> = first.items.iter().map(|row| row.id.clone()).collect();

        // One row inside the range already returned…
        catalog.seed(record(999, id(200)));
        // …and one newer than anything the walk has seen.
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

    /// The cursor names the last row of the page, not the first row of the next
    /// one — that is what lets the next page be computed without the caller
    /// holding anything but the token.
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

    /// A page that exactly fits gets no cursor: one more row is the only
    /// evidence there is another page.
    #[tokio::test]
    async fn a_page_that_exactly_fits_has_no_next_cursor() {
        let rows: Vec<SnapshotRecord> = (0..3u8)
            .map(|n| record(1_000 - i64::from(n), id(n)))
            .collect();

        let page = page_of(&catalog_of(&rows), 3, None).await;

        assert_eq!(page.items.len(), 3);
        assert!(page.next.is_none());
    }

    /// 🔴 A page size of zero. The old HTTP-layer pager computed
    /// `items[limit - 1]` and was kept from underflowing only by an early
    /// return one function up; this is the case that used to reach it.
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

    /// 🔴 Asking for nothing is not asking for everything. This is the defect
    /// that let one request pull the whole catalog into memory, and the default
    /// has to match the central catalog's or an unbounded request means two
    /// different things either side of a read-side switch.
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

    /// 🔴 One filter carries both the page bounds and what to match, and a
    /// listing has to apply both. This replaces the test that pinned
    /// `without_pagination`, the helper that stripped the bounds back off a
    /// filter before handing it to the unbounded `list`: with `list_page` the
    /// only read there is, nothing strips anything, and what is left to get
    /// wrong is honouring one half of the filter and dropping the other.
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
