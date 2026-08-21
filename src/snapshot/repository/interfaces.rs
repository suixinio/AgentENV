use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use super::errors::RepositoryResult;
use crate::sandbox::FirecrackerSnapshotManifest;
use crate::snapshot::types::{
    CommittedAttachedDrive, CommittedSnapshot, ManagedLayer, OverlaybdLayerRef,
    PersistedDiskImagePublication, RunnableSnapshot, SnapshotAlias, SnapshotId,
    SnapshotPublishMetadata, SnapshotPublishSource, SnapshotRecord, SnapshotSourceKind,
    TemplateBuildErrorReason, TemplateBuildStatus,
};
use crate::types::{ExecutionId, SandboxResources};

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
}

impl SnapshotListFilter {
    pub fn matches_all() -> Self {
        Self::default()
    }

    pub fn by_ids<I>(snapshot_ids: I) -> Self
    where
        I: IntoIterator<Item = SnapshotId>,
    {
        Self {
            snapshot_ids: Some(snapshot_ids.into_iter().collect()),
            ..Self::default()
        }
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
    /// Which incarnation staged this.
    ///
    /// 🔴 Written and never checked in this phase. It is here so the phase
    /// where several processes may commit can add the predicate without a
    /// migration that backfills a column onto rows already in flight; the
    /// catalog column it feeds (`snapshots.publishing_execution_id`) exists for
    /// the same reason and is written the same way.
    pub execution_id: Option<ExecutionId>,
}

impl StagedSnapshot {
    pub fn id(&self) -> &SnapshotId {
        &self.commit.id
    }

    pub fn alias(&self) -> Option<&SnapshotAlias> {
        self.commit.alias.as_ref()
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
    async fn get(&self, id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>>;

    /// Lists snapshot records matching the provided filter.
    async fn list(&self, filter: SnapshotListFilter) -> RepositoryResult<Vec<SnapshotRecord>>;

    /// Deletes one snapshot's row and any alias binding that still points at
    /// it. Idempotent. Leaves the artifacts to [`SnapshotArtifactStore`].
    ///
    /// Takes the record rather than the id because unbinding the alias needs
    /// it, and the caller has just read it: asking for the id alone would make
    /// every backend re-read the row it was handed.
    async fn delete_record(&self, record: &SnapshotRecord) -> RepositoryResult<()>;

    /// Resolves a human-readable alias to the current snapshot id.
    async fn resolve_alias(&self, alias: &str) -> RepositoryResult<Option<SnapshotId>>;

    /// Atomically transitions one template build from waiting to building.
    ///
    /// Backends should reject non-template records and template records that are
    /// no longer waiting.
    async fn try_start_build(&self, id: &SnapshotId) -> RepositoryResult<SnapshotRecord>;

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
