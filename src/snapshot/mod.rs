mod artifact_cache;
pub mod image_export;
mod manager;
#[doc(hidden)]
pub mod mock;
mod p2p;
pub mod repository;
pub(crate) mod runtime_support;
mod types;

pub use manager::SnapshotManager;
pub use repository::{
    CatalogReadScope, RepositoryError, RepositoryResult, SnapshotAbsence, SnapshotCursor,
    SnapshotListFilter, SnapshotListPage,
};
pub(crate) use types::rootfs_snapshot_image_tag;
pub use types::{
    CommandContext, CommittedAttachedDrive, CommittedSnapshot, ExternalLayer, ManagedLayer,
    OverlaybdLayerRef, PersistedDiskImagePublication, SnapshotAlias, SnapshotId,
    SnapshotPublishMetadata, SnapshotPublishSource, SnapshotRecord, SnapshotRuntimeVersions,
    SnapshotSource, SnapshotSourceKind, StartupCommand, TemplateBuildErrorReason,
    TemplateBuildInfo, TemplateBuildStatus, SNAPSHOT_ARTIFACT_LAYOUT,
};

// 🔴 Transitional. The node-local resolved view of a snapshot moved to
// `crate::runtime_snapshot`; this keeps the old `crate::snapshot::X` spelling
// working for the resolvers and orchestrator that still produce and consume it
// from here. `crate::sandbox` no longer uses it — it names the new module
// directly — and this re-export goes away with the crate split, once the
// resolvers move to the node side too.
pub use crate::runtime_snapshot::{ResolvedAttachedDrive, RunnableSnapshot};
