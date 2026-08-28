mod captured;
pub mod manager;
#[doc(hidden)]
pub mod mock;
pub mod repository;
pub mod types;

pub use captured::{
    CallerOwnedArtifacts, CapturedSandboxSnapshot, LocalCapturedArtifacts,
    SnapshotArtifactAdvertiser, UnpublishableCapture,
};
pub use manager::SnapshotManager;
pub use repository::{
    CatalogReadScope, RepositoryError, RepositoryResult, SnapshotAbsence, SnapshotCursor,
    SnapshotListFilter, SnapshotListPage,
};
pub use types::rootfs_snapshot_image_tag;
pub use types::{
    CommandContext, CommittedAttachedDrive, CommittedSnapshot, ExternalLayer, ManagedLayer,
    OverlaybdLayerRef, PersistedDiskImagePublication, SnapshotAlias, SnapshotId,
    SnapshotPublishMetadata, SnapshotPublishSource, SnapshotRecord, SnapshotRuntimeVersions,
    SnapshotSource, SnapshotSourceKind, StartupCommand, TemplateBuildErrorReason,
    TemplateBuildInfo, TemplateBuildStatus, SNAPSHOT_ARTIFACT_LAYOUT,
};
