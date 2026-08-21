pub mod backends;
pub mod composite;
pub mod errors;
pub mod interfaces;
pub(crate) mod metrics;
pub mod mirror;

pub use composite::SnapshotRepository;
pub use errors::{RepositoryError, RepositoryResult};
pub use interfaces::{
    ImportedSnapshotArtifacts, SnapshotArtifactStore, SnapshotCatalog, SnapshotCommit,
    SnapshotListFilter, SnapshotRuntimeResolver, StagedSnapshot,
};
