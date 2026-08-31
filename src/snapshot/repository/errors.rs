use anyhow::Error as AnyhowError;
use thiserror::Error;

use crate::snapshot::types::SnapshotId;

pub type RepositoryResult<T> = Result<T, RepositoryError>;

#[derive(Debug, Error)]
pub enum RepositoryError {
    #[error("invalid repository request: {reason}")]
    InvalidRequest { reason: String },

    #[error("snapshot not found: {lookup}")]
    SnapshotNotFound { lookup: String },

    #[error("snapshot alias not found: {alias}")]
    AliasNotFound { alias: String },

    #[error("alias '{alias}' already points to '{existing}', cannot rebind to '{new_id}'")]
    AliasConflict {
        alias: String,
        existing: SnapshotId,
        new_id: SnapshotId,
    },

    #[error("artifact not found: {artifact}")]
    ArtifactNotFound { artifact: String },

    #[error("managed layer not found: {digest}")]
    ManagedLayerNotFound { digest: String },

    #[error("integrity mismatch for {artifact}: expected {expected}, got {actual}")]
    IntegrityMismatch {
        artifact: String,
        expected: String,
        actual: String,
    },

    #[error("unsupported operation: {feature}")]
    Unsupported { feature: String },

    #[error("backend error: {message}")]
    Backend {
        message: String,
        #[source]
        source: Option<AnyhowError>,
    },
}

impl RepositoryError {
    pub fn backend(message: impl Into<String>, source: impl Into<AnyhowError>) -> Self {
        Self::Backend {
            message: message.into(),
            source: Some(source.into()),
        }
    }

    /// Returns the stable metric label for this error variant.
    pub fn as_metric_label(&self) -> &'static str {
        match self {
            Self::InvalidRequest { .. } => "invalid_request",
            Self::SnapshotNotFound { .. } => "snapshot_not_found",
            Self::AliasNotFound { .. } => "alias_not_found",
            Self::AliasConflict { .. } => "alias_conflict",
            Self::ArtifactNotFound { .. } => "artifact_not_found",
            Self::ManagedLayerNotFound { .. } => "managed_layer_not_found",
            Self::IntegrityMismatch { .. } => "integrity_mismatch",
            Self::Unsupported { .. } => "unsupported",
            Self::Backend { .. } => "backend",
        }
    }

    /// Returns whether this is an ordinary refusal rather than a backend failure.
    ///
    /// Admission conflicts and lookup misses are refusals.
    pub fn is_rejection(&self) -> bool {
        matches!(
            self,
            Self::InvalidRequest { .. }
                | Self::AliasConflict { .. }
                | Self::SnapshotNotFound { .. }
                | Self::AliasNotFound { .. }
        )
    }
}
