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

    /// A short, stable label for this error's shape.
    ///
    /// 🔴 Exists for `postgres::metrics::record_catalog_rpc`'s `code` label —
    /// the mapping `metrics.rs`'s own module doc says is needed before that
    /// series can be wired to anything. There is no gRPC status on this side
    /// of Stage B to read Go's original label from (Go's `catalogRPCTotal`
    /// carries the RPC's gRPC status code), so the variant name is what a
    /// caller of this catalog can already tell apart by matching on it — a
    /// build refused because the ceiling is full and a query that could not
    /// reach PostgreSQL are different problems, and the dashboard should be
    /// able to tell them apart too.
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

    /// Whether this error is a refusal the caller can act on as an ordinary
    /// decision — matches Go's `catalogRejections` counter, which fires for
    /// exactly this shape of outcome (`catalog_service.go`'s own admission
    /// refusals) and not for a transport or backend failure.
    ///
    /// `InvalidRequest` and `AliasConflict` are the two shapes
    /// `postgres::writes`'s admission/alias paths actually produce for a
    /// refusal (`refused`/`alias_conflict`/`build_refusal` in that module);
    /// `SnapshotNotFound`/`AliasNotFound` are look-up misses, the same
    /// ordinary-decision shape for a read. Every other variant — `Backend`
    /// above all — means something went wrong reaching the answer, not that
    /// the answer was no.
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
