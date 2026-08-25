pub(crate) mod cache;
pub(crate) mod commit_index;
pub(crate) mod local_layer;
mod metadata;
pub(crate) mod oci_image;
mod reference;
mod resolver;

use thiserror::Error;

pub use metadata::ImageBaseContext;
pub(crate) use metadata::{env_vars_from_entries, ImageResolutionMetadata};
pub use resolver::{ImageResolver, ResolvedBlockImage};

/// Boundedly closes every local image-cache RocksDB metadata store this
/// process opened, so the shutdown path can wait on it with a bound instead of
/// trusting an implicit `Drop` deep inside the tokio runtime's blocking pool.
///
/// 🔴 Not a method on [`ImageResolver`]: the store this closes is shared
/// process-wide (`ImageCacheService`'s own instance registry deduplicates by
/// cache root directory and never forgets an entry), so an individual
/// resolver's handle is not this store's owner in any sense that would make
/// "close the resolver" the right shape. See
/// `crate::local_store::LocalKvStore::close` for the mechanism.
pub async fn close_image_cache_stores(timeout: std::time::Duration) {
    cache::close_shared_metadata_stores(timeout).await;
}

/// The image module's single error type.
///
/// Variants exist only for the distinctions a caller actually branches on (the
/// HTTP status it returns); every other, server-side failure funnels into
/// [`ImageError::Other`], which keeps the full `anyhow` context chain for
/// diagnostics. Callers classify by matching the variant — never by downcasting
/// a type-erased error.
#[derive(Debug, Error)]
pub enum ImageError {
    /// The image reference is syntactically invalid or disallowed by config (400).
    #[error("{reason}")]
    InvalidReference { reason: String },
    /// The reference is valid but the registry has no such image/tag (404).
    #[error("{reason}")]
    NotFound { reason: String },
    /// The image exists but its format/shape is not supported by AgentENV
    /// (e.g. overlaybd turbo-OCI, tar-wrapped overlaybd, unknown layer
    /// mediaTypes). This is the publisher's/caller's image problem (400).
    #[error("{reason}")]
    UnsupportedImage { reason: String },
    /// Any other, server-side failure: network, conversion, storage, ... (500).
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// `Result` for the image module; every fallible image API returns this.
pub type ImageResult<T> = std::result::Result<T, ImageError>;

impl ImageError {
    /// `true` when the failure is the caller's fault (bad or missing image) and
    /// should be surfaced as a 4xx rather than a 5xx.
    pub fn is_user_error(&self) -> bool {
        matches!(
            self,
            Self::InvalidReference { .. } | Self::NotFound { .. } | Self::UnsupportedImage { .. }
        )
    }

    /// Prepend human-readable context while preserving the variant. This is the
    /// variant-safe counterpart to [`anyhow::Context`], which would collapse
    /// every variant into [`ImageError::Other`] and so lose the 4xx/5xx
    /// classification when context is added mid-flight.
    pub(crate) fn context(self, context: impl std::fmt::Display) -> Self {
        match self {
            Self::InvalidReference { reason } => Self::InvalidReference {
                reason: format!("{context}: {reason}"),
            },
            Self::NotFound { reason } => Self::NotFound {
                reason: format!("{context}: {reason}"),
            },
            Self::UnsupportedImage { reason } => Self::UnsupportedImage {
                reason: format!("{context}: {reason}"),
            },
            Self::Other(err) => Self::Other(err.context(context.to_string())),
        }
    }
}
