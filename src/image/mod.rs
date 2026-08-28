//! What the rest of the system asks of the image layer.
//!
//! 🔴 The resolving half — `regctl`, the layer cache and overlaybd — is
//! `aenv-node`'s `image` module. What is here is the contract every caller
//! spells, and the error type they classify by. See [`contract`]'s own module
//! doc for the argument.

mod contract;
#[cfg(any(test, feature = "test-support"))]
pub mod mock;
pub mod regctl;

use thiserror::Error;

pub use contract::{
    DisabledRuntimeImageRefs, ImageBaseContext, RefusingImageResolver, ResolvedBlockImage,
    RootfsImageResolver, RuntimeImageOwner, RuntimeImageRefs,
};
#[cfg(any(test, feature = "test-support"))]
pub use mock::RecordingRuntimeImageRefs;

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
    pub fn context(self, context: impl std::fmt::Display) -> Self {
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
