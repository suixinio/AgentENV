//! `aenv-core`'s image contract, plus the half that actually resolves one.
//!
//! Fetching a manifest, pulling blobs and converting them into a local
//! overlaybd image is what this side adds; the request and answer types and
//! the traits callers spell live in `aenv-core`'s `image::contract`.

pub use aenv_core::image::*;

pub mod cache;
pub mod commit_index;
pub mod local_layer;
pub mod oci_image;
pub mod reference;

mod metadata;
mod resolver;

pub use metadata::{env_vars_from_entries, ImageResolutionMetadata};
pub use resolver::ImageResolver;

/// This machine's own layer-cache handle, for the orchestrator that runs on it.
///
/// 🔴 Constructing this is what opens the node-local cache, which is why it is
/// the caller's call and not a default inside `Orchestrator::new` — see that
/// function's own doc.
pub fn local_runtime_image_refs() -> std::sync::Arc<dyn RuntimeImageRefs> {
    cache::local_image_services_from_global_config().runtime_refs
}

/// Boundedly closes every local image-cache RocksDB metadata store this
/// process opened, so the shutdown path can wait on it with a bound instead of
/// trusting an implicit `Drop` deep inside the tokio runtime's blocking pool.
///
/// 🔴 Not a method on [`ImageResolver`]: the store this closes is shared
/// process-wide (`ImageCacheService`'s own instance registry deduplicates by
/// cache root directory and never forgets an entry), so an individual
/// resolver's handle is not this store's owner in any sense that would make
/// "close the resolver" the right shape. See
/// `aenv_core::local_store::LocalKvStore::close` for the mechanism.
pub async fn close_image_cache_stores(timeout: std::time::Duration) {
    cache::close_shared_metadata_stores(timeout).await;
}
