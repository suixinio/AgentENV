//! `aenv-core`'s image contract plus node-local resolution and caching.

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

/// Returns this node's shared runtime layer-cache handle.
pub fn local_runtime_image_refs() -> std::sync::Arc<dyn RuntimeImageRefs> {
    cache::local_image_services_from_global_config().runtime_refs
}

/// Best-effort bounded shutdown for every local image-cache metadata store.
pub async fn close_image_cache_stores(timeout: std::time::Duration) {
    cache::close_shared_metadata_stores(timeout).await;
}
