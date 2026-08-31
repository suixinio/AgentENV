mod gc;
mod graph;
mod service;
mod source_config;
mod store;

#[cfg(test)]
mod orchestrator_gc_tests;

pub use store::{
    local_image_services_from_app_config, local_image_services_from_global_config,
    CachedImageConfig, OverlaybdLayerLocation, OverlaybdLayerStore, SourceImageStore,
};

#[cfg(test)]
pub mod test_support {
    pub use super::service::ImageCacheService;
    pub use super::store::test_local_image_services_from_service;
}
