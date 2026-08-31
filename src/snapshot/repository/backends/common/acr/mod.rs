//! Registry access for snapshot disk-image publications.
//!
//! Rollback uses recorded digests; publication remains on the node holding the layers.

pub mod client;
#[cfg(any(test, feature = "test-support"))]
pub mod fake_registry;
pub mod manifest;
pub mod reference;
pub mod rollback;

pub use manifest::{
    build_oci_image_manifest, host_architecture_for_oci, snapshot_oci_config_blob, OciDescriptor,
    SnapshotOciConfigInput, OCI_IMAGE_MANIFEST_MEDIA_TYPE,
};
pub use reference::SourceRegistryRepository;
pub use rollback::AcrPublicationRollback;
