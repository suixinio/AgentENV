//! Talking to the registry a snapshot's disk image was published to.
//!
//! 🔴 Creating a publication reads overlaybd layers off local disk, so
//! `publisher` and `source_image` are `aenv-node`'s. Removing one
//! is a registry `DELETE` against a digest already written down in the row,
//! which is why `client`, `reference` and `rollback` are here: the half that
//! owns catalog rows has to be able to delete a snapshot even when the node
//! that wrote it is gone.

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
