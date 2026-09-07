//! Drives sandboxes on remote nodes through a [`RemoteSandboxBackendFactory`].
//!
//! Pause captures remain pinned to the node holding their bytes unless publication
//! produces a cluster-portable snapshot.

mod build;
mod credential;
pub mod factory;
mod native_placement;
mod node_status;
pub mod placement;
pub mod reap;
pub mod record_owner;
pub mod stub;
pub mod wire;

pub use build::build_template_on_a_node;
pub use credential::{client, NodeClient, NodeGateCredential};
pub use factory::RemoteSandboxBackendFactory;
pub use native_placement::NativeNodePlacement;
pub use node_status::override_node_status;
pub use placement::{
    FixedNodePlacement, NodeEndpoint, NodeMembership, NodePlacement, PlacementRuntimeRouting,
};
pub use reap::NodeServiceSandboxDeleter;
pub use record_owner::{SandboxRecordOwner, StoreRecordOwner, UnknownRecordOwner};
pub use stub::RemoteSandboxStub;
