//! Drives sandboxes on remote nodes through a [`RemoteSandboxBackendFactory`].
//!
//! Pause captures remain pinned to the node holding their bytes unless publication
//! produces a cluster-portable snapshot.

mod build;
pub mod factory;
mod native_placement;
mod node_status;
pub mod paused_state;
pub mod placement;
pub mod stub;
pub mod wire;

pub use build::build_template_on_a_node;
pub use factory::RemoteSandboxBackendFactory;
pub use native_placement::NativeNodePlacement;
pub use node_status::override_node_status;
pub use paused_state::RemotePausedState;
pub use placement::{FixedNodePlacement, NodeEndpoint, NodeMembership, NodePlacement};
pub use stub::RemoteSandboxStub;
