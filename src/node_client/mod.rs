//! Driving sandboxes on other machines.
//!
//! This is the half of the node service that the deciding half of the split
//! speaks. It exists so an `Orchestrator` — which is written entirely in terms
//! of a local [`SandboxBackendFactory`][crate::sandbox::SandboxBackendFactory]
//! — can be assembled against machines it is not running on, by being handed
//! [`RemoteSandboxBackendFactory`] instead of the Firecracker one.
//!
//! 🔴 **`aenv-api` assembles this now** (`src/bin/aenv-api.rs`,
//! `assemble_api`), which changes what is unproven about it rather than
//! removing it. What is exercised is this module against a node service
//! running in the same process, over a real socket; what it has still never
//! done is drive a sandbox on a real machine, because nothing has deployed the
//! two halves as two processes yet.
//!
//! The gaps that are known are written on [`RemoteSandboxBackendFactory`] and
//! on the methods that refuse. One is worth naming here because a reader
//! looking for "what does the API half not do" will otherwise find it a piece
//! at a time: **a cold create refuses**, and it is a refusal rather than a
//! stub.
//!
//! Pausing and resuming a sandbox held on another machine both work: a pause
//! sends [`node.proto`'s `Pause`][crate::node_server] and keeps what comes
//! back, and a resume sends `Resume`, which asks the machine holding the
//! capture to reopen it.
//!
//! 🔴 One arm of a pause is still missing and is refused rather than faked:
//! **publication**. `Pause` is sent with `publish: false`, because nothing here
//! commits a staged snapshot row, and a node asked to publish refuses. So a
//! sandbox paused from this half is resumable on the machine that holds it and
//! nowhere else — which is exactly the arm `Resume` implements, and it is a
//! statement of what this build does rather than a silent absence. See
//! `RemoteSandboxStub::pause`.

mod build;
pub mod factory;
mod native_placement;
pub mod paused_state;
pub mod placement;
mod scheduler_placement;
pub mod stub;
pub mod wire;

pub use build::build_template_on_a_node;
pub use factory::RemoteSandboxBackendFactory;
pub use native_placement::NativeNodePlacement;
pub use paused_state::RemotePausedState;
pub use placement::{FixedNodePlacement, NodeEndpoint, NodeMembership, NodePlacement};
pub use scheduler_placement::SchedulerNodePlacement;
pub use stub::RemoteSandboxStub;
