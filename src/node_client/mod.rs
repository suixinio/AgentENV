//! Driving sandboxes on other machines.
//!
//! This is the half of the node service that the deciding half of the split
//! speaks. It exists so an `Orchestrator` — which is written entirely in terms
//! of a local [`SandboxBackendFactory`][crate::sandbox::SandboxBackendFactory]
//! — can be assembled against machines it is not running on, by being handed
//! [`RemoteSandboxBackendFactory`] instead of the Firecracker one.
//!
//! 🔴 **`--role api` assembles this now** (`src/bin/server.rs`,
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
//! Resuming a paused sandbox held on another machine no longer refuses: it
//! sends [`node.proto`'s `Resume`][crate::node_server], which asks the machine
//! holding the capture to reopen it. What still cannot be reached from here is
//! the *other* half of that round trip — the node does not serve `Pause` yet,
//! so nothing driven from this side produces the paused record a resume
//! consumes. See `build_from_paused_state`'s note on
//! [`RemoteSandboxBackendFactory`].

mod factory;
mod paused_state;
mod placement;
mod scheduler_placement;
mod stub;
mod wire;

#[cfg(test)]
mod tests;

pub use factory::RemoteSandboxBackendFactory;
pub use paused_state::RemotePausedState;
pub use placement::{FixedNodePlacement, NodeEndpoint, NodePlacement};
pub use scheduler_placement::SchedulerNodePlacement;
pub use stub::RemoteSandboxStub;
