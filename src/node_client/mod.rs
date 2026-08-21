//! Driving sandboxes on other machines.
//!
//! This is the half of the node service that the deciding half of the split
//! speaks. It exists so an `Orchestrator` — which is written entirely in terms
//! of a local [`SandboxBackendFactory`][crate::sandbox::SandboxBackendFactory]
//! — can be assembled against machines it is not running on, by being handed
//! [`RemoteSandboxBackendFactory`] instead of the Firecracker one.
//!
//! 🔴 **Nothing assembles it yet.** `--role api` does not start, and this is
//! one of the two pieces it is waiting for. What is here is exercised by tests
//! against a node service running in the same process; what it has never done
//! is drive a real sandbox on a real machine. The gaps that are known are
//! written on [`RemoteSandboxBackendFactory`] and on the methods that refuse.

mod factory;
mod paused_state;
mod placement;
mod stub;
mod wire;

#[cfg(test)]
mod tests;

pub use factory::RemoteSandboxBackendFactory;
pub use paused_state::RemotePausedState;
pub use placement::{FixedNodePlacement, NodeEndpoint, NodePlacement};
pub use stub::RemoteSandboxStub;
