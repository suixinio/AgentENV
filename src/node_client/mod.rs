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
//! Pausing a sandbox held on another machine no longer refuses: a pause sends
//! [`node.proto`'s `Pause`][crate::node_server] and keeps what comes back, and
//! a resume sends `Resume`, which asks the machine holding the capture to
//! reopen it.
//!
//! 🔴 Two things are still missing and neither is faked:
//!
//! - **publication.** `Pause` is sent with `publish: false`, because nothing
//!   here commits a staged snapshot row, and a node asked to publish refuses
//!   rather than answering with nothing. A sandbox paused from this half is
//!   therefore resumable on the machine that holds it and nowhere else.
//! - **the resume's read of the record.** `Orchestrator::resume_sandbox` reads
//!   the paused state out of `SandboxMetadata::paused_state`, which is
//!   `#[serde(skip)]` and so is always absent on a store that writes its
//!   records out. Until that reads `MetadataStore::paused_handle` instead, the
//!   record this half now writes cannot be read back by the resume that needs
//!   it.

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
