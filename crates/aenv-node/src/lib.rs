//! The running half's own crate: Firecracker, ublk, overlaybd and everything
//! that only a machine with `/dev/kvm` can do.
//!
//! # 🔴 What makes this a crate and not a module
//!
//! Resolving an image, materializing a snapshot and booting a microVM all go
//! through overlaybd and the ublk driver. A process that decides *where* a
//! sandbox runs does none of it and must not link the code that could — see
//! `snapshot::repository::backends::storage`'s own module doc for the gate
//! this replaced.
//!
//! `cargo tree -p aenv-api -e normal | grep -E 'overlaybd|uvm-ublk|storage-util'`
//! is the executable version of that sentence; `make check-crate-boundaries`
//! runs it.
//!
//! # The module tree mirrors `aenv-core`'s
//!
//! Every module here that also exists in `aenv-core` re-exports its
//! counterpart, so `crate::sandbox::SandboxBackend`,
//! `crate::snapshot::SnapshotId` and the rest resolve exactly as they did
//! inside the single crate. The split is a move, not a rewrite.

pub use aenv_core::{
    api, binding_store, cfg, digest, identity, leader_task, local_store, logging, node_client,
    node_registry, observability, orchestrator, p2p, privileges, proto, role, runtime_snapshot,
    scheduler_endpoint, server_main, types, virtualization,
};

#[cfg(test)]
mod node_client_tests;
#[cfg(test)]
mod tests;

pub mod image;
pub mod node_reclaim;
pub mod node_server;
pub mod overlaybd;
pub mod sandbox;
pub mod setup;
pub mod snapshot;
pub mod template;
