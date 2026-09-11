//! `aenv-core`'s snapshot layer, plus everything that turns a snapshot into
//! bytes a VM can mmap.

pub use aenv_core::snapshot::*;

pub mod artifact_cache;
pub mod mem_prefetch;
pub mod p2p;
pub mod repository;
pub mod runtime_support;

pub use p2p::P2pSnapshotAdvertiser;
