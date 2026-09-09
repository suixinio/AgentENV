//! The metadata store a node keeps, over the store contract in `aenv-core`.
//!
//! A node's records live in this process only: a node restart loses the
//! sandboxes that were running on it, so nothing here outlives it.

mod in_memory;

pub use aenv_core::orchestrator::store::*;

pub use in_memory::InMemoryMetadataStore;
