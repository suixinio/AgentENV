//! `aenv-core`'s orchestrator, re-exported for the api half.

pub use aenv_core::orchestrator::*;

/// The in-memory metadata store, for this crate's own tests. `aenv-node` owns
/// it -- a node's records live in its process and nowhere else -- and no api
/// binary builds one: `aenv-api` runs the control path over Redis.
#[cfg(test)]
pub use aenv_node::orchestrator::InMemoryMetadataStore;
