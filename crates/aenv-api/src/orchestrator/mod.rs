//! `aenv-core`'s orchestration model, plus the pieces only the deciding half
//! has: the Redis metadata store every replica reads, and the routing verdict
//! that outlives the node a sandbox ran on.

mod runtime_routing;
pub mod store;

pub use aenv_core::orchestrator::*;

pub use runtime_routing::RuntimeRouting;
pub use store::{
    ActiveStateRecord, RedisMetadataStore, RedisStoreConfig, RedisStoreConfigError,
    StoredSandboxRecord, DEFAULT_STORE_KEY_PREFIX, STORE_RECORD_VERSION,
};

/// The in-memory metadata store, for this crate's own tests. `aenv-node` owns
/// it -- a node's records live in its process and nowhere else -- and no api
/// binary builds one: `aenv-api` runs the control path over Redis.
#[cfg(test)]
pub use aenv_node::orchestrator::InMemoryMetadataStore;
