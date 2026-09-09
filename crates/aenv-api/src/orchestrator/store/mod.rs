//! The metadata store the deciding half keeps, over the store contract in
//! `aenv-core`.
//!
//! A record here outlives the replica that wrote it, which is what lets any
//! replica answer for a sandbox and what a node's own store cannot do.

pub mod redis;

pub use aenv_core::orchestrator::store::*;

pub use redis::{
    ActiveStateRecord, RedisMetadataStore, RedisStoreConfig, RedisStoreConfigError,
    StoredSandboxRecord, DEFAULT_KEY_PREFIX as DEFAULT_STORE_KEY_PREFIX,
    RECORD_VERSION as STORE_RECORD_VERSION,
};
