//! Shared PostgreSQL pool and advisory-lock election infrastructure.
//! This control-plane-only module must never enter `aenv-node`'s dependency
//! graph or receive node-side credentials.

pub mod election;
pub mod lock_keys;
pub mod pool;

pub use election::{spawn_singleton_task, LeaderContext, SingletonTaskBody, SingletonTaskHandle};
pub use lock_keys::{AdvisoryLockKey, GO_BUILD_ADMISSION_LOCK_KEY, GO_SCHEMA_LOCK_KEY};
pub use pool::{
    connect, redact_dsn, PgPoolSettings, DEFAULT_CONNECT_TIMEOUT, DEFAULT_MAX_CONNECTIONS,
};

#[cfg(test)]
pub mod harness;
