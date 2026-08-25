//! Shared PostgreSQL infrastructure for the control plane (`--role api` /
//! `--role all`): one connection pool convention and one cluster-leadership
//! primitive, so Stage B's catalog fold and Stage C's paused-registry fold
//! build on the same foundation instead of inventing their own.
//!
//! This module is foundation only — it holds no business logic of either
//! stage's own. It exists because both need the same two things and neither
//! should own them: a per-replica connection pool ([`pool`]) and a way to
//! run exactly one cluster-wide instance of a background task
//! ([`election`]), built on PostgreSQL session-scoped advisory locks rather
//! than a Redis-style TTL lock.
//!
//! # 🔴 `--role node` never reaches this module
//!
//! Database credentials, the connection budget and the schema are the
//! deciding half's business, not the machines that run user code —
//! `src/snapshot/repository/backends/central/mod.rs` states the same
//! invariant for the snapshot catalog's own central backend, and
//! `PausedRegistryBackendKind::Postgres` (`src/cfg.rs`) already refuses a
//! node that tries to connect to the registry database directly. This module
//! is the same invariant generalized to *any* `[pg]` DSN: `--role node`
//! refuses to start with one configured at all, checked by
//! [`crate::role::ServerRole::check_pg_dsn`] before any role-specific
//! assembly runs in `src/bin/server.rs`.

pub mod election;
pub mod lock_keys;
pub mod pool;

pub use election::{spawn_singleton_task, LeaderContext, SingletonTaskBody, SingletonTaskHandle};
pub use lock_keys::{AdvisoryLockKey, GO_BUILD_ADMISSION_LOCK_KEY, GO_SCHEMA_LOCK_KEY};
pub use pool::{
    connect, redact_dsn, PgPoolSettings, DEFAULT_CONNECT_TIMEOUT, DEFAULT_MAX_CONNECTIONS,
};

#[cfg(test)]
pub(crate) mod harness;
