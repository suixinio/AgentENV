//! The deciding half's own crate: everything that talks to PostgreSQL.
//!
//! # 🔴 What makes this a crate and not a module
//!
//! Database credentials, the connection budget and the schema are the deciding
//! half's business, never the machines that run user code — `pg`'s own module
//! doc has stated that invariant since Stage B, and `ServerRole::check_pg_dsn`
//! enforced it at startup. This crate is the compile-time form of the same
//! statement: `aenv-node` does not depend on it, so `sqlx` is not in that
//! binary's dependency graph at all.
//!
//! `cargo tree -p aenv-node -e normal | grep sqlx` is the executable version of
//! that sentence; `make check-crate-boundaries` runs it.
//!
//! # The module tree mirrors `aenv-core`'s
//!
//! `orchestrator::paused_registry::postgres` and
//! `snapshot::repository::backends::postgres` live at the same paths they had
//! inside the single crate, and each intermediate module re-exports its
//! `aenv-core` counterpart. That is deliberate: the moved files reach their
//! neighbours through `super::`/`crate::` exactly as before, so the split is a
//! move rather than a rewrite, and a reader following a path from either half
//! lands in the same place.

// Everything this crate does not extend is `aenv-core`'s, re-exported under the
// same name so that `crate::cfg`, `crate::types`, ... resolve here exactly as
// they do there.
pub use aenv_core::{
    api, binding_store, cfg, digest, identity, image, leader_task, local_store, logging,
    node_client, node_reclaim, node_registry, node_server, observability, overlaybd, p2p,
    privileges, proto, role, runtime_snapshot, sandbox, scheduler_endpoint, server_main, setup,
    template, types, virtualization,
};

pub mod orchestrator;
pub mod pg;
pub mod snapshot;
