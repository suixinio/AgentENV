//! `PostgresSnapshotCatalog`: a `SnapshotCatalog` implementation that talks to
//! PostgreSQL directly, in-process — the Stage B replacement for
//! [`super::central::CentralSnapshotCatalog`]'s gRPC hop to
//! `services/scheduler`.
//!
//! Module skeleton only for now (Stage B step 1,
//! `docs/proposals/_sd-phase4-stageB-catalog.md` §7): the type exists and
//! `cargo build` passes, but nothing constructs one and it implements no
//! trait yet. The read path, write path, and the `build_snapshot_backend`
//! wiring that actually puts this on a request path land in later steps.
//!
//! 🔴 `--role node` must never hold one of these — see `src/pg/mod.rs`'s own
//! module doc and `crate::role::ServerRole::check_pg_dsn`, which refuses
//! `--role node` startup outright if `[pg].dsn` is configured at all.
#![allow(dead_code)]

pub(crate) mod metrics;
pub(crate) mod migrate;
pub(crate) mod migration_state;
pub(crate) mod reaper;

use sqlx::PgPool;
use uuid::Uuid;

/// A `SnapshotCatalog` backed by a direct, in-process connection pool to the
/// shared control-plane PostgreSQL database, rather than an RPC hop to
/// `services/scheduler`.
pub(crate) struct PostgresSnapshotCatalog {
    pool: PgPool,
    cluster_id: Uuid,
}

impl PostgresSnapshotCatalog {
    /// Wraps an already-connected pool. The pool itself is built by
    /// `src/pg::connect` — this type never dials PostgreSQL on its own, the
    /// same division `CentralSnapshotCatalog` draws between "how to reach the
    /// database" and "what to do once connected".
    pub(crate) fn new(pool: PgPool, cluster_id: Uuid) -> Self {
        Self { pool, cluster_id }
    }
}
