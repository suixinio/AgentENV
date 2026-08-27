//! The deciding half's own crate: everything that talks to PostgreSQL.
//!
//! # 🔴 What makes this a crate and not a module
//!
//! Database credentials, the connection budget and the schema are the deciding
//! half's business, never the machines that run user code — `pg`'s own module
//! doc has stated that invariant since Stage B, and `aenv-node`'s own
//! `refuse_configured_pg_dsn` enforces it at startup. This crate is the
//! compile-time form of the same statement: `aenv-node` does not depend on it,
//! so `sqlx` is not in that binary's dependency graph at all. The two are not
//! redundant — the graph says a node cannot *use* a DSN, the startup check says
//! it must not be *handed* one.
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
    node_client, node_registry, observability, p2p, privileges, proto, runtime_snapshot, sandbox,
    scheduler_endpoint, server_main, template, types, virtualization,
};

pub mod orchestrator;
pub mod pg;
pub mod snapshot;

/// Proves `aenv-core`'s default log filter still lets **this** crate's logs out.
///
/// `aenv_core::logging::DEFAULT_FILTER` names target prefixes, and a target is
/// `module_path!()` unless a call site says otherwise — so the filter has to
/// name `aenv_api`, and nothing in `aenv-core` can check that from over there.
/// This is the copy of the guard that lives where the target is decided: rename
/// `aenv-api` and `module_path!()` follows, the filter does not, and this goes red.
#[cfg(test)]
mod default_filter_guard {
    use aenv_core::logging::capture::targets_passing_filter;
    use aenv_core::logging::{DEFAULT_FILTER, PRE_RENAME_FILTER};

    /// One callsite, reused by both directions, with no explicit `target:`.
    fn emit_untargeted() {
        tracing::info!("default-filter probe");
    }

    #[test]
    fn default_filter_covers_this_crate() {
        assert_eq!(
            targets_passing_filter(DEFAULT_FILTER, emit_untargeted),
            vec![module_path!().to_string()],
            "{DEFAULT_FILTER} drops this crate's own logs; a crate rename \
             (or a typo) has put it out of step with module_path!()"
        );
    }

    #[test]
    fn the_pre_rename_filter_no_longer_covers_this_crate() {
        // Same callsite as above, so an empty result is the filter's doing and
        // not a callsite that was never interesting.
        assert_eq!(
            targets_passing_filter(PRE_RENAME_FILTER, emit_untargeted),
            Vec::<String>::new(),
            "the pre-split filter matched {}, so this guard cannot tell a \
             stale filter from a current one and is worth nothing",
            module_path!()
        );
    }
}
