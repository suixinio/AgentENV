//! The running half's own crate: Firecracker, ublk, overlaybd and everything
//! that only a machine with `/dev/kvm` can do.
//!
//! # 🔴 What makes this a crate and not a module
//!
//! Resolving an image, materializing a snapshot and booting a microVM all go
//! through overlaybd and the ublk driver. A process that decides *where* a
//! sandbox runs does none of it and must not link the code that could — see
//! `snapshot::repository::backends::storage`'s own module doc for the gate
//! this replaced.
//!
//! `cargo tree -p aenv-api -e normal | grep -E 'overlaybd|uvm-ublk|storage-util'`
//! is the executable version of that sentence; `make check-crate-boundaries`
//! runs it.
//!
//! # The module tree mirrors `aenv-core`'s
//!
//! Every module here that also exists in `aenv-core` re-exports its
//! counterpart, so `crate::sandbox::SandboxBackend`,
//! `crate::snapshot::SnapshotId` and the rest resolve exactly as they did
//! inside the single crate. The split is a move, not a rewrite.

pub use aenv_core::{
    api, binding_store, cfg, digest, identity, leader_task, local_store, logging, node_client,
    node_registry, observability, orchestrator, p2p, privileges, proto, runtime_snapshot,
    scheduler_endpoint, server_main, types, virtualization,
};

#[cfg(test)]
mod node_client_tests;
#[cfg(test)]
mod tests;

pub mod image;
pub mod node_reclaim;
pub mod node_server;
pub mod overlaybd;
pub mod sandbox;
pub mod setup;
pub mod snapshot;
pub mod template;

/// Proves `aenv-core`'s default log filter still lets **this** crate's logs out.
///
/// `aenv_core::logging::DEFAULT_FILTER` names target prefixes, and a target is
/// `module_path!()` unless a call site says otherwise — so the filter has to
/// name `aenv_node`, and nothing in `aenv-core` can check that from over there.
/// This is the copy of the guard that lives where the target is decided: rename
/// `aenv-node` and `module_path!()` follows, the filter does not, and this goes red.
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
