//! Node-only runtime support for Firecracker, ublk, overlaybd, image
//! resolution, snapshots, and templates.

pub use aenv_core::{
    binding_store, digest, identity, leader_task, logging, node_client, node_registry,
    orchestrator, privileges, proto, record_dir, runtime_snapshot, scheduler_endpoint, server_main,
    types, virtualization,
};

#[cfg(test)]
mod node_client_tests;
#[cfg(test)]
mod tests;

pub mod api;
pub mod cfg;
pub mod image;
pub mod node_reclaim;
pub mod node_server;
pub mod observability;
pub mod overlaybd;
pub mod p2p;
pub mod sandbox;
pub mod setup;
pub mod snapshot;
pub mod template;

#[cfg(test)]
mod default_filter_guard {
    use aenv_core::logging::capture::targets_passing_filter;
    use aenv_core::logging::{DEFAULT_FILTER, PRE_RENAME_FILTER};

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
        assert_eq!(
            targets_passing_filter(PRE_RENAME_FILTER, emit_untargeted),
            Vec::<String>::new(),
            "the pre-split filter matched {}, so this guard cannot tell a \
             stale filter from a current one and is worth nothing",
            module_path!()
        );
    }
}
