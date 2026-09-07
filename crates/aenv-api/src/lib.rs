//! PostgreSQL-backed control-plane components for `aenv-api`.
//! `aenv-node` must not depend on this crate, keeping `sqlx` and database
//! credentials out of the node binary.

// Re-export `aenv-core` so shared module paths remain unchanged.
pub use aenv_core::{
    api, binding_store, cfg, digest, identity, image, leader_task, logging, node_client,
    node_registry, observability, p2p, privileges, proto, record_dir, runtime_snapshot, sandbox,
    scheduler_endpoint, server_main, template, types, virtualization,
};

pub mod internal_api;
pub mod internal_auth;
pub mod orchestrator;
pub mod pg;
pub mod secrets;
pub mod snapshot;

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
