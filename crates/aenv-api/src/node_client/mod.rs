//! Drives sandboxes on remote nodes through a [`RemoteSandboxBackendFactory`].
//!
//! Pause captures remain pinned to the node holding their bytes unless publication
//! produces a cluster-portable snapshot.

mod build;
mod credential;
pub mod factory;
mod native_placement;
mod node_status;
pub mod placement;
pub mod reap;
pub mod record_owner;
pub mod stub;
#[cfg(test)]
mod tests;
pub mod wire;

pub use build::build_template_on_a_node;
pub use credential::{client, NodeClient, NodeGateCredential};
pub use factory::RemoteSandboxBackendFactory;
pub use native_placement::NativeNodePlacement;
pub use node_status::override_node_status;
pub use placement::{
    FixedNodePlacement, NodeEndpoint, NodeMembership, NodePlacement, PlacementRuntimeRouting,
};
pub use reap::NodeServiceSandboxDeleter;
pub use record_owner::{SandboxRecordOwner, StoreRecordOwner, UnknownRecordOwner};
pub use stub::RemoteSandboxStub;

#[cfg(test)]
mod redis_harness_tests {
    use std::sync::atomic::AtomicU32;

    use aenv_core::redis_test_server::{redis_required, RedisTestServer};

    /// Redis subsystem harnesses and their independently owned resources.
    fn harnesses() -> [(
        &'static str,
        &'static AtomicU32,
        Option<&'static RedisTestServer>,
    ); 3] {
        [
            (
                "orchestrator::store::redis",
                aenv_core::orchestrator::store::redis::harness::db_counter(),
                aenv_core::orchestrator::store::redis::harness::server(),
            ),
            (
                "binding_store::redis",
                crate::binding_store::redis::harness::db_counter(),
                crate::binding_store::redis::harness::server(),
            ),
            (
                "node_registry::redis",
                crate::node_registry::redis::harness::db_counter(),
                crate::node_registry::redis::harness::server(),
            ),
        ]
    }

    #[test]
    fn the_three_redis_harnesses_allocate_logical_databases_from_independent_counters() {
        let harnesses = harnesses();
        for (index, (name, counter, _)) in harnesses.iter().enumerate() {
            for (other_name, other_counter, _) in harnesses.iter().skip(index + 1) {
                assert!(
                    !std::ptr::eq(*counter, *other_counter),
                    "{name} and {other_name} allocate logical databases from one shared \
                     counter. They must not: three independent 512-database spaces is what \
                     stops two suites' concurrent tests landing on the same logical database, \
                     which surfaces as cross-suite flakiness nobody can attribute. Give each \
                     harness back its own `static NEXT: AtomicU32`."
                );
            }
        }
    }

    #[test]
    fn the_three_redis_harnesses_own_distinct_redis_server_instances() {
        let harnesses = harnesses();
        let mut started = Vec::new();
        for (name, _, server) in harnesses {
            let Some(server) = server else {
                if redis_required() {
                    panic!(
                        "AENV_REDIS_TEST_REQUIRED=1 but no redis-server could be started for \
                         {name}'s harness. Install redis-server, or point REDIS_SERVER_BIN at \
                         one."
                    );
                }
                eprintln!(
                    "SKIPPED[redis]: the_three_redis_harnesses_own_distinct_redis_server_instances \
                     (no redis-server available)"
                );
                return;
            };
            started.push((name, server));
        }

        for (index, (name, server)) in started.iter().enumerate() {
            for (other_name, other_server) in started.iter().skip(index + 1) {
                assert_ne!(
                    server.port(),
                    other_server.port(),
                    "{name} and {other_name} are talking to one shared redis-server. They must \
                     not: each harness owns its own process, so that one suite flushing its key \
                     namespace cannot reach a database another suite is mid-test on."
                );
                assert_ne!(server.url(7), other_server.url(7));
            }
        }
    }
}
