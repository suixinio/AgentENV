//! `aenv-core`'s paused-sandbox registry, plus the PostgreSQL backend.
//!
//! The shared half — the trait, the error type, `build_paused_registry` and
//! its `local` arm — is `aenv-core`'s and is re-exported here
//! unchanged. What this crate adds is the `postgres` arm's implementation:
//! the schema bootstrap, the SQL, the leader-elected reconcile/reclaim loops
//! and the per-replica lease renewal.

pub use aenv_core::orchestrator::paused_registry::*;

pub mod postgres;

pub use postgres::{
    spawn_paused_registry_background_tasks, PausedRegistryBackgroundTasks, PgPausedRegistryFactory,
    PostgresPausedSandboxRegistry,
};

#[cfg(test)]
mod pg {
    use std::sync::Arc;
    use std::time::SystemTime;

    use super::*;
    use crate::cfg::{PausedRegistryBackendKind, PausedRegistryConfig};
    use crate::identity::NodeIdentity;
    use crate::node_registry::registry::NodeRegistry;
    use crate::pg::harness::isolated_schema_pool_or_skip;

    // 🔴 Copy of `aenv-core`'s own `build_tests::identity` helper rather than
    // an import of it: that lives in a `#[cfg(test)]` module, which is not
    // compiled when `aenv-core` is a dependency. Kept identical in shape so
    // the two suites still describe the same identity.
    fn identity() -> NodeIdentity {
        NodeIdentity::from_config(&Default::default())
    }

    /// The `postgres` arm's factory, as `build_paused_registry` now takes it.
    fn factory(pool: &sqlx::PgPool) -> PgPausedRegistryFactory {
        PgPausedRegistryFactory::new(pool.clone())
    }

    struct NoopNodeRegistry;
    impl NodeRegistry for NoopNodeRegistry {
        fn snapshot(&self, _allow_lingering: bool) -> Vec<crate::node_registry::types::Node> {
            Vec::new()
        }
        fn contains(&self, _node: &crate::node_registry::types::Node) -> bool {
            false
        }
        fn resolve(&self, _node_id: &str) -> Option<crate::node_registry::types::Node> {
            None
        }
        fn heartbeat(
            &self,
            _req: &crate::proto::scheduler::HeartbeatRequest,
            _now: SystemTime,
        ) -> Result<
            (crate::node_registry::types::Node, String),
            crate::node_registry::registry::NodeNotInRegistry,
        > {
            Err(crate::node_registry::registry::NodeNotInRegistry)
        }
        fn list_observed(
            &self,
            _cluster_id: &str,
            _now: SystemTime,
        ) -> Vec<crate::proto::scheduler::ObservedNode> {
            Vec::new()
        }
        fn list_p2p_peers(
            &self,
            _cluster_id: &str,
            _backend: &str,
            _exclude_node_id: &str,
            _now: SystemTime,
        ) -> Vec<crate::proto::scheduler::P2pPeer> {
            Vec::new()
        }
        fn filter_p2p_peers(
            &self,
            _cluster_id: &str,
            _backend: &str,
            _node_ids: &[String],
            _exclude_node_id: &str,
            _now: SystemTime,
        ) -> Vec<crate::proto::scheduler::P2pPeer> {
            Vec::new()
        }
        fn get_observed(
            &self,
            _node_id: &str,
            _cluster_id: &str,
            _now: SystemTime,
        ) -> Option<crate::proto::scheduler::ObservedNode> {
            None
        }
        fn peek_observed(&self, _node_id: &str) -> Option<crate::proto::scheduler::NodeSnapshot> {
            None
        }
        fn roster_of(
            &self,
            _node_id: &str,
        ) -> Option<(Vec<crate::node_registry::types::RosterEntry>, SystemTime)> {
            None
        }
        fn nodes_holding(&self, _sandbox_id: &str) -> Vec<String> {
            Vec::new()
        }
        fn rosters_in_cluster(
            &self,
            _cluster_id: &str,
        ) -> Vec<crate::node_registry::types::Roster> {
            Vec::new()
        }
        fn unregister_observed(
            &self,
            _node_id: &str,
            _service_instance_id: &str,
        ) -> Result<(), crate::node_registry::registry::ServiceInstanceMismatch> {
            Ok(())
        }
        fn applied_cpu_intersection(&self, _cluster_id: &str) -> Option<String> {
            None
        }
    }

    fn config() -> PausedRegistryConfig {
        PausedRegistryConfig {
            backend: PausedRegistryBackendKind::Postgres,
            reconcile_interval_secs: 30,
            lease_ttl_secs: 90,
            reclaim_interval_secs: 30,
        }
    }

    /// 🔴 D1's central startup guard, proved with a real (reachable) pool
    /// this time: a `postgres` backend still refuses to start without a node
    /// registry, even once the *other* precondition
    /// (`the_postgres_backend_without_a_pool_is_a_startup_failure`, in
    /// `build_tests`) is satisfied.
    ///
    /// 🔴 Unconditional now. M1 narrowed this guard to `aenv-api`
    /// specifically, because the pre-split single process's own identity
    /// coincided with `origin_node_id` for everything it ran and `renew_lease`
    /// already covered what D2 Fix A exists to cover; a sibling test pinned
    /// that exemption. No process is both halves any more, `aenv-api` is
    /// `build_paused_registry`'s only caller, and the exemption's test went
    /// with the shape it was about.
    #[tokio::test]
    async fn the_postgres_backend_without_a_node_registry_is_a_startup_failure() {
        let pool = isolated_schema_pool_or_skip!(
            "the_postgres_backend_without_a_node_registry_is_a_startup_failure"
        );

        let failure =
            build_paused_registry(&config(), &identity(), Some(&factory(&pool)), None).await;

        let Err(failure) = failure else {
            panic!(
                "a postgres backend with no node registry must not build a registry under aenv-api"
            );
        };
        assert!(
            failure.to_string().contains("heartbeat roster"),
            "the refusal has to name what is missing, got {failure}"
        );
    }

    /// The happy path: a real pool and a (fake, but present) node registry
    /// build a working, cluster-backed registry, and the schema bootstrap
    /// this function is documented to run actually leaves the table usable.
    #[tokio::test]
    async fn the_postgres_backend_builds_a_working_cluster_backed_registry() {
        let pool = isolated_schema_pool_or_skip!(
            "the_postgres_backend_builds_a_working_cluster_backed_registry"
        );

        let registry = build_paused_registry(
            &config(),
            &identity(),
            Some(&factory(&pool)),
            Some(Arc::new(NoopNodeRegistry) as Arc<dyn NodeRegistry>),
        )
        .await
        .expect("a real pool and a real node registry should build successfully");

        assert!(registry.is_cluster_backed());

        // The migration this call is documented to run actually happened:
        // the table is queryable without the caller having to migrate it
        // separately first.
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM paused_sandboxes")
            .fetch_one(&pool)
            .await
            .expect("paused_sandboxes should exist after build_paused_registry");
        assert_eq!(count, 0);
    }

    /// 🔴 B2(1)'s own wiring pin: `build_paused_registry`'s `postgres` arm
    /// must actually call `postgres::attempt_initial_grace_entry` before it
    /// returns -- proving the connection, not just
    /// `postgres::grace::attempt_initial_entry`'s own isolated pg-gated
    /// tests (`postgres::grace::pg`), which exercise the function directly
    /// but say nothing about whether `build_paused_registry` ever calls it.
    /// A `paused_registry_grace` row for this cluster must exist the moment
    /// this call returns, with no leader-elected background loop having run
    /// yet.
    #[tokio::test]
    async fn building_the_postgres_backend_enters_grace_synchronously() {
        let pool = isolated_schema_pool_or_skip!(
            "building_the_postgres_backend_enters_grace_synchronously"
        );

        let node_identity = identity();
        let registry = build_paused_registry(
            &config(),
            &node_identity,
            Some(&factory(&pool)),
            Some(Arc::new(NoopNodeRegistry) as Arc<dyn NodeRegistry>),
        )
        .await
        .expect("a real pool and a real node registry should build successfully");
        assert!(registry.is_cluster_backed());

        let row_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM paused_registry_grace WHERE cluster_id = $1)",
        )
        .bind(node_identity.cluster_id)
        .fetch_one(&pool)
        .await
        .expect("query should succeed");
        assert!(
            row_exists,
            "build_paused_registry must have entered grace synchronously before returning -- \
             no background loop has run yet at this point in the test"
        );
    }
}
