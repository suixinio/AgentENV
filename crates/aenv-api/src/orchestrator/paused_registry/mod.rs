//! Paused-sandbox registry with the PostgreSQL backend added to `aenv-core`.

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

    fn identity() -> NodeIdentity {
        NodeIdentity::from_config(&Default::default())
    }

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
        fn peek_observed_with_freshness(
            &self,
            _node_id: &str,
            _now: SystemTime,
        ) -> Option<(
            crate::proto::scheduler::NodeSnapshot,
            crate::node_registry::placement::score::SnapshotFreshness,
        )> {
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

        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM paused_sandboxes")
            .fetch_one(&pool)
            .await
            .expect("paused_sandboxes should exist after build_paused_registry");
        assert_eq!(count, 0);
    }

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
