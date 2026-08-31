//! Direct PostgreSQL snapshot catalog for control-plane processes.
//! Object storage holds byte artifacts only; catalog rows live here.
//! `aenv-node` must not link this crate or hold database credentials.

pub mod convert;
pub mod metrics;
pub mod migrate;
pub mod reads;
pub mod reaper;
pub mod writes;

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use sqlx::PgPool;
use uuid::Uuid;

use crate::cfg::AppConfig;
use crate::snapshot::repository::interfaces::{
    CatalogReadScope, SnapshotCatalog, SnapshotCommit, SnapshotListFilter, SnapshotListPage,
    StartedBuild,
};
use crate::snapshot::repository::RepositoryResult;
use crate::snapshot::types::{SnapshotId, SnapshotRecord, TemplateBuildErrorReason};

/// Default cluster-wide concurrent-build ceiling; zero resolves to this value.
pub const DEFAULT_MAX_CONCURRENT_BUILDS: i32 = 20;

/// Snapshot catalog backed by the shared control-plane PostgreSQL pool.
pub struct PostgresSnapshotCatalog {
    pool: PgPool,
    cluster_id: Uuid,
    node_id: String,
    max_concurrent_builds: i32,
}

impl PostgresSnapshotCatalog {
    /// Wraps an already-connected and migrated pool.
    pub fn new(pool: PgPool, cluster_id: Uuid, node_id: String) -> Self {
        Self {
            pool,
            cluster_id,
            node_id,
            max_concurrent_builds: DEFAULT_MAX_CONCURRENT_BUILDS,
        }
    }

    /// Sets the build ceiling. Zero uses [`DEFAULT_MAX_CONCURRENT_BUILDS`];
    /// negative disables the ceiling.
    pub fn with_max_concurrent_builds(mut self, max: i32) -> Self {
        self.max_concurrent_builds = if max == 0 {
            DEFAULT_MAX_CONCURRENT_BUILDS
        } else {
            max
        };
        self
    }

    pub async fn get_scoped(
        &self,
        id_or_alias: &str,
        scope: CatalogReadScope,
    ) -> RepositoryResult<Option<SnapshotRecord>> {
        metrics::record_catalog_outcome(
            "get_scoped",
            reads::get_scoped(&self.pool, self.cluster_id, id_or_alias, scope).await,
        )
    }

    pub async fn resolve_alias_scoped(
        &self,
        alias: &str,
        scope: CatalogReadScope,
    ) -> RepositoryResult<Option<SnapshotId>> {
        metrics::record_catalog_outcome(
            "resolve_alias_scoped",
            reads::resolve_alias_scoped(&self.pool, self.cluster_id, alias, scope).await,
        )
    }

    pub async fn list_page_scoped(
        &self,
        filter: SnapshotListFilter,
        scope: CatalogReadScope,
    ) -> RepositoryResult<SnapshotListPage> {
        let (items, next) = metrics::record_catalog_outcome(
            "list_page_scoped",
            reads::list_page_scoped(&self.pool, self.cluster_id, &filter, scope).await,
        )?;
        Ok(SnapshotListPage { items, next })
    }
}

#[async_trait]
impl SnapshotCatalog for PostgresSnapshotCatalog {
    async fn create(&self, record: SnapshotRecord) -> RepositoryResult<SnapshotRecord> {
        metrics::record_catalog_outcome(
            "create",
            writes::create(&self.pool, self.cluster_id, &self.node_id, record).await,
        )
    }

    async fn publish_commit(&self, commit: SnapshotCommit) -> RepositoryResult<SnapshotRecord> {
        metrics::record_catalog_outcome(
            "publish_commit",
            writes::publish_commit(&self.pool, self.cluster_id, &self.node_id, commit).await,
        )
    }

    async fn get(&self, id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>> {
        PostgresSnapshotCatalog::get_scoped(self, id_or_alias, CatalogReadScope::Resolvable).await
    }

    async fn get_scoped(
        &self,
        id_or_alias: &str,
        scope: CatalogReadScope,
    ) -> RepositoryResult<Option<SnapshotRecord>> {
        PostgresSnapshotCatalog::get_scoped(self, id_or_alias, scope).await
    }

    async fn list_page(&self, filter: SnapshotListFilter) -> RepositoryResult<SnapshotListPage> {
        PostgresSnapshotCatalog::list_page_scoped(self, filter, CatalogReadScope::Resolvable).await
    }

    async fn list_page_scoped(
        &self,
        filter: SnapshotListFilter,
        scope: CatalogReadScope,
    ) -> RepositoryResult<SnapshotListPage> {
        PostgresSnapshotCatalog::list_page_scoped(self, filter, scope).await
    }

    async fn delete_record(&self, record: &SnapshotRecord) -> RepositoryResult<()> {
        metrics::record_catalog_outcome(
            "delete_record",
            writes::delete_record(&self.pool, self.cluster_id, record).await,
        )
    }

    async fn resolve_alias(&self, alias: &str) -> RepositoryResult<Option<SnapshotId>> {
        PostgresSnapshotCatalog::resolve_alias_scoped(self, alias, CatalogReadScope::Resolvable)
            .await
    }

    async fn resolve_alias_scoped(
        &self,
        alias: &str,
        scope: CatalogReadScope,
    ) -> RepositoryResult<Option<SnapshotId>> {
        PostgresSnapshotCatalog::resolve_alias_scoped(self, alias, scope).await
    }

    async fn try_start_build(&self, id: &SnapshotId) -> RepositoryResult<StartedBuild> {
        metrics::record_catalog_outcome(
            "try_start_build",
            writes::try_start_build(
                &self.pool,
                self.cluster_id,
                &self.node_id,
                id,
                self.max_concurrent_builds,
            )
            .await,
        )
    }

    async fn renew_build_lease(&self, build_id: &SnapshotId) -> RepositoryResult<bool> {
        metrics::record_catalog_outcome(
            "renew_build_lease",
            writes::renew_lease(&self.pool, self.cluster_id, &self.node_id, build_id).await,
        )
    }

    async fn mark_build_error(
        &self,
        id: &SnapshotId,
        reason: TemplateBuildErrorReason,
    ) -> RepositoryResult<()> {
        metrics::record_catalog_outcome(
            "mark_build_error",
            writes::mark_build_error(&self.pool, self.cluster_id, id, reason).await,
        )
    }
}

/// Builds the production PostgreSQL catalog from the process pool and config.
pub fn pg_snapshot_catalog(config: &AppConfig, pool: &PgPool) -> Arc<dyn SnapshotCatalog> {
    let identity = crate::identity::NodeIdentity::from_config(&config.node_identity);
    let catalog = Arc::new(
        PostgresSnapshotCatalog::new(pool.clone(), identity.cluster_id, identity.id)
            .with_max_concurrent_builds(config.snapshot.catalog.max_concurrent_builds),
    );
    if config.snapshot.catalog.max_concurrent_builds < 0 {
        // Negative explicitly disables the cluster-wide build ceiling.
        tracing::warn!(
            target: "agentenv",
            max_concurrent_builds = config.snapshot.catalog.max_concurrent_builds,
            "snapshot catalog build queue has no cluster-wide ceiling: nothing but the \
             fleet's capacity limits how many builds run at once"
        );
    }

    catalog as Arc<dyn SnapshotCatalog>
}

/// Starts the optional catalog-build-reaper singleton.
///
/// Shut down the returned handle asynchronously to release its advisory lock.
pub fn spawn_catalog_build_reaper(
    pool: Option<sqlx::PgPool>,
    cluster_id: uuid::Uuid,
    interval: std::time::Duration,
    ttl: std::time::Duration,
) -> Option<crate::pg::SingletonTaskHandle> {
    reaper::spawn(pool?, cluster_id, interval, ttl)
}

/// Migrates the catalog schema before any catalog or reaper uses the pool.
///
/// Idempotent concurrent calls serialize on the schema advisory lock.
pub async fn migrate_catalog_schema(pool: &sqlx::PgPool) -> Result<()> {
    migrate::migrate(pool).await
}

#[cfg(test)]
mod pg {
    use super::*;
    use crate::pg::harness::isolated_schema_pool_or_skip;
    use crate::snapshot::repository::interfaces::SnapshotListFilter;
    use crate::snapshot::repository::RepositoryError;
    use crate::snapshot::types::{
        CommittedSnapshot, SnapshotAlias, SnapshotPublishSource, SnapshotSourceKind,
    };
    use crate::types::SandboxResources;
    use migrate::migrate;

    macro_rules! catalog {
        ($test:literal) => {{
            let pool = isolated_schema_pool_or_skip!($test);
            migrate(&pool).await.expect("migration should succeed");
            PostgresSnapshotCatalog::new(pool, Uuid::new_v4(), "node-a".to_string())
        }};
    }

    fn resources() -> SandboxResources {
        SandboxResources {
            cpu_count: 1,
            memory_mib: 512,
            disk_size_mib: 1024,
        }
    }

    fn template_record(alias: Option<&str>) -> SnapshotRecord {
        SnapshotRecord::template_waiting(
            SnapshotId::generate(),
            alias.map(|a| SnapshotAlias::parse(a).unwrap()),
            resources(),
        )
    }

    fn commit_for(id: SnapshotId, alias: Option<&str>) -> SnapshotCommit {
        SnapshotCommit {
            id,
            alias: alias.map(|a| SnapshotAlias::parse(a).unwrap()),
            source: SnapshotPublishSource::Template,
            resources: resources(),
            created_at_unix_ms: None,
            committed: CommittedSnapshot::mock(),
        }
    }

    #[tokio::test]
    async fn a_created_template_is_waiting_and_readable_only_at_any_status() {
        let catalog = catalog!("a_created_template_is_waiting_and_readable_only_at_any_status");
        let record = template_record(Some("my-template"));

        let created = catalog
            .create(record.clone())
            .await
            .expect("create should succeed");
        assert_eq!(created.id, record.id);

        assert!(
            catalog.get(&record.id.to_string()).await.unwrap().is_none(),
            "a waiting template is not resolvable"
        );
        let any_status = catalog
            .get_scoped(&record.id.to_string(), CatalogReadScope::AnyStatus)
            .await
            .unwrap()
            .expect("a waiting template is visible at AnyStatus");
        assert_eq!(any_status.id, record.id);
    }

    #[tokio::test]
    async fn creating_the_same_id_twice_is_refused() {
        let catalog = catalog!("creating_the_same_id_twice_is_refused");
        let record = template_record(None);
        catalog
            .create(record.clone())
            .await
            .expect("first create should succeed");
        let error = catalog
            .create(record)
            .await
            .expect_err("a duplicate id must be refused");
        assert!(format!("{error:#}").to_lowercase().contains("refused"));
    }

    #[tokio::test]
    async fn a_taken_alias_refuses_the_create_and_names_the_holder() {
        let catalog = catalog!("a_taken_alias_refuses_the_create_and_names_the_holder");
        let first = template_record(Some("shared-name"));
        catalog
            .create(first.clone())
            .await
            .expect("first create should succeed");

        let second = template_record(Some("shared-name"));
        let error = catalog
            .create(second)
            .await
            .expect_err("a second snapshot may not take an alias already held");
        match error {
            RepositoryError::AliasConflict { existing, .. } => assert_eq!(existing, first.id),
            other => panic!("expected AliasConflict, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn publish_commit_opens_and_flips_a_row_in_one_call() {
        let catalog = catalog!("publish_commit_opens_and_flips_a_row_in_one_call");
        let id = SnapshotId::generate();
        let commit = commit_for(id.clone(), Some("ready-template"));

        let published = catalog
            .publish_commit(commit)
            .await
            .expect("publish_commit should succeed");
        assert_eq!(published.id, id);
        assert!(published.committed.is_some());

        let resolvable = catalog
            .get(&id.to_string())
            .await
            .unwrap()
            .expect("a ready row is resolvable");
        assert_eq!(resolvable.id, id);
        assert_eq!(
            catalog.resolve_alias("ready-template").await.unwrap(),
            Some(id)
        );
    }

    #[tokio::test]
    async fn publish_commit_of_a_sandbox_snapshot_reports_its_source_correctly() {
        let catalog = catalog!("publish_commit_of_a_sandbox_snapshot_reports_its_source_correctly");
        let commit = SnapshotCommit {
            id: SnapshotId::generate(),
            alias: None,
            source: SnapshotPublishSource::Sandbox {
                source_sandbox_id: "sbx-42".to_string(),
            },
            resources: resources(),
            created_at_unix_ms: None,
            committed: CommittedSnapshot::mock(),
        };

        let published = catalog
            .publish_commit(commit)
            .await
            .expect("publish_commit should succeed");
        match published.source {
            crate::snapshot::types::SnapshotSource::Sandbox { source_sandbox_id } => {
                assert_eq!(source_sandbox_id, "sbx-42");
            }
            other => panic!(
                "publish_commit's own return value must report the sandbox source, not \
                 fabricate a template: {other:?}"
            ),
        }
    }

    #[tokio::test]
    async fn publish_commit_over_an_existing_template_row_reuses_it() {
        let catalog = catalog!("publish_commit_over_an_existing_template_row_reuses_it");
        let record = template_record(None);
        catalog
            .create(record.clone())
            .await
            .expect("create should succeed");
        catalog
            .try_start_build(&record.id)
            .await
            .expect("starting the build should succeed");

        let commit = commit_for(record.id.clone(), None);
        let published = catalog
            .publish_commit(commit)
            .await
            .expect("publishing over an existing building row should succeed");
        assert_eq!(published.id, record.id);
    }

    #[tokio::test]
    async fn publish_commit_rebinding_its_own_alias_is_idempotent_not_alias_taken() {
        let catalog =
            catalog!("publish_commit_rebinding_its_own_alias_is_idempotent_not_alias_taken");
        let record = template_record(Some("p4-acc-builder"));
        catalog
            .create(record.clone())
            .await
            .expect("create should succeed, binding the alias to the new template's own id");
        catalog
            .try_start_build(&record.id)
            .await
            .expect("starting the build should succeed");

        let commit = commit_for(record.id.clone(), Some("p4-acc-builder"));
        let published = catalog.publish_commit(commit).await.expect(
            "rebinding the alias this exact snapshot already holds must succeed, not AliasTaken",
        );
        assert_eq!(published.id, record.id);
        assert_eq!(
            catalog.resolve_alias("p4-acc-builder").await.unwrap(),
            Some(record.id)
        );
    }

    #[tokio::test]
    async fn publish_commit_refuses_to_steal_an_alias_held_by_a_different_snapshot() {
        let catalog =
            catalog!("publish_commit_refuses_to_steal_an_alias_held_by_a_different_snapshot");
        let holder = commit_for(SnapshotId::generate(), Some("shared-build-alias"));
        let holder_id = holder.id.clone();
        catalog
            .publish_commit(holder)
            .await
            .expect("the first publish should succeed and bind the alias");

        let thief = template_record(None);
        catalog
            .create(thief.clone())
            .await
            .expect("opening the second row should succeed -- it claims no alias yet");
        catalog
            .try_start_build(&thief.id)
            .await
            .expect("starting the build should succeed");

        let commit = commit_for(thief.id.clone(), Some("shared-build-alias"));
        let error = catalog
            .publish_commit(commit)
            .await
            .expect_err("a different snapshot must not be able to steal a live alias");
        match error {
            RepositoryError::AliasConflict { existing, .. } => assert_eq!(existing, holder_id),
            other => panic!("expected AliasConflict naming the real holder, got {other:?}"),
        }
        assert_eq!(
            catalog.resolve_alias("shared-build-alias").await.unwrap(),
            Some(holder_id),
            "the refused attempt must not have displaced the original holder"
        );
        assert!(
            catalog.get(&thief.id.to_string()).await.unwrap().is_none(),
            "a commit refused at alias binding must roll its whole transaction back, \
             including the flip to 'ready' that ran before the bind"
        );
    }

    #[tokio::test]
    async fn a_disk_size_of_zero_is_allowed_while_waiting_but_refused_at_ready() {
        let catalog = catalog!("a_disk_size_of_zero_is_allowed_while_waiting_but_refused_at_ready");
        let mut record = template_record(None);
        record.resources.disk_size_mib = 0;
        catalog
            .create(record.clone())
            .await
            .expect("a v3 template with no known disk size yet must be creatable");
        catalog
            .try_start_build(&record.id)
            .await
            .expect("starting the build should succeed with disk size still unknown");

        let mut commit = commit_for(record.id.clone(), None);
        commit.resources.disk_size_mib = 0;
        let error = catalog
            .publish_commit(commit)
            .await
            .expect_err("a ready row must know its disk size");
        let _ = error;

        let mut commit = commit_for(record.id, None);
        commit.resources.disk_size_mib = 2048;
        catalog
            .publish_commit(commit)
            .await
            .expect("a real disk size at commit time must succeed");
    }

    #[tokio::test]
    async fn deleting_drops_the_row_and_its_alias() {
        let catalog = catalog!("deleting_drops_the_row_and_its_alias");
        let commit = commit_for(SnapshotId::generate(), Some("to-delete"));
        let record = catalog
            .publish_commit(commit)
            .await
            .expect("publish should succeed");

        catalog
            .delete_record(&record)
            .await
            .expect("delete should succeed");
        assert!(catalog.get(&record.id.to_string()).await.unwrap().is_none());
        assert!(catalog.resolve_alias("to-delete").await.unwrap().is_none());

        catalog
            .delete_record(&record)
            .await
            .expect("deleting an already-deleted row should still succeed");
    }

    #[tokio::test]
    async fn listing_pages_newest_first_and_the_cursor_walks_every_row_once() {
        let catalog = catalog!("listing_pages_newest_first_and_the_cursor_walks_every_row_once");
        let mut ids = Vec::new();
        for _ in 0..5 {
            let commit = commit_for(SnapshotId::generate(), None);
            let record = catalog
                .publish_commit(commit)
                .await
                .expect("publish should succeed");
            ids.push(record.id);
            // Distinct timestamps avoid depending on ID tie-breaking.
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }

        let mut seen = Vec::new();
        let mut filter = SnapshotListFilter {
            limit: Some(2),
            ..SnapshotListFilter::default()
        };
        loop {
            let page = catalog
                .list_page(filter.clone())
                .await
                .expect("listing a page should succeed");
            seen.extend(page.items.into_iter().map(|record| record.id));
            match page.next {
                Some(cursor) => filter.cursor = Some(cursor),
                None => break,
            }
        }

        assert_eq!(
            seen.len(),
            5,
            "every row must appear exactly once across pages"
        );
        let mut expected = ids.clone();
        expected.reverse(); // newest first
        assert_eq!(seen, expected);
    }

    #[tokio::test]
    async fn a_listing_with_no_limit_returns_the_default_page() {
        let catalog = catalog!("a_listing_with_no_limit_returns_the_default_page");
        for _ in 0..3 {
            let commit = commit_for(SnapshotId::generate(), None);
            catalog
                .publish_commit(commit)
                .await
                .expect("publish should succeed");
        }

        assert!(SnapshotListFilter::default().limit.is_none());
        let page = catalog
            .list_page(SnapshotListFilter::default())
            .await
            .expect("listing should succeed");
        assert_eq!(page.items.len(), 3);
        assert!(
            page.next.is_none(),
            "three rows fit inside the default page, so the walk ends here"
        );
    }

    #[tokio::test]
    async fn filtering_by_source_kind_excludes_the_other_kind() {
        let catalog = catalog!("filtering_by_source_kind_excludes_the_other_kind");
        let template_commit = commit_for(SnapshotId::generate(), None);
        catalog
            .publish_commit(template_commit)
            .await
            .expect("publishing the template should succeed");

        let sandbox_commit = SnapshotCommit {
            id: SnapshotId::generate(),
            alias: None,
            source: SnapshotPublishSource::Sandbox {
                source_sandbox_id: "sbx-1".to_string(),
            },
            resources: resources(),
            created_at_unix_ms: None,
            committed: CommittedSnapshot::mock(),
        };
        catalog
            .publish_commit(sandbox_commit)
            .await
            .expect("publishing the sandbox snapshot should succeed");

        let templates_only = catalog
            .list_page(SnapshotListFilter {
                sources: Some(vec![SnapshotSourceKind::Template]),
                ..SnapshotListFilter::default()
            })
            .await
            .expect("listing should succeed")
            .items;
        assert_eq!(templates_only.len(), 1);
        assert!(matches!(
            templates_only[0].source,
            crate::snapshot::types::SnapshotSource::Template { .. }
        ));
    }

    #[tokio::test]
    async fn a_second_build_of_the_same_template_is_refused_while_one_is_active() {
        let catalog =
            catalog!("a_second_build_of_the_same_template_is_refused_while_one_is_active");
        let record = template_record(None);
        catalog
            .create(record.clone())
            .await
            .expect("create should succeed");

        catalog
            .try_start_build(&record.id)
            .await
            .expect("the first build should be admitted");

        let error = catalog
            .try_start_build(&record.id)
            .await
            .expect_err("a template already building must refuse a second build");
        assert!(matches!(error, RepositoryError::InvalidRequest { .. }));
    }

    #[tokio::test]
    async fn two_concurrent_admissions_for_the_same_template_leave_only_one_winner() {
        let catalog =
            catalog!("two_concurrent_admissions_for_the_same_template_leave_only_one_winner");
        let record = template_record(None);
        catalog
            .create(record.clone())
            .await
            .expect("create should succeed");

        let (first, second) = tokio::join!(
            catalog.try_start_build(&record.id),
            catalog.try_start_build(&record.id)
        );

        let outcomes = [first.is_ok(), second.is_ok()];
        assert_eq!(
            outcomes.iter().filter(|ok| **ok).count(),
            1,
            "exactly one of two concurrent admissions for the same template must win: {outcomes:?}"
        );

        let loser = if first.is_ok() { second } else { first };
        assert!(matches!(
            loser.unwrap_err(),
            RepositoryError::InvalidRequest { .. }
        ));

        let active: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM builds WHERE template_id = $1 AND status_group IN ('pending', 'in_progress')",
        )
        .bind(record.id.to_uuid())
        .fetch_one(&catalog.pool)
        .await
        .expect("counting active builds should succeed");
        assert_eq!(
            active, 1,
            "the table itself must hold exactly one active build row"
        );
    }

    #[tokio::test]
    async fn a_retry_after_a_failed_build_is_the_ordinary_case() {
        let catalog = catalog!("a_retry_after_a_failed_build_is_the_ordinary_case");
        let record = template_record(None);
        catalog
            .create(record.clone())
            .await
            .expect("create should succeed");

        catalog
            .try_start_build(&record.id)
            .await
            .expect("the first build should be admitted");
        catalog
            .mark_build_error(
                &record.id,
                TemplateBuildErrorReason::new("the build failed on purpose"),
            )
            .await
            .expect("failing the build should succeed");

        catalog
            .try_start_build(&record.id)
            .await
            .expect("a retry of a failed build is the ordinary case, not an exception");
    }

    #[tokio::test]
    async fn the_cluster_wide_build_ceiling_refuses_once_it_is_reached() {
        let pool = isolated_schema_pool_or_skip!(
            "the_cluster_wide_build_ceiling_refuses_once_it_is_reached"
        );
        migrate(&pool).await.expect("migration should succeed");
        let cluster_id = Uuid::new_v4();
        let catalog = PostgresSnapshotCatalog::new(pool, cluster_id, "node-a".to_string())
            .with_max_concurrent_builds(1);

        let first = template_record(None);
        catalog
            .create(first.clone())
            .await
            .expect("create should succeed");
        catalog
            .try_start_build(&first.id)
            .await
            .expect("the first build should be admitted under the ceiling");

        let second = template_record(None);
        catalog
            .create(second.clone())
            .await
            .expect("create should succeed");
        let error = catalog
            .try_start_build(&second.id)
            .await
            .expect_err("a cluster already at its ceiling must refuse a second template's build");
        assert!(matches!(error, RepositoryError::InvalidRequest { .. }));
    }

    #[tokio::test]
    async fn zero_resolves_to_the_default_ceiling_not_to_unlimited() {
        let pool =
            isolated_schema_pool_or_skip!("zero_resolves_to_the_default_ceiling_not_to_unlimited");
        migrate(&pool).await.expect("migration should succeed");
        let cluster_id = Uuid::new_v4();
        let catalog = PostgresSnapshotCatalog::new(pool, cluster_id, "node-a".to_string())
            .with_max_concurrent_builds(0);

        for n in 0..DEFAULT_MAX_CONCURRENT_BUILDS {
            let record = template_record(None);
            catalog
                .create(record.clone())
                .await
                .unwrap_or_else(|e| panic!("create #{n} should succeed: {e}"));
            catalog
                .try_start_build(&record.id)
                .await
                .unwrap_or_else(|e| {
                    panic!("build #{n} should be admitted under the default ceiling: {e}")
                });
        }

        let one_more = template_record(None);
        catalog
            .create(one_more.clone())
            .await
            .expect("create should succeed");
        let error = catalog.try_start_build(&one_more.id).await.expect_err(
            "the 21st build must be refused: 0 resolves to a ceiling of 20, not unlimited",
        );
        assert!(matches!(error, RepositoryError::InvalidRequest { .. }));
    }

    #[tokio::test]
    async fn a_negative_ceiling_removes_it_entirely() {
        let pool = isolated_schema_pool_or_skip!("a_negative_ceiling_removes_it_entirely");
        migrate(&pool).await.expect("migration should succeed");
        let cluster_id = Uuid::new_v4();
        let catalog = PostgresSnapshotCatalog::new(pool, cluster_id, "node-a".to_string())
            .with_max_concurrent_builds(-1);

        for n in 0..(DEFAULT_MAX_CONCURRENT_BUILDS + 2) {
            let record = template_record(None);
            catalog
                .create(record.clone())
                .await
                .unwrap_or_else(|e| panic!("create #{n} should succeed: {e}"));
            catalog
                .try_start_build(&record.id)
                .await
                .unwrap_or_else(|e| {
                    panic!(
                    "build #{n} should be admitted: a negative ceiling must not refuse anything, \
                     even past where 0's own default would have: {e}"
                )
                });
        }
    }

    #[tokio::test]
    async fn renewing_a_lease_from_the_wrong_node_fails_and_the_right_node_succeeds() {
        let catalog =
            catalog!("renewing_a_lease_from_the_wrong_node_fails_and_the_right_node_succeeds");
        let record = template_record(None);
        catalog
            .create(record.clone())
            .await
            .expect("create should succeed");
        let started = catalog
            .try_start_build(&record.id)
            .await
            .expect("the build should be admitted");

        assert!(
            !writes::renew_lease(
                &catalog.pool,
                catalog.cluster_id,
                "some-other-node",
                &started.build_id
            )
            .await
            .expect("renewing should not error"),
            "a node that did not admit the build must not be able to renew its lease"
        );
        assert!(
            SnapshotCatalog::renew_build_lease(&catalog, &started.build_id)
                .await
                .unwrap(),
            "the admitting node's own renewal must succeed"
        );
    }

    #[tokio::test]
    async fn committing_a_row_that_is_not_building_is_refused() {
        let catalog = catalog!("committing_a_row_that_is_not_building_is_refused");
        let id = SnapshotId::generate();
        let commit = commit_for(id.clone(), None);
        catalog
            .publish_commit(commit)
            .await
            .expect("first publish should succeed");

        let second_commit = commit_for(id, None);
        let error = catalog
            .publish_commit(second_commit)
            .await
            .expect_err("committing an already-ready row must be refused");
        let _ = error;
    }

    #[test]
    fn a_call_and_a_rejection_are_both_recorded() {
        use metrics_util::debugging::DebuggingRecorder;

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a runtime should build");
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        // Qualify the crate root because this module shadows `metrics`.
        ::metrics::with_local_recorder(&recorder, || {
            runtime.block_on(async {
                let pool =
                    isolated_schema_pool_or_skip!("a_call_and_a_rejection_are_both_recorded");
                migrate(&pool).await.expect("migration should succeed");
                let catalog =
                    PostgresSnapshotCatalog::new(pool, Uuid::new_v4(), "node-a".to_string());

                let record = template_record(None);
                catalog
                    .create(record.clone())
                    .await
                    .expect("create should succeed");
                catalog
                    .try_start_build(&record.id)
                    .await
                    .expect("the first build should be admitted");
                catalog
                    .try_start_build(&record.id)
                    .await
                    .expect_err("a second build of the same template must be refused");
            });
        });

        // Snapshot once because reading drains the recorder.
        let sample = snapshotter.snapshot().into_vec();
        let total_of = |name: &str| -> u64 {
            sample
                .iter()
                .filter(|(composite, _, _, _)| composite.key().name() == name)
                .map(|(_, _, _, value)| match value {
                    metrics_util::debugging::DebugValue::Counter(count) => *count,
                    _ => 0,
                })
                .sum()
        };

        assert!(
            total_of(metrics::CATALOG_RPC_TOTAL) >= 3,
            "every SnapshotCatalog call this test made — one create, two try_start_build — must \
             record agentenv_scheduler_catalog_rpc_total"
        );
        assert!(
            total_of(metrics::CATALOG_REJECTED_TOTAL) >= 1,
            "the second try_start_build was refused as an ordinary admission decision, not a \
             backend failure, and must record agentenv_scheduler_catalog_rejected_total"
        );
    }
}
