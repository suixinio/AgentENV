//! `PostgresSnapshotCatalog`: a `SnapshotCatalog` implementation that talks to
//! PostgreSQL directly, in-process — the Stage B replacement for
//! [`super::central::CentralSnapshotCatalog`]'s gRPC hop to
//! `services/scheduler`.
//!
//! 🔴 `--role node` must never hold one of these — see `src/pg/mod.rs`'s own
//! module doc and `crate::role::ServerRole::check_pg_dsn`, which refuses
//! `--role node` startup outright if `[pg].dsn` is configured at all.
//!
//! Not yet wired into `build_snapshot_backend` — see the Stage B report's
//! "not done" list for what step 8/9 (`docs/proposals/_sd-phase4-stageB-catalog.md`
//! §7) still needs: primarily implementing `mirror::CentralCatalogWrites`
//! for this type (the dual-write mirror's raw-write abstraction,
//! `CentralSnapshotCatalog`'s other trait) so it can stand in as the
//! "central" side of `write = "both"`. Every method below is implemented and
//! covered by `pg::` contract tests; only that last wiring step is
//! outstanding, which is why `#![allow(dead_code)]` is still here — nothing
//! constructs one of these outside tests yet.
#![allow(dead_code)]

pub(crate) mod convert;
pub(crate) mod metrics;
pub(crate) mod migrate;
pub(crate) mod migration_state;
pub(crate) mod reads;
pub(crate) mod reaper;
pub(crate) mod writes;

use async_trait::async_trait;
use sqlx::PgPool;
use uuid::Uuid;

use crate::snapshot::repository::interfaces::{
    CatalogReadScope, SnapshotCatalog, SnapshotCommit, SnapshotListFilter, SnapshotListPage,
    StartedBuild,
};
use crate::snapshot::repository::RepositoryResult;
use crate::snapshot::types::{SnapshotId, SnapshotRecord, TemplateBuildErrorReason};

/// Default cluster-wide concurrent-build ceiling when the caller does not
/// override it — mirrors Go's own default
/// (`scheduler.catalog.max_concurrent_builds`); `0` means unlimited, matching
/// `writes.rs::start_build`'s own reading of the value.
pub(crate) const DEFAULT_MAX_CONCURRENT_BUILDS: u32 = 0;

/// A `SnapshotCatalog` backed by a direct, in-process connection pool to the
/// shared control-plane PostgreSQL database, rather than an RPC hop to
/// `services/scheduler`.
pub(crate) struct PostgresSnapshotCatalog {
    pool: PgPool,
    cluster_id: Uuid,
    node_id: String,
    max_concurrent_builds: u32,
}

impl PostgresSnapshotCatalog {
    /// Wraps an already-connected, already-migrated pool. The pool itself is
    /// built by `src/pg::connect` and migrated by
    /// `crate::snapshot::repository::backends::postgres::migrate::migrate` —
    /// this type never dials PostgreSQL or applies schema changes on its
    /// own, the same division `CentralSnapshotCatalog` draws between "how to
    /// reach the database" and "what to do once connected".
    pub(crate) fn new(pool: PgPool, cluster_id: Uuid, node_id: String) -> Self {
        Self {
            pool,
            cluster_id,
            node_id,
            max_concurrent_builds: DEFAULT_MAX_CONCURRENT_BUILDS,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_max_concurrent_builds(mut self, max: u32) -> Self {
        self.max_concurrent_builds = max;
        self
    }

    pub(crate) async fn get_scoped(
        &self,
        id_or_alias: &str,
        scope: CatalogReadScope,
    ) -> RepositoryResult<Option<SnapshotRecord>> {
        reads::get_scoped(&self.pool, self.cluster_id, id_or_alias, scope).await
    }

    pub(crate) async fn resolve_alias_scoped(
        &self,
        alias: &str,
        scope: CatalogReadScope,
    ) -> RepositoryResult<Option<SnapshotId>> {
        reads::resolve_alias_scoped(&self.pool, self.cluster_id, alias, scope).await
    }

    pub(crate) async fn list_scoped(
        &self,
        filter: SnapshotListFilter,
        scope: CatalogReadScope,
    ) -> RepositoryResult<Vec<SnapshotRecord>> {
        reads::list_scoped(&self.pool, self.cluster_id, &filter, scope).await
    }

    pub(crate) async fn list_page_scoped(
        &self,
        filter: SnapshotListFilter,
        scope: CatalogReadScope,
    ) -> RepositoryResult<SnapshotListPage> {
        let (items, next) =
            reads::list_page_scoped(&self.pool, self.cluster_id, &filter, scope).await?;
        Ok(SnapshotListPage { items, next })
    }
}

#[async_trait]
impl SnapshotCatalog for PostgresSnapshotCatalog {
    async fn create(&self, record: SnapshotRecord) -> RepositoryResult<SnapshotRecord> {
        writes::create(&self.pool, self.cluster_id, &self.node_id, record).await
    }

    async fn publish_commit(&self, commit: SnapshotCommit) -> RepositoryResult<SnapshotRecord> {
        writes::publish_commit(&self.pool, self.cluster_id, &self.node_id, commit).await
    }

    /// Resolvable rows only — see [`Self::get_scoped`] for the surface that
    /// must see a `building`/`waiting`/`error` row.
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

    async fn list(&self, filter: SnapshotListFilter) -> RepositoryResult<Vec<SnapshotRecord>> {
        PostgresSnapshotCatalog::list_scoped(self, filter, CatalogReadScope::Resolvable).await
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
        writes::delete_record(&self.pool, self.cluster_id, record).await
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
        writes::try_start_build(
            &self.pool,
            self.cluster_id,
            &self.node_id,
            id,
            self.max_concurrent_builds,
        )
        .await
    }

    async fn renew_build_lease(&self, build_id: &SnapshotId) -> RepositoryResult<bool> {
        writes::renew_lease(&self.pool, self.cluster_id, &self.node_id, build_id).await
    }

    async fn mark_build_error(
        &self,
        id: &SnapshotId,
        reason: TemplateBuildErrorReason,
    ) -> RepositoryResult<()> {
        writes::mark_build_error(&self.pool, self.cluster_id, id, reason).await
    }
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

    /// A fresh, migrated, schema-isolated catalog, or an early `return` out
    /// of the calling test — see [`isolated_schema_pool_or_skip`], which
    /// this expands to. A macro rather than an `async fn` because that macro
    /// itself needs a string *literal* (its skip message), not a value.
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

    // ── create / get / list ────────────────────────────────────────────

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

    // ── publish_commit ──────────────────────────────────────────────────

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
    async fn publish_commit_over_an_existing_template_row_reuses_it() {
        let catalog = catalog!("publish_commit_over_an_existing_template_row_reuses_it");
        let record = template_record(None);
        catalog
            .create(record.clone())
            .await
            .expect("create should succeed");
        // The real flow: create (waiting) -> try_start_build (waiting ->
        // building) -> publish_commit (building -> ready). Without the
        // middle step the row is still `waiting`, and `commit_snapshot`'s own
        // fencing (`WHERE status = 'building'`) correctly refuses it -- that
        // refusal is a different, already-covered case
        // (`committing_a_row_that_is_not_building_is_refused`), not this one.
        catalog
            .try_start_build(&record.id)
            .await
            .expect("starting the build should succeed");

        // A template build commits into the row `create` opened and
        // `try_start_build` moved to `building` -- `begin_snapshot`'s
        // ALREADY_EXISTS must be swallowed, not refused.
        let commit = commit_for(record.id.clone(), None);
        let published = catalog
            .publish_commit(commit)
            .await
            .expect("publishing over an existing building row should succeed");
        assert_eq!(published.id, record.id);
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
        // The refusal surfaces as a backend error carrying the CHECK
        // constraint's own message; this only pins that publishing a
        // zero-sized ready row is rejected, not the exact wording.
        let _ = error;

        // The failed attempt must not have moved the row off `building`, or
        // this second, real-sized commit would itself be fenced out.
        let mut commit = commit_for(record.id, None);
        commit.resources.disk_size_mib = 2048;
        catalog
            .publish_commit(commit)
            .await
            .expect("a real disk size at commit time must succeed");
    }

    // ── delete ───────────────────────────────────────────────────────────

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

        // Idempotent.
        catalog
            .delete_record(&record)
            .await
            .expect("deleting an already-deleted row should still succeed");
    }

    // ── listing / keyset pagination ────────────────────────────────────

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
            // Ensure distinct millisecond timestamps so ordering is
            // unambiguous without relying on id tie-breaking.
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
    async fn the_unbounded_list_ignores_the_limit_and_returns_everything() {
        let catalog = catalog!("the_unbounded_list_ignores_the_limit_and_returns_everything");
        for _ in 0..3 {
            let commit = commit_for(SnapshotId::generate(), None);
            catalog
                .publish_commit(commit)
                .await
                .expect("publish should succeed");
        }

        let all = catalog
            .list(SnapshotListFilter::default())
            .await
            .expect("listing should succeed");
        assert_eq!(all.len(), 3);
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
            .list(SnapshotListFilter {
                sources: Some(vec![SnapshotSourceKind::Template]),
                ..SnapshotListFilter::default()
            })
            .await
            .expect("listing should succeed");
        assert_eq!(templates_only.len(), 1);
        assert!(matches!(
            templates_only[0].source,
            crate::snapshot::types::SnapshotSource::Template { .. }
        ));
    }

    // ── build admission ─────────────────────────────────────────────────

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
            catalog.renew_build_lease(&started.build_id).await.unwrap(),
            "the admitting node's own renewal must succeed"
        );
    }

    // ── fencing ─────────────────────────────────────────────────────────

    #[tokio::test]
    async fn committing_a_row_that_is_not_building_is_refused() {
        let catalog = catalog!("committing_a_row_that_is_not_building_is_refused");
        let id = SnapshotId::generate();
        // Never opened at all -- the commit's own begin_snapshot pre-step
        // opens it as `building`, then the commit itself flips it. Publish
        // it once, then try to publish the exact same id again: the second
        // commit finds a `ready` row, not a `building` one, and must refuse.
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
}
