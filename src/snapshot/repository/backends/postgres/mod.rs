//! `PostgresSnapshotCatalog`: a `SnapshotCatalog` implementation that talks to
//! PostgreSQL directly, in-process — the Stage B replacement for
//! [`super::central::CentralSnapshotCatalog`]'s gRPC hop to
//! `services/scheduler`.
//!
//! 🔴 `--role node` must never hold one of these — see `src/pg/mod.rs`'s own
//! module doc and `crate::role::ServerRole::check_pg_dsn`, which refuses
//! `--role node` startup outright if `[pg].dsn` is configured at all.
//!
//! Wired into `build_snapshot_backend` (`backends/mod.rs::build_central_catalog`):
//! whenever a `[pg]` pool is available, this type stands in for
//! [`super::central::CentralSnapshotCatalog`] as the "central" side of
//! `write = "both"` and `write = "postgres"`, via the [`CentralCatalogWrites`]
//! impl below.

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

use crate::snapshot::repository::backends::central::CatalogWrite;
use crate::snapshot::repository::interfaces::{
    CatalogReadScope, SnapshotCatalog, SnapshotCommit, SnapshotListFilter, SnapshotListPage,
    StartedBuild,
};
use crate::snapshot::repository::mirror::{CatalogCensus, CentralCatalogWrites};
use crate::snapshot::repository::RepositoryResult;
use crate::snapshot::types::{SnapshotId, SnapshotRecord, TemplateBuildErrorReason};

/// Cluster-wide concurrent-build ceiling `PostgresSnapshotCatalog::new`
/// starts at, and what an explicit `0` passed to
/// [`PostgresSnapshotCatalog::with_max_concurrent_builds`] resolves to —
/// mirrors Go's own default (`defaultMaxConcurrentBuilds`,
/// `store_postgres.go:27`, re-read from `config.snapshot.catalog` at
/// `build_central_catalog`'s call site). A *negative* value removes the
/// ceiling entirely (see `with_max_concurrent_builds`'s own doc); `0` is
/// never unlimited on either side.
pub(crate) const DEFAULT_MAX_CONCURRENT_BUILDS: i32 = 20;

/// A `SnapshotCatalog` backed by a direct, in-process connection pool to the
/// shared control-plane PostgreSQL database, rather than an RPC hop to
/// `services/scheduler`.
pub(crate) struct PostgresSnapshotCatalog {
    pool: PgPool,
    cluster_id: Uuid,
    node_id: String,
    max_concurrent_builds: i32,
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

    /// Overrides the cluster-wide build ceiling `new` started at — the
    /// production caller is `build_central_catalog`
    /// (`backends/mod.rs`), which passes
    /// `config.snapshot.catalog.max_concurrent_builds` here on every
    /// `PostgresSnapshotCatalog` it builds; tests call it directly to drive
    /// admission at a ceiling other than the default.
    ///
    /// Mirrors Go's own resolution in `NewStoreWithPool`
    /// (`store_postgres.go:134-137`): `0` takes [`DEFAULT_MAX_CONCURRENT_BUILDS`]
    /// — matching a config that was left unset, not "no ceiling" — while any
    /// other value, including negative, passes straight through. A negative
    /// value is what actually removes the ceiling: `writes::start_build`
    /// only takes the advisory lock and runs the cluster-wide `count(*)`
    /// when `max_concurrent_builds > 0` (`store_postgres.go:809`'s
    /// `if s.maxConcurrentBuilds > 0`), so 0 and a positive number are both
    /// enforced and only a negative number skips the check (and its cost)
    /// entirely.
    pub(crate) fn with_max_concurrent_builds(mut self, max: i32) -> Self {
        self.max_concurrent_builds = if max == 0 {
            DEFAULT_MAX_CONCURRENT_BUILDS
        } else {
            max
        };
        self
    }

    pub(crate) async fn get_scoped(
        &self,
        id_or_alias: &str,
        scope: CatalogReadScope,
    ) -> RepositoryResult<Option<SnapshotRecord>> {
        metrics::record_catalog_outcome(
            "get_scoped",
            reads::get_scoped(&self.pool, self.cluster_id, id_or_alias, scope).await,
        )
    }

    pub(crate) async fn resolve_alias_scoped(
        &self,
        alias: &str,
        scope: CatalogReadScope,
    ) -> RepositoryResult<Option<SnapshotId>> {
        metrics::record_catalog_outcome(
            "resolve_alias_scoped",
            reads::resolve_alias_scoped(&self.pool, self.cluster_id, alias, scope).await,
        )
    }

    pub(crate) async fn list_scoped(
        &self,
        filter: SnapshotListFilter,
        scope: CatalogReadScope,
    ) -> RepositoryResult<Vec<SnapshotRecord>> {
        metrics::record_catalog_outcome(
            "list_scoped",
            reads::list_scoped(&self.pool, self.cluster_id, &filter, scope).await,
        )
    }

    pub(crate) async fn list_page_scoped(
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

// 🔴 P6: `metrics::record_catalog_outcome` is wired here, on the
// `SnapshotCatalog` trait surface only — this is the *sole* path a
// `write = "postgres"` deployment ever calls (`assemble_postgres_only_backend`
// wires `Arc<dyn SnapshotCatalog>` straight into the repository, with no
// mirror in front of it) and the read half of every `write = "both"`
// deployment too. `CentralCatalogWrites` below — the double write's own
// surface, live during the `write = "both"` observation window — is left
// unwired: its methods answer `RepositoryResult<CatalogWrite<T>>`, so a
// refusal is `Ok(CatalogWrite::Refused(_))`, not `Err(_)`, and
// `record_catalog_outcome`'s `Result`-shaped wrapper cannot see it as a
// rejection without unwrapping that enum too — left for whoever wires that
// surface next rather than guessed at here.
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

/// The central catalog's census asks at the *any status* scope — see
/// [`CatalogCensus`]'s own doc on [`super::central::CentralSnapshotCatalog`],
/// which this mirrors exactly: a `waiting` template must be counted here the
/// same as it is counted in object storage, or the population comparison
/// that guards the read-side switch refuses every cluster that has ever
/// built one.
#[async_trait]
impl CatalogCensus for PostgresSnapshotCatalog {
    async fn every_snapshot_id(&self) -> RepositoryResult<Vec<SnapshotId>> {
        Ok(self
            .list_scoped(
                SnapshotListFilter::matches_all(),
                CatalogReadScope::AnyStatus,
            )
            .await?
            .into_iter()
            .map(|record| record.id)
            .collect())
    }
}

/// What the double write needs from the central catalog — see
/// [`CentralCatalogWrites`]'s own doc and
/// `crate::snapshot::repository::mirror::central`'s identical impl for
/// [`super::central::CentralSnapshotCatalog`], which this mirrors call for
/// call. The difference is only how each call reaches PostgreSQL: a direct
/// query here, a gRPC hop to `services/scheduler` there.
#[async_trait]
impl CentralCatalogWrites for PostgresSnapshotCatalog {
    async fn begin(
        &self,
        record: &SnapshotRecord,
        status: &str,
        published: bool,
    ) -> RepositoryResult<CatalogWrite<SnapshotRecord>> {
        writes::begin_snapshot(
            &self.pool,
            self.cluster_id,
            &self.node_id,
            record,
            status,
            published,
        )
        .await
    }

    async fn commit(
        &self,
        commit: &SnapshotCommit,
        published: bool,
        updated_at_unix_ms: i64,
    ) -> RepositoryResult<CatalogWrite<SnapshotRecord>> {
        writes::commit(
            &self.pool,
            self.cluster_id,
            &self.node_id,
            commit,
            published,
            updated_at_unix_ms,
        )
        .await
    }

    async fn start_build(
        &self,
        id: &SnapshotId,
        build_id: &SnapshotId,
        started_at_unix_ms: i64,
    ) -> RepositoryResult<CatalogWrite<StartedBuild>> {
        writes::start_build(
            &self.pool,
            self.cluster_id,
            &self.node_id,
            id,
            build_id,
            self.max_concurrent_builds,
            started_at_unix_ms,
        )
        .await
    }

    async fn renew_build_lease(&self, build_id: &SnapshotId) -> RepositoryResult<bool> {
        writes::renew_lease(&self.pool, self.cluster_id, &self.node_id, build_id).await
    }

    async fn fail(
        &self,
        id: &SnapshotId,
        reason: &TemplateBuildErrorReason,
        updated_at_unix_ms: i64,
    ) -> RepositoryResult<CatalogWrite<SnapshotRecord>> {
        writes::fail(&self.pool, self.cluster_id, id, reason, updated_at_unix_ms).await
    }

    async fn delete(&self, id_or_alias: &str, deleted_at_unix_ms: i64) -> RepositoryResult<bool> {
        writes::delete(&self.pool, self.cluster_id, id_or_alias, deleted_at_unix_ms).await
    }

    async fn get_any_status(&self, id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>> {
        self.get_scoped(id_or_alias, CatalogReadScope::AnyStatus)
            .await
    }

    async fn get_resolvable(&self, id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>> {
        self.get_scoped(id_or_alias, CatalogReadScope::Resolvable)
            .await
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

    /// Regression: `commit_snapshot`'s in-memory return value used to
    /// hardcode `SnapshotSource::Template` regardless of what was actually
    /// committed, so a sandbox (pause) commit's immediate return value would
    /// silently claim to be a template and lose `source_sandbox_id` — even
    /// though the row written to the database was always correct (`reads.rs`
    /// decodes it right back). Any caller trusting `publish_commit`'s return
    /// value directly, rather than re-reading the row, would have seen the
    /// wrong thing.
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

    /// Regression for the v3 template build path failing with `AliasTaken`
    /// against its *own* id every time: `POST /v3/templates` pre-binds the
    /// alias to the new template's id via `create` (`begin_snapshot` ->
    /// `bind_alias`), and the build's own `publish_commit` later binds the
    /// same alias again onto the same id via `commit_snapshot` ->
    /// `bind_alias`. That second bind must be a no-op, not a conflict --
    /// `bind_alias`'s `INSERT ... ON CONFLICT DO NOTHING` was unconditionally
    /// reporting `AliasTaken` on any conflict, including one where the
    /// row already found by `SELECT snapshot_id` is this exact snapshot.
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

    /// The other side of the idempotence fix above: the branch must recognise
    /// *this* snapshot, not accept anyone. A different snapshot reaching for a
    /// name someone else still holds must still be refused, must still name
    /// the real holder, and must leave both rows where they were.
    ///
    /// Deliberately staged so the steal is attempted by `commit_snapshot`'s
    /// `bind_alias` rather than `begin_snapshot`'s: `publish_commit` over a
    /// row that already exists never reaches the opening bind (the id insert
    /// conflicts first and `AlreadyExists` is swallowed), so the commit-time
    /// bind is the only one that runs -- and it is the one whose refusal has
    /// to take the flip to `ready` back out with it.
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

    /// The concurrent form of the test above, and the one that actually
    /// exercises `builds_one_active_per_template` rather than the
    /// `pg_advisory_xact_lock` serializing two sequential calls that would
    /// have been refused anyway. Two `try_start_build` calls launched at the
    /// same instant simulate two `--role api` replicas racing to admit the
    /// same template's build — under READ COMMITTED, a read-modify-write
    /// implementation (SELECT the active-build count, then INSERT) would let
    /// both through, which is exactly the defect `queries_admin.go`'s
    /// `buildAdmissionKey` comment explains the advisory lock exists to
    /// close. Only one of the two calls here may succeed.
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

        // And the loser's own error is the ordinary in-progress refusal, not
        // some other failure mode (a raw unique-violation leaking through,
        // for instance).
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

    /// 🔴 Regression for the exact defect P1 fixes: an explicit `0` used to
    /// mean "no ceiling" (`writes.rs`'s old `max_concurrent_builds > 0 &&`
    /// guard never fired for `0`), which silently dropped Go's cluster-wide
    /// ceiling the moment `write = "both"`/`"postgres"` came up with no
    /// override. `with_max_concurrent_builds(0)` must resolve to exactly
    /// [`DEFAULT_MAX_CONCURRENT_BUILDS`] (20, matching Go's own default) —
    /// admitting the 20th build and refusing the 21st proves both halves at
    /// once: `0` is *bounded* (not unlimited) and bounded at the *right*
    /// number, not some other finite one.
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

    /// The other half of the same proof: a *negative* ceiling — not `0` — is
    /// what actually removes the check, past the point `0`'s own default
    /// would have refused at.
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

    // ── metrics (P6) ─────────────────────────────────────────────────────

    /// 🔴 `agentenv_scheduler_catalog_rpc_total` and
    /// `agentenv_scheduler_catalog_rejected_total` were declared and
    /// entirely unwired before this — see `metrics.rs`'s own former "not
    /// wired to anything yet" note. This drives the `SnapshotCatalog` trait
    /// surface (not `#[tokio::test]`: `metrics::with_local_recorder` is
    /// thread-local, so the driven calls have to run on the very thread that
    /// installed the recorder, which a `#[tokio::test]` runtime does not
    /// guarantee — same reason `composite.rs`'s own metrics tests build
    /// their runtime by hand) and checks both series actually moved: one
    /// ordinary call, and one call admission refuses.
    #[test]
    fn a_call_and_a_rejection_are_both_recorded() {
        use metrics_util::debugging::DebuggingRecorder;

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a runtime should build");
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        // 🔴 `::metrics::`, the crate root — bare `metrics::` in this scope
        // resolves to `postgres::metrics` (this file's own submodule,
        // brought in by `use super::*` above), which shadows the
        // `with_local_recorder` free function the external `metrics` crate
        // exports.
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

        // 🔴 One `snapshot()` call, not two through separate `counter_total`
        // calls: `Snapshotter::snapshot()` drains what it reports —a second
        // call sees zero even though the first genuinely observed the
        // recorded counters — so both totals have to be read off the same
        // snapshot.
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
