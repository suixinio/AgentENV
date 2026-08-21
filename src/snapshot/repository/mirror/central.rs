//! What the double write needs from the central catalog, and nothing else.
//!
//! 🔴 A trait rather than the concrete client, and the reason is testability
//! with resolving power. `publish_commit` makes *two* central calls, `create`
//! and `mark_build_error` and `delete_record` one each, and each of those is a
//! separate guard that can be broken on its own. Against a single real gRPC
//! endpoint the only failure a test can induce is "the whole catalog is
//! unreachable", which breaks every guard at once — so one test covers all four
//! paths and none of them individually. Behind this trait each call can be
//! failed by itself, which is what lets a mutation of one guard fail exactly
//! one test.
//!
//! The surface is deliberately narrow: the five writes the mirror makes, and
//! the two reads it needs to decide whether a replay already landed. Nothing
//! here is a general catalog interface — [`crate::snapshot::repository::interfaces::SnapshotCatalog`]
//! is that — because the double write must be able to see the difference
//! between a refusal and a failure, which `SnapshotCatalog` deliberately
//! flattens.

use async_trait::async_trait;

use crate::snapshot::repository::backends::central::{
    CatalogReadScope, CatalogWrite, CentralSnapshotCatalog,
};
use crate::snapshot::repository::interfaces::SnapshotCommit;
use crate::snapshot::repository::RepositoryResult;
use crate::snapshot::types::{SnapshotId, SnapshotRecord, TemplateBuildErrorReason};

/// The central catalog, as the double write and its compensator use it.
#[async_trait]
pub trait CentralCatalogWrites: Send + Sync {
    /// Opens a row before any bytes exist.
    async fn begin(
        &self,
        record: &SnapshotRecord,
        status: &str,
        published: bool,
    ) -> RepositoryResult<CatalogWrite<SnapshotRecord>>;

    /// Flips a row to `ready`.
    async fn commit(
        &self,
        commit: &SnapshotCommit,
        published: bool,
        updated_at_unix_ms: i64,
    ) -> RepositoryResult<CatalogWrite<SnapshotRecord>>;

    /// Moves a row to `error` with a reason.
    async fn fail(
        &self,
        id: &SnapshotId,
        reason: &TemplateBuildErrorReason,
        updated_at_unix_ms: i64,
    ) -> RepositoryResult<CatalogWrite<SnapshotRecord>>;

    /// Soft-deletes one row. Idempotent.
    async fn delete(&self, id_or_alias: &str, deleted_at_unix_ms: i64) -> RepositoryResult<bool>;

    /// 🔴 Every row, `waiting` and `building` and `error` included.
    ///
    /// This is the probe a replay is judged against, and the rows it has to see
    /// are precisely the ones the resolvable reading hides: a template the
    /// central catalog holds is `waiting`, and a build it failed is `error`. A
    /// probe at the resolvable scope would report both as absent and replay
    /// every one of them forever.
    async fn get_any_status(&self, id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>>;

    /// Only rows a caller may launch from.
    async fn get_resolvable(&self, id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>>;
}

#[async_trait]
impl CentralCatalogWrites for CentralSnapshotCatalog {
    async fn begin(
        &self,
        record: &SnapshotRecord,
        status: &str,
        published: bool,
    ) -> RepositoryResult<CatalogWrite<SnapshotRecord>> {
        self.begin_snapshot(record, status, published).await
    }

    async fn commit(
        &self,
        commit: &SnapshotCommit,
        published: bool,
        updated_at_unix_ms: i64,
    ) -> RepositoryResult<CatalogWrite<SnapshotRecord>> {
        self.commit_snapshot(commit, published, updated_at_unix_ms)
            .await
    }

    async fn fail(
        &self,
        id: &SnapshotId,
        reason: &TemplateBuildErrorReason,
        updated_at_unix_ms: i64,
    ) -> RepositoryResult<CatalogWrite<SnapshotRecord>> {
        self.fail_snapshot(id, reason, updated_at_unix_ms).await
    }

    async fn delete(&self, id_or_alias: &str, deleted_at_unix_ms: i64) -> RepositoryResult<bool> {
        self.delete_snapshot(id_or_alias, deleted_at_unix_ms).await
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
