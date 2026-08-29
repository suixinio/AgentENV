//! The catalog a process that has none installs.
//!
//! # 🔴 Why a refusing implementation rather than `Option<Arc<dyn SnapshotCatalog>>`
//!
//! PostgreSQL is the snapshot catalog, and `[pg]` is `aenv-api`'s alone
//! (`crates/aenv-api/src/pg/mod.rs`'s module doc, and `aenv-node`'s own
//! `refuse_configured_pg_dsn`). So `aenv-node` composes a
//! [`SnapshotRepository`][super::SnapshotRepository] out of an artifact store
//! and *no catalog at all* — it stages bytes and the row is written by the half
//! that owns the database.
//!
//! Making the repository's catalog optional would push that decision into every
//! one of its ~15 delegating methods, at every call site, in a crate both
//! binaries link. So one type says "not here" in the type system's stead, and
//! says it loudly.
//!
//! 🔴 Loudly is the point. Until the catalog moved into PostgreSQL, `aenv-node`
//! carried an object-storage catalog that answered these calls, and after the
//! cutover it answered them out of a store nothing had written since —
//! reporting *absence*, which callers act on by deleting artifacts and refusing
//! resumes. An error is the one answer that cannot be mistaken for "no such
//! snapshot".

use async_trait::async_trait;

use super::interfaces::{
    SnapshotCatalog, SnapshotCommit, SnapshotListFilter, SnapshotListPage, StartedBuild,
};
use super::{RepositoryError, RepositoryResult};
use crate::snapshot::types::{SnapshotId, SnapshotRecord, TemplateBuildErrorReason};

/// Refuses every catalog operation, naming the process that has no catalog.
///
/// Installed by `aenv-node`'s repository assembly and by the durable halves
/// `aenv-api` hands to `build_snapshot_backend`, which then swaps in the
/// PostgreSQL catalog. A call that reaches this type is a call that was routed
/// to the wrong half.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoSnapshotCatalog;

impl NoSnapshotCatalog {
    fn refuse<T>(operation: &'static str) -> RepositoryResult<T> {
        Err(RepositoryError::Unsupported {
            feature: format!(
                "snapshot catalog {operation}: this process holds no snapshot catalog. The \
                 catalog is PostgreSQL and lives in aenv-api, which owns the only [pg] pool; \
                 aenv-node stages artifacts and never writes or reads a catalog row. A request \
                 that needs one has been routed to the wrong half"
            ),
        })
    }
}

#[async_trait]
impl SnapshotCatalog for NoSnapshotCatalog {
    async fn create(&self, _record: SnapshotRecord) -> RepositoryResult<SnapshotRecord> {
        Self::refuse("create")
    }

    async fn publish_commit(&self, _commit: SnapshotCommit) -> RepositoryResult<SnapshotRecord> {
        Self::refuse("publish_commit")
    }

    async fn get(&self, _id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>> {
        Self::refuse("get")
    }

    async fn list_page(&self, _filter: SnapshotListFilter) -> RepositoryResult<SnapshotListPage> {
        Self::refuse("list_page")
    }

    async fn delete_record(&self, _record: &SnapshotRecord) -> RepositoryResult<()> {
        Self::refuse("delete_record")
    }

    async fn resolve_alias(&self, _alias: &str) -> RepositoryResult<Option<SnapshotId>> {
        Self::refuse("resolve_alias")
    }

    async fn try_start_build(&self, _id: &SnapshotId) -> RepositoryResult<StartedBuild> {
        Self::refuse("try_start_build")
    }

    async fn mark_build_error(
        &self,
        _id: &SnapshotId,
        _reason: TemplateBuildErrorReason,
    ) -> RepositoryResult<()> {
        Self::refuse("mark_build_error")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::repository::interfaces::CatalogReadScope;

    /// 🔴 The default `absence_of` reads `get_scoped`, which reads `get`. A
    /// refusal has to propagate rather than being turned into
    /// `SnapshotAbsence::Settled` — "settled" means *the catalog says it is not
    /// there*, and that is precisely the answer this type must never give.
    #[tokio::test]
    async fn absence_is_refused_rather_than_settled() {
        let catalog = NoSnapshotCatalog;
        let id = SnapshotId::generate();

        let error = catalog
            .absence_of(&id)
            .await
            .expect_err("a process with no catalog cannot settle an absence");
        assert!(
            matches!(error, RepositoryError::Unsupported { .. }),
            "absence must refuse, not answer: {error}"
        );
    }

    /// The scoped reads default onto the unscoped ones; both must refuse.
    #[tokio::test]
    async fn every_read_refuses() {
        let catalog = NoSnapshotCatalog;

        assert!(catalog.get("anything").await.is_err());
        assert!(catalog
            .get_scoped("anything", CatalogReadScope::AnyStatus)
            .await
            .is_err());
        assert!(catalog.resolve_alias("anything").await.is_err());
        assert!(catalog
            .resolve_alias_scoped("anything", CatalogReadScope::AnyStatus)
            .await
            .is_err());
        assert!(catalog
            .list_page(SnapshotListFilter::default())
            .await
            .is_err());
        assert!(catalog
            .list_page_scoped(SnapshotListFilter::default(), CatalogReadScope::AnyStatus)
            .await
            .is_err());
    }
}
