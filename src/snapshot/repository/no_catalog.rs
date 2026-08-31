//! Refusing catalog implementation for processes with no catalog access.
//!
//! Refusal prevents missing access from being mistaken for settled absence.

use async_trait::async_trait;

use super::interfaces::{
    SnapshotCatalog, SnapshotCommit, SnapshotListFilter, SnapshotListPage, StartedBuild,
};
use super::{RepositoryError, RepositoryResult};
use crate::snapshot::types::{SnapshotId, SnapshotRecord, TemplateBuildErrorReason};

/// Refuses every catalog operation.
///
/// Reaching this type means a catalog request was routed to the wrong process.
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
