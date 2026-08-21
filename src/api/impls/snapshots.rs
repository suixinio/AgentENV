use async_trait::async_trait;
use axum_extra::extract::CookieJar;
use headers::Host;
use http::Method;

use agentenv_http_server::apis::snapshots::*;
use agentenv_http_server::models;

use crate::snapshot::{SnapshotListFilter, SnapshotRecord, SnapshotSource};

use super::pagination::{
    snapshot_cursor_from_token, snapshot_next_token, system_time_from_unix_ms,
};
use super::ApiImpl;

impl From<SnapshotRecord> for models::SnapshotInfo {
    fn from(record: SnapshotRecord) -> Self {
        let snapshot_id = record.id.to_string();
        let image_ref = record.published_rootfs_image_ref().map(str::to_owned);
        let names = if let Some(alias) = record.alias {
            vec![alias.to_string()]
        } else {
            vec![]
        };
        models::SnapshotInfo {
            snapshot_id,
            names,
            cpu_count: record.resources.cpu_count,
            memory_mb: record.resources.memory_mib,
            disk_size_mb: record.resources.disk_size_mib,
            created_at: chrono::DateTime::<chrono::Utc>::from(system_time_from_unix_ms(
                record.created_at_unix_ms,
            )),
            updated_at: chrono::DateTime::<chrono::Utc>::from(system_time_from_unix_ms(
                record.updated_at_unix_ms,
            )),
            image_ref,
        }
    }
}

#[async_trait]
impl Snapshots<()> for ApiImpl {
    type Claims = super::Claims;

    async fn snapshots_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        query_params: &models::SnapshotsGetQueryParams,
    ) -> Result<SnapshotsGetResponse, ()> {
        // 🔴 No cursor means "start at the newest row", and it is expressed by
        // its absence rather than by a sentinel at `now`. The sentinel had a
        // failure only a fast cluster shows: its instant carries nanoseconds
        // while a row's carries milliseconds, so a snapshot created during the
        // current millisecond ties with it and loses the id comparison against
        // the maximum UUID — and falls off the first page it should have led.
        let cursor = match query_params.next_token.as_deref() {
            Some(token) => match snapshot_cursor_from_token(token) {
                Ok(cursor) => Some(cursor),
                Err(err) => {
                    return Ok(SnapshotsGetResponse::Status400_BadRequest(Self::error(
                        400,
                        format!("invalid next token: {}", err),
                    )));
                }
            },
            None => None,
        };

        // 🔴 The page bounds travel *with* the filter, and that is the whole
        // change: a catalog that can push them into its storage does, and one
        // that cannot answers the same page from a scan. Before this the
        // listing was read whole and sliced up here, so `?limit=5` cost exactly
        // what `?limit=100` did.
        let filter = SnapshotListFilter::sandbox_snapshots(
            query_params.sandbox_id.clone(),
            query_params.name.clone(),
        )
        .paginated(query_params.limit, cursor);

        let page = match self.snapshot_manager.list_page(filter).await {
            Ok(page) => page,
            Err(err) => {
                return Ok(SnapshotsGetResponse::Status500_ServerError(
                    Self::snapshot_manager_error(&err),
                ));
            }
        };

        Ok(
            SnapshotsGetResponse::Status200_SuccessfullyReturnedSnapshots {
                body: page
                    .items
                    .into_iter()
                    .map(models::SnapshotInfo::from)
                    .collect(),
                x_next_token: page.next.as_ref().map(snapshot_next_token),
            },
        )
    }

    async fn snapshots_snapshot_id_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::SnapshotsSnapshotIdGetPathParams,
    ) -> Result<SnapshotsSnapshotIdGetResponse, ()> {
        match self.snapshot_manager.get(&path_params.snapshot_id).await {
            // Scope this endpoint to sandbox-sourced snapshots so it stays
            // consistent with the list API, which only exposes
            // `SnapshotSourceKind::Sandbox` records. Template records are
            // surfaced through the template APIs instead.
            Ok(Some(record)) if matches!(record.source, SnapshotSource::Sandbox { .. }) => Ok(
                SnapshotsSnapshotIdGetResponse::Status200_SuccessfullyReturnedTheSnapshot(
                    models::SnapshotInfo::from(record),
                ),
            ),
            Ok(_) => Ok(SnapshotsSnapshotIdGetResponse::Status404_NotFound(
                Self::error(
                    404,
                    format!("snapshot '{}' not found", path_params.snapshot_id),
                ),
            )),
            Err(err) => Ok(SnapshotsSnapshotIdGetResponse::Status500_ServerError(
                Self::snapshot_manager_error(&err),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::{
        rootfs_snapshot_image_tag, CommittedSnapshot, PersistedDiskImagePublication,
    };

    #[test]
    fn snapshot_info_includes_published_rootfs_image_ref() {
        let mut record = SnapshotRecord::mock_ready(CommittedSnapshot::mock());
        let tag = rootfs_snapshot_image_tag(&record.id);
        let expected = format!("registry.example/ns/app:{tag}");
        record.committed.as_mut().unwrap().disk_publications =
            vec![PersistedDiskImagePublication {
                image_ref: expected.clone(),
                tag,
                manifest_digest: "sha256:manifest".to_string(),
                repo_blob_url: "https://registry.example/v2/ns/app/blobs".to_string(),
            }];

        let info = models::SnapshotInfo::from(record);

        assert_eq!(info.image_ref.as_deref(), Some(expected.as_str()));
    }

    #[test]
    fn snapshot_info_omits_image_ref_without_publication() {
        let record = SnapshotRecord::mock_ready(CommittedSnapshot::mock());

        let info = models::SnapshotInfo::from(record);

        assert_eq!(info.image_ref, None);
        let serialized = serde_json::to_value(&info).expect("serialize SnapshotInfo");
        assert!(serialized.get("imageRef").is_none());
    }
}
