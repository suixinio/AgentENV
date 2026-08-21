//! The OSS catalog: snapshot rows and alias bindings, and nothing else.
//!
//! Object layout under the configured prefix:
//!
//! ```text
//! catalog/records/{id}.json   → SnapshotRecord
//! catalog/aliases/{name}.json → "snapshot-id"
//! ```
//!
//! Snapshot bytes live in [`super::artifacts`]. Before the split these two sat
//! in one 1,394-line file whose `publish()` ran from exporting a disk image
//! straight through to writing the row; the seam that separates them now is the
//! same seam a remote catalog will eventually sit behind.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use futures::{stream, StreamExt, TryStreamExt};
use tracing::{debug, warn};

use super::client::{OssClient, OssUploadArtifact};
use super::layout::OssSnapshotArtifactLayout;
use crate::snapshot::repository::interfaces::{
    SnapshotCatalog, SnapshotCommit, SnapshotListFilter,
};
use crate::snapshot::repository::{RepositoryError, RepositoryResult};
use crate::snapshot::{
    CommittedSnapshot, SnapshotAlias, SnapshotId, SnapshotPublishSource, SnapshotRecord,
    SnapshotSource, SnapshotSourceKind, TemplateBuildErrorReason, TemplateBuildInfo,
    TemplateBuildStatus,
};

const MAX_ALIAS_BIND_ATTEMPTS: usize = 5;

/// Snapshot rows stored as one JSON object per record in OSS.
pub(crate) struct OssSnapshotCatalog {
    client: Arc<OssClient>,
}

impl OssSnapshotCatalog {
    pub(crate) fn new(client: Arc<OssClient>) -> Self {
        Self { client }
    }
}

fn validated_alias_key(alias: &str) -> RepositoryResult<String> {
    SnapshotAlias::parse(alias).map_err(|e| RepositoryError::InvalidRequest {
        reason: format!("invalid alias '{alias}': {e}"),
    })?;
    Ok(OssSnapshotArtifactLayout::alias_key(alias))
}

fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

#[async_trait]
impl SnapshotCatalog for OssSnapshotCatalog {
    async fn create(&self, record: SnapshotRecord) -> RepositoryResult<SnapshotRecord> {
        if !matches!(record.source, SnapshotSource::Template { .. }) {
            return Err(RepositoryError::InvalidRequest {
                reason: "only template snapshots can be pre-created".to_string(),
            });
        }
        if record.committed.is_some() {
            return Err(RepositoryError::InvalidRequest {
                reason: "pre-created template snapshots must not already be committed".to_string(),
            });
        }
        if self.snapshot_exists(&record.id).await? {
            return Err(RepositoryError::InvalidRequest {
                reason: format!("snapshot '{}' already exists", record.id),
            });
        }
        if let Some(alias) = record.alias.as_ref() {
            if let Some(existing) = self.load_alias_target(alias.as_ref()).await? {
                if existing != record.id && self.snapshot_exists(&existing).await? {
                    return Err(RepositoryError::AliasConflict {
                        alias: alias.to_string(),
                        existing,
                        new_id: record.id.clone(),
                    });
                }
            }
        }
        self.write_record(&record).await?;
        if let Some(alias) = record.alias.as_ref() {
            if let Err(error) = self.bind_alias(alias.as_ref(), &record.id).await {
                let _ = self
                    .client
                    .delete(&OssSnapshotArtifactLayout::record_key(&record.id))
                    .await;
                return Err(error);
            }
        }
        Ok(record)
    }

    async fn publish_commit(&self, commit: SnapshotCommit) -> RepositoryResult<SnapshotRecord> {
        let id = commit.id.clone();
        // Alias first, then the record. That order predates the split and is a
        // known defect — `interfaces.rs` asks for the opposite, and the read
        // path's stale-alias cleanup can delete the alias of a snapshot that is
        // mid-publish. It is preserved verbatim here because fixing it belongs
        // with the move to a catalog that can write both in one transaction,
        // not with a refactor that is supposed to change nothing.
        if let Some(alias) = commit.alias.as_ref() {
            self.bind_alias(alias.as_ref(), &id).await?;
        }
        let record = self
            .write_committed_record(
                id.clone(),
                commit.alias,
                commit.resources,
                commit.committed,
                commit.source,
                commit.created_at_unix_ms,
            )
            .await?;
        debug!(snapshot_id = %id, "published snapshot to oss");
        Ok(record)
    }

    async fn get(&self, id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>> {
        // Try by id first.
        if let Ok(direct_id) = SnapshotId::parse(id_or_alias) {
            if let Some(record) = self.read_record(&direct_id).await? {
                return Ok(Some(record));
            }
        }

        // Try by alias.
        let Some(resolved_id) = self.resolve_alias(id_or_alias).await? else {
            return Ok(None);
        };
        self.read_record(&resolved_id).await
    }

    async fn list(&self, filter: SnapshotListFilter) -> RepositoryResult<Vec<SnapshotRecord>> {
        // One LIST plus one GET per record, with the filtering and the sort
        // done in memory. This `1 + N` is the cost the catalog migration is
        // measured against, so it is deliberately left intact.
        let keys = self
            .client
            .list_keys_recursive("catalog/records/")
            .await
            .map_err(|e| RepositoryError::backend("list snapshot records", e))?;

        let mut records: Vec<SnapshotRecord> = stream::iter(keys)
            .map(|key| async move {
                let bytes = self.client.get_bytes(&key).await.map_err(|e| {
                    RepositoryError::backend(format!("read snapshot record '{key}'"), e)
                })?;
                serde_json::from_slice::<SnapshotRecord>(&bytes).map_err(|e| {
                    RepositoryError::backend(format!("parse snapshot record '{key}'"), e)
                })
            })
            .buffer_unordered(16)
            .try_collect()
            .await?;

        records.retain(|record| Self::matches_record_filter(record, &filter));
        records.sort_by(|a, b| {
            b.created_at_unix_ms
                .cmp(&a.created_at_unix_ms)
                .then_with(|| a.id.to_string().cmp(&b.id.to_string()))
        });

        Ok(records)
    }

    async fn delete_record(&self, record: &SnapshotRecord) -> RepositoryResult<()> {
        let id = &record.id;

        // 1. Delete alias binding.
        if let Some(alias) = record.alias.as_ref() {
            if self.load_alias_target(alias.as_ref()).await?.as_ref() == Some(id) {
                if let Err(error) = self
                    .client
                    .delete(&OssSnapshotArtifactLayout::alias_key(alias.as_ref()))
                    .await
                {
                    warn!(snapshot_id = %id, alias = %alias, error = %error, "failed to delete oss alias during snapshot removal");
                }
            }
        }

        // 2. Delete the catalog record.
        self.client
            .delete(&OssSnapshotArtifactLayout::record_key(id))
            .await
            .map_err(|e| RepositoryError::backend("delete snapshot record from oss", e))?;

        debug!(snapshot_id = %id, "deleted snapshot record from oss");
        Ok(())
    }

    async fn resolve_alias(&self, alias: &str) -> RepositoryResult<Option<SnapshotId>> {
        let key = validated_alias_key(alias)?;
        let data = match self.client.get_bytes(&key).await {
            Ok(d) => d,
            Err(e) if OssClient::is_not_found_error(&e) => return Ok(None),
            Err(e) => {
                return Err(RepositoryError::backend(format!("read alias '{alias}'"), e));
            }
        };

        let id: SnapshotId = serde_json::from_slice(&data)
            .map_err(|e| RepositoryError::backend(format!("parse alias '{alias}'"), e))?;

        // Stale-alias cleanup: if the snapshot record doesn't exist, delete the alias
        // and return None (mirrors PosixFs catalog.rs behavior).
        let snapshot_exists = self.snapshot_exists(&id).await?;
        if !snapshot_exists {
            warn!(alias = %alias, snapshot_id = %id, "cleaning up stale alias pointing to missing snapshot");
            if let Err(error) = self.client.delete(&key).await {
                warn!(alias = %alias, snapshot_id = %id, error = %error, "failed to delete stale oss alias");
            }
            return Ok(None);
        }

        Ok(Some(id))
    }

    async fn try_start_build(&self, id: &SnapshotId) -> RepositoryResult<SnapshotRecord> {
        let mut record =
            self.read_record(id)
                .await?
                .ok_or_else(|| RepositoryError::SnapshotNotFound {
                    lookup: id.to_string(),
                })?;
        let now = now_unix_ms();
        let SnapshotSource::Template { build } = &mut record.source else {
            return Err(RepositoryError::InvalidRequest {
                reason: format!("snapshot '{id}' is not a template build"),
            });
        };
        if build.status != TemplateBuildStatus::Waiting {
            return Err(RepositoryError::InvalidRequest {
                reason: format!("template build '{id}' is not in waiting state"),
            });
        }
        build.status = TemplateBuildStatus::Building;
        build.started_at_unix_ms = Some(now);
        build.error_reason = None;
        record.updated_at_unix_ms = now;
        self.write_record(&record).await?;
        Ok(record)
    }

    async fn mark_build_error(
        &self,
        id: &SnapshotId,
        reason: TemplateBuildErrorReason,
    ) -> RepositoryResult<()> {
        let mut record =
            self.read_record(id)
                .await?
                .ok_or_else(|| RepositoryError::SnapshotNotFound {
                    lookup: id.to_string(),
                })?;
        let now = now_unix_ms();
        let SnapshotSource::Template { build } = &mut record.source else {
            return Err(RepositoryError::InvalidRequest {
                reason: format!("snapshot '{id}' is not a template build"),
            });
        };
        build.status = TemplateBuildStatus::Error;
        build.finished_at_unix_ms = Some(now);
        build.error_reason = Some(reason);
        record.updated_at_unix_ms = now;
        self.write_record(&record).await
    }

    // `retains_artifacts_on_publish_failure` keeps the trait default of
    // `false`. That is what this backend has always done: `publish()`'s error
    // path deleted `artifacts/{id}/` unconditionally, including when an earlier
    // publish of the same id had already committed. Answering honestly here
    // would change behaviour, so it is left alone and registered as a finding.
}

// ── private helpers ────────────────────────────────────────────────────

impl OssSnapshotCatalog {
    async fn snapshot_exists(&self, id: &SnapshotId) -> RepositoryResult<bool> {
        self.client
            .exists(&OssSnapshotArtifactLayout::record_key(id))
            .await
            .map_err(|e| RepositoryError::backend(format!("check snapshot record '{id}'"), e))
    }

    async fn read_record(&self, id: &SnapshotId) -> RepositoryResult<Option<SnapshotRecord>> {
        let key = OssSnapshotArtifactLayout::record_key(id);
        match self.client.get_bytes(&key).await {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(|e| RepositoryError::backend(format!("parse snapshot record '{id}'"), e)),
            Err(e) if OssClient::is_not_found_error(&e) => Ok(None),
            Err(e) => Err(RepositoryError::backend(
                format!("read snapshot record '{id}'"),
                e,
            )),
        }
    }

    async fn write_record(&self, record: &SnapshotRecord) -> RepositoryResult<()> {
        let bytes = serde_json::to_vec_pretty(record)
            .map_err(|e| RepositoryError::backend("serialize snapshot record", e))?;
        self.client
            .put_bytes(
                &OssSnapshotArtifactLayout::record_key(&record.id),
                bytes,
                OssUploadArtifact::CatalogRecord,
            )
            .await
            .map_err(|e| RepositoryError::backend("write snapshot record", e))
    }

    /// `created_at_unix_ms` is the instant the *caller* decided this snapshot
    /// came into being, and it is used only when this call is the one creating
    /// the row. A record that already exists keeps the creation time it has —
    /// that is the same rule `mark_committed` follows, and it is why a template
    /// published long after it was created does not appear to be new.
    #[allow(clippy::too_many_arguments)]
    async fn write_committed_record(
        &self,
        id: SnapshotId,
        alias: Option<SnapshotAlias>,
        resources: crate::types::SandboxResources,
        committed: CommittedSnapshot,
        source: SnapshotPublishSource,
        created_at_unix_ms: Option<i64>,
    ) -> RepositoryResult<SnapshotRecord> {
        let now = now_unix_ms();
        let record = if let Some(mut record) = self.read_record(&id).await? {
            record.mark_committed(alias, resources, committed, source, now);
            record
        } else {
            let source = match source {
                SnapshotPublishSource::Template => SnapshotSource::Template {
                    build: TemplateBuildInfo {
                        status: TemplateBuildStatus::Ready,
                        started_at_unix_ms: None,
                        finished_at_unix_ms: Some(now),
                        error_reason: None,
                    },
                },
                SnapshotPublishSource::Sandbox { source_sandbox_id } => {
                    SnapshotSource::Sandbox { source_sandbox_id }
                }
            };
            SnapshotRecord {
                id,
                alias,
                source,
                resources,
                created_at_unix_ms: created_at_unix_ms.unwrap_or(now),
                updated_at_unix_ms: now,
                committed: Some(committed),
            }
        };
        self.write_record(&record).await?;
        Ok(record)
    }

    /// Bind an alias to a snapshot id with best-effort conflict detection.
    ///
    /// 🔴 Carried across the split unchanged, deliberately. It is weaker than a
    /// true CAS and it is meant to stay exactly that weak until the catalog
    /// moves somewhere that can express a unique index; strengthening it here
    /// would make the change that replaces it harder to evaluate.
    ///
    /// Alibaba Cloud OSS does not support conditional write headers
    /// (`If-None-Match`, `x-oss-forbid-overwrite`) on any S3-compatible
    /// write path, so we cannot use a true atomic `put_if_not_exists`.
    ///
    /// Instead the algorithm is:
    ///   1. Read the current alias target.
    ///   2. If it already points to `id`, return success (idempotent).
    ///   3. If it points to a live snapshot, return `AliasConflict`.
    ///   4. If it points to a deleted snapshot, remove the stale alias.
    ///   5. Write our binding unconditionally.
    ///   6. Read back and verify we won the race.  If someone else wrote a
    ///      different binding between steps 5 and 6, detect it here and
    ///      either retry or report a conflict.
    ///
    /// The read-back verification (step 6) narrows the race window to the
    /// interval between our write and the subsequent read.  This is weaker
    /// than a true CAS but sufficient for the current deployment model
    /// where concurrent publishes for the *same alias* are rare.
    async fn bind_alias(&self, alias: &str, id: &SnapshotId) -> RepositoryResult<()> {
        let key = validated_alias_key(alias)?;
        let payload = serde_json::to_vec(id)
            .map_err(|e| RepositoryError::backend("serialize alias binding", e))?;

        for _attempt in 0..MAX_ALIAS_BIND_ATTEMPTS {
            // Step 1-4: check current state and clean up stale bindings.
            if let Some(existing_id) = self.load_alias_target(alias).await? {
                if existing_id == *id {
                    return Ok(());
                }

                let still_exists = self.snapshot_exists(&existing_id).await?;
                if still_exists {
                    return Err(RepositoryError::AliasConflict {
                        alias: alias.to_string(),
                        existing: existing_id,
                        new_id: id.clone(),
                    });
                }

                self.client
                    .delete(&key)
                    .await
                    .map_err(|e| RepositoryError::backend("delete stale alias", e))?;
            }

            // Step 5: write our binding (unconditional — OSS does not
            // support conditional headers on S3-compatible writes).
            self.client
                .put_bytes(&key, payload.clone(), OssUploadArtifact::Alias)
                .await
                .map_err(|e| RepositoryError::backend("write alias binding", e))?;

            // Step 6: read back and verify we won.
            match self.load_alias_target(alias).await? {
                Some(bound_id) if bound_id == *id => return Ok(()),
                Some(existing_id) => {
                    // A concurrent writer overwrote our binding.
                    let still_exists = self.snapshot_exists(&existing_id).await?;
                    if still_exists {
                        return Err(RepositoryError::AliasConflict {
                            alias: alias.to_string(),
                            existing: existing_id,
                            new_id: id.clone(),
                        });
                    }

                    // The concurrent binding points to a deleted snapshot;
                    // clean it up and retry.
                    self.client
                        .delete(&key)
                        .await
                        .map_err(|e| RepositoryError::backend("delete stale alias", e))?;
                }
                None => {
                    warn!(
                        alias,
                        snapshot_id = %id,
                        "alias disappeared after bind attempt; retrying"
                    );
                    continue;
                }
            }
        }

        Err(RepositoryError::Backend {
            message: format!(
                "alias bind for '{alias}' exceeded {MAX_ALIAS_BIND_ATTEMPTS} attempts"
            ),
            source: None,
        })
    }

    async fn load_alias_target(&self, alias: &str) -> RepositoryResult<Option<SnapshotId>> {
        let key = validated_alias_key(alias)?;
        let data = match self.client.get_bytes(&key).await {
            Ok(data) => data,
            Err(e) if OssClient::is_not_found_error(&e) => return Ok(None),
            Err(e) => {
                return Err(RepositoryError::backend(
                    format!("read alias target '{alias}'"),
                    e,
                ));
            }
        };

        let target = serde_json::from_slice::<SnapshotId>(&data)
            .map_err(|e| RepositoryError::backend(format!("parse alias target '{alias}'"), e))?;
        Ok(Some(target))
    }

    fn matches_record_filter(record: &SnapshotRecord, filter: &SnapshotListFilter) -> bool {
        if let Some(alias_prefix) = filter.alias_prefix.as_deref() {
            match record.alias.as_ref() {
                Some(alias) if alias.to_string().starts_with(alias_prefix) => {}
                _ => return false,
            }
        }

        if let Some(ids) = filter.snapshot_ids.as_ref() {
            if !ids.iter().any(|id| id == &record.id) {
                return false;
            }
        }

        if let Some(id_or_alias) = filter.snapshot_id_or_alias.as_deref() {
            if record.id.to_string() != id_or_alias
                && record
                    .alias
                    .as_ref()
                    .is_none_or(|alias| alias.as_ref() != id_or_alias)
            {
                return false;
            }
        }

        if let Some(source_sandbox_id) = filter.source_sandbox_id.as_deref() {
            match &record.source {
                SnapshotSource::Sandbox {
                    source_sandbox_id: record_source_sandbox_id,
                } if record_source_sandbox_id == source_sandbox_id => {}
                _ => return false,
            }
        }

        if let Some(sources) = filter.sources.as_ref() {
            let source = match &record.source {
                SnapshotSource::Template { .. } => SnapshotSourceKind::Template,
                SnapshotSource::Sandbox { .. } => SnapshotSourceKind::Sandbox,
            };
            if !sources.contains(&source) {
                return false;
            }
        }

        if let Some(statuses) = filter.template_statuses.as_ref() {
            let SnapshotSource::Template { build } = &record.source else {
                return false;
            };
            if !statuses.contains(&build.status) {
                return false;
            };
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use metrics_util::debugging::DebuggingRecorder;

    use super::super::test_support::{fake_s3_client, spawn_fake_s3, TEST_PREFIX};
    use super::*;
    use crate::snapshot::repository::metrics::test_support::object_store_requests;
    use crate::types::SandboxResources;

    /// A commit for a fresh snapshot, with an optional alias to bind.
    fn commit(alias: Option<&str>) -> SnapshotCommit {
        SnapshotCommit {
            id: SnapshotId::generate(),
            alias: alias.map(|value| SnapshotAlias::parse(value).expect("alias parses")),
            source: SnapshotPublishSource::Template,
            resources: SandboxResources::default(),
            created_at_unix_ms: None,
            committed: CommittedSnapshot::mock(),
        }
    }

    /// Today a single `GET /snapshots` costs one LIST of `catalog/records/`
    /// plus one GET per record. Pin that `1 + N` down now, while it is still
    /// non-zero: a counter that reads zero both before and after the catalog
    /// moves out of object storage cannot tell the two apart. The same read
    /// also proves the `surface` label separates catalog rows from snapshot
    /// bytes, which is what keeps the later zero from being drowned out by
    /// byte traffic that legitimately continues.
    #[tokio::test]
    async fn catalog_list_and_artifact_read_are_counted_on_separate_surfaces() {
        let record_count = 3_u64;
        let mut objects = BTreeMap::new();
        for index in 0..record_count {
            let record = SnapshotRecord::template_waiting(
                SnapshotId::generate(),
                Some(SnapshotAlias::parse(&format!("template-{index}")).expect("alias parses")),
                SandboxResources::default(),
            );
            objects.insert(
                format!("{TEST_PREFIX}/catalog/records/{}.json", record.id),
                serde_json::to_vec(&record).expect("serialize record"),
            );
        }
        let artifact_key = "artifacts/0198f0a1-0000-7000-8000-000000000000/vm_state.bin";
        objects.insert(
            format!("{TEST_PREFIX}/{artifact_key}"),
            b"vm state".to_vec(),
        );

        let addr = spawn_fake_s3(objects).await;
        let client = fake_s3_client(addr);
        let catalog = OssSnapshotCatalog::new(Arc::clone(&client));

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let guard = metrics::set_default_local_recorder(&recorder);

        let listed = catalog
            .list(SnapshotListFilter::matches_all())
            .await
            .expect("list should work");
        let artifact = client
            .get_bytes(artifact_key)
            .await
            .expect("artifact read should work");

        drop(guard);

        assert_eq!(listed.len(), record_count as usize);
        assert_eq!(artifact.as_ref(), b"vm state");
        assert_eq!(
            object_store_requests(&snapshotter),
            BTreeMap::from([
                ("list/catalog/ok".to_owned(), 1),
                ("get/catalog/ok".to_owned(), record_count),
                ("get/artifact/ok".to_owned(), 1),
            ]),
        );
    }

    /// The cluster measurement this test exists for: seeding 30 template
    /// snapshots produced `get/catalog/error = 60` — exactly two per snapshot —
    /// alongside `put/catalog/ok = 60`, while every create returned 201. Both
    /// "errors" are lookups that legitimately miss on a store that has never
    /// seen the snapshot before:
    ///
    ///   1. `create()`'s pre-flight `load_alias_target`, and
    ///   2. `bind_alias()`'s step 1, which reads the same alias key again.
    ///
    /// Nothing failed. Anyone alerting on `outcome="error"` would have paged on
    /// ordinary snapshot creation, so the miss gets its own outcome label.
    #[tokio::test]
    async fn expected_catalog_misses_are_not_counted_as_errors() {
        let addr = spawn_fake_s3(BTreeMap::new()).await;
        let catalog = OssSnapshotCatalog::new(fake_s3_client(addr));
        let record = SnapshotRecord::template_waiting(
            SnapshotId::generate(),
            Some(SnapshotAlias::parse("template-alias").expect("alias parses")),
            SandboxResources::default(),
        );

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let guard = metrics::set_default_local_recorder(&recorder);
        let created = catalog.create(record.clone()).await;
        drop(guard);

        created.expect("create should succeed against an empty store");
        assert_eq!(
            object_store_requests(&snapshotter),
            BTreeMap::from([
                // The record key is probed once and is absent.
                ("head/catalog/not_found".to_owned(), 1),
                // The two alias lookups that used to read as errors.
                ("get/catalog/not_found".to_owned(), 2),
                // `bind_alias` step 6 reads its own binding back.
                ("get/catalog/ok".to_owned(), 1),
                // The record and the alias binding.
                ("put/catalog/ok".to_owned(), 2),
            ]),
            "a create against an empty store must record no failed request"
        );
    }

    /// The catalog half must be usable on its own: create a row, read it back
    /// by id and by alias, and delete it, without an artifact store existing.
    #[tokio::test]
    async fn catalog_round_trips_a_record_without_an_artifact_store() {
        let addr = spawn_fake_s3(BTreeMap::new()).await;
        let client = fake_s3_client(addr);
        let catalog = OssSnapshotCatalog::new(Arc::clone(&client));
        let record = SnapshotRecord::template_waiting(
            SnapshotId::generate(),
            Some(SnapshotAlias::parse("standalone").expect("alias parses")),
            SandboxResources::default(),
        );

        catalog
            .create(record.clone())
            .await
            .expect("create should work");

        let by_id = catalog
            .get(&record.id.to_string())
            .await
            .expect("get by id should work")
            .expect("record should exist");
        assert_eq!(by_id.id, record.id);

        let by_alias = catalog
            .get("standalone")
            .await
            .expect("get by alias should work")
            .expect("record should resolve through its alias");
        assert_eq!(by_alias.id, record.id);
        assert_eq!(
            catalog
                .resolve_alias("standalone")
                .await
                .expect("resolve should work"),
            Some(record.id.clone())
        );

        catalog
            .delete_record(&record)
            .await
            .expect("delete should work");
        assert!(catalog
            .get(&record.id.to_string())
            .await
            .expect("get after delete should work")
            .is_none());
        // Assert the binding object itself is gone rather than asking
        // `resolve_alias`: that call cleans up aliases pointing at missing
        // records, so it would answer `None` even if the delete had left the
        // binding behind.
        assert!(
            !client
                .exists("catalog/aliases/standalone.json")
                .await
                .expect("alias existence check should work"),
            "deleting a record must remove the alias binding that pointed at it"
        );
    }

    /// `publish_commit` is the flip: before it the bytes are unfindable, after
    /// it they resolve. It has to do both halves of that — mark the record
    /// committed *and* bind the alias — because a commit that only wrote the
    /// record leaves the alias pointing nowhere, and one that only bound the
    /// alias leaves it pointing at an uncommitted row.
    #[tokio::test]
    async fn publish_commit_marks_the_record_committed_and_binds_its_alias() {
        let addr = spawn_fake_s3(BTreeMap::new()).await;
        let catalog = OssSnapshotCatalog::new(fake_s3_client(addr));
        let published = commit(Some("published"));
        let id = published.id.clone();

        let record = catalog
            .publish_commit(published)
            .await
            .expect("commit should work");

        assert_eq!(record.id, id);
        assert!(record.committed.is_some(), "the record must be committed");
        assert_eq!(
            catalog
                .resolve_alias("published")
                .await
                .expect("resolve should work"),
            Some(id.clone()),
            "the alias must resolve to the snapshot the commit published"
        );
        assert!(
            catalog
                .get("published")
                .await
                .expect("get should work")
                .expect("record should resolve through the alias")
                .committed
                .is_some(),
            "resolving through the alias must reach the committed record"
        );
    }

    /// A commit must not take an alias a live snapshot already holds.
    #[tokio::test]
    async fn publish_commit_refuses_an_alias_a_live_snapshot_holds() {
        let addr = spawn_fake_s3(BTreeMap::new()).await;
        let catalog = OssSnapshotCatalog::new(fake_s3_client(addr));
        let held = commit(Some("held"));
        let first_id = held.id.clone();
        catalog
            .publish_commit(held)
            .await
            .expect("first commit should work");

        let error = catalog
            .publish_commit(commit(Some("held")))
            .await
            .expect_err("a second commit must not steal the alias");

        assert!(
            matches!(error, RepositoryError::AliasConflict { .. }),
            "expected an alias conflict, got {error:?}"
        );
        assert_eq!(
            catalog
                .resolve_alias("held")
                .await
                .expect("resolve should work"),
            Some(first_id),
            "the alias must still point at the snapshot that won it"
        );
    }

    /// Template builds pre-create a `Waiting` row and only later publish over
    /// it, so `publish_commit` has a second branch: fold the commit into the
    /// row that is already there rather than mint a new one. Folding is what
    /// preserves the template's identity — its creation time and its build
    /// reaching `Ready` rather than appearing from nowhere as a fresh record.
    #[tokio::test]
    async fn publish_commit_folds_into_a_pre_created_template_record() {
        let addr = spawn_fake_s3(BTreeMap::new()).await;
        let catalog = OssSnapshotCatalog::new(fake_s3_client(addr));
        let pending = SnapshotRecord::template_waiting(
            SnapshotId::generate(),
            Some(SnapshotAlias::parse("built").expect("alias parses")),
            SandboxResources::default(),
        );
        catalog
            .create(pending.clone())
            .await
            .expect("pre-create should work");

        let committed = catalog
            .publish_commit(SnapshotCommit {
                id: pending.id.clone(),
                alias: pending.alias.clone(),
                ..commit(None)
            })
            .await
            .expect("commit should work");

        assert!(committed.committed.is_some());
        assert_eq!(
            committed.created_at_unix_ms, pending.created_at_unix_ms,
            "committing must fold into the pre-created row, not replace it"
        );
        let SnapshotSource::Template { build } = &committed.source else {
            panic!(
                "a template build must stay a template, got {:?}",
                committed.source
            );
        };
        assert_eq!(build.status, TemplateBuildStatus::Ready);

        let reloaded = catalog
            .get(&pending.id.to_string())
            .await
            .expect("get should work")
            .expect("record should exist");
        assert!(
            reloaded.committed.is_some(),
            "the stored row must carry the commit, not just the returned value"
        );
        assert_eq!(reloaded.created_at_unix_ms, pending.created_at_unix_ms);
    }

    /// A second snapshot must not be able to steal a live alias. This is the
    /// weak read-modify-write-reread path, kept exactly as weak as it was.
    #[tokio::test]
    async fn binding_a_live_alias_to_a_second_snapshot_conflicts() {
        let addr = spawn_fake_s3(BTreeMap::new()).await;
        let catalog = OssSnapshotCatalog::new(fake_s3_client(addr));
        let alias = SnapshotAlias::parse("contested").expect("alias parses");
        let first = SnapshotRecord::template_waiting(
            SnapshotId::generate(),
            Some(alias.clone()),
            SandboxResources::default(),
        );
        let second = SnapshotRecord::template_waiting(
            SnapshotId::generate(),
            Some(alias),
            SandboxResources::default(),
        );

        catalog.create(first).await.expect("first create works");
        let error = catalog
            .create(second)
            .await
            .expect_err("second create should not steal the alias");
        assert!(
            matches!(error, RepositoryError::AliasConflict { .. }),
            "expected an alias conflict, got {error:?}"
        );
    }
}
