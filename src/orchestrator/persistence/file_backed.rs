use std::collections::HashSet;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::fs;
use tokio::sync::OnceCell;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use super::{ClusterRegistration, PersistenceResult, SandboxPersistenceError, SandboxPersister};
use crate::orchestrator::{store::SandboxMetadata, SandboxState};
use crate::record_dir::{discard_unreadable_store, JsonRecordDir, RecordDurability};
use crate::sandbox::{PausedSandboxState, SandboxBackendFactory};
use crate::types::SandboxId;
use crate::virtualization::VirtualizationMode;

const RECORD_VERSION: u32 = 1;
const RECORD_DIR: &str = "records";
const LEGACY_RECORD_DB_DIR: &str = "records.db";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PersistedPausedLifecycle {
    Paused,
    Resuming,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PersistedPausedRecord {
    version: u32,
    lifecycle: PersistedPausedLifecycle,
    metadata: SandboxMetadata,
    artifact_root: PathBuf,
    state: Value,
    /// Whether the record was announced to a cluster registry.
    #[serde(default)]
    cluster_registered: bool,
    /// Node identity used when the record was announced.
    #[serde(default)]
    registered_as: Option<String>,
}

impl PersistedPausedRecord {
    fn into_metadata<F>(mut self, factory: &F) -> PersistenceResult<SandboxMetadata>
    where
        F: SandboxBackendFactory,
    {
        ensure_supported_version(self.version)?;

        let paused_state = factory
            .decode_paused_state(self.artifact_root, self.state)
            .map_err(|source| SandboxPersistenceError::InvalidRecord {
                reason: "failed to decode paused sandbox state".to_string(),
                source: Some(source),
            })?;
        self.metadata.state = SandboxState::Paused;
        self.metadata.paused_state = Some(paused_state);

        Ok(self.metadata)
    }

    fn into_metadata_without_runtime_state(mut self) -> SandboxMetadata {
        self.metadata.state = SandboxState::Paused;
        self.metadata.paused_state = None;
        self.metadata
    }
}

fn decode_record(bytes: &[u8]) -> PersistenceResult<PersistedPausedRecord> {
    let record: PersistedPausedRecord =
        serde_json::from_slice(bytes).map_err(|source| SandboxPersistenceError::InvalidRecord {
            reason: "failed to deserialize record".to_string(),
            source: Some(source.into()),
        })?;
    ensure_supported_version(record.version)?;
    Ok(record)
}

// Pre-incarnation records are retained for explicit operator recovery.
fn record_predates_executions(bytes: &[u8]) -> bool {
    let Ok(value) = serde_json::from_slice::<Value>(bytes) else {
        return false;
    };
    let Some(metadata) = value.get("metadata") else {
        return false;
    };
    metadata.is_object() && metadata.get("execution_id").is_none()
}

fn ensure_supported_version(version: u32) -> PersistenceResult<()> {
    if version == RECORD_VERSION {
        Ok(())
    } else {
        Err(SandboxPersistenceError::InvalidRecord {
            reason: format!("unsupported record version {version}"),
            source: None,
        })
    }
}

pub struct FileBackedSandboxPersister {
    root: PathBuf,
    virtualization_mode: VirtualizationMode,
    durability: RecordDurability,
    records: OnceCell<JsonRecordDir>,
}

impl FileBackedSandboxPersister {
    pub fn new(root: PathBuf, virtualization_mode: VirtualizationMode) -> Self {
        Self {
            root,
            virtualization_mode,
            durability: RecordDurability::Full,
            records: OnceCell::new(),
        }
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn new_for_test(root: PathBuf) -> Self {
        Self::new(root, VirtualizationMode::Kvm)
    }

    pub fn with_durability(mut self, durability: RecordDurability) -> Self {
        self.durability = durability;
        self
    }

    fn records_path(&self) -> PathBuf {
        self.root.join(RECORD_DIR)
    }

    fn legacy_records_db_path(&self) -> PathBuf {
        self.root.join(LEGACY_RECORD_DB_DIR)
    }

    fn artifacts_root(&self) -> PathBuf {
        self.root.join("artifacts")
    }

    fn sandbox_artifact_root(&self, sandbox_id: &SandboxId) -> PathBuf {
        self.artifacts_root().join(sandbox_id.to_string())
    }

    async fn records(&self) -> PersistenceResult<JsonRecordDir> {
        self.records
            .get_or_try_init(|| async {
                let legacy = self.legacy_records_db_path();
                if discard_unreadable_store(&legacy).await {
                    warn!(
                        store = %legacy.display(),
                        "discarded paused sandbox records this build cannot read; the sandboxes \
                         they described are gone, and any cluster registry rows still naming them \
                         are orphans an operator has to clear"
                    );
                }
                JsonRecordDir::open(self.records_path(), self.durability)
                    .await
                    .map_err(|source| {
                        SandboxPersistenceError::store("open paused sandbox records", source)
                    })
            })
            .await
            .cloned()
    }

    async fn get_record(&self, sandbox_id: &SandboxId) -> PersistenceResult<PersistedPausedRecord> {
        let bytes = self
            .records()
            .await?
            .get(&sandbox_id.to_string())
            .await
            .map_err(|source| SandboxPersistenceError::store("read paused sandbox record", source))?
            .ok_or_else(|| SandboxPersistenceError::InvalidRecord {
                reason: format!("paused sandbox record {sandbox_id} not found"),
                source: None,
            })?;
        decode_record(&bytes)
    }

    async fn put_record(&self, record: &PersistedPausedRecord) -> PersistenceResult<()> {
        let bytes = serde_json::to_vec(record).map_err(|source| {
            SandboxPersistenceError::InvalidRecord {
                reason: "failed to serialize record".to_string(),
                source: Some(source.into()),
            }
        })?;

        self.records()
            .await?
            .put(&record.metadata.id.to_string(), bytes)
            .await
            .map_err(|source| {
                SandboxPersistenceError::store("persist paused sandbox record", source)
            })
    }

    async fn remove_record(&self, sandbox_id: &SandboxId) -> PersistenceResult<()> {
        self.records()
            .await?
            .remove(&sandbox_id.to_string())
            .await
            .map_err(|source| {
                SandboxPersistenceError::store("remove paused sandbox record", source)
            })
    }

    async fn remove_artifact_root(path: &Path) -> PersistenceResult<()> {
        match fs::remove_dir_all(path).await {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(SandboxPersistenceError::io(
                "remove paused sandbox artifacts",
                path,
                source,
            )),
        }
    }

    async fn cleanup_invalid_record(&self, sandbox_id: &SandboxId) -> PersistenceResult<()> {
        debug!(sandbox_id = %sandbox_id, "cleaning up invalid paused sandbox record");
        self.remove_record(sandbox_id).await?;
        Self::remove_artifact_root(&self.sandbox_artifact_root(sandbox_id)).await?;
        Ok(())
    }

    async fn cleanup_orphan_artifacts(
        &self,
        retained_sandbox_ids: &HashSet<SandboxId>,
    ) -> PersistenceResult<()> {
        let artifacts_root = self.artifacts_root();
        let mut entries = match fs::read_dir(&artifacts_root).await {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(source) => {
                return Err(SandboxPersistenceError::io(
                    "read paused sandbox artifacts",
                    &artifacts_root,
                    source,
                ));
            }
        };

        while let Some(entry) = entries.next_entry().await.map_err(|source| {
            SandboxPersistenceError::io("scan paused sandbox artifacts", &artifacts_root, source)
        })? {
            let file_type = entry.file_type().await.map_err(|source| {
                SandboxPersistenceError::io(
                    "inspect paused sandbox artifacts",
                    entry.path(),
                    source,
                )
            })?;
            if !file_type.is_dir() {
                continue;
            }

            let Some(sandbox_id) = entry
                .file_name()
                .to_str()
                .and_then(|name| SandboxId::parse_str(name).ok())
            else {
                continue;
            };

            if !retained_sandbox_ids.contains(&sandbox_id) {
                info!(
                    sandbox_id = %sandbox_id,
                    artifacts = %entry.path().display(),
                    "removing orphaned paused sandbox artifacts"
                );
                Self::remove_artifact_root(&entry.path()).await?;
            }
        }

        Ok(())
    }
}

#[async_trait]
impl SandboxPersister for FileBackedSandboxPersister {
    async fn load_all<F>(&self, factory: &F) -> PersistenceResult<Vec<SandboxMetadata>>
    where
        F: SandboxBackendFactory,
    {
        info!(store = %self.root.display(), "loading paused sandbox records");
        let records = self.records().await?.load_all().await.map_err(|source| {
            SandboxPersistenceError::store("scan paused sandbox records", source)
        })?;
        let mut sandboxes = Vec::new();
        let mut retained_artifacts = HashSet::new();
        // Count rejected records for alerting.
        let mut rejected = 0_u64;

        for (key, bytes) in records {
            let sandbox_id_from_key = SandboxId::parse_str(&key).ok();
            let record = match decode_record(&bytes) {
                Ok(record) => record,
                Err(_) if record_predates_executions(&bytes) => {
                    // Retain pre-incarnation records and report actionable recovery.
                    rejected += 1;
                    error!(
                        record_key = %key,
                        store = %self.root.display(),
                        "paused sandbox record was written before sandboxes carried an execution \
                         id, so it cannot be loaded and is being kept rather than discarded. \
                         Clear this node's paused records to continue — this deletes the paused \
                         sandboxes on this machine, and it is the node half of the same step that \
                         drops the registry table: rm -rf {}",
                        self.root.display()
                    );
                    continue;
                }
                Err(err) => {
                    rejected += 1;
                    warn!(record_key = %key, error = %err, "discarding invalid paused sandbox record");
                    if let Some(sandbox_id) = sandbox_id_from_key {
                        let _ = self.remove_record(&sandbox_id).await;
                    }
                    continue;
                }
            };
            let sandbox_id = record.metadata.id;

            if record.lifecycle == PersistedPausedLifecycle::Resuming {
                warn!(sandbox_id = %sandbox_id, "discarding paused sandbox record left in resuming state");
                rejected += 1;
                self.cleanup_invalid_record(&sandbox_id).await?;
                continue;
            }

            if record.metadata.virtualization_mode != self.virtualization_mode {
                warn!(
                    sandbox_id = %sandbox_id,
                    record_mode = %record.metadata.virtualization_mode,
                    node_mode = %self.virtualization_mode,
                    "loading paused sandbox metadata without resumable runtime state because its virtualization mode is incompatible"
                );
                retained_artifacts.insert(sandbox_id);
                sandboxes.push(record.into_metadata_without_runtime_state());
                continue;
            }

            match record.into_metadata(factory) {
                Ok(metadata) => {
                    retained_artifacts.insert(sandbox_id);
                    sandboxes.push(metadata);
                }
                Err(err) => {
                    rejected += 1;
                    warn!(sandbox_id = %sandbox_id, error = %err, "discarding unusable paused sandbox record");
                    self.cleanup_invalid_record(&sandbox_id).await?;
                }
            }
        }

        self.cleanup_orphan_artifacts(&retained_artifacts).await?;

        metrics::counter!("agentenv_persisted_sandbox_load_total", "result" => "restored")
            .increment(sandboxes.len() as u64);
        metrics::counter!("agentenv_persisted_sandbox_load_total", "result" => "rejected")
            .increment(rejected);

        info!(
            loaded = sandboxes.len(),
            retained = retained_artifacts.len(),
            rejected,
            "loaded paused sandbox records"
        );

        Ok(sandboxes)
    }

    async fn allocate_artifact_root(
        &self,
        sandbox_id: &SandboxId,
    ) -> PersistenceResult<Option<PathBuf>> {
        let artifact_root = self
            .sandbox_artifact_root(sandbox_id)
            .join(Uuid::now_v7().to_string());
        fs::create_dir_all(&artifact_root).await.map_err(|source| {
            SandboxPersistenceError::io(
                "allocate paused sandbox artifact root",
                &artifact_root,
                source,
            )
        })?;
        Ok(Some(artifact_root))
    }

    async fn persist_paused(
        &self,
        metadata: &SandboxMetadata,
        artifact_root: Option<&Path>,
        paused_state: &dyn PausedSandboxState,
    ) -> PersistenceResult<()> {
        let artifact_root = artifact_root.ok_or_else(|| SandboxPersistenceError::RuntimeState {
            reason: "file-backed persister requires an allocated artifact root",
        })?;
        debug!(
            sandbox_id = %metadata.id,
            artifact_root = %artifact_root.display(),
            "persisting paused sandbox"
        );
        let state = match paused_state.encode() {
            Ok(state) => state,
            Err(source) => {
                let _ = Self::remove_artifact_root(artifact_root).await;
                return Err(SandboxPersistenceError::InvalidRecord {
                    reason: "failed to encode paused sandbox state".to_string(),
                    source: Some(source),
                });
            }
        };
        let record = PersistedPausedRecord {
            version: RECORD_VERSION,
            lifecycle: PersistedPausedLifecycle::Paused,
            metadata: metadata.clone(),
            artifact_root: artifact_root.to_path_buf(),
            cluster_registered: false,
            registered_as: None,
            state,
        };
        let result = self.put_record(&record).await;
        if result.is_err() {
            let _ = Self::remove_artifact_root(artifact_root).await;
        }
        result
    }

    /// Returns absence without converting it to an invalid-record error.
    async fn paused_artifact_root(
        &self,
        sandbox_id: &SandboxId,
    ) -> PersistenceResult<Option<PathBuf>> {
        let raw = self
            .records()
            .await?
            .get(&sandbox_id.to_string())
            .await
            .map_err(|source| {
                SandboxPersistenceError::store("read paused sandbox record", source)
            })?;
        let Some(raw) = raw else {
            return Ok(None);
        };
        Ok(Some(decode_record(&raw)?.artifact_root))
    }

    async fn mark_resuming(&self, sandbox_id: &SandboxId) -> PersistenceResult<()> {
        debug!(sandbox_id = %sandbox_id, "marking paused sandbox as resuming");
        let mut record = self.get_record(sandbox_id).await?;
        record.lifecycle = PersistedPausedLifecycle::Resuming;
        self.put_record(&record).await
    }

    async fn mark_cluster_registered(
        &self,
        sandbox_id: &SandboxId,
        node_id: &str,
    ) -> PersistenceResult<()> {
        debug!(sandbox_id = %sandbox_id, node_id, "marking paused sandbox as cluster-registered");
        let mut record = self.get_record(sandbox_id).await?;
        record.cluster_registered = true;
        record.registered_as = Some(node_id.to_string());
        self.put_record(&record).await
    }

    async fn cluster_registration(
        &self,
        sandbox_id: &SandboxId,
    ) -> PersistenceResult<ClusterRegistration> {
        let record = self.get_record(sandbox_id).await?;
        Ok(match (record.cluster_registered, record.registered_as) {
            (false, _) => ClusterRegistration::Never,
            (true, Some(node_id)) => ClusterRegistration::As(node_id),
            (true, None) => ClusterRegistration::Anonymous,
        })
    }

    async fn rollback_resuming(&self, sandbox_id: &SandboxId) -> PersistenceResult<()> {
        debug!(sandbox_id = %sandbox_id, "rolling back paused sandbox to paused");
        let mut record = self.get_record(sandbox_id).await?;
        record.lifecycle = PersistedPausedLifecycle::Paused;
        self.put_record(&record).await
    }

    async fn delete_record(&self, sandbox_id: &SandboxId) -> PersistenceResult<()> {
        debug!(sandbox_id = %sandbox_id, "deleting paused sandbox record");
        self.remove_record(sandbox_id).await
    }

    async fn delete_record_and_artifacts(&self, sandbox_id: &SandboxId) -> PersistenceResult<()> {
        debug!(sandbox_id = %sandbox_id, "deleting paused sandbox record and artifacts");
        self.remove_record(sandbox_id).await?;
        Self::remove_artifact_root(&self.sandbox_artifact_root(sandbox_id)).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_snapshot::RunnableSnapshot;
    use crate::sandbox::{
        mock::{MockBackendFactory, MockSnapshot},
        FreshSandboxBuildSpec, PausedSandboxState, RuntimeArtifactSet, SandboxBackend,
        SandboxLaunchConfig,
    };
    use anyhow::Result;
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::TempDir;

    #[derive(Debug)]
    struct FailingEncodeState;

    impl PausedSandboxState for FailingEncodeState {
        fn encode(&self) -> Result<Value> {
            anyhow::bail!("forced encode failure")
        }

        fn runtime_artifacts(&self) -> RuntimeArtifactSet {
            RuntimeArtifactSet::empty()
        }
    }

    #[derive(Default)]
    struct RejectingFactory;

    impl SandboxBackendFactory for RejectingFactory {
        fn build(
            &self,
            _build_spec: FreshSandboxBuildSpec,
            _launch_config: SandboxLaunchConfig,
            _execution_id: crate::types::ExecutionId,
        ) -> Result<Box<dyn SandboxBackend>> {
            unreachable!("persister tests only decode state")
        }

        fn build_from_snapshot(
            &self,
            _snapshot: &RunnableSnapshot,
            _launch_config: SandboxLaunchConfig,
            _execution_id: crate::types::ExecutionId,
        ) -> Result<Box<dyn SandboxBackend>> {
            unreachable!("persister tests only decode state")
        }

        fn build_from_paused_state(
            &self,
            _sandbox_id: SandboxId,
            _execution_id: crate::types::ExecutionId,
            _state: &dyn PausedSandboxState,
            _envd_access_token: Option<crate::sandbox::EnvdAccessToken>,
        ) -> Result<Box<dyn SandboxBackend>> {
            unreachable!("persister tests only decode state")
        }

        fn decode_paused_state(
            &self,
            _artifact_root: PathBuf,
            _state: Value,
        ) -> Result<Arc<dyn PausedSandboxState>> {
            anyhow::bail!("forced decode failure")
        }
    }

    fn paused_state(root: &Path) -> Arc<dyn PausedSandboxState> {
        std::fs::create_dir_all(root).expect("create test artifact root");
        Arc::new(MockSnapshot)
    }

    fn test_persister(root: &Path) -> FileBackedSandboxPersister {
        FileBackedSandboxPersister::new_for_test(root.to_path_buf())
            .with_durability(RecordDurability::Memory)
    }

    async fn persist_test_record(
        persister: &FileBackedSandboxPersister,
        snapshot_root: &Path,
    ) -> anyhow::Result<(SandboxId, Arc<dyn PausedSandboxState>)> {
        let paused_state = paused_state(snapshot_root);
        let metadata = SandboxMetadata {
            id: SandboxId::new(),
            virtualization_mode: persister.virtualization_mode,
            paused_state: Some(Arc::clone(&paused_state)),
            ..Default::default()
        };
        let sandbox_id = metadata.id;
        persister
            .persist_paused(&metadata, Some(snapshot_root), paused_state.as_ref())
            .await?;
        Ok((sandbox_id, paused_state))
    }

    async fn has_record(
        persister: &FileBackedSandboxPersister,
        sandbox_id: &SandboxId,
    ) -> anyhow::Result<bool> {
        Ok(persister
            .records()
            .await?
            .get(&sandbox_id.to_string())
            .await?
            .is_some())
    }

    #[tokio::test]
    async fn file_persister_round_trips_paused_record() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let snapshot_root = temp.path().join("artifacts");
        let paused_state = paused_state(&snapshot_root);
        let metadata = SandboxMetadata {
            timeout: Some(Duration::from_secs(5)),
            paused_state: Some(Arc::clone(&paused_state)),
            ..Default::default()
        };

        persister
            .persist_paused(&metadata, Some(&snapshot_root), paused_state.as_ref())
            .await?;
        let loaded = persister.load_all(&MockBackendFactory::new()).await?;

        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].id, metadata.id);
        assert!(loaded[0]
            .paused_state
            .as_ref()
            .expect("paused state should be restored")
            .downcast_ref::<MockSnapshot>()
            .is_some());
        Ok(())
    }

    #[tokio::test]
    async fn paused_record_from_other_mode_is_visible_but_not_resumable() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let kvm_persister = test_persister(temp.path());
        let snapshot_root = temp.path().join("artifacts");
        let (sandbox_id, _paused_state) =
            persist_test_record(&kvm_persister, &snapshot_root).await?;
        drop(kvm_persister);
        let pvm_persister =
            FileBackedSandboxPersister::new(temp.path().to_path_buf(), VirtualizationMode::Pvm)
                .with_durability(RecordDurability::Memory);

        let loaded = pvm_persister.load_all(&MockBackendFactory::new()).await?;

        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].id, sandbox_id);
        assert_eq!(loaded[0].state, SandboxState::Paused);
        assert_eq!(loaded[0].virtualization_mode, VirtualizationMode::Kvm);
        assert!(loaded[0].paused_state.is_none());
        assert!(has_record(&pvm_persister, &sandbox_id).await?);
        assert!(snapshot_root.exists());
        Ok(())
    }

    #[tokio::test]
    async fn mixed_mode_records_are_both_visible_and_retained() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let kvm_persister = test_persister(temp.path());
        let kvm_root = temp.path().join("kvm-artifacts");
        let (kvm_id, _kvm_state) = persist_test_record(&kvm_persister, &kvm_root).await?;
        drop(kvm_persister);

        let pvm_persister =
            FileBackedSandboxPersister::new(temp.path().to_path_buf(), VirtualizationMode::Pvm)
                .with_durability(RecordDurability::Memory);
        let pvm_root = temp.path().join("pvm-artifacts");
        let (pvm_id, _pvm_state) = persist_test_record(&pvm_persister, &pvm_root).await?;

        let mut loaded = pvm_persister.load_all(&MockBackendFactory::new()).await?;
        loaded.sort_by_key(|metadata| metadata.id);

        let kvm_metadata = loaded
            .iter()
            .find(|metadata| metadata.id == kvm_id)
            .expect("KVM metadata should remain visible");
        assert_eq!(kvm_metadata.virtualization_mode, VirtualizationMode::Kvm);
        assert!(kvm_metadata.paused_state.is_none());

        let pvm_metadata = loaded
            .iter()
            .find(|metadata| metadata.id == pvm_id)
            .expect("PVM metadata should load");
        assert_eq!(pvm_metadata.virtualization_mode, VirtualizationMode::Pvm);
        assert!(pvm_metadata.paused_state.is_some());

        assert!(has_record(&pvm_persister, &kvm_id).await?);
        assert!(has_record(&pvm_persister, &pvm_id).await?);
        assert!(kvm_root.exists());
        assert!(pvm_root.exists());
        Ok(())
    }

    #[tokio::test]
    async fn allocate_artifact_root_creates_unique_snapshot_roots() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let sandbox_id = SandboxId::new();

        let first_root = persister
            .allocate_artifact_root(&sandbox_id)
            .await?
            .expect("file-backed persister should allocate artifact root");
        let second_root = persister
            .allocate_artifact_root(&sandbox_id)
            .await?
            .expect("file-backed persister should allocate artifact root");
        let sandbox_id_dir = sandbox_id.to_string();

        assert_ne!(first_root, second_root);
        assert!(first_root.is_dir());
        assert!(second_root.is_dir());
        assert_eq!(
            first_root.parent().and_then(Path::file_name),
            Some(std::ffi::OsStr::new(&sandbox_id_dir))
        );
        assert_eq!(
            first_root
                .parent()
                .and_then(Path::parent)
                .and_then(Path::file_name),
            Some(std::ffi::OsStr::new("artifacts"))
        );
        Ok(())
    }

    #[tokio::test]
    async fn resuming_records_are_cleaned_on_load() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let sandbox_id = SandboxId::new();
        let snapshot_root = persister
            .sandbox_artifact_root(&sandbox_id)
            .join("snapshot");
        let paused_state = paused_state(&snapshot_root);
        let metadata = SandboxMetadata {
            id: sandbox_id,
            paused_state: Some(Arc::clone(&paused_state)),
            ..Default::default()
        };

        persister
            .persist_paused(&metadata, Some(&snapshot_root), paused_state.as_ref())
            .await?;
        persister.mark_resuming(&metadata.id).await?;

        let loaded = persister.load_all(&MockBackendFactory::new()).await?;

        assert!(loaded.is_empty());
        assert!(!has_record(&persister, &metadata.id).await?);
        assert!(!persister.sandbox_artifact_root(&metadata.id).exists());
        Ok(())
    }

    #[tokio::test]
    async fn persist_paused_accepts_backend_agnostic_state() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let snapshot_root = temp.path().join("artifacts");
        let paused_state = paused_state(&snapshot_root);
        let metadata = SandboxMetadata::default();

        persister
            .persist_paused(&metadata, Some(&snapshot_root), paused_state.as_ref())
            .await?;
        drop(paused_state);

        assert!(snapshot_root.exists());
        Ok(())
    }

    #[tokio::test]
    async fn persist_paused_cleans_artifacts_when_encode_fails() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let snapshot_root = temp.path().join("artifacts");
        tokio::fs::create_dir_all(&snapshot_root).await?;
        let paused_state: Arc<dyn PausedSandboxState> = Arc::new(FailingEncodeState);
        let err = persister
            .persist_paused(
                &SandboxMetadata::default(),
                Some(&snapshot_root),
                paused_state.as_ref(),
            )
            .await
            .expect_err("encode failure should reject paused state");

        assert!(matches!(err, SandboxPersistenceError::InvalidRecord { .. }));
        assert!(!snapshot_root.exists());
        Ok(())
    }

    #[tokio::test]
    async fn records_are_not_cluster_registered_until_marked() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let snapshot_root = temp.path().join("artifacts");
        let (sandbox_id, _paused_state) = persist_test_record(&persister, &snapshot_root).await?;

        assert_eq!(
            persister.cluster_registration(&sandbox_id).await?,
            ClusterRegistration::Never
        );

        persister
            .mark_cluster_registered(&sandbox_id, "node-a")
            .await?;

        assert_eq!(
            persister.cluster_registration(&sandbox_id).await?,
            ClusterRegistration::As("node-a".to_string())
        );

        Ok(())
    }

    #[tokio::test]
    async fn records_registered_before_the_identity_was_stored_load_as_anonymous(
    ) -> anyhow::Result<()> {
        let legacy = serde_json::json!({
            "version": RECORD_VERSION,
            "lifecycle": "paused",
            "metadata": SandboxMetadata::default(),
            "artifactRoot": "/tmp/does-not-matter",
            "state": {},
            "clusterRegistered": true,
        });

        let record = decode_record(&serde_json::to_vec(&legacy)?)?;

        assert!(record.cluster_registered);
        assert_eq!(record.registered_as, None);

        Ok(())
    }

    #[tokio::test]
    async fn records_written_before_the_flag_existed_load_as_unregistered() -> anyhow::Result<()> {
        let legacy = serde_json::json!({
            "version": RECORD_VERSION,
            "lifecycle": "paused",
            "metadata": SandboxMetadata::default(),
            "artifactRoot": "/tmp/does-not-matter",
            "state": {},
        });

        let record = decode_record(&serde_json::to_vec(&legacy)?)?;

        assert!(!record.cluster_registered);

        Ok(())
    }

    #[tokio::test]
    async fn mark_resuming_and_rollback_preserve_loadability() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let snapshot_root = temp.path().join("artifacts");
        let (sandbox_id, _paused_state) = persist_test_record(&persister, &snapshot_root).await?;

        persister.mark_resuming(&sandbox_id).await?;
        assert_eq!(
            persister.get_record(&sandbox_id).await?.lifecycle,
            PersistedPausedLifecycle::Resuming
        );

        persister.rollback_resuming(&sandbox_id).await?;
        let loaded = persister.load_all(&MockBackendFactory::new()).await?;

        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].id, sandbox_id);
        assert!(snapshot_root.exists());
        Ok(())
    }

    #[tokio::test]
    async fn delete_record_removes_record_but_keeps_artifacts() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let snapshot_root = temp.path().join("artifacts");
        let (sandbox_id, _paused_state) = persist_test_record(&persister, &snapshot_root).await?;

        persister.delete_record(&sandbox_id).await?;

        assert!(!has_record(&persister, &sandbox_id).await?);
        assert!(snapshot_root.exists());
        Ok(())
    }

    #[tokio::test]
    async fn load_all_removes_orphan_artifacts_without_records() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let sandbox_id = SandboxId::new();
        let artifact_root = persister
            .sandbox_artifact_root(&sandbox_id)
            .join("resumed-generation");
        tokio::fs::create_dir_all(&artifact_root).await?;

        let loaded = persister.load_all(&MockBackendFactory::new()).await?;

        assert!(loaded.is_empty());
        assert!(!persister.sandbox_artifact_root(&sandbox_id).exists());
        Ok(())
    }

    #[tokio::test]
    async fn load_all_keeps_artifacts_for_valid_paused_record() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let sandbox_id = SandboxId::new();
        let snapshot_root = persister
            .sandbox_artifact_root(&sandbox_id)
            .join("paused-generation");
        let paused_state = paused_state(&snapshot_root);
        let metadata = SandboxMetadata {
            id: sandbox_id,
            paused_state: Some(Arc::clone(&paused_state)),
            ..Default::default()
        };
        persister
            .persist_paused(&metadata, Some(&snapshot_root), paused_state.as_ref())
            .await?;

        let loaded = persister.load_all(&MockBackendFactory::new()).await?;

        assert_eq!(loaded.len(), 1);
        assert!(persister.sandbox_artifact_root(&sandbox_id).exists());
        Ok(())
    }

    #[tokio::test]
    async fn delete_record_and_artifacts_removes_both() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let sandbox_id = SandboxId::new();
        let snapshot_root = persister
            .sandbox_artifact_root(&sandbox_id)
            .join("snapshot");
        let paused_state = paused_state(&snapshot_root);
        let metadata = SandboxMetadata {
            id: sandbox_id,
            paused_state: Some(Arc::clone(&paused_state)),
            ..Default::default()
        };
        persister
            .persist_paused(&metadata, Some(&snapshot_root), paused_state.as_ref())
            .await?;

        persister.delete_record_and_artifacts(&sandbox_id).await?;

        assert!(!has_record(&persister, &sandbox_id).await?);
        assert!(!persister.sandbox_artifact_root(&sandbox_id).exists());
        Ok(())
    }

    #[tokio::test]
    async fn delete_record_and_artifacts_removes_artifacts_without_record() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let sandbox_id = SandboxId::new();
        let sandbox_artifact_root = persister.sandbox_artifact_root(&sandbox_id);
        tokio::fs::create_dir_all(sandbox_artifact_root.join("stale-generation")).await?;

        persister.delete_record_and_artifacts(&sandbox_id).await?;

        assert!(!sandbox_artifact_root.exists());
        Ok(())
    }

    #[tokio::test]
    async fn delete_record_and_artifacts_removes_invalid_record() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let sandbox_id = SandboxId::new();
        persister
            .records()
            .await?
            .put(&sandbox_id.to_string(), b"not-json")
            .await?;

        persister.delete_record_and_artifacts(&sandbox_id).await?;

        assert!(!has_record(&persister, &sandbox_id).await?);
        Ok(())
    }

    #[tokio::test]
    async fn load_all_discards_invalid_record() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let sandbox_id = SandboxId::new();
        persister
            .records()
            .await?
            .put(&sandbox_id.to_string(), b"not-json")
            .await?;

        let loaded = persister.load_all(&MockBackendFactory::new()).await?;

        assert!(loaded.is_empty());
        assert!(!has_record(&persister, &sandbox_id).await?);
        Ok(())
    }

    #[tokio::test]
    async fn load_all_keeps_and_reports_a_record_that_predates_executions() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let sandbox_id = SandboxId::new();

        let mut record = serde_json::to_value(PersistedPausedRecord {
            version: RECORD_VERSION,
            lifecycle: PersistedPausedLifecycle::Paused,
            metadata: SandboxMetadata {
                id: sandbox_id,
                ..Default::default()
            },
            artifact_root: temp.path().join("artifacts"),
            state: serde_json::json!({}),
            cluster_registered: false,
            registered_as: None,
        })?;
        record["metadata"]
            .as_object_mut()
            .expect("metadata is an object")
            .remove("execution_id")
            .expect("the field is there to remove");
        persister
            .records()
            .await?
            .put(&sandbox_id.to_string(), serde_json::to_vec(&record)?)
            .await?;

        let loaded = persister.load_all(&MockBackendFactory::new()).await?;

        assert!(loaded.is_empty(), "the record must not load");
        assert!(
            has_record(&persister, &sandbox_id).await?,
            "a record that predates incarnations must be kept for an operator to clear, \
             never silently deleted"
        );
        Ok(())
    }

    #[tokio::test]
    async fn load_all_discards_unusable_record_and_artifacts() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let sandbox_id = SandboxId::new();
        let snapshot_root = persister
            .sandbox_artifact_root(&sandbox_id)
            .join("snapshot");
        let paused_state = paused_state(&snapshot_root);
        let metadata = SandboxMetadata {
            id: sandbox_id,
            paused_state: Some(Arc::clone(&paused_state)),
            ..Default::default()
        };
        persister
            .persist_paused(&metadata, Some(&snapshot_root), paused_state.as_ref())
            .await?;

        let loaded = persister.load_all(&RejectingFactory).await?;

        assert!(loaded.is_empty());
        assert!(!has_record(&persister, &sandbox_id).await?);
        assert!(!persister.sandbox_artifact_root(&sandbox_id).exists());
        Ok(())
    }

    #[tokio::test]
    async fn each_record_is_one_json_file_named_for_its_sandbox() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let snapshot_root = temp.path().join("artifacts");
        let (sandbox_id, _paused_state) = persist_test_record(&persister, &snapshot_root).await?;

        let record_file = persister.records_path().join(format!("{sandbox_id}.json"));
        let stored: PersistedPausedRecord =
            serde_json::from_slice(&tokio::fs::read(&record_file).await?)?;

        assert_eq!(stored.version, RECORD_VERSION);
        assert_eq!(stored.metadata.id, sandbox_id);

        persister.delete_record(&sandbox_id).await?;
        assert!(!record_file.exists());
        Ok(())
    }

    #[tokio::test]
    async fn a_store_this_build_cannot_read_is_discarded_on_load() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let legacy = persister.legacy_records_db_path();
        tokio::fs::create_dir_all(&legacy).await?;
        tokio::fs::write(legacy.join("CURRENT"), b"opaque").await?;
        let stranded = persister.sandbox_artifact_root(&SandboxId::new());
        tokio::fs::create_dir_all(&stranded).await?;

        let loaded = persister.load_all(&MockBackendFactory::new()).await?;

        assert!(loaded.is_empty());
        assert!(!legacy.exists());
        assert!(
            !stranded.exists(),
            "artifacts no surviving record retains must be cleaned up like any other orphan"
        );
        Ok(())
    }

    #[tokio::test]
    async fn discarding_an_unreadable_store_leaves_current_records_alone() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let seeder = test_persister(temp.path());
        let snapshot_root = temp.path().join("artifacts");
        let (sandbox_id, _paused_state) = persist_test_record(&seeder, &snapshot_root).await?;
        tokio::fs::create_dir_all(seeder.legacy_records_db_path()).await?;

        let persister = test_persister(temp.path());
        let loaded = persister.load_all(&MockBackendFactory::new()).await?;

        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].id, sandbox_id);
        assert!(!persister.legacy_records_db_path().exists());
        Ok(())
    }
}
