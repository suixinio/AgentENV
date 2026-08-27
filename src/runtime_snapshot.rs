//! The node-local, resolved view of a committed snapshot.
//!
//! # 🔴 Why this is not part of `crate::snapshot`
//!
//! `crate::snapshot` is the catalog model: rows, layers, aliases, publication
//! metadata — data every role reads, including the one that runs no sandbox.
//! The types here are the other half: what a snapshot looks like *after* a node
//! has downloaded its bytes, materialized overlaybd image configs and taken a
//! lease over them. They are node-only by construction — since `aenv-api`
//! stopped resolving snapshots into local bytes, the api half never builds one.
//!
//! They lived in `crate::snapshot::types` next to the catalog model, which is
//! why `crate::sandbox` had to reach into `crate::snapshot` to name the input
//! its factories take. Splitting them into their own module is what lets the
//! two halves land in different crates later: a crate boundary can move a
//! module, not half a file.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[cfg(any(test, feature = "test-support"))]
use std::sync::OnceLock;

use serde::{Deserialize, Serialize};

use crate::snapshot::{CommittedSnapshot, SnapshotRecord};
use crate::types::{ExtraDrive, FirecrackerSnapshotManifest, SandboxResources};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
/// Attached-drive state resolved into local runtime inputs for the current node.
pub enum ResolvedAttachedDrive {
    /// Node-local OverlayBD drive runtime input.
    ///
    /// These absolute paths are valid only on the current node after repository
    /// resolution. They must not be persisted directly in committed snapshot
    /// metadata.
    Overlaybd {
        drive_id: String,
        image_config_path: PathBuf,
        read_only: bool,
        virtual_size: u64,
        #[serde(default)]
        mount_path: PathBuf,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sub_path: Option<PathBuf>,
    },
}

impl ResolvedAttachedDrive {
    pub fn to_extra_drive(&self) -> ExtraDrive {
        match self {
            Self::Overlaybd {
                drive_id,
                image_config_path,
                read_only,
                virtual_size,
                mount_path,
                sub_path,
            } => ExtraDrive::Overlaybd {
                drive_id: drive_id.clone(),
                image_config_path: image_config_path.clone(),
                read_only: *read_only,
                mount_path: mount_path.clone(),
                virtual_size: Some(*virtual_size),
                sub_path: sub_path.clone(),
            },
        }
    }

    /// Returns the image config path.
    pub fn image_config_path(&self) -> &Path {
        match self {
            Self::Overlaybd {
                image_config_path, ..
            } => image_config_path,
        }
    }

    /// Returns the drive id.
    pub fn drive_id(&self) -> &str {
        match self {
            Self::Overlaybd { drive_id, .. } => drive_id,
        }
    }

    /// Returns whether the drive should be attached read-only.
    pub fn read_only(&self) -> bool {
        match self {
            Self::Overlaybd { read_only, .. } => *read_only,
        }
    }

    pub fn virtual_size(&self) -> u64 {
        match self {
            Self::Overlaybd { virtual_size, .. } => *virtual_size,
        }
    }

    pub fn mount_path(&self) -> PathBuf {
        match self {
            Self::Overlaybd { mount_path, .. } => mount_path.clone(),
        }
    }
}

pub trait RuntimeArtifactLease: Send + Sync {}

#[derive(Clone, Default)]
#[cfg(any(test, feature = "test-support"))]
struct EmptyRuntimeArtifactLease;

#[cfg(any(test, feature = "test-support"))]
impl RuntimeArtifactLease for EmptyRuntimeArtifactLease {}

#[cfg(any(test, feature = "test-support"))]
fn default_runtime_artifact_lease() -> Arc<dyn RuntimeArtifactLease> {
    static INSTANCE: OnceLock<Arc<dyn RuntimeArtifactLease>> = OnceLock::new();
    INSTANCE
        .get_or_init(|| Arc::new(EmptyRuntimeArtifactLease))
        .clone()
}

#[derive(Clone)]
/// Runtime-ready snapshot with node-local artifact paths.
pub struct RunnableSnapshot {
    record: SnapshotRecord,
    manifest: FirecrackerSnapshotManifest,
    _lease: Arc<dyn RuntimeArtifactLease>,
}

impl RunnableSnapshot {
    pub fn new(
        record: SnapshotRecord,
        manifest: FirecrackerSnapshotManifest,
        lease: Arc<dyn RuntimeArtifactLease>,
    ) -> Self {
        Self {
            record,
            manifest,
            _lease: lease,
        }
    }

    /// Returns the runtime-resolved attached drives for this snapshot.
    pub fn attached_drives(&self) -> Vec<ResolvedAttachedDrive> {
        self.manifest
            .attached_drives
            .iter()
            .map(|drive| ResolvedAttachedDrive::Overlaybd {
                drive_id: drive.drive_id.clone(),
                image_config_path: drive.image_config_path.clone(),
                read_only: drive.read_only,
                virtual_size: drive.virtual_size,
                mount_path: crate::types::normalize_mount_path_for_drive(
                    &drive.drive_id,
                    drive.mount_path.clone(),
                )
                .unwrap_or_else(|_| crate::types::ExtraDrive::default_mount_path(&drive.drive_id)),
                sub_path: drive.sub_path.clone(),
            })
            .collect()
    }

    pub fn manifest(&self) -> &FirecrackerSnapshotManifest {
        &self.manifest
    }

    /// Returns the committed snapshot record backing this runnable snapshot.
    pub fn record(&self) -> &SnapshotRecord {
        &self.record
    }

    /// Returns the committed snapshot artifact payload backing this runnable snapshot.
    pub fn committed(&self) -> &CommittedSnapshot {
        self.record
            .committed
            .as_ref()
            .expect("runnable snapshots always have committed artifact payloads")
    }

    /// Returns the CPU and memory settings for this runnable snapshot.
    pub fn resources(&self) -> &SandboxResources {
        &self.record.resources
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn mock() -> Self {
        Self::from_test_manifest(
            SnapshotRecord::mock_ready(CommittedSnapshot::mock()),
            Vec::new(),
        )
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn from_test_manifest(
        record: SnapshotRecord,
        attached_drives: Vec<ResolvedAttachedDrive>,
    ) -> Self {
        let extra_drives: Vec<crate::types::ExtraDrive> = attached_drives
            .iter()
            .map(ResolvedAttachedDrive::to_extra_drive)
            .collect();

        Self {
            record,
            manifest: FirecrackerSnapshotManifest::for_test(0, &extra_drives),
            _lease: default_runtime_artifact_lease(),
        }
    }
}

impl fmt::Debug for RunnableSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RunnableSnapshot")
            .field("record", &self.record)
            .field("manifest", &self.manifest)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use super::ResolvedAttachedDrive;
    use crate::snapshot::CommittedAttachedDrive;
    use crate::types::ExtraDrive;

    fn sample_resolved() -> ResolvedAttachedDrive {
        ResolvedAttachedDrive::Overlaybd {
            drive_id: "data".to_string(),
            image_config_path: PathBuf::from("/tmp/image.json"),
            read_only: false,
            virtual_size: 4096,
            mount_path: ExtraDrive::default_mount_path("data"),
            sub_path: None,
        }
    }

    #[test]
    fn to_extra_drive_preserves_fields() {
        let extra_drive = sample_resolved().to_extra_drive();

        assert_eq!(extra_drive.drive_id(), "data");
        assert!(!extra_drive.read_only());
        assert_eq!(extra_drive.mount_path(), Path::new("/mnt/data"));
        assert_eq!(extra_drive.virtual_size(), Some(4096));
        assert_eq!(extra_drive.sub_path(), None);
    }

    #[test]
    fn to_extra_drive_propagates_sub_path() {
        let mut drive = sample_resolved();
        let ResolvedAttachedDrive::Overlaybd { sub_path, .. } = &mut drive;
        *sub_path = Some(PathBuf::from("workspace/data"));

        let extra_drive = drive.to_extra_drive();
        assert_eq!(extra_drive.sub_path(), Some(Path::new("workspace/data")));
    }

    #[test]
    fn attached_drive_virtual_size_is_required() {
        let resolved = serde_json::from_value::<ResolvedAttachedDrive>(serde_json::json!({
            "Overlaybd": {
                "drive_id": "data",
                "image_config_path": "/tmp/image.json",
                "read_only": true
            }
        }))
        .expect_err("resolved attached drive should require virtual_size");
        assert!(resolved.to_string().contains("virtual_size"));

        let committed = serde_json::from_value::<CommittedAttachedDrive>(serde_json::json!({
            "Overlaybd": {
                "drive_id": "data",
                "layers": [],
                "read_only": true
            }
        }))
        .expect_err("committed attached drive should require virtual_size");
        assert!(committed.to_string().contains("virtual_size"));
    }

    #[test]
    fn attached_drive_virtual_size_is_serialized() {
        let resolved_json = serde_json::to_value(sample_resolved()).unwrap();
        assert_eq!(resolved_json["Overlaybd"]["virtual_size"], 4096);

        let committed = CommittedAttachedDrive::Overlaybd {
            drive_id: "data".to_string(),
            layers: Vec::new(),
            read_only: true,
            virtual_size: 4096,
            mount_path: ExtraDrive::default_mount_path("data"),
            sub_path: None,
        };
        let committed_json = serde_json::to_value(&committed).unwrap();
        assert_eq!(committed_json["Overlaybd"]["virtual_size"], 4096);
    }
}
