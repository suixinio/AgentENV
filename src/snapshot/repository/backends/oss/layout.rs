use crate::snapshot::SnapshotId;

/// Committed object layout for the OSS snapshot backend.
///
/// 🔴 Artifacts only — see `posixfs::layout`'s counterpart note. The
/// `catalog/records/` and `catalog/aliases/` keys this also produced are gone
/// with the object-storage catalog; existing buckets still hold those objects,
/// frozen at the cutover and read by nothing.
pub struct OssSnapshotArtifactLayout<'a> {
    snapshot_id: &'a SnapshotId,
}

impl<'a> OssSnapshotArtifactLayout<'a> {
    pub fn new(snapshot_id: &'a SnapshotId) -> Self {
        Self { snapshot_id }
    }

    pub fn managed_layer_key(digest: &str) -> String {
        format!("managed-layers/{digest}")
    }

    pub fn artifact_prefix(&self) -> String {
        format!("artifacts/{}/", self.snapshot_id)
    }

    pub fn artifact_key(&self, relative_path: &str) -> String {
        format!("{}{}", self.artifact_prefix(), relative_path)
    }
}
