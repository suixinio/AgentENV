use crate::snapshot::SnapshotId;

/// Committed object layout for the OSS snapshot backend.
pub struct OssSnapshotArtifactLayout<'a> {
    snapshot_id: &'a SnapshotId,
}

impl<'a> OssSnapshotArtifactLayout<'a> {
    pub fn new(snapshot_id: &'a SnapshotId) -> Self {
        Self { snapshot_id }
    }

    pub fn alias_key(alias: &str) -> String {
        format!("catalog/aliases/{alias}.json")
    }

    pub fn record_key(id: &SnapshotId) -> String {
        format!("catalog/records/{id}.json")
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
