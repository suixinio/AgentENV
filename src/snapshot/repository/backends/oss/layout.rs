use crate::snapshot::SnapshotId;

/// Committed object layout for the OSS snapshot backend.
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

    /// A memory image lineage's working-set list. Keyed by the bottom memory
    /// layer, which a template and every pause row descended from it share,
    /// so a pause row resolves the list the template's first resume recorded.
    pub fn mem_prefetch_key(base_memory_layer_digest: &str) -> String {
        format!("mem-prefetch/{base_memory_layer_digest}.json")
    }

    pub fn artifact_prefix(&self) -> String {
        format!("artifacts/{}/", self.snapshot_id)
    }

    pub fn artifact_key(&self, relative_path: &str) -> String {
        format!("{}{}", self.artifact_prefix(), relative_path)
    }
}
