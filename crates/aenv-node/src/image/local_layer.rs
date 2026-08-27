use std::path::{Path, PathBuf};

use overlaybd::config::LayerConfig;

const SNAPSHOT_DELTA_LAYER_FILE: &str = "snapshot.commit";
const SELF_CONTAINED_BASE_LAYER_FILE: &str = "managed-base.commit";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalLayer {
    pub path: PathBuf,
    pub digest: String,
    pub size: u64,
}

pub fn rootfs_layer_is_runtime_generated_delta(path: &Path) -> bool {
    matches!(
        path.file_name().and_then(|name| name.to_str()),
        Some(SNAPSHOT_DELTA_LAYER_FILE | SELF_CONTAINED_BASE_LAYER_FILE)
    )
}

impl From<LocalLayer> for LayerConfig {
    fn from(layer: LocalLayer) -> Self {
        Self {
            file: layer.path.display().to_string(),
            digest: layer.digest,
            size: layer.size,
            ..Default::default()
        }
    }
}
