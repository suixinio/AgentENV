use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use super::snapshot::OverlaybdLayerRef;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommittedAttachedDrive {
    Overlaybd {
        drive_id: String,
        layers: Vec<OverlaybdLayerRef>,
        read_only: bool,
        virtual_size: u64,
        #[serde(default)]
        mount_path: PathBuf,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sub_path: Option<PathBuf>,
    },
}
