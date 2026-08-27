use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::ImageBaseContext;

pub fn env_vars_from_entries(entries: &[String]) -> HashMap<String, String> {
    // Docker-compatible last-wins behavior for duplicate ENV keys.
    entries
        .iter()
        .filter_map(|entry| {
            let (key, value) = entry.split_once('=')?;
            (!key.is_empty()).then(|| (key.to_string(), value.to_string()))
        })
        .collect()
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImageResolutionMetadata {
    #[serde(default)]
    pub base_context: ImageBaseContext,
    /// Raw source image config JSON, preserved as-is for transparent pass-through
    /// to consumers (e.g. MMDS metadata) without field-level interpretation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw_config: Option<serde_json::Value>,
}
