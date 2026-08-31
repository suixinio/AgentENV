//! Opaque parameters handed through to the optional custom extension.

/// Custom extension params: an opaque JSON object interpreted only by the
/// custom extension. `None` and an empty map are equivalent (empty params).
pub type CustomExtensionParams = serde_json::Map<String, serde_json::Value>;
