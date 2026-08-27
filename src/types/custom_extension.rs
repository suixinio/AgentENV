//! Opaque parameters handed through to the optional custom extension.

/// Custom extension params: an opaque JSON object interpreted only by the
/// custom extension. `None` and an empty map are equivalent (empty params).
///
/// 🔴 Defined here rather than beside the hook client so that
/// `crate::snapshot` can persist the value without depending on
/// `crate::sandbox`. `crate::sandbox` re-exports it.
pub type CustomExtensionParams = serde_json::Map<String, serde_json::Value>;
