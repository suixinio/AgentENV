//! Versioned Redis record preserving metadata fields skipped by plain serde.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use super::super::{Result, SandboxMetadata, StoreError};
use crate::types::{ExecutionId, SandboxId};

/// Current stored-record schema version.
pub const RECORD_VERSION: u32 = 2;

/// Versioned JSON stored under a sandbox record key.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StoredSandboxRecord {
    /// Required schema version.
    pub version: u32,

    /// Monotonic revision used by read-modify-write CAS.
    pub rev: u64,

    /// Flattened so Lua can read state and execution predicates directly.
    #[serde(flatten)]
    pub metadata: SandboxMetadata,

    /// Lua-comparable expiry milliseconds derived by [`StoredSandboxRecord::new`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at_ms: Option<i64>,

    /// Current running-interval start retained separately from skipped metadata.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub running_since_ms: Option<i64>,

    /// Node running the sandbox.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin_node_id: Option<String>,

    /// Whether bytes are durable beyond the origin node; defaults fail-closed.
    #[serde(default)]
    pub published: bool,
}

impl StoredSandboxRecord {
    pub fn new(metadata: &SandboxMetadata, rev: u64) -> Result<Self> {
        Ok(Self {
            version: RECORD_VERSION,
            rev,
            expires_at_ms: metadata.expires_at.map(to_unix_millis),
            running_since_ms: metadata.running_since.map(to_unix_millis),
            metadata: metadata.clone(),
            origin_node_id: None,
            published: false,
        })
    }

    /// Preserves placement fields absent from [`SandboxMetadata`].
    pub fn inherit_placement_from(&mut self, previous: &Self) {
        if self.origin_node_id.is_none() {
            self.origin_node_id = previous.origin_node_id.clone();
        }
        self.published = self.published || previous.published;
    }

    pub fn sandbox_id(&self) -> SandboxId {
        self.metadata.id
    }

    pub fn execution_id(&self) -> ExecutionId {
        self.metadata.execution_id
    }

    /// Derives a positive, rounded record TTL beyond the sandbox deadline.
    pub fn record_ttl(&self, now: SystemTime, grace: Duration) -> Option<Duration> {
        let deadline = self.metadata.lifetime_deadline(now)?;
        let remaining = deadline.duration_since(now).unwrap_or(Duration::ZERO);
        let secs = remaining
            .as_secs()
            .saturating_add(u64::from(remaining.subsec_nanos() > 0))
            .saturating_add(grace.as_secs())
            .max(1);
        Some(Duration::from_secs(secs))
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(self).map_err(|source| StoreError::Backend {
            source: anyhow::Error::from(source).context("failed to encode sandbox record"),
        })
    }

    /// Decode failures are backend errors, never absence.
    pub fn decode(raw: &[u8]) -> Result<Self> {
        let mut record: Self =
            serde_json::from_slice(raw).map_err(|source| StoreError::Backend {
                source: anyhow::Error::from(source).context("failed to decode sandbox record"),
            })?;
        ensure_supported_version(record.version)?;
        // Restore fields intentionally skipped by `SandboxMetadata`.
        record.metadata.running_since = record.running_since_ms.map(from_unix_millis);
        Ok(record)
    }

    pub fn into_metadata(self) -> SandboxMetadata {
        self.metadata
    }
}

/// Versioned active-state payload stored opaquely by nodes for store-loss recovery.
#[derive(Clone, Debug)]
pub struct ActiveStateRecord(StoredSandboxRecord);

impl ActiveStateRecord {
    pub fn of(record: &StoredSandboxRecord) -> Self {
        Self(record.clone())
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        self.0.encode()
    }

    /// Rejects unsupported record versions.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        Ok(Self(StoredSandboxRecord::decode(bytes)?))
    }

    pub fn metadata(&self) -> &SandboxMetadata {
        &self.0.metadata
    }

    /// Resets revision to one for insertion into a rebuilt store.
    pub fn into_record(mut self) -> StoredSandboxRecord {
        self.0.rev = 1;
        self.0
    }
}

pub fn ensure_supported_version(version: u32) -> Result<()> {
    if version == 0 || version > RECORD_VERSION {
        return Err(StoreError::Backend {
            source: anyhow::anyhow!(
                "sandbox record version {version} is not supported by this build (understands 1..={RECORD_VERSION}); \
                 a newer replica wrote it, or the key namespace is being shared with something else"
            ),
        });
    }
    Ok(())
}

pub fn from_unix_millis(millis: i64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(millis.max(0) as u64)
}

pub fn to_unix_millis(time: SystemTime) -> i64 {
    match time.duration_since(UNIX_EPOCH) {
        Ok(delta) => delta.as_millis().min(i64::MAX as u128) as i64,
        // Clamp impossible pre-epoch values to preserve ordering.
        Err(_) => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orchestrator::SandboxState;
    use serde_json::Value;

    fn metadata() -> SandboxMetadata {
        SandboxMetadata {
            state: SandboxState::Running,
            ..Default::default()
        }
    }

    #[test]
    fn execution_and_state_are_top_level_in_the_encoded_json() {
        let record = StoredSandboxRecord::new(&metadata(), 7).unwrap();
        let json: Value = serde_json::from_slice(&record.encode().unwrap()).unwrap();
        assert_eq!(
            json.get("execution_id").and_then(Value::as_str),
            Some(record.metadata.execution_id.to_string().as_str())
        );
        assert_eq!(json.get("state").and_then(Value::as_str), Some("Running"));
        assert_eq!(json.get("rev").and_then(Value::as_u64), Some(7));
        assert_eq!(
            json.get("version").and_then(Value::as_u64),
            Some(u64::from(RECORD_VERSION))
        );
    }

    #[test]
    fn a_record_written_before_the_brokered_egress_fields_existed_still_decodes() {
        let mut json: Value = serde_json::from_slice(
            &StoredSandboxRecord::new(&metadata(), 3)
                .unwrap()
                .encode()
                .unwrap(),
        )
        .unwrap();
        json["version"] = serde_json::json!(1);
        let raw = serde_json::to_vec(&json).unwrap();

        let decoded =
            StoredSandboxRecord::decode(&raw).expect("a version 1 record must still load");
        assert_eq!(decoded.version, 1);
        assert!(decoded.metadata.network_policy.egress.rules.is_empty());
        assert!(decoded.metadata.network_policy.egress.brokers.is_empty());
    }

    #[test]
    fn the_running_clock_survives_the_round_trip() {
        let started = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let mut sandbox = metadata();
        sandbox.running_since = Some(started);

        let encoded = StoredSandboxRecord::new(&sandbox, 1)
            .unwrap()
            .encode()
            .unwrap();
        let decoded = StoredSandboxRecord::decode(&encoded).unwrap();
        assert_eq!(decoded.metadata.running_since, Some(started));

        let bytes = serde_json::to_vec(&sandbox).unwrap();
        let back: SandboxMetadata = serde_json::from_slice(&bytes).unwrap();
        assert!(back.running_since.is_none());
    }

    #[test]
    fn a_restored_running_clock_keeps_the_lifetime_deadline_pinned() {
        let started = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let mut sandbox = metadata();
        sandbox.max_lifetime = Some(Duration::from_secs(600));
        sandbox.running_since = Some(started);

        let encoded = StoredSandboxRecord::new(&sandbox, 1)
            .unwrap()
            .encode()
            .unwrap();
        let decoded = StoredSandboxRecord::decode(&encoded)
            .unwrap()
            .into_metadata();

        let later = started + Duration::from_secs(120);
        assert_eq!(
            decoded.lifetime_deadline(later),
            Some(started + Duration::from_secs(600)),
            "the deadline must stay anchored to the start of the run"
        );
    }

    #[test]
    fn the_ownership_marker_survives_the_stored_record() {
        use crate::orchestrator::store::ControlPlaneConfig;

        let mut sandbox = metadata();
        sandbox.control_plane_config = ControlPlaneConfig::from_bytes(vec![0, 1, 254, 255, 7]);

        let encoded = StoredSandboxRecord::new(&sandbox, 1)
            .unwrap()
            .encode()
            .unwrap();
        let json: Value = serde_json::from_slice(&encoded).unwrap();
        assert!(
            json.get("control_plane_config")
                .and_then(Value::as_str)
                .is_some(),
            "the marker must be a string in the record, not a byte array: {json}"
        );

        let decoded = StoredSandboxRecord::decode(&encoded).unwrap();
        assert_eq!(
            decoded.metadata.control_plane_config,
            sandbox.control_plane_config
        );
    }

    #[test]
    fn a_record_written_before_the_ownership_marker_existed_still_decodes() {
        let encoded = StoredSandboxRecord::new(&metadata(), 1)
            .unwrap()
            .encode()
            .unwrap();
        let mut json: Value = serde_json::from_slice(&encoded).unwrap();
        json.as_object_mut().unwrap().remove("control_plane_config");
        let raw = serde_json::to_vec(&json).unwrap();

        let decoded = StoredSandboxRecord::decode(&raw).expect("an older record must still load");
        assert!(decoded.metadata.control_plane_config.is_none());
    }

    #[test]
    fn the_active_state_record_round_trips_and_restarts_the_revision() {
        let mut sandbox = metadata();
        sandbox.max_lifetime = Some(Duration::from_secs(600));
        sandbox.running_since = Some(UNIX_EPOCH + Duration::from_secs(1_700_000_000));
        sandbox.snapshot_id = "tpl-42".to_string();

        let mut stored = StoredSandboxRecord::new(&sandbox, 97).unwrap();
        stored.origin_node_id = Some("node-b".to_string());
        stored.published = true;

        let bytes = ActiveStateRecord::of(&stored).encode().unwrap();
        let rebuilt = ActiveStateRecord::decode(&bytes).unwrap();
        assert_eq!(rebuilt.metadata().snapshot_id, "tpl-42");
        assert_eq!(rebuilt.metadata().running_since, sandbox.running_since);

        let record = rebuilt.into_record();
        assert_eq!(
            record.rev, 1,
            "the revision counted writes to a store that is gone"
        );
        assert_eq!(record.origin_node_id.as_deref(), Some("node-b"));
        assert!(record.published);
        assert_eq!(record.metadata.max_lifetime, Some(Duration::from_secs(600)));
    }

    #[test]
    fn an_active_state_record_from_a_newer_build_is_refused() {
        let stored = StoredSandboxRecord::new(&metadata(), 1).unwrap();
        let mut json: Value = serde_json::from_slice(&stored.encode().unwrap()).unwrap();
        json["version"] = serde_json::json!(RECORD_VERSION + 1);
        assert!(ActiveStateRecord::decode(&serde_json::to_vec(&json).unwrap()).is_err());
    }

    #[test]
    fn expires_at_ms_tracks_expires_at() {
        let mut sandbox = metadata();
        sandbox.set_timeout(Some(Duration::from_secs(60)));
        let record = StoredSandboxRecord::new(&sandbox, 1).unwrap();
        assert_eq!(
            record.expires_at_ms,
            Some(to_unix_millis(sandbox.expires_at.unwrap()))
        );

        sandbox.set_timeout(None);
        let record = StoredSandboxRecord::new(&sandbox, 2).unwrap();
        assert_eq!(record.expires_at_ms, None);
    }

    #[test]
    fn an_unknown_version_fails_rather_than_looking_new() {
        let mut json: Value = serde_json::from_slice(
            &StoredSandboxRecord::new(&metadata(), 1)
                .unwrap()
                .encode()
                .unwrap(),
        )
        .unwrap();
        json["version"] = serde_json::json!(RECORD_VERSION + 1);
        let raw = serde_json::to_vec(&json).unwrap();
        assert!(StoredSandboxRecord::decode(&raw).is_err());

        let mut json: Value = serde_json::from_slice(
            &StoredSandboxRecord::new(&metadata(), 1)
                .unwrap()
                .encode()
                .unwrap(),
        )
        .unwrap();
        json.as_object_mut().unwrap().remove("version");
        let raw = serde_json::to_vec(&json).unwrap();
        assert!(StoredSandboxRecord::decode(&raw).is_err());
    }

    #[test]
    fn a_record_without_an_incarnation_is_refused() {
        let mut json: Value = serde_json::from_slice(
            &StoredSandboxRecord::new(&metadata(), 1)
                .unwrap()
                .encode()
                .unwrap(),
        )
        .unwrap();
        json.as_object_mut().unwrap().remove("execution_id");
        let raw = serde_json::to_vec(&json).unwrap();
        assert!(StoredSandboxRecord::decode(&raw).is_err());
    }

    #[test]
    fn record_ttl_is_ceiled_floored_and_absent_without_a_ceiling() {
        let now = UNIX_EPOCH + Duration::from_secs(1_000_000);
        let mut sandbox = metadata();
        sandbox.max_lifetime = None;
        assert_eq!(
            StoredSandboxRecord::new(&sandbox, 1)
                .unwrap()
                .record_ttl(now, Duration::from_secs(3600)),
            None
        );

        sandbox.max_lifetime = Some(Duration::from_millis(1500));
        sandbox.running_since = Some(now);
        let ttl = StoredSandboxRecord::new(&sandbox, 1)
            .unwrap()
            .record_ttl(now, Duration::from_secs(10))
            .unwrap();
        assert_eq!(ttl, Duration::from_secs(12));

        sandbox.running_elapsed = Duration::from_secs(999);
        let ttl = StoredSandboxRecord::new(&sandbox, 1)
            .unwrap()
            .record_ttl(now, Duration::ZERO)
            .unwrap();
        assert_eq!(ttl, Duration::from_secs(1));
    }

    #[test]
    fn placement_fields_are_carried_forward_rather_than_erased() {
        let previous = StoredSandboxRecord {
            origin_node_id: Some("node-a".to_string()),
            published: true,
            ..StoredSandboxRecord::new(&metadata(), 1).unwrap()
        };
        let mut next = StoredSandboxRecord::new(&metadata(), 2).unwrap();
        next.inherit_placement_from(&previous);

        assert_eq!(next.origin_node_id.as_deref(), Some("node-a"));
        assert!(next.published);
    }
}
