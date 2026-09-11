use std::{
    collections::HashMap,
    sync::OnceLock,
    time::{Duration, SystemTime},
};

use serde::{Deserialize, Serialize};

use crate::orchestrator::SandboxState;
use crate::sandbox::CustomExtensionParams;
use crate::sandbox::SandboxNetworkPolicy;
use crate::snapshot::{CommandContext, SnapshotRuntimeVersions, StartupCommand};
use crate::types::{ExecutionId, ImageConfigs, SandboxId, SandboxResources};
use crate::virtualization::VirtualizationMode;

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum SandboxTimeoutAction {
    Pause,
    Delete,
}

/// Opaque control-plane ownership marker stored verbatim by nodes.
/// Empty bytes map to `None`; the control plane alone encodes and decodes it.
#[derive(Clone, PartialEq, Eq)]
pub struct ControlPlaneConfig(Vec<u8>);

impl ControlPlaneConfig {
    /// Builds a non-empty marker, returning `None` for empty bytes.
    pub fn from_bytes(bytes: impl Into<Vec<u8>>) -> Option<Self> {
        let bytes = bytes.into();
        (!bytes.is_empty()).then_some(Self(bytes))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Always false because empty markers cannot be constructed.
    pub fn is_empty(&self) -> bool {
        false
    }

    /// Encodes the complete, versioned sandbox record for store-loss recovery.
    pub fn for_record(record: &SandboxMetadata) -> Option<Self> {
        // Local live-state fields are skipped by record serialization.
        let envelope = OwnershipMarker {
            version: OWNERSHIP_MARKER_VERSION,
            record,
        };
        match serde_json::to_vec(&envelope) {
            Ok(bytes) => Self::from_bytes(bytes),
            Err(error) => {
                tracing::error!(
                    target: "agentenv",
                    sandbox_id = %record.id,
                    %error,
                    "could not encode the control plane's own record as an ownership marker; \
                     the sandbox will be created without one and will not be recognised as the \
                     control plane's"
                );
                None
            }
        }
    }

    /// Decodes a record only when its marker version matches this build.
    pub fn decode_record(&self) -> anyhow::Result<SandboxMetadata> {
        let envelope: OwnedOwnershipMarker = serde_json::from_slice(&self.0)?;
        anyhow::ensure!(
            envelope.version == OWNERSHIP_MARKER_VERSION,
            "ownership marker is version {}, and this build reads version {}",
            envelope.version,
            OWNERSHIP_MARKER_VERSION
        );
        Ok(envelope.record)
    }
}

/// Ownership-marker schema version written and accepted by this build.
pub const OWNERSHIP_MARKER_VERSION: u32 = 1;

#[derive(Serialize)]
struct OwnershipMarker<'a> {
    version: u32,
    record: &'a SandboxMetadata,
}

// Owned decode envelope avoids cloning the borrowed encode record.
#[derive(Deserialize)]
struct OwnedOwnershipMarker {
    version: u32,
    record: SandboxMetadata,
}

/// Debug output reports size without exposing marker contents.
impl std::fmt::Debug for ControlPlaneConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ControlPlaneConfig({} bytes)", self.0.len())
    }
}

/// Serializes marker bytes as base64 for JSON records.
impl Serialize for ControlPlaneConfig {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use base64::{engine::general_purpose::STANDARD, Engine as _};
        serializer.serialize_str(&STANDARD.encode(&self.0))
    }
}

/// Deserializes a present, non-empty marker.
impl<'de> Deserialize<'de> for ControlPlaneConfig {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let encoded = String::deserialize(deserializer)?;
        decode_control_plane_config::<D>(&encoded)?
            .ok_or_else(|| serde::de::Error::custom("empty control plane config"))
    }
}

fn decode_control_plane_config<'de, D: serde::Deserializer<'de>>(
    encoded: &str,
) -> Result<Option<ControlPlaneConfig>, D::Error> {
    use base64::{engine::general_purpose::STANDARD, Engine as _};

    let bytes = STANDARD
        .decode(encoded)
        .map_err(|err| serde::de::Error::custom(format!("control plane config: {err}")))?;
    Ok(ControlPlaneConfig::from_bytes(bytes))
}

// Absent and empty markers map to `None`; malformed base64 remains an error.
pub(crate) fn deserialize_optional_control_plane_config<'de, D>(
    deserializer: D,
) -> Result<Option<ControlPlaneConfig>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let Some(encoded) = Option::<String>::deserialize(deserializer)? else {
        return Ok(None);
    };
    decode_control_plane_config::<D>(&encoded)
}

fn configured_projection_ttl_grace_secs() -> u64 {
    static GRACE_SECS: OnceLock<u64> = OnceLock::new();

    *GRACE_SECS.get_or_init(|| {
        crate::cfg::ConfigManager::global_config()
            .orchestrator
            .projection_ttl_grace_secs
    })
}

/// Configured sandbox lifetime ceiling, or `None` when disabled.
pub fn configured_max_sandbox_lifetime() -> Option<Duration> {
    static MAX_LIFETIME: OnceLock<Option<Duration>> = OnceLock::new();

    *MAX_LIFETIME.get_or_init(|| {
        match crate::cfg::ConfigManager::global_config()
            .orchestrator
            .max_sandbox_lifetime_secs
        {
            // Zero disables the ceiling.
            0 => None,
            secs => Some(Duration::from_secs(secs)),
        }
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NewTimeout {
    UseExisting,
    Set(Duration),
    EnsureMinimum(Duration),
    None,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SandboxMetadata {
    pub id: SandboxId,
    /// Required incarnation fence; older records without it are rejected.
    pub execution_id: ExecutionId,
    pub snapshot_id: String,
    pub snapshot_alias: Option<String>,
    pub state: SandboxState,
    pub created_at: SystemTime,
    pub timeout: Option<Duration>,
    pub timeout_action: SandboxTimeoutAction,
    pub expires_at: Option<SystemTime>,
    pub auto_resume: bool,
    /// Virtualization mode used by this sandbox for its entire lifecycle.
    #[serde(default)]
    pub virtualization_mode: VirtualizationMode,
    /// Guest memory on 2 MiB hugetlbfs pages, fixed at boot and inherited
    /// by every snapshot and resume of this sandbox.
    #[serde(default)]
    pub huge_pages: bool,
    pub runtime_versions: SnapshotRuntimeVersions,
    pub resources: SandboxResources,
    pub context: CommandContext,
    pub startup: Option<StartupCommand>,
    #[serde(default, skip_serializing_if = "ImageConfigs::is_empty")]
    pub image_configs: ImageConfigs,
    pub user_metadata: Option<HashMap<String, String>>,
    pub network_policy: SandboxNetworkPolicy,
    /// Opaque user-provided JSON passed through to the custom extension hooks.
    /// Persisted into committed snapshots so template launches inherit it
    /// unless overridden at create time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_extension_params: Option<CustomExtensionParams>,
    /// Whether envd requires the access token derived from this sandbox's ID.
    /// Older records deserialize as non-secure sandboxes.
    #[serde(default)]
    pub secure: bool,
    /// The token a client must present to reach a non-envd port through the
    /// data-plane proxy. Older records deserialize with none, which is the
    /// open default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub traffic_access_token: Option<String>,
    /// Opaque ownership marker stored and returned verbatim by the node.
    /// Absence means the control plane does not own this sandbox.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_control_plane_config"
    )]
    pub control_plane_config: Option<ControlPlaneConfig>,
    /// Running-time lifetime budget; paused time does not consume it.
    /// Defaults to `None` so older paused records remain readable.
    #[serde(default)]
    pub max_lifetime: Option<Duration>,
    /// Running time spent by completed runs; defaults to zero for older records.
    #[serde(default)]
    pub running_elapsed: Duration,
    /// Start of the current running interval; not persisted.
    #[serde(skip)]
    pub running_since: Option<SystemTime>,
}

impl Default for SandboxMetadata {
    fn default() -> Self {
        Self {
            id: SandboxId::new(),
            // Default metadata is a fixture, not a real execution.
            execution_id: ExecutionId::new(),
            snapshot_id: "unknown".to_string(),
            snapshot_alias: None,
            state: SandboxState::Creating,
            created_at: SystemTime::now(),
            timeout: None,
            timeout_action: SandboxTimeoutAction::Pause,
            expires_at: None,
            auto_resume: false,
            virtualization_mode: VirtualizationMode::default(),
            huge_pages: false,
            runtime_versions: SnapshotRuntimeVersions::new(
                "unknown".to_string(),
                "unknown".to_string(),
                "unknown".to_string(),
                "unknown".to_string(),
            ),
            resources: SandboxResources::default(),
            context: CommandContext::default(),
            startup: None,
            image_configs: ImageConfigs::new(),
            user_metadata: None,
            network_policy: SandboxNetworkPolicy::default(),
            custom_extension_params: None,
            secure: false,
            traffic_access_token: None,
            // Default fixtures are not control-plane owned.
            control_plane_config: None,
            max_lifetime: None,
            running_elapsed: Duration::ZERO,
            running_since: None,
        }
    }
}

impl SandboxMetadata {
    pub fn set_timeout(&mut self, timeout: Option<Duration>) {
        self._set_timeout(timeout, SystemTime::now());
    }

    /// Returns the deadline for remaining running-time budget.
    pub fn lifetime_deadline(&self, now: SystemTime) -> Option<SystemTime> {
        let remaining = self.max_lifetime?.saturating_sub(self.running_elapsed);
        let anchor = self.running_since.unwrap_or(now);

        anchor.checked_add(remaining)
    }

    /// Starts the running clock if no interval is open. Every recorded state
    /// spends lifetime: a paused sandbox has no record.
    pub fn sync_running_clock(&mut self, now: SystemTime) {
        if self.running_since.is_none() {
            self.running_since = Some(now);
        }
    }

    /// Running time spent by this and earlier runs, as of `now`.
    pub fn running_elapsed_at(&self, now: SystemTime) -> Duration {
        let open = self
            .running_since
            .and_then(|since| now.duration_since(since).ok())
            .unwrap_or(Duration::ZERO);
        self.running_elapsed.saturating_add(open)
    }

    /// Restarts the lifetime budget for a newly forked child.
    pub fn restart_lifetime_clock(&mut self, now: SystemTime) {
        self.running_elapsed = Duration::ZERO;
        self.running_since = Some(now);
    }

    /// Returns remaining lifetime plus routing grace, or `0` for receiver default.
    pub fn projection_ttl_secs(&self, now: SystemTime) -> u32 {
        self._projection_ttl_secs(now, configured_projection_ttl_grace_secs())
    }

    fn _projection_ttl_secs(&self, now: SystemTime, grace_secs: u64) -> u32 {
        let Some(cap) = self.lifetime_deadline(now) else {
            return 0;
        };
        let remaining = cap.duration_since(now).unwrap_or(Duration::ZERO);
        // Round up and floor at one so projections never expire early or become unbounded.
        let secs = remaining
            .as_secs()
            .saturating_add(u64::from(remaining.subsec_nanos() > 0));
        secs.saturating_add(grace_secs)
            .clamp(1, u64::from(u32::MAX)) as u32
    }

    fn _set_timeout(&mut self, timeout: Option<Duration>, from: SystemTime) {
        self.timeout = timeout;
        let deadline = timeout.and_then(|ttl| from.checked_add(ttl));
        // All expiry-setting paths apply the lifetime ceiling here.
        self.expires_at = match (deadline, self.lifetime_deadline(from)) {
            (Some(deadline), Some(cap)) => Some(deadline.min(cap)),
            (deadline, _) => deadline,
        };
    }

    pub fn update_timeout(&mut self, new_timeout: NewTimeout) {
        self._update_timeout(new_timeout, SystemTime::now());
    }

    fn _update_timeout(&mut self, new_timeout: NewTimeout, from: SystemTime) {
        let timeout = match new_timeout {
            NewTimeout::UseExisting => self.timeout,
            NewTimeout::Set(timeout) => Some(timeout),
            NewTimeout::EnsureMinimum(minimum) => match self.timeout {
                Some(existing) => Some(existing.max(minimum)),
                None => Some(minimum),
            },
            NewTimeout::None => None,
        };
        self._set_timeout(timeout, from);
    }

    pub fn is_expired(&self, now: SystemTime) -> bool {
        self.expires_at.is_some_and(|deadline| deadline <= now)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::UNIX_EPOCH;

    mod ownership_marker {
        use serde_json::Value;

        use super::*;

        fn record_with(field: Value) -> Value {
            let mut value = serde_json::to_value(SandboxMetadata::default()).expect("encode");
            value
                .as_object_mut()
                .expect("an object")
                .insert("control_plane_config".to_string(), field);
            value
        }

        #[test]
        fn zero_bytes_is_not_a_marker() {
            assert_eq!(ControlPlaneConfig::from_bytes(Vec::new()), None);
            assert_eq!(ControlPlaneConfig::from_bytes(""), None);
            assert!(ControlPlaneConfig::from_bytes(vec![0u8]).is_some());
        }

        #[test]
        fn the_bytes_come_back_exactly_as_they_went_in() {
            for bytes in [vec![0u8], vec![0xff, 0x00, 0xff], b"{}".to_vec()] {
                let marker = ControlPlaneConfig::from_bytes(bytes.clone()).expect("non-empty");
                assert_eq!(marker.as_bytes(), &bytes[..]);

                let encoded = serde_json::to_string(&marker).expect("encode");
                let decoded: ControlPlaneConfig = serde_json::from_str(&encoded).expect("decode");
                assert_eq!(decoded.into_bytes(), bytes);
            }
        }

        #[test]
        fn it_survives_a_record_round_trip() {
            let metadata = SandboxMetadata {
                control_plane_config: ControlPlaneConfig::from_bytes(vec![1, 2, 3, 0xfe]),
                ..Default::default()
            };

            let encoded = serde_json::to_string(&metadata).expect("encode");
            let decoded: SandboxMetadata = serde_json::from_str(&encoded).expect("decode");

            assert_eq!(
                decoded
                    .control_plane_config
                    .map(ControlPlaneConfig::into_bytes),
                Some(vec![1, 2, 3, 0xfe])
            );
        }

        #[test]
        fn an_absent_or_null_or_empty_field_means_unowned() {
            for field in [
                Value::Null,
                Value::String(String::new()),
                Value::String("".to_string()),
            ] {
                let decoded: SandboxMetadata =
                    serde_json::from_value(record_with(field)).expect("decode");
                assert!(decoded.control_plane_config.is_none());
            }

            let mut without = serde_json::to_value(SandboxMetadata::default()).expect("encode");
            without
                .as_object_mut()
                .expect("an object")
                .remove("control_plane_config");
            let decoded: SandboxMetadata = serde_json::from_value(without).expect("decode");
            assert!(decoded.control_plane_config.is_none());
        }

        #[test]
        fn a_mangled_marker_is_an_error_and_not_an_absence() {
            let err = serde_json::from_value::<SandboxMetadata>(record_with(Value::String(
                "not base64!!".to_string(),
            )))
            .expect_err("mangled base64 must not decode");
            assert!(
                err.to_string().contains("control plane config"),
                "the error should name the field: {err}"
            );
        }

        #[test]
        fn debug_prints_the_size_and_not_the_contents() {
            let marker =
                ControlPlaneConfig::from_bytes(b"secret-workspace".to_vec()).expect("non-empty");
            let rendered = format!("{marker:?}");
            assert_eq!(rendered, "ControlPlaneConfig(16 bytes)");
            assert!(!rendered.contains("secret"));
        }
    }

    #[test]
    fn metadata_timeout_work() {
        let base = UNIX_EPOCH + Duration::from_secs(100);
        let mut metadata = SandboxMetadata {
            created_at: base,
            ..Default::default()
        };
        metadata._set_timeout(Some(Duration::from_secs(10)), base);

        assert_eq!(metadata.expires_at, Some(base + Duration::from_secs(10)));
        assert!(!metadata.is_expired(base + Duration::from_secs(9)));
        assert!(metadata.is_expired(base + Duration::from_secs(10)));

        metadata.set_timeout(None);
        assert_eq!(metadata.timeout, None);
        assert_eq!(metadata.expires_at, None);
    }

    #[test]
    fn update_timeout_ensure_minimum_respects_longer_existing_timeout() {
        let base = UNIX_EPOCH + Duration::from_secs(100);
        let mut metadata = SandboxMetadata::default();
        metadata.set_timeout(Some(Duration::from_secs(900)));

        metadata._update_timeout(NewTimeout::EnsureMinimum(Duration::from_secs(300)), base);
        assert_eq!(metadata.timeout, Some(Duration::from_secs(900)));
        assert_eq!(metadata.expires_at, Some(base + Duration::from_secs(900)));
    }

    #[test]
    fn update_timeout_supports_set_use_existing_and_clear() {
        let base = UNIX_EPOCH + Duration::from_secs(100);
        let mut metadata = SandboxMetadata::default();
        metadata.set_timeout(Some(Duration::from_secs(120)));

        metadata._update_timeout(NewTimeout::EnsureMinimum(Duration::from_secs(300)), base);
        assert_eq!(metadata.timeout, Some(Duration::from_secs(300)));
        assert_eq!(metadata.expires_at, Some(base + Duration::from_secs(300)));

        metadata._update_timeout(NewTimeout::UseExisting, base);
        assert_eq!(metadata.timeout, Some(Duration::from_secs(300)));
        assert_eq!(metadata.expires_at, Some(base + Duration::from_secs(300)));

        metadata._update_timeout(NewTimeout::Set(Duration::from_secs(45)), base);
        assert_eq!(metadata.timeout, Some(Duration::from_secs(45)));
        assert_eq!(metadata.expires_at, Some(base + Duration::from_secs(45)));

        metadata.update_timeout(NewTimeout::None);
        assert_eq!(metadata.timeout, None);
        assert_eq!(metadata.expires_at, None);
    }

    fn uncapped(base: SystemTime) -> SandboxMetadata {
        SandboxMetadata {
            traffic_access_token: None,
            created_at: base,
            max_lifetime: None,
            running_since: Some(base),
            ..Default::default()
        }
    }

    fn capped(base: SystemTime, lifetime_secs: u64) -> SandboxMetadata {
        SandboxMetadata {
            created_at: base,
            max_lifetime: Some(Duration::from_secs(lifetime_secs)),
            running_since: Some(base),
            ..Default::default()
        }
    }

    #[test]
    fn lifetime_deadline_is_the_run_start_plus_what_is_left_of_the_budget() {
        let base = UNIX_EPOCH + Duration::from_secs(100);

        assert_eq!(uncapped(base).lifetime_deadline(base), None);
        assert_eq!(
            capped(base, 300).lifetime_deadline(base),
            Some(base + Duration::from_secs(300))
        );

        let mut resumed = capped(base, 300);
        resumed.running_elapsed = Duration::from_secs(150);
        assert_eq!(
            resumed.lifetime_deadline(base),
            Some(base + Duration::from_secs(150))
        );

        assert_eq!(
            resumed.lifetime_deadline(base + Duration::from_secs(100)),
            Some(base + Duration::from_secs(150))
        );
    }

    #[test]
    fn a_record_with_no_open_run_carries_its_budget_forward_instead_of_burning_it() {
        let base = UNIX_EPOCH + Duration::from_secs(100);
        let parked = SandboxMetadata {
            created_at: base,
            state: SandboxState::Running,
            max_lifetime: Some(Duration::from_secs(86_400)),
            running_elapsed: Duration::from_secs(60),
            running_since: None,
            ..Default::default()
        };

        let much_later = base + Duration::from_secs(90_000);
        assert_eq!(
            parked.lifetime_deadline(much_later),
            Some(much_later + Duration::from_secs(86_340)),
            "the budget left is the ceiling minus the minute actually spent running"
        );
    }

    #[test]
    fn the_running_clock_charges_a_run_once_and_only_once() {
        let base = UNIX_EPOCH + Duration::from_secs(100);
        let mut metadata = SandboxMetadata {
            created_at: base,
            state: SandboxState::Running,
            max_lifetime: Some(Duration::from_secs(300)),
            ..Default::default()
        };

        metadata.sync_running_clock(base);
        assert_eq!(metadata.running_since, Some(base));
        metadata.sync_running_clock(base + Duration::from_secs(30));
        assert_eq!(metadata.running_since, Some(base));
        assert_eq!(metadata.running_elapsed, Duration::ZERO);

        metadata.state = SandboxState::Pausing;
        metadata.sync_running_clock(base + Duration::from_secs(60));
        assert_eq!(
            metadata.running_since,
            Some(base),
            "a state change does not close the run"
        );
        assert_eq!(metadata.running_elapsed, Duration::ZERO);

        metadata.running_since = None;
        metadata.sync_running_clock(base + Duration::from_secs(90_000));
        assert_eq!(
            metadata.running_since,
            Some(base + Duration::from_secs(90_000))
        );
        assert_eq!(metadata.running_elapsed, Duration::ZERO);
    }

    #[test]
    fn restarting_the_clock_clears_the_spent_budget() {
        let base = UNIX_EPOCH + Duration::from_secs(100);
        let mut child = SandboxMetadata {
            created_at: base,
            state: SandboxState::Running,
            max_lifetime: Some(Duration::from_secs(300)),
            running_elapsed: Duration::from_secs(290),
            running_since: Some(base),
            ..Default::default()
        };

        let forked_at = base + Duration::from_secs(10);
        child.restart_lifetime_clock(forked_at);

        assert_eq!(child.running_elapsed, Duration::ZERO);
        assert_eq!(child.running_since, Some(forked_at));
        assert_eq!(
            child.lifetime_deadline(forked_at),
            Some(forked_at + Duration::from_secs(300))
        );
    }

    #[test]
    fn set_timeout_clamps_to_the_lifetime_ceiling() {
        let base = UNIX_EPOCH + Duration::from_secs(100);
        let mut metadata = capped(base, 300);

        metadata._set_timeout(Some(Duration::from_secs(3600)), base);
        assert_eq!(metadata.expires_at, Some(base + Duration::from_secs(300)));
        assert_eq!(metadata.timeout, Some(Duration::from_secs(3600)));
    }

    #[test]
    fn set_timeout_without_a_ceiling_is_not_clamped() {
        let base = UNIX_EPOCH + Duration::from_secs(100);
        let mut metadata = uncapped(base);

        metadata._set_timeout(Some(Duration::from_secs(3600)), base);
        assert_eq!(metadata.expires_at, Some(base + Duration::from_secs(3600)));
    }

    #[test]
    fn set_timeout_leaves_a_deadline_inside_the_ceiling_alone() {
        let base = UNIX_EPOCH + Duration::from_secs(100);
        let mut metadata = capped(base, 300);

        metadata._set_timeout(Some(Duration::from_secs(60)), base);
        assert_eq!(metadata.expires_at, Some(base + Duration::from_secs(60)));
    }

    #[test]
    fn a_later_renewal_cannot_push_the_deadline_past_the_ceiling() {
        let base = UNIX_EPOCH + Duration::from_secs(100);
        let mut metadata = capped(base, 300);
        metadata._set_timeout(Some(Duration::from_secs(60)), base);

        let halfway = base + Duration::from_secs(150);
        metadata._set_timeout(Some(Duration::from_secs(3600)), halfway);
        assert_eq!(metadata.expires_at, Some(base + Duration::from_secs(300)));
    }

    #[test]
    fn clearing_the_timeout_clears_the_deadline_even_under_a_ceiling() {
        let base = UNIX_EPOCH + Duration::from_secs(100);
        let mut metadata = capped(base, 300);
        metadata._set_timeout(Some(Duration::from_secs(60)), base);

        metadata._set_timeout(None, base);
        assert_eq!(metadata.expires_at, None);
    }

    #[test]
    fn a_forked_child_gets_a_fresh_window_because_the_clock_restarted() {
        let base = UNIX_EPOCH + Duration::from_secs(100);
        let mut parent = capped(base, 300);
        parent._set_timeout(Some(Duration::from_secs(300)), base);
        let later = base + Duration::from_secs(280);
        assert_eq!(
            parent.lifetime_deadline(later),
            Some(base + Duration::from_secs(300))
        );

        let mut child = parent.clone();
        child.created_at = later;
        child.restart_lifetime_clock(later);
        child._set_timeout(Some(Duration::from_secs(300)), later);

        assert_eq!(child.expires_at, Some(later + Duration::from_secs(300)));
    }

    #[test]
    fn projection_ttl_is_zero_without_a_ceiling() {
        let base = UNIX_EPOCH + Duration::from_secs(100);

        assert_eq!(uncapped(base)._projection_ttl_secs(base, 60), 0);
    }

    #[test]
    fn projection_ttl_is_the_remaining_budget_plus_the_grace() {
        let base = UNIX_EPOCH + Duration::from_secs(100);
        let metadata = capped(base, 300);

        assert_eq!(metadata._projection_ttl_secs(base, 60), 360);
        assert_eq!(
            metadata._projection_ttl_secs(base + Duration::from_secs(100), 60),
            260
        );
        assert_eq!(metadata._projection_ttl_secs(base, 0), 300);
    }

    #[test]
    fn projection_ttl_rounds_a_partial_second_up() {
        let base = UNIX_EPOCH + Duration::from_secs(100);
        let metadata = capped(base, 300);

        assert_eq!(metadata._projection_ttl_secs(base, 0), 300);
        assert_eq!(
            metadata._projection_ttl_secs(base + Duration::new(0, 500_000_000), 0),
            300
        );

        let ragged = SandboxMetadata {
            created_at: base,
            max_lifetime: Some(Duration::new(300, 1)),
            running_since: Some(base),
            ..Default::default()
        };
        assert_eq!(ragged._projection_ttl_secs(base, 0), 301);
    }

    #[test]
    fn projection_ttl_floors_at_one_second_and_never_at_zero() {
        let base = UNIX_EPOCH + Duration::from_secs(100);
        let metadata = SandboxMetadata {
            created_at: base,
            max_lifetime: Some(Duration::new(0, 1)),
            running_since: Some(base),
            ..Default::default()
        };

        assert_eq!(metadata._projection_ttl_secs(base, 0), 1);

        let expired = capped(base, 300);
        assert_eq!(
            expired._projection_ttl_secs(base + Duration::from_secs(9_000), 0),
            1
        );
    }

    #[test]
    fn projection_ttl_saturates_instead_of_wrapping() {
        let base = UNIX_EPOCH + Duration::from_secs(100);
        let metadata = capped(base, u64::from(u32::MAX));

        assert_eq!(metadata._projection_ttl_secs(base, 3600), u32::MAX);
    }

    #[test]
    fn a_record_written_before_the_ceiling_existed_still_decodes() {
        let mut document = serde_json::to_value(&SandboxMetadata {
            created_at: UNIX_EPOCH + Duration::from_secs(100),
            max_lifetime: Some(Duration::from_secs(300)),
            ..Default::default()
        })
        .expect("serialize");
        document
            .as_object_mut()
            .expect("an object")
            .remove("max_lifetime")
            .expect("the field is written");

        let decoded: SandboxMetadata =
            serde_json::from_value(document).expect("an older record must still load");

        assert_eq!(decoded.max_lifetime, None);
        assert_eq!(decoded.lifetime_deadline(SystemTime::now()), None);
        assert_eq!(decoded.projection_ttl_secs(SystemTime::now()), 0);
    }

    #[test]
    fn the_ceiling_survives_a_round_trip() {
        let metadata = SandboxMetadata {
            created_at: UNIX_EPOCH + Duration::from_secs(100),
            max_lifetime: Some(Duration::from_secs(86_400)),
            ..Default::default()
        };

        let decoded: SandboxMetadata =
            serde_json::from_str(&serde_json::to_string(&metadata).expect("serialize"))
                .expect("deserialize");

        assert_eq!(decoded.max_lifetime, Some(Duration::from_secs(86_400)));
    }

    #[test]
    fn a_record_written_before_the_running_clock_existed_still_decodes() {
        let mut document = serde_json::to_value(SandboxMetadata {
            created_at: UNIX_EPOCH + Duration::from_secs(100),
            state: SandboxState::Running,
            max_lifetime: Some(Duration::from_secs(86_400)),
            running_elapsed: Duration::from_secs(600),
            ..Default::default()
        })
        .expect("serialize");
        document
            .as_object_mut()
            .expect("an object")
            .remove("running_elapsed")
            .expect("the field is written");

        let decoded: SandboxMetadata =
            serde_json::from_value(document).expect("an older record must still load");

        assert_eq!(decoded.running_elapsed, Duration::ZERO);
        assert_eq!(decoded.running_since, None);

        let now = UNIX_EPOCH + Duration::from_secs(500_000);
        assert_eq!(
            decoded.lifetime_deadline(now),
            Some(now + Duration::from_secs(86_400)),
            "a record with no spend history resumes with the whole ceiling, however \
             long ago it was written"
        );
    }

    #[test]
    fn the_spent_budget_survives_a_round_trip_and_the_open_interval_does_not() {
        let base = UNIX_EPOCH + Duration::from_secs(100);
        let metadata = SandboxMetadata {
            created_at: base,
            state: SandboxState::Running,
            max_lifetime: Some(Duration::from_secs(86_400)),
            running_elapsed: Duration::from_secs(3_600),
            running_since: Some(base),
            ..Default::default()
        };

        let decoded: SandboxMetadata =
            serde_json::from_str(&serde_json::to_string(&metadata).expect("serialize"))
                .expect("deserialize");

        assert_eq!(decoded.running_elapsed, Duration::from_secs(3_600));
        assert_eq!(decoded.running_since, None);
    }

    #[test]
    fn the_marker_carries_the_record_and_not_a_summary() {
        let mut record = SandboxMetadata {
            snapshot_id: "snapshot-a".to_string(),
            secure: true,
            max_lifetime: Some(Duration::from_secs(3600)),
            running_elapsed: Duration::from_secs(90),
            ..Default::default()
        };
        record.execution_id = ExecutionId::new();
        record.user_metadata = Some(HashMap::from([("owner".to_string(), "team".to_string())]));

        let marker = ControlPlaneConfig::for_record(&record).expect("a record encodes");
        let decoded = marker.decode_record().expect("and decodes");

        assert_eq!(decoded.id, record.id);
        assert_eq!(decoded.execution_id, record.execution_id);
        assert_eq!(decoded.snapshot_id, record.snapshot_id);
        assert_eq!(decoded.resources, record.resources);
        assert_eq!(decoded.virtualization_mode, record.virtualization_mode);
        assert_eq!(decoded.secure, record.secure);
        assert_eq!(decoded.max_lifetime, record.max_lifetime);
        assert_eq!(decoded.running_elapsed, record.running_elapsed);
        assert_eq!(decoded.user_metadata, record.user_metadata);
        assert_eq!(decoded.state, record.state);

        assert!(decoded.running_since.is_none());
    }

    #[test]
    fn a_marker_from_another_schema_is_refused_rather_than_defaulted() {
        let marker =
            ControlPlaneConfig::for_record(&SandboxMetadata::default()).expect("a record encodes");
        let mut envelope: serde_json::Value =
            serde_json::from_slice(marker.as_bytes()).expect("the envelope is JSON");
        assert_eq!(
            envelope["version"], OWNERSHIP_MARKER_VERSION,
            "the version this build writes"
        );

        envelope["version"] = serde_json::json!(OWNERSHIP_MARKER_VERSION + 1);
        let future =
            ControlPlaneConfig::from_bytes(serde_json::to_vec(&envelope).expect("re-encode"))
                .expect("a non-empty marker");
        let err = future
            .decode_record()
            .expect_err("a version this build does not write")
            .to_string();
        assert!(err.contains("version"), "{err}");

        assert!(marker.decode_record().is_ok());
    }
}

/// Cross-language golden fixtures for the metadata JSONB contract.
#[cfg(test)]
mod golden {
    use std::collections::{BTreeMap, HashMap};
    use std::path::PathBuf;
    use std::time::{Duration, UNIX_EPOCH};

    use serde_json::{json, Value};

    use super::*;
    use crate::sandbox::network::policy::{DomainRule, HeaderTransform};
    use crate::sandbox::{BaseSandboxNetworkPolicy, SandboxNetworkEgressPolicy};
    use crate::snapshot::CommandContext;
    use crate::types::ImageConfigs;

    // Fields without `Option` or serde defaults.
    const REQUIRED_FIELDS: [&str; 11] = [
        "id",
        // Incarnation is required for fencing.
        "execution_id",
        "snapshot_id",
        "state",
        "created_at",
        "timeout_action",
        "auto_resume",
        "runtime_versions",
        "resources",
        "context",
        "network_policy",
    ];

    // Fields omitted when empty.
    const OMISSIBLE_FIELDS: [&str; 3] = [
        "image_configs",
        "custom_extension_params",
        // An absent ownership marker is a valid unowned record.
        "control_plane_config",
    ];

    // Shared fixture publishing the required field set.
    const REQUIRED_FIELDS_FIXTURE: &str = "sandbox_metadata_required_fields.json";

    fn fixture_path(name: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures")
            .join(name)
    }

    // Sort keys because JSON object order is not contractual or deterministic.
    fn canonical(metadata: &SandboxMetadata) -> String {
        let value = serde_json::to_value(metadata).expect("serialize sandbox metadata");
        let mut text = serde_json::to_string_pretty(&sort_keys(value)).expect("render metadata");
        text.push('\n');

        text
    }

    fn sort_keys(value: Value) -> Value {
        match value {
            Value::Object(object) => {
                let mut sorted: Vec<_> = object.into_iter().collect();
                sorted.sort_by(|(left, _), (right, _)| left.cmp(right));

                Value::Object(
                    sorted
                        .into_iter()
                        .map(|(key, value)| (key, sort_keys(value)))
                        .collect(),
                )
            }
            Value::Array(items) => Value::Array(items.into_iter().map(sort_keys).collect()),
            other => other,
        }
    }

    // Populates every optional field.
    fn full_metadata() -> SandboxMetadata {
        let mut image_configs = ImageConfigs::new();
        image_configs.add(
            None::<String>,
            "/",
            json!({ "Env": ["PATH=/usr/local/bin:/usr/bin"], "Cmd": ["/bin/sh"] }),
        );
        image_configs.add(Some("drive-1"), "/data", json!({}));

        let mut custom_extension_params = serde_json::Map::new();
        custom_extension_params.insert("tenant".to_string(), json!("workspace-42"));
        custom_extension_params.insert("retries".to_string(), json!(3));

        SandboxMetadata {
            traffic_access_token: None,
            id: SandboxId::parse_str("0199c9a1-4f2e-7c31-a0b4-6d5e8f2a1c07").unwrap(),
            execution_id: ExecutionId::parse_str("0199c9a1-4f2e-7c31-a0b4-6d5e8f2a1c08").unwrap(),
            snapshot_id: "0199c8ff-1122-7000-8000-aabbccddeeff".to_string(),
            snapshot_alias: Some("tpl-node22".to_string()),
            state: SandboxState::Pausing,
            created_at: UNIX_EPOCH + Duration::new(1_755_561_600, 123_456_789),
            timeout: Some(Duration::new(900, 0)),
            timeout_action: SandboxTimeoutAction::Delete,
            expires_at: Some(UNIX_EPOCH + Duration::new(1_755_562_500, 0)),
            auto_resume: true,
            virtualization_mode: VirtualizationMode::Pvm,
            huge_pages: false,
            runtime_versions: SnapshotRuntimeVersions::new(
                "6.1.102".to_string(),
                "1.13.1".to_string(),
                "0.5.15".to_string(),
                "2026.08.01".to_string(),
            ),
            resources: crate::types::SandboxResources {
                cpu_count: 2,
                memory_mib: 4096,
                disk_size_mib: 20480,
            },
            context: CommandContext {
                env_vars: HashMap::from([
                    ("PATH".to_string(), "/usr/local/bin:/usr/bin".to_string()),
                    ("LANG".to_string(), "C.UTF-8".to_string()),
                ]),
                workdir: "/home/user".to_string(),
                user: Some("user".to_string()),
                exposed_ports: vec!["5173/tcp".to_string()],
                entrypoint: Some(vec!["/bin/sh".to_string(), "-lc".to_string()]),
                cmd: Some(vec!["npm run dev".to_string()]),
                volumes: vec!["/data".to_string()],
                labels: HashMap::from([("owner".to_string(), "agentenv".to_string())]),
            },
            startup: Some(StartupCommand {
                start_cmd: "npm run dev".to_string(),
                ready_cmd: "curl -sf http://127.0.0.1:5173".to_string(),
                context: CommandContext {
                    env_vars: HashMap::from([("NODE_ENV".to_string(), "development".to_string())]),
                    workdir: "/home/user/app".to_string(),
                    ..CommandContext::default()
                },
            }),
            image_configs,
            user_metadata: Some(HashMap::from([
                ("sandboxId".to_string(), "sbx-42".to_string()),
                ("workspaceId".to_string(), "42".to_string()),
            ])),
            network_policy: SandboxNetworkPolicy::new(
                BaseSandboxNetworkPolicy::Deny,
                // Derived the way the api half derives it, so the fixture pins
                // both the public `rules` and the internal `brokers` shape.
                SandboxNetworkEgressPolicy::with_rules(
                    Some(vec![
                        "10.20.0.0/16".to_string(),
                        "registry.npmjs.org".to_string(),
                    ]),
                    Some(vec!["10.20.30.0/24".to_string()]),
                    Some(BTreeMap::from([(
                        "api.openai.com".to_string(),
                        vec![DomainRule {
                            transform: HeaderTransform {
                                headers: BTreeMap::from([(
                                    "authorization".to_string(),
                                    "Bearer ${aenv.secrets.openai}".to_string(),
                                )]),
                            },
                        }],
                    )])),
                )
                .expect("the fixture policy is valid"),
            ),
            custom_extension_params: Some(custom_extension_params),
            secure: true,
            // Opaque non-JSON bytes ensure the node never interprets this field.
            control_plane_config: ControlPlaneConfig::from_bytes(vec![
                0x00, 0x01, 0xfe, 0xff, b'o', b'w', b'n', b'e', b'd',
            ]),
            max_lifetime: Some(Duration::new(86_400, 0)),
            running_elapsed: Duration::new(5_400, 0),
            // Skipped from serialization.
            running_since: None,
        }
    }

    // Leaves all omissible fields absent.
    fn minimal_metadata() -> SandboxMetadata {
        SandboxMetadata {
            id: SandboxId::parse_str("0199c9a1-4f2e-7c31-a0b4-6d5e8f2a1c08").unwrap(),
            // Pin the generated default incarnation for a stable fixture.
            execution_id: ExecutionId::parse_str("0199c9a1-4f2e-7c31-a0b4-6d5e8f2a1c09").unwrap(),
            created_at: UNIX_EPOCH + Duration::new(1_755_561_600, 0),
            ..SandboxMetadata::default()
        }
    }

    // Compares with or regenerates the checked-in fixture.
    fn assert_matches_fixture(name: &str, metadata: &SandboxMetadata) -> Value {
        let path = fixture_path(name);
        let rendered = canonical(metadata);

        if std::env::var("UPDATE_METADATA_GOLDEN").is_ok() {
            std::fs::write(&path, &rendered).expect("rewrite the fixture");
        }

        let stored = std::fs::read_to_string(&path).unwrap_or_else(|err| {
            panic!(
                "read {}: {err}. Regenerate with UPDATE_METADATA_GOLDEN=1",
                path.display()
            )
        });
        assert_eq!(
            stored,
            rendered,
            "{} is out of step with SandboxMetadata. If the change is intended, \
             regenerate with UPDATE_METADATA_GOLDEN=1 and update the Go side, which reads \
             this same file",
            path.display()
        );

        serde_json::from_str(&stored).expect("the fixture is JSON")
    }

    #[test]
    fn the_full_fixture_matches_what_sandbox_metadata_serialises_to() {
        let value = assert_matches_fixture("sandbox_metadata_full.json", &full_metadata());
        let object = value.as_object().expect("an object");

        for field in REQUIRED_FIELDS.iter().chain(OMISSIBLE_FIELDS.iter()) {
            assert!(object.contains_key(*field), "fixture is missing {field}");
        }

        let entries = object["image_configs"].as_array().expect("an array");
        assert!(entries.iter().any(|entry| entry
            .as_object()
            .is_some_and(|entry| entry.contains_key("mountPath"))));
        assert!(entries.iter().any(|entry| entry
            .as_object()
            .is_some_and(|entry| entry.contains_key("driveId"))));
    }

    #[test]
    fn the_minimal_fixture_omits_the_fields_that_carry_nothing() {
        let value = assert_matches_fixture("sandbox_metadata_minimal.json", &minimal_metadata());
        let object = value.as_object().expect("an object");

        for field in REQUIRED_FIELDS {
            assert!(object.contains_key(field), "fixture is missing {field}");
        }
        for field in OMISSIBLE_FIELDS {
            assert!(
                !object.contains_key(field),
                "{field} carries nothing and must not be written at all"
            );
        }
    }

    #[test]
    fn both_fixtures_round_trip_through_the_struct() {
        for name in [
            "sandbox_metadata_full.json",
            "sandbox_metadata_minimal.json",
        ] {
            let stored = std::fs::read_to_string(fixture_path(name)).expect("read the fixture");
            let decoded: SandboxMetadata =
                serde_json::from_str(&stored).expect("decode the fixture");

            assert_eq!(canonical(&decoded), stored, "{name} did not round-trip");
        }
    }

    #[test]
    fn dropping_any_required_field_makes_the_record_undecodable() {
        let full: Value = serde_json::from_str(
            &std::fs::read_to_string(fixture_path("sandbox_metadata_full.json"))
                .expect("read the fixture"),
        )
        .expect("the fixture is JSON");

        for field in REQUIRED_FIELDS {
            let mut damaged = full.clone();
            damaged
                .as_object_mut()
                .expect("an object")
                .remove(field)
                .unwrap_or_else(|| panic!("{field} is not in the fixture"));

            assert!(
                serde_json::from_value::<SandboxMetadata>(damaged).is_err(),
                "dropping {field} must not decode: a writer that loses it would strand the \
                 sandbox in every read path at once"
            );
        }
    }

    #[test]
    fn the_required_field_list_is_published_for_the_other_side() {
        let path = fixture_path(REQUIRED_FIELDS_FIXTURE);
        let mut rendered =
            serde_json::to_string_pretty(&REQUIRED_FIELDS).expect("render the required field list");
        rendered.push('\n');

        if std::env::var("UPDATE_METADATA_GOLDEN").is_ok() {
            std::fs::write(&path, &rendered).expect("rewrite the fixture");
        }

        let stored = std::fs::read_to_string(&path).unwrap_or_else(|err| {
            panic!(
                "read {}: {err}. Regenerate with UPDATE_METADATA_GOLDEN=1",
                path.display()
            )
        });
        assert_eq!(
            stored,
            rendered,
            "{} is out of step with REQUIRED_FIELDS. If the change is intended, regenerate with \
             UPDATE_METADATA_GOLDEN=1 — and expect the Go side to fail next, because it compares \
             its own list against this file and has to be brought along in the same change",
            path.display()
        );
    }

    #[test]
    fn dropping_an_optional_field_still_decodes() {
        let full: Value = serde_json::from_str(
            &std::fs::read_to_string(fixture_path("sandbox_metadata_full.json"))
                .expect("read the fixture"),
        )
        .expect("the fixture is JSON");

        for field in [
            "snapshot_alias",
            "timeout",
            "expires_at",
            "startup",
            "user_metadata",
            // Older paused records legitimately lack lifetime fields.
            "max_lifetime",
            "running_elapsed",
        ] {
            let mut trimmed = full.clone();
            trimmed
                .as_object_mut()
                .expect("an object")
                .remove(field)
                .unwrap_or_else(|| panic!("{field} is not in the fixture"));

            serde_json::from_value::<SandboxMetadata>(trimmed)
                .unwrap_or_else(|err| panic!("dropping {field} should still decode: {err}"));
        }
    }

    #[test]
    fn a_policy_written_before_brokered_egress_still_decodes() {
        let mut full: Value = serde_json::from_str(
            &std::fs::read_to_string(fixture_path("sandbox_metadata_full.json"))
                .expect("read the fixture"),
        )
        .expect("the fixture is JSON");

        let egress = full["network_policy"]["egress"]
            .as_object_mut()
            .expect("an egress object");
        for field in ["rules", "brokers"] {
            egress
                .remove(field)
                .unwrap_or_else(|| panic!("{field} is not in the fixture"));
        }

        let decoded: SandboxMetadata =
            serde_json::from_value(full).expect("a record written before rules existed must load");
        assert!(decoded.network_policy.egress.rules.is_empty());
        assert!(decoded.network_policy.egress.brokers.is_empty());
        assert_eq!(
            decoded.network_policy.egress.allowed_domains,
            vec!["registry.npmjs.org".to_string()],
            "the rest of the policy must survive the older shape"
        );
    }
}
