//! What a pause writes into its snapshot row so a resume can rebuild the
//! sandbox from the row alone.

use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::orchestrator::store::deserialize_optional_control_plane_config;
use crate::orchestrator::{ControlPlaneConfig, SandboxMetadata, SandboxTimeoutAction};
use crate::sandbox::SandboxNetworkPolicy;

/// The sandbox-level configuration a paused sandbox carries in its catalog
/// row. Everything runtime-shaped (incarnation, deadline, node) is deliberately
/// absent: a resume mints a new run.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PausedSandboxConfig {
    /// The template the sandbox was originally created from.
    pub template_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub template_alias: Option<String>,
    /// When the sandbox was first created, as clients see `startedAt`.
    pub created_at_unix_ms: i64,
    /// The timeout the sandbox was configured with; a resume may override it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_secs: Option<u64>,
    pub timeout_action: SandboxTimeoutAction,
    pub auto_resume: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_metadata: Option<HashMap<String, String>>,
    pub network_policy: SandboxNetworkPolicy,
    #[serde(default)]
    pub secure: bool,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_control_plane_config"
    )]
    pub control_plane_config: Option<ControlPlaneConfig>,
    /// Running-time lifetime budget, when the sandbox has one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_lifetime_secs: Option<u64>,
    /// Running time already charged against that budget, up to the pause.
    #[serde(default)]
    pub running_elapsed_secs: u64,
}

impl PausedSandboxConfig {
    /// Snapshots the sandbox's configuration at the moment it pauses, charging
    /// the current run's time against its lifetime budget.
    pub fn of(metadata: &SandboxMetadata, now: SystemTime) -> Self {
        Self {
            template_id: metadata.snapshot_id.clone(),
            template_alias: metadata.snapshot_alias.clone(),
            created_at_unix_ms: metadata
                .created_at
                .duration_since(UNIX_EPOCH)
                .map(|since| since.as_millis() as i64)
                .unwrap_or(0),
            timeout_secs: metadata.timeout.map(|timeout| timeout.as_secs()),
            timeout_action: metadata.timeout_action,
            auto_resume: metadata.auto_resume,
            user_metadata: metadata.user_metadata.clone(),
            network_policy: metadata.network_policy.clone(),
            secure: metadata.secure,
            control_plane_config: metadata.control_plane_config.clone(),
            max_lifetime_secs: metadata.max_lifetime.map(|lifetime| lifetime.as_secs()),
            running_elapsed_secs: metadata.running_elapsed_at(now).as_secs(),
        }
    }

    pub fn created_at(&self) -> SystemTime {
        UNIX_EPOCH + Duration::from_millis(self.created_at_unix_ms.max(0) as u64)
    }

    pub fn timeout(&self) -> Option<Duration> {
        self.timeout_secs.map(Duration::from_secs)
    }

    pub fn max_lifetime(&self) -> Option<Duration> {
        self.max_lifetime_secs.map(Duration::from_secs)
    }

    pub fn running_elapsed(&self) -> Duration {
        Duration::from_secs(self.running_elapsed_secs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_config_charges_the_open_run_and_survives_a_round_trip() {
        let started = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let mut metadata = SandboxMetadata {
            snapshot_id: "tpl-1".to_string(),
            timeout: Some(Duration::from_secs(600)),
            auto_resume: true,
            secure: true,
            max_lifetime: Some(Duration::from_secs(3600)),
            running_elapsed: Duration::from_secs(100),
            running_since: Some(started),
            ..Default::default()
        };
        metadata.control_plane_config = ControlPlaneConfig::from_bytes(vec![1, 2, 3]);

        let config = PausedSandboxConfig::of(&metadata, started + Duration::from_secs(50));
        assert_eq!(config.running_elapsed_secs, 150);
        assert_eq!(config.timeout(), Some(Duration::from_secs(600)));
        assert_eq!(config.max_lifetime(), Some(Duration::from_secs(3600)));

        let json = serde_json::to_string(&config).unwrap();
        let back: PausedSandboxConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(back.template_id, "tpl-1");
        assert!(back.auto_resume);
        assert!(back.secure);
        assert_eq!(back.control_plane_config, metadata.control_plane_config);
        assert_eq!(back.running_elapsed(), Duration::from_secs(150));
    }

    #[test]
    fn a_row_written_without_the_optional_fields_still_decodes() {
        let json = serde_json::json!({
            "template_id": "tpl",
            "created_at_unix_ms": 0,
            "timeout_action": "Pause",
            "auto_resume": false,
            "network_policy": SandboxNetworkPolicy::default(),
        });
        let config: PausedSandboxConfig = serde_json::from_value(json).unwrap();
        assert!(config.control_plane_config.is_none());
        assert_eq!(config.running_elapsed_secs, 0);
    }
}
