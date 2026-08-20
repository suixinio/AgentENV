use std::{
    collections::HashMap,
    sync::{Arc, OnceLock},
    time::{Duration, SystemTime},
};

use serde::{Deserialize, Serialize};

use crate::orchestrator::SandboxState;
use crate::sandbox::CustomExtensionParams;
use crate::sandbox::{PausedSandboxState, SandboxNetworkPolicy};
use crate::snapshot::{CommandContext, SnapshotRuntimeVersions, StartupCommand};
use crate::types::{ExecutionId, ImageConfigs, SandboxId, SandboxResources};
use crate::virtualization::VirtualizationMode;

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum SandboxTimeoutAction {
    Pause,
    Delete,
}

/// The configured slack added to a routing projection's TTL.
///
/// Read once: it is a deployment-wide constant, and every response header and
/// heartbeat roster entry asks for it.
fn configured_projection_ttl_grace_secs() -> u64 {
    static GRACE_SECS: OnceLock<u64> = OnceLock::new();

    *GRACE_SECS.get_or_init(|| {
        crate::cfg::ConfigManager::global_config()
            .orchestrator
            .projection_ttl_grace_secs
    })
}

/// The lifetime ceiling this node creates sandboxes under, or `None` when the
/// ceiling is disabled.
pub fn configured_max_sandbox_lifetime() -> Option<Duration> {
    static MAX_LIFETIME: OnceLock<Option<Duration>> = OnceLock::new();

    *MAX_LIFETIME.get_or_init(|| {
        match crate::cfg::ConfigManager::global_config()
            .orchestrator
            .max_sandbox_lifetime_secs
        {
            // 🔴 0 is "no ceiling", not "expire immediately".
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
    /// The incarnation this record belongs to: the single run of the sandbox
    /// that produced it.
    ///
    /// 🔴 Required, and deliberately without `#[serde(default)]`. A default
    /// here would be a permanent fail-open path — every record that has no
    /// incarnation would quietly acquire a fresh one at load time, and fencing
    /// would then be comparing a value nobody ever ran under. Records written
    /// before this field existed are refused at load; see
    /// `FileBackedSandboxPersister` for the message that says so and what to do
    /// about it.
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
    /// The lifetime ceiling this sandbox was created under, measured from
    /// `created_at`, or `None` when the node had no ceiling configured.
    ///
    /// Stored as the budget rather than as an absolute deadline on purpose. A
    /// fork resets `created_at` and so gets a whole fresh window; a resume
    /// keeps it and so keeps shrinking the same one. Both are the right reading
    /// of "maximum lifetime", and neither needs a line of clamping code at the
    /// call site.
    ///
    /// 🔴 `#[serde(default)]` is load-bearing here, unlike on `execution_id`
    /// above: `persister.load_all` runs inside `Orchestrator::new`, so a
    /// required field would mean a node that cannot start after an upgrade,
    /// because every paused record written by an earlier build is missing it.
    /// Do not copy the neighbour.
    #[serde(default)]
    pub max_lifetime: Option<Duration>,
    /// Paused state produced by the sandbox backend during `pause`.
    /// Passed back to the backend factory when `resume_sandbox` is called.
    #[serde(skip)]
    pub paused_state: Option<Arc<dyn PausedSandboxState>>,
}

impl Default for SandboxMetadata {
    fn default() -> Self {
        Self {
            id: SandboxId::new(),
            // A `Default` record never stands for a run that happened — it is a
            // test fixture — so minting here names nothing that could be
            // confused with a real incarnation.
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
            max_lifetime: None,
            paused_state: None,
        }
    }
}

impl SandboxMetadata {
    pub fn set_timeout(&mut self, timeout: Option<Duration>) {
        self._set_timeout(timeout, SystemTime::now());
    }

    /// `created_at + max_lifetime`. `None` means this sandbox has no ceiling.
    pub fn lifetime_deadline(&self) -> Option<SystemTime> {
        self.max_lifetime
            .and_then(|lifetime| self.created_at.checked_add(lifetime))
    }

    /// How long this sandbox's routing projection should live, in whole
    /// seconds, or `0` when there is no ceiling to derive it from.
    ///
    /// 🔴 `0` is the only non-positive value that may ever leave here, and it
    /// means "use the receiver's default TTL". It must never be read as "never
    /// expires": a projection that outlives every path able to delete it is a
    /// route pointing at a sandbox nobody can reach.
    pub fn projection_ttl_secs(&self, now: SystemTime) -> u32 {
        self._projection_ttl_secs(now, configured_projection_ttl_grace_secs())
    }

    fn _projection_ttl_secs(&self, now: SystemTime, grace_secs: u64) -> u32 {
        let Some(cap) = self.lifetime_deadline() else {
            return 0;
        };
        let remaining = cap.duration_since(now).unwrap_or(Duration::ZERO);
        // 🔴 Ceil, not truncate, and floored at 1. Truncating whole units is
        // how a projection comes to expire before the sandbox it points at;
        // flooring at 1 is how a sub-unit remainder avoids collapsing into a
        // zero that the receiver would have to interpret.
        let secs = remaining.as_secs() + u64::from(remaining.subsec_nanos() > 0);
        secs.saturating_add(grace_secs)
            .clamp(1, u64::from(u32::MAX)) as u32
    }

    fn _set_timeout(&mut self, timeout: Option<Duration>, from: SystemTime) {
        self.timeout = timeout;
        let deadline = timeout.and_then(|ttl| from.checked_add(ttl));
        // Every path that sets an expiry — create, resume, fork, connect,
        // auto-resume, keep-alive — funnels through here, which is why the
        // ceiling is applied here and at no call site: there are six of them
        // and clamping them one by one would miss one.
        self.expires_at = match (deadline, self.lifetime_deadline()) {
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

    /// A record with no ceiling behaves exactly as it did before the ceiling
    /// existed. This is the control for every test below it: without it,
    /// "clamped to 300" could just as well be "the timeout was 300 all along".
    fn uncapped(base: SystemTime) -> SandboxMetadata {
        SandboxMetadata {
            created_at: base,
            max_lifetime: None,
            ..Default::default()
        }
    }

    fn capped(base: SystemTime, lifetime_secs: u64) -> SandboxMetadata {
        SandboxMetadata {
            created_at: base,
            max_lifetime: Some(Duration::from_secs(lifetime_secs)),
            ..Default::default()
        }
    }

    #[test]
    fn lifetime_deadline_is_created_at_plus_the_budget() {
        let base = UNIX_EPOCH + Duration::from_secs(100);

        assert_eq!(uncapped(base).lifetime_deadline(), None);
        assert_eq!(
            capped(base, 300).lifetime_deadline(),
            Some(base + Duration::from_secs(300))
        );
    }

    #[test]
    fn set_timeout_clamps_to_the_lifetime_ceiling() {
        let base = UNIX_EPOCH + Duration::from_secs(100);
        let mut metadata = capped(base, 300);

        metadata._set_timeout(Some(Duration::from_secs(3600)), base);
        assert_eq!(metadata.expires_at, Some(base + Duration::from_secs(300)));
        // 🔴 The requested timeout is kept as requested; only the deadline is
        // clamped. A caller that reads `timeout` back is told what it asked
        // for, and the ceiling is a property of the deadline.
        assert_eq!(metadata.timeout, Some(Duration::from_secs(3600)));
    }

    /// The control face for the test above.
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

    /// Renewal is where the ceiling earns its keep: a keep-alive issued halfway
    /// through the window may not push the deadline past the end of it.
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
        // No timeout means no expiry, and the ceiling does not invent one: the
        // eviction loop reads `expires_at` and nothing else, so writing the cap
        // in here would start deleting sandboxes that asked never to expire.
        assert_eq!(metadata.expires_at, None);
    }

    #[test]
    fn a_forked_child_gets_a_fresh_window_because_created_at_moved() {
        let base = UNIX_EPOCH + Duration::from_secs(100);
        let mut parent = capped(base, 300);
        parent._set_timeout(Some(Duration::from_secs(300)), base);

        let mut child = parent.clone();
        let later = base + Duration::from_secs(280);
        child.created_at = later;
        child._set_timeout(Some(Duration::from_secs(300)), later);

        assert_eq!(child.expires_at, Some(later + Duration::from_secs(300)));
    }

    #[test]
    fn projection_ttl_is_zero_without_a_ceiling() {
        let base = UNIX_EPOCH + Duration::from_secs(100);

        // 🔴 0 is the handoff to the receiver's default, and the only
        // non-positive value that may ever be emitted.
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

    /// 🔴 Ceil, not truncate. Truncation is the bug that makes a projection
    /// expire before the sandbox it points at, and it only shows up on a
    /// remainder — which a whole-numbered fixture would never produce.
    #[test]
    fn projection_ttl_rounds_a_partial_second_up() {
        let base = UNIX_EPOCH + Duration::from_secs(100);
        let metadata = capped(base, 300);

        // Exactly on a second boundary: ceil is not "+1".
        assert_eq!(metadata._projection_ttl_secs(base, 0), 300);
        // Half a second in, 299.5s left. Rounding up gives the sandbox's own
        // last second back; truncating to 299 is what retires the projection
        // while the sandbox it points at is still answering.
        assert_eq!(
            metadata._projection_ttl_secs(base + Duration::new(0, 500_000_000), 0),
            300
        );

        // A budget that is not itself a whole number of seconds.
        let ragged = SandboxMetadata {
            created_at: base,
            max_lifetime: Some(Duration::new(300, 1)),
            ..Default::default()
        };
        assert_eq!(ragged._projection_ttl_secs(base, 0), 301);
    }

    /// 🔴 The second, worse half of the bug this formula replaces: a budget
    /// smaller than one unit truncating to 0, and 0 being read downstream as
    /// "no expiry at all". Nothing here may emit a non-positive value once a
    /// ceiling exists.
    #[test]
    fn projection_ttl_floors_at_one_second_and_never_at_zero() {
        let base = UNIX_EPOCH + Duration::from_secs(100);
        let metadata = SandboxMetadata {
            created_at: base,
            max_lifetime: Some(Duration::new(0, 1)),
            ..Default::default()
        };

        assert_eq!(metadata._projection_ttl_secs(base, 0), 1);

        // Already past the deadline: still 1, still not 0. An expired sandbox
        // asks for a short-lived record, not for an immortal one.
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

    /// 🔴 N2. `load_all` runs inside `Orchestrator::new`, so a record written
    /// before this field existed has to decode or the node does not start.
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
        assert_eq!(decoded.lifetime_deadline(), None);
        // And an uncapped record keeps behaving as it did before the ceiling
        // existed, rather than being retro-fitted with this node's ceiling.
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
}

/// Cross-language golden fixtures for the `metadata` JSONB column.
///
/// 🔴 Why these files exist. The registry stores this struct as JSONB, and the
/// control plane is about to start carrying that column between processes. The
/// struct has no `deny_unknown_fields`, four `#[serde(default)]` fields, two
/// `skip_serializing_if` fields (so the key set is not even fixed), and two
/// camelCase keys buried in an otherwise snake_case document. Anything that
/// round-trips this JSON through a hand-written schema on the other side drops
/// what it does not recognise, and drops it **silently**.
///
/// Ten of the fields are not optional: if one of them goes missing the row
/// stops decoding, and every read path — `get`, `get_many`, `claim_for_resume`
/// — shares that decoder, so the sandbox becomes both unreadable and
/// unclaimable at once. It surfaces at the next resume, which may be days
/// later. These fixtures are the shared truth that both sides check themselves
/// against; the Go side reads the very same files.
#[cfg(test)]
mod golden {
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::time::{Duration, UNIX_EPOCH};

    use serde_json::{json, Value};

    use super::*;
    use crate::sandbox::{BaseSandboxNetworkPolicy, SandboxNetworkEgressPolicy};
    use crate::snapshot::CommandContext;
    use crate::types::ImageConfigs;

    /// Fields with neither `Option` nor `#[serde(default)]`. Losing any one of
    /// them makes the row permanently undecodable.
    const REQUIRED_FIELDS: [&str; 11] = [
        "id",
        // 🔴 Required, and deliberately so. A record without it is refused at
        // load rather than given a fresh incarnation, because a fresh one would
        // be a value nothing ever ran under — which is exactly what fencing
        // cannot detect.
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

    /// Fields written only when they carry something, so a valid document may
    /// or may not have them. Both shapes have a fixture.
    const OMISSIBLE_FIELDS: [&str; 2] = ["image_configs", "custom_extension_params"];

    /// The file REQUIRED_FIELDS is published in, so the other side can check
    /// itself against the list rather than against a restatement of it.
    const REQUIRED_FIELDS_FIXTURE: &str = "sandbox_metadata_required_fields.json";

    fn fixture_path(name: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures")
            .join(name)
    }

    /// Renders the metadata with every object's keys in sorted order.
    ///
    /// 🔴 The sort is not cosmetic. `storage/overlaybd` turns on serde_json's
    /// `preserve_order`, and Cargo unifies features across the workspace, so
    /// `serde_json::Map` is insertion-ordered here — which means the iteration
    /// order of the `HashMap` fields leaks straight into the output and differs
    /// on every process. A fixture rendered without this could never be pinned.
    ///
    /// Key order is not part of the contract either way: PostgreSQL's `jsonb`
    /// reorders keys on its own, so both sides compare key sets and values.
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

    /// Every optional field populated, so both `skip_serializing_if` fields are
    /// present and the camelCase keys inside `image_configs` are exercised.
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
            id: SandboxId::parse_str("0199c9a1-4f2e-7c31-a0b4-6d5e8f2a1c07").unwrap(),
            execution_id: ExecutionId::parse_str("0199c9a1-4f2e-7c31-a0b4-6d5e8f2a1c08").unwrap(),
            snapshot_id: "0199c8ff-1122-7000-8000-aabbccddeeff".to_string(),
            snapshot_alias: Some("tpl-node22".to_string()),
            state: SandboxState::Paused,
            created_at: UNIX_EPOCH + Duration::new(1_755_561_600, 123_456_789),
            timeout: Some(Duration::new(900, 0)),
            timeout_action: SandboxTimeoutAction::Delete,
            expires_at: Some(UNIX_EPOCH + Duration::new(1_755_562_500, 0)),
            auto_resume: true,
            virtualization_mode: VirtualizationMode::Pvm,
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
                SandboxNetworkEgressPolicy {
                    allowed_cidrs: vec!["10.20.0.0/16".to_string()],
                    allowed_domains: vec!["registry.npmjs.org".to_string()],
                    denied_cidrs: vec!["10.20.30.0/24".to_string()],
                },
            ),
            custom_extension_params: Some(custom_extension_params),
            secure: true,
            max_lifetime: Some(Duration::new(86_400, 0)),
            paused_state: None,
        }
    }

    /// The other shape: nothing optional set, so both omissible fields are
    /// absent from the document entirely.
    fn minimal_metadata() -> SandboxMetadata {
        SandboxMetadata {
            id: SandboxId::parse_str("0199c9a1-4f2e-7c31-a0b4-6d5e8f2a1c08").unwrap(),
            // Pinned, like the id above: `Default` mints a fresh incarnation,
            // and a fixture that changes on every run can never be a fixture.
            execution_id: ExecutionId::parse_str("0199c9a1-4f2e-7c31-a0b4-6d5e8f2a1c09").unwrap(),
            created_at: UNIX_EPOCH + Duration::new(1_755_561_600, 0),
            ..SandboxMetadata::default()
        }
    }

    /// Compares against the checked-in file, or rewrites it when
    /// `UPDATE_METADATA_GOLDEN=1` is set.
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

        // 🔴 The only two camelCase keys in the whole document. A Go tag policy
        // that snake-cases or camel-cases wholesale gets exactly these wrong.
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

    /// Both fixtures survive a decode and re-encode unchanged, which is what
    /// the registry does to them on every read.
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

    /// 🔴 Proves the fixture actually exercises the constraint it is here to
    /// protect: drop any one required field and the row stops decoding. Without
    /// this the fixture could be missing a field and still look healthy.
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

    /// 🔴 Publishes REQUIRED_FIELDS as a file of its own, which is what lets
    /// the other side be *wrong about the list* rather than merely wrong about
    /// a document.
    ///
    /// Until now the Go side restated these names in its own source and checked
    /// that each of them was present in the two metadata fixtures. That had
    /// teeth in one direction only: the fixtures carry more keys than the list
    /// names, so adding a name this side does not require failed over there,
    /// while dropping one this side does require passed. `execution_id` went
    /// missing exactly that way — it was made required here, the fixtures were
    /// regenerated, and the Go list stayed green while naming one field fewer.
    ///
    /// With the list itself in a file, both sides read the same bytes and the
    /// comparison over there is set equality, so a name added here and a name
    /// dropped there each fail. Order is not part of the contract — the other
    /// side compares sets — but the rendering is declaration order, because a
    /// fixture whose contents move on their own is not a fixture.
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

    /// The other half of the boundary. These are genuinely optional, and a
    /// document without them is valid — so a reader must not treat "fewer keys"
    /// as damage.
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
            // 🔴 The one whose absence a node actually meets in production:
            // every paused record written before the lifetime ceiling existed
            // is missing it, and `load_all` runs inside `Orchestrator::new`, so
            // a decode failure here is a node that will not start.
            "max_lifetime",
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
}
