use std::{
    collections::HashMap,
    sync::Arc,
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
            paused_state: None,
        }
    }
}

impl SandboxMetadata {
    pub fn set_timeout(&mut self, timeout: Option<Duration>) {
        self._set_timeout(timeout, SystemTime::now());
    }

    fn _set_timeout(&mut self, timeout: Option<Duration>, from: SystemTime) {
        self.timeout = timeout;
        self.expires_at = timeout.and_then(|ttl| from.checked_add(ttl));
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

    /// The other half of the boundary. These five are genuinely optional, and a
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
