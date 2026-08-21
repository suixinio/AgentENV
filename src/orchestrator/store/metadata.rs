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

/// The control plane's own record of a sandbox, as the node holds it.
///
/// # What it is for
///
/// It answers one question — *does the control plane own this sandbox?* — and
/// it answers it by being present. The node gRPC surface reports a sandbox on
/// [`ListSandboxes`][crate::node_server] only when the sandbox carries one of
/// these, so a sandbox created by any other path is structurally absent from
/// the answer the API half reconciles against.
///
/// # Why it is a blob
///
/// 🔴 **The node never looks inside.** It stores the bytes the API half sent
/// with the create and returns the same bytes; it does not parse, validate,
/// re-encode or generate them. That is not laziness about the format — it is
/// the property that makes the marker safe to change. The contents are the API
/// half's versioned encoding of its own record of the sandbox, which exists so
/// that a control plane whose store was lost can rebuild every running
/// sandbox's record from what the nodes hand back. A node that understood the
/// format would be a second place that has to be upgraded in step with it.
///
/// So this is a contract between the API half and its *future self*, not a
/// cross-language wire contract, and nothing on the node may come to depend on
/// its shape.
///
/// # Empty is not a value
///
/// 🔴 There is no empty `ControlPlaneConfig`. The wire type is protobuf
/// `bytes`, which has no null, so "no marker" arrives as zero bytes — and a
/// `Some(<zero bytes>)` would be a third state meaning neither *owned* nor
/// *not owned*. [`ControlPlaneConfig::from_bytes`] collapses it into `None`
/// instead, at the one boundary where the ambiguity can appear, so that
/// `Option<ControlPlaneConfig>` has exactly the two states the ownership
/// question has.
#[derive(Clone, PartialEq, Eq)]
pub struct ControlPlaneConfig(Vec<u8>);

impl ControlPlaneConfig {
    /// The marker for `bytes`, or `None` when there are none.
    ///
    /// 🔴 Fallible on purpose, and the fallible direction is the safe one: an
    /// empty blob becomes "the control plane does not own this", which keeps
    /// the sandbox out of the listing rather than putting it in with a marker
    /// that says nothing.
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

    /// Always `false`; see the type's note on why there is no empty marker.
    /// Present because `len` without it draws a clippy lint, and answering it
    /// honestly is better than allowing the lint.
    pub fn is_empty(&self) -> bool {
        false
    }
}

/// Prints the size and not the contents.
///
/// 🔴 Deliberate. The blob is a whole sandbox record; a derived `Debug` would
/// put one in every log line that formats [`SandboxMetadata`], including the
/// user metadata inside it.
impl std::fmt::Debug for ControlPlaneConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ControlPlaneConfig({} bytes)", self.0.len())
    }
}

/// Base64, because the records this rides in are JSON — in Redis and on disk —
/// and serde renders `Vec<u8>` there as an array of numbers, one per byte.
impl Serialize for ControlPlaneConfig {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use base64::{engine::general_purpose::STANDARD, Engine as _};
        serializer.serialize_str(&STANDARD.encode(&self.0))
    }
}

/// Reads what [`Serialize`] wrote, for a field that is present.
///
/// The absent and empty cases are handled by
/// [`deserialize_optional_control_plane_config`], which is what the field
/// actually uses; this impl exists so the type round-trips on its own.
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

/// Decodes the ownership marker, mapping both "absent" and "present but empty"
/// onto `None`.
///
/// 🔴 Malformed base64 is still an error. The two failures are not the same
/// one: a field nobody wrote is a sandbox the control plane does not own, while
/// a field somebody wrote and got wrong is a corrupt record, and silently
/// reading the second as the first would let a mangled record pass for an
/// ordinary unowned sandbox.
fn deserialize_optional_control_plane_config<'de, D>(
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
    /// The control plane's own record of this sandbox, stored verbatim.
    ///
    /// 🔴 **This is the ownership marker, and it is explicit on purpose.**
    /// `Some(_)` means the control plane created this sandbox and owns the
    /// cluster-wide record of it; `None` means it does not. Nothing infers
    /// ownership from where a create came from, from which port it arrived on,
    /// or from what else is on the node — those are all inferences that hold
    /// today and stop holding the first time someone adds a second caller.
    ///
    /// 🔴 **Opaque to this node.** The node stores what the API half sent with
    /// the create and hands the same bytes back on
    /// [`SandboxOrchestration::list_live_sandboxes`][crate::orchestrator::SandboxOrchestration::list_live_sandboxes].
    /// It never parses, validates or generates one. See [`ControlPlaneConfig`].
    ///
    /// 🔴 `#[serde(default)]`, unlike `execution_id` above, and in the opposite
    /// direction: a record from before this field existed — or one written by a
    /// path that does not set it — decodes to `None`, which reads as *not owned
    /// by the control plane*. That is fail-closed. The consumer of this field
    /// filters sandboxes **out** of a listing when it is absent, and a listing
    /// that omits a sandbox costs nothing, while a listing that includes one it
    /// should not is how something else comes to delete a live VM.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_control_plane_config"
    )]
    pub control_plane_config: Option<ControlPlaneConfig>,
    /// The lifetime ceiling this sandbox was created under, or `None` when the
    /// node had no ceiling configured.
    ///
    /// 🔴 A budget of *running* time, not a wall-clock window from
    /// `created_at`. It is spent by `running_elapsed` below and by the run in
    /// progress, and a sandbox that sits paused spends none of it. See
    /// [`SandboxState::spends_lifetime`] for why paused time is free.
    ///
    /// Stored as the budget rather than as an absolute deadline on purpose: a
    /// fork restarts the clock and so gets a whole fresh window, while a resume
    /// picks the same one back up where it was left. Neither needs a line of
    /// clamping code at the call site.
    ///
    /// 🔴 `#[serde(default)]` is load-bearing here, unlike on `execution_id`
    /// above: `persister.load_all` runs inside `Orchestrator::new`, so a
    /// required field would mean a node that cannot start after an upgrade,
    /// because every paused record written by an earlier build is missing it.
    /// Do not copy the neighbour.
    #[serde(default)]
    pub max_lifetime: Option<Duration>,
    /// Running time already spent, summed over the runs that have finished.
    /// The run in progress is not in here — `running_since` holds its start.
    ///
    /// 🔴 `#[serde(default)]` for the same reason as `max_lifetime`: a paused
    /// record written before this field existed has to decode, or the node
    /// stops starting. Decoding to zero is also the right answer for such a
    /// record — the build that wrote it had no way to spend the budget.
    #[serde(default)]
    pub running_elapsed: Duration,
    /// When the run in progress started, or `None` while the sandbox is paused.
    ///
    /// 🔴 Deliberately not persisted. This names an interval inside *one
    /// process's* run of the sandbox, and a record only ever leaves this
    /// process at pause, by which point `pause_sandbox_inner` has already
    /// charged the interval into `running_elapsed`. Persisting it would buy
    /// nothing and introduce exactly one new failure mode: a record whose open
    /// interval spans a node outage, charging the downtime as running time —
    /// which is the bug this whole field exists to prevent.
    ///
    /// The invariant `running_since.is_some() == state.spends_lifetime()` is
    /// maintained by [`SandboxMetadata::sync_running_clock`], which the
    /// metadata store calls after every mutation it performs.
    #[serde(skip)]
    pub running_since: Option<SystemTime>,
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
            // 🔴 Not owned by the control plane. A `Default` record is a test
            // fixture, and a fixture that arrived pre-owned would make every
            // ownership test pass for the wrong reason.
            control_plane_config: None,
            max_lifetime: None,
            running_elapsed: Duration::ZERO,
            running_since: None,
            paused_state: None,
        }
    }
}

impl SandboxMetadata {
    pub fn set_timeout(&mut self, timeout: Option<Duration>) {
        self._set_timeout(timeout, SystemTime::now());
    }

    /// The instant this sandbox runs out of lifetime budget. `None` means it
    /// has no ceiling.
    ///
    /// 🔴 Two anchors, one formula. While the sandbox is running the window is
    /// pinned to the start of the run, so the answer is fixed for the duration
    /// of that run and may already be in the past — which is exactly what
    /// `keep_alive_for` reports as `SandboxLifetimeExceeded`. While it is
    /// paused nothing is being spent, so the window is anchored at `now` and
    /// the deadline recedes with it: a sandbox paused for a week resumes with
    /// the budget it went away with, instead of resuming into a deadline that
    /// expired while it was gone.
    pub fn lifetime_deadline(&self, now: SystemTime) -> Option<SystemTime> {
        let remaining = self.max_lifetime?.saturating_sub(self.running_elapsed);
        let anchor = match self.running_since {
            // The state is consulted as well as the timestamp, so a record that
            // somehow carries a stale start — one that never went through
            // `sync_running_clock` — degrades to "the clock is not running"
            // rather than to "the budget has been draining since then".
            Some(since) if self.state.spends_lifetime() => since,
            _ => now,
        };

        anchor.checked_add(remaining)
    }

    /// Brings the running clock in step with `state`, charging a run that has
    /// just ended and starting one that has just begun.
    ///
    /// Idempotent, and driven by nothing but `state`. That is what lets the
    /// metadata store call it after *every* mutation it performs — including
    /// the ones whose callback sets `state` directly — and still lets a caller
    /// that needs the exact instant call it early. `pause_sandbox_inner` does
    /// exactly that: it has to charge the run before the record is persisted,
    /// because the persisted copy is the one a restarted node reads back.
    pub fn sync_running_clock(&mut self, now: SystemTime) {
        match (self.state.spends_lifetime(), self.running_since) {
            (true, None) => self.running_since = Some(now),
            (false, Some(since)) => {
                self.running_elapsed = self
                    .running_elapsed
                    .saturating_add(now.duration_since(since).unwrap_or(Duration::ZERO));
                self.running_since = None;
            }
            _ => {}
        }
    }

    /// Gives this record a whole fresh lifetime window.
    ///
    /// Only a fork's child gets one: it is a new sandbox that happens to have
    /// been built from a running one, so it starts its budget at zero the same
    /// way it starts `created_at` at `now`. A resume is the other case, and it
    /// deliberately does not call this — the sandbox coming back is the same
    /// sandbox, and it picks its budget up where it left it.
    pub fn restart_lifetime_clock(&mut self, now: SystemTime) {
        self.running_elapsed = Duration::ZERO;
        self.running_since = self.state.spends_lifetime().then_some(now);
    }

    /// How long this sandbox's routing projection should live, in whole
    /// seconds, or `0` when there is no ceiling to derive it from.
    ///
    /// Derived from the *remaining running budget*, so a paused sandbox reports
    /// the whole of what it has left rather than a window that has been
    /// draining while it sat still. The worst-case leak the ceiling bounds is
    /// unchanged by that: a record still cannot outlive `max_lifetime` plus the
    /// grace without a heartbeat to renew it.
    ///
    /// 🔴 `0` is the only non-positive value that may ever leave here, and it
    /// means "use the receiver's default TTL". It must never be read as "never
    /// expires": a projection that outlives every path able to delete it is a
    /// route pointing at a sandbox nobody can reach.
    pub fn projection_ttl_secs(&self, now: SystemTime) -> u32 {
        self._projection_ttl_secs(now, configured_projection_ttl_grace_secs())
    }

    fn _projection_ttl_secs(&self, now: SystemTime, grace_secs: u64) -> u32 {
        let Some(cap) = self.lifetime_deadline(now) else {
            return 0;
        };
        let remaining = cap.duration_since(now).unwrap_or(Duration::ZERO);
        // 🔴 Ceil, not truncate, and floored at 1. Truncating whole units is
        // how a projection comes to expire before the sandbox it points at;
        // flooring at 1 is how a sub-unit remainder avoids collapsing into a
        // zero that the receiver would have to interpret.
        let secs = remaining
            .as_secs()
            .saturating_add(u64::from(remaining.subsec_nanos() > 0));
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

    /// The ownership marker, on its own.
    ///
    /// 🔴 These are the tests that decide whether a sandbox appears in the
    /// answer `node_reclaim`'s counterpart reconciles against, so each one
    /// states which direction its failure goes.
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
            // 🔴 The wire type is protobuf `bytes`, which cannot distinguish
            // "no marker" from "an empty one". Collapsing them here is what
            // stops `Some(<zero bytes>)` existing as a third answer to a
            // two-valued question.
            assert_eq!(ControlPlaneConfig::from_bytes(Vec::new()), None);
            assert_eq!(ControlPlaneConfig::from_bytes(""), None);
            assert!(ControlPlaneConfig::from_bytes(vec![0u8]).is_some());
        }

        #[test]
        fn the_bytes_come_back_exactly_as_they_went_in() {
            // The node stores what it was sent. Anything that normalises,
            // re-encodes or trims would break the only consumer there is: a
            // control plane decoding its own record.
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
            // 🔴 All three are fail-closed: the sandbox is left out of the
            // control plane's listing, which costs nothing, rather than put
            // into it carrying a marker that says nothing.
            for field in [
                Value::Null,
                Value::String(String::new()),
                // base64 of zero bytes is also the empty string, so this is
                // the same case reached the other way.
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
            // 🔴 The opposite direction from the test above, and deliberately
            // so. A field nobody wrote is a sandbox nobody owns; a field
            // somebody wrote and got wrong is a corrupt record, and reading
            // the second as the first would let corruption pass for an
            // ordinary unowned sandbox.
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
            // The blob is a whole sandbox record, user metadata included, and
            // `SandboxMetadata` is formatted into logs.
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

    /// A record with no ceiling behaves exactly as it did before the ceiling
    /// existed. This is the control for every test below it: without it,
    /// "clamped to 300" could just as well be "the timeout was 300 all along".
    fn uncapped(base: SystemTime) -> SandboxMetadata {
        SandboxMetadata {
            created_at: base,
            max_lifetime: None,
            running_since: Some(base),
            ..Default::default()
        }
    }

    /// A sandbox that has been running since `base` and has spent none of its
    /// budget yet. `running_since` is set explicitly rather than left to the
    /// store, so every deadline below is a fixed instant these tests can name.
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

        // Half the budget already spent by earlier runs: the window that is
        // left is half as long, and it still starts at this run's start.
        let mut resumed = capped(base, 300);
        resumed.running_elapsed = Duration::from_secs(150);
        assert_eq!(
            resumed.lifetime_deadline(base),
            Some(base + Duration::from_secs(150))
        );

        // 🔴 And it does not move while the run is under way: the deadline read
        // 100 seconds in is the same instant, not 100 seconds later.
        assert_eq!(
            resumed.lifetime_deadline(base + Duration::from_secs(100)),
            Some(base + Duration::from_secs(150))
        );
    }

    /// 🔴 The defect this model exists to fix. A paused sandbox is not running,
    /// so its clock is stopped: the deadline is measured from whenever it comes
    /// back, however long it has been away.
    #[test]
    fn a_paused_sandbox_carries_its_budget_forward_instead_of_burning_it() {
        let base = UNIX_EPOCH + Duration::from_secs(100);
        let paused = SandboxMetadata {
            created_at: base,
            state: SandboxState::Paused,
            max_lifetime: Some(Duration::from_secs(86_400)),
            running_elapsed: Duration::from_secs(60),
            running_since: None,
            ..Default::default()
        };

        // Twenty-five hours later — past the point where a deadline derived
        // from `created_at` alone would already have expired.
        let much_later = base + Duration::from_secs(90_000);
        assert_eq!(
            paused.lifetime_deadline(much_later),
            Some(much_later + Duration::from_secs(86_340)),
            "the budget left is the ceiling minus the minute actually spent running"
        );
    }

    /// The clock is driven by `state` and by nothing else, and running it twice
    /// costs nothing — which is what lets the store call it after every write
    /// while `pause_sandbox_inner` also calls it early, at the instant it needs.
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
        // Still Running: a second sync must not restart the run and throw the
        // elapsed time away.
        metadata.sync_running_clock(base + Duration::from_secs(30));
        assert_eq!(metadata.running_since, Some(base));
        assert_eq!(metadata.running_elapsed, Duration::ZERO);

        metadata.state = SandboxState::Paused;
        metadata.sync_running_clock(base + Duration::from_secs(60));
        assert_eq!(metadata.running_elapsed, Duration::from_secs(60));
        assert_eq!(metadata.running_since, None);
        // And a second sync while paused charges nothing further, however much
        // wall-clock time goes by.
        metadata.sync_running_clock(base + Duration::from_secs(90_000));
        assert_eq!(metadata.running_elapsed, Duration::from_secs(60));

        // Resuming picks the same budget back up rather than starting over.
        metadata.state = SandboxState::Running;
        metadata.sync_running_clock(base + Duration::from_secs(90_000));
        assert_eq!(
            metadata.running_since,
            Some(base + Duration::from_secs(90_000))
        );
        assert_eq!(metadata.running_elapsed, Duration::from_secs(60));
    }

    /// A fork's child is a new sandbox, so it starts the budget over. A resume
    /// is the same sandbox, so it does not — the contrast is the test.
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

    /// A fork's child gets a whole fresh window. The parent, 280 seconds into
    /// a 300-second budget, is 20 seconds from its own ceiling at that moment —
    /// so a child that merely cloned the record would be too.
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
            running_since: Some(base),
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
            running_since: Some(base),
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
        assert_eq!(decoded.lifetime_deadline(SystemTime::now()), None);
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

    /// 🔴 N2 again, for the field the ceiling is actually spent through. A node
    /// upgraded onto this build reads paused records that predate
    /// `running_elapsed`, and `load_all` runs inside `Orchestrator::new` — so a
    /// record that does not decode is a node that does not start.
    #[test]
    fn a_record_written_before_the_running_clock_existed_still_decodes() {
        let mut document = serde_json::to_value(SandboxMetadata {
            created_at: UNIX_EPOCH + Duration::from_secs(100),
            state: SandboxState::Paused,
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

        // Nothing spent, which is the truthful reading: the build that wrote
        // the record had no way to spend the budget.
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

    /// The spent budget is the half of the model that has to survive a restart,
    /// and it does — while the open interval deliberately does not.
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
        // 🔴 Dropped on purpose. A persisted record is a paused record, and the
        // only way an open interval could reach the wire is a run this process
        // never closed — reading it back would charge the whole outage.
        assert_eq!(decoded.running_since, None);
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
    const OMISSIBLE_FIELDS: [&str; 3] = [
        "image_configs",
        "custom_extension_params",
        // 🔴 Absent means "the control plane does not own this sandbox", which
        // is why it belongs here and not among the required fields: the whole
        // point of the marker is that a record without one is still a valid
        // record, describing a sandbox nobody claims.
        "control_plane_config",
    ];

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
            // The bytes are nonsense on purpose: the node stores whatever the
            // control plane sent, so a fixture that held valid JSON would
            // invite someone to start reading it.
            control_plane_config: ControlPlaneConfig::from_bytes(vec![
                0x00, 0x01, 0xfe, 0xff, b'o', b'w', b'n', b'e', b'd',
            ]),
            max_lifetime: Some(Duration::new(86_400, 0)),
            running_elapsed: Duration::new(5_400, 0),
            // Not written: `#[serde(skip)]`, and this record is paused anyway.
            running_since: None,
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
            // 🔴 The two whose absence a node actually meets in production:
            // every paused record written before the lifetime ceiling existed
            // is missing them, and `load_all` runs inside `Orchestrator::new`,
            // so a decode failure here is a node that will not start.
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
}
