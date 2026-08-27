//! What actually goes into a record key.
//!
//! # 🔴 `SandboxMetadata` does not survive a round trip through serde
//!
//! ```ignore
//! // orchestrator/store/metadata.rs
//! #[serde(skip)]
//! pub paused_state: Option<Arc<dyn PausedSandboxState>>,
//! ```
//!
//! `#[serde(skip)]` means the value encodes cleanly and comes back gone. A
//! store that simply serialised the struct and left `resume_sandbox` reading
//! that field would therefore make **every resume fail**, and would do it only
//! when a resume was actually attempted, long after the type checked and the
//! tests passed. `resume_sandbox` reads
//! [`MetadataStore::paused_handle`](super::super::MetadataStore::paused_handle)
//! instead, and [`PausedStateRef`] below is what this store answers it with.
//!
//! The fix already exists in this repository, in the file-backed persister:
//! `PausedSandboxState::encode() -> Value` and
//! `SandboxBackendFactory::decode_paused_state(PathBuf, Value)` are a matched
//! serialisable boundary, and `PersistedPausedRecord` stores exactly the pair
//! `{ artifact_root, state }`. [`PausedStateRef`] below is that same pair, not
//! a new format — which is what lets a record written here be understood by
//! the same decode path that reads a persisted one.
//!
//! The handle itself stays a handle: `paused_state` remains `#[serde(skip)]`
//! and remains node-local. Under `aenv-api` there is no backend factory to
//! turn the reference back into one, and there should not be: the reference
//! travels to the node that owns the bytes and is decoded there.

use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::super::{Result, SandboxMetadata, StoreError};
use crate::types::{ExecutionId, SandboxId};

/// Bumped whenever the meaning of an existing field changes.
pub const RECORD_VERSION: u32 = 1;

/// A serialisable stand-in for `paused_state`.
///
/// Same shape as `PersistedPausedRecord`'s `artifact_root` + `state` pair, on
/// purpose.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PausedStateRef {
    /// Where the paused bytes live **on the node that produced them**.
    ///
    /// 🔴 A node-local path, which is the whole reason a record has to carry
    /// `origin_node_id` as well: an `api` replica holding this path has no way
    /// to know which machine it means.
    ///
    /// 🔴 Nothing populates this, and a resume does not need it to. The store
    /// never sees an artifact root — `pause_sandbox_inner` allocates it from
    /// the persister and hands it straight to the backend — and both in-tree
    /// factories read the location out of the encoded state itself and ignore
    /// the argument when decoding. Under `aenv-api` the directory the node
    /// named travels *inside* `state`, because `RemotePausedState` puts it
    /// there along with the machine it is on. Whoever needs it at this level
    /// has to supply it; until then it decodes as `None`, which
    /// `Orchestrator::paused_state_for_resume` passes on as an empty path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_root: Option<PathBuf>,
    /// The backend's own encoding of its paused state.
    pub state: Value,
}

/// The bytes under a record key.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StoredSandboxRecord {
    /// 🔴 First, and without `#[serde(default)]`. A record whose version this
    /// build does not understand has to fail loudly; defaulting it would make
    /// every such record look like a brand-new one.
    pub version: u32,

    /// Incremented on every write. The compare-and-set at the end of a
    /// read-modify-write compares this.
    ///
    /// 🔴 Not replaced by `execution_id`: a `keep_alive` does not start a new
    /// incarnation but does change the record, so an incarnation-only
    /// predicate would let two concurrent `keep_alive`s overwrite each other.
    pub rev: u64,

    /// 🔴 Flattened, with the field names `SandboxMetadata` already uses, so
    /// that a Lua script can read `execution_id` and `state` straight out of
    /// `cjson.decode(raw)` without unwrapping a nesting level.
    #[serde(flatten)]
    pub metadata: SandboxMetadata,

    /// Expiry in unix milliseconds, or absent when the sandbox has none.
    ///
    /// 🔴 Redundant with `metadata.expires_at`, and deliberately so: Lua cannot
    /// compare serde's `SystemTime` encoding, and the eviction script has to
    /// re-check expiry atomically with the state write. Derived in
    /// [`StoredSandboxRecord::new`] and nowhere else — a second assignment site
    /// is how the two come to disagree.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at_ms: Option<i64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paused_state_ref: Option<PausedStateRef>,

    /// When the run in progress started, in unix milliseconds.
    ///
    /// 🔴 `SandboxMetadata::running_since` is `#[serde(skip)]`, and its own
    /// comment explains why: it names an interval inside *one process's* run,
    /// and a record used to leave the process only at pause, by which point the
    /// interval had already been charged into `running_elapsed`. Persisting it
    /// would have charged a node outage as running time.
    ///
    /// That reasoning does not extend to this store, and following it here
    /// would be a silent product regression. A record leaves the process on
    /// **every write** now, so dropping the field means every read anchors
    /// `lifetime_deadline` at `now` instead of at the start of the run — and a
    /// deadline that recedes with the clock is a lifetime ceiling that never
    /// bites.
    ///
    /// The hazard the original note names is also no longer the same hazard: if
    /// every `api` replica is down, the VMs on the nodes keep running, so
    /// charging that stretch as running time is the correct answer rather than
    /// an error.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub running_since_ms: Option<i64>,

    /// The node carrying this sandbox: the one running it while it is running,
    /// the one whose disk holds the bytes while it is paused.
    ///
    /// 🔴 Nothing populates this in this batch; it is here because the record
    /// format is the thing that cannot be changed later. Two independent lines
    /// of reasoning arrived at it — the structural half needed it for placement,
    /// and `PausedStateRef::artifact_root` above needs it to mean anything.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin_node_id: Option<String>,

    /// Whether the sandbox's bytes exist anywhere but the origin node's disk.
    ///
    /// 🔴 Defaults to `false`, which is the fail-closed reading: a record of
    /// unknown provenance is treated as unpublished, so a resume is pinned to
    /// the origin and fails loudly if that node is gone. The alternative
    /// failure — scheduling the sandbox onto a machine that does not have its
    /// bytes — is worse and quieter.
    #[serde(default)]
    pub published: bool,
}

impl StoredSandboxRecord {
    /// Builds a record from live metadata, encoding the paused-state handle.
    pub fn new(metadata: &SandboxMetadata, rev: u64) -> Result<Self> {
        let paused_state_ref = match metadata.paused_state.as_ref() {
            Some(state) => Some(PausedStateRef {
                artifact_root: None,
                state: state.encode().map_err(|source| StoreError::Backend {
                    source: source.context("failed to encode paused sandbox state"),
                })?,
            }),
            None => None,
        };

        Ok(Self {
            version: RECORD_VERSION,
            rev,
            expires_at_ms: metadata.expires_at.map(to_unix_millis),
            running_since_ms: metadata.running_since.map(to_unix_millis),
            metadata: metadata.clone(),
            paused_state_ref,
            origin_node_id: None,
            published: false,
        })
    }

    /// Carries forward the fields a write must not silently drop.
    ///
    /// 🔴 `origin_node_id`, `published` and the paused-state reference describe
    /// where the sandbox's bytes are. A write that recomputed them from
    /// `SandboxMetadata` alone would erase them, because `SandboxMetadata` does
    /// not carry them — and erasing `published` in particular flips a resume
    /// from "pinned to the node that has the bytes" to "pinned, but to nothing".
    pub fn inherit_placement_from(&mut self, previous: &Self) {
        if self.origin_node_id.is_none() {
            self.origin_node_id = previous.origin_node_id.clone();
        }
        if self.paused_state_ref.is_none() {
            self.paused_state_ref = previous.paused_state_ref.clone();
        }
        self.published = self.published || previous.published;
    }

    pub fn sandbox_id(&self) -> SandboxId {
        self.metadata.id
    }

    pub fn execution_id(&self) -> ExecutionId {
        self.metadata.execution_id
    }

    /// How long the record key should live, or `None` when the node has no
    /// lifetime ceiling configured.
    ///
    /// 🔴 Rounded up and floored at 1, never 0 and never negative. And note
    /// what the grace is for: the record must outlive the sandbox it describes
    /// by a wide margin, because a record that vanishes while the VM is still
    /// running makes that VM an orphan, and an orphan is killed. This is the
    /// mirror image of the projection-TTL defect from stage 1 — that one made
    /// a long TTL short; getting this one wrong with a bare `SET` makes a
    /// finite TTL infinite.
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

    /// 🔴 A record that will not decode is a backend error, never a "sandbox
    /// not found". The caller's response to absence is to delete things.
    pub fn decode(raw: &[u8]) -> Result<Self> {
        let mut record: Self =
            serde_json::from_slice(raw).map_err(|source| StoreError::Backend {
                source: anyhow::Error::from(source).context("failed to decode sandbox record"),
            })?;
        ensure_supported_version(record.version)?;
        // Restore the two fields `SandboxMetadata` refuses to carry itself.
        record.metadata.running_since = record.running_since_ms.map(from_unix_millis);
        Ok(record)
    }

    /// The metadata, with the paused-state handle left empty.
    ///
    /// Callers under the pre-split single process restore the handle from their own factory;
    /// callers under `aenv-api` pass [`StoredSandboxRecord::paused_state_ref`]
    /// to the node that owns the bytes and let it decode there.
    pub fn into_metadata(self) -> SandboxMetadata {
        self.metadata
    }
}

/// One active-state record, encoded for a node to hold on the api half's
/// behalf.
///
/// # 🔴 The name, and why it is not `control_plane_config`
///
/// `ControlPlaneConfig` is the **ownership marker**: an opaque envelope the api
/// half attaches at create time, whose presence is what makes a sandbox appear
/// in `ListSandboxes`. This is what goes *inside* that envelope. Two different
/// questions — *do we own this sandbox?* and *what did our record of it say?* —
/// and one field carrying both would eventually have to answer one of them
/// wrongly. Putting this on `SandboxMetadata` would be worse still: the payload
/// is an encoding of the record that contains `SandboxMetadata`, so a field on
/// it would contain its own container.
///
/// # What it is for
///
/// Exactly one thing: rebuilding the store after it is lost. This deployment
/// cannot make Redis highly available — two machines, every volume pinned to
/// one of them — so "the store is gone, rebuild it from the nodes" is the main
/// path rather than a fallback, and a main path that has never been run is the
/// same thing as no path at all.
///
/// It therefore has to carry **the whole record**, not a hand-picked subset. A
/// summary of the identifiers cannot reconstruct `resources`, `max_lifetime`,
/// `network_policy`, `custom_extension_params`, `runtime_versions`, `context`,
/// `startup`, `image_configs`, `virtualization_mode`, `created_at`, or the
/// placement fields — and a rebuilt record missing any of those is a sandbox
/// the control plane can list and cannot correctly manage.
///
/// # Not a cross-language contract
///
/// The node stores these bytes and returns them; it never parses them. So this
/// is the api half's contract with its own future self, versioned by
/// [`RECORD_VERSION`] like every other record here, and a node never has to be
/// upgraded in step with it.
#[derive(Clone, Debug)]
pub struct ActiveStateRecord(StoredSandboxRecord);

impl ActiveStateRecord {
    pub fn of(record: &StoredSandboxRecord) -> Self {
        Self(record.clone())
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        self.0.encode()
    }

    /// 🔴 Rejects a version it does not understand, exactly as a record read
    /// from the store does. A blob that came back from a node running against a
    /// newer api half is not a blank record.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        Ok(Self(StoredSandboxRecord::decode(bytes)?))
    }

    pub fn metadata(&self) -> &SandboxMetadata {
        &self.0.metadata
    }

    /// The record to insert during a rebuild.
    ///
    /// 🔴 The revision restarts at 1. The number counted writes against a
    /// store that no longer exists, and carrying it forward would let a write
    /// still in flight from before the loss compare-and-set successfully
    /// against the rebuilt record.
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
        // Pre-epoch instants cannot be produced by any path here, and clamping
        // is the only answer that keeps the ZSET ordering meaningful.
        Err(_) => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orchestrator::SandboxState;
    use std::sync::Arc;

    use crate::sandbox::{PausedSandboxState, RuntimeArtifactSet};

    #[derive(Debug)]
    struct FakePausedState(Value);

    impl PausedSandboxState for FakePausedState {
        fn encode(&self) -> anyhow::Result<Value> {
            Ok(self.0.clone())
        }

        fn runtime_artifacts(&self) -> RuntimeArtifactSet {
            RuntimeArtifactSet::default()
        }
    }

    fn metadata() -> SandboxMetadata {
        SandboxMetadata {
            state: SandboxState::Running,
            ..Default::default()
        }
    }

    /// 🔴 The defect this whole module exists for. Without the reference, a
    /// paused record round-trips into one that resume rejects.
    #[test]
    fn paused_state_survives_a_round_trip_as_a_reference() {
        let mut paused = metadata();
        paused.state = SandboxState::Paused;
        paused.paused_state = Some(Arc::new(FakePausedState(
            serde_json::json!({"snapshot": "abc", "mem": 42}),
        )));

        let encoded = StoredSandboxRecord::new(&paused, 1)
            .unwrap()
            .encode()
            .unwrap();
        let decoded = StoredSandboxRecord::decode(&encoded).unwrap();

        let reference = decoded
            .paused_state_ref
            .as_ref()
            .expect("paused state reference lost in the round trip");
        assert_eq!(
            reference.state,
            serde_json::json!({"snapshot": "abc", "mem": 42})
        );
    }

    /// The control for the test above: a plain serde round trip of the
    /// metadata loses the handle, which is exactly why the reference exists.
    #[test]
    fn a_plain_metadata_round_trip_still_loses_the_handle() {
        let mut paused = metadata();
        paused.paused_state = Some(Arc::new(FakePausedState(serde_json::json!({"a": 1}))));
        let bytes = serde_json::to_vec(&paused).unwrap();
        let back: SandboxMetadata = serde_json::from_slice(&bytes).unwrap();
        assert!(back.paused_state.is_none());
    }

    /// 🔴 Lua reads these two out of the decoded record directly. If the
    /// flatten is ever removed, the scripts silently stop finding them and
    /// every predicate they carry evaluates against `nil`.
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
        assert_eq!(json.get("version").and_then(Value::as_u64), Some(1));
    }

    /// 🔴 `running_since` is `#[serde(skip)]` on the metadata, so without the
    /// explicit field a Redis-backed record would come back with the clock
    /// stopped — and `lifetime_deadline` would then anchor at `now` on every
    /// read, which is a lifetime ceiling that recedes for ever and never bites.
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

        // The control: a plain metadata round trip still loses it, which is why
        // the record carries it separately.
        let bytes = serde_json::to_vec(&sandbox).unwrap();
        let back: SandboxMetadata = serde_json::from_slice(&bytes).unwrap();
        assert!(back.running_since.is_none());
    }

    /// And with the clock preserved, the deadline of a running sandbox is
    /// pinned rather than receding.
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

    /// 🔴 The seam with the node lane. `control_plane_config` lives on
    /// `SandboxMetadata`, which is `#[serde(flatten)]`ed into the stored
    /// record, so its encoding is this store's problem too — and it is a
    /// `Vec<u8>`, which plain serde would render into a JSON array of
    /// per-byte numbers inside every record.
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

    /// 🔴 The rolling-upgrade case. A record written by a replica that predates
    /// the marker must still decode, or the first upgrade makes every existing
    /// record unreadable at once.
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

    /// The rebuild payload carries the whole record and restarts the revision.
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

    /// A payload from a newer api half is refused, not read as a blank record.
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

        // A record with no version at all is not a version-1 record either.
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

    /// 🔴 `execution_id` must not acquire a default the way `max_lifetime` did:
    /// a record with no incarnation that quietly gets a fresh one is a record
    /// whose fencing compares a value nobody ever ran under.
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
        // 1.5s rounds up to 2s, plus the 10s grace.
        assert_eq!(ttl, Duration::from_secs(12));

        // An already-exhausted budget still yields a positive TTL.
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
            paused_state_ref: Some(PausedStateRef {
                artifact_root: Some(PathBuf::from("/var/lib/agentenv/x")),
                state: serde_json::json!({"k": 1}),
            }),
            ..StoredSandboxRecord::new(&metadata(), 1).unwrap()
        };
        let mut next = StoredSandboxRecord::new(&metadata(), 2).unwrap();
        next.inherit_placement_from(&previous);

        assert_eq!(next.origin_node_id.as_deref(), Some("node-a"));
        assert!(next.published);
        assert_eq!(next.paused_state_ref, previous.paused_state_ref);
    }
}
