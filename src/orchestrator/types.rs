use std::collections::HashMap;
use std::fmt::Display;
use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::sandbox::CustomExtensionParams;
use crate::snapshot::CommandContext;
use crate::types::{ExecutionId, ImageConfigs, SandboxId, SandboxResources};

#[derive(Clone)]
pub enum SandboxLaunchSource {
    Snapshot(Box<crate::runtime_snapshot::RunnableSnapshot>),
    Image {
        image_ref: String,
        overlaybd_config_path: PathBuf,
        context: Box<CommandContext>,
        resources: Option<crate::types::SandboxResources>,
        extra_drives: Vec<crate::sandbox::ExtraDrive>,
        extra_boot_args: Option<String>,
        /// Raw source image config metadata for the sandbox's resolved images.
        image_configs: Box<ImageConfigs>,
    },
    /// A cold create from an OCI image reference this orchestrator cannot
    /// resolve itself.
    ///
    /// # 🔴 The unresolved counterpart to [`Self::Image`]
    ///
    /// `Image` above carries a `regctl`-resolved local path — what a
    /// machine-local cold create (`aenv-node`) already has by
    /// the time it builds a launch source. `aenv-api` has no `regctl`, so
    /// `sandboxes_cold_post` cannot produce that variant at all when
    /// `ApiImpl::runs_sandbox_runtime` is false; this is what it builds
    /// instead. It carries only what is known without an image resolver: the
    /// reference itself, the already-computed resources (no image needed to
    /// size a sandbox), and attached drives named by reference rather than by
    /// local path. `create_sandbox_inner` routes it to
    /// `SandboxBackendFactory::build_from_image_ref`, whose only real
    /// implementation (`RemoteSandboxBackendFactory`) ships it to a node that
    /// *can* resolve it.
    UnresolvedImage {
        image_ref: String,
        resources: crate::types::SandboxResources,
        attached_drives: Vec<crate::sandbox::UnresolvedAttachedDrive>,
        extra_boot_args: Option<String>,
    },
    /// A create from a committed snapshot this orchestrator has *not* resolved
    /// into local bytes.
    ///
    /// # 🔴 The unresolved counterpart to [`Self::Snapshot`]
    ///
    /// [`Self::Snapshot`] above carries a [`RunnableSnapshot`], and obtaining
    /// one is not a lookup: `SnapshotRuntimeResolver::resolve` downloads
    /// `vm_state.bin` onto *this* machine's disk, materializes the memory and
    /// rootfs overlaybd `image.json` files, and returns a lease pinning all of
    /// it in this process's local artifact cache. That is exactly right for a
    /// process that is about to boot a Firecracker VM from those bytes, and
    /// pure waste for one that is not: `aenv-api` hands the create to a node
    /// over gRPC, and `RemoteSandboxBackendFactory::build_from_snapshot` reads
    /// only `record.id`, the serialized catalog row and `record.resources`
    /// back out of the `RunnableSnapshot` — the manifest, the lease and every
    /// downloaded file are dropped unread. The node resolves the row itself
    /// (`NodeSandboxService::create` calls `resolve_runnable` on the row this
    /// variant carries), against the local cache whose bytes its VM will
    /// actually mmap.
    ///
    /// So this variant carries the catalog row and nothing else — the same row
    /// `SnapshotSource.resolved_record` already puts on the wire, which is why
    /// the request a node receives is byte-for-byte what the resolving path
    /// used to send. `create_sandbox_inner` routes it to
    /// [`SandboxBackendFactory::build_from_snapshot_record`], whose only real
    /// implementation ships it to a node that can resolve it.
    ///
    /// [`RunnableSnapshot`]: crate::runtime_snapshot::RunnableSnapshot
    /// [`SandboxBackendFactory::build_from_snapshot_record`]: crate::sandbox::SandboxBackendFactory::build_from_snapshot_record
    SnapshotRecord(Box<crate::snapshot::SnapshotRecord>),
}

/// Who keeps a new sandbox's deadline, and — when it is this orchestrator —
/// what it is.
///
/// # 🔴 Three answers, not two
///
/// This used to be an `Option<Duration>`, and `None` was made to carry two
/// unrelated instructions at once:
///
/// * *the caller named no deadline, so use the configured default* — which is
///   what a user posting to `POST /sandboxes` without a `timeout` means; and
/// * *the caller keeps this sandbox's deadline itself, so keep none* — which is
///   what the API half means when it asks a node to run a sandbox whose record,
///   whose expiry index and whose eviction loop all live in the API half.
///
/// One process answered both the same way and nothing noticed, because in a
/// the pre-split single process server the second sender does not exist. Split the halves apart
/// and it does: the API half sent "you do not own this deadline", the node read
/// "use your own default", and `[orchestrator].default_sandbox_timeout_secs`
/// then paused a running VM out from under an owner that went on reporting it
/// as running. Every later call on that sandbox failed for a reason that named
/// something else — `invalid state Paused` on a network update, 404 on a
/// pause — so the one fact worth knowing was the one nothing said.
///
/// Making the caller pick one of three is what stops that from being
/// expressible again: there is no value here that a sender can leave unset and
/// have guessed at.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SandboxExpiry {
    /// This orchestrator keeps the deadline, and it is this long.
    After(Duration),
    /// This orchestrator keeps the deadline and the caller named none, so
    /// [`default_sandbox_timeout_secs`][crate::cfg::OrchestratorConfig::default_sandbox_timeout_secs]
    /// is the answer.
    ///
    /// 🔴 The answer for a *client*, and only for a client. Something that
    /// keeps its own record of the sandbox is not a caller that "did not say";
    /// it is a caller that said [`NotKeptHere`](Self::NotKeptHere).
    AfterConfiguredDefault,
    /// This orchestrator keeps no deadline for the sandbox, and its eviction
    /// loop will therefore never touch it.
    ///
    /// 🔴 Read it as *not mine to keep*, which covers both callers that send
    /// it: the API half, which keeps the deadline in its own record and evicts
    /// from there, and a restore whose record genuinely carries no deadline.
    /// What both are saying is that this orchestrator deciding on its own that
    /// the sandbox's time is up would be a second ledger for one sandbox.
    NotKeptHere,
}

#[derive(Clone)]
pub struct CreateSandboxRequest {
    pub source: SandboxLaunchSource,
    /// 🔴 Deliberately not an `Option<Duration>`; see [`SandboxExpiry`].
    pub expiry: SandboxExpiry,
    pub timeout_action: super::SandboxTimeoutAction,
    pub auto_resume: bool,
    pub user_metadata: Option<HashMap<String, String>>,
    pub env_vars: Option<HashMap<String, String>>,
    pub network_policy: crate::sandbox::SandboxNetworkPolicy,
    pub secure: bool,
    /// Opaque user-provided JSON passed through to the custom extension hooks.
    pub custom_extension_params: Option<CustomExtensionParams>,
    /// The control plane's ownership marker for this sandbox, stored verbatim.
    ///
    /// 🔴 `None` for every user-facing create. Only the node gRPC surface — the
    /// one the API half drives — supplies one, and its presence is the *only*
    /// thing that marks a sandbox as the control plane's. See
    /// [`ControlPlaneConfig`][crate::orchestrator::ControlPlaneConfig].
    pub control_plane_config: Option<crate::orchestrator::ControlPlaneConfig>,
    /// The incarnation this create must run under, when the caller has already
    /// minted one.
    ///
    /// 🔴 `None` everywhere a user asks for a sandbox, and that is the case
    /// that mints. `Some` exists for one caller: a node running a create on
    /// behalf of the orchestrator that owns the sandbox, which minted the
    /// incarnation before it asked and has already written it into its own
    /// record. Two records of one sandbox naming two different runs is what
    /// this prevents, and fencing compares exactly that value.
    pub execution_id: Option<ExecutionId>,
}

/// One sandbox this node is *running*, as against one it has a record of.
///
/// # 🔴 Why the two are not the same list
///
/// The record store is what a node claims to be holding; the table of live
/// sandbox handles is what it holds. They come apart exactly when something has
/// gone wrong — a create that half-failed, a teardown that did not finish — and
/// those are the cases anything reconciling a cluster against its nodes exists
/// to find. Answering from the store would be reconciling one ledger against
/// another.
///
/// So membership here comes from the handle table and nothing else. The record
/// store contributes attributes, and the fields it contributes are `Option`
/// because a live sandbox with no record is a real thing that must still be
/// reported.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LiveSandbox {
    pub sandbox_id: SandboxId,
    /// The run that is live, taken from the handle when the handle could be
    /// read and from the record otherwise.
    ///
    /// 🔴 `None` only when neither had one, which means the sandbox has no
    /// record and its handle was busy. Such a sandbox necessarily has no
    /// ownership marker either — the marker lives in the record — so it is not
    /// one any control plane is looking for.
    pub execution_id: Option<ExecutionId>,
    /// Whether the facts below were read from the live handle.
    ///
    /// 🔴 `false` means the handle was mid-operation and was not waited on. A
    /// listing that blocked behind a pause would take as long as the slowest
    /// operation on the node, and the thing waiting for it is deciding whether
    /// sandboxes still exist — so it reports what the record knows and says so,
    /// rather than either stalling or leaving the sandbox out.
    pub facts_from_handle: bool,
    pub host_interaction_ip: Option<std::net::Ipv4Addr>,
    pub rootfs_virtual_size: Option<u64>,
    pub created_at: Option<std::time::SystemTime>,
    pub expires_at: Option<std::time::SystemTime>,
    pub resources: Option<SandboxResources>,
    /// The control plane's ownership marker for this sandbox, when it has one.
    ///
    /// 🔴 Reported, not filtered on. Whether a caller wants only the sandboxes
    /// some control plane owns is a property of the surface being served, not
    /// of this list: the same list also answers "what is running on this
    /// machine at all", and a filter baked in here would quietly give that
    /// question the other one's answer.
    pub control_plane_config: Option<crate::orchestrator::ControlPlaneConfig>,
}

/// The children a fork is being asked to produce.
///
/// 🔴 Identity and ownership travel together, and both are the *caller's* to
/// decide when the caller is the control plane. Two reasons they cannot be
/// split:
///
/// - A child's record is built by cloning its parent's, so anything the child
///   must not inherit has to be overwritten from here. The incarnation was
///   already such a field; the ownership marker is the second.
/// - The marker is how the control plane recognises its own sandbox, so it is
///   written before the sandbox exists and names it. A node that minted the
///   child's id would therefore be handing back a marker written before anyone
///   knew which sandbox it described.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ForkChildren {
    /// Mint `count` fresh identities here. No child carries an ownership
    /// marker: this is the user-facing fork, and the control plane did not ask
    /// for these sandboxes.
    Fresh(u32),
    /// One entry per child, in request order.
    ///
    /// 🔴 The order is the contract [`SandboxBackend::fork`] already states for
    /// its results — one result per spec, same order — and the outcomes this
    /// produces line up with it entry for entry.
    ///
    /// [`SandboxBackend::fork`]: crate::sandbox::SandboxBackend::fork
    Assigned(Vec<ForkChildAssignment>),
}

/// One fork child whose identity the caller has already decided.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ForkChildAssignment {
    /// 🔴 The caller's to choose, because its own record of the child names it
    /// and that record is written before the child exists.
    pub sandbox_id: SandboxId,
    /// The incarnation this child must run under, when the caller has already
    /// minted one.
    ///
    /// 🔴 Same rule as [`CreateSandboxRequest::execution_id`]: `None` mints
    /// here, and `Some` is for the one caller that is itself the orchestrator
    /// that owns the child and has already recorded the value.
    pub execution_id: Option<ExecutionId>,
    /// The control plane's ownership marker for this child, or `None`.
    pub control_plane_config: Option<crate::orchestrator::ControlPlaneConfig>,
}

impl ForkChildren {
    /// How many children this asks for.
    pub fn count(&self) -> u32 {
        match self {
            Self::Fresh(count) => *count,
            // A fork request cannot carry more children than a u32 counts, and
            // the API surface it arrives through is bounded far below that.
            Self::Assigned(children) => children.len().try_into().unwrap_or(u32::MAX),
        }
    }
}

/// One sandbox as the heartbeat reports it: which sandbox, which incarnation,
/// and how long its routing projection should live.
///
/// The TTL travels with the roster and not only on the create response because
/// the roster is the *repair* path — the one that reinstalls a projection write
/// that was lost. A repair that installs the receiver's default TTL instead of
/// the sandbox's real budget turns one dropped write into a permanently
/// short-lived record, which is exactly the failure the budget exists to avoid.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SandboxRosterEntry {
    pub sandbox_id: SandboxId,
    pub execution_id: ExecutionId,
    /// 🔴 `0` means "use the receiver's default", never "never expires".
    pub projection_ttl_secs: u32,
    /// Whether this sandbox is parked: on this node's disk, with no VM behind
    /// it.
    ///
    /// Reported rather than filtered out here, and the distinction is the
    /// whole point of the flag. The heartbeat roster is the *only* thing that
    /// renews a paused sandbox's row in the cluster registry, so a node that
    /// stopped naming its paused sandboxes would let their leases lapse and
    /// another node claim rows whose snapshot lives on this node's disk alone.
    /// What the receiver does with the flag is withhold the entry from
    /// *binding* reconciliation, so a parked sandbox holds no routing
    /// projection and the gateway takes its wake path instead of answering the
    /// data plane out of a projection that points at a VM that is not running.
    pub paused: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SandboxLifecycleEventType {
    Create,
    Delete,
    Pause,
    Resume,
    Fork,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SandboxLifecycleEvent {
    pub event_type: SandboxLifecycleEventType,
    pub sandbox_id: SandboxId,
    /// The incarnation this event belongs to.
    ///
    /// 🔴 Not an `Option`. Every one of the five publish points holds the
    /// incarnation on the line that sends the event, so an absent value here
    /// could only ever mean "somebody forgot", and a delete guarded by a value
    /// that means that is not guarded at all. `ExecutionId` is a `Uuid`
    /// newtype, so this stays `Copy`.
    pub execution_id: ExecutionId,
    pub resources: SandboxResources,
}

#[derive(Clone, Debug, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SandboxState {
    Creating,
    Resuming,
    Running,
    Snapshotting,
    Forking,
    Pausing,
    Paused,
    Killing,
}

impl SandboxState {
    /// Whether a sandbox in this state is spending its lifetime budget.
    ///
    /// 🔴 Everything but `Paused`, and the asymmetry is the point. A paused
    /// sandbox is a row and a set of layers on disk: no VM, no vCPU, no memory,
    /// no network slot. Every other state — the transitional ones included —
    /// has a machine attached to it. So the ceiling bounds how long a sandbox
    /// may *run*, and wall-clock time spent paused does not count against it.
    ///
    /// The alternative, charging paused time, is what the first cut of the
    /// ceiling did by deriving the deadline from `created_at` alone, and it
    /// made a sandbox resumed a day after creation resume successfully and then
    /// be evicted within the second — because its deadline was already in the
    /// past. e2b, which this whole stage follows, cannot even ask the question:
    /// it has no paused state (`sandboxtypes/states.go` is running / pausing /
    /// killing / snapshotting), and a paused sandbox there leaves the active
    /// store entirely to become a catalog row, so its `MaxInstanceLength` can
    /// only ever bound running time. Charging paused time here would be a
    /// product change smuggled into an infrastructure stage.
    pub fn spends_lifetime(self) -> bool {
        !matches!(self, SandboxState::Paused)
    }
}

impl Display for SandboxState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            SandboxState::Creating => "creating",
            SandboxState::Resuming => "resuming",
            SandboxState::Running => "running",
            SandboxState::Snapshotting => "snapshotting",
            SandboxState::Forking => "forking",
            SandboxState::Pausing => "pausing",
            SandboxState::Paused => "paused",
            SandboxState::Killing => "killing",
        };
        write!(f, "{s}")
    }
}

#[derive(Debug)]
pub struct SnapshotCaptureResult {
    pub metadata: super::store::SandboxMetadata,
    pub captured_snapshot: crate::sandbox::CapturedSandboxSnapshot,
}

/// Describes the snapshot a capture of `metadata`'s sandbox will become.
///
/// # 🔴 One place, because the fields are facts about a machine and there is
/// only one machine that has them
///
/// Every field but two is copied straight off the sandbox's own record: the
/// kernel and Firecracker it is running under, the images it resolved, the
/// resources it was given. Three call sites need this value — the user-facing
/// capture API, the pause publisher, and the node service serving a
/// `Checkpoint` for a caller that has no sandbox of its own — and a second copy
/// of the list is how one of them comes to be missing `custom_extension_params`
/// on the day it is added, which nothing downstream would report as wrong.
///
/// 🔴 The id is minted here and is **not** a caller's to choose. Staging writes
/// the bytes into the directory the id names, so the id belongs to whoever
/// writes them.
pub fn capture_publish_metadata(
    metadata: &super::store::SandboxMetadata,
    alias: Option<crate::snapshot::SnapshotAlias>,
) -> crate::snapshot::SnapshotPublishMetadata {
    crate::snapshot::SnapshotPublishMetadata {
        id: crate::snapshot::SnapshotId::generate(),
        alias,
        source: crate::snapshot::SnapshotPublishSource::Sandbox {
            source_sandbox_id: metadata.id.to_string(),
        },
        context: metadata.context.clone(),
        startup: metadata.startup.clone(),
        resources: metadata.resources,
        runtime_versions: metadata.runtime_versions.clone(),
        virtualization_mode: metadata.virtualization_mode,
        image_configs: metadata.image_configs.clone(),
        custom_extension_params: metadata.custom_extension_params.clone(),
    }
}

/// What a completed pause leaves for the caller to act on.
///
/// The sandbox is already paused, persisted and stopped by the time this is
/// returned; `publishable` is the same capture in a form a snapshot repository
/// can commit, offered so the caller can make the paused sandbox resumable
/// beyond this node. It is `None` when the backend captured into managed
/// temporaries, and callers that only care about the pause itself may drop it.
#[derive(Debug)]
pub struct PauseOutcome {
    pub metadata: super::store::SandboxMetadata,
    pub publishable: Option<crate::sandbox::CapturedSandboxSnapshot>,
}

impl PauseOutcome {
    /// A pause with no capture to hand anyone.
    ///
    /// 🔴 Three different situations answer this way and none of them is a
    /// failure: the sandbox was already paused, this call joined someone else's
    /// pause, or the capture has already been given to a publisher in this
    /// process. In all three the capture belongs to the pause that produced it
    /// and is gone, which is what an absent `publishable` has always meant.
    pub fn nothing_to_publish(metadata: super::store::SandboxMetadata) -> Self {
        Self {
            metadata,
            publishable: None,
        }
    }
}

/// Who commits the capture a pause produces.
///
/// 🔴 Not a boolean, because the two arms differ in more than whether a value
/// comes back: [`PausePublication::Here`] offers the capture to this process's
/// own publisher and [`PausePublication::ByCaller`] does not offer it to
/// anyone. A pause that did both would write two rows for one pause.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PausePublication {
    /// This process's paused-sandbox publisher, if it has one. The ordinary
    /// pause, and the only one a machine-local role ever performs.
    Here,
    /// Whoever asked for the pause. Used when the deciding process is on the
    /// other side of a wire and holds the cluster record this pause belongs to.
    ByCaller,
}
