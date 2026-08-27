//! What a previous process on this machine left behind, and how a node gets
//! rid of it before it starts.
//!
//! A node that dies without unwinding leaves live Firecracker VMMs, their work
//! directories, and the network namespaces they were running in. Nothing in the
//! new process knows about any of it: the in-memory ledger died with the old
//! process, and the leftovers on the host carry no sandbox id — a namespace is
//! named `{NETNS_PREFIX}{uuid_v7}` (`sandbox::network::slot`) and a work
//! directory is `agentenv-fc-XXXXXX`. Neither says which sandbox it was, who
//! created it, or whether anyone is coming back for it.
//!
//! # 🔴 What makes the sweep safe: when it runs, *and* what it asks
//!
//! Two independent things, and the second one was missing.
//!
//! ## 1. When it runs
//!
//! > The sweep runs before the listener opens and before `FirecrackerPool` is
//! > primed, on the premise that **the previous process on this machine is
//! > gone**. At that instant nothing on this host belongs to this process:
//! > not a user sandbox, not a template build, not a warm pool VMM.
//!
//! ## 2. 🔴 What it asks about each candidate
//!
//! The premise above is held up by two *whole-host* probes ([`premise_holds`]):
//! is another copy of this executable running, and does the ublk daemon socket
//! still answer. Each is a heuristic, and each has a shape of host it is wrong
//! about — a binary replaced on disk under a running server, two servers with
//! different `AENV_HOME` and a shared `[firecracker].work_dir`. When one of
//! them is wrong, the cost is not a leak: it is this process killing another
//! server's running VMs and deleting the directories they are running in.
//!
//! So a whole-host premise is no longer the only thing between the sweep and a
//! live machine. Every sandbox work directory now carries a stamp naming the
//! server process that created it, and **every candidate is asked
//! individually whether its creator is still running** ([`owner`]). A
//! Firecracker whose server is up is left alone and said out loud; a directory
//! whose server is up is left alone; and anything the stamp cannot settle is
//! left alone and counted as a failure, never as a leftover.
//!
//! 🔴 What that does *not* claim, because the distinction is worth being exact
//! about: it does not make a still-running VMM of a **dead** server safe from
//! the sweep, and nothing could. A sandbox whose server process is gone is
//! already unreachable in this codebase — its route table, its handle and its
//! in-memory record died with the process, nothing adopts it, and
//! `sandbox::network::prepare_runtime` unlinks its namespace on the next
//! startup whether this module runs or not. "Leftover" and "live sandbox of a
//! process that no longer exists" are the same thing here. What the stamp
//! rules out is the case that is *not* the same thing: a sandbox belonging to a
//! server that is still up.
//!
//! Two things already in the tree hold the timing premise up, and neither was
//! put there for this module:
//!
//! 1. `deploy/k8s/base/agentenv-daemonset.yaml` pins `maxSurge: 0`, and says
//!    why in a comment that is this same premise, written for the paused
//!    -sandbox release path: "the previous process on this machine is gone —
//!    which is exactly what 'old pod fully terminated before the new one
//!    starts' guarantees". This is that premise's second consumer.
//! 2. `UblkDaemonClient::wait_for_socket_available` refuses to start while an
//!    old ublk daemon still answers its socket. "The old process is still
//!    here" is already a startup failure, not a state this process runs in.
//!
//! 🔴 **The consequence to be explicit about**: a template build caught by a
//! restart *is* reclaimed — its server is gone, so its stamp says so — and that
//! is correct rather than tolerated. §8.3's
//! argument is that at the moment of the sweep the build's VMM should not
//! exist — not that it deserves to be spared. `_sd-impl-phase3-role.md` §12 P2
//! makes exactly that assertion, that a mid-build restart drives
//! `agentenv_node_reclaim_reclaimed_total{resource_type="firecracker"}` above
//! zero.
//!
//! # 🔴 The ownership marker is a different question, and is not consulted
//!
//! Not to be confused with the owner stamp above, which is about *which server
//! process* made a directory on this machine. This one is about which control
//! plane owns a sandbox, and it has no business here.
//!
//! 🔴 **The ownership marker is not consulted here, and must not be wired in.**
//! `SandboxMetadata::control_plane_config` says whether the API half owns a
//! sandbox. It lives in a metadata store which, at the moment this runs, is
//! empty — the process that held it is the process that died. And nothing on
//! the host carries it: not the namespace name, not the work directory name,
//! not the process's argv. Consulting it is not merely unavailable, it would be
//! the wrong question: this sweep is about a machine, and that marker is about
//! a sandbox.
//!
//! 🔴 And the mistake it would be is not a marginal one. An absent marker means
//! *the control plane does not own this record* — never *leftover*, never *safe
//! to kill*. Through the shadow phase the DaemonSet stays on `--role all`, and
//! in that role every sandbox created through the node's own REST leaves the
//! marker absent. Absence is therefore the **majority** case on a node, and
//! every one of those is a live user sandbox. A reclaim that read absence as a
//! licence would not fail rarely at the margin; it would take most of a node's
//! sandboxes the first time it ran, in exactly the deployment shape the shadow
//! phase specifies. `nothing_in_this_module_consults_the_ownership_marker`
//! checks the source, because the regression is an addition that compiles,
//! passes everything else here, and reads like a safety improvement.
//!
//! # The premise, checked rather than assumed
//!
//! Everything above turns on "the previous process on this machine is gone",
//! and until it was checked that was a sentence in a comment. [`sweep`] now
//! refuses outright when another copy of this executable is running on the
//! host, and refuses when it cannot work out what its own executable is. The
//! DaemonSet's `maxSurge: 0` still carries the argument; this makes a broken
//! premise a refusal rather than a silent, expensive assumption — which matters
//! most on a development host, where two servers on one machine is ordinary.
//!
//! # What this is not
//!
//! It is not reconciliation, and the two must not grow into each other. The API
//! half's reconciliation compares what a node reports against what the control
//! plane believes and settles the difference; it can only ever see what the node
//! knows about. This sweep handles the complement — what the node itself does
//! not know is there — which is why it works from `/proc` and a directory
//! listing rather than from any ledger. Neither covers the other's case.
//!
//! # What is swept, and what is deliberately not
//!
//! | e2b `startupreclaim` | here | |
//! |---|---|---|
//! | `reclaimFirecrackers` | [`firecracker`] | scans `/proc`, kills what is ours |
//! | `storage.ReclaimSandboxFiles` | [`work_dirs`] | the `agentenv-fc-*` directories those VMMs were running in |
//! | `network.ReclaimLeakedSlots` | **already done** | `sandbox::network::prepare_runtime`, called from `setup::ensure_environment`, unlinks every stale `{NETNS_PREFIX}*` |
//! | `nbd.ReclaimLeaked` | **not done** | see below |
//! | `cgroup.ReclaimLeaked` | **not applicable** | this codebase creates no per-sandbox cgroup; the only cgroup write is the DaemonSet's `postStart` on the container's own |
//!
//! 🔴 **Ordering, and a bug it fixes.** e2b's `reclaim.go` says it in a comment
//! worth copying verbatim: "Order matters: firecracker runs first so the VMMs
//! are killed before the network reclaim tears down the slots they used."
//! Today `prepare_runtime` runs inside `ensure_environment` with *nothing*
//! killing VMMs before it, so a leftover Firecracker has its namespace unlinked
//! out from under it while it is still running. Calling [`run`] before
//! `ensure_environment` is what puts those two back in e2b's order.
//!
//! # 🔴 Which half of this earns its keep where
//!
//! On the Kubernetes DaemonSet, **the process sweep finds nothing, and that is
//! correct rather than broken.** The container's ENTRYPOINT is `/server`
//! (`deploy/docker/Dockerfile.agentenv`), the pod sets neither `hostPID` nor
//! `shareProcessNamespace`, so the server is PID 1 of its own PID namespace —
//! and when the init process of a PID namespace exits, the kernel `SIGKILL`s
//! everything left in it (`man 7 pid_namespaces`). A leftover Firecracker
//! cannot outlive the process that started it there. What *does* outlive it is
//! everything on the `hostPath` volume: the work directories, and the network
//! namespace files `prepare_runtime` already unlinks.
//!
//! So on that deployment the file sweep is the half that fires and the process
//! sweep reports three zeroes. 🔴 Read that reading correctly: it is "there
//! were no Firecrackers on this host", which the `left_alone` counter tells
//! apart from "there were, and they were someone else's", and which
//! `failed` tells apart from "there were, and I could not classify them".
//! `_sd-impl-phase3-role.md` §12 P2 expects a mid-build restart to push
//! `reclaimed_total{resource_type="firecracker"}` above zero; on a pod with its
//! own PID namespace it will not, and the work-directory counter is where that
//! probe has to look instead.
//!
//! The process sweep is load-bearing anywhere the server is *not* the init of
//! its PID namespace: a bare-metal or systemd install, `make start-server`, a
//! development host, or a container deliberately run with `hostPID: true`. It
//! is written for those, and it is the half whose mistakes are expensive, which
//! is why its refusals are where the tests are concentrated.
//!
//! 🔴 **Leaked ublk devices are out of scope, on purpose.** Deciding whether a
//! ublk device is abandoned means reading `ublksrv_pid` back through the
//! io_uring control ring, and the mistake direction is deleting a block device
//! out from under a running VM. It also cannot be exercised anywhere without
//! root and `/dev/ublk-control`, which makes it precisely the shape of code
//! that ships looking fine and is wrong the first time it runs. The half of the
//! premise it would enforce is already enforced, and enforced harder:
//! `wait_for_socket_available` turns "an old daemon is still here" into a
//! refusal to start.

mod firecracker;
mod owner;
mod work_dirs;

pub use owner::stamp_work_dir;

use std::path::{Path, PathBuf};
use std::time::Duration;

use tracing::{info, warn};

use crate::cfg::AppConfig;
use crate::role::ServerRole;

/// How long the sweep waits for a VMM it signalled to actually be gone before
/// it starts deleting the directory that VMM was running in.
///
/// A `SIGKILL`ed Firecracker is gone in milliseconds unless it is stuck in
/// uninterruptible I/O; this is generous enough to cover that and short enough
/// that a node whose leftovers cannot be killed still starts, degraded and
/// loud, rather than not at all.
const EXIT_WAIT: Duration = Duration::from_secs(5);

/// Where the sweep looks, and the one place it must never look.
#[derive(Debug, Clone)]
pub struct ReclaimPaths {
    /// The directory sandbox work directories are created in — Firecracker's
    /// `cwd` is one level below this. `[firecracker].work_dir`, or the system
    /// temp directory when that is unset, matching
    /// `sandbox::firecracker::config::create_firecracker_work_dir`.
    pub work_base: PathBuf,
    /// `/proc`, or a stand-in in tests.
    pub proc_dir: PathBuf,
    /// 🔴 `[orchestrator].persisted_sandbox_store_path`. Held here so the
    /// refusal to touch it is a named input rather than a property that
    /// happens to hold — those are the artifacts of every paused sandbox on
    /// this node, and `Orchestrator::new` reads them back a few hundred
    /// milliseconds after this sweep finishes. Deleting them turns a restart
    /// into permanent data loss for every sandbox parked here.
    pub persisted_sandbox_store: PathBuf,
    /// 🔴 This process's own executable, used to check that no other copy of
    /// it is running on this host — the premise the whole sweep rests on. See
    /// [`another_server_instance`].
    ///
    /// `None` when the executable could not be resolved, which is treated as
    /// "cannot tell" and refuses the sweep rather than assuming the host is
    /// ours alone.
    pub server_exe: Option<PathBuf>,
    /// 🔴 `[ublk].daemon_socket_path`. A socket that still *answers* is a
    /// server that still owns this machine's state, and it catches the case
    /// [`another_server_instance`] cannot: two servers built from different
    /// paths sharing one `AENV_HOME`, which is what a development host running
    /// `cargo run` beside an installed binary looks like.
    ///
    /// This is not a new signal. `UblkDaemonClient::wait_for_socket_available`
    /// already treats a socket that answers as "the old process is still here"
    /// and refuses to start over it; §8.3 cites that as one of the two things
    /// holding the sweep's premise up. All this does is ask the same question
    /// before killing anything rather than after.
    pub ublk_daemon_socket: PathBuf,
}

impl ReclaimPaths {
    pub fn from_config(config: &AppConfig) -> Self {
        Self {
            work_base: config
                .firecracker
                .work_dir
                .clone()
                .unwrap_or_else(std::env::temp_dir),
            proc_dir: PathBuf::from("/proc"),
            persisted_sandbox_store: config.orchestrator.persisted_sandbox_store_path.clone(),
            server_exe: std::env::current_exe().ok(),
            ublk_daemon_socket: config.ublk.daemon_socket_path.clone(),
        }
    }
}

/// What one pass over one kind of resource did.
///
/// 🔴 Three counts, not two, and the third is the one this exists for. e2b
/// keeps `reclaimed` and `failed`; a `reclaimed` of zero then reads the same
/// whether the sweep found nothing or found seven things and correctly left
/// every one of them alone. `left_alone` is the difference between "there was
/// nothing to do" and "the refusal is working", and the second is the claim
/// this module's safety rests on.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReclaimCounts {
    /// Leftovers of ours that were retired.
    pub reclaimed: u64,
    /// Candidates that were examined and deliberately not touched: another
    /// tenant's Firecracker, a directory that is not one of ours, or — the
    /// answer this counter exists to make visible — a Firecracker whose own
    /// server process is still running.
    pub left_alone: u64,
    /// 🔴 Candidates whose ownership could not be determined, plus reclaims
    /// that were attempted and did not work. Never reclaimed and never counted
    /// as `left_alone`: "I could not tell" and "it is not mine" produce the
    /// same action and must not produce the same reading, or a sweep that has
    /// been blind since some kernel upgrade reports the same clean zero as one
    /// that had nothing to do.
    pub failed: u64,
}

impl ReclaimCounts {
    fn record(self, resource_type: &'static str) -> Self {
        metrics::counter!(
            "agentenv_node_reclaim_reclaimed_total",
            "resource_type" => resource_type,
        )
        .increment(self.reclaimed);
        metrics::counter!(
            "agentenv_node_reclaim_left_alone_total",
            "resource_type" => resource_type,
        )
        .increment(self.left_alone);
        metrics::counter!(
            "agentenv_node_reclaim_failed_total",
            "resource_type" => resource_type,
        )
        .increment(self.failed);
        self
    }
}

/// What the whole sweep did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReclaimReport {
    pub firecracker: ReclaimCounts,
    pub work_dirs: ReclaimCounts,
}

/// Whether this process sweeps the host at startup.
///
/// 🔴 Three states, because two would be wrong. `Some(_)` is an operator who
/// said so; `None` is nobody having said anything, which is not the same as
/// having said "no" — it means the role decides, and the roles disagree. A
/// `node` is a machine dedicated to one server process and sweeps; an `all` is
/// what runs on a laptop next to a second copy of itself and does not.
pub fn enabled_for(role: ServerRole, configured: Option<bool>) -> bool {
    let enabled = configured.unwrap_or_else(|| role.reclaims_host_leftovers_at_startup());

    // 🔴 The one setting that can take the rollback away.
    //
    // `--role all` is defined as the pre-split process, and §11.3 needs it to
    // stay a working rollback target. It does not sweep, so on `all` this whole
    // module is one gauge and one log line — unless somebody sets
    // `AENV_STARTUP_RECLAIM_ENABLED=true`, at which point the rollback target
    // starts killing processes and deleting directories, which the thing being
    // rolled back to never did.
    //
    // Said loudly rather than refused: the premise checks in `sweep` already
    // catch the failure this would cause (another server on the host), and
    // taking a documented override away is its own kind of surprise. But an
    // operator who set this on a `--role all` node has almost certainly set it
    // on the wrong workload, and nothing else would tell them.
    if enabled && !role.reclaims_host_leftovers_at_startup() {
        warn!(
            target: "agentenv",
            role = role.as_str(),
            "startup reclaim has been turned on for a role that does not sweep by default. \
             --role all is the rollback target and is meant to behave exactly as the process \
             before the split did; sweeping the host is not something it ever did. This is \
             only safe while nothing else on this machine owns a sandbox — unset \
             AENV_STARTUP_RECLAIM_ENABLED unless that is deliberate"
        );
    }

    // A gauge, so "is this node sweeping" is answerable from a scrape with no
    // traffic and no restart to observe. A node that never sweeps and a node
    // whose sweep never found anything are otherwise the same three zeroes.
    metrics::gauge!("agentenv_node_reclaim_enabled").set(if enabled { 1.0 } else { 0.0 });
    enabled
}

/// Sweeps the host for what a previous process left, if this role sweeps.
///
/// 🔴 **Call this before `setup::ensure_environment`.** That function unlinks
/// every stale network namespace, and a namespace must not be unlinked while a
/// VMM is still running in it. Also before the listener opens and before
/// `FirecrackerPool::prime`, for the reason in this module's header: after
/// either of those the premise the sweep rests on stops being true and the same
/// call starts killing this process's own VMs.
///
/// Best-effort throughout, and never fatal — the e2b property, kept for the
/// same reason: a node that cannot clean up a leftover is a degraded node, and
/// a node that refuses to start is an absent one.
pub async fn run(role: ServerRole, config: &AppConfig) -> ReclaimReport {
    if !enabled_for(role, config.orchestrator.startup_reclaim_enabled) {
        info!(
            target: "agentenv",
            role = role.as_str(),
            "startup reclaim is off for this role; leaving host leftovers alone"
        );
        return ReclaimReport::default();
    }

    let paths = ReclaimPaths::from_config(config);
    info!(
        target: "agentenv",
        work_base = %paths.work_base.display(),
        "reclaiming what a previous process on this machine left behind"
    );

    let report = sweep(&paths).await;

    // A timestamp rather than a "ran" boolean: a boolean that is true forever
    // after the first sweep answers "did this node ever sweep", and the
    // question worth asking is "did *this* process sweep".
    metrics::gauge!("agentenv_node_reclaim_last_run_unix_seconds").set(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|since| since.as_secs_f64())
            .unwrap_or(0.0),
    );

    if report.firecracker.failed > 0 || report.work_dirs.failed > 0 {
        warn!(
            target: "agentenv",
            firecracker_failed = report.firecracker.failed,
            work_dirs_failed = report.work_dirs.failed,
            "startup reclaim could not account for everything it looked at; \
             this node may be holding resources nothing will free"
        );
    }
    info!(
        target: "agentenv",
        firecracker_reclaimed = report.firecracker.reclaimed,
        firecracker_left_alone = report.firecracker.left_alone,
        work_dirs_reclaimed = report.work_dirs.reclaimed,
        work_dirs_left_alone = report.work_dirs.left_alone,
        "startup reclaim complete"
    );

    report
}

/// Why a sweep was refused before it looked at anything.
///
/// A closed set: the label goes on a metric.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Refusal {
    /// Another copy of this executable is running on this host, so the
    /// previous process on this machine is *not* gone.
    AnotherServerInstance,
    /// This process could not work out what its own executable is, so it
    /// cannot check for the above.
    OwnBinaryUnknown,
    /// The ublk daemon socket still answers, so a server still owns this
    /// machine's state even though no process is running this same binary.
    AnotherServerOwnsThisHost,
}

impl Refusal {
    fn label(self) -> &'static str {
        match self {
            Self::AnotherServerInstance => "another_server_instance",
            Self::OwnBinaryUnknown => "own_binary_unknown",
            Self::AnotherServerOwnsThisHost => "another_server_owns_this_host",
        }
    }
}

/// The pid of another process running this same executable, if there is one.
///
/// 🔴 This is §8.3's premise turned into a check. Everything the sweep does is
/// safe *because* the previous process on this machine is gone; that is
/// asserted by the DaemonSet's `maxSurge: 0` and by the ublk client refusing to
/// start while an old daemon answers, and until now it was asserted nowhere
/// else. A second live server on the host makes it false, and the cost of it
/// being false is not a leak — it is this process killing the other one's
/// running VMs and deleting the directories they are running in.
///
/// The comparison is the resolved executable, not a name: `comm` is truncated
/// to 15 bytes and says nothing about which build or which install a process
/// came from, while `/proc/<pid>/exe` is the file itself.
///
/// 🔴 Three answers collapse to two here, in the safe direction. A candidate
/// whose `exe` cannot be read is *not* claimed as another instance — it is
/// another user's process, and this process could not be running as a user that
/// cannot read its own binary. Getting that wrong the other way would refuse
/// every sweep on any host that happens to run something unreadable, which is
/// every host.
fn another_server_instance(proc_dir: &Path, own_exe: &Path, own_pid: i32) -> Option<i32> {
    let own_exe = std::fs::canonicalize(own_exe).unwrap_or_else(|_| own_exe.to_path_buf());
    for entry in std::fs::read_dir(proc_dir).ok()?.flatten() {
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<i32>().ok())
        else {
            continue;
        };
        if pid <= 0 || pid == own_pid {
            continue;
        }
        let Ok(exe) = std::fs::read_link(entry.path().join("exe")) else {
            continue;
        };
        // 🔴 A running server whose binary was replaced on disk reads back as
        // `"<path> (deleted)"`, which matches nothing. That is not an exotic
        // case: it is what an in-place upgrade looks like, and what `cargo
        // build` over a running `make start-server` looks like — the two
        // situations where a second server on the host is most likely and this
        // check is most needed.
        let (exe, _) = firecracker::strip_deleted_marker(&exe);
        let exe = std::fs::canonicalize(&exe).unwrap_or(exe);
        if exe == own_exe {
            return Some(pid);
        }
    }
    None
}

/// Whether another server is already answering on this machine's ublk daemon
/// socket.
///
/// A socket file that nobody is listening on is a leftover, not a server, and
/// says nothing — which is the same reading `wait_for_socket_available` takes
/// before it deletes one.
fn ublk_daemon_answers(socket_path: &Path) -> bool {
    socket_path.exists() && std::os::unix::net::UnixStream::connect(socket_path).is_ok()
}

/// Whether the premise the sweep rests on holds right now.
///
/// 🔴 Two questions, because one of them has a hole. Comparing executables
/// misses two servers built from different paths that share one `AENV_HOME` —
/// `cargo run` next to an installed binary, which is the ordinary shape of a
/// development host and exactly where the work directories *do* collide. The
/// ublk daemon socket is the artefact that identifies the machine's state
/// rather than the binary, so it catches what the first question cannot.
fn premise_holds(paths: &ReclaimPaths) -> Result<(), Refusal> {
    let Some(own_exe) = paths.server_exe.as_deref() else {
        return Err(Refusal::OwnBinaryUnknown);
    };
    let own_pid = i32::try_from(std::process::id()).unwrap_or(-1);
    if another_server_instance(&paths.proc_dir, own_exe, own_pid).is_some() {
        return Err(Refusal::AnotherServerInstance);
    }
    if ublk_daemon_answers(&paths.ublk_daemon_socket) {
        return Err(Refusal::AnotherServerOwnsThisHost);
    }
    Ok(())
}

/// The sweep itself, without the enablement decision or the configuration
/// lookup, so tests can drive it against a directory tree.
async fn sweep(paths: &ReclaimPaths) -> ReclaimReport {
    // 🔴 Before anything is read, let alone killed. Everything below is blind
    // to ownership by design — a live user sandbox and a leftover look
    // identical on the host — so the only thing separating "reclaim the
    // leftovers" from "kill this machine's running VMs" is that no other
    // process on this machine has any.
    if let Err(refusal) = premise_holds(paths) {
        metrics::counter!(
            "agentenv_node_reclaim_refused_total",
            "reason" => refusal.label(),
        )
        .increment(1);
        warn!(
            target: "agentenv",
            reason = refusal.label(),
            "refusing to reclaim host leftovers: this sweep cannot tell a leftover from a live \
             sandbox and is only safe while nothing else on this machine owns any. Leftovers stay \
             where they are"
        );
        return ReclaimReport::default();
    }

    // 🔴 Processes before files, and the whole reason the two are separate
    // steps: deleting the directory a live VMM is running in leaves the VMM
    // running with its files gone, which is strictly worse than either doing
    // both or doing neither.
    let firecracker = firecracker::reclaim(paths, EXIT_WAIT).await;

    // 🔴 And the process sweep's result is a veto over the file sweep, not
    // context for it. `failed` is "there is a Firecracker on this host I could
    // not classify, or could not kill" — which is exactly the case where a
    // directory about to be deleted might still be in use.
    let work_dirs = work_dirs::reclaim(paths, firecracker.failed == 0).record("work_dir");

    ReclaimReport {
        firecracker: firecracker.record("firecracker"),
        work_dirs,
    }
}

/// Whether `candidate` is inside `root`, or is `root`.
///
/// Purely lexical, and that is deliberate: it is asked about paths that may
/// already have been deleted, where `canonicalize` fails and would turn "this
/// is the protected directory" into "I could not tell".
fn is_within(candidate: &Path, root: &Path) -> bool {
    candidate == root || candidate.starts_with(root)
}

#[cfg(test)]
mod tests {
    use super::*;

    use tracing::Level;

    use crate::logging::capture::Recorder;

    #[test]
    fn the_role_decides_only_when_configuration_has_not() {
        // 🔴 The three states, each distinct. The middle one is the one a
        // two-state reading would lose: an operator who has set nothing is not
        // an operator who has said no.
        assert!(enabled_for(ServerRole::Node, None));
        assert!(!enabled_for(ServerRole::All, None));
        assert!(!enabled_for(ServerRole::Api, None));

        assert!(enabled_for(ServerRole::All, Some(true)));
        assert!(!enabled_for(ServerRole::Node, Some(false)));
    }

    #[test]
    fn containment_is_lexical_and_covers_the_root_itself() {
        let root = Path::new("/var/lib/aenv/persisted-sandboxes");
        assert!(is_within(root, root));
        assert!(is_within(
            Path::new("/var/lib/aenv/persisted-sandboxes/artifacts/sbx"),
            root
        ));
        assert!(!is_within(
            Path::new("/var/lib/aenv/firecracker-work"),
            root
        ));
        // 🔴 Not a string prefix: a sibling whose name merely starts with the
        // protected one is a different directory.
        assert!(!is_within(
            Path::new("/var/lib/aenv/persisted-sandboxes-old"),
            root
        ));
    }

    /// A sweep of a host with nothing on it reports nothing, and does not
    /// invent a failure out of directories that do not exist.
    ///
    /// 🔴 In that order, and both halves in one test. "The report was empty" is
    /// evidence of nothing on its own — a sweep that returned
    /// `ReclaimReport::default()` unconditionally would pass a test that only
    /// looked at a clean host, and would look identical to a working one
    /// forever. So the host is swept once with something on it first, and the
    /// zeroes below mean "settled" rather than "never happened".
    #[tokio::test]
    async fn a_clean_host_reclaims_nothing_and_fails_at_nothing() {
        let host = Host::new();
        let work_dir = host.leftover("agentenv-fc-A1");

        assert_eq!(
            sweep(&host.paths()).await.work_dirs.reclaimed,
            1,
            "the probe has resolution before it reports an absence"
        );
        assert!(!work_dir.exists());

        // ...and now there is genuinely nothing left.
        assert_eq!(sweep(&host.paths()).await, ReclaimReport::default());
    }
    /// A host a test can lay out: a forged `/proc`, a work base, a paused
    /// -sandbox store, and a stand-in for this process's own executable.
    ///
    /// 🔴 Nothing here ever gives `reclaim` a Firecracker to signal. `reclaim`
    /// calls `kill(2)` with the pid it was handed, and a pid invented by a test
    /// is a pid that may belong to something real on the machine running it.
    /// Tests that need the process half assert against `plan`, which decides
    /// and does not signal; tests that need the sweep end to end use leftovers
    /// with no process at all — which is also what a leftover looks like on the
    /// DaemonSet, where the pod's PID namespace has already taken the VMMs.
    struct Host {
        root: tempfile::TempDir,
    }

    impl Host {
        fn new() -> Self {
            let root = tempfile::tempdir().unwrap();
            std::fs::create_dir_all(root.path().join("proc")).unwrap();
            std::fs::create_dir_all(root.path().join("firecracker-work")).unwrap();
            std::fs::create_dir_all(root.path().join("persisted")).unwrap();
            std::fs::write(root.path().join("server"), "#!/bin/false\n").unwrap();
            Self { root }
        }

        fn paths(&self) -> ReclaimPaths {
            ReclaimPaths {
                work_base: self.root.path().join("firecracker-work"),
                proc_dir: self.root.path().join("proc"),
                persisted_sandbox_store: self.root.path().join("persisted"),
                server_exe: Some(self.root.path().join("server")),
                // A path with nothing listening on it: no other server owns
                // this machine. `a_ublk_daemon_that_still_answers_refuses_the_sweep`
                // is where that is varied.
                ublk_daemon_socket: self.root.path().join("ublk.sock"),
            }
        }

        /// What a sandbox leaves behind once both its VMM and the server that
        /// started it are gone: the directory, stamped with a process that is
        /// not in this host's `/proc`.
        fn leftover(&self, name: &str) -> std::path::PathBuf {
            let work_dir = self.work_dir(name);
            owner::stamp_as_leftover(&work_dir);
            work_dir
        }

        /// A directory under the work base with no owner stamp at all: one
        /// created by a build from before the stamp existed.
        fn work_dir(&self, name: &str) -> std::path::PathBuf {
            let work_dir = self.root.path().join("firecracker-work").join(name);
            std::fs::create_dir_all(&work_dir).unwrap();
            work_dir
        }

        /// A Firecracker in this host's forged `/proc` whose working directory
        /// is `work_dir`.
        fn firecracker(&self, pid: i32, work_dir: &Path) {
            let process = self.root.path().join("proc").join(pid.to_string());
            std::fs::create_dir_all(&process).unwrap();
            std::fs::write(process.join("comm"), "firecracker\n").unwrap();
            std::fs::write(
                process.join("stat"),
                format!("{pid} (firecracker) S 1 {pid} {pid} 0 -1 4194304"),
            )
            .unwrap();
            std::os::unix::fs::symlink(work_dir, process.join("cwd")).unwrap();
        }

        /// A sandbox left by a server that has exited: a Firecracker, its work
        /// directory, and a stamp naming a process that is gone.
        ///
        /// For `plan` only — see the note on [`Host`].
        fn sandbox_of_a_dead_server(&self, name: &str, pid: i32) -> std::path::PathBuf {
            let work_dir = self.leftover(name);
            self.firecracker(pid, &work_dir);
            work_dir
        }

        /// 🔴 A sandbox of a server that is **still running**: the same two
        /// things on the host, plus a stamp naming a process this fixture also
        /// puts in `/proc`.
        ///
        /// Safe to drive the whole `sweep` against, unlike the case above,
        /// precisely because of what this test asserts: nothing here is ever
        /// signalled.
        fn sandbox_of_a_live_server(
            &self,
            name: &str,
            pid: i32,
            server_pid: i32,
        ) -> std::path::PathBuf {
            let work_dir = self.work_dir(name);
            owner::stamp_for_test(&work_dir, server_pid, 77_000, None);
            self.firecracker(pid, &work_dir);
            let process = self.root.path().join("proc").join(server_pid.to_string());
            std::fs::create_dir_all(&process).unwrap();
            std::fs::write(
                process.join("stat"),
                format!(
                    "{server_pid} (server) S 1 {server_pid} {server_pid} 0 -1 0 0 0 0 0 0 0 0 0 \
                     20 0 1 0 77000"
                ),
            )
            .unwrap();
            work_dir
        }

        /// A second copy of this process's own executable, running.
        fn second_server(&self, pid: i32) {
            let process = self.root.path().join("proc").join(pid.to_string());
            std::fs::create_dir_all(&process).unwrap();
            std::os::unix::fs::symlink(self.root.path().join("server"), process.join("exe"))
                .unwrap();
        }
    }

    /// 🔴 T-NR-30. **A second server on this host stops the sweep dead.**
    ///
    /// §8.3's argument is that everything here is safe *because* the previous
    /// process on this machine is gone. Until now that was asserted by a
    /// DaemonSet setting and a comment, and by nothing in the code. It is the
    /// one premise whose failure is not a leak but this process deleting
    /// another one's running sandboxes — and two servers sharing a development
    /// host is ordinary rather than exotic.
    ///
    /// Both faces, against the same host and the same leftover. Without the
    /// second half the refusal is indistinguishable from a sweep that never
    /// worked at all.
    #[tokio::test]
    async fn a_second_copy_of_this_server_on_the_host_refuses_the_whole_sweep() {
        let host = Host::new();
        let work_dir = host.leftover("agentenv-fc-A1");
        host.second_server(4002);

        assert_eq!(
            sweep(&host.paths()).await,
            ReclaimReport::default(),
            "nothing may be examined, let alone retired, while the premise is false"
        );
        assert!(work_dir.exists(), "and nothing may be deleted");

        // Push it up: the same host without the second server does retire it,
        // so the zeroes above are the refusal and not an inert sweep.
        std::fs::remove_dir_all(host.root.path().join("proc").join("4002")).unwrap();
        assert_eq!(sweep(&host.paths()).await.work_dirs.reclaimed, 1);
        assert!(!work_dir.exists());
    }

    /// 🔴 T-NR-31. Not being able to name our own executable is "cannot tell",
    /// and "cannot tell" does not sweep.
    #[tokio::test]
    async fn a_process_that_cannot_identify_its_own_binary_refuses_to_sweep() {
        let host = Host::new();
        let work_dir = host.leftover("agentenv-fc-A1");

        let blind = ReclaimPaths {
            server_exe: None,
            ..host.paths()
        };
        assert_eq!(sweep(&blind).await, ReclaimReport::default());
        assert!(work_dir.exists());

        // ...and knowing it, the same host is swept.
        assert_eq!(sweep(&host.paths()).await.work_dirs.reclaimed, 1);
        assert!(!work_dir.exists());
    }

    /// 🔴 T-NR-36. A ublk daemon that still answers is a server that still
    /// owns this machine.
    ///
    /// Closes the hole in the executable comparison: two servers built from
    /// different paths sharing one `AENV_HOME` are not the same binary, but
    /// they are the same machine's state, and their work directories are the
    /// same directories. That is what `cargo run` beside an installed binary
    /// looks like.
    ///
    /// Both faces, and the second one is the one that keeps this from being an
    /// unconditional refusal: a socket *file* with nothing listening is a
    /// leftover, says nothing, and does not stop the sweep — the same reading
    /// `wait_for_socket_available` takes before it deletes one.
    #[tokio::test]
    async fn a_ublk_daemon_that_still_answers_refuses_the_sweep() {
        let host = Host::new();
        let work_dir = host.leftover("agentenv-fc-A1");
        let socket = host.root.path().join("ublk.sock");
        let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();

        assert_eq!(sweep(&host.paths()).await, ReclaimReport::default());
        assert!(work_dir.exists(), "nothing may be deleted while it answers");

        // A socket file nobody is listening on is a leftover, not a server.
        drop(listener);
        assert!(socket.exists(), "the file outlives the listener");
        assert_eq!(sweep(&host.paths()).await.work_dirs.reclaimed, 1);
        assert!(!work_dir.exists());
    }

    /// T-NR-32. A process whose `exe` cannot be read is not claimed as a
    /// second instance.
    ///
    /// Reading it the other way — "unreadable, so it might be us" — would
    /// refuse every sweep on every host, because every host runs something this
    /// process may not look at.
    #[test]
    fn a_process_whose_executable_cannot_be_read_is_not_a_second_instance() {
        let host = Host::new();
        let proc_dir = host.root.path().join("proc");
        // No `exe` link at all, which is what an unreadable one looks like from
        // here.
        std::fs::create_dir_all(proc_dir.join("5001")).unwrap();
        // ...and one that points somewhere else entirely.
        std::fs::create_dir_all(proc_dir.join("5002")).unwrap();
        std::os::unix::fs::symlink("/bin/sh", proc_dir.join("5002").join("exe")).unwrap();

        let own_exe = host.root.path().join("server");
        assert_eq!(another_server_instance(&proc_dir, &own_exe, 1), None);

        // The control face: a link to the same binary is found.
        host.second_server(5003);
        assert_eq!(
            another_server_instance(&proc_dir, &own_exe, 1),
            Some(5003),
            "the probe has resolution"
        );
        // ...and this process is never its own second instance.
        assert_eq!(another_server_instance(&proc_dir, &own_exe, 5003), None);
    }

    /// 🔴 T-NR-33. **What a node full of live sandboxes looks like to this
    /// sweep — and what saves it.**
    ///
    /// This is the case the whole module turns on, and until the owner stamp
    /// landed the answer was "the sweep takes everything". The host below is
    /// laid out as a running node with three sandboxes, and the two whole-host
    /// premise checks both come back clear: there is no second copy of this
    /// executable in `/proc` (the running server is a different binary, or its
    /// own has been replaced on disk), and nothing answers the ublk socket.
    /// That is precisely the state in which the old sweep killed three live
    /// VMs and deleted the directories they were running in.
    ///
    /// 🔴 Both faces in one run, against hosts that differ in exactly one
    /// thing: whether the process the stamp names is in `/proc`. Without the
    /// second half the first would pass against a sweep that had been switched
    /// off entirely.
    #[tokio::test]
    async fn a_node_whose_server_is_still_running_is_left_entirely_alone() {
        const SERVER: i32 = 7100;

        let live = Host::new();
        let live_dirs: Vec<_> = (0..3)
            .map(|index| {
                live.sandbox_of_a_live_server(
                    &format!("agentenv-fc-live{index}"),
                    6000 + index,
                    SERVER,
                )
            })
            .collect();

        // The premise checks find nothing wrong: this is a sweep that believes
        // it has the machine to itself.
        assert!(premise_holds(&live.paths()).is_ok());

        // Safe to run end to end, because the point is that nothing is
        // signalled: every plan below is `Nothing`.
        for plan in firecracker::plan(&live.paths()) {
            assert_eq!(plan.ownership, firecracker::Ownership::LiveOwner(SERVER));
            assert_eq!(plan.action, firecracker::Action::Nothing, "{plan:?}");
        }
        let report = sweep(&live.paths()).await;
        assert_eq!(report.firecracker.reclaimed, 0, "a live node was swept");
        assert_eq!(report.firecracker.left_alone, 3);
        assert_eq!(report.work_dirs.reclaimed, 0);
        assert_eq!(report.work_dirs.left_alone, 3);
        for work_dir in &live_dirs {
            assert!(work_dir.exists(), "a live sandbox's files were deleted");
        }

        // 🔴 The other face: the same three sandboxes, whose server has exited.
        // The process half through `plan`, which decides without signalling —
        // see the note on `Host`.
        let dead = Host::new();
        for index in 0..3 {
            dead.sandbox_of_a_dead_server(&format!("agentenv-fc-live{index}"), 6000 + index);
        }
        let plans = firecracker::plan(&dead.paths());
        assert_eq!(plans.len(), 3);
        for plan in &plans {
            assert_eq!(
                plan.ownership,
                firecracker::Ownership::Ours,
                "a leftover of a server that is gone: {plan:?}"
            );
        }

        // And the file half end to end, once the VMMs are gone — which on the
        // DaemonSet the pod's own PID namespace has already done.
        let files_only = Host::new();
        let dirs: Vec<_> = (0..3)
            .map(|index| files_only.leftover(&format!("agentenv-fc-live{index}")))
            .collect();
        assert_eq!(sweep(&files_only.paths()).await.work_dirs.reclaimed, 3);
        for work_dir in &dirs {
            assert!(!work_dir.exists());
        }
    }

    /// 🔴 T-NR-38. A directory nothing can account for is left in place, and
    /// counted as a failure rather than as a decision.
    ///
    /// This is what every work directory on a host looks like immediately after
    /// this build is installed over one that did not write stamps. Leaking them
    /// costs disk until an operator clears them; reclaiming them on the old
    /// rule would, on a host where the premise checks were wrong, cost VMs.
    #[tokio::test]
    async fn a_work_directory_with_no_owner_stamp_is_never_reclaimed() {
        let host = Host::new();
        let unstamped = host.work_dir("agentenv-fc-fromanolderbuild");
        let leftover = host.leftover("agentenv-fc-A1");

        let report = sweep(&host.paths()).await;
        assert!(
            unstamped.exists(),
            "a directory nobody could account for was deleted"
        );
        assert_eq!(report.work_dirs.failed, 1);

        // Resolution, in the same run: the stamped leftover beside it *was*
        // reclaimed, so the line above is the refusal and not an inert sweep.
        assert_eq!(report.work_dirs.reclaimed, 1);
        assert!(!leftover.exists());
    }

    /// 🔴 T-NR-39. A second server whose binary was replaced on disk is still a
    /// second server.
    ///
    /// `/proc/<pid>/exe` for a running process whose file has been unlinked
    /// reads back as `"<path> (deleted)"`. Compared literally it matches
    /// nothing, so the premise check said "the machine is ours" in exactly the
    /// two situations where it most likely is not: an in-place upgrade, and a
    /// `cargo build` over a running `make start-server`.
    #[test]
    fn a_second_server_whose_binary_was_replaced_is_still_found() {
        let host = Host::new();
        let proc_dir = host.root.path().join("proc");
        let own_exe = host.root.path().join("server");

        let process = proc_dir.join("6100");
        std::fs::create_dir_all(&process).unwrap();
        std::os::unix::fs::symlink(
            format!("{} (deleted)", own_exe.display()),
            process.join("exe"),
        )
        .unwrap();

        assert_eq!(
            another_server_instance(&proc_dir, &own_exe, 1),
            Some(6100),
            "a running server whose binary was replaced went unnoticed"
        );

        // Resolution: a *different* binary that was also replaced is still not
        // us, so the match above is the path and not the suffix.
        let other = proc_dir.join("6101");
        std::fs::create_dir_all(&other).unwrap();
        std::os::unix::fs::symlink("/bin/sh (deleted)", other.join("exe")).unwrap();
        std::fs::remove_dir_all(&process).unwrap();
        assert_eq!(another_server_instance(&proc_dir, &own_exe, 1), None);
    }

    /// 🔴 T-NR-34. The role that runs during the shadow phase does not sweep.
    ///
    /// This is the protection for the case above, and it is one line of policy
    /// rather than any cleverness in the sweep. Pushed up in both directions,
    /// so the `false` is a decision and not a constant.
    #[test]
    fn the_role_that_runs_during_the_shadow_phase_does_not_sweep() {
        // The shadow phase keeps the DaemonSet on `--role all`, so the sweep
        // never runs there, whatever is on the node.
        assert!(!enabled_for(ServerRole::All, None));
        // ...and it is a decision: an operator can turn it on, which is what
        // makes the line above worth asserting.
        assert!(enabled_for(ServerRole::All, Some(true)));
        // The role that does sweep, whose startup is the one moment at which
        // the premise holds.
        assert!(enabled_for(ServerRole::Node, None));
    }

    /// 🔴 T-NR-53. An operator who turned the sweep on for a role that does not
    /// sweep is told so, and one who turned it on for a role that does is not.
    ///
    /// The warning is the whole of that decision — `enabled_for` returns the
    /// same `true` either way, and the gauge it sets is the same `1`. So the
    /// only thing that distinguishes "this is the ordinary configuration" from
    /// "somebody has taken §11.3's rollback target and pointed it at the host"
    /// is the line, and a test that does not read the line cannot tell a
    /// predicate over two operands from one that is always true.
    #[test]
    fn enabling_the_sweep_for_a_role_that_does_not_sweep_says_so() {
        const ANNOUNCEMENT: &str = "AENV_STARTUP_RECLAIM_ENABLED";

        // Enabled, by a role that would not have swept: the two operands
        // disagree, which is the case this line exists for.
        let overridden = Recorder::default();
        let guard = overridden.install();
        assert!(enabled_for(ServerRole::All, Some(true)));
        drop(guard);
        assert!(
            overridden.saw(Level::WARN, ANNOUNCEMENT),
            "the rollback target was pointed at the host and nothing said so: {:?}",
            overridden.events()
        );

        // 🔴 The same `enabled`, a role that sweeps by default. Nothing unusual
        // has happened and there is nothing to say — and this is the half a
        // predicate reading `||` where it says `&&` gets wrong.
        let ordinary = Recorder::default();
        let guard = ordinary.install();
        assert!(enabled_for(ServerRole::Node, Some(true)));
        drop(guard);
        assert!(
            !ordinary.saw(Level::WARN, ANNOUNCEMENT),
            "an ordinary node was warned about its own default: {:?}",
            ordinary.events()
        );

        // ...and the other operand alone: the role that does not sweep, left
        // alone. Not enabled, so again nothing to say.
        let untouched = Recorder::default();
        let guard = untouched.install();
        assert!(!enabled_for(ServerRole::All, None));
        drop(guard);
        assert!(
            !untouched.saw(Level::WARN, ANNOUNCEMENT),
            "a role that is simply not sweeping was warned about an override nobody set: {:?}",
            untouched.events()
        );
    }

    /// 🔴 T-NR-54. Each refusal is recorded under the reason it actually is.
    ///
    /// The label goes on `agentenv_node_reclaim_refused_total`, and the three
    /// reasons are three different operator actions: another server running
    /// this same binary, a process that cannot name its own executable, and a
    /// ublk daemon that still answers. They are one alert and one dashboard,
    /// and collapsed into a single value — or into the empty string — that
    /// alert says a node refused to sweep and cannot say why.
    ///
    /// Four sweeps, and the fourth is the non-empty half: with the premise
    /// established there is no refusal to label, and the sweep does real work.
    #[tokio::test]
    async fn each_refusal_is_recorded_under_the_reason_it_actually_is() {
        let host = Host::new();
        let work_dir = host.leftover("agentenv-fc-A1");

        host.second_server(4002);
        let (report, reasons) = refusal_reasons(&host.paths()).await;
        assert_eq!(report, ReclaimReport::default());
        assert_eq!(reasons, ["another_server_instance"]);

        let blind = ReclaimPaths {
            server_exe: None,
            ..host.paths()
        };
        let (_, reasons) = refusal_reasons(&blind).await;
        assert_eq!(reasons, ["own_binary_unknown"]);

        std::fs::remove_dir_all(host.root.path().join("proc").join("4002")).unwrap();
        let socket = host.root.path().join("ublk.sock");
        let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
        let (_, reasons) = refusal_reasons(&host.paths()).await;
        assert_eq!(reasons, ["another_server_owns_this_host"]);

        // Nothing left to refuse the sweep, so nothing is labelled and the
        // leftover goes.
        drop(listener);
        let (report, reasons) = refusal_reasons(&host.paths()).await;
        assert!(
            reasons.is_empty(),
            "a sweep that was not refused recorded a refusal: {reasons:?}"
        );
        assert_eq!(report.work_dirs.reclaimed, 1);
        assert!(!work_dir.exists());
    }

    /// The `reason` labels a sweep put on `agentenv_node_reclaim_refused_total`.
    async fn refusal_reasons(paths: &ReclaimPaths) -> (ReclaimReport, Vec<String>) {
        use metrics_util::debugging::DebuggingRecorder;

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let guard = metrics::set_default_local_recorder(&recorder);
        let report = sweep(paths).await;
        drop(guard);

        let mut reasons: Vec<String> = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter(|(composite, _unit, _description, _value)| {
                composite.key().name() == "agentenv_node_reclaim_refused_total"
            })
            .map(|(composite, _unit, _description, _value)| {
                composite
                    .key()
                    .labels()
                    .find(|label| label.key() == "reason")
                    .map_or_else(
                        || "<no reason label>".to_owned(),
                        |label| label.value().to_owned(),
                    )
            })
            .collect();
        reasons.sort();
        (report, reasons)
    }

    /// A host laid out for the two suites that drive [`run`], which takes an
    /// `AppConfig` rather than a [`ReclaimPaths`].
    ///
    /// 🔴 `ReclaimPaths::from_config` hardcodes `/proc`, so unlike every other
    /// suite in this file these two read the machine they are running on. That
    /// is bounded, and it is the only way to cover `run` at all. `plan`
    /// considers a process only if its `comm` is `firecracker` **and** its
    /// working directory is directly under the work base, and the work base
    /// here is a temporary directory that did not exist when any process on
    /// this machine started. Nothing real can be classified as ours and nothing
    /// real is signalled.
    ///
    /// What the machine can still do is make the sweep *refuse* — a second copy
    /// of this test binary running at the same instant is a second server
    /// instance, and declining is the correct answer. The assertions below say
    /// what they expected rather than reading a refusal as a pass.
    struct ConfiguredHost {
        _root: tempfile::TempDir,
        config: AppConfig,
    }

    impl ConfiguredHost {
        fn new() -> Self {
            let root = tempfile::tempdir().unwrap();
            let work_base = root.path().join("firecracker-work");
            std::fs::create_dir_all(&work_base).unwrap();
            let persisted = root.path().join("persisted-sandboxes");
            std::fs::create_dir_all(&persisted).unwrap();

            let mut config = AppConfig::default();
            config.firecracker.work_dir = Some(work_base);
            config.orchestrator.persisted_sandbox_store_path = persisted;
            // A path with nothing listening on it, which is a leftover socket
            // rather than a server that still owns this host.
            config.ublk.daemon_socket_path = root.path().join("ublk.sock");
            // Nobody has said anything, so the role decides.
            config.orchestrator.startup_reclaim_enabled = None;

            Self {
                _root: root,
                config,
            }
        }

        fn work_base(&self) -> PathBuf {
            self.config
                .firecracker
                .work_dir
                .clone()
                .expect("this fixture sets the work base")
        }

        /// A sandbox work directory stamped as belonging to a server that has
        /// exited.
        ///
        /// 🔴 Stamped from a *different boot* rather than with a pid nothing is
        /// using, which is what every other fixture here does. Those run against
        /// a forged `/proc` where the fixture decides which pids exist; this one
        /// runs against the real one, where `999001` is an ordinary pid — this
        /// machine's `pid_max` is over four million — and a real process wearing
        /// it while the sweep looks makes the stamp read as
        /// [`owner::Owner::Unknown`] rather than `Gone`. That was a flake at
        /// roughly one run in twenty-five. A boot id that is not this boot's is
        /// settled before any pid is consulted at all.
        fn leftover(&self, name: &str) -> PathBuf {
            let work_dir = self.work_dir(name);
            owner::stamp_for_test(
                &work_dir,
                owner::DEAD_OWNER_PID,
                1,
                Some("a-boot-this-machine-has-not-had"),
            );
            work_dir
        }

        /// A directory under the work base with no owner stamp at all, which is
        /// the shape of "nothing can say whose this is".
        fn work_dir(&self, name: &str) -> PathBuf {
            let work_dir = self.work_base().join(name);
            std::fs::create_dir_all(&work_dir).unwrap();
            work_dir
        }
    }

    /// 🔴 T-NR-55. `run` is where the decision becomes a sweep, and it is the
    /// only call `main` makes.
    ///
    /// Everything else in this file drives `sweep`, one level below the
    /// enablement check — so a `run` that returned an empty report and did
    /// nothing, for every role, on every host, would leave all of it green.
    /// Both directions are asserted against the same host: the role that does
    /// not sweep leaves the leftover where it is, and the role that does
    /// retires it.
    #[tokio::test]
    async fn run_turns_the_decision_into_a_sweep_for_the_role_that_sweeps() {
        let host = ConfiguredHost::new();
        let leftover = host.leftover("agentenv-fc-A1");

        // §11.3's rollback target. "Does not sweep" has to mean the host is
        // untouched, not merely that the report came back empty.
        assert_eq!(
            run(ServerRole::All, &host.config).await,
            ReclaimReport::default()
        );
        assert!(
            leftover.exists(),
            "--role all reclaimed a host it is defined not to touch"
        );

        // The same host, the same configuration, the role that does sweep.
        let report = run(ServerRole::Node, &host.config).await;
        assert_eq!(
            report.firecracker.failed, 0,
            "a Firecracker on this machine could not be classified, which vetoes the file \
             sweep; this suite reads the real /proc and needs one it can account for"
        );
        assert_eq!(
            report.work_dirs.reclaimed, 1,
            "--role node swept nothing; if the premise was refused, something else on this \
             machine is running this same test binary"
        );
        assert!(!leftover.exists());
    }

    /// 🔴 T-NR-56. A sweep that could not account for everything it looked at
    /// says so, and one that accounted for everything does not.
    ///
    /// The counters are already in the report; this line is what an operator
    /// sees without one. A node holding a directory nothing will ever free
    /// looks exactly like a healthy node in every other respect, and the two
    /// halves below are the two operands of the predicate that decides between
    /// them.
    #[tokio::test]
    async fn a_sweep_that_could_not_account_for_everything_says_so() {
        const ANNOUNCEMENT: &str = "could not account for everything it looked at";

        let host = ConfiguredHost::new();
        let leftover = host.leftover("agentenv-fc-A1");
        // Nothing can say whose this one is, which is a failure rather than a
        // decision — and the only half of the report a test can move without a
        // forged `/proc`.
        let nameless = host.work_dir("agentenv-fc-Nameless");

        let unaccounted = Recorder::default();
        let guard = unaccounted.install();
        let report = run(ServerRole::Node, &host.config).await;
        drop(guard);

        assert_eq!(
            report.firecracker.failed, 0,
            "see T-NR-55 on the real /proc"
        );
        assert_eq!(report.work_dirs.failed, 1, "the unstamped directory");
        assert_eq!(report.work_dirs.reclaimed, 1, "and the leftover beside it");
        assert!(!leftover.exists());
        assert!(nameless.exists());
        assert!(
            unaccounted.saw(Level::WARN, ANNOUNCEMENT),
            "a node holding a directory nothing will free said nothing: {:?}",
            unaccounted.events()
        );

        // 🔴 The same sweep over a host with nothing wrong with it. Both
        // counters are zero, and a predicate that is always true — or that
        // reads either counter the wrong way round — warns here anyway.
        let clean = ConfiguredHost::new();
        let settled = clean.leftover("agentenv-fc-B2");
        let accounted = Recorder::default();
        let guard = accounted.install();
        let report = run(ServerRole::Node, &clean.config).await;
        drop(guard);

        assert_eq!(
            report.firecracker.failed, 0,
            "see T-NR-55 on the real /proc"
        );
        assert_eq!(
            report.work_dirs,
            ReclaimCounts {
                reclaimed: 1,
                left_alone: 0,
                failed: 0,
            },
            "the probe has resolution: this sweep did retire something"
        );
        assert!(!settled.exists());
        assert!(
            !accounted.saw(Level::WARN, ANNOUNCEMENT),
            "a sweep that accounted for everything reported that it had not: {:?}",
            accounted.events()
        );
    }

    /// Whether a manifest *sets* `var`, as against mentioning it.
    ///
    /// 🔴 Comment lines are not a loophole. A manifest that names this variable
    /// in order to say it is deliberately absent is doing the thing the scan
    /// wants; a predicate that could not tell the two apart would push that
    /// explanation out of the file — and the explanation is what stops somebody
    /// copying the line from a workload where it is right.
    ///
    /// What is still caught is the variable on every line a deployment tool
    /// reads. There are three such forms and the scan below checks all three: a
    /// `- name:` entry in a container's `env:`, a `KEY: value` under a
    /// ConfigMap's `data:`, and a `KEY=value` kustomize literal.
    fn manifest_sets(contents: &str, var: &str) -> bool {
        contents
            .lines()
            .any(|line| line.contains(var) && !line.trim_start().starts_with('#'))
    }

    /// 🔴 T-NR-37. No deployment manifest turns the sweep on.
    ///
    /// `AENV_STARTUP_RECLAIM_ENABLED=true` on a `--role all` node makes the
    /// rollback target sweep the host, which the process before the split never
    /// did — the sharpest available way to lose §11.3's rollback. It is a
    /// deliberate operator override and stays one; what it must never be is
    /// something that arrives in a manifest and is noticed later.
    ///
    /// The startup warning in [`enabled_for`] covers the operator who types it.
    /// This covers the one who commits it, which nothing at runtime can.
    ///
    /// 🔴 The scan's whole result is an absence, so the proof that it *would*
    /// find a setting lives in this same test rather than in a sibling one. A
    /// separate control can be filtered out of a run, deleted on its own, or
    /// simply not noticed, and what is left then passes identically against a
    /// scanner that reads nothing at all — the shape this programme has now
    /// paid for five or six times.
    #[test]
    fn no_deployment_manifest_turns_the_startup_sweep_on() {
        const VAR: &str = "AENV_STARTUP_RECLAIM_ENABLED";

        // 🔴 The non-empty half, ahead of the scan rather than beside it.
        // These are the three forms a manifest in this tree can express the
        // setting in; the predicate has to catch all three before the absence
        // the scan reports means anything.
        for (form, shape) in [
            (
                format!("            - name: {VAR}\n              value: \"true\""),
                "a container env: entry",
            ),
            (format!("  {VAR}: \"true\""), "a ConfigMap data: key"),
            (format!("      - {VAR}=true"), "a kustomize literal"),
        ] {
            assert!(
                manifest_sets(&form, VAR),
                "the scan cannot see {VAR} written as {shape}, so the sweep could be turned on \
                 in that form and this test would still pass"
            );
        }
        // And the direction the narrowing exists for, plus a line that merely
        // resembles one: neither is a setting.
        assert!(!manifest_sets(
            &format!("            # {VAR} is deliberately absent, here and everywhere"),
            VAR
        ));
        assert!(!manifest_sets("            - name: AENV_ROLE", VAR));

        // 🔴 Resolution: the name below has to be the name the config actually
        // reads, or this scan looks for a string nothing would ever contain and
        // passes on every manifest including one that sets the real variable.
        assert!(
            include_str!("../../../../src/cfg.rs").contains(&format!("env = \"{VAR}\"")),
            "{VAR} is no longer the environment variable this setting reads; \
             update this test with it"
        );

        // 🔴 The repository's `deploy/`, not this crate's: `aenv-node` is a
        // member crate now and `CARGO_MANIFEST_DIR` points at
        // `crates/aenv-node`, which has no manifests under it at all — a walk
        // that read nothing would have passed every assertion below.
        let deploy = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../deploy")
            .canonicalize()
            .expect("the repository's deploy/ tree");
        let mut checked = 0;
        // One real file from the walk, kept so the predicate can be shown to
        // have teeth against an actual manifest and not only against the
        // fragments above — a whole file has comments, blank lines, block
        // scalars and indentation that a three-line literal does not.
        let mut sample: Option<(std::path::PathBuf, String)> = None;
        let mut stack = vec![deploy.clone()];
        while let Some(dir) = stack.pop() {
            let Ok(entries) = std::fs::read_dir(&dir) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                    continue;
                }
                let Ok(contents) = std::fs::read_to_string(&path) else {
                    continue;
                };
                checked += 1;
                assert!(
                    !manifest_sets(&contents, VAR),
                    "{} sets {VAR}. On --role all that makes the rollback target sweep the \
                     host, which the pre-split process never did. If this is deliberate, it \
                     belongs on a --role node workload and this test needs to say so",
                    path.display()
                );
                if sample.is_none() && contents.contains('\n') {
                    sample = Some((path.clone(), contents));
                }
            }
        }

        // 🔴 The same assertion the walk just made, on the same file, with one
        // setting line added — so "no manifest sets it" is a fact about the
        // tree rather than about the scan. Whichever file this is, it passed
        // above and must fail here.
        let (sampled_path, sampled) =
            sample.expect("the walk read no file with more than one line");
        assert!(
            manifest_sets(&format!("{sampled}\n  {VAR}: \"true\"\n"), VAR),
            "adding a real setting to {} did not make the scan notice it",
            sampled_path.display()
        );
        assert!(
            checked > 10,
            "only {checked} files under {} were read; a scan that reads nothing passes \
             everything",
            deploy.display()
        );
    }

    /// 🔴 T-NR-35. The reclaim never reads the ownership marker.
    ///
    /// The contract is that an absent `control_plane_config` means "the control
    /// plane does not own this record" and never "leftover, safe to kill".
    /// Nothing here consults it — it is not on the host, and the store is empty
    /// at this point — but it is a plausible thing for someone arriving later
    /// to reach for, and during the shadow phase it would retire most of a
    /// node's live sandboxes on the first run.
    ///
    /// Checked against the source rather than trusted to review, because the
    /// regression is an addition that compiles, passes every other test here,
    /// and reads like a safety improvement.
    #[test]
    fn nothing_in_this_module_consults_the_ownership_marker() {
        const MARKER: &str = "control_plane";

        // 🔴 Resolution, once: the module header names the marker deliberately
        // while explaining why it is never read. If it did not match here, the
        // scan below would be searching for a string that appears nowhere —
        // passing on every file, forever, including one that had just started
        // reading it.
        assert!(
            include_str!("mod.rs").contains(MARKER),
            "the module header should still explain why the marker is not consulted"
        );

        for (name, source) in [
            ("mod.rs", include_str!("mod.rs")),
            ("firecracker.rs", include_str!("firecracker.rs")),
            ("owner.rs", include_str!("owner.rs")),
            ("work_dirs.rs", include_str!("work_dirs.rs")),
        ] {
            // Production code only. The prose above this line names the marker
            // on purpose, and so does this test.
            let production = source.split("#[cfg(test)]").next().unwrap();
            for (number, line) in production.lines().enumerate() {
                let is_prose = line.trim_start().starts_with("//");
                assert!(
                    is_prose || !line.contains(MARKER),
                    "{name}:{} reads the ownership marker. An absent marker is not evidence \
                     about leftovers: it means the control plane does not own the record, and \
                     during the shadow phase that is true of most of the live sandboxes on a \
                     node. This sweep is sound because of when it runs, not because of what it \
                     knows.",
                    number + 1
                );
            }
        }
    }
}
