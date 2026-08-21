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
//! # 🔴 Why "reclaim everything" is safe, and what makes it safe
//!
//! Not the ability to tell leftovers apart — there is none, and this module
//! deliberately does not pretend otherwise. What makes it safe is **when it
//! runs**:
//!
//! > The sweep runs before the listener opens and before `FirecrackerPool` is
//! > primed, on the premise that **the previous process on this machine is
//! > gone**. At that instant nothing on this host belongs to this process:
//! > not a user sandbox, not a template build, not a warm pool VMM. So
//! > "reclaim everything of ours" and "reclaim precisely the leftovers" have
//! > the same result, and the second one is not implementable anyway.
//!
//! Two things already in the tree hold that premise up, and neither was put
//! there for this module:
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
//! restart *is* reclaimed, and that is correct rather than tolerated. §8.3's
//! argument is that at the moment of the sweep the build's VMM should not
//! exist — not that it deserves to be spared. `_sd-impl-phase3-role.md` §12 P2
//! makes exactly that assertion, that a mid-build restart drives
//! `agentenv_node_reclaim_reclaimed_total{resource_type="firecracker"}` above
//! zero.
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
mod work_dirs;

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
    /// tenant's Firecracker, a directory that is not one of ours.
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

        /// What a sandbox leaves behind once its VMM is gone: the directory.
        fn leftover(&self, name: &str) -> std::path::PathBuf {
            let work_dir = self.root.path().join("firecracker-work").join(name);
            std::fs::create_dir_all(&work_dir).unwrap();
            work_dir
        }

        /// A live sandbox exactly as one looks on the host: a Firecracker whose
        /// working directory is one of ours, and that directory.
        ///
        /// For `plan` only — see the note on [`Host`].
        fn live_sandbox(&self, name: &str, pid: i32) -> std::path::PathBuf {
            let work_dir = self.leftover(name);
            let process = self.root.path().join("proc").join(pid.to_string());
            std::fs::create_dir_all(&process).unwrap();
            std::fs::write(process.join("comm"), "firecracker\n").unwrap();
            std::fs::write(
                process.join("stat"),
                format!("{pid} (firecracker) S 1 {pid} {pid} 0 -1 4194304"),
            )
            .unwrap();
            std::os::unix::fs::symlink(&work_dir, process.join("cwd")).unwrap();
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
    /// sweep, said plainly.**
    ///
    /// Throughout the shadow phase the DaemonSet stays on `--role all`, and in
    /// that role every sandbox a user creates leaves `control_plane_config` as
    /// `None`. `None` is therefore not the exceptional case on a node — it is
    /// the majority case, and every one of those is a live user sandbox that
    /// the API half simply does not own.
    ///
    /// 🔴 This sweep cannot spare them, and this test says so rather than
    /// implying a protection that does not exist. A live sandbox and a leftover
    /// are the same two things on the host — a Firecracker under the work base,
    /// and a directory — and the marker is not on the host at all. Pointed at a
    /// running node, the sweep takes everything.
    ///
    /// What keeps it from being pointed at one is the two tests above and the
    /// one below: the premise is checked before anything is touched, and the
    /// role that runs during the shadow phase does not sweep at all.
    #[tokio::test]
    async fn a_host_laid_out_like_a_running_node_is_swept_wholesale() {
        let host = Host::new();
        for index in 0..3 {
            host.live_sandbox(&format!("agentenv-fc-live{index}"), 6000 + index);
        }

        // The process half, through `plan`, which decides without signalling.
        let plans = firecracker::plan(&host.paths());
        assert_eq!(plans.len(), 3);
        for plan in &plans {
            assert_eq!(
                plan.ownership,
                firecracker::Ownership::Ours,
                "a live user sandbox is indistinguishable from a leftover here: {plan:?}"
            );
        }

        // The file half, end to end, once the VMMs are gone — which on the
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
    #[test]
    fn no_deployment_manifest_turns_the_startup_sweep_on() {
        const VAR: &str = "AENV_STARTUP_RECLAIM_ENABLED";

        // 🔴 Resolution: the name below has to be the name the config actually
        // reads, or this scan looks for a string nothing would ever contain and
        // passes on every manifest including one that sets the real variable.
        assert!(
            include_str!("../cfg.rs").contains(&format!("env = \"{VAR}\"")),
            "{VAR} is no longer the environment variable this setting reads; \
             update this test with it"
        );

        let deploy = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("deploy");
        let mut checked = 0;
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
                    !contents.contains(VAR),
                    "{} sets {VAR}. On --role all that makes the rollback target sweep the \
                     host, which the pre-split process never did. If this is deliberate, it \
                     belongs on a --role node workload and this test needs to say so",
                    path.display()
                );
            }
        }
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
