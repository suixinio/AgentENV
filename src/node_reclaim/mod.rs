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

/// The sweep itself, without the enablement decision or the configuration
/// lookup, so tests can drive it against a directory tree.
async fn sweep(paths: &ReclaimPaths) -> ReclaimReport {
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
    #[tokio::test]
    async fn a_clean_host_reclaims_nothing_and_fails_at_nothing() {
        let temp = tempfile::tempdir().unwrap();
        let paths = ReclaimPaths {
            work_base: temp.path().join("firecracker-work"),
            proc_dir: temp.path().join("proc"),
            persisted_sandbox_store: temp.path().join("persisted"),
        };

        assert_eq!(sweep(&paths).await, ReclaimReport::default());
    }
}
