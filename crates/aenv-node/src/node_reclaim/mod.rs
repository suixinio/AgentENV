//! Reclaims Firecracker processes and work directories left by a dead node.
//!
//! Run before environment setup, listener binding, or pool priming. Process
//! reclamation must precede work-directory and network cleanup.
//!
//! Reclamation is fail-closed: another server instance, a live ublk daemon,
//! a live stamped owner, or indeterminate ownership prevents deletion. Every
//! candidate is checked against its server-owner stamp; an absent or unreadable
//! stamp is never a deletion licence.
//!
//! `control_plane_config` ownership markers are intentionally ignored because
//! this machine-local sweep runs before metadata recovery. The paused-sandbox
//! store is excluded, and leaked ublk-device reclamation remains out of scope.

mod firecracker;
mod owner;
mod work_dirs;

pub use owner::stamp_work_dir;

use std::path::{Path, PathBuf};
use std::time::Duration;

use tracing::{info, warn};

use crate::cfg::AppConfig;

// Wait briefly for a signalled VMM before touching its work directory.
const EXIT_WAIT: Duration = Duration::from_secs(5);

/// Paths consulted or protected by startup reclamation.
#[derive(Debug, Clone)]
pub struct ReclaimPaths {
    /// Firecracker work-directory parent.
    pub work_base: PathBuf,
    /// `/proc`, or a stand-in in tests.
    pub proc_dir: PathBuf,
    /// Paused-sandbox storage that reclamation must never touch.
    pub persisted_sandbox_store: PathBuf,
    /// Current executable; `None` prevents reclamation.
    pub server_exe: Option<PathBuf>,
    /// ublk daemon socket; a live listener prevents reclamation.
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

/// Counts reclaimed, deliberately retained, and indeterminate candidates.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReclaimCounts {
    /// Retired leftovers owned by this node.
    pub reclaimed: u64,
    /// Candidates deliberately retained because they are not leftovers.
    pub left_alone: u64,
    /// Indeterminate candidates and failed reclamations.
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

/// Returns the configured startup-reclamation decision, defaulting to enabled.
pub fn enabled_for(configured: Option<bool>) -> bool {
    let enabled = configured.unwrap_or(true);

    // Publish enablement even when no sweep or traffic occurs.
    metrics::gauge!("agentenv_node_reclaim_enabled").set(if enabled { 1.0 } else { 0.0 });
    enabled
}

/// Best-effort startup reclamation.
///
/// Call before environment setup, listener binding, or Firecracker pool priming.
pub async fn run(config: &AppConfig) -> ReclaimReport {
    if !enabled_for(config.orchestrator.startup_reclaim_enabled) {
        info!(
            target: "agentenv",
            "startup reclaim is switched off; leaving host leftovers alone"
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

    // Record whether this process, rather than any prior process, swept.
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Refusal {
    AnotherServerInstance,
    OwnBinaryUnknown,
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

// Compare resolved executables; unreadable candidates are ignored fail-closed.
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
        // Linux appends " (deleted)" when a running executable is replaced.
        let (exe, _) = firecracker::strip_deleted_marker(&exe);
        let exe = std::fs::canonicalize(&exe).unwrap_or(exe);
        if exe == own_exe {
            return Some(pid);
        }
    }
    None
}

// A stale socket file does not count unless a server answers it.
fn ublk_daemon_answers(socket_path: &Path) -> bool {
    socket_path.exists() && std::os::unix::net::UnixStream::connect(socket_path).is_ok()
}

// Both executable identity and the shared ublk socket must indicate sole ownership.
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

async fn sweep(paths: &ReclaimPaths) -> ReclaimReport {
    // Refuse before examining candidates unless this process owns the host.
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

    // Kill VMMs before deleting their work directories.
    let firecracker = firecracker::reclaim(paths, EXIT_WAIT).await;

    // Any unclassified or unkillable VMM vetoes directory deletion.
    let work_dirs = work_dirs::reclaim(paths, firecracker.failed == 0).record("work_dir");

    ReclaimReport {
        firecracker: firecracker.record("firecracker"),
        work_dirs,
    }
}

// Lexical containment still works after a candidate has disappeared.
fn is_within(candidate: &Path, root: &Path) -> bool {
    candidate == root || candidate.starts_with(root)
}

#[cfg(test)]
mod tests {
    use super::*;

    use tracing::Level;

    use crate::logging::capture::Recorder;

    #[test]
    fn configuration_decides_in_both_directions_and_absence_means_sweep() {
        assert!(enabled_for(None));
        assert!(enabled_for(Some(true)));
        assert!(!enabled_for(Some(false)));
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
        assert!(!is_within(
            Path::new("/var/lib/aenv/persisted-sandboxes-old"),
            root
        ));
    }

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

        assert_eq!(sweep(&host.paths()).await, ReclaimReport::default());
    }
    // Uses forged `/proc`; fixtures never signal invented process ids.
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

        fn leftover(&self, name: &str) -> std::path::PathBuf {
            let work_dir = self.work_dir(name);
            owner::stamp_as_leftover(&work_dir);
            work_dir
        }

        fn work_dir(&self, name: &str) -> std::path::PathBuf {
            let work_dir = self.root.path().join("firecracker-work").join(name);
            std::fs::create_dir_all(&work_dir).unwrap();
            work_dir
        }

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

        // Plan-only fixture; never reclaim an invented process id.
        fn sandbox_of_a_dead_server(&self, name: &str, pid: i32) -> std::path::PathBuf {
            let work_dir = self.leftover(name);
            self.firecracker(pid, &work_dir);
            work_dir
        }

        // Safe for full sweep because the live-owner stamp prevents signalling.
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

        fn second_server(&self, pid: i32) {
            let process = self.root.path().join("proc").join(pid.to_string());
            std::fs::create_dir_all(&process).unwrap();
            std::os::unix::fs::symlink(self.root.path().join("server"), process.join("exe"))
                .unwrap();
        }
    }

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
        assert_eq!(another_server_instance(&proc_dir, &own_exe, 5003), None);
    }

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

    // Uses real `/proc`, but a fresh temporary work base prevents matching any
    // real process as a reclaimable Firecracker.
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
            // Nobody has said anything, which is what every deployment does.
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

        // A foreign boot id avoids collisions with real process ids.
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

        fn work_dir(&self, name: &str) -> PathBuf {
            let work_dir = self.work_base().join(name);
            std::fs::create_dir_all(&work_dir).unwrap();
            work_dir
        }
    }

    #[tokio::test]
    async fn run_turns_the_decision_into_a_sweep_unless_it_is_switched_off() {
        let mut host = ConfiguredHost::new();
        let leftover = host.leftover("agentenv-fc-A1");

        // Switched off. "Does not sweep" has to mean the host is untouched,
        // not merely that the report came back empty.
        host.config.orchestrator.startup_reclaim_enabled = Some(false);
        assert_eq!(run(&host.config).await, ReclaimReport::default());
        assert!(
            leftover.exists(),
            "a sweep that was switched off reclaimed the host anyway"
        );

        // The same host, the same leftover, with the setting back where every
        // deployment leaves it.
        host.config.orchestrator.startup_reclaim_enabled = None;
        let report = run(&host.config).await;
        assert_eq!(
            report.firecracker.failed, 0,
            "a Firecracker on this machine could not be classified, which vetoes the file \
             sweep; this suite reads the real /proc and needs one it can account for"
        );
        assert_eq!(
            report.work_dirs.reclaimed, 1,
            "aenv-node swept nothing; if the premise was refused, something else on this \
             machine is running this same test binary"
        );
        assert!(!leftover.exists());
    }

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
        let report = run(&host.config).await;
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

        let clean = ConfiguredHost::new();
        let settled = clean.leftover("agentenv-fc-B2");
        let accounted = Recorder::default();
        let guard = accounted.install();
        let report = run(&clean.config).await;
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

    /// Returns whether a manifest sets `var`, ignoring comment lines.
    fn manifest_sets(contents: &str, var: &str) -> bool {
        contents
            .lines()
            .any(|line| line.contains(var) && !line.trim_start().starts_with('#'))
    }

    #[test]
    fn no_deployment_manifest_turns_the_startup_sweep_on() {
        const VAR: &str = "AENV_STARTUP_RECLAIM_ENABLED";

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
        assert!(!manifest_sets(
            &format!("            # {VAR} is deliberately absent, here and everywhere"),
            VAR
        ));
        assert!(!manifest_sets(
            "            - name: AENV_STARTUP_RECLAIM",
            VAR
        ));

        // Anchor the scan to the environment variable the config reads.
        assert!(
            include_str!("../../../../src/cfg.rs").contains(&format!("env = \"{VAR}\"")),
            "{VAR} is no longer the environment variable this setting reads; \
             update this test with it"
        );

        // `CARGO_MANIFEST_DIR` is this member crate, not the repository root.
        let deploy = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../deploy")
            .canonicalize()
            .expect("the repository's deploy/ tree");
        let mut checked = 0;
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
                    "{} sets {VAR}, which makes every node in the fleet sweep the host at \
                     startup by deployment rather than by a deliberate local decision. If this \
                     is intended, this test needs to say so",
                    path.display()
                );
                if sample.is_none() && contents.contains('\n') {
                    sample = Some((path.clone(), contents));
                }
            }
        }

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

    #[test]
    fn nothing_in_this_module_consults_the_ownership_marker() {
        const MARKER: &str = "control_plane";

        // Prove the source scan has a positive anchor.
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
            // Ignore explanatory prose and inspect production code only.
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
