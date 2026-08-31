//! Plans Firecracker reclamation from `/proc` before applying any signals.
//! Ownership and signal blast radius are decided in the pure planning half.

use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use nix::sys::signal::{kill, Signal};
use nix::unistd::{getpgrp, Pid};
use tracing::{debug, info, warn};

use super::{owner, ReclaimCounts, ReclaimPaths};

// Candidate filter only; ownership is established from work directory and stamp.
const FIRECRACKER_COMM: &str = "firecracker";

/// Prefix shared with sandbox work-directory creation.
pub const WORK_DIR_PREFIX: &str = "agentenv-fc-";

/// Ownership classification for a candidate Firecracker process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Ownership {
    /// Firecracker under this work base whose stamped server is gone.
    Ours,
    /// Firecracker outside this deployment's work base.
    Foreign,
    /// Firecracker whose stamped server is still running.
    LiveOwner(i32),
    /// Process disappeared while being inspected.
    Vanished,
    /// Ownership could not be determined; never safe to reclaim.
    Undetermined(&'static str),
}

/// Signal action for a candidate already classified as owned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    /// Signal a Firecracker-led process group.
    KillGroup(i32),
    /// Signal only the candidate process.
    KillProcess(i32),
    Nothing,
}

/// Planned ownership and action for one process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessPlan {
    pub pid: i32,
    pub ownership: Ownership,
    pub action: Action,
}

// Never negate these values: `kill(-0, …)` targets this process group and
// `kill(-1, …)` targets every permitted process.
const NEVER_A_GROUP_TARGET: [i32; 2] = [0, 1];

/// Purely plans reclamation without sending signals.
pub fn plan(paths: &ReclaimPaths) -> Vec<ProcessPlan> {
    let work_base = resolve(&paths.work_base);
    let own_pid = i32::try_from(std::process::id()).unwrap_or(-1);
    let own_pgid = getpgrp().as_raw();

    let entries = match std::fs::read_dir(&paths.proc_dir) {
        Ok(entries) => entries,
        Err(error) => {
            // A failed `/proc` listing means the sweep examined nothing.
            warn!(
                target: "agentenv",
                proc_dir = %paths.proc_dir.display(),
                %error,
                "cannot read /proc; no Firecracker leftovers will be reclaimed"
            );
            return Vec::new();
        }
    };

    let mut plans = Vec::new();
    for entry in entries.flatten() {
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<i32>().ok())
        else {
            continue;
        };
        if pid <= 0 {
            continue;
        }
        // Never classify this process as a reclaim candidate.
        if pid == own_pid {
            continue;
        }

        let process_dir = paths.proc_dir.join(pid.to_string());
        if !is_candidate(&process_dir) {
            continue;
        }

        plans.push(plan_for_candidate(
            pid,
            &process_dir,
            &paths.proc_dir,
            &work_base,
            own_pgid,
        ));
    }

    plans.sort_by_key(|plan| plan.pid);
    plans
}

// Candidate selection is intentionally silent for unrelated host processes.
fn is_candidate(process_dir: &Path) -> bool {
    std::fs::read_to_string(process_dir.join("comm"))
        .map(|comm| comm.trim() == FIRECRACKER_COMM)
        .unwrap_or(false)
}

fn plan_for_candidate(
    pid: i32,
    process_dir: &Path,
    proc_dir: &Path,
    work_base: &Path,
    own_pgid: i32,
) -> ProcessPlan {
    let ownership = match std::fs::read_link(process_dir.join("cwd")) {
        Ok(cwd) => {
            let (stripped, work_dir_unlinked) = strip_deleted_marker(&cwd);
            let cwd = resolve(&stripped);
            if !is_sandbox_work_dir(&cwd, work_base) {
                Ownership::Foreign
            } else if work_dir_unlinked {
                // An unlinked work directory cannot provide a stamp or serve a live VMM.
                Ownership::Ours
            } else {
                match owner::owner_of(&cwd, proc_dir) {
                    owner::Owner::Gone => Ownership::Ours,
                    owner::Owner::Alive(owner_pid) => Ownership::LiveOwner(owner_pid),
                    owner::Owner::Unknown(reason) => Ownership::Undetermined(reason),
                }
            }
        }
        // Disappearance is the intended outcome, not an ownership failure.
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ownership::Vanished,
        Err(error) => {
            warn!(
                target: "agentenv",
                pid,
                %error,
                "cannot read a Firecracker's working directory; leaving it alone"
            );
            Ownership::Undetermined("working directory unreadable")
        }
    };

    let action = match ownership {
        Ownership::Ours => plan_for_owned(pid, process_dir, own_pgid),
        _ => Action::Nothing,
    };

    ProcessPlan {
        pid,
        ownership,
        action,
    }
}

// Fall back to signalling only the process whenever group blast radius is unsafe.
fn plan_for_owned(pid: i32, process_dir: &Path, own_pgid: i32) -> Action {
    let Some(pgid) = read_pgid(process_dir) else {
        // Ownership is known, but group ownership is not.
        return Action::KillProcess(pid);
    };
    if NEVER_A_GROUP_TARGET.contains(&pgid) {
        return Action::KillProcess(pid);
    }
    if pgid == own_pgid {
        // Never signal this server's own process group.
        warn!(
            target: "agentenv",
            pid,
            pgid,
            "a leftover Firecracker shares this process's group; signalling it alone"
        );
        return Action::KillProcess(pid);
    }
    if pgid != pid {
        // A non-leader does not own its process group.
        return Action::KillProcess(pid);
    }
    Action::KillGroup(pgid)
}

// Require both the configured parent and sandbox work-directory prefix.
fn is_sandbox_work_dir(cwd: &Path, work_base: &Path) -> bool {
    cwd.parent() == Some(work_base)
        && cwd
            .file_name()
            .and_then(OsStr::to_str)
            .is_some_and(|name| name.starts_with(WORK_DIR_PREFIX))
}

// Parse pgrp after the final `)` because `/proc/<pid>/stat` comm may contain spaces.
fn read_pgid(process_dir: &Path) -> Option<i32> {
    let stat = std::fs::read_to_string(process_dir.join("stat")).ok()?;
    let after_comm = &stat[stat.rfind(')')? + 1..];
    // Fields after comm: state, ppid, pgrp.
    after_comm.split_whitespace().nth(2)?.parse().ok()
}

/// Strips Linux's ` (deleted)` suffix and reports whether it was present.
pub fn strip_deleted_marker(path: &Path) -> (PathBuf, bool) {
    match path
        .to_str()
        .and_then(|path| path.strip_suffix(" (deleted)"))
    {
        Some(stripped) => (PathBuf::from(stripped), true),
        None => (path.to_path_buf(), false),
    }
}

// Canonicalize when possible; vanished paths must retain their lexical identity.
fn resolve(path: &Path) -> PathBuf {
    std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}

/// Applies a plan and waits for signalled processes to exit.
pub async fn reclaim(paths: &ReclaimPaths, exit_wait: Duration) -> ReclaimCounts {
    let plans = plan(paths);
    let mut counts = ReclaimCounts::default();
    let mut signalled = Vec::new();

    for plan in &plans {
        match (&plan.ownership, plan.action) {
            (Ownership::Ours, Action::KillGroup(pgid)) => {
                match kill(Pid::from_raw(-pgid), Signal::SIGKILL) {
                    Ok(()) | Err(nix::errno::Errno::ESRCH) => {
                        info!(target: "agentenv", pid = plan.pid, pgid, "reclaimed a leftover Firecracker process group");
                        counts.reclaimed += 1;
                        signalled.push(plan.pid);
                    }
                    Err(error) => {
                        warn!(target: "agentenv", pid = plan.pid, pgid, %error, "cannot signal a leftover Firecracker process group");
                        counts.failed += 1;
                    }
                }
            }
            (Ownership::Ours, Action::KillProcess(pid)) => {
                match kill(Pid::from_raw(pid), Signal::SIGKILL) {
                    Ok(()) | Err(nix::errno::Errno::ESRCH) => {
                        info!(target: "agentenv", pid, "reclaimed a leftover Firecracker process");
                        counts.reclaimed += 1;
                        signalled.push(pid);
                    }
                    Err(error) => {
                        warn!(target: "agentenv", pid, %error, "cannot signal a leftover Firecracker process");
                        counts.failed += 1;
                    }
                }
            }
            (Ownership::Ours, Action::Nothing) => {
                // Preserve a visible failure if planning ever makes this reachable.
                counts.failed += 1;
            }
            (Ownership::Foreign, _) => {
                debug!(target: "agentenv", pid = plan.pid, "leaving a Firecracker this deployment did not start");
                counts.left_alone += 1;
            }
            (Ownership::LiveOwner(owner_pid), _) => {
                // A live stamped owner disproves the host-wide ownership premise.
                info!(
                    target: "agentenv",
                    pid = plan.pid,
                    owner_pid = *owner_pid,
                    "leaving a Firecracker whose server process is still running"
                );
                counts.left_alone += 1;
            }
            (Ownership::Vanished, _) => {}
            (Ownership::Undetermined(reason), _) => {
                warn!(target: "agentenv", pid = plan.pid, reason, "cannot tell whether a Firecracker is a leftover of ours");
                counts.failed += 1;
            }
        }
    }

    counts.failed += wait_for_exit(&paths.proc_dir, &signalled, exit_wait).await;
    counts
}

// Failure to exit vetoes the subsequent work-directory sweep.
async fn wait_for_exit(proc_dir: &Path, pids: &[i32], exit_wait: Duration) -> u64 {
    if pids.is_empty() {
        return 0;
    }

    let deadline = Instant::now() + exit_wait;
    let mut remaining: Vec<i32> = pids.to_vec();
    loop {
        remaining.retain(|pid| proc_dir.join(pid.to_string()).exists());
        if remaining.is_empty() {
            return 0;
        }
        if Instant::now() >= deadline {
            warn!(
                target: "agentenv",
                pids = ?remaining,
                "signalled Firecracker processes are still present; not deleting their work \
                 directories"
            );
            return remaining.len() as u64;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::os::unix::fs::symlink;
    use std::path::PathBuf;

    use tempfile::TempDir;

    struct FakeProc {
        root: TempDir,
    }

    impl FakeProc {
        fn new() -> Self {
            Self {
                root: TempDir::new().unwrap(),
            }
        }

        fn dir(&self) -> PathBuf {
            self.root.path().to_path_buf()
        }

        fn process(&self, pid: i32, comm: &str, cwd: Option<&Path>, pgid: i32) -> &Self {
            let dir = self.root.path().join(pid.to_string());
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("comm"), format!("{comm}\n")).unwrap();
            // The real thing brackets `comm` and can contain spaces; write it
            // that way so the parser is exercised as it will be used.
            std::fs::write(
                dir.join("stat"),
                format!("{pid} ({comm}) S 1 {pgid} {pgid} 0 -1 4194304 0 0"),
            )
            .unwrap();
            if let Some(cwd) = cwd {
                symlink(cwd, dir.join("cwd")).unwrap();
            }
            self
        }

        fn vanished_process(&self, pid: i32, comm: &str) -> &Self {
            let dir = self.root.path().join(pid.to_string());
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("comm"), format!("{comm}\n")).unwrap();
            self
        }
    }

    struct Fixture {
        proc: FakeProc,
        work: TempDir,
        persisted: TempDir,
    }

    impl Fixture {
        fn new() -> Self {
            Self {
                proc: FakeProc::new(),
                work: TempDir::new().unwrap(),
                persisted: TempDir::new().unwrap(),
            }
        }

        fn work_base(&self) -> PathBuf {
            std::fs::canonicalize(self.work.path()).unwrap()
        }

        fn sandbox_work_dir(&self, name: &str) -> PathBuf {
            let dir = self.work_base().join(name);
            std::fs::create_dir_all(&dir).unwrap();
            owner::stamp_as_leftover(&dir);
            dir
        }

        fn work_dir_of_a_live_server(&self, name: &str, server_pid: i32) -> PathBuf {
            let dir = self.work_base().join(name);
            std::fs::create_dir_all(&dir).unwrap();
            owner::stamp_for_test(&dir, server_pid, 77_000, None);
            let process = self.proc.dir().join(server_pid.to_string());
            std::fs::create_dir_all(&process).unwrap();
            std::fs::write(
                process.join("stat"),
                format!(
                    "{server_pid} (server) S 1 {server_pid} {server_pid} 0 -1 0 0 0 0 0 0 0 0 0 \
                     20 0 1 0 77000"
                ),
            )
            .unwrap();
            dir
        }

        fn paths(&self) -> ReclaimPaths {
            ReclaimPaths {
                work_base: self.work.path().to_path_buf(),
                proc_dir: self.proc.dir(),
                persisted_sandbox_store: self.persisted.path().to_path_buf(),
                // Not exercised here: these suites call `plan`/`reclaim`
                // directly, below the premise check `sweep` makes.
                server_exe: None,
                ublk_daemon_socket: PathBuf::from("/nonexistent/ublk.sock"),
            }
        }

        fn plan(&self) -> Vec<ProcessPlan> {
            plan(&self.paths())
        }
    }

    #[test]
    fn a_firecracker_whose_server_is_still_running_is_not_a_leftover() {
        const SERVER: i32 = 7200;

        let fixture = Fixture::new();
        let live = fixture.work_dir_of_a_live_server("agentenv-fc-live", SERVER);
        let dead = fixture.sandbox_work_dir("agentenv-fc-dead");
        fixture
            .proc
            .process(4101, "firecracker", Some(&live), 4101)
            .process(4102, "firecracker", Some(&dead), 4102);

        let plans = fixture.plan();
        assert_eq!(plans.len(), 2);
        assert_eq!(
            plans[0],
            ProcessPlan {
                pid: 4101,
                ownership: Ownership::LiveOwner(SERVER),
                action: Action::Nothing,
            },
            "a sandbox of a server that is up must never be signalled"
        );
        // The other face, in the same run and the same work base: a leftover
        // whose server is gone is still reclaimed, so the refusal above is a
        // decision rather than the sweep having been turned off.
        assert_eq!(
            plans[1],
            ProcessPlan {
                pid: 4102,
                ownership: Ownership::Ours,
                action: Action::KillGroup(4102),
            }
        );
    }

    #[test]
    fn a_firecracker_whose_work_directory_carries_no_stamp_is_left_alone() {
        let fixture = Fixture::new();
        let unstamped = fixture.work_base().join("agentenv-fc-unstamped");
        std::fs::create_dir_all(&unstamped).unwrap();
        let stamped = fixture.sandbox_work_dir("agentenv-fc-stamped");
        fixture
            .proc
            .process(4201, "firecracker", Some(&unstamped), 4201)
            .process(4202, "firecracker", Some(&stamped), 4202);

        let plans = fixture.plan();
        assert_eq!(plans[0].action, Action::Nothing);
        assert!(
            matches!(plans[0].ownership, Ownership::Undetermined(_)),
            "{:?}",
            plans[0]
        );
        // Resolution: the stamped one beside it is reclaimed.
        assert_eq!(plans[1].ownership, Ownership::Ours);
        assert_eq!(plans[1].action, Action::KillGroup(4202));
    }

    #[test]
    fn a_firecracker_outside_our_work_base_is_left_alone_and_ours_is_not() {
        let fixture = Fixture::new();
        let ours = fixture.sandbox_work_dir("agentenv-fc-A1b2C3");
        let elsewhere = TempDir::new().unwrap();
        let theirs = elsewhere.path().join("agentenv-fc-A1b2C3");
        std::fs::create_dir_all(&theirs).unwrap();

        fixture.proc.process(4001, "firecracker", Some(&ours), 4001);
        fixture
            .proc
            .process(4002, "firecracker", Some(&theirs), 4002);

        let plans = fixture.plan();
        assert_eq!(plans.len(), 2, "both are candidates: {plans:?}");

        assert_eq!(plans[0].ownership, Ownership::Ours);
        assert_eq!(plans[0].action, Action::KillGroup(4001));

        assert_eq!(
            plans[1].ownership,
            Ownership::Foreign,
            "a Firecracker running under someone else's work base is not ours to kill, \
             even though its directory carries our prefix"
        );
        assert_eq!(plans[1].action, Action::Nothing);
    }

    #[test]
    fn a_process_directly_under_the_work_base_is_not_ours_without_the_prefix() {
        let fixture = Fixture::new();
        let unrelated = fixture.work_base().join("some-other-tools-scratch");
        std::fs::create_dir_all(&unrelated).unwrap();

        fixture
            .proc
            .process(5001, "firecracker", Some(&unrelated), 5001);

        let plans = fixture.plan();
        assert_eq!(plans[0].ownership, Ownership::Foreign);
        assert_eq!(plans[0].action, Action::Nothing);

        // ...and the same place with the prefix is ours, so the assertion above
        // is about the prefix and not about the fixture.
        let ours = fixture.sandbox_work_dir("agentenv-fc-Zz9");
        fixture.proc.process(5002, "firecracker", Some(&ours), 5002);
        let plans = fixture.plan();
        assert_eq!(plans[1].ownership, Ownership::Ours);
    }

    #[test]
    fn only_the_work_directory_itself_counts_never_something_inside_it() {
        let fixture = Fixture::new();
        let inside = fixture.sandbox_work_dir("agentenv-fc-A1").join("rootfs");
        std::fs::create_dir_all(&inside).unwrap();
        let sibling_base = fixture.work_base().parent().unwrap().to_path_buf();
        let above = sibling_base.join("agentenv-fc-A1");
        std::fs::create_dir_all(&above).unwrap();

        fixture
            .proc
            .process(6001, "firecracker", Some(&inside), 6001);
        fixture
            .proc
            .process(6002, "firecracker", Some(&above), 6002);

        for plan in fixture.plan() {
            assert_eq!(
                plan.ownership,
                Ownership::Foreign,
                "neither a child of a work directory nor a sibling of the work base is one: {plan:?}"
            );
        }

        let itself = fixture.sandbox_work_dir("agentenv-fc-A2");
        fixture
            .proc
            .process(6003, "firecracker", Some(&itself), 6003);
        assert_eq!(fixture.plan()[2].ownership, Ownership::Ours);
    }

    #[test]
    fn a_process_group_of_zero_or_one_is_never_signalled_as_a_group() {
        let fixture = Fixture::new();
        let ours = fixture.sandbox_work_dir("agentenv-fc-A1");

        for (pid, pgid) in [(7001, 0), (7002, 1)] {
            fixture.proc.process(pid, "firecracker", Some(&ours), pgid);
        }

        let plans = fixture.plan();
        assert_eq!(plans.len(), 2);
        for plan in &plans {
            assert_eq!(plan.ownership, Ownership::Ours, "{plan:?}");
            assert_eq!(
                plan.action,
                Action::KillProcess(plan.pid),
                "a process group of 0 or 1 must degrade to signalling the one process: {plan:?}"
            );
        }

        // The control face: an ordinary group is signalled as a group, so the
        // two assertions above are about the values 0 and 1.
        fixture.proc.process(7003, "firecracker", Some(&ours), 7003);
        let plans = fixture.plan();
        assert_eq!(plans[2].action, Action::KillGroup(7003));
    }

    fn stat_dir(pid: i32, pgid: i32) -> TempDir {
        let dir = TempDir::new().unwrap();
        std::fs::write(
            dir.path().join("stat"),
            format!("{pid} (firecracker) S 1 {pgid} {pgid} 0 -1 4194304"),
        )
        .unwrap();
        dir
    }

    #[test]
    fn a_group_leader_with_id_one_is_never_signalled_as_a_group() {
        let dir = stat_dir(1, 1);
        assert_eq!(
            plan_for_owned(1, dir.path(), 4242),
            Action::KillProcess(1),
            "kill(-1, SIGKILL) signals every process this node is allowed to signal"
        );

        let dir = stat_dir(0, 0);
        assert_eq!(
            plan_for_owned(0, dir.path(), 4242),
            Action::KillProcess(0),
            "kill(-0, SIGKILL) signals the caller's own process group"
        );

        // The control face: an ordinary group leader is signalled as a group,
        // so the two above are about the values 0 and 1.
        let dir = stat_dir(7003, 7003);
        assert_eq!(
            plan_for_owned(7003, dir.path(), 4242),
            Action::KillGroup(7003)
        );
    }

    #[test]
    fn the_leader_of_our_own_group_is_never_signalled_as_a_group() {
        let own_pgid = 31337;
        let dir = stat_dir(own_pgid, own_pgid);
        assert_eq!(
            plan_for_owned(own_pgid, dir.path(), own_pgid),
            Action::KillProcess(own_pgid),
            "signalling our own group kills this server on the way past"
        );

        // ...and the same process under a different group is a group kill.
        assert_eq!(
            plan_for_owned(own_pgid, dir.path(), 9),
            Action::KillGroup(own_pgid)
        );
    }

    #[test]
    fn an_unreadable_stat_narrows_the_signal_to_one_process() {
        let empty = TempDir::new().unwrap();
        assert_eq!(
            plan_for_owned(4242, empty.path(), 9),
            Action::KillProcess(4242)
        );
    }

    #[test]
    fn our_own_process_group_is_never_signalled_as_a_group() {
        let fixture = Fixture::new();
        let ours = fixture.sandbox_work_dir("agentenv-fc-A1");
        let own_pgid = getpgrp().as_raw();

        fixture
            .proc
            .process(8001, "firecracker", Some(&ours), own_pgid);

        let plans = fixture.plan();
        assert_eq!(plans[0].ownership, Ownership::Ours);
        assert_eq!(
            plans[0].action,
            Action::KillProcess(8001),
            "signalling our own group would kill this server on the way past"
        );
    }

    #[test]
    fn a_process_that_does_not_lead_its_group_is_signalled_alone() {
        let fixture = Fixture::new();
        let ours = fixture.sandbox_work_dir("agentenv-fc-A1");
        fixture.proc.process(9001, "firecracker", Some(&ours), 42);

        let plans = fixture.plan();
        assert_eq!(plans[0].ownership, Ownership::Ours);
        assert_eq!(plans[0].action, Action::KillProcess(9001));
    }

    #[tokio::test]
    async fn absence_not_mine_and_cannot_tell_are_told_apart() {
        let fixture = Fixture::new();
        let ours = fixture.sandbox_work_dir("agentenv-fc-A1");
        let elsewhere = TempDir::new().unwrap();

        // Gone: the entry is there, the `cwd` link is not.
        fixture.proc.vanished_process(1001, "firecracker");
        // Not mine.
        fixture
            .proc
            .process(1002, "firecracker", Some(elsewhere.path()), 1002);
        // Not even a candidate.
        fixture
            .proc
            .process(1003, "qemu-system-x86_64", Some(&ours), 1003);

        let plans = fixture.plan();
        assert_eq!(
            plans.len(),
            2,
            "a process that is not Firecracker is not a candidate at all: {plans:?}"
        );
        assert_eq!(plans[0].ownership, Ownership::Vanished);
        assert_eq!(plans[1].ownership, Ownership::Foreign);

        let counts = reclaim(&fixture.paths(), Duration::from_millis(50)).await;
        assert_eq!(
            counts,
            ReclaimCounts {
                reclaimed: 0,
                left_alone: 1,
                failed: 0,
            },
            "a vanished process is not a failure and not a refusal; the stranger is a refusal"
        );
    }

    #[tokio::test]
    async fn a_candidate_that_cannot_be_classified_is_counted_as_a_failure() {
        let fixture = Fixture::new();
        let dir = fixture.proc.dir().join("2001");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("comm"), "firecracker\n").unwrap();
        std::fs::write(dir.join("stat"), "2001 (firecracker) S 1 2001 2001 0").unwrap();
        // Not a symlink: `read_link` answers EINVAL.
        std::fs::write(dir.join("cwd"), "not a link").unwrap();

        let plans = fixture.plan();
        assert!(
            matches!(plans[0].ownership, Ownership::Undetermined(_)),
            "{plans:?}"
        );
        assert_eq!(plans[0].action, Action::Nothing);

        let counts = reclaim(&fixture.paths(), Duration::from_millis(50)).await;
        assert_eq!(
            counts,
            ReclaimCounts {
                reclaimed: 0,
                left_alone: 0,
                failed: 1,
            },
            "an unreadable candidate must not read as an absent one"
        );
    }

    #[test]
    fn a_deleted_working_directory_is_still_ours() {
        let fixture = Fixture::new();
        let dir = fixture.proc.dir().join("3001");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("comm"), "firecracker\n").unwrap();
        std::fs::write(dir.join("stat"), "3001 (firecracker) S 1 3001 3001 0").unwrap();
        symlink(
            format!(
                "{} (deleted)",
                fixture.work_base().join("agentenv-fc-Gone").display()
            ),
            dir.join("cwd"),
        )
        .unwrap();

        assert_eq!(fixture.plan()[0].ownership, Ownership::Ours);
    }

    #[test]
    fn the_process_group_is_parsed_from_after_the_last_bracket() {
        let temp = TempDir::new().unwrap();
        std::fs::write(
            temp.path().join("stat"),
            "77 (fire (cracker) x) S 1 4242 4242 0 -1 4194304",
        )
        .unwrap();
        assert_eq!(read_pgid(temp.path()), Some(4242));

        std::fs::write(temp.path().join("stat"), "malformed").unwrap();
        assert_eq!(
            read_pgid(temp.path()),
            None,
            "an unparsable stat line yields no group rather than a plausible number"
        );
    }

    #[test]
    fn an_unreadable_proc_yields_no_plans() {
        let fixture = Fixture::new();
        let paths = ReclaimPaths {
            proc_dir: fixture.proc.dir().join("does-not-exist"),
            ..fixture.paths()
        };
        assert!(plan(&paths).is_empty());
    }

    #[tokio::test]
    async fn a_process_the_plan_names_is_actually_killed() {
        use std::os::unix::process::CommandExt;

        let fixture = Fixture::new();
        let ours = fixture.sandbox_work_dir("agentenv-fc-Live");

        let mut command = std::process::Command::new("sleep");
        command.arg("60");
        // Its own group, the way `FirecrackerInstance` spawns a VMM.
        unsafe {
            command.pre_exec(|| {
                nix::unistd::setpgid(Pid::from_raw(0), Pid::from_raw(0))?;
                Ok(())
            });
        }
        let mut child = command.spawn().unwrap();
        let pid = i32::try_from(child.id()).unwrap();

        fixture.proc.process(pid, "firecracker", Some(&ours), pid);
        let plans = fixture.plan();
        assert_eq!(plans[0].action, Action::KillGroup(pid));

        let counts = reclaim(&fixture.paths(), Duration::from_millis(200)).await;
        assert_eq!(counts.reclaimed, 1);

        let status = child.wait().unwrap();
        assert!(
            !status.success(),
            "the child must have been killed rather than have exited on its own"
        );
        assert_eq!(counts.failed, 1);
    }

    #[tokio::test]
    async fn the_whole_group_goes_not_just_the_process_the_plan_names() {
        use std::io::BufRead;
        use std::os::unix::process::CommandExt;

        let fixture = Fixture::new();
        let ours = fixture.sandbox_work_dir("agentenv-fc-Group");

        let mut command = std::process::Command::new("sh");
        command
            .arg("-c")
            .arg("sleep 60 & echo $!; wait")
            .stdout(std::process::Stdio::piped());
        // Its own group, the way `FirecrackerInstance` spawns a VMM.
        unsafe {
            command.pre_exec(|| {
                nix::unistd::setpgid(Pid::from_raw(0), Pid::from_raw(0))?;
                Ok(())
            });
        }
        let mut child = command.spawn().unwrap();
        let leader = i32::try_from(child.id()).unwrap();
        let follower: i32 = {
            let stdout = child.stdout.take().expect("stdout was piped");
            let mut line = String::new();
            std::io::BufReader::new(stdout)
                .read_line(&mut line)
                .unwrap();
            line.trim()
                .parse()
                .expect("the shell prints its background job's pid")
        };
        assert_ne!(follower, leader);
        // Said out loud rather than assumed: a shell that had put its
        // background job in a different group would make this test pass for a
        // reason that has nothing to do with the sign.
        assert_eq!(
            read_pgid(&PathBuf::from(format!("/proc/{follower}"))),
            Some(leader),
            "the second process is not in the leader's group"
        );

        fixture
            .proc
            .process(leader, "firecracker", Some(&ours), leader);
        assert_eq!(fixture.plan()[0].action, Action::KillGroup(leader));

        let counts = reclaim(&fixture.paths(), Duration::from_millis(200)).await;
        assert_eq!(counts.reclaimed, 1);
        let status = child.wait().unwrap();
        assert!(
            !status.success(),
            "the group leader must have been killed rather than have exited on its own"
        );

        let follower_gone = left_the_host(follower);
        if !follower_gone {
            let _ = kill(Pid::from_raw(follower), Signal::SIGKILL);
        }
        assert!(
            follower_gone,
            "the second process in the group outlived the kill: only its leader was signalled"
        );
    }

    #[tokio::test]
    async fn a_leftover_that_may_not_be_signalled_as_a_group_is_still_killed() {
        let fixture = Fixture::new();
        let ours = fixture.sandbox_work_dir("agentenv-fc-Alone");

        // No `setpgid`, so it is in this test process's own group — which is
        // the one group the sweep may never signal, and so the plan narrows.
        let mut child = std::process::Command::new("sleep")
            .arg("60")
            .spawn()
            .unwrap();
        let pid = i32::try_from(child.id()).unwrap();
        fixture
            .proc
            .process(pid, "firecracker", Some(&ours), getpgrp().as_raw());

        assert_eq!(fixture.plan()[0].action, Action::KillProcess(pid));

        let counts = reclaim(&fixture.paths(), Duration::from_millis(200)).await;
        assert_eq!(counts.reclaimed, 1, "a narrowed kill is still a reclaim");
        let status = child.wait().unwrap();
        assert!(
            !status.success(),
            "the leftover must have been killed rather than have exited on its own"
        );
        // As above: the forged entry cannot show a process leaving, so the wait
        // times out and says so.
        assert_eq!(counts.failed, 1);
    }

    #[tokio::test]
    async fn the_wait_ends_when_the_process_leaves_and_not_before() {
        let proc = FakeProc::new();
        proc.process(5001, "firecracker", None, 5001);
        let entry = proc.dir().join("5001");

        // Present when the wait starts and gone shortly after: a wait that did
        // not outlast its own first look cannot see this happen.
        let leaving = entry.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(120)).await;
            std::fs::remove_dir_all(&leaving).unwrap();
        });
        let started = Instant::now();
        assert_eq!(
            wait_for_exit(&proc.dir(), &[5001], Duration::from_secs(5)).await,
            0,
            "the process left, so nothing outlived its SIGKILL"
        );
        assert!(
            started.elapsed() >= Duration::from_millis(120),
            "the wait returned before the process it was waiting for had left"
        );
        assert!(!entry.exists());

        // The other half, on the same forged `/proc`: one that never leaves is
        // reported rather than waited on forever — and not before its deadline.
        proc.process(5002, "firecracker", None, 5002);
        let started = Instant::now();
        assert_eq!(
            wait_for_exit(&proc.dir(), &[5002], Duration::from_millis(200)).await,
            1,
            "a process still in /proc after its wait is a failure, not a success"
        );
        assert!(
            started.elapsed() >= Duration::from_millis(200),
            "the deadline was not waited out"
        );
    }

    // Treat zombies as gone; only an unreaped exit status remains.
    fn left_the_host(pid: i32) -> bool {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
                Err(_) => return true,
                Ok(stat) => {
                    let state = stat
                        .rfind(')')
                        .and_then(|end| stat[end + 1..].split_whitespace().next());
                    if state == Some("Z") {
                        return true;
                    }
                }
            }
            if Instant::now() >= deadline {
                return false;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}
