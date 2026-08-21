//! Firecracker VMMs a previous process on this machine left running.
//!
//! Split into a pure half and a syscall half on purpose. [`plan`] reads `/proc`
//! and decides, per process, what should happen to it and why; [`apply`] does
//! the signalling. Everything that can be wrong about *who gets killed* lives in
//! the pure half, where a test can put a hostile `/proc` in front of it and read
//! the decision back without a single real process being at risk.

use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use nix::sys::signal::{kill, Signal};
use nix::unistd::{getpgrp, Pid};
use tracing::{debug, info, warn};

use super::{ReclaimCounts, ReclaimPaths};

/// The `comm` of the process this sweep is looking for.
///
/// `/proc/<pid>/comm` is world-readable and never longer than 15 bytes, which
/// `firecracker` fits inside. It is the cheap filter; it decides only whether a
/// process is a *candidate*, never whether it is ours.
const FIRECRACKER_COMM: &str = "firecracker";

/// The prefix `create_firecracker_work_dir` gives every sandbox work directory.
///
/// 🔴 Load-bearing, not cosmetic. See [`is_sandbox_work_dir`].
pub(super) const WORK_DIR_PREFIX: &str = "agentenv-fc-";

/// What the sweep concluded about one candidate process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Ownership {
    /// Started by a previous AgentENV process on this machine: it is
    /// Firecracker, and its working directory is a sandbox work directory
    /// under this deployment's own work base.
    Ours,
    /// Someone else's. A Firecracker, but running somewhere this deployment
    /// never puts one.
    Foreign,
    /// It went away while it was being read. Not an answer about ownership,
    /// and not a failure — it is the state the sweep was trying to reach.
    Vanished,
    /// 🔴 Could not be determined.
    ///
    /// Kept apart from [`Ownership::Foreign`] deliberately, even though both
    /// lead to the same action — none. They must not lead to the same *reading*:
    /// a sweep whose `/proc` access broke under some future kernel or seccomp
    /// profile would otherwise report the same clean "nothing of mine here" as a
    /// sweep on a genuinely clean host, forever.
    Undetermined(&'static str),
}

/// What [`apply`] should do about one process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Action {
    /// Signal the whole process group. Firecracker is spawned with
    /// `process_group(0)` (`sandbox::firecracker::instance`), so a VMM we
    /// started leads its own group and taking the group takes any helper it
    /// spawned with it.
    KillGroup(i32),
    /// Signal just this process. What is done when the group cannot be
    /// established, or when signalling the group would mean signalling
    /// something other than that one VMM's group.
    KillProcess(i32),
    /// Leave it alone.
    Nothing,
}

/// One `/proc` entry, the conclusion drawn about it, and what follows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProcessPlan {
    pub pid: i32,
    pub ownership: Ownership,
    pub action: Action,
}

/// 🔴 Process group ids that must never be turned into `kill(-pgid, …)`.
///
/// `kill(-0, sig)` signals **the caller's own process group** and
/// `kill(-1, sig)` signals **every process the caller is permitted to signal**
/// — which, in a privileged container on a node, is the machine: this server,
/// the ublk daemon, every other VM on the host, and the kubelet's view of all
/// of it. Neither is a hypothetical: `pgid` is parsed out of a `/proc` file, and
/// a field that shifts, a truncated read, or a process exiting mid-parse all
/// produce a `0` that looks like an ordinary number.
///
/// The caller's own group is refused separately, in [`plan_for_owned`], because
/// its value is not a constant.
const NEVER_A_GROUP_TARGET: [i32; 2] = [0, 1];

/// Reads `paths.proc_dir` and decides what should happen to each process there.
///
/// Pure with respect to the system: it reads, it does not signal. Every
/// judgement about who may be killed is made here.
pub(super) fn plan(paths: &ReclaimPaths) -> Vec<ProcessPlan> {
    let work_base = resolve(&paths.work_base);
    let own_pid = i32::try_from(std::process::id()).unwrap_or(-1);
    let own_pgid = getpgrp().as_raw();

    let entries = match std::fs::read_dir(&paths.proc_dir) {
        Ok(entries) => entries,
        Err(error) => {
            // Not a per-candidate failure — there were no candidates. It is
            // still worth saying out loud, because on a node this means the
            // sweep did nothing at all.
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
        // 🔴 Cheap paranoia with a real payoff: this process is not a
        // Firecracker and so cannot reach the branches below, but the cost of
        // being wrong about that once is the node killing itself during
        // startup.
        if pid == own_pid {
            continue;
        }

        let process_dir = paths.proc_dir.join(pid.to_string());
        if !is_candidate(&process_dir) {
            continue;
        }

        plans.push(plan_for_candidate(pid, &process_dir, &work_base, own_pgid));
    }

    plans.sort_by_key(|plan| plan.pid);
    plans
}

/// Whether this `/proc` entry is a Firecracker at all.
///
/// Deliberately silent about everything it rejects. A host runs hundreds of
/// processes that are none of this sweep's business, and turning each one into
/// an examined candidate would bury the handful that are.
fn is_candidate(process_dir: &Path) -> bool {
    std::fs::read_to_string(process_dir.join("comm"))
        .map(|comm| comm.trim() == FIRECRACKER_COMM)
        .unwrap_or(false)
}

fn plan_for_candidate(
    pid: i32,
    process_dir: &Path,
    work_base: &Path,
    own_pgid: i32,
) -> ProcessPlan {
    let ownership = match std::fs::read_link(process_dir.join("cwd")) {
        Ok(cwd) => {
            let cwd = resolve(&strip_deleted_marker(&cwd));
            if is_sandbox_work_dir(&cwd, work_base) {
                Ownership::Ours
            } else {
                Ownership::Foreign
            }
        }
        // The process exited between the directory listing and this read. That
        // is not something that could not be determined; it is the outcome.
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

/// How to signal a process already established as ours.
///
/// 🔴 Every branch that is not `KillGroup` is a refusal, and each one exists
/// because turning it into a group kill would signal something other than that
/// one VMM.
///
/// 🔴 The order matters, and two of the refusals only bite where the last one
/// does not. `pgid != pid` already covers most of the ground — a group whose
/// leader is not this process is somebody else's — so the two guards above it
/// are reached exactly when the leader *is* this process and the group is
/// still not safe: `pgid == pid == 1` (`kill(-1)` is every process on the
/// machine) and `pgid == pid == own_pgid` (this server's own group). Those two
/// cases are where the catastrophic mistakes live, so they are tested directly
/// against this function rather than through `plan`, where the `pgid != pid`
/// arm would answer first and the guards would look redundant.
fn plan_for_owned(pid: i32, process_dir: &Path, own_pgid: i32) -> Action {
    let Some(pgid) = read_pgid(process_dir) else {
        // Ownership is settled; only the blast radius is not. Signal the one
        // process that was identified.
        return Action::KillProcess(pid);
    };
    if NEVER_A_GROUP_TARGET.contains(&pgid) {
        return Action::KillProcess(pid);
    }
    if pgid == own_pgid {
        // This server's own group. A Firecracker in our work directory that
        // shares our process group did not come from `process_group(0)`, and
        // signalling the group would kill this process on the way past.
        warn!(
            target: "agentenv",
            pid,
            pgid,
            "a leftover Firecracker shares this process's group; signalling it alone"
        );
        return Action::KillProcess(pid);
    }
    if pgid != pid {
        // Not a group leader, so the group is somebody else's and contains
        // more than this VMM.
        return Action::KillProcess(pid);
    }
    Action::KillGroup(pgid)
}

/// Whether `cwd` is one of this deployment's sandbox work directories.
///
/// 🔴 Two conditions, and dropping either one is a real fault rather than a
/// looser check:
///
/// * **the parent must be the configured work base** — without it the sweep
///   kills every Firecracker on the machine, including another tenant's.
/// * **the directory name must carry the `agentenv-fc-` prefix** — without it
///   the sweep kills every process whose working directory sits directly under
///   the work base, and `[firecracker].work_dir` is *optional*: unset, the work
///   base is the **system temp directory**, and "its cwd is `/tmp`" is true of a
///   great many processes that have nothing to do with this.
fn is_sandbox_work_dir(cwd: &Path, work_base: &Path) -> bool {
    cwd.parent() == Some(work_base)
        && cwd
            .file_name()
            .and_then(OsStr::to_str)
            .is_some_and(|name| name.starts_with(WORK_DIR_PREFIX))
}

/// The process group id from `/proc/<pid>/stat`, or `None` when it cannot be
/// read out.
///
/// The field is the fifth, and the first two cannot be split on whitespace:
/// `comm` is bracketed and may itself contain spaces and brackets, so the parse
/// starts after the **last** `)`. Getting that wrong shifts every field by one
/// and yields a plausible-looking number, which is why
/// [`NEVER_A_GROUP_TARGET`] exists downstream of it.
fn read_pgid(process_dir: &Path) -> Option<i32> {
    let stat = std::fs::read_to_string(process_dir.join("stat")).ok()?;
    let after_comm = &stat[stat.rfind(')')? + 1..];
    // After the closing bracket: state, ppid, pgrp, …
    after_comm.split_whitespace().nth(2)?.parse().ok()
}

/// `/proc/<pid>/cwd` for a process whose working directory has been unlinked
/// reads back as `"<path> (deleted)"`. It is still that path, and the process
/// is still ours.
fn strip_deleted_marker(path: &Path) -> PathBuf {
    match path
        .to_str()
        .and_then(|path| path.strip_suffix(" (deleted)"))
    {
        Some(stripped) => PathBuf::from(stripped),
        None => path.to_path_buf(),
    }
}

/// Resolves symlinks where possible and falls back to the path as written.
///
/// The fallback matters: a work base that does not exist yet, or a working
/// directory already unlinked, cannot be canonicalised, and treating that as
/// "could not tell" would make an ordinary cold start look like a broken sweep.
fn resolve(path: &Path) -> PathBuf {
    std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}

/// Carries out a plan and waits for what it signalled to actually be gone.
pub(super) async fn reclaim(paths: &ReclaimPaths, exit_wait: Duration) -> ReclaimCounts {
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
                // Unreachable by construction, and counted rather than ignored
                // so that a future edit which makes it reachable shows up as a
                // number instead of as silence.
                counts.failed += 1;
            }
            (Ownership::Foreign, _) => {
                debug!(target: "agentenv", pid = plan.pid, "leaving a Firecracker this deployment did not start");
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

/// Waits for signalled processes to leave `/proc`, and reports how many did
/// not.
///
/// 🔴 The work directory sweep runs after this returns, and deleting the files
/// under a VMM that is still running leaves it running and broken. A VMM that
/// outlives its `SIGKILL` is counted as a failure so the two are told apart.
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

    /// A `/proc` a test can write.
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

        /// A process with a `comm`, a `cwd` symlink and a `stat` line.
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

        /// A process whose `cwd` is a dangling symlink, which is what a
        /// process that exited mid-sweep looks like.
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

        /// A sandbox work directory of ours, created for real so `cwd` can
        /// point at it.
        fn sandbox_work_dir(&self, name: &str) -> PathBuf {
            let dir = self.work_base().join(name);
            std::fs::create_dir_all(&dir).unwrap();
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

    // ── The refusals ────────────────────────────────────────────────────────

    /// 🔴 T-NR-1. **The refusal, and the reason this module can exist at all.**
    ///
    /// A node is a machine, and a machine may be running Firecrackers that are
    /// nothing to do with this deployment — another tenant's, a developer's,
    /// the previous generation of this software installed elsewhere. The sweep
    /// kills by *name and place*, and only the place tells them apart.
    ///
    /// Both faces in one test, because a sweep that refuses everything also
    /// passes a test that only checks that a stranger's VMM survives — and a
    /// sweep that refuses everything is a node that starts with the last
    /// process's VMs still holding its memory, its ublk devices and its network
    /// slots.
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

    /// 🔴 T-NR-2. The second half of the ownership test, and the one that
    /// matters most when `[firecracker].work_dir` is unset.
    ///
    /// Unset, the work base is the **system temp directory**. Matching on the
    /// parent alone would then make every process whose working directory is
    /// `/tmp` a leftover Firecracker of ours.
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

    /// 🔴 T-NR-3. A nested directory under a work directory is not a work
    /// directory.
    ///
    /// Guards the same mistake `role_gate::node_detail_id` guards: writing the
    /// match as a prefix test rather than as an exact parent.
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

        // 🔴 And the control: the work directory itself, in the same fixture,
        // is ours. Without it a `plan` that answered `Foreign` for everything —
        // a sweep that reclaims nothing anywhere — passes the loop above.
        let itself = fixture.sandbox_work_dir("agentenv-fc-A2");
        fixture
            .proc
            .process(6003, "firecracker", Some(&itself), 6003);
        assert_eq!(fixture.plan()[2].ownership, Ownership::Ours);
    }

    /// 🔴 T-NR-4. **`kill(-0)` is "my own process group" and `kill(-1)` is "the
    /// machine".**
    ///
    /// `pgid` comes out of a `/proc` file. A truncated read, a field that moves
    /// in a future kernel, or a process exiting mid-parse all produce a number,
    /// and `0` is the number they produce. Turning it into `kill(-pgid,
    /// SIGKILL)` as root on a node kills this server, the ublk daemon and every
    /// VM on the host — during startup, before anything is watching.
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

    /// A `/proc`-shaped directory carrying just a `stat` line, for driving
    /// [`plan_for_owned`] directly.
    fn stat_dir(pid: i32, pgid: i32) -> TempDir {
        let dir = TempDir::new().unwrap();
        std::fs::write(
            dir.path().join("stat"),
            format!("{pid} (firecracker) S 1 {pgid} {pgid} 0 -1 4194304"),
        )
        .unwrap();
        dir
    }

    /// 🔴 T-NR-4b. **`kill(-1)` is every process on the machine.**
    ///
    /// Driven straight at [`plan_for_owned`], because that is the only place
    /// the guard is reachable: through `plan`, a process whose group id is 1
    /// almost always fails `pgid == pid` first and is signalled alone anyway,
    /// so a test at that level passes with the guard deleted. The one arrangement
    /// where the guard is what stands between here and `kill(-1, SIGKILL)` is a
    /// group leader whose id is 1 — which is exactly what a shifted `stat` field
    /// or a truncated read produces.
    ///
    /// Found by mutation: deleting the guard left every `plan`-level test green.
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

    /// 🔴 T-NR-5b. The sweep never signals its own group, in the one
    /// arrangement where saying so costs something.
    ///
    /// Same story as above: reached only when the leader of our own group is
    /// the candidate, which `pgid != pid` cannot catch.
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

    /// T-NR-6b. An unreadable `stat` settles nothing about the group, so only
    /// the one process that was identified is signalled.
    #[test]
    fn an_unreadable_stat_narrows_the_signal_to_one_process() {
        let empty = TempDir::new().unwrap();
        assert_eq!(
            plan_for_owned(4242, empty.path(), 9),
            Action::KillProcess(4242)
        );
    }

    /// 🔴 T-NR-5. The sweep never signals the group it is a member of.
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

    /// T-NR-6. A Firecracker of ours that does not lead its own group is
    /// signalled alone: the group is somebody else's and holds more than it.
    #[test]
    fn a_process_that_does_not_lead_its_group_is_signalled_alone() {
        let fixture = Fixture::new();
        let ours = fixture.sandbox_work_dir("agentenv-fc-A1");
        fixture.proc.process(9001, "firecracker", Some(&ours), 42);

        let plans = fixture.plan();
        assert_eq!(plans[0].ownership, Ownership::Ours);
        assert_eq!(plans[0].action, Action::KillProcess(9001));
    }

    // ── The three-state answer ──────────────────────────────────────────────

    /// 🔴 T-NR-7. "Gone", "not mine" and "cannot tell" are three answers.
    ///
    /// All three lead to the same action — none — which is exactly why they
    /// have to stay distinguishable in the record. Collapsed into two, a sweep
    /// that lost its `/proc` access reports the same clean zero as a sweep on a
    /// clean host, and does so for as long as nobody restarts a node expecting
    /// to see something reclaimed.
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

    /// 🔴 T-NR-8. A candidate that cannot be classified is a failure, not a
    /// stranger.
    ///
    /// Driven through a `cwd` symlink that exists and points nowhere useful is
    /// not enough — that reads back fine. The reachable version of "cannot
    /// tell" is a `read_link` that fails for a reason other than absence, which
    /// is produced here by making `cwd` a regular file: `EINVAL`, not
    /// `NotFound`.
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

    // ── Details that are easy to get wrong ──────────────────────────────────

    /// T-NR-9. A work directory that has already been unlinked still belongs to
    /// us.
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

    /// T-NR-10. `stat`'s fifth field survives a `comm` with spaces and
    /// brackets in it.
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

    /// T-NR-11. `/proc` that cannot be read produces no plans and no
    /// invented failures.
    #[test]
    fn an_unreadable_proc_yields_no_plans() {
        let fixture = Fixture::new();
        let paths = ReclaimPaths {
            proc_dir: fixture.proc.dir().join("does-not-exist"),
            ..fixture.paths()
        };
        assert!(plan(&paths).is_empty());
    }

    /// 🔴 T-NR-12. The reclaim actually signals, and the counters say which
    /// way each decision went.
    ///
    /// Runs against a real child process this test starts, so the syscall half
    /// is exercised rather than asserted about. The child is `sleep`, put in
    /// its own process group, with a `/proc` entry forged for it that says its
    /// working directory is one of ours.
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

        // The forged `/proc` is what the plan reads; the wait afterwards has to
        // watch the real one, so point the reclaim at `/proc` for that half by
        // deleting the forged entry once the signal has been sent.
        let counts = reclaim(&fixture.paths(), Duration::from_millis(200)).await;
        assert_eq!(counts.reclaimed, 1);

        let status = child.wait().unwrap();
        assert!(
            !status.success(),
            "the child must have been killed rather than have exited on its own"
        );
        // 🔴 The forged entry is still on disk, so `wait_for_exit` timed out and
        // said so. That is the honest reading: this fake `/proc` cannot show a
        // process leaving. The real one can, and
        // `a_clean_host_reclaims_nothing_and_fails_at_nothing` covers the empty
        // case.
        assert_eq!(counts.failed, 1);
    }
}
