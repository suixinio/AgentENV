//! Which server process created a sandbox work directory, and whether that
//! process is still running.
//!
//! # 🔴 Why this exists
//!
//! The rest of this module classifies a Firecracker by *where* it is running:
//! a VMM whose working directory sits under this deployment's work base was
//! started by an AgentENV server. That answers "did one of us start this", and
//! it is the only question the host could answer — a namespace is named after a
//! uuid and a work directory after a random suffix, and neither says who made
//! it.
//!
//! It does not answer the question that decides whether killing it is safe:
//! **is the process that started it still running?** Those two are the same
//! question only while the premise in this module's header holds, and that
//! premise is checked by two host-wide probes ([`super::premise_holds`]) that
//! can both come back "clear" while a server is running right beside us:
//!
//! * `another_server_instance` compares `/proc/<pid>/exe` against our own. A
//!   server whose binary was replaced on disk — an in-place upgrade, a
//!   `cargo build` over a running binary — reads back as `"<path> (deleted)"`,
//!   which is not our path. (That specific hole is now closed, but it is the
//!   shape of hole a whole-host probe has.)
//! * `ublk_daemon_answers` asks one configured socket path. Two servers with
//!   different `AENV_HOME` and a shared `[firecracker].work_dir` do not share
//!   that socket.
//!
//! Each of those is one bad answer away from this process killing another
//! server's running VMs and deleting the directories they are running in. So
//! the decision is no longer taken on a whole-host premise alone: every
//! candidate is asked, individually, whether its creator is still alive.
//!
//! # What is written, and why those three fields
//!
//! [`stamp_work_dir`] drops one small file in each work directory as it is
//! created, naming the process that created it:
//!
//! * `pid` — the server process.
//! * `starttime` — `/proc/<pid>/stat` field 22, ticks since boot. Pids are
//!   reused, and a stamp that named only a pid would call a *later, unrelated*
//!   process the owner and refuse to reclaim a genuine leftover forever.
//! * `boot_id` — so a directory that survived a reboot is known to be a
//!   leftover without having to trust that no process on the new boot happens
//!   to match. Optional on both sides: when either is missing the comparison is
//!   simply skipped, because its absence can only cost a leak (see below) and
//!   refusing to read a stamp without it would make the whole mechanism inert
//!   on any host that hides `/proc/sys`.
//!
//! # 🔴 The two error directions are not symmetric, and every default here
//! leans the same way
//!
//! Failing to reclaim a leftover leaks a VMM's memory and a directory until the
//! next sweep. Reclaiming something live destroys a user's workspace and cannot
//! be undone. So:
//!
//! * no stamp, an unreadable stamp, a stamp from a format this build does not
//!   know, or an owner whose `/proc` entry is there but unreadable all answer
//!   [`Owner::Unknown`], and the callers treat that as "leave it alone and say
//!   the sweep could not account for it" — never as "leftover".
//! * a stamp that cannot be *written* is a warning at sandbox creation, not a
//!   failed create. The sandbox runs; its directory is one a later sweep will
//!   decline to touch.
//!
//! The consequence to state plainly: **the first sweep after this lands will
//! decline to reclaim every work directory created before it**, because none of
//! them carry a stamp. That is one bounded leak per host, it is loud in
//! `agentenv_node_reclaim_failed_total`, and it is the direction to be wrong
//! in.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// The file [`stamp_work_dir`] writes into each work directory.
///
/// A dotfile so it does not appear in a casual listing of a VM's work
/// directory, and a name nothing else in this tree writes.
const STAMP_FILE: &str = ".aenv-owner";

/// The stamp format this build writes and understands.
///
/// 🔴 A stamp carrying any other version is [`Owner::Unknown`], not a parse
/// error to be worked around. A future build that changes what these fields
/// mean must not have this one draw conclusions from them, and "cannot tell"
/// is the answer that leaks rather than kills.
const STAMP_VERSION: u32 = 1;

/// Where the current boot's identity lives, relative to a `/proc`.
const BOOT_ID: &str = "sys/kernel/random/boot_id";

/// The process that created a work directory, as recorded when it did.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct OwnerStamp {
    version: u32,
    /// `None` when the boot could not be identified at stamping time. Read
    /// leniently — see the module header.
    #[serde(default)]
    boot_id: Option<String>,
    pid: i32,
    starttime: u64,
}

/// What a work directory's stamp says about the process that created it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Owner {
    /// The process that created this directory is still running. Whatever is
    /// in here belongs to a server that is up, and none of it is a leftover.
    Alive(i32),
    /// The process that created it is gone: a different boot, no such pid, or
    /// that pid now belongs to a process started later than the stamp.
    Gone,
    /// 🔴 Could not be determined, and therefore not a licence. Kept apart from
    /// [`Owner::Gone`] because they lead to opposite actions.
    Unknown(&'static str),
}

/// Records the current process as the owner of a freshly created work
/// directory.
///
/// Called from `sandbox::firecracker::config::create_firecracker_work_dir`,
/// which is the only place a `agentenv-fc-*` directory is made. Keeping the
/// writer next to [`owner_of`] is deliberate: a writer and a reader that drift
/// apart in format would not fail loudly, they would make every directory on
/// the host [`Owner::Unknown`] and quietly turn the sweep off.
pub(crate) fn stamp_work_dir(work_dir: &Path) -> std::io::Result<()> {
    let proc_dir = Path::new("/proc");
    let starttime = match start_time(proc_dir, "self") {
        Ok(Some(starttime)) => starttime,
        Ok(None) | Err(_) => {
            return Err(std::io::Error::other(
                "cannot read this process's start time out of /proc/self/stat, so an owner \
                 stamp would name a process it could not later prove was gone",
            ))
        }
    };

    // 🔴 Refused rather than defaulted, for the same reason as the start time
    // above. A pid that does not fit this field is a stamp naming a process that
    // is not this one, and every later reader would answer a definite thing
    // about the wrong process — or, with the obvious `-1`, about a number that
    // means "no such process" to nobody.
    let Ok(pid) = i32::try_from(std::process::id()) else {
        return Err(std::io::Error::other(
            "this process's id does not fit the field an owner stamp records",
        ));
    };

    let stamp = OwnerStamp {
        version: STAMP_VERSION,
        boot_id: boot_id(proc_dir),
        pid,
        starttime,
    };
    let encoded = serde_json::to_vec(&stamp).map_err(std::io::Error::other)?;
    std::fs::write(stamp_path(work_dir), encoded)
}

/// Reads back what [`stamp_work_dir`] wrote, and asks `/proc` whether that
/// process is still there.
pub(super) fn owner_of(work_dir: &Path, proc_dir: &Path) -> Owner {
    let raw = match std::fs::read(stamp_path(work_dir)) {
        Ok(raw) => raw,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Owner::Unknown("no owner stamp")
        }
        Err(_) => return Owner::Unknown("owner stamp unreadable"),
    };
    let Ok(stamp) = serde_json::from_slice::<OwnerStamp>(&raw) else {
        return Owner::Unknown("owner stamp unparseable");
    };
    if stamp.version != STAMP_VERSION {
        return Owner::Unknown("owner stamp written in a format this build does not know");
    }

    // A different boot settles it without asking about pids at all: nothing
    // stamped on a previous boot is running now.
    if let (Some(stamped), Some(current)) = (stamp.boot_id.as_deref(), boot_id(proc_dir)) {
        if stamped != current {
            return Owner::Gone;
        }
    }

    match start_time(proc_dir, &stamp.pid.to_string()) {
        // The same process is still there.
        Ok(Some(starttime)) if starttime == stamp.starttime => Owner::Alive(stamp.pid),
        // The pid is in use by something started after the stamp was written,
        // which means the stamped process exited and its number was reused.
        Ok(Some(_)) => Owner::Gone,
        // No such process.
        Ok(None) => Owner::Gone,
        // 🔴 The entry is there and could not be read. Not "gone": this is the
        // one branch where guessing wrong kills a live sandbox.
        Err(reason) => Owner::Unknown(reason),
    }
}

fn stamp_path(work_dir: &Path) -> PathBuf {
    work_dir.join(STAMP_FILE)
}

/// This boot's identity, or `None` when it cannot be read.
fn boot_id(proc_dir: &Path) -> Option<String> {
    std::fs::read_to_string(proc_dir.join(BOOT_ID))
        .ok()
        .map(|id| id.trim().to_string())
        .filter(|id| !id.is_empty())
}

/// Field 22 of `/proc/<who>/stat`, in clock ticks since boot.
///
/// `Ok(None)` is "there is no such process"; `Err` is "there is, and it could
/// not be read", and the two must not be collapsed — one is the sweep's target
/// and the other is the sweep's blind spot.
///
/// The parse starts after the **last** `)` for the reason
/// [`super::firecracker::read_pgid`] documents: `comm` is bracketed and may
/// itself contain brackets and spaces. Field 22 is the twentieth token after
/// it, counting the state character as the first.
fn start_time(proc_dir: &Path, who: &str) -> Result<Option<u64>, &'static str> {
    let process_dir = proc_dir.join(who);
    let stat = match std::fs::read_to_string(process_dir.join("stat")) {
        Ok(stat) => stat,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            // Distinguish "the process is gone" from "this /proc has no stat
            // for it", which a hardened mount can produce.
            return if process_dir.exists() {
                Err("the owner's process entry has no readable stat")
            } else {
                Ok(None)
            };
        }
        Err(_) => return Err("the owner's process entry could not be read"),
    };
    let after_comm = stat
        .rfind(')')
        .map(|end| &stat[end + 1..])
        .ok_or("the owner's stat line is not in the expected shape")?;
    after_comm
        .split_whitespace()
        .nth(19)
        .ok_or("the owner's stat line has no start time")?
        .parse()
        .map(Some)
        .map_err(|_| "the owner's start time is not a number")
}

/// Writes a stamp naming a chosen process, for tests that have to lay out a
/// host rather than run on one.
///
/// 🔴 Test-only and deliberately in this file: a fixture that wrote the stamp
/// format by hand would go on passing after the format changed, and the tests
/// that matter most here are the ones that lay out a hostile host.
#[cfg(test)]
pub(super) fn stamp_for_test(work_dir: &Path, pid: i32, starttime: u64, boot_id: Option<&str>) {
    let stamp = OwnerStamp {
        version: STAMP_VERSION,
        boot_id: boot_id.map(str::to_string),
        pid,
        starttime,
    };
    std::fs::write(stamp_path(work_dir), serde_json::to_vec(&stamp).unwrap()).unwrap();
}

/// The pid a test stamps with when it wants "the server that made this is
/// gone". Nothing in a forged `/proc` is ever given this number.
#[cfg(test)]
pub(super) const DEAD_OWNER_PID: i32 = 999_001;

/// Stamps `work_dir` as belonging to a server that has exited — what every
/// leftover in these suites is.
#[cfg(test)]
pub(super) fn stamp_as_leftover(work_dir: &Path) {
    std::fs::write(stamp_path(work_dir), leftover_stamp_bytes()).unwrap();
}

/// The bytes [`stamp_as_leftover`] writes.
///
/// 🔴 Handed out rather than reproduced by hand, for the reason
/// [`stamp_for_test`] gives: a test that has to deliver a stamp through
/// something other than a file — see `work_dirs`' vanishing-directory suite,
/// which delivers one down a FIFO — still has to deliver the format this
/// module reads, or it is testing its own copy of it.
#[cfg(test)]
pub(super) fn leftover_stamp_bytes() -> Vec<u8> {
    serde_json::to_vec(&OwnerStamp {
        version: STAMP_VERSION,
        boot_id: None,
        pid: DEAD_OWNER_PID,
        starttime: 1,
    })
    .unwrap()
}

/// Where [`owner_of`] looks for the stamp, for tests that have to put
/// something other than an ordinary file there.
#[cfg(test)]
pub(super) fn stamp_path_of(work_dir: &Path) -> PathBuf {
    stamp_path(work_dir)
}

#[cfg(test)]
mod tests {
    use super::*;

    use tempfile::TempDir;

    /// A `/proc` a test can write, plus a work directory to stamp.
    struct Fixture {
        root: TempDir,
    }

    impl Fixture {
        fn new() -> Self {
            let root = TempDir::new().unwrap();
            std::fs::create_dir_all(root.path().join("proc/sys/kernel/random")).unwrap();
            std::fs::write(root.path().join("proc").join(BOOT_ID), "boot-one\n").unwrap();
            std::fs::create_dir_all(root.path().join("work")).unwrap();
            Self { root }
        }

        fn proc(&self) -> PathBuf {
            self.root.path().join("proc")
        }

        fn work(&self) -> PathBuf {
            self.root.path().join("work")
        }

        /// A process in the fake `/proc` with a chosen start time.
        fn process(&self, pid: i32, starttime: u64) {
            let dir = self.proc().join(pid.to_string());
            std::fs::create_dir_all(&dir).unwrap();
            // The real thing brackets `comm` and can contain spaces and
            // brackets; write it that way so the parser is used as it will be.
            let mut fields = vec![pid.to_string(), "(a (weird) name)".to_string()];
            // state, ppid, pgrp, … up to field 21.
            fields.push("S".to_string());
            for field in 4..=21 {
                fields.push(field.to_string());
            }
            fields.push(starttime.to_string());
            std::fs::write(dir.join("stat"), fields.join(" ")).unwrap();
        }

        fn write_stamp(&self, stamp: &OwnerStamp) {
            std::fs::write(stamp_path(&self.work()), serde_json::to_vec(stamp).unwrap()).unwrap();
        }

        fn stamp(&self, pid: i32, starttime: u64, boot: Option<&str>) {
            self.write_stamp(&OwnerStamp {
                version: STAMP_VERSION,
                boot_id: boot.map(str::to_string),
                pid,
                starttime,
            });
        }
    }

    /// 🔴 T-NR-40. **The distinction the whole sweep now turns on.**
    ///
    /// Both faces in one run, against one fixture: the same directory, the same
    /// stamp, and the only thing that changes is whether the process it names
    /// is still in `/proc`. Without the live half this would pass against a
    /// reader that answered `Gone` unconditionally, which is exactly today's
    /// behaviour and the thing being fixed.
    #[test]
    fn an_owner_that_is_running_is_told_from_one_that_is_not() {
        let fixture = Fixture::new();
        fixture.stamp(4242, 99_000, Some("boot-one"));

        fixture.process(4242, 99_000);
        assert_eq!(
            owner_of(&fixture.work(), &fixture.proc()),
            Owner::Alive(4242),
            "a work directory whose creator is still running is not a leftover"
        );

        std::fs::remove_dir_all(fixture.proc().join("4242")).unwrap();
        assert_eq!(owner_of(&fixture.work(), &fixture.proc()), Owner::Gone);
    }

    /// 🔴 T-NR-41. A pid alone is not an identity.
    ///
    /// The stamped process exited and something else took its number. Reading
    /// that as "the owner is alive" leaks the directory forever; the start time
    /// is what stops a pid from being reused into a permanent refusal.
    #[test]
    fn a_reused_pid_is_not_the_owner() {
        let fixture = Fixture::new();
        fixture.stamp(4242, 99_000, Some("boot-one"));

        fixture.process(4242, 120_000);
        assert_eq!(
            owner_of(&fixture.work(), &fixture.proc()),
            Owner::Gone,
            "a later process with the same pid is not the process that was stamped"
        );

        // Resolution: the same fixture with the recorded start time does say
        // alive, so the line above is the start-time comparison and not an
        // inert reader.
        fixture.process(4242, 99_000);
        assert_eq!(
            owner_of(&fixture.work(), &fixture.proc()),
            Owner::Alive(4242)
        );
    }

    /// T-NR-42. A directory stamped on a previous boot is a leftover whatever
    /// `/proc` says now.
    #[test]
    fn a_stamp_from_another_boot_is_gone_even_if_the_pid_matches() {
        let fixture = Fixture::new();
        fixture.stamp(4242, 99_000, Some("a-previous-boot"));
        fixture.process(4242, 99_000);

        assert_eq!(owner_of(&fixture.work(), &fixture.proc()), Owner::Gone);

        // And the control face: the same pid, start time and directory, stamped
        // on *this* boot, is a live owner. Without it the assertion above would
        // also pass against a reader that ignored the pid entirely.
        fixture.stamp(4242, 99_000, Some("boot-one"));
        assert_eq!(
            owner_of(&fixture.work(), &fixture.proc()),
            Owner::Alive(4242)
        );
    }

    /// 🔴 T-NR-43. Every way of not knowing answers `Unknown`, and `Unknown` is
    /// never `Gone`.
    ///
    /// 🔴 The reasons are asserted verbatim, not merely `matches!`. They are
    /// the only thing that separates the ordinary case — a directory made
    /// before stamps existed, which an operator should ignore — from a stamp or
    /// a `/proc` this process was not allowed to read, which is a host that has
    /// gone blind and where the sweep will now decline everything forever. A
    /// test that accepted any `Unknown` lets those collapse into one, and
    /// mutation showed exactly that: the guards telling them apart survived it.
    #[test]
    fn everything_that_cannot_be_read_is_unknown_rather_than_a_leftover() {
        let fixture = Fixture::new();

        // Nothing was ever written — a directory from a build before the stamp
        // existed, or one whose stamp could not be written.
        assert_eq!(
            owner_of(&fixture.work(), &fixture.proc()),
            Owner::Unknown("no owner stamp")
        );

        // 🔴 A stamp that is there and cannot be read, which is a different
        // fact and has to read differently: a directory in its place stands in
        // for the permission and I/O failures that produce it.
        std::fs::create_dir(stamp_path(&fixture.work())).unwrap();
        assert_eq!(
            owner_of(&fixture.work(), &fixture.proc()),
            Owner::Unknown("owner stamp unreadable"),
            "a stamp that could not be read must not read as one that was never written"
        );
        std::fs::remove_dir(stamp_path(&fixture.work())).unwrap();

        // Written, but not something this build can read.
        std::fs::write(stamp_path(&fixture.work()), b"{ not json").unwrap();
        assert_eq!(
            owner_of(&fixture.work(), &fixture.proc()),
            Owner::Unknown("owner stamp unparseable")
        );

        // A format from another build.
        fixture.write_stamp(&OwnerStamp {
            version: STAMP_VERSION + 1,
            boot_id: Some("boot-one".to_string()),
            pid: 4242,
            starttime: 99_000,
        });
        fixture.process(4242, 99_000);
        assert_eq!(
            owner_of(&fixture.work(), &fixture.proc()),
            Owner::Unknown("owner stamp written in a format this build does not know"),
            "a stamp this build cannot interpret must not be interpreted"
        );

        // The owner's process entry exists and its stat does not.
        fixture.stamp(4242, 99_000, Some("boot-one"));
        std::fs::remove_file(fixture.proc().join("4242").join("stat")).unwrap();
        assert_eq!(
            owner_of(&fixture.work(), &fixture.proc()),
            Owner::Unknown("the owner's process entry has no readable stat")
        );

        // 🔴 ...and its stat being there and unreadable, which reaches the
        // other side of the same guard. Without this the two are told apart by
        // a branch nothing ever takes.
        std::fs::create_dir(fixture.proc().join("4242").join("stat")).unwrap();
        assert_eq!(
            owner_of(&fixture.work(), &fixture.proc()),
            Owner::Unknown("the owner's process entry could not be read")
        );
        std::fs::remove_dir(fixture.proc().join("4242").join("stat")).unwrap();

        // Resolution for all six: the same fixture, made readable, answers a
        // definite thing.
        fixture.process(4242, 99_000);
        assert_eq!(
            owner_of(&fixture.work(), &fixture.proc()),
            Owner::Alive(4242)
        );
    }

    /// T-NR-44. A stamp with no boot id is read on its pid and start time
    /// alone, rather than refused.
    #[test]
    fn a_stamp_without_a_boot_id_is_still_usable() {
        let fixture = Fixture::new();
        fixture.stamp(4242, 99_000, None);
        fixture.process(4242, 99_000);
        assert_eq!(
            owner_of(&fixture.work(), &fixture.proc()),
            Owner::Alive(4242)
        );

        std::fs::remove_dir_all(fixture.proc().join("4242")).unwrap();
        assert_eq!(owner_of(&fixture.work(), &fixture.proc()), Owner::Gone);
    }

    /// 🔴 T-NR-45. **The writer and the reader are the same format.**
    ///
    /// Against the real `/proc` and this real process, because that is the only
    /// way to check the pair end to end: a writer and a reader that agreed with
    /// each other but not with the kernel would make every directory on every
    /// host `Unknown`, and the sweep would go quietly inert without a single
    /// test failing.
    #[test]
    fn what_is_written_reads_back_as_this_live_process() {
        let work = TempDir::new().unwrap();
        stamp_work_dir(work.path()).expect("a stamp must be writable in a fresh directory");

        assert_eq!(
            owner_of(work.path(), Path::new("/proc")),
            Owner::Alive(i32::try_from(std::process::id()).unwrap()),
            "the process that just stamped this directory is running, and is this one"
        );
    }
}
