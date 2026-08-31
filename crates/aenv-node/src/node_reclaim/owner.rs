//! Records which server process created each sandbox work directory.
//!
//! Stamps contain a format version, PID, process start time, and optional boot
//! ID. PID and start time prevent reuse from impersonating a live owner; boot
//! ID settles directories surviving a reboot.
//!
//! Reads are fail-closed: missing, unreadable, unknown-version, or unverifiable
//! stamps yield [`Owner::Unknown`] and are never reclamation licences. Stamp
//! write failure warns but does not fail sandbox creation.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

// Hidden owner-stamp filename shared by the writer and reader.
const STAMP_FILE: &str = ".aenv-owner";

// Unknown stamp versions are never interpreted by this build.
const STAMP_VERSION: u32 = 1;

// Boot identifier relative to `/proc`.
const BOOT_ID: &str = "sys/kernel/random/boot_id";

// Serialized work-directory owner identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct OwnerStamp {
    version: u32,
    #[serde(default)]
    boot_id: Option<String>,
    pid: i32,
    starttime: u64,
}

/// Liveness classification for a stamped owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Owner {
    /// Stamped process identity is still running.
    Alive(i32),
    /// Stamped process is gone or its PID has been reused.
    Gone,
    /// Owner identity cannot be verified; never safe to reclaim.
    Unknown(&'static str),
}

/// Records this process as the owner of a new sandbox work directory.
pub fn stamp_work_dir(work_dir: &Path) -> std::io::Result<()> {
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

    // Never write a fallback PID that could identify another process.
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

/// Classifies the stamped owner against `/proc`.
pub fn owner_of(work_dir: &Path, proc_dir: &Path) -> Owner {
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

    // A different boot proves the stamped process is gone.
    if let (Some(stamped), Some(current)) = (stamp.boot_id.as_deref(), boot_id(proc_dir)) {
        if stamped != current {
            return Owner::Gone;
        }
    }

    match start_time(proc_dir, &stamp.pid.to_string()) {
        Ok(Some(starttime)) if starttime == stamp.starttime => Owner::Alive(stamp.pid),
        Ok(Some(_)) => Owner::Gone,
        Ok(None) => Owner::Gone,
        // An unreadable live entry is not evidence that the owner is gone.
        Err(reason) => Owner::Unknown(reason),
    }
}

fn stamp_path(work_dir: &Path) -> PathBuf {
    work_dir.join(STAMP_FILE)
}

fn boot_id(proc_dir: &Path) -> Option<String> {
    std::fs::read_to_string(proc_dir.join(BOOT_ID))
        .ok()
        .map(|id| id.trim().to_string())
        .filter(|id| !id.is_empty())
}

// Reads process start time while preserving absent versus unreadable entries.
fn start_time(proc_dir: &Path, who: &str) -> Result<Option<u64>, &'static str> {
    let process_dir = proc_dir.join(who);
    let stat = match std::fs::read_to_string(process_dir.join("stat")) {
        Ok(stat) => stat,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            // A present process directory with no stat is indeterminate.
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

/// Test helper that writes the production stamp format.
#[cfg(test)]
pub fn stamp_for_test(work_dir: &Path, pid: i32, starttime: u64, boot_id: Option<&str>) {
    let stamp = OwnerStamp {
        version: STAMP_VERSION,
        boot_id: boot_id.map(str::to_string),
        pid,
        starttime,
    };
    std::fs::write(stamp_path(work_dir), serde_json::to_vec(&stamp).unwrap()).unwrap();
}

/// Owner PID reserved for forged test process trees.
#[cfg(test)]
pub const DEAD_OWNER_PID: i32 = 999_001;

/// Stamps a test work directory as owned by an exited server.
#[cfg(test)]
pub fn stamp_as_leftover(work_dir: &Path) {
    std::fs::write(stamp_path(work_dir), leftover_stamp_bytes()).unwrap();
}

/// Returns the production-format leftover stamp bytes for non-file fixtures.
#[cfg(test)]
pub fn leftover_stamp_bytes() -> Vec<u8> {
    serde_json::to_vec(&OwnerStamp {
        version: STAMP_VERSION,
        boot_id: None,
        pid: DEAD_OWNER_PID,
        starttime: 1,
    })
    .unwrap()
}

/// Returns the production stamp path for hostile test fixtures.
#[cfg(test)]
pub fn stamp_path_of(work_dir: &Path) -> PathBuf {
    stamp_path(work_dir)
}

#[cfg(test)]
mod tests {
    use super::*;

    use tempfile::TempDir;

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

        fn process(&self, pid: i32, starttime: u64) {
            let dir = self.proc().join(pid.to_string());
            std::fs::create_dir_all(&dir).unwrap();
            // Exercise parsing with a bracketed comm containing spaces.
            let mut fields = vec![pid.to_string(), "(a (weird) name)".to_string()];
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

        fixture.process(4242, 99_000);
        assert_eq!(
            owner_of(&fixture.work(), &fixture.proc()),
            Owner::Alive(4242)
        );
    }

    #[test]
    fn a_stamp_from_another_boot_is_gone_even_if_the_pid_matches() {
        let fixture = Fixture::new();
        fixture.stamp(4242, 99_000, Some("a-previous-boot"));
        fixture.process(4242, 99_000);

        assert_eq!(owner_of(&fixture.work(), &fixture.proc()), Owner::Gone);

        fixture.stamp(4242, 99_000, Some("boot-one"));
        assert_eq!(
            owner_of(&fixture.work(), &fixture.proc()),
            Owner::Alive(4242)
        );
    }

    #[test]
    fn everything_that_cannot_be_read_is_unknown_rather_than_a_leftover() {
        let fixture = Fixture::new();

        assert_eq!(
            owner_of(&fixture.work(), &fixture.proc()),
            Owner::Unknown("no owner stamp")
        );

        std::fs::create_dir(stamp_path(&fixture.work())).unwrap();
        assert_eq!(
            owner_of(&fixture.work(), &fixture.proc()),
            Owner::Unknown("owner stamp unreadable"),
            "a stamp that could not be read must not read as one that was never written"
        );
        std::fs::remove_dir(stamp_path(&fixture.work())).unwrap();

        std::fs::write(stamp_path(&fixture.work()), b"{ not json").unwrap();
        assert_eq!(
            owner_of(&fixture.work(), &fixture.proc()),
            Owner::Unknown("owner stamp unparseable")
        );

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

        fixture.stamp(4242, 99_000, Some("boot-one"));
        std::fs::remove_file(fixture.proc().join("4242").join("stat")).unwrap();
        assert_eq!(
            owner_of(&fixture.work(), &fixture.proc()),
            Owner::Unknown("the owner's process entry has no readable stat")
        );

        std::fs::create_dir(fixture.proc().join("4242").join("stat")).unwrap();
        assert_eq!(
            owner_of(&fixture.work(), &fixture.proc()),
            Owner::Unknown("the owner's process entry could not be read")
        );
        std::fs::remove_dir(fixture.proc().join("4242").join("stat")).unwrap();

        fixture.process(4242, 99_000);
        assert_eq!(
            owner_of(&fixture.work(), &fixture.proc()),
            Owner::Alive(4242)
        );
    }

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
