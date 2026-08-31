//! Plans and deletes abandoned sandbox work directories.
//! Deletion remains separate from ownership planning and is vetoed by any
//! unaccounted Firecracker process.

use std::ffi::OsStr;
use std::path::PathBuf;

use tracing::{debug, info, warn};

use super::firecracker::WORK_DIR_PREFIX;
use super::{is_within, owner, ReclaimCounts, ReclaimPaths};

/// Planned verdict for one work directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkDirPlan {
    pub path: PathBuf,
    pub verdict: Verdict,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// Delete a stamped directory whose owner is gone.
    Remove,
    /// Keep a directory for the recorded reason.
    Keep(&'static str),
    /// Ownership cannot be established; retain and count as failure.
    Undetermined(&'static str),
}

/// Plans candidate directories; a false process-sweep result vetoes deletion.
pub fn plan(paths: &ReclaimPaths, every_firecracker_accounted_for: bool) -> Vec<WorkDirPlan> {
    let entries = match std::fs::read_dir(&paths.work_base) {
        Ok(entries) => entries,
        Err(_) => return Vec::new(),
    };

    let persisted = &paths.persisted_sandbox_store;
    let mut plans = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        let is_ours = path
            .file_name()
            .and_then(OsStr::to_str)
            .is_some_and(|name| name.starts_with(WORK_DIR_PREFIX));
        if !is_ours {
            // The work base may be the system temporary directory.
            continue;
        }

        // Never touch a directory that contains or is contained by paused storage.
        if is_within(&path, persisted) || is_within(persisted, &path) {
            warn!(
                target: "agentenv",
                path = %path.display(),
                persisted_sandbox_store = %persisted.display(),
                "refusing to reclaim a directory that overlaps the paused-sandbox store; \
                 these are the only durable copies of the sandboxes parked on this node"
            );
            plans.push(WorkDirPlan {
                path,
                verdict: Verdict::Keep("overlaps the paused-sandbox store"),
            });
            continue;
        }

        // Record a live stamped owner before applying the blanket process veto.
        let verdict = match owner::owner_of(&path, &paths.proc_dir) {
            owner::Owner::Alive(pid) => {
                warn!(
                    target: "agentenv",
                    path = %path.display(),
                    owner_pid = pid,
                    "keeping a sandbox work directory whose server process is still running"
                );
                Some(Verdict::Keep("the server that created it is still running"))
            }
            owner::Owner::Unknown(reason) => Some(Verdict::Undetermined(reason)),
            owner::Owner::Gone => None,
        };
        if let Some(verdict) = verdict {
            plans.push(WorkDirPlan { path, verdict });
            continue;
        }

        if !every_firecracker_accounted_for {
            plans.push(WorkDirPlan {
                path,
                verdict: Verdict::Keep("a Firecracker on this host could not be accounted for"),
            });
            continue;
        }

        plans.push(WorkDirPlan {
            path,
            verdict: Verdict::Remove,
        });
    }

    plans.sort_by(|left, right| left.path.cmp(&right.path));
    plans
}

/// Deletes only planned directories when every Firecracker was accounted for.
pub fn reclaim(paths: &ReclaimPaths, every_firecracker_accounted_for: bool) -> ReclaimCounts {
    let plans = plan(paths, every_firecracker_accounted_for);
    if !plans.is_empty() && !every_firecracker_accounted_for {
        warn!(
            target: "agentenv",
            directories = plans.len(),
            "leaving every sandbox work directory in place: the Firecracker sweep could not \
             account for everything on this host, so it is not known that these belong to nothing"
        );
    }

    let mut counts = ReclaimCounts::default();
    for plan in plans {
        match plan.verdict {
            Verdict::Remove => match std::fs::remove_dir_all(&plan.path) {
                Ok(()) => {
                    info!(target: "agentenv", path = %plan.path.display(), "reclaimed a leftover sandbox work directory");
                    counts.reclaimed += 1;
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    warn!(target: "agentenv", path = %plan.path.display(), %error, "cannot remove a leftover sandbox work directory");
                    counts.failed += 1;
                }
            },
            Verdict::Keep(reason) => {
                debug!(target: "agentenv", path = %plan.path.display(), reason, "keeping a directory under the work base");
                counts.left_alone += 1;
            }
            Verdict::Undetermined(reason) => {
                warn!(
                    target: "agentenv",
                    path = %plan.path.display(),
                    reason,
                    "cannot tell which process owns a sandbox work directory; leaving it in \
                     place. Nothing will free it until something can say whose it is"
                );
                counts.failed += 1;
            }
        }
    }

    counts
}

#[cfg(test)]
mod tests {
    use super::*;

    use tempfile::TempDir;

    use std::path::Path;

    use tracing::Level;

    use crate::logging::capture::Recorder;

    struct Fixture {
        work: TempDir,
        persisted: TempDir,
        proc: TempDir,
    }

    impl Fixture {
        fn new() -> Self {
            Self {
                work: TempDir::new().unwrap(),
                persisted: TempDir::new().unwrap(),
                proc: TempDir::new().unwrap(),
            }
        }

        fn paths(&self) -> ReclaimPaths {
            ReclaimPaths {
                work_base: self.work.path().to_path_buf(),
                proc_dir: self.proc.path().to_path_buf(),
                persisted_sandbox_store: self.persisted.path().to_path_buf(),
                // Not exercised here: these suites call `plan`/`reclaim`
                // directly, below the premise check `sweep` makes.
                server_exe: None,
                ublk_daemon_socket: PathBuf::from("/nonexistent/ublk.sock"),
            }
        }

        fn dir(&self, name: &str) -> PathBuf {
            let path = self.dir_without_a_stamp(name);
            super::owner::stamp_as_leftover(&path);
            path
        }

        fn dir_of_a_live_server(&self, name: &str, server_pid: i32) -> PathBuf {
            let path = self.dir_without_a_stamp(name);
            super::owner::stamp_for_test(&path, server_pid, 77_000, None);
            let process = self.proc.path().join(server_pid.to_string());
            std::fs::create_dir_all(&process).unwrap();
            std::fs::write(
                process.join("stat"),
                format!(
                    "{server_pid} (server) S 1 {server_pid} {server_pid} 0 -1 0 0 0 0 0 0 0 0 0 \
                     20 0 1 0 77000"
                ),
            )
            .unwrap();
            path
        }

        fn dir_without_a_stamp(&self, name: &str) -> PathBuf {
            let path = self.work.path().join(name);
            std::fs::create_dir_all(&path).unwrap();
            path
        }

        // A FIFO synchronizes mutation after planning but before deletion.
        fn dir_that_changes_while_it_is_being_decided(
            &self,
            name: &str,
            change: WhileTheSweepIsDeciding,
        ) -> (PathBuf, std::thread::JoinHandle<Result<(), String>>) {
            let path = self.dir_without_a_stamp(name);
            let stamp = super::owner::stamp_path_of(&path);
            nix::unistd::mkfifo(
                &stamp,
                nix::sys::stat::Mode::S_IRUSR | nix::sys::stat::Mode::S_IWUSR,
            )
            .unwrap();

            let directory = path.clone();
            let helper = std::thread::spawn(move || {
                answer_the_stamp_then_change(&stamp, &directory, change)
            });
            (path, helper)
        }
    }

    #[derive(Clone, Copy)]
    enum WhileTheSweepIsDeciding {
        ItDisappears,
        ItBecomesAFile,
    }

    fn answer_the_stamp_then_change(
        stamp: &Path,
        directory: &Path,
        change: WhileTheSweepIsDeciding,
    ) -> Result<(), String> {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        let mut writer = loop {
            match std::fs::OpenOptions::new()
                .write(true)
                .custom_flags(libc::O_NONBLOCK)
                .open(stamp)
            {
                Ok(file) => break file,
                Err(error) if std::time::Instant::now() >= deadline => {
                    // Nothing ever read it. Open both ends so that a reader
                    // arriving late is released rather than left hanging, and
                    // report it: a test that quietly stopped exercising this is
                    // worse than one that fails.
                    let _ = std::fs::OpenOptions::new()
                        .read(true)
                        .write(true)
                        .open(stamp);
                    return Err(format!("nothing read the owner stamp within 30s: {error}"));
                }
                Err(_) => std::thread::sleep(std::time::Duration::from_millis(1)),
            }
        };

        writer
            .write_all(&super::owner::leftover_stamp_bytes())
            .map_err(|error| format!("cannot write the owner stamp: {error}"))?;

        std::fs::remove_dir_all(directory)
            .map_err(|error| format!("cannot take the directory away: {error}"))?;
        if matches!(change, WhileTheSweepIsDeciding::ItBecomesAFile) {
            std::fs::write(directory, "not a directory any more")
                .map_err(|error| format!("cannot put a file in its place: {error}"))?;
        }
        Ok(())
    }

    #[test]
    fn a_directory_whose_server_is_still_running_is_kept() {
        const SERVER: i32 = 7300;

        let fixture = Fixture::new();
        let live = fixture.dir_of_a_live_server("agentenv-fc-live", SERVER);
        let leftover = fixture.dir("agentenv-fc-dead");

        let counts = reclaim(&fixture.paths(), true);
        assert!(live.exists(), "a live server's work directory was deleted");
        // Both faces, one run: the leftover beside it went, so the line above
        // is the stamp and not a sweep that deletes nothing.
        assert!(!leftover.exists());
        assert_eq!(counts.reclaimed, 1);
        assert_eq!(counts.left_alone, 1);
        assert_eq!(
            counts.failed, 0,
            "a live owner is a decision, not a failure"
        );

        assert_eq!(
            plan(&fixture.paths(), true)
                .into_iter()
                .find(|entry| entry.path == live)
                .map(|entry| entry.verdict),
            Some(Verdict::Keep("the server that created it is still running")),
            "kept for the specific reason, not by the blanket veto"
        );
    }

    #[test]
    fn a_directory_with_no_stamp_is_a_failure_rather_than_a_decision() {
        let fixture = Fixture::new();
        let unstamped = fixture.dir_without_a_stamp("agentenv-fc-unstamped");
        let leftover = fixture.dir("agentenv-fc-dead");

        let counts = reclaim(&fixture.paths(), true);
        assert!(unstamped.exists());
        assert_eq!(counts.failed, 1);
        assert_eq!(counts.left_alone, 0);
        assert_eq!(counts.reclaimed, 1, "the probe has resolution");
        assert!(!leftover.exists());
    }

    #[test]
    fn the_paused_sandbox_store_is_never_reclaimed_even_when_it_looks_like_a_work_directory() {
        let fixture = Fixture::new();
        let store = fixture.dir("agentenv-fc-persisted");
        std::fs::write(store.join("record.json"), "a paused sandbox").unwrap();
        let ordinary = fixture.dir("agentenv-fc-A1b2C3");

        let paths = ReclaimPaths {
            persisted_sandbox_store: store.clone(),
            ..fixture.paths()
        };

        let counts = reclaim(&paths, true);
        assert!(
            store.join("record.json").exists(),
            "the paused-sandbox store must survive the sweep"
        );
        assert!(
            !ordinary.exists(),
            "the probe has resolution: an ordinary work directory beside it was removed"
        );
        assert_eq!(
            counts,
            ReclaimCounts {
                reclaimed: 1,
                left_alone: 1,
                failed: 0,
            }
        );
    }

    #[test]
    fn a_work_directory_containing_the_store_is_never_reclaimed() {
        let fixture = Fixture::new();
        let outer = fixture.dir("agentenv-fc-Outer");
        let store = outer.join("persisted-sandboxes");
        std::fs::create_dir_all(&store).unwrap();
        std::fs::write(store.join("record.json"), "a paused sandbox").unwrap();

        let paths = ReclaimPaths {
            persisted_sandbox_store: store.clone(),
            ..fixture.paths()
        };

        assert_eq!(
            reclaim(&paths, true),
            ReclaimCounts {
                reclaimed: 0,
                left_alone: 1,
                failed: 0,
            }
        );
        assert!(store.join("record.json").exists());
    }

    #[test]
    fn nothing_without_the_prefix_is_touched() {
        let fixture = Fixture::new();
        let strangers = [
            fixture.dir("systemd-private-abc"),
            fixture.dir("agentenv-something-else"),
            fixture.dir(".X11-unix"),
        ];
        let ours = fixture.dir("agentenv-fc-Ours");

        let counts = reclaim(&fixture.paths(), true);
        for stranger in &strangers {
            assert!(stranger.exists(), "{} was deleted", stranger.display());
        }
        assert!(!ours.exists());
        assert_eq!(
            counts,
            ReclaimCounts {
                reclaimed: 1,
                left_alone: 0,
                failed: 0,
            },
            "a directory that is not a candidate is not an examined one either"
        );
    }

    #[test]
    fn an_unaccounted_firecracker_stops_every_deletion() {
        let fixture = Fixture::new();
        let first = fixture.dir("agentenv-fc-A1");
        let second = fixture.dir("agentenv-fc-B2");

        let counts = reclaim(&fixture.paths(), false);
        assert!(first.exists() && second.exists());
        assert_eq!(
            counts,
            ReclaimCounts {
                reclaimed: 0,
                left_alone: 2,
                failed: 0,
            },
            "the directories must be reported as deliberately kept, not as absent"
        );

        // ...and the same fixture with the premise established deletes both, so
        // the assertion above is about the veto.
        assert_eq!(
            reclaim(&fixture.paths(), true),
            ReclaimCounts {
                reclaimed: 2,
                left_alone: 0,
                failed: 0,
            }
        );
    }

    #[test]
    fn a_missing_work_base_is_not_a_failure() {
        let fixture = Fixture::new();
        let paths = ReclaimPaths {
            work_base: fixture.work.path().join("never-created"),
            ..fixture.paths()
        };
        assert!(plan(&paths, true).is_empty());
        assert_eq!(reclaim(&paths, true), ReclaimCounts::default());

        // The same fixture with the directory there, and something in it.
        fixture.dir("agentenv-fc-A1");
        assert_eq!(plan(&fixture.paths(), true).len(), 1);
        assert_eq!(reclaim(&fixture.paths(), true).reclaimed, 1);
    }

    #[test]
    fn a_leftover_that_is_not_a_directory_is_reported_rather_than_ignored() {
        let fixture = Fixture::new();
        let stray = fixture.work.path().join("agentenv-fc-stray");
        std::fs::write(&stray, "x").unwrap();
        // The non-empty half: an ordinary leftover beside it, so the numbers
        // below are a reading of the stray file and not of an inert sweep.
        let leftover = fixture.dir("agentenv-fc-dead");

        let counts = reclaim(&fixture.paths(), true);
        assert_eq!(
            counts,
            ReclaimCounts {
                reclaimed: 1,
                left_alone: 0,
                failed: 1,
            },
            "something that cannot be classified is a failure, never a decision"
        );
        assert!(stray.exists(), "and it is left where it is");
        assert!(!leftover.exists());
    }

    #[test]
    fn the_veto_is_announced_only_when_it_holds_something_back() {
        const ANNOUNCEMENT: &str = "leaving every sandbox work directory in place";

        // Candidates, and no premise: the veto bites, and says so.
        let held_back = Fixture::new();
        held_back.dir("agentenv-fc-A1");
        let announced = Recorder::default();
        let guard = announced.install();
        let counts = reclaim(&held_back.paths(), false);
        drop(guard);
        assert_eq!(counts.left_alone, 1, "there was something to hold back");
        assert!(
            announced.saw(Level::WARN, ANNOUNCEMENT),
            "the veto went unannounced: {:?}",
            announced.events()
        );

        let swept = Fixture::new();
        swept.dir("agentenv-fc-A1");
        let unvetoed = Recorder::default();
        let guard = unvetoed.install();
        let counts = reclaim(&swept.paths(), true);
        drop(guard);
        assert_eq!(counts.reclaimed, 1, "and this one did sweep");
        assert!(
            !unvetoed.saw(Level::WARN, ANNOUNCEMENT),
            "a sweep that reclaimed everything announced a veto: {:?}",
            unvetoed.events()
        );

        // ...and the first operand on its own: no premise, but nothing under
        // the work base either, so there is nothing to hold back and nothing
        // to say.
        let empty = Fixture::new();
        let nothing_held = Recorder::default();
        let guard = nothing_held.install();
        let counts = reclaim(&empty.paths(), false);
        drop(guard);
        assert_eq!(counts, ReclaimCounts::default());
        assert!(
            !nothing_held.saw(Level::WARN, ANNOUNCEMENT),
            "a veto over nothing was announced as if it held something: {:?}",
            nothing_held.events()
        );
    }

    #[test]
    fn a_directory_that_vanished_before_the_delete_is_not_a_failure() {
        let fixture = Fixture::new();
        // The non-empty half: an ordinary leftover that goes through normally.
        let ordinary = fixture.dir("agentenv-fc-Ordinary");
        let (vanishing, helper) = fixture.dir_that_changes_while_it_is_being_decided(
            "agentenv-fc-Vanishing",
            WhileTheSweepIsDeciding::ItDisappears,
        );

        let counts = reclaim(&fixture.paths(), true);
        helper
            .join()
            .expect("the helper thread did not panic")
            .expect("the helper thread reached the directory in time");

        assert!(!ordinary.exists());
        assert!(!vanishing.exists());
        assert_eq!(
            counts,
            ReclaimCounts {
                reclaimed: 1,
                left_alone: 0,
                failed: 0,
            },
            "a directory that was already gone is not one that could not be removed"
        );
    }

    #[test]
    fn a_delete_that_fails_for_any_other_reason_is_counted() {
        let fixture = Fixture::new();
        let ordinary = fixture.dir("agentenv-fc-Ordinary");
        let (changed, helper) = fixture.dir_that_changes_while_it_is_being_decided(
            "agentenv-fc-Changed",
            WhileTheSweepIsDeciding::ItBecomesAFile,
        );

        let counts = reclaim(&fixture.paths(), true);
        helper
            .join()
            .expect("the helper thread did not panic")
            .expect("the helper thread reached the directory in time");

        assert!(!ordinary.exists());
        assert!(changed.exists(), "what replaced it is still there");
        assert_eq!(
            counts,
            ReclaimCounts {
                reclaimed: 1,
                left_alone: 0,
                failed: 1,
            },
            "a delete that did not work must not read as one that had nothing to do"
        );
    }
}
