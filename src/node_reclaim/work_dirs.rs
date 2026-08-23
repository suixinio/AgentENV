//! Sandbox work directories a previous process on this machine left behind.
//!
//! Each one is a `TempDir` in the live process, so they exist on disk only
//! after a crash — an orderly shutdown removes its own. What is in them is a
//! Firecracker API socket, a rootfs drive path, logs, and whatever the VMM
//! wrote: all of it scoped to a VM that no longer exists.
//!
//! Same split as [`super::firecracker`]: [`plan`] decides and explains, and the
//! deleting is a handful of lines underneath it.

use std::ffi::OsStr;
use std::path::PathBuf;

use tracing::{debug, info, warn};

use super::firecracker::WORK_DIR_PREFIX;
use super::{is_within, owner, ReclaimCounts, ReclaimPaths};

/// One directory under the work base, and what the sweep decided about it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkDirPlan {
    pub path: PathBuf,
    pub verdict: Verdict,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Verdict {
    /// One of ours, from a process that is gone. Delete it.
    Remove,
    /// Left alone, and why. Never a silent skip: a directory that is not
    /// removed and not explained is one nobody will ever look at again.
    Keep(&'static str),
    /// 🔴 Whose it is could not be worked out, so it is left alone *and*
    /// counted as a failure.
    ///
    /// Kept apart from [`Verdict::Keep`] for the reason [`ReclaimCounts`] gives
    /// for its third counter: "I decided not to" and "I could not tell" produce
    /// the same action and must not produce the same reading. Every directory
    /// created before the owner stamp existed lands here, which is a bounded,
    /// loud, one-off leak per host — and is the direction to be wrong in.
    Undetermined(&'static str),
}

/// Decides what to do with each entry directly under the work base.
///
/// `every_firecracker_accounted_for` is the result of the process sweep that
/// ran first. 🔴 When it is false, nothing is deleted at all — see
/// [`reclaim`].
pub(super) fn plan(
    paths: &ReclaimPaths,
    every_firecracker_accounted_for: bool,
) -> Vec<WorkDirPlan> {
    let entries = match std::fs::read_dir(&paths.work_base) {
        Ok(entries) => entries,
        // A work base that does not exist is a cold host, not a problem.
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
            // Not a candidate and not worth a word: the work base may be the
            // system temp directory, which is full of other people's things.
            continue;
        }

        // 🔴 The refusal that matters, and it is checked in both directions.
        //
        // `[orchestrator].persisted_sandbox_store_path` holds the artifacts of
        // every paused sandbox on this node, and `Orchestrator::new` reads them
        // back moments after this sweep finishes. Deleting them turns a restart
        // into permanent, silent data loss for every sandbox parked here — and
        // nothing about the name of a directory stops an operator from
        // configuring the store *inside* the work base, or the work base
        // *inside* the store. Both are refused.
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

        // 🔴 Asked before the blanket veto below, so the reason recorded is
        // the specific one. A directory whose creating server is still up is
        // not "unaccounted for": it is somebody's live sandbox, and deleting
        // the files under a running VMM leaves it running and broken — the
        // exact failure the veto exists to prevent, arriving through the half
        // of the sweep that has no process to look at.
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

/// Deletes the work directories [`plan`] named, and counts the ones it did not.
///
/// 🔴 `every_firecracker_accounted_for` is a veto, not a hint. The process
/// sweep runs first precisely so that these directories belong to nothing; if
/// even one Firecracker on the host could not be classified or would not die,
/// that premise is not established, and deleting the files under a VMM that is
/// still running leaves it running and broken — strictly worse than leaving
/// both alone. Leaking a few directories costs disk. This costs a VM.
pub(super) fn reclaim(
    paths: &ReclaimPaths,
    every_firecracker_accounted_for: bool,
) -> ReclaimCounts {
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

        /// A directory under the work base, stamped as belonging to a server
        /// that has exited — which is what a leftover is.
        fn dir(&self, name: &str) -> PathBuf {
            let path = self.dir_without_a_stamp(name);
            super::owner::stamp_as_leftover(&path);
            path
        }

        /// A directory whose server process is still running, laid out in this
        /// fixture's forged `/proc` so the stamp can be checked against it.
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

        /// A directory with no owner stamp: one made by a build before the
        /// stamp existed, or one whose stamp could not be written.
        fn dir_without_a_stamp(&self, name: &str) -> PathBuf {
            let path = self.work.path().join(name);
            std::fs::create_dir_all(&path).unwrap();
            path
        }
    }

    /// 🔴 T-NR-48. A directory whose server process is still running is kept,
    /// and kept for that reason rather than by the blanket veto.
    ///
    /// The file half has no process to look at — on the DaemonSet that is the
    /// only half that ever fires — so without the stamp its only protection is
    /// a veto that a clean process sweep switches off. A live server's work
    /// directory deleted out from under it leaves its VMM running and broken,
    /// which is the exact failure the veto exists to prevent.
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

    /// 🔴 T-NR-49. A directory with no stamp is kept *and* counted as a
    /// failure, because nothing can say whose it is.
    #[test]
    fn a_directory_with_no_stamp_is_a_failure_rather_than_a_decision() {
        let fixture = Fixture::new();
        let unstamped = fixture.dir_without_a_stamp("agentenv-fc-unstamped");
        let leftover = fixture.dir("agentenv-fc-dead");

        let counts = reclaim(&fixture.paths(), true);
        assert!(unstamped.exists());
        assert_eq!(counts.failed, 1);
        // 🔴 And not counted as `left_alone`: "I could not tell" and "it is not
        // mine" must not read the same, or a sweep that has gone blind reports
        // the same clean numbers as one with nothing to do.
        assert_eq!(counts.left_alone, 0);
        assert_eq!(counts.reclaimed, 1, "the probe has resolution");
        assert!(!leftover.exists());
    }

    /// 🔴 T-NR-20. **The refusal that keeps a restart from being data loss.**
    ///
    /// Written as the hostile configuration rather than the ordinary one:
    /// the paused-sandbox store placed *inside* the work base under a name
    /// this sweep would otherwise recognise as its own. Nothing forbids that
    /// configuration, the prefix rule alone happily deletes it, and what it
    /// deletes is the only durable copy of every sandbox paused on this node.
    ///
    /// Both faces, in one test: the store survives, and an ordinary work
    /// directory sitting beside it does not — otherwise a sweep that deletes
    /// nothing would pass.
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

    /// 🔴 T-NR-21. The refusal holds in the other direction too: a work
    /// directory that *contains* the store.
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

    /// 🔴 T-NR-22. Only the sweep's own directories are touched.
    ///
    /// The work base is `[firecracker].work_dir`, which is **optional**: unset,
    /// it is the system temp directory, shared with everything else on the
    /// machine.
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

    /// 🔴 T-NR-23. A Firecracker sweep that could not account for everything
    /// vetoes the whole file sweep.
    ///
    /// The premise these directories belong to nothing is established by the
    /// process sweep. Without it, the directory being deleted may be the one a
    /// live VMM is running in — and a VMM whose files vanish underneath it is
    /// worse than a directory nobody deleted.
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

    /// T-NR-24. A work base that is not there is a cold host, not a fault.
    ///
    /// 🔴 The empty plan is only evidence next to a non-empty one. A `plan`
    /// that returned `vec![]` for everything would pass the first two
    /// assertions and go on passing them while reclaiming nothing, anywhere,
    /// forever.
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

    /// T-NR-25. A file (not a directory) carrying the prefix is removed too —
    /// leftovers are not all directories, and `remove_dir_all` on a file is an
    /// error rather than a silent skip, so it is counted.
    #[test]
    fn a_leftover_that_is_not_a_directory_is_reported_rather_than_ignored() {
        let fixture = Fixture::new();
        std::fs::write(fixture.work.path().join("agentenv-fc-stray"), "x").unwrap();

        let counts = reclaim(&fixture.paths(), true);
        assert_eq!(counts.reclaimed + counts.failed, 1);
        assert_eq!(counts.left_alone, 0);
    }
}
