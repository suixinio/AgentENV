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
use super::{is_within, ReclaimCounts, ReclaimPaths};

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
            }
        }

        fn dir(&self, name: &str) -> PathBuf {
            let path = self.work.path().join(name);
            std::fs::create_dir_all(&path).unwrap();
            path
        }
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
    #[test]
    fn a_missing_work_base_is_not_a_failure() {
        let fixture = Fixture::new();
        let paths = ReclaimPaths {
            work_base: fixture.work.path().join("never-created"),
            ..fixture.paths()
        };
        assert!(plan(&paths, true).is_empty());
        assert_eq!(reclaim(&paths, true), ReclaimCounts::default());
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
