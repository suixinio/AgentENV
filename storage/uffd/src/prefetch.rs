//! The working-set list one resume records and the next resume of the same
//! image replays through `UffdHandler::prefault`.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrefetchList {
    pub version: u32,
    /// The page size the indices are in; a list for another page size is
    /// not replayed.
    pub page_size: u64,
    /// The byte size of the image the list was recorded against. A restack
    /// or a different VM size changes it, and the list is not replayed.
    #[serde(default)]
    pub image_size: u64,
    /// Page indices (image offset over page size), ascending.
    pub pages: Vec<u64>,
}

impl PrefetchList {
    pub const VERSION: u32 = 2;
    /// A list shorter than this describes a resume that never got going and
    /// is not worth keeping.
    pub const MIN_PAGES: usize = 8;
    /// A list longer than this (4 GiB of 4 KiB pages) is not a working set
    /// to replay ahead of the guest, and its file would be read whole on
    /// every serve.
    pub const MAX_PAGES: usize = 1 << 20;

    pub fn new(page_size: u64, image_size: u64, mut pages: Vec<u64>) -> Self {
        pages.sort_unstable();
        pages.dedup();
        Self {
            version: Self::VERSION,
            page_size,
            image_size,
            pages,
        }
    }

    pub fn is_worth_recording(&self) -> bool {
        (Self::MIN_PAGES..=Self::MAX_PAGES).contains(&self.pages.len())
    }

    /// The pages to replay for a handler serving `page_size` pages of an
    /// image of `image_size` bytes; `None` when the list was recorded for
    /// another page size, image size or format version.
    pub fn pages_for(&self, page_size: u64, image_size: u64) -> Option<&[u64]> {
        (self.version == Self::VERSION
            && self.page_size == page_size
            && self.image_size == image_size
            && self.pages.len() <= Self::MAX_PAGES)
            .then_some(&self.pages)
    }

    /// `Ok(None)` when there is no list at `path`.
    pub fn read(path: &Path) -> Result<Option<Self>> {
        let bytes = match std::fs::read(path) {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(err).with_context(|| format!("read {}", path.display())),
        };
        let list = serde_json::from_slice(&bytes)
            .with_context(|| format!("decode the prefetch list {}", path.display()))?;
        Ok(Some(list))
    }

    /// Writes the list atomically unless one exists; the first resume wins
    /// and later ones do not churn it. Returns whether it was written.
    pub fn write_if_absent(&self, path: &Path) -> Result<bool> {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        if path.exists() {
            return Ok(false);
        }
        let dir = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        // Several servers in one process may record the same path at once;
        // each writes its own temp file.
        let tmp = dir.join(format!(
            ".{}.{}.{}.tmp",
            path.file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("prefetch"),
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        let bytes = serde_json::to_vec(self).context("encode the prefetch list")?;
        std::fs::write(&tmp, bytes).with_context(|| format!("write {}", tmp.display()))?;
        // Linking the final name is the race arbiter: the loser's temp file
        // is removed, the winner's content stays.
        let written = match std::fs::hard_link(&tmp, path) {
            Ok(()) => true,
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => false,
            Err(err) => {
                let _ = std::fs::remove_file(&tmp);
                return Err(err)
                    .with_context(|| format!("link {} to {}", tmp.display(), path.display()));
            }
        };
        let _ = std::fs::remove_file(&tmp);
        Ok(written)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const IMAGE: u64 = 64 << 20;

    #[test]
    fn a_list_round_trips_sorted_and_deduplicated() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mem_prefetch.json");
        let list = PrefetchList::new(4096, IMAGE, vec![9, 1, 5, 1, 0]);
        assert_eq!(list.pages, vec![0, 1, 5, 9]);
        assert!(list.write_if_absent(&path).unwrap());
        assert_eq!(PrefetchList::read(&path).unwrap(), Some(list.clone()));
        assert_eq!(list.pages_for(4096, IMAGE), Some(&[0u64, 1, 5, 9][..]));
        assert_eq!(list.pages_for(2 << 20, IMAGE), None);
        assert_eq!(list.pages_for(4096, IMAGE + 4096), None);
    }

    #[test]
    fn the_first_writer_wins_and_a_missing_list_reads_as_none() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mem_prefetch.json");
        assert_eq!(PrefetchList::read(&path).unwrap(), None);
        let first = PrefetchList::new(4096, IMAGE, (0..16).collect());
        let second = PrefetchList::new(4096, IMAGE, (100..116).collect());
        assert!(first.write_if_absent(&path).unwrap());
        assert!(!second.write_if_absent(&path).unwrap());
        assert_eq!(PrefetchList::read(&path).unwrap(), Some(first));
        assert!(
            std::fs::read_dir(dir.path()).unwrap().count() == 1,
            "no temp file left"
        );
    }

    #[test]
    fn concurrent_writers_leave_one_list_and_no_temp_files() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mem_prefetch.json");
        let writers: Vec<_> = (0..8u64)
            .map(|w| {
                let path = path.clone();
                std::thread::spawn(move || {
                    PrefetchList::new(4096, IMAGE, (w * 100..w * 100 + 16).collect())
                        .write_if_absent(&path)
                        .unwrap()
                })
            })
            .collect();
        let wins = writers
            .into_iter()
            .map(|w| w.join().unwrap())
            .filter(|won| *won)
            .count();
        assert_eq!(wins, 1);
        let list = PrefetchList::read(&path).unwrap().unwrap();
        assert_eq!(list.pages.len(), 16);
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn lists_outside_the_size_band_are_not_worth_recording() {
        assert!(!PrefetchList::new(4096, IMAGE, vec![1, 2, 3]).is_worth_recording());
        assert!(PrefetchList::new(4096, IMAGE, (0..8).collect()).is_worth_recording());
        let huge = PrefetchList::new(
            4096,
            IMAGE,
            (0..PrefetchList::MAX_PAGES as u64 + 1).collect(),
        );
        assert!(!huge.is_worth_recording());
        assert_eq!(huge.pages_for(4096, IMAGE), None);
    }

    #[test]
    fn a_version_one_list_is_not_replayed() {
        let json = r#"{"version":1,"page_size":4096,"pages":[0,1,2,3,4,5,6,7]}"#;
        let list: PrefetchList = serde_json::from_str(json).unwrap();
        assert_eq!(list.pages_for(4096, 0), None);
    }
}
