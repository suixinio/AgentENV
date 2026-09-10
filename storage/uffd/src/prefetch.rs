//! The working-set list one resume records and the next resume of the same
//! image replays through `UffdHandler::prefault`.

use std::path::Path;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrefetchList {
    pub version: u32,
    /// The page size the indices are in; a list for another page size is
    /// not replayed.
    pub page_size: u64,
    /// Page indices (image offset over page size), ascending.
    pub pages: Vec<u64>,
}

impl PrefetchList {
    pub const VERSION: u32 = 1;
    /// A list shorter than this describes a resume that never got going and
    /// is not worth keeping.
    pub const MIN_PAGES: usize = 8;

    pub fn new(page_size: u64, mut pages: Vec<u64>) -> Self {
        pages.sort_unstable();
        pages.dedup();
        Self {
            version: Self::VERSION,
            page_size,
            pages,
        }
    }

    pub fn is_worth_recording(&self) -> bool {
        self.pages.len() >= Self::MIN_PAGES
    }

    /// The pages to replay for a handler serving `page_size` pages; `None`
    /// when the list is for another page size or version.
    pub fn pages_for(&self, page_size: u64) -> Option<&[u64]> {
        (self.version == Self::VERSION && self.page_size == page_size).then_some(&self.pages)
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
        if path.exists() {
            return Ok(false);
        }
        let dir = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let tmp = dir.join(format!(
            ".{}.{}.tmp",
            path.file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("prefetch"),
            std::process::id()
        ));
        let bytes = serde_json::to_vec(self).context("encode the prefetch list")?;
        std::fs::write(&tmp, bytes).with_context(|| format!("write {}", tmp.display()))?;
        if path.exists() {
            let _ = std::fs::remove_file(&tmp);
            return Ok(false);
        }
        std::fs::rename(&tmp, path)
            .with_context(|| format!("rename {} to {}", tmp.display(), path.display()))?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_list_round_trips_sorted_and_deduplicated() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mem_prefetch.json");
        let list = PrefetchList::new(4096, vec![9, 1, 5, 1, 0]);
        assert_eq!(list.pages, vec![0, 1, 5, 9]);
        assert!(list.write_if_absent(&path).unwrap());
        assert_eq!(PrefetchList::read(&path).unwrap(), Some(list.clone()));
        assert_eq!(list.pages_for(4096), Some(&[0u64, 1, 5, 9][..]));
        assert_eq!(list.pages_for(2 << 20), None);
    }

    #[test]
    fn the_first_writer_wins_and_a_missing_list_reads_as_none() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mem_prefetch.json");
        assert_eq!(PrefetchList::read(&path).unwrap(), None);
        let first = PrefetchList::new(4096, (0..16).collect());
        let second = PrefetchList::new(4096, (100..116).collect());
        assert!(first.write_if_absent(&path).unwrap());
        assert!(!second.write_if_absent(&path).unwrap());
        assert_eq!(PrefetchList::read(&path).unwrap(), Some(first));
        assert!(
            std::fs::read_dir(dir.path()).unwrap().count() == 1,
            "no temp file left"
        );
    }

    #[test]
    fn short_lists_are_not_worth_recording() {
        assert!(!PrefetchList::new(4096, vec![1, 2, 3]).is_worth_recording());
        assert!(PrefetchList::new(4096, (0..8).collect()).is_worth_recording());
    }
}
