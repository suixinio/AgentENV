//! The working-set list the uffd memory backend records on the first resume
//! of a memory image and replays on every later one. It lives on the node
//! under `{home_path}/mem-prefetch/` and travels with the image: a node that
//! has none downloads the repository's copy, and a node whose copy is not yet
//! in the repository uploads it, so the list a fleet replays is the first one
//! any node recorded.
//!
//! One list serves a whole lineage. The indices are image offsets and a
//! resume never resizes guest memory, so the set a template's first resume
//! faulted is the set every pause row descended from it needs.

use std::future::Future;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::snapshot::ManagedLayer;

/// The bottom memory layer names the lineage: a template and every pause row
/// descended from it stack on the same one. `None` for an image with no
/// managed layers, which has nothing to key a list by.
pub fn lineage_key(memory_layers: &[ManagedLayer]) -> Option<&str> {
    memory_layers.first().map(|layer| layer.digest.as_str())
}

pub fn local_path(home_path: &Path, lineage_key: &str) -> PathBuf {
    let name: String = lineage_key
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect();
    home_path.join("mem-prefetch").join(format!("{name}.json"))
}

/// Set once the local list is known to be in the repository, so a resume
/// does not ask the repository again.
fn uploaded_marker(local: &Path) -> PathBuf {
    let mut marker = local.as_os_str().to_owned();
    marker.push(".uploaded");
    PathBuf::from(marker)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncOutcome {
    /// Neither side has a list yet; the daemon records one at the end of
    /// this resume.
    Absent,
    /// The repository's list is now local.
    Downloaded,
    /// The local list is now in the repository.
    Uploaded,
    /// Both sides had it already.
    InSync,
}

/// Brings the local list and the repository's copy together before a resume.
/// `download` writes the repository's list to the given path and reports
/// whether one existed; `remote_exists` and `upload` are the repository's
/// other two verbs.
pub async fn sync<D, DF, E, EF, U, UF>(
    local: &Path,
    download: D,
    remote_exists: E,
    upload: U,
) -> Result<SyncOutcome>
where
    D: FnOnce(PathBuf) -> DF,
    DF: Future<Output = Result<bool>>,
    E: FnOnce() -> EF,
    EF: Future<Output = Result<bool>>,
    U: FnOnce(PathBuf) -> UF,
    UF: Future<Output = Result<()>>,
{
    let marker = uploaded_marker(local);
    if !local.exists() {
        if let Some(dir) = local.parent() {
            tokio::fs::create_dir_all(dir)
                .await
                .with_context(|| format!("create {}", dir.display()))?;
        }
        let mut part = local.as_os_str().to_owned();
        part.push(".part");
        let part = PathBuf::from(part);
        let found = download(part.clone()).await;
        match found {
            Ok(true) => {
                tokio::fs::rename(&part, local)
                    .await
                    .with_context(|| format!("move {} into place", part.display()))?;
                touch(&marker).await?;
                Ok(SyncOutcome::Downloaded)
            }
            Ok(false) => {
                let _ = tokio::fs::remove_file(&part).await;
                Ok(SyncOutcome::Absent)
            }
            Err(err) => {
                let _ = tokio::fs::remove_file(&part).await;
                Err(err)
            }
        }
    } else if marker.exists() {
        Ok(SyncOutcome::InSync)
    } else if remote_exists().await? {
        touch(&marker).await?;
        Ok(SyncOutcome::InSync)
    } else {
        upload(local.to_path_buf()).await?;
        touch(&marker).await?;
        Ok(SyncOutcome::Uploaded)
    }
}

async fn touch(path: &Path) -> Result<()> {
    tokio::fs::write(path, b"")
        .await
        .with_context(|| format!("write {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    fn not_called<T>() -> impl FnOnce(PathBuf) -> std::future::Ready<Result<T>> {
        |_| panic!("not expected to be called")
    }

    fn layer(digest: &str) -> ManagedLayer {
        ManagedLayer {
            digest: digest.to_string(),
            size: 1,
            uuid: None,
        }
    }

    #[test]
    fn the_lineage_is_the_bottom_memory_layer() {
        assert_eq!(lineage_key(&[]), None);
        assert_eq!(
            lineage_key(&[layer("sha256:base"), layer("sha256:pause")]),
            Some("sha256:base")
        );
    }

    #[test]
    fn a_lineage_key_becomes_one_path_segment() {
        let home = Path::new("/var/lib/agentenv");
        assert_eq!(
            local_path(home, "sha256:ab/cd"),
            home.join("mem-prefetch").join("sha256-ab-cd.json")
        );
    }

    #[tokio::test]
    async fn a_missing_list_is_downloaded_when_the_repository_has_one() {
        let dir = tempfile::tempdir().unwrap();
        let local = dir.path().join("mem-prefetch").join("id.json");
        let outcome = sync(
            &local,
            |part| async move {
                tokio::fs::write(&part, b"{}").await.unwrap();
                Ok(true)
            },
            || async { panic!("no need to ask") },
            not_called(),
        )
        .await
        .unwrap();
        assert_eq!(outcome, SyncOutcome::Downloaded);
        assert_eq!(std::fs::read(&local).unwrap(), b"{}");
        assert!(uploaded_marker(&local).exists());
        assert!(!dir
            .path()
            .join("mem-prefetch")
            .join("id.json.part")
            .exists());
    }

    #[tokio::test]
    async fn a_missing_list_stays_missing_when_the_repository_has_none() {
        let dir = tempfile::tempdir().unwrap();
        let local = dir.path().join("id.json");
        let outcome = sync(
            &local,
            |_| async { Ok(false) },
            || async { panic!("no need to ask") },
            not_called(),
        )
        .await
        .unwrap();
        assert_eq!(outcome, SyncOutcome::Absent);
        assert!(!local.exists());
        assert!(!uploaded_marker(&local).exists());
    }

    #[tokio::test]
    async fn a_local_list_the_repository_lacks_is_uploaded_once() {
        let dir = tempfile::tempdir().unwrap();
        let local = dir.path().join("id.json");
        std::fs::write(&local, b"{}").unwrap();
        let uploaded = Cell::new(0);
        let outcome = sync(
            &local,
            not_called(),
            || async { Ok(false) },
            |path| {
                assert_eq!(path, local);
                uploaded.set(uploaded.get() + 1);
                async { Ok(()) }
            },
        )
        .await
        .unwrap();
        assert_eq!(outcome, SyncOutcome::Uploaded);
        assert_eq!(uploaded.get(), 1);

        let outcome = sync(
            &local,
            not_called(),
            || async { panic!("the marker answers this") },
            not_called(),
        )
        .await
        .unwrap();
        assert_eq!(outcome, SyncOutcome::InSync);
    }

    #[tokio::test]
    async fn a_local_list_the_repository_already_has_is_marked_and_not_uploaded() {
        let dir = tempfile::tempdir().unwrap();
        let local = dir.path().join("id.json");
        std::fs::write(&local, b"{}").unwrap();
        let outcome = sync(&local, not_called(), || async { Ok(true) }, not_called())
            .await
            .unwrap();
        assert_eq!(outcome, SyncOutcome::InSync);
        assert!(uploaded_marker(&local).exists());
    }

    #[tokio::test]
    async fn a_failed_download_leaves_no_partial_file() {
        let dir = tempfile::tempdir().unwrap();
        let local = dir.path().join("id.json");
        let err = sync(
            &local,
            |part| async move {
                tokio::fs::write(&part, b"half").await.unwrap();
                anyhow::bail!("connection reset")
            },
            || async { panic!() },
            not_called(),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("connection reset"));
        assert!(!local.exists());
        assert!(std::fs::read_dir(dir.path()).unwrap().count() == 0);
    }
}
