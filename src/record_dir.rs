use std::path::{Path, PathBuf};

use anyhow::Context;
use tokio::fs;
use tokio::io::AsyncWriteExt;

use crate::local_store::LocalStoreDurability;

const RECORD_SUFFIX: &str = ".json";
const TEMP_SUFFIX: &str = ".json.tmp";

/// Longest `NAME_MAX` a record file may occupy once the suffixes are appended.
const MAX_FILE_NAME: usize = 255;

/// A directory of records, one atomically replaced `<key>.json` file each.
///
/// Keys are encoded into file names reversibly, so the key a record was
/// written under is always recoverable from the directory listing alone.
#[derive(Clone, Debug)]
pub struct JsonRecordDir {
    dir: PathBuf,
    durability: LocalStoreDurability,
}

impl JsonRecordDir {
    /// Open the directory at `dir`, creating it and its parents when absent.
    pub async fn open(
        dir: impl Into<PathBuf>,
        durability: LocalStoreDurability,
    ) -> anyhow::Result<Self> {
        let dir = dir.into();
        fs::create_dir_all(&dir)
            .await
            .with_context(|| format!("create record directory {}", dir.display()))?;
        Ok(Self { dir, durability })
    }

    /// The directory these records live in.
    pub fn path(&self) -> &Path {
        &self.dir
    }

    /// Read one record, or `Ok(None)` when no record is stored under `key`.
    pub async fn get(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        let path = self.record_path(key)?;
        match fs::read(&path).await {
            Ok(bytes) => Ok(Some(bytes)),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err).with_context(|| format!("read record {}", path.display())),
        }
    }

    /// Write one record, replacing any previous value for `key` atomically.
    pub async fn put(&self, key: &str, value: impl AsRef<[u8]>) -> anyhow::Result<()> {
        let encoded = self.encode_name(key)?;
        let temp = self.dir.join(format!("{encoded}{TEMP_SUFFIX}"));
        let target = self.dir.join(format!("{encoded}{RECORD_SUFFIX}"));

        let mut file = fs::File::create(&temp)
            .await
            .with_context(|| format!("create record staging file {}", temp.display()))?;
        let write = async {
            file.write_all(value.as_ref()).await?;
            file.flush().await?;
            if syncs_file(self.durability) {
                file.sync_all().await?;
            }
            Ok::<_, std::io::Error>(())
        }
        .await
        .with_context(|| format!("write record staging file {}", temp.display()));
        drop(file);
        if let Err(err) = write {
            let _ = fs::remove_file(&temp).await;
            return Err(err);
        }

        fs::rename(&temp, &target).await.with_context(|| {
            format!(
                "publish record {} from {}",
                target.display(),
                temp.display()
            )
        })?;
        if syncs_dir(self.durability) {
            sync_dir(&self.dir).await?;
        }
        Ok(())
    }

    /// Delete the record stored under `key`; a missing record is success.
    pub async fn remove(&self, key: &str) -> anyhow::Result<()> {
        let path = self.record_path(key)?;
        match fs::remove_file(&path).await {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(err) => {
                return Err(err).with_context(|| format!("remove record {}", path.display()))
            }
        }
        if syncs_dir(self.durability) {
            sync_dir(&self.dir).await?;
        }
        Ok(())
    }

    /// Read every record, discarding staging files left by an interrupted write.
    ///
    /// Entries whose file name is not a valid encoded key are skipped rather
    /// than failing the scan.
    pub async fn load_all(&self) -> anyhow::Result<Vec<(String, Vec<u8>)>> {
        let mut dir = match fs::read_dir(&self.dir).await {
            Ok(dir) => dir,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => {
                return Err(err)
                    .with_context(|| format!("read record directory {}", self.dir.display()))
            }
        };

        let mut records = Vec::new();
        while let Some(entry) = dir
            .next_entry()
            .await
            .with_context(|| format!("scan record directory {}", self.dir.display()))?
        {
            let file_name = entry.file_name();
            let Some(file_name) = file_name.to_str() else {
                continue;
            };
            let is_file = entry
                .file_type()
                .await
                .with_context(|| format!("inspect record {}", entry.path().display()))?
                .is_file();
            if !is_file {
                continue;
            }

            if file_name.ends_with(TEMP_SUFFIX) {
                let _ = fs::remove_file(entry.path()).await;
                continue;
            }

            let Some(encoded) = file_name.strip_suffix(RECORD_SUFFIX) else {
                continue;
            };
            let Some(key) = decode_name(encoded) else {
                tracing::warn!(
                    record = %entry.path().display(),
                    "skipping record whose file name is not an encoded key"
                );
                continue;
            };

            match fs::read(entry.path()).await {
                Ok(bytes) => records.push((key, bytes)),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => {
                    return Err(err)
                        .with_context(|| format!("read record {}", entry.path().display()))
                }
            }
        }

        Ok(records)
    }

    fn record_path(&self, key: &str) -> anyhow::Result<PathBuf> {
        let encoded = self.encode_name(key)?;
        Ok(self.dir.join(format!("{encoded}{RECORD_SUFFIX}")))
    }

    fn encode_name(&self, key: &str) -> anyhow::Result<String> {
        let encoded = encode_name(key);
        anyhow::ensure!(
            encoded.len() + TEMP_SUFFIX.len() <= MAX_FILE_NAME,
            "record key {key:?} does not fit in a file name under {}",
            self.dir.display()
        );
        Ok(encoded)
    }
}

/// Removes a node-local store directory this build cannot read, reporting
/// whether one was there.
///
/// Removal failure is logged rather than propagated: the directory is inert to
/// this build either way, and callers rebuild their state without it.
pub async fn discard_unreadable_store(path: &Path) -> bool {
    match fs::metadata(path).await {
        Ok(metadata) if metadata.is_dir() => {}
        Ok(_) => return false,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return false,
        Err(err) => {
            tracing::warn!(
                store = %path.display(),
                error = %err,
                "could not inspect a store this build cannot read"
            );
            return false;
        }
    }

    match fs::remove_dir_all(path).await {
        Ok(()) => true,
        Err(err) => {
            tracing::warn!(
                store = %path.display(),
                error = %err,
                "could not remove a store this build cannot read"
            );
            true
        }
    }
}

fn syncs_file(durability: LocalStoreDurability) -> bool {
    matches!(
        durability,
        LocalStoreDurability::Wal | LocalStoreDurability::Sync
    )
}

fn syncs_dir(durability: LocalStoreDurability) -> bool {
    matches!(durability, LocalStoreDurability::Sync)
}

async fn sync_dir(dir: &Path) -> anyhow::Result<()> {
    let handle = fs::File::open(dir)
        .await
        .with_context(|| format!("open record directory {} to sync", dir.display()))?;
    handle
        .sync_all()
        .await
        .with_context(|| format!("sync record directory {}", dir.display()))
}

/// Percent-escapes every byte outside `[A-Za-z0-9._-]`, so no key can name a
/// path component other than a plain file in the record directory.
fn encode_name(key: &str) -> String {
    use std::fmt::Write;

    let mut encoded = String::with_capacity(key.len());
    for byte in key.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-') {
            encoded.push(char::from(byte));
        } else {
            let _ = write!(encoded, "%{byte:02X}");
        }
    }
    encoded
}

fn decode_name(encoded: &str) -> Option<String> {
    let bytes = encoded.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'%' => {
                let hex = encoded.get(index + 1..index + 3)?;
                decoded.push(u8::from_str_radix(hex, 16).ok()?);
                index += 3;
            }
            byte if byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-') => {
                decoded.push(byte);
                index += 1;
            }
            _ => return None,
        }
    }
    String::from_utf8(decoded).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    async fn record_dir(temp: &TempDir) -> anyhow::Result<JsonRecordDir> {
        JsonRecordDir::open(temp.path().join("records"), LocalStoreDurability::Memory).await
    }

    #[tokio::test]
    async fn put_get_remove_round_trip() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let records = record_dir(&temp).await?;

        assert_eq!(records.get("alpha").await?, None);
        records.put("alpha", b"{\"a\":1}").await?;
        assert_eq!(
            records.get("alpha").await?.as_deref(),
            Some(&b"{\"a\":1}"[..])
        );

        records.put("alpha", b"{\"a\":2}").await?;
        assert_eq!(
            records.get("alpha").await?.as_deref(),
            Some(&b"{\"a\":2}"[..])
        );

        records.remove("alpha").await?;
        assert_eq!(records.get("alpha").await?, None);
        records.remove("alpha").await?;
        Ok(())
    }

    #[tokio::test]
    async fn load_all_returns_every_record_keyed_by_its_original_key() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let records = record_dir(&temp).await?;
        records.put("one", b"1").await?;
        records.put("two", b"2").await?;

        let mut loaded = records.load_all().await?;
        loaded.sort();

        assert_eq!(
            loaded,
            vec![
                ("one".to_string(), b"1".to_vec()),
                ("two".to_string(), b"2".to_vec())
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn load_all_on_a_missing_directory_is_empty() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let records = record_dir(&temp).await?;
        tokio::fs::remove_dir_all(records.path()).await?;

        assert!(records.load_all().await?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn load_all_drops_staging_files_and_keeps_the_record_they_would_replace(
    ) -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let records = record_dir(&temp).await?;
        records.put("alpha", b"committed").await?;
        let staging = records.path().join("alpha.json.tmp");
        tokio::fs::write(&staging, b"half-written").await?;

        let loaded = records.load_all().await?;

        assert_eq!(loaded, vec![("alpha".to_string(), b"committed".to_vec())]);
        assert!(!staging.exists());
        Ok(())
    }

    #[tokio::test]
    async fn load_all_ignores_files_that_are_not_records() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let records = record_dir(&temp).await?;
        records.put("alpha", b"1").await?;
        tokio::fs::write(records.path().join("notes.txt"), b"ignored").await?;
        tokio::fs::create_dir(records.path().join("nested.json")).await?;

        let loaded = records.load_all().await?;

        assert_eq!(loaded, vec![("alpha".to_string(), b"1".to_vec())]);
        Ok(())
    }

    #[tokio::test]
    async fn keys_that_are_not_file_name_safe_stay_inside_the_directory() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let records = record_dir(&temp).await?;
        let key = "../escape/attempt";

        records.put(key, b"contained").await?;

        assert_eq!(records.get(key).await?.as_deref(), Some(&b"contained"[..]));
        assert_eq!(
            records.load_all().await?,
            vec![(key.to_string(), b"contained".to_vec())]
        );
        let entries: Vec<_> = std::fs::read_dir(records.path())?
            .map(|entry| entry.map(|entry| entry.file_name().to_string_lossy().into_owned()))
            .collect::<std::io::Result<_>>()?;
        assert_eq!(entries, vec!["..%2Fescape%2Fattempt.json".to_string()]);
        Ok(())
    }

    #[tokio::test]
    async fn a_key_too_long_for_a_file_name_is_rejected() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let records = record_dir(&temp).await?;

        let err = records
            .put(&"k".repeat(MAX_FILE_NAME), b"x")
            .await
            .expect_err("an over-long key has no file name");

        assert!(err.to_string().contains("does not fit in a file name"));
        Ok(())
    }

    #[tokio::test]
    async fn distinct_keys_never_share_a_file() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let records = record_dir(&temp).await?;
        records.put("a/b", b"slash").await?;
        records.put("a%2Fb", b"literal").await?;

        assert_eq!(records.get("a/b").await?.as_deref(), Some(&b"slash"[..]));
        assert_eq!(
            records.get("a%2Fb").await?.as_deref(),
            Some(&b"literal"[..])
        );
        Ok(())
    }

    #[tokio::test]
    async fn discarding_a_store_reports_only_a_directory_that_was_there() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let absent = temp.path().join("absent");
        let file = temp.path().join("a-file");
        let store = temp.path().join("store");
        tokio::fs::write(&file, b"not a store").await?;
        tokio::fs::create_dir_all(store.join("nested")).await?;
        tokio::fs::write(store.join("nested").join("data"), b"payload").await?;

        assert!(!discard_unreadable_store(&absent).await);
        assert!(!discard_unreadable_store(&file).await);
        assert!(discard_unreadable_store(&store).await);

        assert!(file.exists());
        assert!(!store.exists());
        Ok(())
    }

    #[tokio::test]
    async fn synced_writes_are_readable() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let records =
            JsonRecordDir::open(temp.path().join("synced"), LocalStoreDurability::Sync).await?;

        records.put("alpha", b"durable").await?;
        assert_eq!(
            records.get("alpha").await?.as_deref(),
            Some(&b"durable"[..])
        );
        records.remove("alpha").await?;
        assert!(records.load_all().await?.is_empty());
        Ok(())
    }
}
