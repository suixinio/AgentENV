use std::path::{Path, PathBuf};

use anyhow::Context;
use tokio::fs;
use tokio::io::AsyncWriteExt;

/// How far a [`JsonRecordDir`] write is pushed to disk before it returns.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecordDurability {
    /// No fsync. For tests and for records that are rebuilt after a crash.
    Memory,
    /// Fsync the record and the directory entry naming it.
    Full,
}

impl RecordDurability {
    fn fsyncs(self) -> bool {
        matches!(self, Self::Full)
    }
}

const RECORD_SUFFIX: &str = ".json";
const TEMP_SUFFIX: &str = ".json.tmp";

/// Longest `NAME_MAX` a record file may occupy once the suffixes are appended.
const MAX_FILE_NAME: usize = 255;

/// Widest `.{pid}-{sequence}` a staging name can carry, from the decimal forms
/// of [`u32::MAX`] and [`u64::MAX`].
const MAX_STAGING_TAG: usize = 1 + 10 + 1 + 20;

/// Budget every key must leave for the longest name [`JsonRecordDir::put`] can
/// build, which is always the staging one.
const MAX_NAME_OVERHEAD: usize = MAX_STAGING_TAG + TEMP_SUFFIX.len();

/// A directory of records, one atomically replaced `<key>.json` file each.
///
/// Keys are encoded into file names reversibly, so the key a record was
/// written under is always recoverable from the directory listing alone.
#[derive(Clone, Debug)]
pub struct JsonRecordDir {
    dir: PathBuf,
    durability: RecordDurability,
}

impl JsonRecordDir {
    /// Open the directory at `dir`, creating it and its parents when absent.
    pub async fn open(
        dir: impl Into<PathBuf>,
        durability: RecordDurability,
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
    ///
    /// Concurrent writers of one key each stage into their own file, so the
    /// published record is always exactly one of the values written.
    pub async fn put(&self, key: &str, value: impl AsRef<[u8]>) -> anyhow::Result<()> {
        let encoded = self.encode_name(key)?;
        let temp = self.dir.join(staging_name(&encoded));
        let target = self.dir.join(format!("{encoded}{RECORD_SUFFIX}"));

        let mut file = fs::File::create(&temp)
            .await
            .with_context(|| format!("create record staging file {}", temp.display()))?;
        let write = async {
            file.write_all(value.as_ref()).await?;
            file.flush().await?;
            if self.durability.fsyncs() {
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
        if self.durability.fsyncs() {
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
        if self.durability.fsyncs() {
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
            encoded.len() + MAX_NAME_OVERHEAD <= MAX_FILE_NAME,
            "record key {key:?} does not fit in a file name under {}",
            self.dir.display()
        );
        Ok(encoded)
    }
}

/// Names a staging file no other in-flight `put` can be holding open, and that
/// a scan still recognizes by [`TEMP_SUFFIX`].
fn staging_name(encoded: &str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_STAGING_SEQUENCE: AtomicU64 = AtomicU64::new(0);
    let sequence = NEXT_STAGING_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!("{encoded}.{}-{sequence}{TEMP_SUFFIX}", std::process::id())
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
        JsonRecordDir::open(temp.path().join("records"), RecordDurability::Memory).await
    }

    /// Big enough to span several write syscalls, so two writers that shared a
    /// staging file would interleave rather than race only at the rename.
    const TORN_WRITE_VALUE_LEN: usize = 6 * 1024 * 1024;

    fn concurrent_value(marker: u8) -> Vec<u8> {
        let mut value = Vec::with_capacity(TORN_WRITE_VALUE_LEN);
        value.extend_from_slice(b"[\"");
        value.resize(TORN_WRITE_VALUE_LEN - 2, marker);
        value.extend_from_slice(b"\"]");
        value
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_writers_of_one_key_never_publish_a_mixed_record() -> anyhow::Result<()> {
        const WRITERS: u8 = 6;
        let temp = TempDir::new()?;
        let records = record_dir(&temp).await?;
        let candidates: Vec<Vec<u8>> = (0..WRITERS).map(|n| concurrent_value(b'a' + n)).collect();

        let mut writers = tokio::task::JoinSet::new();
        for value in candidates.clone() {
            let records = records.clone();
            writers.spawn(async move { records.put("contended", value).await });
        }
        while let Some(joined) = writers.join_next().await {
            joined??;
        }

        let published = records
            .get("contended")
            .await?
            .expect("a writer published a record");
        let matched = candidates
            .iter()
            .position(|candidate| candidate == &published);
        assert!(
            matched.is_some(),
            "the published record is not any single writer's value: {} bytes, \
             first mismatch against every candidate",
            published.len()
        );
        assert!(
            serde_json::from_slice::<serde_json::Value>(&published).is_ok(),
            "the published record is not valid JSON"
        );

        let residue: Vec<String> = std::fs::read_dir(records.path())?
            .map(|entry| entry.map(|entry| entry.file_name().to_string_lossy().into_owned()))
            .collect::<std::io::Result<Vec<_>>>()?
            .into_iter()
            .filter(|name| name.ends_with(TEMP_SUFFIX))
            .collect();
        assert!(
            residue.is_empty(),
            "staging files were left behind: {residue:?}"
        );
        Ok(())
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

    #[test]
    fn the_staging_tag_budget_covers_the_widest_pid_and_sequence() {
        let widest = format!(".{}-{}", u32::MAX, u64::MAX);
        assert_eq!(widest.len(), MAX_STAGING_TAG);
    }

    #[tokio::test]
    async fn a_record_is_never_mistaken_for_a_staging_file_and_the_reverse() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let records = record_dir(&temp).await?;
        // A key that renders every part of the staging name inside the record's
        // own name, which is as close as an encoded key can get.
        let key = "looks.999-999.json.tmp";
        records.put(key, b"{\"kept\":true}").await?;
        let staged = records.path().join(staging_name(&encode_name(key)));
        tokio::fs::write(&staged, b"half-written").await?;

        assert!(staged.to_string_lossy().ends_with(TEMP_SUFFIX));
        let loaded = records.load_all().await?;

        assert_eq!(
            loaded,
            vec![(key.to_string(), b"{\"kept\":true}".to_vec())],
            "the record must survive the scan that reclaims staging files"
        );
        assert!(!staged.exists(), "the staging file must be reclaimed");
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
            JsonRecordDir::open(temp.path().join("synced"), RecordDurability::Full).await?;

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
