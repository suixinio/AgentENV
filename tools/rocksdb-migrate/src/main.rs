//! Converts a node's paused-sandbox record database into one JSON file per record.
//!
//! Run it once per node, with `aenv-node` stopped, before rolling out a build
//! that reads `records/`.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use clap::Parser;
use rocksdb::{IteratorMode, Options, DB};

const LEGACY_RECORD_DB_DIR: &str = "records.db";
const RECORD_DIR: &str = "records";
const RECORD_SUFFIX: &str = ".json";
const TEMP_SUFFIX: &str = ".json.tmp";
const MAX_FILE_NAME: usize = 255;

#[derive(Parser)]
#[command(
    name = "aenv-rocksdb-migrate",
    about = "Convert records.db into records/<sandbox-id>.json"
)]
struct Cli {
    /// Paused-sandbox store root, i.e. `[orchestrator].persisted_sandbox_store_path`.
    #[arg(long)]
    store: PathBuf,

    /// Report what would be written without touching the record directory.
    #[arg(long)]
    dry_run: bool,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let legacy = cli.store.join(LEGACY_RECORD_DB_DIR);
    let records = cli.store.join(RECORD_DIR);

    if !legacy.is_dir() {
        bail!("{} is not a directory", legacy.display());
    }

    let mut options = Options::default();
    options.create_if_missing(false);
    let db = DB::open_for_read_only(&options, &legacy, false)
        .with_context(|| format!("open {} read-only", legacy.display()))?;

    if !cli.dry_run {
        std::fs::create_dir_all(&records)
            .with_context(|| format!("create {}", records.display()))?;
    }

    let mut converted = 0_usize;
    let mut skipped = 0_usize;
    for item in db.iterator(IteratorMode::Start) {
        let (key, value) = item.with_context(|| format!("iterate {}", legacy.display()))?;
        let key = match std::str::from_utf8(&key) {
            Ok(key) => key,
            Err(_) => {
                eprintln!("skipping record with a non-UTF-8 key");
                skipped += 1;
                continue;
            }
        };

        if serde_json::from_slice::<serde_json::Value>(&value).is_err() {
            eprintln!("skipping record {key}: its value is not JSON");
            skipped += 1;
            continue;
        }

        let name = encode_name(key);
        if name.len() + TEMP_SUFFIX.len() > MAX_FILE_NAME {
            eprintln!("skipping record {key}: it does not fit in a file name");
            skipped += 1;
            continue;
        }

        if cli.dry_run {
            println!(
                "would write {}",
                records.join(format!("{name}{RECORD_SUFFIX}")).display()
            );
        } else {
            write_record(&records, &name, &value).with_context(|| format!("write record {key}"))?;
        }
        converted += 1;
    }

    if !cli.dry_run {
        sync_dir(&records)?;
    }

    println!(
        "{verb} {converted} record(s) into {records}{skipped_note}",
        verb = if cli.dry_run {
            "would convert"
        } else {
            "converted"
        },
        records = records.display(),
        skipped_note = if skipped == 0 {
            String::new()
        } else {
            format!(", skipped {skipped}")
        }
    );
    if !cli.dry_run {
        println!(
            "verify the sandboxes come back, then remove {}",
            legacy.display()
        );
    }
    Ok(())
}

fn write_record(records: &Path, name: &str, value: &[u8]) -> Result<()> {
    let temp = records.join(format!("{name}{TEMP_SUFFIX}"));
    let target = records.join(format!("{name}{RECORD_SUFFIX}"));
    std::fs::write(&temp, value).with_context(|| format!("write {}", temp.display()))?;
    std::fs::File::open(&temp)
        .and_then(|file| file.sync_all())
        .with_context(|| format!("sync {}", temp.display()))?;
    std::fs::rename(&temp, &target).with_context(|| format!("publish {}", target.display()))
}

fn sync_dir(dir: &Path) -> Result<()> {
    std::fs::File::open(dir)
        .and_then(|handle| handle.sync_all())
        .with_context(|| format!("sync {}", dir.display()))
}

/// Mirrors `aenv_core::record_dir`'s file naming so both sides agree on which
/// file a key maps to.
fn encode_name(key: &str) -> String {
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
