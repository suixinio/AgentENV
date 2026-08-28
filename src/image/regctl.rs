//! Shelling out to `regctl`, the one binary every registry operation in
//! AgentENV goes through.
//!
//! 🔴 In `aenv-core` rather than in `aenv-node`'s `image::oci_image`, where it
//! used to live, because two crates shell out to `regctl` for two different
//! reasons: `aenv-node` resolves user images with it, and `aenv-api`'s
//! `aenv-snapshot-image` publishes a committed snapshot's rootfs with it. That
//! tool has to live in the half that holds the snapshot catalog — PostgreSQL,
//! and `aenv-api` owns the only `[pg]` pool — and `aenv-api` does not depend on
//! `aenv-node`. `aenv-node`'s `image::oci_image` re-exports every name here, so
//! its own call sites are unchanged.

use std::path::Path;
use std::time::Duration;

use anyhow::{anyhow, Context};
use tokio::process::Command;
use tracing::warn;

use super::{ImageError, ImageResult};

/// GOMAXPROCS ceiling applied to every spawned `regctl` process; see
/// [`regctl_command`] for the rationale.
const REGCTL_GOMAXPROCS: &str = "4";
pub const REGCTL_RETRY_ATTEMPTS: u32 = 5;
pub const REGCTL_RETRY_BASE_DELAY: Duration = Duration::from_millis(500);

// ---- regctl ----
//
// regctl preserves manifests byte-for-byte and accepts Docker schema2
// manifests that carry OCI descriptor mediaTypes, which skopeo rejects
// (`unsupported docker v2s2 media type`).

/// Return `true` when a failed `regctl` invocation's stderr indicates the
/// registry responded with HTTP 404 (e.g. `MANIFEST_UNKNOWN` / `BLOB_UNKNOWN`).
/// regctl renders these as `... not found [http 404]: {...}`. Network/DNS
/// failures do not carry the `[http 404]` marker and must keep their 5xx
/// classification.
pub fn regctl_stderr_is_not_found(stderr: &str) -> bool {
    stderr.contains("[http 404]")
}

/// Build a [`Command`] for `regctl` with a bounded Go runtime.
///
/// `regctl` is a Go binary; the Go runtime defaults GOMAXPROCS to the number
/// of host CPUs, spawning roughly that many OS threads per process. Because
/// AgentENV forks a short-lived `regctl` per image operation and can do so at
/// high concurrency, on many-core hosts the default fans out into a large
/// number of threads and can exhaust the process/PID limit. regctl's work is
/// network/IO-bound rather than CPU-bound, so a small GOMAXPROCS caps the
/// thread footprint without affecting throughput.
///
/// All `regctl` invocations must be constructed through this so the cap is
/// applied uniformly.
pub fn regctl_command(binary: impl AsRef<std::ffi::OsStr>) -> Command {
    let mut command = Command::new(binary);
    command.env("GOMAXPROCS", REGCTL_GOMAXPROCS);
    command
}

/// Run a regctl invocation, retrying failures with exponential backoff to
/// absorb transient registry errors.
pub async fn run_regctl(regctl_binary: &Path, args: &[&str]) -> ImageResult<std::process::Output> {
    ensure_regctl_binary(regctl_binary)?;
    let mut backoff = REGCTL_RETRY_BASE_DELAY;
    let mut last_stderr = String::new();
    for attempt in 1..=REGCTL_RETRY_ATTEMPTS {
        let output = regctl_command(regctl_binary)
            .args(args)
            .output()
            .await
            .context("spawn regctl")?;
        if output.status.success() {
            return Ok(output);
        }
        last_stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        // A 404 is not transient: the manifest/blob/image simply does not
        // exist. Fail fast with a typed error so the API layer can return a
        // 4xx instead of a 5xx, and skip the retry/backoff budget.
        if regctl_stderr_is_not_found(&last_stderr) {
            return Err(ImageError::NotFound {
                reason: format!(
                    "regctl {} reported the OCI resource does not exist: {last_stderr}",
                    args.join(" ")
                ),
            });
        }
        if attempt < REGCTL_RETRY_ATTEMPTS {
            warn!(attempt, args = ?args, error = %last_stderr, "regctl failed; retrying");
            tokio::time::sleep(backoff).await;
            backoff *= 2;
        }
    }
    Err(ImageError::Other(anyhow!(
        "regctl {} failed after {REGCTL_RETRY_ATTEMPTS} attempts: {last_stderr}",
        args.join(" ")
    )))
}

pub fn ensure_regctl_binary(path: &Path) -> ImageResult<()> {
    if path
        .metadata()
        .is_ok_and(|metadata| metadata.is_file() && metadata.len() > 0)
    {
        Ok(())
    } else {
        Err(ImageError::Other(anyhow!(
            "regctl is required for OCI registry access: {}",
            path.display()
        )))
    }
}
