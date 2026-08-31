//! Shelling out to `regctl`, the one binary every registry operation in
//! AgentENV goes through.

use std::path::Path;
use std::time::Duration;

use anyhow::{anyhow, Context};
use tokio::process::Command;
use tracing::warn;

use super::{ImageError, ImageResult};

const REGCTL_GOMAXPROCS: &str = "4";
pub const REGCTL_RETRY_ATTEMPTS: u32 = 5;
pub const REGCTL_RETRY_BASE_DELAY: Duration = Duration::from_millis(500);

// regctl preserves manifests byte-for-byte and accepts Docker schema2
// manifests that carry OCI descriptor mediaTypes, which skopeo rejects
// (`unsupported docker v2s2 media type`).

/// Returns whether `regctl` reported HTTP 404.
///
/// Network and DNS failures must retain their 5xx classification.
pub fn regctl_stderr_is_not_found(stderr: &str) -> bool {
    stderr.contains("[http 404]")
}

/// Builds a `regctl` command with a bounded Go runtime.
///
/// All invocations must use this helper so concurrent calls cannot exhaust the PID limit.
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
        // A 404 is not transient; preserve the typed not-found classification.
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
