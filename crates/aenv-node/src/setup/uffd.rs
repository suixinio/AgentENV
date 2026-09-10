use std::path::Path;
use std::process::Command;

use anyhow::{bail, Context, Result};
use nix::unistd::{access, AccessFlags};
use tracing::{info, warn};

const DEV_USERFAULTFD: &str = "/dev/userfaultfd";
const UNPRIVILEGED_SYSCTL: &str = "/proc/sys/vm/unprivileged_userfaultfd";
const UDEV_RULES_DIR: &str = "/etc/udev/rules.d";
const UDEV_RULE_PATH: &str = "/etc/udev/rules.d/99-agentenv-uffd.rules";

fn udev_rule(group: &str) -> String {
    format!(
        "# Managed by agentenv server setup\n\
         KERNEL==\"userfaultfd\", MODE=\"0660\", GROUP=\"{group}\"\n"
    )
}

/// One-time root provisioning for the userfaultfd memory backend: Firecracker
/// creates the descriptor itself, and on a kernel that has `/dev/userfaultfd`
/// its userfaultfd crate opens that device and nothing else, so the runtime
/// group needs read and write access to it.
pub fn provision(group: &str) -> Result<()> {
    if !Path::new(DEV_USERFAULTFD).exists() {
        warn!(
            "{DEV_USERFAULTFD} does not exist on this kernel; the runtime account needs \
             CAP_SYS_PTRACE or vm.unprivileged_userfaultfd=1 for Firecracker's userfaultfd(2)"
        );
        return Ok(());
    }
    if !Path::new(UDEV_RULES_DIR).exists() {
        warn!(
            rules_dir = UDEV_RULES_DIR,
            "udev rules directory not present; skipping persistent rule install"
        );
        return Ok(());
    }
    info!(group, "installing userfaultfd device access rule");
    std::fs::write(UDEV_RULE_PATH, udev_rule(group))
        .with_context(|| format!("install {UDEV_RULE_PATH}"))?;
    if which::which("udevadm").is_err() {
        warn!("udevadm not found; device permissions will apply after udev reloads rules");
        return Ok(());
    }
    let status = Command::new("udevadm")
        .args(["control", "--reload-rules"])
        .status()
        .context("reload udev rules")?;
    if !status.success() {
        bail!("udevadm control --reload-rules failed with {status}");
    }
    Command::new("udevadm")
        .args([
            "trigger",
            "--subsystem-match=misc",
            "--sysname-match=userfaultfd",
        ])
        .status()
        .ok();
    Command::new("udevadm").arg("settle").status().ok();
    Ok(())
}

/// Startup validation that this account can hand Firecracker a userfaultfd.
pub fn check() -> Result<()> {
    if Path::new(DEV_USERFAULTFD).exists() {
        return access(DEV_USERFAULTFD, AccessFlags::R_OK | AccessFlags::W_OK).with_context(|| {
            format!(
                "{DEV_USERFAULTFD} is not readable and writable by this user, which Firecracker's \
                 userfaultfd creation needs; run `server --setup-host` as root"
            )
        });
    }
    let unprivileged_allowed = std::fs::read_to_string(UNPRIVILEGED_SYSCTL)
        .map(|v| v.trim() == "1")
        .unwrap_or(false);
    if unprivileged_allowed {
        return Ok(());
    }
    if linux_cap::has_effective_capabilities(&[linux_cap::CAP_SYS_PTRACE])
        .context("read the process capability sets")?
    {
        return Ok(());
    }
    bail!(
        "the userfaultfd memory backend needs Firecracker to create a userfaultfd, which on this \
         kernel takes CAP_SYS_PTRACE or vm.unprivileged_userfaultfd=1; run as root or through \
         scripts/run-with-capabilities.sh"
    )
}
