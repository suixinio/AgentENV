use std::path::Path;
use std::process::Command;

use anyhow::{bail, Context, Result};
use nix::unistd::{access, AccessFlags};
use tracing::{info, warn};
use uvm_ublk::{load_ublk_module, ublk_module_loaded};

use crate::cfg::BlockTransport;

const UDEV_RULES_DIR: &str = "/etc/udev/rules.d";
const NBD_MODULE_PATH: &str = "/sys/module/nbd";

fn supports_persistent_udev_rules() -> bool {
    Path::new(UDEV_RULES_DIR).exists()
}

fn write_udev_rule(path: &str, rule_content: String) -> Result<()> {
    if !supports_persistent_udev_rules() {
        warn!(
            rules_dir = UDEV_RULES_DIR,
            path, "udev rules directory not present; skipping persistent rule install"
        );
        return Ok(());
    }
    std::fs::write(path, rule_content).with_context(|| format!("install {path}"))?;
    Ok(())
}

fn ublk_udev_rule(current_group: &str) -> String {
    format!(
        "# Managed by agentenv server setup\n\
         KERNEL==\"ublk-control\", MODE=\"0660\", GROUP=\"{current_group}\"\n\
         KERNEL==\"ublkc*\", MODE=\"0660\", GROUP=\"{current_group}\"\n\
         KERNEL==\"ublkb*\", MODE=\"0660\", GROUP=\"{current_group}\"\n"
    )
}

fn nbd_udev_rule(current_group: &str) -> String {
    format!(
        "# Managed by agentenv server setup\n\
         KERNEL==\"nbd*\", MODE=\"0660\", GROUP=\"{current_group}\"\n"
    )
}

fn reload_udev(triggers: &[&[&str]]) -> Result<()> {
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
    for trigger in triggers {
        Command::new("udevadm")
            .arg("trigger")
            .args(*trigger)
            .status()
            .ok();
    }
    Command::new("udevadm").arg("settle").status().ok();
    Ok(())
}

fn provision_ublk(group: &str) -> Result<()> {
    if !ublk_module_loaded() {
        install_apt_extra_kernel_modules();
    }
    load_ublk_module()?;
    std::fs::create_dir_all("/etc/modules-load.d").context("create /etc/modules-load.d")?;
    std::fs::write("/etc/modules-load.d/aenv-ublk.conf", "ublk_drv\n")
        .context("install /etc/modules-load.d/aenv-ublk.conf")?;
    info!(group, "installing ublk device access rules");
    write_udev_rule(
        "/etc/udev/rules.d/99-agentenv-ublk.rules",
        ublk_udev_rule(group),
    )?;
    reload_udev(&[
        &["--subsystem-match=misc", "--sysname-match=ublk-control"],
        &["--sysname-match=ublkc*"],
        &["--sysname-match=ublkb*"],
    ])
}

fn nbd_module_loaded() -> bool {
    Path::new(NBD_MODULE_PATH).exists()
}

fn load_nbd_module() -> Result<()> {
    if nbd_module_loaded() {
        return Ok(());
    }
    info!("nbd kernel module not loaded; attempting automatic load");
    let status = Command::new("modprobe")
        .arg("nbd")
        .status()
        .context("run modprobe nbd")?;
    if !status.success() || !nbd_module_loaded() {
        bail!("modprobe nbd failed with {status}; the kernel has no nbd module");
    }
    Ok(())
}

fn provision_nbd(group: &str) -> Result<()> {
    load_nbd_module()?;
    std::fs::create_dir_all("/etc/modules-load.d").context("create /etc/modules-load.d")?;
    std::fs::write("/etc/modules-load.d/aenv-nbd.conf", "nbd\n")
        .context("install /etc/modules-load.d/aenv-nbd.conf")?;
    info!(group, "installing nbd device access rules");
    write_udev_rule(
        "/etc/udev/rules.d/99-agentenv-nbd.rules",
        nbd_udev_rule(group),
    )?;
    reload_udev(&[&["--subsystem-match=block", "--sysname-match=nbd*"]])
}

/// One-time root provisioning for the configured block transport.
pub fn provision(group: &str, transport: BlockTransport) -> Result<()> {
    match transport {
        BlockTransport::Ublk => provision_ublk(group),
        BlockTransport::Nbd => provision_nbd(group),
    }
}

fn check_ublk() -> Result<()> {
    if !ublk_module_loaded() {
        bail!("ublk_drv is not loaded; run `server --setup-host` as root");
    }
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/ublk-control")
        .context("open /dev/ublk-control for read/write access")?;
    Ok(())
}

fn first_nbd_device_node() -> Option<std::path::PathBuf> {
    let mut nodes: Vec<std::path::PathBuf> = std::fs::read_dir("/dev")
        .ok()?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| {
                    name.strip_prefix("nbd").is_some_and(|rest| {
                        !rest.is_empty() && rest.bytes().all(|b| b.is_ascii_digit())
                    })
                })
        })
        .collect();
    nodes.sort();
    nodes.into_iter().next()
}

fn check_nbd() -> Result<()> {
    if !nbd_module_loaded() {
        bail!("the nbd kernel module is not loaded; run `server --setup-host` as root");
    }
    // The kernel allocates indices on connect, so the preallocated nodes may
    // all have been consumed; the access check runs against whichever exists.
    match first_nbd_device_node() {
        Some(node) => {
            access(node.as_path(), AccessFlags::R_OK | AccessFlags::W_OK).with_context(|| {
                format!(
                    "{} is not readable and writable by this user; run `server --setup-host` as root",
                    node.display()
                )
            })?;
        }
        None => warn!("no /dev/nbd* node exists yet; device access is checked at first connect"),
    }
    if !linux_cap::has_effective_capabilities(&[linux_cap::CAP_SYS_ADMIN])
        .context("read the process capability sets")?
    {
        bail!(
            "the nbd transport needs CAP_SYS_ADMIN for the kernel netlink interface; \
             run as root or through scripts/run-with-capabilities.sh"
        );
    }
    Ok(())
}

/// Startup validation that the configured block transport is usable without elevation.
pub fn check(transport: BlockTransport) -> Result<()> {
    match transport {
        BlockTransport::Ublk => check_ublk(),
        BlockTransport::Nbd => check_nbd(),
    }
}

fn install_apt_extra_kernel_modules() {
    if which::which("apt-get").is_err() {
        return;
    }
    let uname = Command::new("uname")
        .args(["-r"])
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_default();
    if uname.is_empty() {
        return;
    }
    let pkg = format!("linux-modules-extra-{uname}");
    if !Command::new("apt-get")
        .args(["install", "-y", pkg.as_str()])
        .status()
        .is_ok_and(|status| status.success())
    {
        warn!("extra kernel modules not found via apt for this kernel");
    }
}
