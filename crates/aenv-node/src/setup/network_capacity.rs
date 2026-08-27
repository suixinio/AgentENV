use std::fs;
use std::path::Path;

use tracing::{debug, info, warn};

const FORCE_SYSCTL_ENV: &str = "AENV_FORCE_SYSCTL_TUNING";

/// Kernel parameter thresholds for large-scale sandbox deployments.
struct CapacityThreshold {
    name: &'static str,
    path: &'static str,
    recommended: u64,
    reason: &'static str,
    /// If set, an unreadable sysctl is expected on some kernels and should be skipped.
    optional_read_failure_reason: Option<&'static str>,
}

const THRESHOLDS: &[CapacityThreshold] = &[
    CapacityThreshold {
        name: "net.ipv4.neigh.default.gc_thresh3",
        path: "/proc/sys/net/ipv4/neigh/default/gc_thresh3",
        recommended: 16384,
        reason: "ARP table GC threshold 3",
        optional_read_failure_reason: None,
    },
    CapacityThreshold {
        name: "net.ipv4.neigh.default.gc_thresh2",
        path: "/proc/sys/net/ipv4/neigh/default/gc_thresh2",
        recommended: 8192,
        reason: "ARP table GC threshold 2",
        optional_read_failure_reason: None,
    },
    CapacityThreshold {
        name: "net.ipv4.neigh.default.gc_thresh1",
        path: "/proc/sys/net/ipv4/neigh/default/gc_thresh1",
        recommended: 4096,
        reason: "ARP table GC threshold 1",
        optional_read_failure_reason: None,
    },
    CapacityThreshold {
        name: "net.netfilter.nf_conntrack_max",
        path: "/proc/sys/net/netfilter/nf_conntrack_max",
        recommended: 1048576,
        reason: "connection tracking table for thousands of sandboxes",
        optional_read_failure_reason: Some(
            "nf_conntrack may not be loaded until conntrack is used",
        ),
    },
    CapacityThreshold {
        name: "kernel.pid_max",
        path: "/proc/sys/kernel/pid_max",
        recommended: 4194304,
        reason: "process ID exhaustion with 10k+ sandboxes",
        optional_read_failure_reason: None,
    },
    CapacityThreshold {
        name: "fs.inotify.max_user_instances",
        path: "/proc/sys/fs/inotify/max_user_instances",
        recommended: 8192,
        reason: "envd/firecracker inotify watches",
        optional_read_failure_reason: None,
    },
];

pub fn install_persistent_config() -> Result<(), std::io::Error> {
    fs::create_dir_all("/etc/sysctl.d")?;
    let mut content = String::from(
        "# Managed by AENV host setup\n\
         net.ipv4.ip_forward = 1\n",
    );
    for threshold in THRESHOLDS {
        content.push_str(&format!("{} = {}\n", threshold.name, threshold.recommended));
    }
    fs::write("/etc/sysctl.d/99-aenv.conf", content)
}

pub fn check() {
    check_inner(false);
}

pub fn check_and_adjust() {
    check_inner(true);
}

fn check_inner(adjust: bool) {
    info!("checking kernel networking capacity for large-scale deployments");

    if should_skip_sysctl_tuning() {
        warn!(
            "skipping automatic sysctl tuning inside a container; configure these host kernel parameters on the host, or set {}=1 only for a privileged container with writable host sysctls",
            FORCE_SYSCTL_ENV
        );
        return;
    }

    if !proc_sys_readable() {
        warn!(
            "skipping automatic sysctl tuning because /proc/sys is not readable; mount procfs or configure the host kernel parameters manually"
        );
        return;
    }

    let mut warnings = Vec::new();
    let mut remediation_commands = Vec::new();
    let mut adjusted = Vec::new();

    for threshold in THRESHOLDS {
        let current = match read_sysctl_u64(threshold.path) {
            Ok(val) => val,
            Err(err) => {
                if let Some(reason) = threshold.optional_read_failure_reason {
                    debug!(
                        sysctl = threshold.name,
                        path = threshold.path,
                        error = %err,
                        reason,
                        "skipping optional kernel capacity threshold"
                    );
                    continue;
                }
                warnings.push(format!(
                    "{}: unable to read (path: {})",
                    threshold.name, threshold.path
                ));
                continue;
            }
        };

        if current < threshold.recommended && adjust {
            if let Err(err) =
                adjust_threshold(threshold, current, &mut adjusted, &mut remediation_commands)
            {
                warnings.push(err);
            }
        } else if current < threshold.recommended {
            remediation_commands.push(format!(
                "sysctl -w {}={}",
                threshold.name, threshold.recommended
            ));
            warnings.push(format!(
                "{}: current={}, recommended={} — {}",
                threshold.name, current, threshold.recommended, threshold.reason
            ));
        }
    }

    if !adjusted.is_empty() {
        info!(
            "automatically adjusted kernel parameters for large-scale deployments:\n  {}",
            adjusted.join("\n  ")
        );
    }

    if warnings.is_empty() {
        info!("kernel networking capacity checks passed");
    } else {
        warn!(
            "kernel parameters below recommended thresholds for large-scale deployments:\n  {}",
            warnings.join("\n  ")
        );
        if !remediation_commands.is_empty() {
            warn!(
                "to fix the low kernel parameters, run as root:\n  {}",
                remediation_commands.join("\n  ")
            );
        }
    }
}

fn should_skip_sysctl_tuning() -> bool {
    if env_truthy(FORCE_SYSCTL_ENV) {
        return false;
    }
    running_in_container()
}

fn env_truthy(name: &str) -> bool {
    std::env::var(name)
        .ok()
        .map(|value| {
            matches!(
                value.as_str(),
                "1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON"
            )
        })
        .unwrap_or(false)
}

fn running_in_container() -> bool {
    Path::new("/.dockerenv").exists()
        || fs::read_to_string("/proc/1/cgroup")
            .map(|content| {
                // Best-effort cgroup v1/container-runtime marker scan. Some
                // cgroup v2 environments expose only "0::/" here, so callers
                // still treat unreadable /proc/sys as a separate skip signal.
                content.contains("/docker/")
                    || content.contains("/kubepods/")
                    || content.contains("/containerd/")
                    || content.contains("/podman/")
            })
            .unwrap_or(false)
}

fn proc_sys_readable() -> bool {
    fs::read_dir("/proc/sys").is_ok()
}

fn adjust_threshold(
    threshold: &CapacityThreshold,
    current: u64,
    adjusted: &mut Vec<String>,
    remediation_commands: &mut Vec<String>,
) -> Result<(), String> {
    match write_sysctl_u64(threshold.path, threshold.recommended) {
        Ok(()) => match read_sysctl_u64(threshold.path) {
            Ok(updated) if updated >= threshold.recommended => {
                adjusted.push(format!(
                    "{}: {} -> {} ({})",
                    threshold.name, current, updated, threshold.reason
                ));
                Ok(())
            }
            Ok(updated) => {
                remediation_commands.push(format!(
                    "sysctl -w {}={}",
                    threshold.name, threshold.recommended
                ));
                Err(format!(
                    "{}: attempted to set recommended value, but current={} remains below recommended={} — {}",
                    threshold.name, updated, threshold.recommended, threshold.reason
                ))
            }
            Err(err) => {
                adjusted.push(format!(
                    "{}: {} -> {} ({})",
                    threshold.name, current, threshold.recommended, threshold.reason
                ));
                debug!(
                    sysctl = threshold.name,
                    path = threshold.path,
                    error = %err,
                    "unable to re-read kernel capacity threshold after automatic adjustment"
                );
                Ok(())
            }
        },
        Err(err) => {
            remediation_commands.push(format!(
                "sysctl -w {}={}",
                threshold.name, threshold.recommended
            ));
            Err(format!(
                "{}: current={}, recommended={} — {} (automatic adjustment failed: {})",
                threshold.name, current, threshold.recommended, threshold.reason, err
            ))
        }
    }
}

fn read_sysctl_u64(path: &str) -> Result<u64, std::io::Error> {
    let content = fs::read_to_string(Path::new(path))?;
    content
        .trim()
        .parse()
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "parse failed"))
}

fn write_sysctl_u64(path: &str, value: u64) -> Result<(), std::io::Error> {
    fs::write(Path::new(path), format!("{value}\n"))
}
