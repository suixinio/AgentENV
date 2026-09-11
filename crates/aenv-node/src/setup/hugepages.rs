//! Host prerequisites for guest memory on 2 MiB hugetlbfs pages: Firecracker
//! maps it `MAP_NORESERVE` and takes pages from the pool on demand, and a
//! fault that finds none is a `SIGBUS` in the VMM.

use anyhow::{bail, Context, Result};
use tracing::{info, warn};

const NR_HUGEPAGES: &str = "/proc/sys/vm/nr_hugepages";
const NR_OVERCOMMIT_HUGEPAGES: &str = "/proc/sys/vm/nr_overcommit_hugepages";
const SYSCTL_CONF: &str = "/etc/sysctl.d/99-agentenv-hugepages.conf";
const HUGEPAGE_BYTES: u64 = 2 << 20;

fn read_u64(path: &str) -> Result<u64> {
    let text = std::fs::read_to_string(path).with_context(|| format!("read {path}"))?;
    text.trim()
        .parse()
        .with_context(|| format!("parse {path}: {text:?}"))
}

fn mem_total_bytes() -> Result<u64> {
    let meminfo = std::fs::read_to_string("/proc/meminfo").context("read /proc/meminfo")?;
    let kib: u64 = meminfo
        .lines()
        .find_map(|line| line.strip_prefix("MemTotal:"))
        .and_then(|rest| rest.split_whitespace().next())
        .context("MemTotal not found in /proc/meminfo")?
        .parse()
        .context("parse MemTotal")?;
    Ok(kib * 1024)
}

/// One-time root provisioning: lets the kernel assemble 2 MiB pages on demand
/// for all of RAM. A boot-time reservation (`vm.nr_hugepages`) stays the
/// operator's to size: on-demand assembly fails once free memory is
/// fragmented, and only a reservation made early at boot is immune to that.
pub fn provision() -> Result<()> {
    let pages = mem_total_bytes()? / HUGEPAGE_BYTES;
    let current = read_u64(NR_OVERCOMMIT_HUGEPAGES)?;
    if current >= pages {
        info!(current, "hugepage overcommit already covers all of RAM");
        return Ok(());
    }
    info!(pages, "allowing 2 MiB hugepages to be assembled on demand");
    std::fs::create_dir_all("/etc/sysctl.d").context("create /etc/sysctl.d")?;
    std::fs::write(
        SYSCTL_CONF,
        format!("# Managed by agentenv server setup\nvm.nr_overcommit_hugepages = {pages}\n"),
    )
    .with_context(|| format!("install {SYSCTL_CONF}"))?;
    std::fs::write(NR_OVERCOMMIT_HUGEPAGES, format!("{pages}\n"))
        .with_context(|| format!("write {NR_OVERCOMMIT_HUGEPAGES}"))?;
    Ok(())
}

/// Startup validation: 2 MiB pages must be obtainable, from a reservation or
/// on demand.
pub fn check() -> Result<()> {
    let reserved = read_u64(NR_HUGEPAGES)?;
    let overcommit = read_u64(NR_OVERCOMMIT_HUGEPAGES)?;
    if reserved == 0 && overcommit == 0 {
        bail!(
            "hugepage-backed guest memory needs 2 MiB pages, but vm.nr_hugepages and \
             vm.nr_overcommit_hugepages are both 0; run `server --setup-host` as root or reserve \
             a pool with vm.nr_hugepages"
        );
    }
    if reserved == 0 {
        warn!(
            overcommit,
            "no reserved hugepage pool (vm.nr_hugepages=0); on-demand assembly fails once free \
             memory is fragmented, and a VM that gets no page is killed by SIGBUS"
        );
    } else {
        info!(reserved, overcommit, "2 MiB hugepages available");
    }
    Ok(())
}
