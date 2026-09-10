//! A userspace block target exposed as `/dev/nbdN` through the in-kernel nbd
//! driver: generic netlink for control, one socket per connection for data.

use anyhow::{bail, Context, Result};
use std::path::{Path, PathBuf};
use std::time::Duration;

mod device;
pub mod impls;
mod netlink;
mod proto;
mod target;

pub use device::{NbdDevice, NbdOptions};
pub use impls::{MemTarget, OverlaybdTarget};
pub use netlink::{ConnectSpec, NbdNetlink, ReconfigureSpec};
pub use proto::{
    reply_error, NbdCommand, NbdReply, NbdRequest, NBD_CFLAG_DESTROY_ON_DISCONNECT,
    NBD_CMD_FLAG_FUA, NBD_FLAG_CAN_MULTI_CONN, NBD_FLAG_HAS_FLAGS, NBD_FLAG_READ_ONLY,
    NBD_FLAG_SEND_FLUSH, NBD_FLAG_SEND_FUA, NBD_FLAG_SEND_TRIM, REPLY_LEN, REQUEST_LEN,
};
pub use target::{Geometry, NbdTarget};

nix::ioctl_read!(blkgetsize64, 0x12, 114, u64);

pub fn device_path(index: u32) -> PathBuf {
    PathBuf::from(format!("/dev/nbd{index}"))
}

/// The capacity the kernel reports for `/dev/nbd<index>` via `BLKGETSIZE64`.
pub fn device_size_bytes(index: u32) -> Result<u64> {
    let path = device_path(index);
    let file = std::fs::File::open(&path).with_context(|| format!("open {}", path.display()))?;
    read_size(&file).with_context(|| format!("BLKGETSIZE64 on {}", path.display()))
}

fn read_size(file: &std::fs::File) -> Result<u64> {
    use std::os::fd::AsRawFd;
    let mut size = 0u64;
    // SAFETY: `file` is an open block device and `size` outlives the call.
    unsafe { blkgetsize64(file.as_raw_fd(), &mut size) }?;
    Ok(size)
}

/// Wait for `/dev/nbd<index>` to become readable and report `expected_size`;
/// the node can appear, and its capacity settle, after CONNECT returns.
pub fn wait_for_nbd_dev(index: u32, expected_size: u64) -> Result<()> {
    let path = device_path(index);
    // Total around 30 seconds timeout.
    let interval_ms = [10, 100, 500, 1000];
    let retry_times = [5, 10, 10, 24];
    let mut warned_permission = false;
    let mut last_size = None;
    for (interval, retry) in interval_ms.into_iter().zip(retry_times) {
        for _ in 0..retry {
            match probe(&path) {
                Ok(size) if size == expected_size => return Ok(()),
                Ok(size) => last_size = Some(size),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                    if !warned_permission {
                        tracing::warn!(
                            path = %path.display(),
                            "nbd device is not readable yet; udev rules may still be applying"
                        );
                        warned_permission = true;
                    }
                }
                Err(error) => {
                    return Err(error).with_context(|| format!("open {}", path.display()))
                }
            }
            std::thread::sleep(Duration::from_millis(interval));
        }
    }
    bail!(
        "nbd device {index} did not report {expected_size} bytes at {} (last seen: {last_size:?})",
        path.display()
    );
}

fn probe(path: &Path) -> std::io::Result<u64> {
    let file = std::fs::File::open(path)?;
    read_size(&file).map_err(|err| std::io::Error::other(format!("{err:#}")))
}
