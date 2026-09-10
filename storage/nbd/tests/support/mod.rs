//! Helpers shared by the `uvm-nbd` device suites.
#![allow(dead_code)]

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;
use uvm_nbd::NbdOptions;

pub const MIB: usize = 1024 * 1024;
pub const BLOCK_SIZE: u32 = 4096;
pub const BLKDISCARD: libc::c_ulong = 0x1277;

// CAP_SYS_ADMIN is not probed: a caller that reaches the device nodes but
// cannot reach the netlink family gets a loud EPERM from the first connect.
pub fn nbd_available(test: &str) -> bool {
    let reason = if !Path::new("/sys/module/nbd").exists() {
        Some("the nbd module is not loaded")
    } else if !a_device_node_is_writable() {
        Some(
            "no /dev/nbd* node is readable and writable by this account; run \
             scripts/tests/setup-nbd-access.sh",
        )
    } else {
        None
    };
    let Some(reason) = reason else {
        return true;
    };
    if std::env::var("AENV_NBD_TEST_REQUIRED").as_deref() == Ok("1") {
        panic!("AENV_NBD_TEST_REQUIRED=1 but {test} cannot run: {reason}");
    }
    eprintln!("SKIPPED[nbd]: {test} ({reason})");
    false
}

// The kernel allocates a device on connect when none is free, so an empty
// /dev/nbd* is not on its own a reason to skip; an unreachable node is.
pub fn a_device_node_is_writable() -> bool {
    let Ok(entries) = std::fs::read_dir("/dev") else {
        return false;
    };
    let mut nodes = 0usize;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if !name.starts_with("nbd") || !name[3..].chars().all(|c| c.is_ascii_digit()) {
            continue;
        }
        nodes += 1;
        let path = std::ffi::CString::new(entry.path().as_os_str().as_encoded_bytes())
            .expect("a /dev path holds no interior nul");
        if unsafe { libc::access(path.as_ptr(), libc::R_OK | libc::W_OK) } == 0 {
            return true;
        }
    }
    nodes == 0
}

/// Everything but the connection count and a shorter io timeout is left at the
/// production default, `dead_conn_timeout` above all: without a reconnect
/// window nothing the supervisor installs can be waited for.
pub fn options(connections: u16) -> NbdOptions {
    NbdOptions {
        connections,
        io_timeout: Duration::from_secs(30),
        queue_depth: 32,
        ..Default::default()
    }
}

// A heap buffer with a 4 KiB aligned window, which O_DIRECT requires.
pub struct Aligned {
    raw: Vec<u8>,
    offset: usize,
    len: usize,
}

impl Aligned {
    pub fn new(len: usize) -> Self {
        let raw = vec![0u8; len + BLOCK_SIZE as usize];
        let offset = raw.as_ptr().align_offset(BLOCK_SIZE as usize);
        Self { raw, offset, len }
    }

    pub fn filled(len: usize, byte: u8) -> Self {
        let mut buf = Self::new(len);
        buf.as_mut().fill(byte);
        buf
    }

    pub fn as_ref(&self) -> &[u8] {
        &self.raw[self.offset..self.offset + self.len]
    }

    pub fn as_mut(&mut self) -> &mut [u8] {
        &mut self.raw[self.offset..self.offset + self.len]
    }
}

pub fn direct(path: &Path, write: bool) -> std::io::Result<File> {
    OpenOptions::new()
        .read(true)
        .write(write)
        .custom_flags(libc::O_DIRECT)
        .open(path)
}

pub fn pattern(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|index| (index as u8).wrapping_mul(31).wrapping_add(seed))
        .collect()
}

pub fn sys_block_size(index: u32) -> Option<u64> {
    let path = format!("/sys/block/nbd{index}/size");
    std::fs::read_to_string(path)
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
}

pub fn raise_max_sectors(index: u32) -> Option<u64> {
    let hw =
        std::fs::read_to_string(format!("/sys/block/nbd{index}/queue/max_hw_sectors_kb")).ok()?;
    let hw = hw.trim().to_string();
    let path = format!("/sys/block/nbd{index}/queue/max_sectors_kb");
    let mut file = OpenOptions::new().write(true).open(&path).ok()?;
    file.write_all(hw.as_bytes()).ok()?;
    hw.parse::<u64>().ok()
}

pub fn assert_no_capacity(index: u32) {
    let size = sys_block_size(index);
    assert!(
        matches!(size, None | Some(0)),
        "nbd{index} still reports {size:?} sectors after stop"
    );
}

pub fn write_global_config(tmp: &tempfile::TempDir) -> PathBuf {
    let path = tmp.path().join("overlaybd-global.json");
    std::fs::write(
        &path,
        serde_json::to_vec_pretty(&serde_json::json!({
            "registryFsVersion": "v2",
            "nrIoRings": 1,
            "cacheConfig": {
                "cacheType": "file",
                "cacheDir": tmp.path().join("cache"),
                "cacheSizeGB": 1,
                "refillSize": 262144,
                "blockSize": 65536
            },
            "download": { "enable": false }
        }))
        .expect("encode the overlaybd global config"),
    )
    .expect("write the overlaybd global config");
    path
}

pub fn write_image_config(tmp: &tempfile::TempDir, upper_data: &Path) -> PathBuf {
    let path = tmp.path().join("overlaybd-image.json");
    std::fs::write(
        &path,
        serde_json::to_vec_pretty(&serde_json::json!({
            "lowers": [],
            "upper": { "mode": "sparse", "data": upper_data },
            "resultFile": tmp.path().join("result.txt")
        }))
        .expect("encode the overlaybd image config"),
    )
    .expect("write the overlaybd image config");
    path
}
