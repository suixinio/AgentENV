use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use uvm_nbd::{device_size_bytes, MemTarget, NbdDevice, NbdOptions, OverlaybdTarget};

const MIB: usize = 1024 * 1024;
const BLOCK_SIZE: u32 = 4096;
const BLKDISCARD: libc::c_ulong = 0x1277;

// CAP_SYS_ADMIN is not probed: a caller that reaches the device nodes but
// cannot reach the netlink family gets a loud EPERM from the first connect.
fn nbd_available(test: &str) -> bool {
    let reason = if !Path::new("/sys/module/nbd").exists() {
        Some("the nbd module is not loaded")
    } else if !a_device_node_is_writable() {
        Some(
            "no /dev/nbd* node is readable and writable by this account; run              scripts/tests/setup-nbd-access.sh",
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
fn a_device_node_is_writable() -> bool {
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

fn options(connections: u16) -> NbdOptions {
    NbdOptions {
        connections,
        io_timeout: Duration::from_secs(30),
        dead_conn_timeout: None,
        queue_depth: 32,
        backend_identifier: None,
        destroy_on_disconnect: false,
    }
}

// A heap buffer with a 4 KiB aligned window, which O_DIRECT requires.
struct Aligned {
    raw: Vec<u8>,
    offset: usize,
    len: usize,
}

impl Aligned {
    fn new(len: usize) -> Self {
        let raw = vec![0u8; len + BLOCK_SIZE as usize];
        let offset = raw.as_ptr().align_offset(BLOCK_SIZE as usize);
        Self { raw, offset, len }
    }

    fn filled(len: usize, byte: u8) -> Self {
        let mut buf = Self::new(len);
        buf.as_mut().fill(byte);
        buf
    }

    fn as_ref(&self) -> &[u8] {
        &self.raw[self.offset..self.offset + self.len]
    }

    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.raw[self.offset..self.offset + self.len]
    }
}

fn direct(path: &Path, write: bool) -> std::io::Result<File> {
    OpenOptions::new()
        .read(true)
        .write(write)
        .custom_flags(libc::O_DIRECT)
        .open(path)
}

fn pattern(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|index| (index as u8).wrapping_mul(31).wrapping_add(seed))
        .collect()
}

fn sys_block_size(index: u32) -> Option<u64> {
    let path = format!("/sys/block/nbd{index}/size");
    std::fs::read_to_string(path)
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
}

fn raise_max_sectors(index: u32) -> Option<u64> {
    let hw =
        std::fs::read_to_string(format!("/sys/block/nbd{index}/queue/max_hw_sectors_kb")).ok()?;
    let hw = hw.trim().to_string();
    let path = format!("/sys/block/nbd{index}/queue/max_sectors_kb");
    let mut file = OpenOptions::new().write(true).open(&path).ok()?;
    file.write_all(hw.as_bytes()).ok()?;
    hw.parse::<u64>().ok()
}

fn assert_no_capacity(index: u32) {
    let size = sys_block_size(index);
    assert!(
        matches!(size, None | Some(0)),
        "nbd{index} still reports {size:?} sectors after stop"
    );
}

#[tokio::test]
async fn a_memory_target_comes_up_as_a_device_node_reporting_its_size() {
    let name = "a_memory_target_comes_up_as_a_device_node_reporting_its_size";
    if !nbd_available(name) {
        return;
    }
    let target = Arc::new(MemTarget::new(64 * MIB, BLOCK_SIZE));
    let device = NbdDevice::start(target, options(2))
        .await
        .expect("start the nbd device");
    let index = device.index();
    assert_eq!(
        device.device_path(),
        PathBuf::from(format!("/dev/nbd{index}"))
    );
    assert_eq!(
        device_size_bytes(index).expect("BLKGETSIZE64"),
        64 * MIB as u64
    );
    device.stop().await.expect("stop the nbd device");
    assert_no_capacity(index);
}

#[tokio::test]
async fn bytes_written_through_the_device_reach_the_target_and_read_back() {
    let name = "bytes_written_through_the_device_reach_the_target_and_read_back";
    if !nbd_available(name) {
        return;
    }
    let target = Arc::new(MemTarget::new(64 * MIB, BLOCK_SIZE));
    let device = NbdDevice::start(target.clone(), options(2))
        .await
        .expect("start the nbd device");
    let index = device.index();

    let payload = pattern(4096, 0x11);
    let mut direct_buf = Aligned::new(4096);
    direct_buf.as_mut().copy_from_slice(&payload);
    {
        let file = direct(device.device_path(), true).expect("open the device with O_DIRECT");
        file.write_all_at(direct_buf.as_ref(), 8192)
            .expect("O_DIRECT pwrite");
        file.sync_all().expect("fsync after the O_DIRECT write");
    }
    assert_eq!(target.snapshot(8192, 4096), payload);

    let mut read_back = Aligned::new(4096);
    {
        let file = direct(device.device_path(), false).expect("open the device with O_DIRECT");
        file.read_exact_at(read_back.as_mut(), 8192)
            .expect("O_DIRECT pread");
    }
    assert_eq!(read_back.as_ref(), payload.as_slice());

    let buffered = pattern(4096, 0x77);
    {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(device.device_path())
            .expect("open the device buffered");
        file.write_all_at(&buffered, 3 * 4096)
            .expect("buffered pwrite");
        file.sync_all().expect("fsync after the buffered write");
    }
    assert_eq!(target.snapshot(3 * 4096, 4096), buffered);
    assert!(
        target.flushes() > 0,
        "a guest fsync must reach the target's flush handler"
    );

    device.stop().await.expect("stop the nbd device");
    assert_no_capacity(index);
}

#[tokio::test]
async fn a_single_thirty_two_mib_read_completes_through_the_device() {
    let name = "a_single_thirty_two_mib_read_completes_through_the_device";
    if !nbd_available(name) {
        return;
    }
    let target = Arc::new(MemTarget::new(64 * MIB, BLOCK_SIZE));
    let expected = pattern(32 * MIB, 0x5A);
    target.fill(0, &expected);
    let device = NbdDevice::start(target.clone(), options(2))
        .await
        .expect("start the nbd device");
    let index = device.index();
    let max_sectors_kb = raise_max_sectors(index);

    let mut buf = Aligned::new(32 * MIB);
    {
        let file = direct(device.device_path(), false).expect("open the device with O_DIRECT");
        file.read_exact_at(buf.as_mut(), 0)
            .expect("a single 32 MiB O_DIRECT pread");
    }
    assert_eq!(buf.as_ref(), expected.as_slice());
    eprintln!(
        "nbd{index}: max_sectors_kb={max_sectors_kb:?}, largest request the target saw = {} bytes",
        target.largest_request()
    );

    device.stop().await.expect("stop the nbd device");
    assert_no_capacity(index);
}

#[tokio::test]
async fn concurrent_reads_from_eight_threads_all_return_the_right_bytes() {
    let name = "concurrent_reads_from_eight_threads_all_return_the_right_bytes";
    if !nbd_available(name) {
        return;
    }
    let target = Arc::new(MemTarget::new(64 * MIB, BLOCK_SIZE));
    for slot in 0..8u8 {
        target.fill(slot as u64 * MIB as u64, &pattern(MIB, slot));
    }
    let device = NbdDevice::start(target.clone(), options(2))
        .await
        .expect("start the nbd device");
    let index = device.index();

    let path = device.device_path().to_path_buf();
    let mut readers = Vec::new();
    for slot in 0..8u8 {
        let path = path.clone();
        readers.push(std::thread::spawn(move || {
            let file = direct(&path, false).expect("open the device with O_DIRECT");
            for _ in 0..8 {
                let mut buf = Aligned::new(MIB);
                file.read_exact_at(buf.as_mut(), slot as u64 * MIB as u64)
                    .expect("O_DIRECT pread");
                assert_eq!(
                    buf.as_ref(),
                    pattern(MIB, slot).as_slice(),
                    "reader {slot} read another slot's bytes"
                );
            }
        }));
    }
    for reader in readers {
        reader.join().expect("a concurrent reader failed");
    }

    device.stop().await.expect("stop the nbd device");
    assert_no_capacity(index);
}

#[tokio::test]
async fn a_discard_zeroes_the_range_on_a_target_that_supports_it() {
    let name = "a_discard_zeroes_the_range_on_a_target_that_supports_it";
    if !nbd_available(name) {
        return;
    }
    let target = Arc::new(MemTarget::new(64 * MIB, BLOCK_SIZE));
    target.fill(0, &vec![0xFFu8; 64 * 1024]);
    let device = NbdDevice::start(target.clone(), options(2))
        .await
        .expect("start the nbd device");
    let index = device.index();

    {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(device.device_path())
            .expect("open the device for write");
        let range: [u64; 2] = [8192, 16384];
        let rc = unsafe {
            libc::ioctl(
                std::os::fd::AsRawFd::as_raw_fd(&file),
                BLKDISCARD,
                std::ptr::addr_of!(range),
            )
        };
        assert_eq!(
            rc,
            0,
            "BLKDISCARD failed: {:?}",
            std::io::Error::last_os_error()
        );
    }

    assert!(target.snapshot(0, 8192).iter().all(|&byte| byte == 0xFF));
    assert!(target.snapshot(8192, 16384).iter().all(|&byte| byte == 0));
    assert!(target
        .snapshot(8192 + 16384, 4096)
        .iter()
        .all(|&byte| byte == 0xFF));

    device.stop().await.expect("stop the nbd device");
    assert_no_capacity(index);
}

#[tokio::test]
async fn a_read_only_target_refuses_a_write_through_the_device() {
    let name = "a_read_only_target_refuses_a_write_through_the_device";
    if !nbd_available(name) {
        return;
    }
    let target = Arc::new(MemTarget::new(64 * MIB, BLOCK_SIZE).read_only(true));
    let device = NbdDevice::start(target.clone(), options(2))
        .await
        .expect("start the nbd device");
    let index = device.index();

    match direct(device.device_path(), true) {
        Err(err) => {
            assert!(
                matches!(err.raw_os_error(), Some(libc::EACCES | libc::EROFS)),
                "opening a read-only nbd device for write failed with {err:?}"
            );
        }
        Ok(file) => {
            let buf = Aligned::filled(4096, 0x42);
            let err = file
                .write_all_at(buf.as_ref(), 0)
                .expect_err("a read-only device must refuse a write");
            assert!(
                matches!(
                    err.raw_os_error(),
                    Some(libc::EPERM | libc::EROFS | libc::EACCES | libc::EIO)
                ),
                "a write to a read-only nbd device failed with {err:?}"
            );
        }
    }
    assert!(target.snapshot(0, 4096).iter().all(|&byte| byte == 0));

    device.stop().await.expect("stop the nbd device");
    assert_no_capacity(index);
}

#[tokio::test]
async fn a_second_device_starts_after_the_first_one_is_stopped() {
    let name = "a_second_device_starts_after_the_first_one_is_stopped";
    if !nbd_available(name) {
        return;
    }
    let first = Arc::new(MemTarget::new(32 * MIB, BLOCK_SIZE));
    let device = NbdDevice::start(first, options(2))
        .await
        .expect("start the first nbd device");
    let first_index = device.index();
    device.stop().await.expect("stop the first nbd device");
    assert_no_capacity(first_index);

    let second = Arc::new(MemTarget::new(16 * MIB, BLOCK_SIZE));
    let device = NbdDevice::start(second, options(2))
        .await
        .expect("start the second nbd device");
    let second_index = device.index();
    assert_eq!(
        device_size_bytes(second_index).expect("BLKGETSIZE64"),
        16 * MIB as u64
    );
    device.stop().await.expect("stop the second nbd device");
    assert_no_capacity(second_index);
}

#[tokio::test]
async fn a_device_asked_to_self_destroy_leaves_no_node_behind() {
    let name = "a_device_asked_to_self_destroy_leaves_no_node_behind";
    if !nbd_available(name) {
        return;
    }
    let target = Arc::new(MemTarget::new(16 * MIB, BLOCK_SIZE));
    let device = NbdDevice::start(
        target,
        NbdOptions {
            destroy_on_disconnect: true,
            ..options(2)
        },
    )
    .await
    .expect("start the nbd device");
    let index = device.index();
    device.stop().await.expect("stop the nbd device");
    assert_eq!(
        sys_block_size(index),
        None,
        "DESTROY_ON_DISCONNECT removes the gendisk, it does not merely empty it"
    );
}

#[tokio::test]
async fn update_size_grows_the_device_the_kernel_reports() {
    let name = "update_size_grows_the_device_the_kernel_reports";
    if !nbd_available(name) {
        return;
    }
    let target = Arc::new(MemTarget::new(32 * MIB, BLOCK_SIZE));
    let device = NbdDevice::start(target.clone(), options(2))
        .await
        .expect("start the nbd device");
    let index = device.index();
    assert_eq!(
        device_size_bytes(index).expect("BLKGETSIZE64"),
        32 * MIB as u64
    );

    target.resize(64 * MIB);
    let grown = device.update_size(64 * MIB as u64).await;
    let observed = device_size_bytes(index).expect("BLKGETSIZE64");
    grown.unwrap_or_else(|err| {
        panic!(
            "update_size(64 MiB) on nbd{index} left the device at {observed} bytes: {err:#}. \
             Linux 6.1 acts on NBD_ATTR_SIZE_BYTES in NBD_CMD_RECONFIGURE with no sockets \
             attached; a kernel that does not needs a different resize path, because \
             NBD_SET_SIZE is refused with EBUSY on a netlink-configured device."
        )
    });
    assert_eq!(observed, 64 * MIB as u64);

    let mut buf = Aligned::new(4096);
    let file = direct(device.device_path(), false).expect("open the device with O_DIRECT");
    file.read_exact_at(buf.as_mut(), 40 * MIB as u64)
        .expect("read past the original capacity");

    drop(file);
    device.stop().await.expect("stop the nbd device");
    assert_no_capacity(index);
}

#[tokio::test]
async fn an_overlaybd_image_serves_reads_and_writes_through_the_device() {
    let name = "an_overlaybd_image_serves_reads_and_writes_through_the_device";
    if !nbd_available(name) {
        return;
    }
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let global_config = write_global_config(&tmp);
    let upper_data = tmp.path().join("upper.data");
    overlaybd::helper::prepare_runtime_upper(
        &upper_data,
        None,
        16 * MIB as u64,
        overlaybd::config::UpperMode::Sparse,
    )
    .expect("prepare the sparse upper");
    let image_config = write_image_config(&tmp, &upper_data);

    let target = Arc::new(
        OverlaybdTarget::open(&global_config, &image_config)
            .await
            .expect("open the overlaybd nbd target"),
    );
    let device = NbdDevice::start(target.clone(), options(2))
        .await
        .expect("start the nbd device");
    let index = device.index();
    assert_eq!(
        device_size_bytes(index).expect("BLKGETSIZE64"),
        16 * MIB as u64
    );

    let payload = pattern(4096, 0x2C);
    let mut buf = Aligned::new(4096);
    buf.as_mut().copy_from_slice(&payload);
    {
        let file = direct(device.device_path(), true).expect("open the device with O_DIRECT");
        file.write_all_at(buf.as_ref(), 4096)
            .expect("O_DIRECT pwrite");
        file.sync_all().expect("fsync after the O_DIRECT write");
    }
    let mut read_back = Aligned::new(4096);
    {
        let file = direct(device.device_path(), false).expect("open the device with O_DIRECT");
        file.read_exact_at(read_back.as_mut(), 4096)
            .expect("O_DIRECT pread");
    }
    assert_eq!(read_back.as_ref(), payload.as_slice());

    let second = tempfile::TempDir::new().expect("tempdir");
    let second_global = write_global_config(&second);
    let second_upper = second.path().join("upper.data");
    overlaybd::helper::prepare_runtime_upper(
        &second_upper,
        None,
        16 * MIB as u64,
        overlaybd::config::UpperMode::Sparse,
    )
    .expect("prepare the second sparse upper");
    let second_config = write_image_config(&second, &second_upper);
    let service = overlaybd::ImageService::from_config_path(&second_global)
        .await
        .expect("open the second image service");
    let image = Arc::new(
        service
            .create_image_file(&second_config)
            .await
            .expect("open the second image"),
    );
    target
        .swap_state(second_config, image, true)
        .expect("swap the backing image");

    let mut after_swap = Aligned::new(4096);
    {
        let file = direct(device.device_path(), false).expect("open the device with O_DIRECT");
        file.read_exact_at(after_swap.as_mut(), 4096)
            .expect("O_DIRECT pread after the swap");
    }
    assert!(
        after_swap.as_ref().iter().all(|&byte| byte == 0),
        "the swapped-in image must not answer with the previous image's bytes"
    );

    device.stop().await.expect("stop the nbd device");
    assert_no_capacity(index);
}

fn write_global_config(tmp: &tempfile::TempDir) -> PathBuf {
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

fn write_image_config(tmp: &tempfile::TempDir, upper_data: &Path) -> PathBuf {
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
