mod support;

use std::fs::OpenOptions;
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use support::*;
use uvm_nbd::{device_size_bytes, MemTarget, NbdDevice, NbdOptions, OverlaybdTarget};

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
    let deadline = Instant::now() + Duration::from_secs(5);
    while sys_block_size(index).is_some() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(50));
    }
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

#[tokio::test]
async fn an_o_dsync_write_and_an_fdatasync_reach_the_target() {
    let name = "an_o_dsync_write_and_an_fdatasync_reach_the_target";
    if !nbd_available(name) {
        return;
    }
    let target = Arc::new(MemTarget::new(16 * MIB, BLOCK_SIZE));
    let device = NbdDevice::start(target.clone(), options(2))
        .await
        .expect("start the nbd device");
    let index = device.index();

    let payload = Aligned::filled(4096, 0xA1);
    {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_DIRECT | libc::O_DSYNC)
            .open(device.device_path())
            .expect("open the device with O_DSYNC");
        file.write_all_at(payload.as_ref(), 4096)
            .expect("O_DSYNC pwrite");
    }
    let durable = target.fua_writes() + target.flushes();
    eprintln!(
        "nbd{index}: after an O_DSYNC write the target saw {} FUA writes and {} flushes",
        target.fua_writes(),
        target.flushes()
    );
    assert!(
        durable > 0,
        "an O_DSYNC write must reach the target as a FUA write or a flush, not as a plain write"
    );
    assert_eq!(target.snapshot(4096, 4096), payload.as_ref());

    let flushes_before = target.flushes();
    {
        let file = direct(device.device_path(), true).expect("open the device with O_DIRECT");
        let second = Aligned::filled(4096, 0xB2);
        file.write_all_at(second.as_ref(), 8192).expect("pwrite");
        file.sync_data().expect("fdatasync");
    }
    eprintln!(
        "nbd{index}: an explicit fdatasync took the flush count from {flushes_before} to {}",
        target.flushes()
    );
    assert!(
        target.flushes() > flushes_before,
        "an explicit fdatasync must reach the target's flush handler"
    );

    device.stop().await.expect("stop the nbd device");
    assert_no_capacity(index);
}

#[tokio::test]
async fn an_overlaybd_write_and_flush_are_visible_to_a_reopened_image() {
    let name = "an_overlaybd_write_and_flush_are_visible_to_a_reopened_image";
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
            .expect("open the overlaybd target"),
    );
    let device = NbdDevice::start(target, options(2))
        .await
        .expect("start the nbd device");
    let index = device.index();

    let payload = pattern(8192, 0x4D);
    let mut buf = Aligned::new(8192);
    buf.as_mut().copy_from_slice(&payload);
    {
        let file = direct(device.device_path(), true).expect("open the device with O_DIRECT");
        file.write_all_at(buf.as_ref(), 12288).expect("pwrite");
        file.sync_data().expect("fdatasync");
    }
    device.stop().await.expect("stop the nbd device");
    assert_no_capacity(index);

    let service = overlaybd::ImageService::from_config_path(&global_config)
        .await
        .expect("open the image service");
    let image = service
        .create_image_file(&image_config)
        .await
        .expect("reopen the image");
    let read = overlaybd::virtual_file::VirtualFile::read_at(&image, 12288, 8192)
        .await
        .expect("read the image");
    assert_eq!(
        read.as_ref(),
        payload.as_slice(),
        "the flushed bytes must be in the image, not only in the device's cache"
    );
}

#[tokio::test]
async fn a_discard_through_the_device_zeroes_the_overlaybd_image() {
    let name = "a_discard_through_the_device_zeroes_the_overlaybd_image";
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
            .expect("open the overlaybd target"),
    );
    let device = NbdDevice::start(target, options(2))
        .await
        .expect("start the nbd device");
    let index = device.index();

    let primed = Aligned::filled(32768, 0xE7);
    {
        let file = direct(device.device_path(), true).expect("open the device with O_DIRECT");
        file.write_all_at(primed.as_ref(), 0).expect("pwrite");
        file.sync_data().expect("fdatasync");

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
        file.sync_data().expect("fdatasync after discard");
    }

    let mut read_back = Aligned::new(32768);
    {
        let file = direct(device.device_path(), false).expect("open the device with O_DIRECT");
        file.read_exact_at(read_back.as_mut(), 0).expect("pread");
    }
    let bytes = read_back.as_ref();
    assert!(bytes[..8192].iter().all(|&byte| byte == 0xE7));
    assert!(
        bytes[8192..24576].iter().all(|&byte| byte == 0),
        "the discarded range must read back as zeros through the device"
    );
    assert!(bytes[24576..].iter().all(|&byte| byte == 0xE7));

    device.stop().await.expect("stop the nbd device");
    assert_no_capacity(index);

    let service = overlaybd::ImageService::from_config_path(&global_config)
        .await
        .expect("open the image service");
    let image = service
        .create_image_file(&image_config)
        .await
        .expect("reopen the image");
    let read = overlaybd::virtual_file::VirtualFile::read_at(&image, 8192, 16384)
        .await
        .expect("read the image");
    assert!(
        read.iter().all(|&byte| byte == 0),
        "the discard must have reached the image, not only the device's cache"
    );
}
