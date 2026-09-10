mod support;

use std::sync::Arc;
use std::time::{Duration, Instant};
use support::*;
use uvm_nbd::{MemTarget, NbdDevice, NbdOptions};

const DEVICES: usize = 32;
const WRITERS_PER_DEVICE: usize = 8;
const DEVICE_BYTES: usize = 16 * MIB;
const SLICE_BYTES: usize = DEVICE_BYTES / WRITERS_PER_DEVICE;
const WRITE_SECONDS: u64 = 10;

/// xorshift64*, so the write pattern is reproducible without a rand crate.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, bound: usize) -> usize {
        (self.next() % bound as u64) as usize
    }
}

#[tokio::test]
async fn thirty_two_devices_under_concurrent_writers_read_back_byte_for_byte() {
    let name = "thirty_two_devices_under_concurrent_writers_read_back_byte_for_byte";
    if !nbd_available(name) {
        return;
    }

    let options = NbdOptions {
        connections: 2,
        io_timeout: Duration::from_secs(60),
        queue_depth: 32,
        ..options(2)
    };
    let mut devices = Vec::with_capacity(DEVICES);
    for ordinal in 0..DEVICES {
        let target = Arc::new(MemTarget::new(DEVICE_BYTES, BLOCK_SIZE));
        let device = NbdDevice::start(target, options.clone())
            .await
            .unwrap_or_else(|err| panic!("start device {ordinal}: {err:#}"));
        devices.push(device);
    }
    let indices: Vec<u32> = devices.iter().map(|device| device.index()).collect();
    let largest = indices.iter().copied().max().expect("at least one device");
    eprintln!(
        "{DEVICES} devices at indices {:?}..={largest}",
        indices.iter().copied().min().unwrap()
    );
    assert!(
        largest >= 16,
        "{DEVICES} devices must push past the 16 the module preallocates, got {largest}"
    );

    let deadline = Instant::now() + Duration::from_secs(WRITE_SECONDS);
    let mut writers = Vec::with_capacity(DEVICES * WRITERS_PER_DEVICE);
    for (ordinal, device) in devices.iter().enumerate() {
        for slot in 0..WRITERS_PER_DEVICE {
            let path = device.device_path().to_path_buf();
            let seed = (ordinal * WRITERS_PER_DEVICE + slot + 1) as u64;
            writers.push(std::thread::spawn(move || {
                write_slice(&path, slot, seed, deadline)
            }));
        }
    }

    let mut shadows: Vec<Vec<(usize, Vec<u8>)>> = vec![Vec::new(); DEVICES];
    let mut written = 0u64;
    for (ordinal, writer) in writers.into_iter().enumerate() {
        let (slot, shadow, bytes) = writer.join().expect("a writer thread panicked");
        written += bytes;
        shadows[ordinal / WRITERS_PER_DEVICE].push((slot, shadow));
    }
    eprintln!(
        "{written} bytes written across {} writer threads in {WRITE_SECONDS}s",
        DEVICES * WRITERS_PER_DEVICE
    );

    for (ordinal, device) in devices.iter().enumerate() {
        let file = direct(device.device_path(), false).expect("open the device for read-back");
        for (slot, shadow) in &shadows[ordinal] {
            let mut buf = Aligned::new(SLICE_BYTES);
            std::os::unix::fs::FileExt::read_exact_at(
                &file,
                buf.as_mut(),
                (slot * SLICE_BYTES) as u64,
            )
            .expect("read back a writer's slice");
            let read = buf.as_ref();
            if read != shadow.as_slice() {
                let at = read
                    .iter()
                    .zip(shadow.iter())
                    .position(|(a, b)| a != b)
                    .unwrap_or(0);
                panic!(
                    "device {ordinal} (nbd{}) slot {slot} differs from the shadow at byte {at}: \
                     read {:#04x}, expected {:#04x}",
                    device.index(),
                    read[at],
                    shadow[at]
                );
            }
        }
    }

    for device in devices {
        let index = device.index();
        device
            .stop()
            .await
            .unwrap_or_else(|err| panic!("stop nbd{index}: {err:#}"));
    }
    for index in indices {
        assert_no_capacity(index);
    }
}

/// One writer's slice of the device: random 4 KiB and 64 KiB writes until the
/// deadline, mirrored into a shadow the read-back compares against.
fn write_slice(
    path: &std::path::Path,
    slot: usize,
    seed: u64,
    deadline: Instant,
) -> (usize, Vec<u8>, u64) {
    let file = direct(path, true).expect("open the device for write");
    let base = (slot * SLICE_BYTES) as u64;
    let mut shadow = vec![0u8; SLICE_BYTES];
    let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
    let mut written = 0u64;

    while Instant::now() < deadline {
        let len = if rng.next().is_multiple_of(2) {
            4096
        } else {
            64 * 1024
        };
        let slots = (SLICE_BYTES - len) / 4096 + 1;
        let offset = rng.below(slots) * 4096;
        let byte = (rng.next() % 251) as u8;

        let mut buf = Aligned::new(len);
        for (index, cell) in buf.as_mut().iter_mut().enumerate() {
            *cell = byte.wrapping_add((index % 97) as u8);
        }
        std::os::unix::fs::FileExt::write_all_at(&file, buf.as_ref(), base + offset as u64)
            .expect("pwrite");
        shadow[offset..offset + len].copy_from_slice(buf.as_ref());
        written += len as u64;
    }

    file.sync_all().expect("fsync the writer's slice");
    (slot, shadow, written)
}
